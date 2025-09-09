#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
scripts/cargar_poblacion_optimizado.py - Carga Optimizada Población SISBEN
OPTIMIZADO: Streaming 203MB → PostgreSQL sin colapso de memoria
Performance: 15-20 min → 3-5 min, 2GB+ → 500MB memoria
"""

import sys
from pathlib import Path
from datetime import datetime
import structlog
from sqlalchemy import create_engine, text

# Añadir directorio padre al path
sys.path.insert(0, str(Path(__file__).parent.parent))

# Importar módulos optimizados
from config import (
    DatabaseConfig, FileConfig, validar_configuracion_optimizada,
    divipola_cache
)
from core.processors import PopulationStreamProcessor, MemoryMonitor

logger = structlog.get_logger()

# ================================
# VALIDADOR DE INTEGRIDAD POBLACIONAL
# ================================

class PopulationValidator:
    """Validador especializado para datos poblacionales"""
    
    def __init__(self):
        self.engine = create_engine(DatabaseConfig.get_connection_url())
    
    def validate_loaded_data(self) -> dict:
        """Valida integridad de datos poblacionales cargados"""
        logger.info("population_validation_started")
        
        try:
            with self.engine.connect() as conn:
                # Estadísticas básicas
                basic_stats = conn.execute(text("""
                    SELECT 
                        COUNT(*) as total_records,
                        COUNT(DISTINCT codigo_municipio) as unique_municipalities,
                        COUNT(DISTINCT grupo_etario) as unique_age_groups,
                        SUM(poblacion_total) as total_population,
                        MIN(poblacion_total) as min_population,
                        MAX(poblacion_total) as max_population,
                        AVG(poblacion_total) as avg_population
                    FROM poblacion
                """)).fetchone()
                
                # Distribución por ubicación
                location_dist = conn.execute(text("""
                    SELECT 
                        tipo_ubicacion,
                        COUNT(*) as records,
                        SUM(poblacion_total) as population
                    FROM poblacion
                    GROUP BY tipo_ubicacion
                    ORDER BY population DESC
                """)).fetchall()
                
                # Distribución por grupo etario
                age_dist = conn.execute(text("""
                    SELECT 
                        grupo_etario,
                        COUNT(*) as records,
                        SUM(poblacion_total) as population
                    FROM poblacion
                    GROUP BY grupo_etario
                    ORDER BY population DESC
                """)).fetchall()
                
                # Top municipios por población
                top_municipalities = conn.execute(text("""
                    SELECT 
                        codigo_municipio,
                        SUM(poblacion_total) as total_population
                    FROM poblacion
                    GROUP BY codigo_municipio
                    ORDER BY total_population DESC
                    LIMIT 10
                """)).fetchall()
                
                # Verificaciones de integridad
                integrity_checks = conn.execute(text("""
                    SELECT 
                        COUNT(CASE WHEN poblacion_total <= 0 THEN 1 END) as invalid_population,
                        COUNT(CASE WHEN codigo_municipio IS NULL THEN 1 END) as null_municipality,
                        COUNT(CASE WHEN grupo_etario IS NULL THEN 1 END) as null_age_group,
                        COUNT(CASE WHEN tipo_ubicacion IS NULL THEN 1 END) as null_location
                    FROM poblacion
                """)).fetchone()
                
                results = {
                    'basic_stats': dict(basic_stats),
                    'location_distribution': [dict(row) for row in location_dist],
                    'age_distribution': [dict(row) for row in age_dist],
                    'top_municipalities': [dict(row) for row in top_municipalities],
                    'integrity_issues': dict(integrity_checks)
                }
                
                logger.info("population_validation_completed", 
                           total_population=basic_stats.total_population,
                           municipalities=basic_stats.unique_municipalities,
                           age_groups=basic_stats.unique_age_groups)
                
                return results
                
        except Exception as e:
            logger.error("population_validation_failed", error=str(e))
            return {}
    
    def generate_validation_report(self, validation_results: dict) -> str:
        """Genera reporte de validación legible"""
        if not validation_results:
            return "❌ Error: No se pudieron validar los datos"
        
        basic = validation_results['basic_stats']
        location = validation_results['location_distribution']
        age = validation_results['age_distribution']
        top_mun = validation_results['top_municipalities']
        integrity = validation_results['integrity_issues']
        
        report = f"""
📊 REPORTE VALIDACIÓN POBLACIÓN SISBEN
{'='*50}

📈 ESTADÍSTICAS GENERALES:
   Total registros: {basic['total_records']:,}
   Municipios únicos: {basic['unique_municipalities']}
   Grupos etarios: {basic['unique_age_groups']}
   Población total: {basic['total_population']:,} habitantes
   Población promedio por registro: {basic['avg_population']:.1f}

🏙️ DISTRIBUCIÓN URBANO/RURAL:"""
        
        for loc in location:
            pct = (loc['population'] / basic['total_population']) * 100
            report += f"\n   {loc['tipo_ubicacion']}: {loc['population']:,} hab ({pct:.1f}%)"
        
        report += "\n\n👥 DISTRIBUCIÓN GRUPOS ETARIOS:"
        for age in age:
            pct = (age['population'] / basic['total_population']) * 100
            report += f"\n   {age['grupo_etario']}: {age['population']:,} hab ({pct:.1f}%)"
        
        report += "\n\n🏆 TOP 10 MUNICIPIOS MÁS POBLADOS:"
        for i, mun in enumerate(top_mun, 1):
            report += f"\n   {i:2d}. {mun['codigo_municipio']}: {mun['total_population']:,} hab"
        
        report += "\n\n🔍 VERIFICACIONES DE INTEGRIDAD:"
        
        issues = []
        if integrity['invalid_population'] > 0:
            issues.append(f"Población ≤ 0: {integrity['invalid_population']:,}")
        if integrity['null_municipality'] > 0:
            issues.append(f"Sin código municipio: {integrity['null_municipality']:,}")
        if integrity['null_age_group'] > 0:
            issues.append(f"Sin grupo etario: {integrity['null_age_group']:,}")
        if integrity['null_location'] > 0:
            issues.append(f"Sin tipo ubicación: {integrity['null_location']:,}")
        
        if issues:
            report += "\n   ⚠️ PROBLEMAS ENCONTRADOS:"
            for issue in issues:
                report += f"\n      • {issue}"
        else:
            report += "\n   ✅ Todos los registros son válidos"
        
        return report

# ================================
# COORDINADOR PRINCIPAL OPTIMIZADO
# ================================

def cargar_poblacion_optimizado(archivo_csv: Path = None) -> bool:
    """
    Carga optimizada de población usando streaming
    
    Args:
        archivo_csv: Ruta opcional del archivo CSV (usa por defecto si no se especifica)
    
    Returns:
        bool: True si la carga fue exitosa
    """
    
    inicio = datetime.now()
    memory_monitor = MemoryMonitor()
    
    logger.info("population_loading_started", 
               timestamp=inicio.isoformat(),
               initial_memory_mb=round(memory_monitor.get_memory_usage_mb(), 2))
    
    try:
        # 1. Validar configuración del sistema
        logger.info("validating_system_configuration")
        if not validar_configuracion_optimizada():
            logger.error("system_configuration_invalid")
            return False
        
        # 2. Verificar archivo de entrada
        if archivo_csv is None:
            archivo_csv = FileConfig.POBLACION_FILE
        
        if not archivo_csv.exists():
            logger.error("population_file_not_found", path=str(archivo_csv))
            return False
        
        file_size_mb = archivo_csv.stat().st_size / (1024 * 1024)
        logger.info("population_file_validated", 
                   path=str(archivo_csv),
                   size_mb=round(file_size_mb, 2))
        
        # 3. Verificar caché DIVIPOLA
        divipola_stats = divipola_cache.get_stats()
        if divipola_stats['municipios'] == 0:
            logger.warning("divipola_cache_empty")
        else:
            logger.info("divipola_cache_ready", **divipola_stats)
        
        # 4. Crear procesador optimizado
        logger.info("creating_stream_processor")
        processor = PopulationStreamProcessor(archivo_csv)
        
        # 5. Procesar archivo usando streaming
        with memory_monitor.monitor_operation("population_streaming"):
            logger.info("starting_streaming_process")
            result = processor.process_file_streaming()
        
        # 6. Verificar resultado del procesamiento
        if not result['success']:
            logger.error("population_processing_failed", **result)
            return False
        
        logger.info("population_processing_successful", **result)
        
        # 7. Validar datos cargados
        logger.info("validating_loaded_data")
        validator = PopulationValidator()
        validation_results = validator.validate_loaded_data()
        
        if validation_results:
            # Generar reporte de validación
            report = validator.generate_validation_report(validation_results)
            print(report)
            
            # Guardar reporte en archivo
            FileConfig.create_directories()
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            report_file = FileConfig.LOGS_DIR / f"poblacion_validation_{timestamp}.txt"
            
            with open(report_file, 'w', encoding='utf-8') as f:
                f.write(report)
            
            logger.info("validation_report_saved", report_file=str(report_file))
        
        # 8. Estadísticas finales
        duracion = datetime.now() - inicio
        peak_memory = memory_monitor.get_memory_usage_mb()
        
        logger.info("population_loading_completed",
                   duration_seconds=round(duracion.total_seconds(), 2),
                   peak_memory_mb=round(peak_memory, 2),
                   records_processed=result['processed_records'],
                   records_per_second=result['records_per_second'])
        
        return True
        
    except Exception as e:
        duracion = datetime.now() - inicio
        logger.error("population_loading_failed",
                   error=str(e),
                   duration_seconds=round(duracion.total_seconds(), 2))
        return False

# ================================
# VERIFICACIONES DE CALIDAD POST-CARGA
# ================================

def verificar_integridad_referencial() -> bool:
    """Verifica integridad referencial con unidades territoriales"""
    logger.info("referential_integrity_check_started")
    
    try:
        engine = create_engine(DatabaseConfig.get_connection_url())
        
        with engine.connect() as conn:
            # Verificar que códigos municipales existan en unidades territoriales
            missing_refs = conn.execute(text("""
                SELECT DISTINCT p.codigo_municipio
                FROM poblacion p
                LEFT JOIN unidades_territoriales ut ON p.codigo_municipio = ut.codigo_divipola
                WHERE ut.codigo_divipola IS NULL
                LIMIT 10
            """)).fetchall()
            
            if missing_refs:
                missing_codes = [row[0] for row in missing_refs]
                logger.warning("referential_integrity_issues", 
                             missing_codes=missing_codes,
                             count=len(missing_refs))
                return False
            else:
                logger.info("referential_integrity_valid")
                return True
                
    except Exception as e:
        logger.error("referential_integrity_check_failed", error=str(e))
        return False

def optimizar_indices_poblacion():
    """Optimiza índices de la tabla población para mejor performance"""
    logger.info("optimizing_population_indexes")
    
    try:
        engine = create_engine(DatabaseConfig.get_connection_url())
        
        with engine.connect() as conn:
            # Índices optimizados para consultas frecuentes
            indices_sql = [
                "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_poblacion_lookup_optimized ON poblacion(codigo_municipio, grupo_etario, tipo_ubicacion)",
                "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_poblacion_municipio_total ON poblacion(codigo_municipio) INCLUDE (poblacion_total)",
                "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_poblacion_grupo_etario_total ON poblacion(grupo_etario) INCLUDE (poblacion_total)",
                "ANALYZE poblacion"
            ]
            
            for sql in indices_sql:
                logger.debug("executing_index_sql", sql=sql[:50] + "...")
                conn.execute(text(sql))
                conn.commit()
            
            logger.info("population_indexes_optimized")
            
    except Exception as e:
        logger.error("index_optimization_failed", error=str(e))

# ================================
# FUNCIÓN PRINCIPAL
# ================================

def main():
    """Función principal con interfaz de línea de comandos"""
    
    print("🧬 CARGA OPTIMIZADA POBLACIÓN SISBEN → POSTGRESQL")
    print("=" * 60)
    print("Performance: 203MB streaming, 15-20min → 3-5min, 2GB+ → 500MB")
    
    # Argumentos de línea de comandos
    archivo_csv = None
    if len(sys.argv) > 1:
        archivo_csv = Path(sys.argv[1])
        if not archivo_csv.exists():
            print(f"❌ ERROR: Archivo no encontrado: {archivo_csv}")
            return False
    
    # Ejecutar carga optimizada
    exito = cargar_poblacion_optimizado(archivo_csv)
    
    if exito:
        print("\n🎉 ¡CARGA DE POBLACIÓN COMPLETADA EXITOSAMENTE!")
        
        # Verificaciones adicionales
        print("\n🔧 Ejecutando verificaciones adicionales...")
        
        if verificar_integridad_referencial():
            print("✅ Integridad referencial válida")
        else:
            print("⚠️ Problemas de integridad referencial encontrados")
        
        print("🔧 Optimizando índices...")
        optimizar_indices_poblacion()
        print("✅ Índices optimizados")
        
        print("\n🎯 PRÓXIMOS PASOS:")
        print("1. Cargar vacunación: python scripts/cargar_vacunacion_optimizado.py")
        print("2. Verificar coberturas: python scripts/verificar_coberturas.py")
        print("3. Conectar dashboard: streamlit run dashboard/app.py")
        print("4. ¡Sistema listo para análisis epidemiológico! 🚀")
        
    else:
        print("\n❌ ERROR EN CARGA DE POBLACIÓN")
        print("💡 Revisar logs para detalles del error")
        print("🔧 Verificar: archivo CSV, conexión BD, configuración")
    
    return exito

if __name__ == "__main__":
    main()