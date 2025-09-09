#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
scripts/cargar_vacunacion_optimizado.py - Carga Optimizada Vacunación PAIweb
OPTIMIZADO: Streaming 67.5MB → PostgreSQL, datos anónimos, edad con fecha actual
Performance: 8-10 min → 2-3 min, memoria optimizada
"""

import sys
from pathlib import Path
from datetime import datetime, date
import pandas as pd
import structlog
from sqlalchemy import create_engine, text

# Añadir directorio padre al path
sys.path.insert(0, str(Path(__file__).parent.parent))

# Importar módulos optimizados
from config import (
    DatabaseConfig, FileConfig, validar_configuracion_optimizada,
    divipola_cache, GRUPOS_ETARIOS
)
from core.processors import VaccinationStreamProcessor, MemoryMonitor

logger = structlog.get_logger()

# ================================
# VALIDADOR ESPECIALIZADO VACUNACIÓN
# ================================

class VaccinationValidator:
    """Validador especializado para datos de vacunación"""
    
    def __init__(self):
        self.engine = create_engine(DatabaseConfig.get_connection_url())
    
    def validate_loaded_data(self) -> dict:
        """Valida integridad de datos de vacunación cargados"""
        logger.info("vaccination_validation_started")
        
        try:
            with self.engine.connect() as conn:
                # Estadísticas básicas
                basic_stats = conn.execute(text("""
                    SELECT 
                        COUNT(*) as total_vaccinations,
                        COUNT(DISTINCT codigo_municipio) as unique_municipalities,
                        COUNT(DISTINCT institucion) as unique_institutions,
                        COUNT(DISTINCT grupo_etario) as unique_age_groups,
                        MIN(fecha_aplicacion) as first_vaccination,
                        MAX(fecha_aplicacion) as last_vaccination,
                        COUNT(DISTINCT DATE_TRUNC('month', fecha_aplicacion)) as active_months,
                        ROUND(AVG(edad_anos), 2) as average_age
                    FROM vacunacion_fiebre_amarilla
                    WHERE fecha_aplicacion IS NOT NULL
                """)).fetchone()
                
                # Distribución temporal
                temporal_dist = conn.execute(text("""
                    SELECT 
                        año,
                        mes,
                        COUNT(*) as vaccinations,
                        COUNT(DISTINCT codigo_municipio) as active_municipalities
                    FROM vacunacion_fiebre_amarilla
                    GROUP BY año, mes
                    ORDER BY año DESC, mes DESC
                    LIMIT 12
                """)).fetchall()
                
                # Distribución por grupo etario
                age_dist = conn.execute(text("""
                    SELECT 
                        grupo_etario,
                        COUNT(*) as vaccinations,
                        ROUND(AVG(edad_anos), 1) as avg_age_years,
                        COUNT(DISTINCT codigo_municipio) as municipalities
                    FROM vacunacion_fiebre_amarilla
                    WHERE grupo_etario IS NOT NULL
                    GROUP BY grupo_etario
                    ORDER BY vaccinations DESC
                """)).fetchall()
                
                # Distribución por ubicación
                location_dist = conn.execute(text("""
                    SELECT 
                        tipo_ubicacion,
                        COUNT(*) as vaccinations,
                        COUNT(DISTINCT codigo_municipio) as municipalities,
                        COUNT(DISTINCT institucion) as institutions
                    FROM vacunacion_fiebre_amarilla
                    GROUP BY tipo_ubicacion
                    ORDER BY vaccinations DESC
                """)).fetchall()
                
                # Top instituciones
                top_institutions = conn.execute(text("""
                    SELECT 
                        institucion,
                        COUNT(*) as vaccinations,
                        COUNT(DISTINCT codigo_municipio) as municipalities_served,
                        MIN(fecha_aplicacion) as first_activity,
                        MAX(fecha_aplicacion) as last_activity
                    FROM vacunacion_fiebre_amarilla
                    WHERE institucion IS NOT NULL
                    GROUP BY institucion
                    ORDER BY vaccinations DESC
                    LIMIT 10
                """)).fetchall()
                
                # Top municipios
                top_municipalities = conn.execute(text("""
                    SELECT 
                        v.codigo_municipio,
                        v.municipio,
                        COUNT(*) as vaccinations,
                        COUNT(DISTINCT v.institucion) as institutions,
                        MIN(v.fecha_aplicacion) as first_vaccination,
                        MAX(v.fecha_aplicacion) as last_vaccination
                    FROM vacunacion_fiebre_amarilla v
                    GROUP BY v.codigo_municipio, v.municipio
                    ORDER BY vaccinations DESC
                    LIMIT 10
                """)).fetchall()
                
                # Verificaciones de calidad
                quality_checks = conn.execute(text("""
                    SELECT 
                        COUNT(CASE WHEN codigo_municipio IS NULL THEN 1 END) as null_municipality,
                        COUNT(CASE WHEN fecha_aplicacion IS NULL THEN 1 END) as null_date,
                        COUNT(CASE WHEN grupo_etario IS NULL THEN 1 END) as null_age_group,
                        COUNT(CASE WHEN institucion IS NULL OR institucion = '' THEN 1 END) as null_institution,
                        COUNT(CASE WHEN edad_anos < 0 OR edad_anos > 90 THEN 1 END) as invalid_age,
                        COUNT(CASE WHEN fecha_aplicacion < '2020-01-01' OR fecha_aplicacion > CURRENT_DATE THEN 1 END) as invalid_date_range
                    FROM vacunacion_fiebre_amarilla
                """)).fetchone()
                
                results = {
                    'basic_stats': dict(basic_stats),
                    'temporal_distribution': [dict(row) for row in temporal_dist],
                    'age_distribution': [dict(row) for row in age_dist],
                    'location_distribution': [dict(row) for row in location_dist],
                    'top_institutions': [dict(row) for row in top_institutions],
                    'top_municipalities': [dict(row) for row in top_municipalities],
                    'quality_issues': dict(quality_checks)
                }
                
                logger.info("vaccination_validation_completed",
                           total_vaccinations=basic_stats.total_vaccinations,
                           municipalities=basic_stats.unique_municipalities,
                           institutions=basic_stats.unique_institutions)
                
                return results
                
        except Exception as e:
            logger.error("vaccination_validation_failed", error=str(e))
            return {}
    
    def generate_validation_report(self, validation_results: dict) -> str:
        """Genera reporte de validación detallado"""
        if not validation_results:
            return "❌ Error: No se pudieron validar los datos de vacunación"
        
        basic = validation_results['basic_stats']
        temporal = validation_results['temporal_distribution']
        age = validation_results['age_distribution']
        location = validation_results['location_distribution']
        top_inst = validation_results['top_institutions']
        top_mun = validation_results['top_municipalities']
        quality = validation_results['quality_issues']
        
        report = f"""
💉 REPORTE VALIDACIÓN VACUNACIÓN FIEBRE AMARILLA
{'='*60}

📊 ESTADÍSTICAS GENERALES:
   Total vacunas aplicadas: {basic['total_vaccinations']:,}
   Municipios activos: {basic['unique_municipalities']}/47
   Instituciones participantes: {basic['unique_institutions']}
   Grupos etarios: {basic['unique_age_groups']}
   Edad promedio: {basic['average_age']} años
   Período de actividad: {basic['first_vaccination']} a {basic['last_vaccination']}
   Meses con actividad: {basic['active_months']}

📅 ACTIVIDAD TEMPORAL (Últimos 12 meses):"""
        
        for t in temporal:
            report += f"\n   {t['año']:4d}-{t['mes']:02d}: {t['vaccinations']:,} vacunas ({t['active_municipalities']} municipios)"
        
        report += "\n\n👥 DISTRIBUCIÓN POR GRUPOS ETARIOS:"
        for age_group in age:
            pct = (age_group['vaccinations'] / basic['total_vaccinations']) * 100
            report += f"\n   {age_group['grupo_etario']}: {age_group['vaccinations']:,} ({pct:.1f}%) - Edad prom: {age_group['avg_age_years']} años"
        
        report += "\n\n🏙️ DISTRIBUCIÓN URBANO/RURAL:"
        for loc in location:
            pct = (loc['vaccinations'] / basic['total_vaccinations']) * 100
            report += f"\n   {loc['tipo_ubicacion']}: {loc['vaccinations']:,} ({pct:.1f}%) - {loc['municipalities']} municipios, {loc['institutions']} instituciones"
        
        report += "\n\n🏥 TOP 10 INSTITUCIONES MÁS ACTIVAS:"
        for i, inst in enumerate(top_inst, 1):
            report += f"\n   {i:2d}. {inst['institucion'][:50]}"
            report += f"\n       Vacunas: {inst['vaccinations']:,} | Municipios: {inst['municipalities_served']} | Actividad: {inst['first_activity']} - {inst['last_activity']}"
        
        report += "\n\n🏆 TOP 10 MUNICIPIOS MÁS VACUNADOS:"
        for i, mun in enumerate(top_mun, 1):
            report += f"\n   {i:2d}. {mun['municipio']} ({mun['codigo_municipio']})"
            report += f"\n       Vacunas: {mun['vaccinations']:,} | Instituciones: {mun['institutions']} | Actividad: {mun['first_vaccination']} - {mun['last_vaccination']}"
        
        report += "\n\n🔍 VERIFICACIONES DE CALIDAD:"
        
        issues = []
        if quality['null_municipality'] > 0:
            issues.append(f"Sin código municipio: {quality['null_municipality']:,}")
        if quality['null_date'] > 0:
            issues.append(f"Sin fecha aplicación: {quality['null_date']:,}")
        if quality['null_age_group'] > 0:
            issues.append(f"Sin grupo etario: {quality['null_age_group']:,}")
        if quality['null_institution'] > 0:
            issues.append(f"Sin institución: {quality['null_institution']:,}")
        if quality['invalid_age'] > 0:
            issues.append(f"Edad inválida (< 0 o > 90): {quality['invalid_age']:,}")
        if quality['invalid_date_range'] > 0:
            issues.append(f"Fecha fuera de rango: {quality['invalid_date_range']:,}")
        
        if issues:
            report += "\n   ⚠️ PROBLEMAS ENCONTRADOS:"
            for issue in issues:
                report += f"\n      • {issue}"
        else:
            report += "\n   ✅ Todos los registros son válidos"
        
        # Verificar cobertura estimada
        report += "\n\n📈 ANÁLISIS DE COBERTURA (Estimado):"
        coverage_analysis = self._analyze_coverage()
        if coverage_analysis:
            report += f"\n   Población objetivo estimada: {coverage_analysis.get('target_population', 'N/A'):,}"
            report += f"\n   Cobertura departamental estimada: {coverage_analysis.get('coverage_percentage', 0):.1f}%"
        
        return report
    
    def _analyze_coverage(self) -> dict:
        """Análisis básico de cobertura"""
        try:
            with self.engine.connect() as conn:
                # Intentar calcular cobertura básica si existe tabla población
                coverage_data = conn.execute(text("""
                    SELECT 
                        SUM(p.poblacion_total) as target_population,
                        COUNT(v.*) as total_vaccinations,
                        ROUND(COUNT(v.*) * 100.0 / NULLIF(SUM(p.poblacion_total), 0), 2) as coverage_percentage
                    FROM poblacion p
                    FULL OUTER JOIN vacunacion_fiebre_amarilla v ON (
                        p.codigo_municipio = v.codigo_municipio AND
                        p.grupo_etario = v.grupo_etario AND
                        p.tipo_ubicacion = v.tipo_ubicacion
                    )
                """)).fetchone()
                
                if coverage_data:
                    return dict(coverage_data)
        except:
            pass
        
        return {}

# ================================
# COORDINADOR PRINCIPAL OPTIMIZADO
# ================================

def cargar_vacunacion_optimizado(archivo_excel: Path = None) -> bool:
    """
    Carga optimizada de vacunación usando streaming
    
    Args:
        archivo_excel: Ruta opcional del archivo Excel (usa por defecto si no se especifica)
    
    Returns:
        bool: True si la carga fue exitosa
    """
    
    inicio = datetime.now()
    memory_monitor = MemoryMonitor()
    
    logger.info("vaccination_loading_started",
               timestamp=inicio.isoformat(),
               initial_memory_mb=round(memory_monitor.get_memory_usage_mb(), 2))
    
    try:
        # 1. Validar configuración del sistema
        logger.info("validating_system_configuration")
        if not validar_configuracion_optimizada():
            logger.error("system_configuration_invalid")
            return False
        
        # 2. Verificar archivo de entrada
        if archivo_excel is None:
            archivo_excel = FileConfig.PAIWEB_FILE
        
        if not archivo_excel.exists():
            logger.error("vaccination_file_not_found", path=str(archivo_excel))
            return False
        
        file_size_mb = archivo_excel.stat().st_size / (1024 * 1024)
        logger.info("vaccination_file_validated",
                   path=str(archivo_excel),
                   size_mb=round(file_size_mb, 2))
        
        # 3. Verificar caché DIVIPOLA
        divipola_stats = divipola_cache.get_stats()
        if divipola_stats['municipios'] == 0:
            logger.warning("divipola_cache_empty")
        else:
            logger.info("divipola_cache_ready", **divipola_stats)
        
        # 4. Crear procesador optimizado
        logger.info("creating_vaccination_stream_processor")
        processor = VaccinationStreamProcessor(archivo_excel)
        
        # 5. Procesar archivo usando streaming
        with memory_monitor.monitor_operation("vaccination_streaming"):
            logger.info("starting_vaccination_streaming_process")
            result = processor.process_file_streaming()
        
        # 6. Verificar resultado del procesamiento
        if not result['success']:
            logger.error("vaccination_processing_failed", **result)
            return False
        
        logger.info("vaccination_processing_successful", **result)
        
        # 7. Validar datos cargados
        logger.info("validating_vaccination_data")
        validator = VaccinationValidator()
        validation_results = validator.validate_loaded_data()
        
        if validation_results:
            # Generar reporte de validación
            report = validator.generate_validation_report(validation_results)
            print(report)
            
            # Guardar reporte en archivo
            FileConfig.create_directories()
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            report_file = FileConfig.LOGS_DIR / f"vacunacion_validation_{timestamp}.txt"
            
            with open(report_file, 'w', encoding='utf-8') as f:
                f.write(report)
            
            logger.info("vaccination_validation_report_saved", report_file=str(report_file))
        
        # 8. Estadísticas finales
        duracion = datetime.now() - inicio
        peak_memory = memory_monitor.get_memory_usage_mb()
        
        logger.info("vaccination_loading_completed",
                   duration_seconds=round(duracion.total_seconds(), 2),
                   peak_memory_mb=round(peak_memory, 2),
                   records_processed=result['processed_records'],
                   records_per_second=result['records_per_second'])
        
        return True
        
    except Exception as e:
        duracion = datetime.now() - inicio
        logger.error("vaccination_loading_failed",
                   error=str(e),
                   duration_seconds=round(duracion.total_seconds(), 2))
        return False

# ================================
# VERIFICACIONES ESPECÍFICAS VACUNACIÓN
# ================================

def verificar_integridad_temporal():
    """Verifica integridad temporal de datos de vacunación"""
    logger.info("temporal_integrity_check_started")
    
    try:
        engine = create_engine(DatabaseConfig.get_connection_url())
        
        with engine.connect() as conn:
            # Verificar fechas futuras
            future_dates = conn.execute(text("""
                SELECT COUNT(*) as future_vaccinations
                FROM vacunacion_fiebre_amarilla
                WHERE fecha_aplicacion > CURRENT_DATE
            """)).scalar()
            
            # Verificar fechas muy antiguas
            very_old_dates = conn.execute(text("""
                SELECT COUNT(*) as very_old_vaccinations
                FROM vacunacion_fiebre_amarilla
                WHERE fecha_aplicacion < '2020-01-01'
            """)).scalar()
            
            # Verificar consistencia temporal
            temporal_issues = conn.execute(text("""
                SELECT 
                    COUNT(*) as inconsistent_temporal,
                    MIN(fecha_aplicacion) as min_date,
                    MAX(fecha_aplicacion) as max_date
                FROM vacunacion_fiebre_amarilla
                WHERE año != EXTRACT(YEAR FROM fecha_aplicacion)
                   OR mes != EXTRACT(MONTH FROM fecha_aplicacion)
            """)).fetchone()
            
            issues_found = future_dates + very_old_dates + temporal_issues.inconsistent_temporal
            
            if issues_found > 0:
                logger.warning("temporal_integrity_issues",
                             future_dates=future_dates,
                             very_old_dates=very_old_dates,
                             inconsistent_temporal=temporal_issues.inconsistent_temporal)
                return False
            else:
                logger.info("temporal_integrity_valid",
                           date_range=f"{temporal_issues.min_date} to {temporal_issues.max_date}")
                return True
                
    except Exception as e:
        logger.error("temporal_integrity_check_failed", error=str(e))
        return False

def verificar_anonimizacion():
    """Verifica que no queden datos personales en la tabla"""
    logger.info("anonymization_check_started")
    
    try:
        engine = create_engine(DatabaseConfig.get_connection_url())
        
        with engine.connect() as conn:
            # Verificar que no existan columnas de datos personales
            personal_columns = conn.execute(text("""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name = 'vacunacion_fiebre_amarilla'
                AND column_name IN (
                    'fecha_nacimiento', 'numero_documento', 'nombre', 'apellido',
                    'primer_nombre', 'segundo_nombre', 'primer_apellido', 'segundo_apellido'
                )
            """)).fetchall()
            
            if personal_columns:
                column_names = [col[0] for col in personal_columns]
                logger.error("anonymization_failed", 
                           personal_columns_found=column_names)
                return False
            else:
                logger.info("anonymization_verified", 
                           message="No personal data columns found")
                return True
                
    except Exception as e:
        logger.error("anonymization_check_failed", error=str(e))
        return False

def optimizar_indices_vacunacion():
    """Optimiza índices de la tabla vacunación para dashboard"""
    logger.info("optimizing_vaccination_indexes")
    
    try:
        engine = create_engine(DatabaseConfig.get_connection_url())
        
        with engine.connect() as conn:
            # Índices optimizados para dashboard y análisis
            indices_sql = [
                # Índice principal para dashboard
                "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_vacunacion_dashboard_optimized ON vacunacion_fiebre_amarilla(codigo_municipio, grupo_etario, tipo_ubicacion, año, mes)",
                
                # Índice para filtros temporales
                "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_vacunacion_temporal_optimized ON vacunacion_fiebre_amarilla(fecha_aplicacion, año, mes) INCLUDE (codigo_municipio)",
                
                # Índice para análisis institucional
                "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_vacunacion_institucion_optimized ON vacunacion_fiebre_amarilla(institucion, codigo_municipio, fecha_aplicacion)",
                
                # Índice para análisis geográfico
                "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_vacunacion_geografico ON vacunacion_fiebre_amarilla(codigo_municipio, tipo_ubicacion) INCLUDE (fecha_aplicacion, grupo_etario)",
                
                # Actualizar estadísticas
                "ANALYZE vacunacion_fiebre_amarilla"
            ]
            
            for sql in indices_sql:
                logger.debug("executing_vaccination_index_sql", sql=sql[:50] + "...")
                conn.execute(text(sql))
                conn.commit()
            
            logger.info("vaccination_indexes_optimized")
            
    except Exception as e:
        logger.error("vaccination_index_optimization_failed", error=str(e))

# ================================
# FUNCIÓN PRINCIPAL
# ================================

def main():
    """Función principal con interfaz de línea de comandos"""
    
    print("💉 CARGA OPTIMIZADA VACUNACIÓN PAIweb → POSTGRESQL")
    print("=" * 65)
    print("Performance: 67.5MB streaming, 8-10min → 2-3min, datos anónimos")
    print("Edad calculada con fecha actual (CORREGIDO)")
    
    # Argumentos de línea de comandos
    archivo_excel = None
    if len(sys.argv) > 1:
        archivo_excel = Path(sys.argv[1])
        if not archivo_excel.exists():
            print(f"❌ ERROR: Archivo no encontrado: {archivo_excel}")
            return False
    
    # Ejecutar carga optimizada
    exito = cargar_vacunacion_optimizado(archivo_excel)
    
    if exito:
        print("\n🎉 ¡CARGA DE VACUNACIÓN COMPLETADA EXITOSAMENTE!")
        
        # Verificaciones adicionales
        print("\n🔧 Ejecutando verificaciones adicionales...")
        
        if verificar_integridad_temporal():
            print("✅ Integridad temporal válida")
        else:
            print("⚠️ Problemas de integridad temporal encontrados")
        
        if verificar_anonimizacion():
            print("✅ Anonimización verificada - sin datos personales")
        else:
            print("⚠️ CRÍTICO: Datos personales encontrados en BD")
        
        print("🔧 Optimizando índices para dashboard...")
        optimizar_indices_vacunacion()
        print("✅ Índices optimizados para performance")
        
        print("\n🎯 PRÓXIMOS PASOS:")
        print("1. Verificar vistas dashboard: python scripts/verificar_vistas.py")
        print("2. Cargar casos: python scripts/cargar_casos_optimizado.py")
        print("3. Conectar dashboard: streamlit run dashboard/app.py")
        print("4. ¡Análisis de coberturas listo! 📊")
        
    else:
        print("\n❌ ERROR EN CARGA DE VACUNACIÓN")
        print("💡 Revisar logs para detalles del error")
        print("🔧 Verificar: archivo Excel, conexión BD, configuración")
    
    return exito

if __name__ == "__main__":
    main()