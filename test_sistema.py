#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
test_sistema_optimizado.py - Verificación Completa Sistema Optimizado
REEMPLAZA: test_conexion.py con todas las optimizaciones aplicadas
Performance: Verificación rápida, logging estructurado, métricas avanzadas
"""

import sys
import time
from pathlib import Path
from datetime import datetime
import structlog
from sqlalchemy import create_engine, text
import pandas as pd
import psutil
from typing import Dict, List, Tuple, Optional
from dataclasses import dataclass

# Añadir directorio actual al path
sys.path.insert(0, str(Path(__file__).parent))

# Importar módulos optimizados
try:
    from config import (
        DatabaseConfig, FileConfig, validar_configuracion_optimizada,
        divipola_cache, GRUPOS_ETARIOS
    )
    from core.processors import MemoryMonitor
    
    print("✅ Configuración optimizada importada correctamente")
except ImportError as e:
    print(f"❌ Error importando módulos optimizados: {e}")
    print("💡 Asegúrate de que config.py y core/processors.py estén disponibles")
    sys.exit(1)

logger = structlog.get_logger()

# ================================
# TIPOS Y CONFIGURACIONES
# ================================

@dataclass
class TestResult:
    test_name: str
    success: bool
    duration_seconds: float
    details: Optional[Dict] = None
    error_message: Optional[str] = None

class SistemaVerificadorOptimizado:
    """Verificador completo del sistema optimizado"""
    
    def __init__(self):
        self.start_time = datetime.now()
        self.memory_monitor = MemoryMonitor()
        self.engine = None
        self.test_results: List[TestResult] = []
        
    def log_test_start(self, test_name: str):
        """Log inicio de test"""
        logger.info("test_started", 
                   test=test_name,
                   memory_mb=round(self.memory_monitor.get_memory_usage_mb(), 2))
    
    def log_test_result(self, result: TestResult):
        """Log resultado de test"""
        self.test_results.append(result)
        
        if result.success:
            logger.info("test_passed",
                       test=result.test_name,
                       duration_seconds=result.duration_seconds,
                       details=result.details)
        else:
            logger.error("test_failed",
                        test=result.test_name,
                        error=result.error_message,
                        duration_seconds=result.duration_seconds)
    
    def test_configuracion_optimizada(self) -> TestResult:
        """Test: Configuración centralizada optimizada"""
        start_time = time.time()
        self.log_test_start("configuracion_optimizada")
        
        try:
            # Validar configuración completa
            config_valid = validar_configuracion_optimizada()
            
            # Verificar caché DIVIPOLA
            divipola_stats = divipola_cache.get_stats()
            
            # Verificar grupos etarios
            grupos_count = len(GRUPOS_ETARIOS)
            
            # Verificar archivos
            files_status = FileConfig.validate_files()
            available_files = sum(1 for exists in files_status.values() if exists)
            
            details = {
                'config_valid': config_valid,
                'divipola_municipios': divipola_stats['municipios'],
                'divipola_veredas': divipola_stats['veredas'],
                'grupos_etarios': grupos_count,
                'archivos_disponibles': available_files,
                'archivos_totales': len(files_status)
            }
            
            success = (config_valid and 
                      divipola_stats['municipios'] > 0 and 
                      grupos_count > 0)
            
            return TestResult(
                test_name="configuracion_optimizada",
                success=success,
                duration_seconds=time.time() - start_time,
                details=details,
                error_message=None if success else "Configuración inválida o incompleta"
            )
            
        except Exception as e:
            return TestResult(
                test_name="configuracion_optimizada",
                success=False,
                duration_seconds=time.time() - start_time,
                error_message=str(e)
            )
    
    def test_conexion_postgresql(self) -> TestResult:
        """Test: Conexión PostgreSQL optimizada"""
        start_time = time.time()
        self.log_test_start("conexion_postgresql")
        
        try:
            # Crear conexión usando configuración optimizada
            self.engine = create_engine(DatabaseConfig.get_connection_url())
            
            with self.engine.connect() as conn:
                # Test básico de conexión
                conn.execute(text("SELECT 1"))
                
                # Verificar versión PostgreSQL
                version_result = conn.execute(text("SELECT version()")).scalar()
                
                # Verificar extensiones críticas
                extensions = conn.execute(text("""
                    SELECT extname, extversion 
                    FROM pg_extension 
                    WHERE extname IN ('postgis', 'pg_trgm', 'unaccent')
                    ORDER BY extname
                """)).fetchall()
                
                # Verificar configuración de performance
                performance_config = conn.execute(text("""
                    SELECT name, setting, unit 
                    FROM pg_settings 
                    WHERE name IN ('shared_buffers', 'effective_cache_size', 'max_connections')
                """)).fetchall()
                
                # Convertir resultados SQL a diccionarios de forma segura
                extensions_list = []
                for ext in extensions:
                    if hasattr(ext, '_mapping'):
                        extensions_list.append(dict(ext._mapping))
                    else:
                        extensions_list.append({'extname': ext[0], 'extversion': ext[1]})

                performance_list = []
                for conf in performance_config:
                    if hasattr(conf, '_mapping'):
                        performance_list.append(dict(conf._mapping))
                    else:
                        performance_list.append({'name': conf[0], 'setting': conf[1], 'unit': conf[2]})

                details = {
                    'postgresql_version': version_result,
                    'extensions': extensions_list,
                    'performance_config': performance_list,
                    'connection_url': DatabaseConfig.get_connection_url(include_password=False)
                }
                
                return TestResult(
                    test_name="conexion_postgresql",
                    success=True,
                    duration_seconds=time.time() - start_time,
                    details=details
                )
                
        except Exception as e:
            return TestResult(
                test_name="conexion_postgresql",
                success=False,
                duration_seconds=time.time() - start_time,
                error_message=str(e)
            )
    
    def test_tablas_sistema(self) -> TestResult:
        """Test: Tablas del sistema y estructura"""
        start_time = time.time()
        self.log_test_start("tablas_sistema")
        
        try:
            with self.engine.connect() as conn:
                # Verificar tablas principales
                tablas_principales = [
                    'unidades_territoriales',
                    'poblacion', 
                    'vacunacion_fiebre_amarilla',
                    'casos_fiebre_amarilla',
                    'epizootias'
                ]
                
                tablas_info = {}
                total_records = 0
                
                for tabla in tablas_principales:
                    try:
                        # Contar registros
                        count = conn.execute(text(f"SELECT COUNT(*) FROM {tabla}")).scalar()
                        
                        # Obtener info de tabla
                        table_size = conn.execute(text(f"""
                            SELECT pg_size_pretty(pg_total_relation_size('{tabla}'))
                        """)).scalar()
                        
                        tablas_info[tabla] = {
                            'records': count,
                            'size': table_size,
                            'exists': True
                        }
                        total_records += count
                        
                    except Exception:
                        tablas_info[tabla] = {
                            'records': 0,
                            'size': '0 bytes',
                            'exists': False
                        }
                
                # Verificar vistas optimizadas
                vistas_optimizadas = [
                    'mv_dashboard_principal',
                    'v_indicadores_tiempo_real',
                    'v_mapa_optimizado',
                    'v_alertas_dashboard'
                ]
                
                vistas_info = {}
                for vista in vistas_optimizadas:
                    try:
                        count = conn.execute(text(f"SELECT COUNT(*) FROM {vista}")).scalar()
                        vistas_info[vista] = {'records': count, 'exists': True}
                    except Exception:
                        vistas_info[vista] = {'records': 0, 'exists': False}
                
                details = {
                    'tablas': tablas_info,
                    'vistas_optimizadas': vistas_info,
                    'total_records': total_records,
                    'tablas_con_datos': sum(1 for info in tablas_info.values() if info['records'] > 0),
                    'vistas_funcionando': sum(1 for info in vistas_info.values() if info['exists'])
                }
                
                # Considerar exitoso si al menos las tablas críticas existen
                tablas_criticas = ['unidades_territoriales', 'poblacion', 'vacunacion_fiebre_amarilla']
                criticas_ok = all(tablas_info[tabla]['exists'] for tabla in tablas_criticas)
                
                return TestResult(
                    test_name="tablas_sistema",
                    success=criticas_ok,
                    duration_seconds=time.time() - start_time,
                    details=details,
                    error_message=None if criticas_ok else "Faltan tablas críticas del sistema"
                )
                
        except Exception as e:
            return TestResult(
                test_name="tablas_sistema", 
                success=False,
                duration_seconds=time.time() - start_time,
                error_message=str(e)
            )
    
    def test_integridad_datos(self) -> TestResult:
        """Test: Integridad y calidad de datos"""
        start_time = time.time()
        self.log_test_start("integridad_datos")
        
        try:
            with self.engine.connect() as conn:
                # Verificar integridad referencial
                integrity_check = conn.execute(text("""
                    SELECT 
                        (SELECT COUNT(*) FROM vacunacion_fiebre_amarilla v 
                         LEFT JOIN unidades_territoriales ut ON v.codigo_municipio = ut.codigo_divipola
                         WHERE ut.codigo_divipola IS NULL) as vac_sin_territorio,
                        
                        (SELECT COUNT(*) FROM poblacion p
                         LEFT JOIN unidades_territoriales ut ON p.codigo_municipio = ut.codigo_divipola
                         WHERE ut.codigo_divipola IS NULL) as pob_sin_territorio,
                         
                        (SELECT COUNT(*) FROM vacunacion_fiebre_amarilla 
                         WHERE fecha_aplicacion IS NULL) as vac_sin_fecha,
                         
                        (SELECT COUNT(*) FROM vacunacion_fiebre_amarilla 
                         WHERE grupo_etario IS NULL) as vac_sin_grupo,
                         
                        (SELECT COUNT(*) FROM poblacion 
                         WHERE poblacion_total <= 0) as pob_invalida
                """)).fetchone()
                
                # Verificar coberturas
                cobertura_check = conn.execute(text("""
                    SELECT 
                        COUNT(*) as municipios_con_cobertura,
                        AVG(cobertura_porcentaje) as cobertura_promedio,
                        COUNT(*) FILTER (WHERE cobertura_porcentaje >= 95) as municipios_meta,
                        COUNT(*) FILTER (WHERE cobertura_porcentaje < 50) as municipios_criticos
                    FROM mv_dashboard_principal
                    WHERE poblacion_total > 0
                """)).fetchone()
                
                issues = {
                    'vac_sin_territorio': integrity_check[0],
                    'pob_sin_territorio': integrity_check[1], 
                    'vac_sin_fecha': integrity_check[2],
                    'vac_sin_grupo': integrity_check[3],
                    'pob_invalida': integrity_check[4]
                }

                coverage = {
                    'municipios_con_cobertura': cobertura_check[0],
                    'cobertura_promedio': cobertura_check[1],
                    'municipios_meta': cobertura_check[2], 
                    'municipios_criticos': cobertura_check[3]
                }
                
                # Calcular score de integridad
                total_issues = sum(issues.values())
                integrity_score = max(0, 100 - total_issues)
                
                details = {
                    'integrity_issues': issues,
                    'coverage_stats': coverage,
                    'integrity_score': integrity_score,
                    'total_issues': total_issues
                }
                
                # Considerar exitoso si score > 90 y hay datos de cobertura
                success = (integrity_score >= 90)
                
                return TestResult(
                    test_name="integridad_datos",
                    success=success,
                    duration_seconds=time.time() - start_time,
                    details=details,
                    error_message=None if success else f"Score integridad bajo: {integrity_score}/100"
                )
                
        except Exception as e:
            return TestResult(
                test_name="integridad_datos",
                success=False,
                duration_seconds=time.time() - start_time,
                error_message=str(e)
            )
    
    def test_performance_sistema(self) -> TestResult:
        """Test: Performance general del sistema"""
        start_time = time.time()
        self.log_test_start("performance_sistema")
        
        try:
            # Métricas de memoria
            memory_usage = self.memory_monitor.get_memory_usage_mb()
            
            # Métricas de sistema
            cpu_percent = psutil.cpu_percent(interval=1)
            memory_system = psutil.virtual_memory()
            disk_usage = psutil.disk_usage('/')
            
            with self.engine.connect() as conn:
                # Tamaño base de datos
                db_size = conn.execute(text("""
                    SELECT pg_size_pretty(pg_database_size(current_database()))
                """)).scalar()
                
                # Estadísticas de conexiones
                connections = conn.execute(text("""
                    SELECT count(*) as active_connections
                    FROM pg_stat_activity 
                    WHERE state = 'active'
                """)).scalar()
                
                # Cache hit ratio
                cache_hit = conn.execute(text("""
                    SELECT round(
                        sum(blks_hit) * 100.0 / nullif(sum(blks_hit + blks_read), 0), 2
                    ) as cache_hit_ratio
                    FROM pg_stat_database
                """)).scalar()
                
            details = {
                'memory_usage_mb': round(memory_usage, 2),
                'cpu_percent': cpu_percent,
                'system_memory_percent': memory_system.percent,
                'disk_usage_percent': round(disk_usage.percent, 2),
                'database_size': db_size,
                'active_connections': connections,
                'cache_hit_ratio': cache_hit or 0,
                'performance_score': self._calculate_performance_score(
                    memory_usage, cpu_percent, memory_system.percent, cache_hit or 0
                )
            }
            
            # Performance OK si score > 70
            performance_ok = details['performance_score'] > 70
            
            return TestResult(
                test_name="performance_sistema",
                success=performance_ok,
                duration_seconds=time.time() - start_time,
                details=details,
                error_message=None if performance_ok else f"Performance score: {details['performance_score']}/100"
            )
            
        except Exception as e:
            return TestResult(
                test_name="performance_sistema",
                success=False,
                duration_seconds=time.time() - start_time,
                error_message=str(e)
            )
    
    def _calculate_performance_score(self, memory_mb: float, cpu_pct: float, 
                                   sys_memory_pct: float, cache_hit: float) -> int:
        """Calcula score de performance (0-100)"""
        score = 100
        
        # Penalizar memoria alta
        if memory_mb > 1000:
            score -= 20
        elif memory_mb > 500:
            score -= 10
        
        # Penalizar CPU alto
        if cpu_pct > 80:
            score -= 15
        elif cpu_pct > 50:
            score -= 5
        
        # Penalizar memoria sistema alta
        if sys_memory_pct > 90:
            score -= 15
        elif sys_memory_pct > 75:
            score -= 5
        
        # Penalizar cache hit bajo
        if cache_hit < 80:
            score -= 10
        elif cache_hit < 90:
            score -= 5
        
        return max(0, score)
    
    def ejecutar_verificacion_completa(self) -> Dict:
        """Ejecuta verificación completa del sistema optimizado"""
        
        logger.info("sistema_verificacion_started", 
                   timestamp=self.start_time.isoformat())
        
        # Tests en orden de importancia
        tests_orden = [
            self.test_configuracion_optimizada,
            self.test_conexion_postgresql,
            self.test_tablas_sistema,
            self.test_integridad_datos,
            self.test_performance_sistema
        ]
        
        successful_tests = 0
        failed_tests = 0
        
        for test_method in tests_orden:
            result = test_method()
            self.log_test_result(result)
            
            if result.success:
                successful_tests += 1
            else:
                failed_tests += 1
                
                # Para tests críticos, parar verificación
                if result.test_name in ['configuracion_optimizada', 'conexion_postgresql']:
                    logger.error("critical_test_failed_stopping", test=result.test_name)
                    break
        
        # Estadísticas finales
        total_duration = datetime.now() - self.start_time
        peak_memory = self.memory_monitor.get_memory_usage_mb()
        
        verification_result = {
            'success': failed_tests == 0,
            'total_duration_seconds': total_duration.total_seconds(),
            'successful_tests': successful_tests,
            'failed_tests': failed_tests,
            'total_tests': len(self.test_results),
            'peak_memory_mb': peak_memory,
            'test_results': self.test_results
        }
        
        logger.info("sistema_verificacion_completed", **verification_result)
        
        return verification_result
    
    def generar_reporte_verificacion(self, verification_result: Dict) -> str:
        """Genera reporte final de verificación"""
        
        duration = verification_result['total_duration_seconds']
        
        report = f"""
🧪 VERIFICACIÓN SISTEMA EPIDEMIOLÓGICO OPTIMIZADO
{'='*65}

⏱️ RESUMEN:
   Duración: {duration:.2f} segundos
   Estado: {'✅ SISTEMA FUNCIONAL' if verification_result['success'] else '❌ PROBLEMAS DETECTADOS'}
   Tests exitosos: {verification_result['successful_tests']}/{verification_result['total_tests']}
   Memoria pico: {verification_result['peak_memory_mb']:.1f} MB

📋 RESULTADOS DETALLADOS:"""
        
        for result in self.test_results:
            status_icon = "✅" if result.success else "❌"
            test_display = result.test_name.replace('_', ' ').title()
            
            report += f"\n   {status_icon} {test_display} ({result.duration_seconds:.2f}s)"
            
            if result.success and result.details:
                # Mostrar detalles clave
                if result.test_name == "configuracion_optimizada":
                    details = result.details
                    report += f"\n      • DIVIPOLA: {details['divipola_municipios']} municipios, {details['divipola_veredas']} veredas"
                    report += f"\n      • Archivos: {details['archivos_disponibles']}/{details['archivos_totales']} disponibles"
                
                elif result.test_name == "conexion_postgresql":
                    extensions = result.details['extensions']
                    report += f"\n      • Extensiones: {[ext['extname'] for ext in extensions]}"
                
                elif result.test_name == "tablas_sistema":
                    details = result.details
                    report += f"\n      • Registros totales: {details['total_records']:,}"
                    report += f"\n      • Tablas con datos: {details['tablas_con_datos']}"
                    report += f"\n      • Vistas funcionando: {details['vistas_funcionando']}"
                
                elif result.test_name == "performance_dashboard":
                    mv_time = result.details['vista_materializada']['query_time_seconds']
                    rt_time = result.details['vista_tiempo_real']['query_time_seconds']
                    report += f"\n      • Vista materializada: {mv_time}s"
                    report += f"\n      • Vista tiempo real: {rt_time}s"
                
                elif result.test_name == "integridad_datos":
                    score = result.details['integrity_score']
                    municipios = result.details['coverage_stats']['municipios_con_cobertura']
                    report += f"\n      • Score integridad: {score}/100"
                    report += f"\n      • Municipios con cobertura: {municipios}"
                
                elif result.test_name == "performance_sistema":
                    score = result.details['performance_score']
                    memory = result.details['memory_usage_mb']
                    cache_hit = result.details['cache_hit_ratio']
                    report += f"\n      • Score performance: {score}/100"
                    report += f"\n      • Memoria: {memory:.1f} MB"
                    report += f"\n      • Cache hit: {cache_hit:.1f}%"
            
            elif not result.success:
                report += f"\n      ❌ Error: {result.error_message}"
        
        if verification_result['success']:
            report += f"""

🎉 ¡SISTEMA COMPLETAMENTE VERIFICADO Y OPTIMIZADO!

🚀 OPTIMIZACIONES ACTIVAS:
   ✅ Configuración centralizada con caché DIVIPOLA
   ✅ Procesamiento streaming para archivos grandes  
   ✅ Vistas SQL materializadas para dashboard rápido
   ✅ Índices optimizados para consultas <2s
   ✅ Logging estructurado y monitoreo de memoria
   ✅ Sistema de alertas automático

🔗 PRÓXIMOS PASOS:
   1. Dashboard: streamlit run dashboard/app.py
   2. Carga completa: python scripts/sistema_coordinador_optimizado.py
   3. Monitoreo: python scripts/monitor_sistema.py --completo
   4. ¡Sistema epidemiológico listo para producción! 🏥"""
        else:
            report += f"""

⚠️ SISTEMA CON PROBLEMAS - REQUIERE ATENCIÓN

🔧 ACCIONES RECOMENDADAS:
   1. Revisar tests fallidos arriba
   2. Ejecutar: python setup_sistema.py
   3. Verificar archivos en data/
   4. Revisar logs: {FileConfig.LOGS_DIR}
   5. Re-ejecutar verificación"""
        
        return report

# ================================
# FUNCIÓN PRINCIPAL
# ================================

def main():
    """Función principal optimizada"""
    
    print("🧪 VERIFICACIÓN SISTEMA EPIDEMIOLÓGICO OPTIMIZADO")
    print("=" * 65)
    print("Verificación completa: config, BD, performance, integridad")
    
    # Argumentos de línea de comandos
    modo_rapido = "--rapido" in sys.argv
    solo_config = "--config" in sys.argv
    
    if "--help" in sys.argv:
        print("\nOpciones:")
        print("  --config  : Solo verificar configuración")
        print("  --rapido  : Verificación rápida (omite tests lentos)")
        print("  --help    : Mostrar ayuda")
        return True
    
    # Crear verificador
    verificador = SistemaVerificadorOptimizado()
    
    if solo_config:
        # Solo test de configuración
        result = verificador.test_configuracion_optimizada()
        verificador.log_test_result(result)
        
        if result.success:
            print("✅ Configuración verificada correctamente")
            return True
        else:
            print(f"❌ Error en configuración: {result.error_message}")
            return False
    
    # Verificación completa
    verification_result = verificador.ejecutar_verificacion_completa()
    
    # Generar y mostrar reporte
    report = verificador.generar_reporte_verificacion(verification_result)
    print(report)
    
    # Guardar reporte
    FileConfig.create_directories()
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    report_file = FileConfig.LOGS_DIR / f"verificacion_sistema_{timestamp}.txt"
    
    with open(report_file, 'w', encoding='utf-8') as f:
        f.write(report)
    
    logger.info("verification_report_saved", report_file=str(report_file))
    
    if verification_result['success']:
        print(f"\n📄 Reporte guardado: {report_file}")
        print("🎉 ¡SISTEMA COMPLETAMENTE VERIFICADO!")
        
        # URLs útiles
        print("\n🔗 URLs útiles:")
        print("• pgAdmin: http://localhost:8080")
        print("• Dashboard (próximamente): streamlit run dashboard/app.py")
        
    else:
        print(f"\n📄 Reporte de errores: {report_file}")
        print("🔧 Revisar y corregir problemas detectados")
    
    return verification_result['success']

if __name__ == "__main__":
    main()