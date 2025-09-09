#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
monitor_sistema.py - Monitor Sistema Epidemiológico Optimizado
LIMPIO: Logging estructurado, métricas avanzadas, reportes HTML optimizados
"""

import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
from datetime import datetime, timedelta
import structlog
import sys
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import psutil
import time

# Añadir path para imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from config import DatabaseConfig, FileConfig

logger = structlog.get_logger()

class MonitorSistemaOptimizado:
    """Monitor avanzado del Sistema Epidemiológico Tolima"""
    
    def __init__(self):
        self.inicio = datetime.now()
        self.engine = None
        self.metricas_sistema = {}
        self.alertas_detectadas = []
        
    def conectar_bd(self) -> bool:
        """Establece conexión optimizada con PostgreSQL"""
        logger.info("connecting_to_database")
        
        try:
            self.engine = create_engine(DatabaseConfig.get_connection_url())
            
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
                
                # Verificar versión PostgreSQL
                version = conn.execute(text("SELECT version()")).scalar()
                logger.info("database_connected", postgresql_version=version.split()[1])
            
            return True
            
        except Exception as e:
            logger.error("database_connection_failed", error=str(e))
            return False
    
    def obtener_indicadores_tiempo_real(self) -> Dict:
        """Obtiene indicadores en tiempo real desde vista optimizada"""
        logger.info("fetching_real_time_indicators")
        
        try:
            with self.engine.connect() as conn:
                # Usar vista optimizada
                indicadores = pd.read_sql(text("""
                    SELECT * FROM v_indicadores_tiempo_real
                """), conn).iloc[0].to_dict()
                
                logger.info("real_time_indicators_fetched", 
                           total_vacunados=indicadores.get('total_vacunados', 0),
                           municipios_activos=indicadores.get('municipios_activos', 0),
                           cobertura_general=indicadores.get('cobertura_general', 0))
                
                return indicadores
                
        except Exception as e:
            logger.error("real_time_indicators_failed", error=str(e))
            return {}
    
    def analizar_calidad_datos(self) -> Dict:
        """Análisis avanzado de calidad de datos"""
        logger.info("analyzing_data_quality")
        
        try:
            with self.engine.connect() as conn:
                # Análisis calidad vacunación
                calidad_vacunacion = conn.execute(text("""
                    SELECT 
                        COUNT(*) as total_registros,
                        COUNT(CASE WHEN codigo_municipio IS NULL THEN 1 END) as sin_municipio,
                        COUNT(CASE WHEN fecha_aplicacion IS NULL THEN 1 END) as sin_fecha,
                        COUNT(CASE WHEN grupo_etario IS NULL THEN 1 END) as sin_grupo_etario,
                        COUNT(CASE WHEN institucion IS NULL OR institucion = '' THEN 1 END) as sin_institucion,
                        COUNT(CASE WHEN edad_anos < 0 OR edad_anos > 90 THEN 1 END) as edad_invalida,
                        COUNT(DISTINCT codigo_municipio) as municipios_unicos,
                        ROUND(AVG(edad_anos), 1) as edad_promedio
                    FROM vacunacion_fiebre_amarilla
                """)).fetchone()
                
                # Análisis calidad población
                calidad_poblacion = conn.execute(text("""
                    SELECT 
                        COUNT(*) as total_registros,
                        COUNT(CASE WHEN poblacion_total <= 0 THEN 1 END) as poblacion_invalida,
                        COUNT(CASE WHEN codigo_municipio IS NULL THEN 1 END) as sin_codigo,
                        SUM(poblacion_total) as poblacion_total_tolima,
                        ROUND(AVG(poblacion_total), 0) as poblacion_promedio
                    FROM poblacion
                """)).fetchone()
                
                # Integridad referencial
                integridad = conn.execute(text("""
                    SELECT 
                        (SELECT COUNT(*) FROM vacunacion_fiebre_amarilla v 
                         LEFT JOIN unidades_territoriales ut ON v.codigo_municipio = ut.codigo_divipola
                         WHERE ut.codigo_divipola IS NULL) as vacunacion_sin_territorio,
                        (SELECT COUNT(*) FROM poblacion p
                         LEFT JOIN unidades_territoriales ut ON p.codigo_municipio = ut.codigo_divipola  
                         WHERE ut.codigo_divipola IS NULL) as poblacion_sin_territorio
                """)).fetchone()
                
                # Calcular score de calidad
                problemas_totales = (
                    dict(calidad_vacunacion)['sin_municipio'] +
                    dict(calidad_vacunacion)['sin_fecha'] + 
                    dict(calidad_poblacion)['poblacion_invalida'] +
                    dict(integridad)['vacunacion_sin_territorio']
                )
                
                score_calidad = max(0, 100 - problemas_totales)
                
                analisis = {
                    'vacunacion': dict(calidad_vacunacion),
                    'poblacion': dict(calidad_poblacion),
                    'integridad': dict(integridad),
                    'score_calidad': score_calidad,
                    'problemas_totales': problemas_totales
                }
                
                logger.info("data_quality_analyzed", score=score_calidad, issues=problemas_totales)
                
                return analisis
                
        except Exception as e:
            logger.error("data_quality_analysis_failed", error=str(e))
            return {}
    
    def detectar_alertas_automaticas(self) -> List[Dict]:
        """Detecta alertas automáticas usando vista optimizada"""
        logger.info("detecting_automatic_alerts")
        
        try:
            with self.engine.connect() as conn:
                # Usar vista de alertas optimizada
                alertas = pd.read_sql(text("""
                    SELECT * FROM v_alertas_dashboard
                    ORDER BY 
                        CASE severidad 
                            WHEN 'ALTA' THEN 1 
                            WHEN 'MEDIA' THEN 2 
                            WHEN 'BAJA' THEN 3 
                        END,
                        valor_metrica DESC
                    LIMIT 20
                """), conn)
                
                alertas_list = alertas.to_dict('records')
                
                # Contar por severidad
                severidad_count = alertas['severidad'].value_counts().to_dict()
                
                logger.info("alerts_detected", 
                           total_alerts=len(alertas_list),
                           by_severity=severidad_count)
                
                self.alertas_detectadas = alertas_list
                return alertas_list
                
        except Exception as e:
            logger.error("alert_detection_failed", error=str(e))
            return []
    
    def analizar_performance_dashboard(self) -> Dict:
        """Analiza performance de vistas dashboard"""
        logger.info("analyzing_dashboard_performance")
        
        try:
            performance_tests = {}
            
            with self.engine.connect() as conn:
                # Test vista materializada principal
                start_time = time.time()
                mv_count = conn.execute(text("SELECT COUNT(*) FROM mv_dashboard_principal")).scalar()
                mv_duration = time.time() - start_time
                
                # Test vista tiempo real
                start_time = time.time()
                conn.execute(text("SELECT * FROM v_indicadores_tiempo_real")).fetchone()
                rt_duration = time.time() - start_time
                
                # Test vista mapa
                start_time = time.time()
                map_count = conn.execute(text("SELECT COUNT(*) FROM v_mapa_municipios")).scalar()
                map_duration = time.time() - start_time
                
                # Test vista alertas
                start_time = time.time()
                alert_count = conn.execute(text("SELECT COUNT(*) FROM v_alertas_dashboard")).scalar()
                alert_duration = time.time() - start_time
                
                performance_tests = {
                    'vista_materializada': {
                        'records': mv_count,
                        'duration_seconds': round(mv_duration, 3),
                        'performance_ok': mv_duration < 2.0
                    },
                    'vista_tiempo_real': {
                        'duration_seconds': round(rt_duration, 3),
                        'performance_ok': rt_duration < 1.0
                    },
                    'vista_mapa': {
                        'records': map_count,
                        'duration_seconds': round(map_duration, 3),
                        'performance_ok': map_duration < 2.0
                    },
                    'vista_alertas': {
                        'records': alert_count,
                        'duration_seconds': round(alert_duration, 3),
                        'performance_ok': alert_duration < 1.0
                    }
                }
                
                # Performance general
                max_duration = max(mv_duration, rt_duration, map_duration, alert_duration)
                performance_general = max_duration < 2.0
                
                logger.info("dashboard_performance_analyzed",
                           max_duration_seconds=round(max_duration, 3),
                           performance_ok=performance_general)
                
                performance_tests['performance_general'] = performance_general
                performance_tests['max_duration'] = round(max_duration, 3)
                
                return performance_tests
                
        except Exception as e:
            logger.error("dashboard_performance_analysis_failed", error=str(e))
            return {}
    
    def analizar_cobertura_avanzada(self) -> Dict:
        """Análisis avanzado de coberturas"""
        logger.info("analyzing_advanced_coverage")
        
        try:
            with self.engine.connect() as conn:
                # Cobertura por región
                cobertura_region = pd.read_sql(text("""
                    SELECT 
                        region,
                        COUNT(DISTINCT codigo_municipio) as municipios,
                        SUM(total_vacunados) as vacunados,
                        SUM(poblacion_total) as poblacion,
                        ROUND(AVG(cobertura_porcentaje), 1) as cobertura_promedio
                    FROM mv_dashboard_principal
                    WHERE poblacion_total > 0
                    GROUP BY region
                    ORDER BY cobertura_promedio DESC
                """), conn)
                
                # Cobertura por grupo etario
                cobertura_grupos = pd.read_sql(text("""
                    SELECT 
                        grupo_etario,
                        SUM(total_vacunados) as vacunados,
                        SUM(poblacion_total) as poblacion,
                        ROUND(
                            SUM(total_vacunados) * 100.0 / SUM(poblacion_total), 1
                        ) as cobertura_real
                    FROM mv_dashboard_principal
                    WHERE poblacion_total > 0
                    GROUP BY grupo_etario
                    ORDER BY cobertura_real DESC
                """), conn)
                
                # Municipios críticos
                municipios_criticos = pd.read_sql(text("""
                    SELECT 
                        municipio,
                        region,
                        SUM(total_vacunados) as vacunados,
                        SUM(poblacion_total) as poblacion,
                        ROUND(AVG(cobertura_porcentaje), 1) as cobertura
                    FROM mv_dashboard_principal
                    WHERE poblacion_total > 500
                    GROUP BY municipio, region
                    HAVING AVG(cobertura_porcentaje) < 50
                    ORDER BY AVG(cobertura_porcentaje) ASC
                    LIMIT 10
                """), conn)
                
                analisis_cobertura = {
                    'por_region': cobertura_region.to_dict('records'),
                    'por_grupo_etario': cobertura_grupos.to_dict('records'),
                    'municipios_criticos': municipios_criticos.to_dict('records'),
                    'municipios_criticos_count': len(municipios_criticos)
                }
                
                logger.info("advanced_coverage_analyzed",
                           regiones=len(cobertura_region),
                           grupos_etarios=len(cobertura_grupos),
                           municipios_criticos=len(municipios_criticos))
                
                return analisis_cobertura
                
        except Exception as e:
            logger.error("advanced_coverage_analysis_failed", error=str(e))
            return {}
    
    def obtener_metricas_sistema(self) -> Dict:
        """Obtiene métricas del sistema operativo"""
        logger.info("collecting_system_metrics")
        
        try:
            # Métricas memoria
            memory = psutil.virtual_memory()
            
            # Métricas CPU
            cpu_percent = psutil.cpu_percent(interval=1)
            
            # Métricas disco
            disk = psutil.disk_usage('/')
            
            # Métricas base de datos
            with self.engine.connect() as conn:
                db_size = conn.execute(text("""
                    SELECT pg_size_pretty(pg_database_size(current_database()))
                """)).scalar()
                
                db_connections = conn.execute(text("""
                    SELECT count(*) 
                    FROM pg_stat_activity 
                    WHERE state = 'active'
                """)).scalar()
                
                cache_hit_ratio = conn.execute(text("""
                    SELECT round(
                        sum(blks_hit) * 100.0 / nullif(sum(blks_hit + blks_read), 0), 2
                    ) 
                    FROM pg_stat_database
                """)).scalar()
            
            metricas = {
                'memoria': {
                    'total_gb': round(memory.total / (1024**3), 2),
                    'disponible_gb': round(memory.available / (1024**3), 2),
                    'porcentaje_uso': memory.percent
                },
                'cpu': {
                    'porcentaje_uso': cpu_percent,
                    'nucleos': psutil.cpu_count()
                },
                'disco': {
                    'total_gb': round(disk.total / (1024**3), 2),
                    'libre_gb': round(disk.free / (1024**3), 2),
                    'porcentaje_uso': round(disk.used / disk.total * 100, 2)
                },
                'base_datos': {
                    'tamaño': db_size,
                    'conexiones_activas': db_connections,
                    'cache_hit_ratio': cache_hit_ratio or 0
                }
            }
            
            # Calcular score de salud del sistema
            health_score = self._calcular_health_score(metricas)
            metricas['health_score'] = health_score
            
            logger.info("system_metrics_collected", health_score=health_score)
            
            return metricas
            
        except Exception as e:
            logger.error("system_metrics_collection_failed", error=str(e))
            return {}
    
    def _calcular_health_score(self, metricas: Dict) -> int:
        """Calcula score de salud del sistema (0-100)"""
        score = 100
        
        # Penalizar memoria alta
        if metricas['memoria']['porcentaje_uso'] > 90:
            score -= 20
        elif metricas['memoria']['porcentaje_uso'] > 75:
            score -= 10
        
        # Penalizar CPU alto
        if metricas['cpu']['porcentaje_uso'] > 80:
            score -= 15
        elif metricas['cpu']['porcentaje_uso'] > 60:
            score -= 5
        
        # Penalizar disco lleno
        if metricas['disco']['porcentaje_uso'] > 90:
            score -= 15
        elif metricas['disco']['porcentaje_uso'] > 80:
            score -= 5
        
        # Penalizar cache hit bajo
        cache_hit = metricas['base_datos']['cache_hit_ratio']
        if cache_hit < 80:
            score -= 10
        elif cache_hit < 90:
            score -= 5
        
        return max(0, score)
    
    def generar_reporte_html_optimizado(self) -> str:
        """Genera reporte HTML optimizado"""
        logger.info("generating_html_report")
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = FileConfig.LOGS_DIR / f"monitor_reporte_{timestamp}.html"
        
        try:
            FileConfig.create_directories()
            
            # Recopilar todos los datos
            indicadores = self.obtener_indicadores_tiempo_real()
            calidad = self.analizar_calidad_datos()
            alertas = self.detectar_alertas_automaticas()
            performance = self.analizar_performance_dashboard()
            cobertura = self.analizar_cobertura_avanzada()
            metricas = self.obtener_metricas_sistema()
            
            # Crear HTML optimizado
            html_content = f"""
<!DOCTYPE html>
<html lang="es">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Monitor Sistema Epidemiológico Tolima</title>
    <style>
        body {{ 
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; 
            margin: 0; 
            padding: 20px;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: #333;
        }}
        .container {{ 
            max-width: 1200px; 
            margin: 0 auto; 
            background: white; 
            border-radius: 15px; 
            box-shadow: 0 20px 40px rgba(0,0,0,0.1);
            overflow: hidden;
        }}
        .header {{ 
            background: linear-gradient(135deg, #2c3e50, #3498db); 
            color: white; 
            padding: 30px; 
            text-align: center;
        }}
        .metrics-grid {{ 
            display: grid; 
            grid-template-columns: repeat(auto-fit, minmax(300px, 1fr)); 
            gap: 20px; 
            padding: 30px; 
        }}
        .metric-card {{ 
            background: #f8f9fa; 
            border-radius: 10px; 
            padding: 25px; 
            border-left: 5px solid #3498db;
            box-shadow: 0 5px 15px rgba(0,0,0,0.08);
        }}
        .metric-card h3 {{ 
            margin-top: 0; 
            color: #2c3e50;
            display: flex;
            align-items: center;
        }}
        .metric-value {{ 
            font-size: 2em; 
            font-weight: bold; 
            color: #e74c3c; 
            margin: 15px 0;
        }}
        .metric-value.good {{ color: #27ae60; }}
        .metric-value.warning {{ color: #f39c12; }}
        .metric-value.critical {{ color: #e74c3c; }}
        .alert {{ 
            background: #fff5f5; 
            border: 1px solid #fed7d7; 
            border-radius: 8px; 
            padding: 15px; 
            margin: 10px 0;
        }}
        .alert.alta {{ border-color: #e53e3e; background: #fff5f5; }}
        .alert.media {{ border-color: #dd6b20; background: #fffaf0; }}
        .alert.baja {{ border-color: #38a169; background: #f0fff4; }}
        .progress-bar {{ 
            width: 100%; 
            height: 20px; 
            background: #ecf0f1; 
            border-radius: 10px; 
            overflow: hidden;
            margin: 10px 0;
        }}
        .progress-fill {{ 
            height: 100%; 
            background: linear-gradient(90deg, #3498db, #2ecc71); 
            transition: width 0.3s ease;
        }}
        .footer {{ 
            background: #34495e; 
            color: white; 
            text-align: center; 
            padding: 20px;
        }}
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>📊 Monitor Sistema Epidemiológico Tolima</h1>
            <p><strong>Reporte Generado:</strong> {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
            <p>Sistema de Pre-procesamiento Local → PostgreSQL → Dashboard Cloud</p>
        </div>
        
        <div class="metrics-grid">
            <div class="metric-card">
                <h3>💉 Indicadores Principales</h3>
                <div class="metric-value good">{indicadores.get('total_vacunados', 0):,}</div>
                <p>Total Vacunados</p>
                <p>Municipios Activos: <strong>{indicadores.get('municipios_activos', 0)}</strong></p>
                <p>Cobertura General: <strong>{indicadores.get('cobertura_general', 0):.1f}%</strong></p>
            </div>
            
            <div class="metric-card">
                <h3>📈 Calidad de Datos</h3>
                <div class="metric-value {'good' if calidad.get('score_calidad', 0) > 90 else 'warning' if calidad.get('score_calidad', 0) > 70 else 'critical'}">{calidad.get('score_calidad', 0)}/100</div>
                <p>Score de Calidad</p>
                <div class="progress-bar">
                    <div class="progress-fill" style="width: {calidad.get('score_calidad', 0)}%"></div>
                </div>
            </div>
            
            <div class="metric-card">
                <h3>⚡ Performance Dashboard</h3>
                <div class="metric-value {'good' if performance.get('performance_general', False) else 'warning'}">{performance.get('max_duration', 0):.2f}s</div>
                <p>Tiempo Máximo de Consulta</p>
                <p>Vista Materializada: <strong>{performance.get('vista_materializada', {}).get('duration_seconds', 0):.2f}s</strong></p>
            </div>
            
            <div class="metric-card">
                <h3>🚨 Alertas Detectadas</h3>
                <div class="metric-value {'critical' if len(alertas) > 5 else 'warning' if len(alertas) > 0 else 'good'}">{len(alertas)}</div>
                <p>Alertas Activas</p>"""
            
            # Agregar alertas críticas
            alertas_criticas = [a for a in alertas if a['severidad'] == 'ALTA'][:3]
            for alerta in alertas_criticas:
                html_content += f"""
                <div class="alert alta">
                    <strong>{alerta['tipo_alerta']}</strong>: {alerta['mensaje']}
                </div>"""
            
            html_content += f"""
            </div>
            
            <div class="metric-card">
                <h3>💻 Métricas Sistema</h3>
                <div class="metric-value {'good' if metricas.get('health_score', 0) > 80 else 'warning' if metricas.get('health_score', 0) > 60 else 'critical'}">{metricas.get('health_score', 0)}/100</div>
                <p>Health Score Sistema</p>
                <p>Memoria: <strong>{metricas.get('memoria', {}).get('porcentaje_uso', 0):.1f}%</strong></p>
                <p>CPU: <strong>{metricas.get('cpu', {}).get('porcentaje_uso', 0):.1f}%</strong></p>
                <p>BD Size: <strong>{metricas.get('base_datos', {}).get('tamaño', 'N/A')}</strong></p>
            </div>
            
            <div class="metric-card">
                <h3>🎯 Cobertura Avanzada</h3>
                <p><strong>Municipios Críticos:</strong> {cobertura.get('municipios_criticos_count', 0)}</p>"""
            
            # Top regiones por cobertura
            regiones = cobertura.get('por_region', [])[:3]
            for region in regiones:
                html_content += f"""
                <p>{region['region']}: <strong>{region['cobertura_promedio']:.1f}%</strong></p>"""
            
            html_content += """
            </div>
        </div>
        
        <div class="footer">
            <p><em>Monitor Sistema Epidemiológico Tolima V1.0</em></p>
            <p>🏥 Secretaría de Salud del Tolima - Vigilancia Epidemiológica</p>
        </div>
    </div>
</body>
</html>"""
            
            # Guardar archivo
            with open(filename, 'w', encoding='utf-8') as f:
                f.write(html_content)
            
            logger.info("html_report_generated", filename=str(filename))
            return str(filename)
            
        except Exception as e:
            logger.error("html_report_generation_failed", error=str(e))
            return ""
    
    def ejecutar_monitoreo_completo(self) -> bool:
        """Ejecuta monitoreo completo del sistema"""
        logger.info("complete_monitoring_started")
        
        try:
            # Conectar a BD
            if not self.conectar_bd():
                return False
            
            # Ejecutar todos los análisis
            logger.info("running_all_monitoring_analyses")
            
            indicadores = self.obtener_indicadores_tiempo_real()
            calidad = self.analizar_calidad_datos()
            alertas = self.detectar_alertas_automaticas()
            performance = self.analizar_performance_dashboard()
            cobertura = self.analizar_cobertura_avanzada()
            metricas = self.obtener_metricas_sistema()
            
            # Generar reporte HTML
            reporte_html = self.generar_reporte_html_optimizado()
            
            # Resumen ejecutivo
            duracion = datetime.now() - self.inicio
            
            resumen = {
                'duracion_segundos': duracion.total_seconds(),
                'total_vacunados': indicadores.get('total_vacunados', 0),
                'score_calidad': calidad.get('score_calidad', 0),
                'alertas_detectadas': len(alertas),
                'performance_ok': performance.get('performance_general', False),
                'health_score': metricas.get('health_score', 0),
                'reporte_html': reporte_html
            }
            
            logger.info("complete_monitoring_finished", **resumen)
            
            # Mostrar resumen en consola
            print(f"""
🔍 MONITOREO COMPLETO FINALIZADO
{'='*40}
⏱️ Duración: {duracion.total_seconds():.2f} segundos
💉 Total Vacunados: {indicadores.get('total_vacunados', 0):,}
📊 Score Calidad: {calidad.get('score_calidad', 0)}/100
🚨 Alertas: {len(alertas)}
⚡ Performance OK: {'✅' if performance.get('performance_general', False) else '❌'}
💻 Health Score: {metricas.get('health_score', 0)}/100
📄 Reporte: {reporte_html}
""")
            
            return True
            
        except Exception as e:
            logger.error("complete_monitoring_failed", error=str(e))
            return False

def main():
    """Función principal con argumentos de línea de comandos"""
    print("📊 MONITOR SISTEMA EPIDEMIOLÓGICO OPTIMIZADO")
    print("=" * 55)
    
    monitor = MonitorSistemaOptimizado()
    
    # Procesar argumentos
    if len(sys.argv) > 1:
        arg = sys.argv[1].lower()
        
        if arg == "--completo":
            success = monitor.ejecutar_monitoreo_completo()
        elif arg == "--indicadores":
            success = monitor.conectar_bd() and bool(monitor.obtener_indicadores_tiempo_real())
        elif arg == "--alertas":
            success = monitor.conectar_bd() and bool(monitor.detectar_alertas_automaticas())
        elif arg == "--calidad":
            success = monitor.conectar_bd() and bool(monitor.analizar_calidad_datos())
        else:
            print("Opciones: --completo, --indicadores, --alertas, --calidad")
            return False
    else:
        # Monitoreo completo por defecto
        success = monitor.ejecutar_monitoreo_completo()
    
    if success:
        print("✅ Monitoreo completado exitosamente")
    else:
        print("❌ Error en monitoreo")
    
    return success

if __name__ == "__main__":
    main()