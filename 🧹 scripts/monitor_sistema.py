#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Monitor y Análisis del Sistema Epidemiológico Tolima
Herramientas de monitoreo, análisis y generación de reportes
"""

import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime, timedelta
import warnings
import os
warnings.filterwarnings('ignore')

# Configuración
DATABASE_URL = "postgresql://tolima_admin:tolima2025!@localhost:5432/epidemiologia_tolima"

class MonitorSistemaTolima:
    def __init__(self):
        self.engine = create_engine(DATABASE_URL)
        self.timestamp = datetime.now()
        
    def test_conexion(self):
        """Prueba la conexión a PostgreSQL"""
        try:
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            print("✅ Conexión PostgreSQL exitosa")
            return True
        except Exception as e:
            print(f"❌ Error conexión: {e}")
            return False
    
    def resumen_sistema(self):
        """Genera resumen general del sistema"""
        print("📊 RESUMEN GENERAL DEL SISTEMA")
        print("=" * 50)
        
        try:
            with self.engine.connect() as conn:
                # Conteos por tabla
                tablas = [
                    'unidades_territoriales', 'poblacion', 'vacunacion_fiebre_amarilla',
                    'casos_fiebre_amarilla', 'epizootias'
                ]
                
                print("🗄️ REGISTROS POR TABLA:")
                for tabla in tablas:
                    try:
                        count = conn.execute(text(f"SELECT COUNT(*) FROM {tabla}")).scalar()
                        print(f"   {tabla.replace('_', ' ').title()}: {count:,}")
                    except:
                        print(f"   {tabla.replace('_', ' ').title()}: No disponible")
                
                # Estadísticas de vacunación
                try:
                    vac_stats = pd.read_sql(text("""
                        SELECT 
                            COUNT(*) as total_vacunados,
                            COUNT(DISTINCT codigo_municipio) as municipios_con_vacunacion,
                            COUNT(DISTINCT institucion) as instituciones_activas,
                            MIN(fecha_aplicacion) as primera_vacuna,
                            MAX(fecha_aplicacion) as ultima_vacuna
                        FROM vacunacion_fiebre_amarilla
                    """), conn)
                    
                    if len(vac_stats) > 0:
                        stats = vac_stats.iloc[0]
                        print(f"\n💉 ESTADÍSTICAS VACUNACIÓN:")
                        print(f"   Total vacunados: {stats['total_vacunados']:,}")
                        print(f"   Municipios activos: {stats['municipios_con_vacunacion']}")
                        print(f"   Instituciones activas: {stats['instituciones_activas']}")
                        print(f"   Período: {stats['primera_vacuna']} a {stats['ultima_vacuna']}")
                        
                except Exception as e:
                    print(f"⚠️ Error estadísticas vacunación: {e}")
                
                # Estadísticas poblacionales
                try:
                    pob_stats = pd.read_sql(text("""
                        SELECT 
                            SUM(poblacion_total) as poblacion_total,
                            COUNT(DISTINCT codigo_municipio) as municipios_con_poblacion,
                            COUNT(DISTINCT grupo_etario) as grupos_etarios
                        FROM poblacion
                    """), conn)
                    
                    if len(pob_stats) > 0:
                        stats = pob_stats.iloc[0]
                        print(f"\n👥 ESTADÍSTICAS POBLACIÓN:")
                        print(f"   Población total: {stats['poblacion_total']:,}")
                        print(f"   Municipios: {stats['municipios_con_poblacion']}")
                        print(f"   Grupos etarios: {stats['grupos_etarios']}")
                        
                except Exception as e:
                    print(f"⚠️ Error estadísticas población: {e}")
                
        except Exception as e:
            print(f"❌ Error generando resumen: {e}")
    
    def analisis_calidad_datos(self):
        """Analiza la calidad de los datos"""
        print("\n🔍 ANÁLISIS CALIDAD DE DATOS")
        print("=" * 40)
        
        try:
            with self.engine.connect() as conn:
                # Calidad vacunación
                print("💉 Calidad datos vacunación:")
                
                calidad_vac = pd.read_sql(text("""
                    SELECT 
                        COUNT(*) as total,
                        COUNT(CASE WHEN codigo_municipio IS NULL THEN 1 END) as sin_municipio,
                        COUNT(CASE WHEN fecha_aplicacion IS NULL THEN 1 END) as sin_fecha,
                        COUNT(CASE WHEN grupo_etario IS NULL THEN 1 END) as sin_grupo_etario,
                        COUNT(CASE WHEN edad_anos < 0 OR edad_anos > 90 THEN 1 END) as edad_invalida,
                        COUNT(DISTINCT codigo_municipio) as municipios_unicos
                    FROM vacunacion_fiebre_amarilla
                """), conn)
                
                if len(calidad_vac) > 0:
                    c = calidad_vac.iloc[0]
                    print(f"   Total registros: {c['total']:,}")
                    print(f"   Sin código municipio: {c['sin_municipio']:,} ({c['sin_municipio']/c['total']*100:.1f}%)")
                    print(f"   Sin fecha: {c['sin_fecha']:,} ({c['sin_fecha']/c['total']*100:.1f}%)")
                    print(f"   Sin grupo etario: {c['sin_grupo_etario']:,} ({c['sin_grupo_etario']/c['total']*100:.1f}%)")
                    print(f"   Edad inválida: {c['edad_invalida']:,} ({c['edad_invalida']/c['total']*100:.1f}%)")
                    print(f"   Municipios únicos: {c['municipios_unicos']}")
                
                # Calidad población
                print("\n👥 Calidad datos población:")
                
                calidad_pob = pd.read_sql(text("""
                    SELECT 
                        COUNT(*) as total,
                        COUNT(CASE WHEN poblacion_total <= 0 THEN 1 END) as poblacion_invalida,
                        COUNT(CASE WHEN codigo_municipio IS NULL THEN 1 END) as sin_codigo,
                        AVG(poblacion_total) as poblacion_promedio,
                        MIN(poblacion_total) as poblacion_minima,
                        MAX(poblacion_total) as poblacion_maxima
                    FROM poblacion
                """), conn)
                
                if len(calidad_pob) > 0:
                    c = calidad_pob.iloc[0]
                    print(f"   Total registros: {c['total']:,}")
                    print(f"   Población inválida: {c['poblacion_invalida']:,}")
                    print(f"   Sin código municipio: {c['sin_codigo']:,}")
                    print(f"   Población promedio: {c['poblacion_promedio']:,.0f}")
                    print(f"   Rango: {c['poblacion_minima']:,} - {c['poblacion_maxima']:,}")
                
        except Exception as e:
            print(f"❌ Error análisis calidad: {e}")
    
    def generar_alertas(self):
        """Genera alertas del sistema"""
        print("\n🚨 ALERTAS DEL SISTEMA")
        print("=" * 30)
        
        alertas = []
        
        try:
            with self.engine.connect() as conn:
                # Alerta: Municipios sin vacunación
                municipios_sin_vac = pd.read_sql(text("""
                    SELECT ut.nombre
                    FROM unidades_territoriales ut
                    LEFT JOIN vacunacion_fiebre_amarilla v ON ut.codigo_divipola = v.codigo_municipio
                    WHERE ut.tipo = 'municipio' AND v.codigo_municipio IS NULL
                """), conn)
                
                if len(municipios_sin_vac) > 0:
                    alertas.append(f"⚠️ {len(municipios_sin_vac)} municipios sin datos de vacunación")
                
                # Alerta: Baja cobertura
                try:
                    baja_cobertura = pd.read_sql(text("""
                        SELECT COUNT(*) as municipios_baja_cobertura
                        FROM v_coberturas_dashboard
                        WHERE cobertura_porcentaje < 70 AND poblacion_total > 0
                    """), conn)
                    
                    if len(baja_cobertura) > 0 and baja_cobertura.iloc[0]['municipios_baja_cobertura'] > 0:
                        count = baja_cobertura.iloc[0]['municipios_baja_cobertura']
                        alertas.append(f"🔴 {count} registros con cobertura < 70%")
                except:
                    pass
                
                # Alerta: Datos desactualizados
                try:
                    fecha_maxima = pd.read_sql(text("""
                        SELECT MAX(fecha_aplicacion) as ultima_fecha
                        FROM vacunacion_fiebre_amarilla
                    """), conn)
                    
                    if len(fecha_maxima) > 0:
                        ultima_fecha = pd.to_datetime(fecha_maxima.iloc[0]['ultima_fecha'])
                        dias_sin_actualizar = (datetime.now() - ultima_fecha).days
                        
                        if dias_sin_actualizar > 30:
                            alertas.append(f"📅 {dias_sin_actualizar} días sin actualizar vacunación")
                except:
                    pass
                
                # Mostrar alertas
                if alertas:
                    for alerta in alertas:
                        print(f"   {alerta}")
                else:
                    print("   ✅ No hay alertas críticas")
                    
        except Exception as e:
            print(f"❌ Error generando alertas: {e}")
    
    def top_municipios_vacunacion(self, limit=10):
        """Top municipios por vacunación"""
        print(f"\n🏆 TOP {limit} MUNICIPIOS POR VACUNACIÓN")
        print("=" * 45)
        
        try:
            top_municipios = pd.read_sql(text(f"""
                SELECT 
                    municipio,
                    COUNT(*) as vacunados,
                    COUNT(DISTINCT institucion) as instituciones,
                    MIN(fecha_aplicacion) as primera_vacuna,
                    MAX(fecha_aplicacion) as ultima_vacuna
                FROM vacunacion_fiebre_amarilla
                GROUP BY municipio
                ORDER BY vacunados DESC
                LIMIT {limit}
            """), self.engine)
            
            for i, row in top_municipios.iterrows():
                print(f"{i+1:2d}. {row['municipio']}: {row['vacunados']:,} vacunados "
                      f"({row['instituciones']} instituciones)")
                      
        except Exception as e:
            print(f"❌ Error top municipios: {e}")
    
    def analisis_temporal_vacunacion(self):
        """Análisis temporal de vacunación"""
        print("\n📈 ANÁLISIS TEMPORAL VACUNACIÓN")
        print("=" * 40)
        
        try:
            # Por año y mes
            temporal = pd.read_sql(text("""
                SELECT 
                    año,
                    mes,
                    COUNT(*) as vacunados,
                    COUNT(DISTINCT municipio) as municipios_activos
                FROM vacunacion_fiebre_amarilla
                GROUP BY año, mes
                ORDER BY año DESC, mes DESC
                LIMIT 12
            """), self.engine)
            
            print("📅 Últimos 12 meses:")
            for _, row in temporal.iterrows():
                print(f"   {row['año']}-{row['mes']:02d}: {row['vacunados']:,} vacunados "
                      f"({row['municipios_activos']} municipios)")
            
            # Análisis por día de la semana
            try:
                por_dia = pd.read_sql(text("""
                    SELECT 
                        EXTRACT(DOW FROM fecha_aplicacion) as dia_semana,
                        COUNT(*) as vacunados
                    FROM vacunacion_fiebre_amarilla
                    GROUP BY EXTRACT(DOW FROM fecha_aplicacion)
                    ORDER BY dia_semana
                """), self.engine)
                
                dias = ['Dom', 'Lun', 'Mar', 'Mié', 'Jue', 'Vie', 'Sáb']
                print("\n📅 Distribución por día de la semana:")
                for _, row in por_dia.iterrows():
                    dia_idx = int(row['dia_semana'])
                    print(f"   {dias[dia_idx]}: {row['vacunados']:,}")
                    
            except Exception as e:
                print(f"⚠️ Error análisis por día: {e}")
                
        except Exception as e:
            print(f"❌ Error análisis temporal: {e}")
    
    def distribucion_grupos_etarios(self):
        """Distribución por grupos etarios"""
        print("\n👥 DISTRIBUCIÓN GRUPOS ETARIOS")
        print("=" * 35)
        
        try:
            grupos = pd.read_sql(text("""
                SELECT 
                    grupo_etario,
                    COUNT(*) as vacunados,
                    ROUND(AVG(edad_anos), 1) as edad_promedio
                FROM vacunacion_fiebre_amarilla
                WHERE grupo_etario IS NOT NULL
                GROUP BY grupo_etario
                ORDER BY 
                    CASE grupo_etario
                        WHEN 'Menor de 9 meses' THEN 1
                        WHEN '09-23 meses' THEN 2
                        WHEN '02-19 años' THEN 3
                        WHEN '20-59 años' THEN 4
                        WHEN '60+ años' THEN 5
                        ELSE 6
                    END
            """), self.engine)
            
            total = grupos['vacunados'].sum()
            
            for _, row in grupos.iterrows():
                porcentaje = (row['vacunados'] / total) * 100
                print(f"   {row['grupo_etario']}: {row['vacunados']:,} ({porcentaje:.1f}%) "
                      f"[edad prom: {row['edad_promedio']:.1f}]")
                      
        except Exception as e:
            print(f"❌ Error distribución grupos: {e}")
    
    def generar_reporte_html(self):
        """Genera reporte HTML completo"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"reportes/reporte_sistema_{timestamp}.html"
        
        try:
            os.makedirs("reportes", exist_ok=True)
            
            html_content = f"""
            <!DOCTYPE html>
            <html>
            <head>
                <title>Reporte Sistema Epidemiológico Tolima</title>
                <style>
                    body {{ font-family: Arial, sans-serif; margin: 40px; }}
                    h1, h2 {{ color: #2c3e50; }}
                    .metric {{ background: #ecf0f1; padding: 10px; margin: 5px; border-radius: 5px; }}
                    .alert {{ background: #e74c3c; color: white; padding: 10px; border-radius: 5px; }}
                    .success {{ background: #27ae60; color: white; padding: 10px; border-radius: 5px; }}
                    table {{ border-collapse: collapse; width: 100%; }}
                    th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
                    th {{ background-color: #34495e; color: white; }}
                </style>
            </head>
            <body>
                <h1>📊 Reporte Sistema Epidemiológico Tolima</h1>
                <p><strong>Generado:</strong> {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
                
                <h2>📈 Métricas Principales</h2>
                <div class="metrics">
            """
            
            # Obtener métricas para el HTML
            with self.engine.connect() as conn:
                try:
                    metrics = pd.read_sql(text("""
                        SELECT 
                            (SELECT COUNT(*) FROM vacunacion_fiebre_amarilla) as vacunados,
                            (SELECT COUNT(DISTINCT codigo_municipio) FROM vacunacion_fiebre_amarilla) as municipios,
                            (SELECT SUM(poblacion_total) FROM poblacion) as poblacion_total,
                            (SELECT COUNT(*) FROM unidades_territoriales WHERE tipo='municipio') as municipios_totales
                    """), conn)
                    
                    if len(metrics) > 0:
                        m = metrics.iloc[0]
                        html_content += f"""
                        <div class="metric">Total Vacunados: {m['vacunados']:,}</div>
                        <div class="metric">Municipios con Vacunación: {m['municipios']}/{m['municipios_totales']}</div>
                        <div class="metric">Población Total: {m['poblacion_total']:,}</div>
                        """
                except:
                    html_content += '<div class="alert">Error obteniendo métricas</div>'
                
            html_content += """
                </div>
                
                <h2>🎯 Estado del Sistema</h2>
                <div class="success">✅ Sistema operativo</div>
                
                <p><em>Reporte generado automáticamente por el Monitor Sistema Tolima</em></p>
            </body>
            </html>
            """
            
            with open(filename, 'w', encoding='utf-8') as f:
                f.write(html_content)
                
            print(f"\n📄 Reporte HTML generado: {filename}")
            return filename
            
        except Exception as e:
            print(f"❌ Error generando reporte HTML: {e}")
            return None
    
    def monitoreo_completo(self):
        """Ejecuta monitoreo completo del sistema"""
        print("🔍 MONITOR SISTEMA EPIDEMIOLÓGICO TOLIMA")
        print("=" * 50)
        print(f"⏰ {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        if not self.test_conexion():
            return False
        
        # Ejecutar todos los análisis
        self.resumen_sistema()
        self.analisis_calidad_datos()
        self.generar_alertas()
        self.top_municipios_vacunacion()
        self.analisis_temporal_vacunacion()
        self.distribucion_grupos_etarios()
        
        # Generar reporte HTML
        self.generar_reporte_html()
        
        print(f"\n✅ Monitoreo completado a las {datetime.now().strftime('%H:%M:%S')}")
        return True


def menu_monitor():
    """Menú interactivo del monitor"""
    monitor = MonitorSistemaTolima()
    
    print("🔍 MONITOR SISTEMA EPIDEMIOLÓGICO TOLIMA")
    print("=" * 45)
    print("1. Monitoreo completo")
    print("2. Solo resumen sistema")
    print("3. Solo análisis calidad")
    print("4. Solo alertas")
    print("5. Top municipios vacunación")
    print("6. Análisis temporal")
    print("7. Distribución grupos etarios")
    print("8. Generar reporte HTML")
    print("0. Salir")
    
    while True:
        try:
            opcion = input("\n🔢 Selecciona opción: ")
            
            if opcion == "1":
                monitor.monitoreo_completo()
            elif opcion == "2":
                if monitor.test_conexion():
                    monitor.resumen_sistema()
            elif opcion == "3":
                if monitor.test_conexion():
                    monitor.analisis_calidad_datos()
            elif opcion == "4":
                if monitor.test_conexion():
                    monitor.generar_alertas()
            elif opcion == "5":
                if monitor.test_conexion():
                    limit = input("¿Cuántos municipios mostrar? (10): ") or "10"
                    monitor.top_municipios_vacunacion(int(limit))
            elif opcion == "6":
                if monitor.test_conexion():
                    monitor.analisis_temporal_vacunacion()
            elif opcion == "7":
                if monitor.test_conexion():
                    monitor.distribucion_grupos_etarios()
            elif opcion == "8":
                if monitor.test_conexion():
                    monitor.generar_reporte_html()
            elif opcion == "0":
                print("👋 ¡Hasta luego!")
                break
            else:
                print("❌ Opción inválida")
                
        except KeyboardInterrupt:
            print("\n👋 Saliendo...")
            break
        except Exception as e:
            print(f"❌ Error: {e}")


# ================================
# FUNCIÓN PRINCIPAL
# ================================
if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1:
        if sys.argv[1] == "--completo":
            monitor = MonitorSistemaTolima()
            monitor.monitoreo_completo()
        elif sys.argv[1] == "--resumen":
            monitor = MonitorSistemaTolima()
            if monitor.test_conexion():
                monitor.resumen_sistema()
        else:
            print("Opciones: --completo, --resumen")
    else:
        menu_monitor()