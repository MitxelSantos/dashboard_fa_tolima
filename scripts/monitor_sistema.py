#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
monitor_sistema.py - Monitor y Análisis del Sistema Epidemiológico Tolima
Versión actualizada con configuración centralizada
Herramientas de monitoreo, análisis y generación de reportes avanzados
"""

import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime, timedelta
import warnings
import os

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

# Importar configuración centralizada
from config import (
    DATABASE_URL,
    FileConfig,
    obtener_grupos_etarios_definidos,
    cargar_codigos_divipola_desde_gpkg,
)

warnings.filterwarnings("ignore")


class MonitorSistemaTolima:
    def __init__(self):
        self.engine = create_engine(DATABASE_URL)
        self.timestamp = datetime.now()
        self.grupos_etarios = obtener_grupos_etarios_definidos()

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
        """Genera resumen general del sistema con métricas avanzadas"""
        print("📊 RESUMEN GENERAL DEL SISTEMA EPIDEMIOLÓGICO")
        print("=" * 60)

        try:
            with self.engine.connect() as conn:
                # Conteos por tabla principales
                tablas_principales = [
                    "unidades_territoriales",
                    "poblacion",
                    "vacunacion_fiebre_amarilla",
                    "casos_fiebre_amarilla",
                    "epizootias",
                ]

                print("🗄️ REGISTROS POR COMPONENTE:")
                totales = {}

                for tabla in tablas_principales:
                    try:
                        count = conn.execute(
                            text(f"SELECT COUNT(*) FROM {tabla}")
                        ).scalar()
                        totales[tabla] = count
                        tabla_display = (
                            tabla.replace("_", " ")
                            .replace("fiebre amarilla", "FA")
                            .title()
                        )
                        print(f"   {tabla_display}: {count:,}")
                    except:
                        totales[tabla] = 0
                        print(f"   {tabla.replace('_', ' ').title()}: No disponible")

                # Métricas epidemiológicas avanzadas
                print(f"\n💉 MÉTRICAS DE VACUNACIÓN:")
                try:
                    vac_metrics = pd.read_sql(
                        text(
                            """
                        SELECT 
                            COUNT(*) as total_vacunados,
                            COUNT(DISTINCT codigo_municipio) as municipios_con_vacunacion,
                            COUNT(DISTINCT institucion) as instituciones_activas,
                            ROUND(AVG(edad_anos), 1) as edad_promedio,
                            MIN(fecha_aplicacion) as primera_vacuna,
                            MAX(fecha_aplicacion) as ultima_vacuna,
                            COUNT(DISTINCT DATE_TRUNC('month', fecha_aplicacion)) as meses_activos
                        FROM vacunacion_fiebre_amarilla
                    """
                        ),
                        conn,
                    )

                    if len(vac_metrics) > 0:
                        m = vac_metrics.iloc[0]
                        print(f"   Total vacunados: {m['total_vacunados']:,}")
                        print(
                            f"   Municipios activos: {m['municipios_con_vacunacion']}"
                        )
                        print(f"   Instituciones activas: {m['instituciones_activas']}")
                        print(f"   Edad promedio: {m['edad_promedio']} años")
                        print(f"   Meses con actividad: {m['meses_activos']}")
                        if m["primera_vacuna"] and m["ultima_vacuna"]:
                            print(
                                f"   Período: {m['primera_vacuna']} a {m['ultima_vacuna']}"
                            )

                except Exception as e:
                    print(f"   ⚠️ Error obteniendo métricas: {e}")

                # Métricas poblacionales
                print(f"\n👥 MÉTRICAS POBLACIONALES:")
                try:
                    pob_metrics = pd.read_sql(
                        text(
                            """
                        SELECT 
                            SUM(poblacion_total) as poblacion_total_tolima,
                            COUNT(DISTINCT codigo_municipio) as municipios_poblacion,
                            COUNT(DISTINCT grupo_etario) as grupos_etarios,
                            SUM(CASE WHEN tipo_ubicacion = 'Urbano' THEN poblacion_total ELSE 0 END) as poblacion_urbana,
                            SUM(CASE WHEN tipo_ubicacion = 'Rural' THEN poblacion_total ELSE 0 END) as poblacion_rural
                        FROM poblacion
                    """
                        ),
                        conn,
                    )

                    if len(pob_metrics) > 0:
                        p = pob_metrics.iloc[0]
                        total_pob = p["poblacion_total_tolima"]
                        print(f"   Población total Tolima: {total_pob:,} habitantes")
                        print(f"   Municipios con datos: {p['municipios_poblacion']}")
                        print(f"   Grupos etarios: {p['grupos_etarios']}")
                        if total_pob > 0:
                            pct_urbano = p["poblacion_urbana"] / total_pob * 100
                            pct_rural = p["poblacion_rural"] / total_pob * 100
                            print(
                                f"   Urbano: {p['poblacion_urbana']:,} ({pct_urbano:.1f}%)"
                            )
                            print(
                                f"   Rural: {p['poblacion_rural']:,} ({pct_rural:.1f}%)"
                            )

                except Exception as e:
                    print(f"   ⚠️ Error obteniendo datos poblacionales: {e}")

                # Indicadores de cobertura aproximada
                if (
                    totales.get("vacunacion_fiebre_amarilla", 0) > 0
                    and totales.get("poblacion", 0) > 0
                ):
                    try:
                        cobertura_general = pd.read_sql(
                            text(
                                """
                            SELECT 
                                COUNT(v.*) as vacunados,
                                SUM(p.poblacion_total) as poblacion_objetivo,
                                ROUND(COUNT(v.*) * 100.0 / SUM(p.poblacion_total), 2) as cobertura_aproximada
                            FROM vacunacion_fiebre_amarilla v
                            LEFT JOIN poblacion p ON (
                                v.codigo_municipio = p.codigo_municipio AND
                                v.grupo_etario = p.grupo_etario AND
                                v.tipo_ubicacion = p.tipo_ubicacion
                            )
                        """
                            ),
                            conn,
                        )

                        if len(cobertura_general) > 0:
                            c = cobertura_general.iloc[0]
                            print(f"\n📈 COBERTURA APROXIMADA:")
                            print(f"   Vacunados: {c['vacunados']:,}")
                            print(f"   Población objetivo: {c['poblacion_objetivo']:,}")
                            print(
                                f"   Cobertura estimada: {c['cobertura_aproximada']:.1f}%"
                            )

                    except Exception as e:
                        print(f"   ⚠️ Error calculando cobertura: {e}")

                # Estado de vistas críticas
                print(f"\n👁️ ESTADO VISTAS CRÍTICAS:")
                vistas_criticas = [
                    "v_coberturas_dashboard",
                    "v_mapa_coberturas",
                    "v_indicadores_clave",
                ]

                for vista in vistas_criticas:
                    try:
                        count_vista = conn.execute(
                            text(f"SELECT COUNT(*) FROM {vista}")
                        ).scalar()
                        print(f"   {vista}: {count_vista:,} registros")
                    except Exception as e:
                        print(f"   {vista}: ❌ ERROR")

        except Exception as e:
            print(f"❌ Error generando resumen: {e}")

    def analisis_calidad_datos(self):
        """Análisis exhaustivo de la calidad de los datos"""
        print("\n🔍 ANÁLISIS AVANZADO DE CALIDAD DE DATOS")
        print("=" * 50)

        try:
            with self.engine.connect() as conn:
                # Calidad vacunación avanzada
                print("💉 Calidad datos vacunación:")

                calidad_vac = pd.read_sql(
                    text(
                        """
                    SELECT 
                        COUNT(*) as total,
                        COUNT(CASE WHEN codigo_municipio IS NULL THEN 1 END) as sin_municipio,
                        COUNT(CASE WHEN fecha_aplicacion IS NULL THEN 1 END) as sin_fecha,
                        COUNT(CASE WHEN grupo_etario IS NULL THEN 1 END) as sin_grupo_etario,
                        COUNT(CASE WHEN institucion IS NULL OR institucion = '' THEN 1 END) as sin_institucion,
                        COUNT(CASE WHEN edad_anos < 0 OR edad_anos > 90 THEN 1 END) as edad_invalida,
                        COUNT(DISTINCT codigo_municipio) as municipios_unicos,
                        COUNT(DISTINCT institucion) as instituciones_unicas,
                        ROUND(AVG(edad_anos), 1) as edad_promedio_calculada
                    FROM vacunacion_fiebre_amarilla
                """
                    ),
                    conn,
                )

                if len(calidad_vac) > 0:
                    c = calidad_vac.iloc[0]
                    total = c["total"]
                    print(f"   📊 Total registros: {total:,}")
                    print(
                        f"   🏙️ Sin código municipio: {c['sin_municipio']:,} ({c['sin_municipio']/total*100:.1f}%)"
                    )
                    print(
                        f"   📅 Sin fecha aplicación: {c['sin_fecha']:,} ({c['sin_fecha']/total*100:.1f}%)"
                    )
                    print(
                        f"   👥 Sin grupo etario: {c['sin_grupo_etario']:,} ({c['sin_grupo_etario']/total*100:.1f}%)"
                    )
                    print(
                        f"   🏥 Sin institución: {c['sin_institucion']:,} ({c['sin_institucion']/total*100:.1f}%)"
                    )
                    print(
                        f"   ⚠️ Edad inválida: {c['edad_invalida']:,} ({c['edad_invalida']/total*100:.1f}%)"
                    )
                    print(f"   🗺️ Municipios únicos: {c['municipios_unicos']}")
                    print(f"   🏥 Instituciones únicas: {c['instituciones_unicas']}")
                    print(f"   👤 Edad promedio: {c['edad_promedio_calculada']} años")

                # Calidad poblacional
                print("\n👥 Calidad datos población:")

                calidad_pob = pd.read_sql(
                    text(
                        """
                    SELECT 
                        COUNT(*) as total,
                        COUNT(CASE WHEN poblacion_total <= 0 THEN 1 END) as poblacion_invalida,
                        COUNT(CASE WHEN codigo_municipio IS NULL THEN 1 END) as sin_codigo,
                        COUNT(DISTINCT codigo_municipio) as municipios_unicos,
                        COUNT(DISTINCT grupo_etario) as grupos_unicos,
                        ROUND(AVG(poblacion_total), 0) as poblacion_promedio,
                        MIN(poblacion_total) as poblacion_minima,
                        MAX(poblacion_total) as poblacion_maxima
                    FROM poblacion
                """
                    ),
                    conn,
                )

                if len(calidad_pob) > 0:
                    c = calidad_pob.iloc[0]
                    total = c["total"]
                    print(f"   📊 Total registros: {total:,}")
                    print(f"   ❌ Población inválida: {c['poblacion_invalida']:,}")
                    print(f"   🏙️ Sin código municipio: {c['sin_codigo']:,}")
                    print(f"   🗺️ Municipios únicos: {c['municipios_unicos']}")
                    print(f"   👥 Grupos etarios: {c['grupos_unicos']}")
                    print(f"   📈 Población promedio: {c['poblacion_promedio']:,}")
                    print(
                        f"   📉 Rango población: {c['poblacion_minima']:,} - {c['poblacion_maxima']:,}"
                    )

                # Análisis integridad referencial
                print("\n🔗 Integridad referencial:")

                try:
                    integridad = pd.read_sql(
                        text(
                            """
                        SELECT 
                            (SELECT COUNT(*) FROM vacunacion_fiebre_amarilla v 
                             LEFT JOIN unidades_territoriales ut ON v.codigo_municipio = ut.codigo_divipola
                             WHERE ut.codigo_divipola IS NULL) as vacunacion_sin_territorio,
                            (SELECT COUNT(*) FROM poblacion p
                             LEFT JOIN unidades_territoriales ut ON p.codigo_municipio = ut.codigo_divipola  
                             WHERE ut.codigo_divipola IS NULL) as poblacion_sin_territorio
                    """
                        ),
                        conn,
                    )

                    if len(integridad) > 0:
                        i = integridad.iloc[0]
                        if i["vacunacion_sin_territorio"] > 0:
                            print(
                                f"   ⚠️ Vacunación sin territorio: {i['vacunacion_sin_territorio']:,}"
                            )
                        else:
                            print(f"   ✅ Vacunación: integridad territorial OK")

                        if i["poblacion_sin_territorio"] > 0:
                            print(
                                f"   ⚠️ Población sin territorio: {i['poblacion_sin_territorio']:,}"
                            )
                        else:
                            print(f"   ✅ Población: integridad territorial OK")

                except Exception as e:
                    print(f"   ❌ Error verificando integridad: {e}")

        except Exception as e:
            print(f"❌ Error análisis calidad: {e}")

    def generar_alertas(self):
        """Genera alertas avanzadas del sistema"""
        print("\n🚨 SISTEMA DE ALERTAS EPIDEMIOLÓGICAS")
        print("=" * 40)

        alertas = []

        try:
            with self.engine.connect() as conn:
                # Alerta: Municipios sin vacunación reciente
                municipios_sin_vac = pd.read_sql(
                    text(
                        """
                    SELECT ut.nombre, ut.codigo_divipola
                    FROM unidades_territoriales ut
                    LEFT JOIN vacunacion_fiebre_amarilla v ON ut.codigo_divipola = v.codigo_municipio
                    WHERE ut.tipo = 'municipio' AND v.codigo_municipio IS NULL
                """
                    ),
                    conn,
                )

                if len(municipios_sin_vac) > 0:
                    alertas.append(
                        f"⚠️ CRÍTICO: {len(municipios_sin_vac)} municipios sin datos de vacunación"
                    )

                # Alerta: Baja cobertura crítica
                try:
                    baja_cobertura = pd.read_sql(
                        text(
                            """
                        SELECT municipio, cobertura_porcentaje, poblacion_total
                        FROM v_coberturas_dashboard
                        WHERE cobertura_porcentaje < 50 AND poblacion_total > 1000
                        ORDER BY cobertura_porcentaje ASC
                        LIMIT 5
                    """
                        ),
                        conn,
                    )

                    if len(baja_cobertura) > 0:
                        alertas.append(
                            f"🔴 CRÍTICO: {len(baja_cobertura)} municipios con cobertura < 50%"
                        )
                        for _, row in baja_cobertura.iterrows():
                            alertas.append(
                                f"    • {row['municipio']}: {row['cobertura_porcentaje']:.1f}% ({row['poblacion_total']:,} hab)"
                            )

                except Exception as e:
                    print(f"   ⚠️ Error verificando coberturas: {e}")

                # Alerta: Datos desactualizados
                try:
                    ultima_actualizacion = pd.read_sql(
                        text(
                            """
                        SELECT 
                            MAX(fecha_aplicacion) as ultima_vacuna,
                            MAX(fecha_carga) as ultima_carga
                        FROM vacunacion_fiebre_amarilla
                    """
                        ),
                        conn,
                    )

                    if len(ultima_actualizacion) > 0:
                        ultima_vacuna = pd.to_datetime(
                            ultima_actualizacion.iloc[0]["ultima_vacuna"]
                        )
                        dias_sin_vacunar = (datetime.now() - ultima_vacuna).days

                        if dias_sin_vacunar > 7:
                            alertas.append(
                                f"📅 ATENCIÓN: {dias_sin_vacunar} días sin vacunación registrada"
                            )

                        ultima_carga = ultima_actualizacion.iloc[0]["ultima_carga"]
                        if ultima_carga:
                            ultima_carga = pd.to_datetime(ultima_carga)
                            dias_sin_actualizar = (datetime.now() - ultima_carga).days
                            if dias_sin_actualizar > 1:
                                alertas.append(
                                    f"💾 INFO: {dias_sin_actualizar} días sin actualizar datos"
                                )

                except Exception as e:
                    print(f"   ⚠️ Error verificando fechas: {e}")

                # Alerta: Instituciones inactivas
                try:
                    instituciones_inactivas = pd.read_sql(
                        text(
                            """
                        SELECT institucion, MAX(fecha_aplicacion) as ultima_actividad
                        FROM vacunacion_fiebre_amarilla
                        GROUP BY institucion
                        HAVING MAX(fecha_aplicacion) < CURRENT_DATE - INTERVAL '30 days'
                        ORDER BY MAX(fecha_aplicacion) ASC
                        LIMIT 5
                    """
                        ),
                        conn,
                    )

                    if len(instituciones_inactivas) > 0:
                        alertas.append(
                            f"🏥 ATENCIÓN: {len(instituciones_inactivas)} instituciones >30 días inactivas"
                        )

                except Exception as e:
                    pass

                # Mostrar alertas
                if alertas:
                    for alerta in alertas:
                        print(f"   {alerta}")
                else:
                    print("   ✅ No hay alertas críticas")

        except Exception as e:
            print(f"❌ Error generando alertas: {e}")

    def analisis_coberturas_avanzado(self):
        """Análisis avanzado de coberturas por múltiples dimensiones"""
        print("\n📈 ANÁLISIS AVANZADO DE COBERTURAS")
        print("=" * 40)

        try:
            with self.engine.connect() as conn:
                # Cobertura por grupo etario
                print("👥 Cobertura por grupo etario:")

                cobertura_grupos = pd.read_sql(
                    text(
                        """
                    SELECT 
                        c.grupo_etario,
                        SUM(c.vacunados) as total_vacunados,
                        SUM(c.poblacion_total) as poblacion_objetivo,
                        ROUND(AVG(c.cobertura_porcentaje), 1) as cobertura_promedio,
                        COUNT(DISTINCT c.municipio) as municipios
                    FROM v_coberturas_dashboard c
                    WHERE c.poblacion_total > 0
                    GROUP BY c.grupo_etario
                    ORDER BY 
                        CASE c.grupo_etario
                            WHEN '09-23 meses' THEN 1
                            WHEN '02-19 años' THEN 2  
                            WHEN '20-59 años' THEN 3
                            WHEN '60+ años' THEN 4
                            ELSE 5
                        END
                """
                    ),
                    conn,
                )

                if len(cobertura_grupos) > 0:
                    for _, row in cobertura_grupos.iterrows():
                        cob_real = (
                            (row["total_vacunados"] / row["poblacion_objetivo"] * 100)
                            if row["poblacion_objetivo"] > 0
                            else 0
                        )
                        print(
                            f"   {row['grupo_etario']}: {cob_real:.1f}% ({row['total_vacunados']:,}/{row['poblacion_objetivo']:,}) en {row['municipios']} municipios"
                        )

                # Cobertura urbano vs rural
                print("\n🏙️ Cobertura urbano vs rural:")

                cobertura_ubicacion = pd.read_sql(
                    text(
                        """
                    SELECT 
                        c.tipo_ubicacion,
                        SUM(c.vacunados) as total_vacunados,
                        SUM(c.poblacion_total) as poblacion_objetivo,
                        ROUND(AVG(c.cobertura_porcentaje), 1) as cobertura_promedio
                    FROM v_coberturas_dashboard c
                    WHERE c.poblacion_total > 0
                    GROUP BY c.tipo_ubicacion
                """
                    ),
                    conn,
                )

                if len(cobertura_ubicacion) > 0:
                    for _, row in cobertura_ubicacion.iterrows():
                        cob_real = (
                            (row["total_vacunados"] / row["poblacion_objetivo"] * 100)
                            if row["poblacion_objetivo"] > 0
                            else 0
                        )
                        print(
                            f"   {row['tipo_ubicacion']}: {cob_real:.1f}% ({row['total_vacunados']:,}/{row['poblacion_objetivo']:,})"
                        )

                # Top y bottom municipios por cobertura
                print("\n🏆 Top 5 municipios mejor cobertura:")

                top_municipios = pd.read_sql(
                    text(
                        """
                    SELECT 
                        municipio,
                        ROUND(AVG(cobertura_porcentaje), 1) as cobertura_promedio,
                        SUM(vacunados) as total_vacunados,
                        SUM(poblacion_total) as poblacion_total
                    FROM v_coberturas_dashboard
                    WHERE poblacion_total > 500  -- Solo municipios con población significativa
                    GROUP BY municipio
                    ORDER BY AVG(cobertura_porcentaje) DESC
                    LIMIT 5
                """
                    ),
                    conn,
                )

                if len(top_municipios) > 0:
                    for i, row in top_municipios.iterrows():
                        print(
                            f"   {i+1}. {row['municipio']}: {row['cobertura_promedio']:.1f}% ({row['total_vacunados']:,}/{row['poblacion_total']:,})"
                        )

                print("\n⚠️ Bottom 5 municipios menor cobertura:")

                bottom_municipios = pd.read_sql(
                    text(
                        """
                    SELECT 
                        municipio,
                        ROUND(AVG(cobertura_porcentaje), 1) as cobertura_promedio,
                        SUM(vacunados) as total_vacunados,
                        SUM(poblacion_total) as poblacion_total
                    FROM v_coberturas_dashboard
                    WHERE poblacion_total > 500
                    GROUP BY municipio
                    ORDER BY AVG(cobertura_porcentaje) ASC
                    LIMIT 5
                """
                    ),
                    conn,
                )

                if len(bottom_municipios) > 0:
                    for i, row in bottom_municipios.iterrows():
                        print(
                            f"   {i+1}. {row['municipio']}: {row['cobertura_promedio']:.1f}% ({row['total_vacunados']:,}/{row['poblacion_total']:,})"
                        )

        except Exception as e:
            print(f"❌ Error análisis coberturas: {e}")

    def generar_reporte_html_avanzado(self):
        """Genera reporte HTML avanzado con métricas completas"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = FileConfig.LOGS_DIR / f"reporte_avanzado_{timestamp}.html"

        try:
            FileConfig.create_directories()

            # Recopilar datos para el reporte
            with self.engine.connect() as conn:
                # Métricas principales
                metricas = pd.read_sql(
                    text(
                        """
                    SELECT 
                        (SELECT COUNT(*) FROM vacunacion_fiebre_amarilla) as vacunados,
                        (SELECT COUNT(DISTINCT codigo_municipio) FROM vacunacion_fiebre_amarilla) as municipios_vacunacion,
                        (SELECT SUM(poblacion_total) FROM poblacion) as poblacion_total,
                        (SELECT COUNT(*) FROM unidades_territoriales WHERE tipo='municipio') as municipios_totales,
                        (SELECT COUNT(*) FROM casos_fiebre_amarilla) as casos_total,
                        (SELECT COUNT(*) FROM epizootias) as epizootias_total
                """
                    ),
                    conn,
                )

            html_content = f"""
            <!DOCTYPE html>
            <html>
            <head>
                <title>Reporte Avanzado Sistema Epidemiológico Tolima</title>
                <meta charset="utf-8">
                <style>
                    body {{ 
                        font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; 
                        margin: 0; 
                        padding: 40px;
                        background: #f8f9fa;
                    }}
                    .header {{ 
                        background: linear-gradient(135deg, #2c3e50, #3498db); 
                        color: white; 
                        padding: 30px; 
                        border-radius: 10px; 
                        margin-bottom: 30px;
                        text-align: center;
                    }}
                    .metric-grid {{ 
                        display: grid; 
                        grid-template-columns: repeat(auto-fit, minmax(250px, 1fr)); 
                        gap: 20px; 
                        margin: 20px 0; 
                    }}
                    .metric-card {{ 
                        background: white; 
                        padding: 20px; 
                        border-radius: 8px; 
                        box-shadow: 0 2px 10px rgba(0,0,0,0.1);
                        border-left: 4px solid #3498db;
                    }}
                    .metric-value {{ 
                        font-size: 2.5em; 
                        font-weight: bold; 
                        color: #2c3e50; 
                        margin-bottom: 5px;
                    }}
                    .metric-label {{ 
                        color: #7f8c8d; 
                        font-size: 0.9em; 
                        text-transform: uppercase;
                    }}
                    .alert {{ 
                        background: #e74c3c; 
                        color: white; 
                        padding: 15px; 
                        border-radius: 5px; 
                        margin: 10px 0;
                    }}
                    .success {{ 
                        background: #27ae60; 
                        color: white; 
                        padding: 15px; 
                        border-radius: 5px; 
                        margin: 10px 0;
                    }}
                    .info {{ 
                        background: #3498db; 
                        color: white; 
                        padding: 15px; 
                        border-radius: 5px; 
                        margin: 10px 0;
                    }}
                    table {{ 
                        border-collapse: collapse; 
                        width: 100%; 
                        background: white;
                        border-radius: 8px;
                        overflow: hidden;
                        box-shadow: 0 2px 10px rgba(0,0,0,0.1);
                    }}
                    th, td {{ 
                        padding: 12px 15px; 
                        text-align: left; 
                        border-bottom: 1px solid #ecf0f1;
                    }}
                    th {{ 
                        background: #34495e; 
                        color: white; 
                        font-weight: 600;
                    }}
                    .footer {{ 
                        margin-top: 40px; 
                        text-align: center; 
                        color: #7f8c8d; 
                        padding: 20px;
                    }}
                </style>
            </head>
            <body>
                <div class="header">
                    <h1>📊 Sistema Epidemiológico Tolima</h1>
                    <h2>Reporte Avanzado de Monitoreo</h2>
                    <p><strong>Generado:</strong> {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
                </div>
                
                <div class="metric-grid">
            """

            # Agregar métricas principales
            if len(metricas) > 0:
                m = metricas.iloc[0]

                metrics_data = [
                    ("Total Vacunados", f"{m['vacunados']:,}", "💉"),
                    ("Población Total", f"{m['poblacion_total']:,}", "👥"),
                    (
                        "Municipios Activos",
                        f"{m['municipios_vacunacion']}/{m['municipios_totales']}",
                        "🏙️",
                    ),
                    ("Casos Registrados", f"{m['casos_total']:,}", "🦠"),
                    ("Epizootias", f"{m['epizootias_total']:,}", "🐒"),
                ]

                # Calcular cobertura
                if m["poblacion_total"] > 0:
                    cobertura = m["vacunados"] / m["poblacion_total"] * 100
                    metrics_data.append(
                        ("Cobertura Estimada", f"{cobertura:.1f}%", "📈")
                    )

                for label, value, icon in metrics_data:
                    html_content += f"""
                    <div class="metric-card">
                        <div class="metric-value">{icon} {value}</div>
                        <div class="metric-label">{label}</div>
                    </div>
                    """

            html_content += """
                </div>
                
                <div class="success">
                    ✅ Sistema operativo y funcionando correctamente
                </div>
                
                <div class="info">
                    📋 Todas las métricas se calculan en tiempo real desde la base de datos PostgreSQL
                </div>
                
                <div class="footer">
                    <p><em>Reporte generado automáticamente por el Monitor Sistema Tolima</em></p>
                    <p>🏥 Secretaría de Salud del Tolima - Vigilancia Epidemiológica</p>
                </div>
            </body>
            </html>
            """

            with open(filename, "w", encoding="utf-8") as f:
                f.write(html_content)

            print(f"\n📄 Reporte HTML avanzado generado: {filename}")
            return str(filename)

        except Exception as e:
            print(f"❌ Error generando reporte HTML: {e}")
            return None

    def monitoreo_completo(self):
        """Ejecuta monitoreo completo y avanzado del sistema"""
        print("🔍 MONITOR AVANZADO SISTEMA EPIDEMIOLÓGICO TOLIMA")
        print("=" * 60)
        print(f"⏰ {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

        if not self.test_conexion():
            return False

        # Ejecutar todos los análisis avanzados
        self.resumen_sistema()
        self.analisis_calidad_datos()
        self.generar_alertas()
        self.analisis_coberturas_avanzado()

        # Generar reporte HTML avanzado
        self.generar_reporte_html_avanzado()

        print(
            f"\n✅ Monitoreo avanzado completado a las {datetime.now().strftime('%H:%M:%S')}"
        )
        return True


def menu_monitor():
    """Menú interactivo del monitor avanzado"""
    monitor = MonitorSistemaTolima()

    print("🔍 MONITOR AVANZADO SISTEMA EPIDEMIOLÓGICO TOLIMA")
    print("=" * 55)
    print("1. 🚀 Monitoreo completo avanzado")
    print("2. 📊 Solo resumen sistema")
    print("3. 🔍 Solo análisis calidad")
    print("4. 🚨 Solo alertas epidemiológicas")
    print("5. 📈 Análisis coberturas avanzado")
    print("6. 📄 Generar reporte HTML")
    print("7. 🧪 Probar conexión PostgreSQL")
    print("0. 👋 Salir")

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
                    monitor.analisis_coberturas_avanzado()
            elif opcion == "6":
                if monitor.test_conexion():
                    monitor.generar_reporte_html_avanzado()
            elif opcion == "7":
                monitor.test_conexion()
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
        elif sys.argv[1] == "--alertas":
            monitor = MonitorSistemaTolima()
            if monitor.test_conexion():
                monitor.generar_alertas()
        else:
            print("Opciones: --completo, --resumen, --alertas")
    else:
        menu_monitor()
