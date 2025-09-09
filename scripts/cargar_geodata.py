#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Script: Cargar Unidades Territoriales desde .gpkg a PostgreSQL
Carga departamento, municipios, veredas y cabeceras del Tolima
"""

import geopandas as gpd
import pandas as pd
from sqlalchemy import create_engine, text
import warnings
from datetime import datetime
import os
warnings.filterwarnings('ignore')

# Configuración BD
DATABASE_URL = "postgresql://tolima_admin:tolima2025!@localhost:5432/epidemiologia_tolima"

def cargar_unidades_territoriales_postgresql(archivo_gpkg):
    """
    Carga unidades territoriales desde .gpkg a PostgreSQL
    """
    print("🗺️ CARGANDO UNIDADES TERRITORIALES A POSTGRESQL")
    print("=" * 60)
    
    inicio = datetime.now()
    
    try:
        # 1. VERIFICAR ARCHIVO
        if not os.path.exists(archivo_gpkg):
            print(f"❌ ERROR: Archivo no encontrado: {archivo_gpkg}")
            return False
            
        print(f"📂 Cargando desde: {archivo_gpkg}")
        
        # 2. CARGAR GEODATAFRAME
        print("🔄 Leyendo archivo .gpkg...")
        gdf = gpd.read_file(archivo_gpkg)
        print(f"📊 Registros leídos: {len(gdf):,}")
        
        # 3. VERIFICAR ESTRUCTURA
        print("🔍 Verificando estructura...")
        print(f"   Columnas: {list(gdf.columns)}")
        print(f"   CRS: {gdf.crs}")
        print(f"   Tipos únicos: {sorted(gdf['tipo'].unique()) if 'tipo' in gdf.columns else 'N/A'}")
        
        # 4. PREPARAR DATOS PARA POSTGRESQL
        print("🔧 Preparando datos para PostgreSQL...")
        
        # Renombrar geometría para PostGIS
        if gdf.geometry.name != 'geometria':
            gdf = gdf.rename_geometry('geometria')
        
        # Asegurar CRS correcto (EPSG:4326)
        if gdf.crs != 'EPSG:4326':
            print(f"🔄 Convirtiendo CRS de {gdf.crs} a EPSG:4326...")
            gdf = gdf.to_crs('EPSG:4326')
        
        # Limpiar datos nulos
        gdf = gdf.where(pd.notnull(gdf), None)
        
        # 5. CONECTAR Y CARGAR A POSTGRESQL
        print("🐘 Conectando a PostgreSQL...")
        engine = create_engine(DATABASE_URL, pool_size=10, max_overflow=20)
        
        # Verificar conexión
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        print("✅ Conexión exitosa")
        
        # 6. CARGAR CON to_postgis
        print(f"📥 Cargando {len(gdf):,} registros a tabla 'unidades_territoriales'...")
        
        gdf.to_postgis(
            'unidades_territoriales',
            engine,
            if_exists='replace',
            index=False,
            chunksize=1000
        )
        
        # 7. VERIFICAR CARGA Y ESTADÍSTICAS
        with engine.connect() as conn:
            # Contar registros
            total = conn.execute(text("SELECT COUNT(*) FROM unidades_territoriales")).scalar()
            print(f"✅ Registros cargados: {total:,}")
            
            # Estadísticas por tipo
            stats_tipo = pd.read_sql(text("""
                SELECT tipo, COUNT(*) as cantidad, 
                       ROUND(AVG(area_oficial_km2), 2) as area_promedio
                FROM unidades_territoriales 
                GROUP BY tipo 
                ORDER BY tipo
            """), conn)
            
            print("\n📊 ESTADÍSTICAS POR TIPO:")
            for _, row in stats_tipo.iterrows():
                print(f"   {row['tipo'].capitalize()}: {row['cantidad']:,} unidades "
                      f"(área prom: {row['area_promedio'] or 0:.2f} km²)")
            
            # Estadísticas por región
            stats_region = pd.read_sql(text("""
                SELECT region, COUNT(*) as cantidad
                FROM unidades_territoriales 
                WHERE region IS NOT NULL AND region != 'TODAS'
                GROUP BY region 
                ORDER BY region
            """), conn)
            
            if len(stats_region) > 0:
                print("\n📍 ESTADÍSTICAS POR REGIÓN:")
                for _, row in stats_region.iterrows():
                    print(f"   {row['region']}: {row['cantidad']:,} unidades")
            
            # Verificar geometrías válidas
            geom_validas = conn.execute(text("""
                SELECT COUNT(*) 
                FROM unidades_territoriales 
                WHERE geometria IS NOT NULL AND ST_IsValid(geometria)
            """)).scalar()
            
            print(f"\n🗺️ Geometrías válidas: {geom_validas:,} de {total:,}")
            
            # Crear índices espaciales si no existen
            print("🔧 Creando/verificando índices espaciales...")
            try:
                conn.execute(text("""
                    CREATE INDEX IF NOT EXISTS idx_unidades_territoriales_geom 
                    ON unidades_territoriales USING GIST(geometria)
                """))
                conn.execute(text("""
                    CREATE INDEX IF NOT EXISTS idx_unidades_territoriales_divipola 
                    ON unidades_territoriales(codigo_divipola)
                """))
                conn.execute(text("""
                    CREATE INDEX IF NOT EXISTS idx_unidades_territoriales_tipo 
                    ON unidades_territoriales(tipo)
                """))
                conn.commit()
                print("✅ Índices espaciales creados/verificados")
            except Exception as e:
                print(f"⚠️ Error creando índices: {e}")
        
        # 8. RESUMEN FINAL
        duracion = datetime.now() - inicio
        print(f"\n{'='*60}")
        print("✅ CARGA COMPLETADA EXITOSAMENTE")
        print("=" * 60)
        print(f"📊 Total unidades: {total:,}")
        print(f"🗺️ Sistema coordenadas: EPSG:4326")
        print(f"⏱️ Tiempo total: {duracion.total_seconds():.1f} segundos")
        print("🎯 Listo para análisis geoespaciales!")
        
        return True
        
    except Exception as e:
        print(f"❌ Error cargando unidades territoriales: {e}")
        import traceback
        traceback.print_exc()
        return False


def verificar_integridad_territorial():
    """
    Verifica integridad de datos territoriales cargados
    """
    print("\n🔍 VERIFICANDO INTEGRIDAD TERRITORIAL...")
    
    try:
        engine = create_engine(DATABASE_URL)
        
        with engine.connect() as conn:
            # Verificaciones básicas
            verificaciones = {
                "total_unidades": "SELECT COUNT(*) FROM unidades_territoriales",
                "sin_codigo_divipola": "SELECT COUNT(*) FROM unidades_territoriales WHERE codigo_divipola IS NULL",
                "sin_geometria": "SELECT COUNT(*) FROM unidades_territoriales WHERE geometria IS NULL",
                "geometrias_invalidas": "SELECT COUNT(*) FROM unidades_territoriales WHERE NOT ST_IsValid(geometria)",
                "municipios_tolima": "SELECT COUNT(*) FROM unidades_territoriales WHERE tipo='municipio' AND codigo_dpto='73'"
            }
            
            print("📊 Verificaciones integridad:")
            for nombre, query in verificaciones.items():
                try:
                    resultado = conn.execute(text(query)).scalar()
                    print(f"   {nombre}: {resultado:,}")
                except Exception as e:
                    print(f"   {nombre}: ERROR - {e}")
            
            # Verificar municipios esperados del Tolima
            municipios_esperados = [
                "Ibagué", "Mariquita", "Espinal", "Honda", "Flandes", 
                "Melgar", "Líbano", "Chaparral", "Purificación", "Guamo"
            ]
            
            municipios_encontrados = pd.read_sql(text("""
                SELECT nombre FROM unidades_territoriales 
                WHERE tipo = 'municipio' AND codigo_dpto = '73'
                ORDER BY nombre
            """), conn)
            
            print(f"\n📍 Municipios cargados: {len(municipios_encontrados)} encontrados")
            
            missing = set(municipios_esperados) - set(municipios_encontrados['nombre'])
            if missing:
                print(f"⚠️ Municipios esperados faltantes: {missing}")
            else:
                print("✅ Municipios principales encontrados")
        
        return True
        
    except Exception as e:
        print(f"❌ Error verificación: {e}")
        return False


def generar_muestra_consultas_territoriales():
    """
    Genera consultas de muestra para probar datos territoriales
    """
    print("\n📝 GENERANDO CONSULTAS DE MUESTRA...")
    
    consultas_muestra = {
        "Municipios por región": """
            SELECT region, COUNT(*) as municipios, 
                   ROUND(SUM(area_oficial_km2), 2) as area_total_km2
            FROM unidades_territoriales 
            WHERE tipo = 'municipio' 
            GROUP BY region 
            ORDER BY area_total_km2 DESC;
        """,
        
        "Top 10 municipios más grandes": """
            SELECT nombre, area_oficial_km2, region
            FROM unidades_territoriales 
            WHERE tipo = 'municipio' AND area_oficial_km2 IS NOT NULL
            ORDER BY area_oficial_km2 DESC 
            LIMIT 10;
        """,
        
        "Veredas por municipio (top 5)": """
            SELECT municipio, COUNT(*) as veredas
            FROM unidades_territoriales 
            WHERE tipo = 'vereda' 
            GROUP BY municipio 
            ORDER BY veredas DESC 
            LIMIT 5;
        """,
        
        "Resumen por tipo territorial": """
            SELECT tipo, COUNT(*) as cantidad,
                   ROUND(SUM(area_oficial_km2), 2) as area_total,
                   ROUND(AVG(area_oficial_km2), 2) as area_promedio
            FROM unidades_territoriales 
            GROUP BY tipo 
            ORDER BY cantidad DESC;
        """
    }
    
    try:
        engine = create_engine(DATABASE_URL)
        
        for nombre, query in consultas_muestra.items():
            print(f"\n🔎 {nombre}:")
            try:
                resultado = pd.read_sql(text(query), engine)
                print(resultado.to_string(index=False))
            except Exception as e:
                print(f"   ERROR: {e}")
        
        return True
        
    except Exception as e:
        print(f"❌ Error generando muestras: {e}")
        return False


# ================================
# FUNCIÓN PRINCIPAL
# ================================
if __name__ == "__main__":
    print("🗺️ CARGADOR UNIDADES TERRITORIALES → POSTGRESQL")
    print("=" * 60)
    
    # Archivo por defecto
    archivo_gpkg_default = "data/tolima_cabeceras_veredas.gpkg"
    
    # Verificar archivo
    if not os.path.exists(archivo_gpkg_default):
        print(f"❌ ERROR: No se encuentra '{archivo_gpkg_default}'")
        print("\n💡 Opciones:")
        print("1. Generar archivo .gpkg usando tu script 'preprocesamiento_geodata.py'")
        print("2. Colocar archivo .gpkg en 'data/tolima_cabeceras_veredas.gpkg'")  
        print("3. Modificar variable archivo_gpkg_default")
        print("4. Llamar función: cargar_unidades_territoriales_postgresql('archivo.gpkg')")
    else:
        print(f"📂 Procesando: {archivo_gpkg_default}")
        
        # Ejecutar carga completa
        exito = cargar_unidades_territoriales_postgresql(archivo_gpkg_default)
        
        if exito:
            print("\n🔧 Ejecutando verificaciones...")
            verificar_integridad_territorial()
            
            print("\n📊 Generando consultas de muestra...")
            generar_muestra_consultas_territoriales()
            
            print(f"\n🎯 PRÓXIMOS PASOS:")
            print("1. Abrir DBeaver/pgAdmin y explorar tabla 'unidades_territoriales'")
            print("2. Verificar geometrías en mapa")
            print("3. Cargar datos de población y vacunación")
            print("4. ¡Usar en análisis geoespaciales! 🚀")
        else:
            print("\n❌ Carga fallida. Revisar errores arriba.")