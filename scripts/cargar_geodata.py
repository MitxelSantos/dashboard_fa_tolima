#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
cargar_geodata.py - Unidades Territoriales → PostgreSQL OPTIMIZADO
LIMPIO: PostGIS optimizado, validaciones robustas, performance mejorado
"""

import geopandas as gpd
import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime
import structlog
import sys
from pathlib import Path

# Añadir path para imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from config import DatabaseConfig, FileConfig

logger = structlog.get_logger()

def cargar_unidades_territoriales_postgresql(archivo_gpkg: Path = None) -> bool:
    """
    Carga unidades territoriales desde .gpkg a PostgreSQL optimizado
    LIMPIO: Validaciones robustas, índices optimizados, logging estructurado
    """
    if archivo_gpkg is None:
        archivo_gpkg = FileConfig.TERRITORIOS_FILE
    
    logger.info("territories_loading_started", file_path=str(archivo_gpkg))
    
    inicio = datetime.now()
    
    try:
        # 1. VERIFICAR ARCHIVO
        if not archivo_gpkg.exists():
            raise FileNotFoundError(f"Archivo no encontrado: {archivo_gpkg}")
        
        file_size_mb = archivo_gpkg.stat().st_size / 1024 / 1024
        logger.info("file_validated", size_mb=round(file_size_mb, 2))
        
        # 2. CARGAR GEODATAFRAME
        logger.info("loading_geopackage")
        gdf = gpd.read_file(archivo_gpkg)
        
        logger.info("geopackage_loaded",
                   records=len(gdf),
                   columns=list(gdf.columns),
                   crs=str(gdf.crs))
        
        # 3. VALIDAR ESTRUCTURA CRÍTICA
        logger.info("validating_structure")
        
        required_columns = ['tipo', 'codigo_divipola', 'nombre']
        missing_columns = [col for col in required_columns if col not in gdf.columns]
        
        if missing_columns:
            raise ValueError(f"Columnas críticas faltantes: {missing_columns}")
        
        # Verificar tipos únicos
        tipos_unicos = sorted(gdf['tipo'].unique())
        logger.info("territory_types", types=tipos_unicos)
        
        # Verificar códigos DIVIPOLA
        codigos_divipola = gdf['codigo_divipola'].nunique()
        logger.info("divipola_codes", unique_codes=codigos_divipola)
        
        # 4. PREPARAR DATOS PARA POSTGRESQL
        logger.info("preparing_data_for_postgresql")
        
        # Asegurar CRS correcto (EPSG:4326)
        if gdf.crs != 'EPSG:4326':
            logger.info("converting_crs", from_crs=str(gdf.crs), to_crs="EPSG:4326")
            gdf = gdf.to_crs('EPSG:4326')
        
        # Renombrar geometría para PostGIS
        if gdf.geometry.name != 'geometria':
            gdf = gdf.rename_geometry('geometria')
        
        # Limpiar datos nulos
        gdf = gdf.where(pd.notnull(gdf), None)
        
        # Validar geometrías
        geometrias_validas = gdf.geometria.is_valid.sum()
        geometrias_invalidas = len(gdf) - geometrias_validas
        
        if geometrias_invalidas > 0:
            logger.warning("invalid_geometries", 
                          invalid_count=geometrias_invalidas,
                          valid_count=geometrias_validas)
            
            # Intentar reparar geometrías inválidas
            gdf.loc[~gdf.geometria.is_valid, 'geometria'] = gdf.loc[~gdf.geometria.is_valid, 'geometria'].buffer(0)
            
            # Verificar después de reparación
            geometrias_validas_post = gdf.geometria.is_valid.sum()
            logger.info("geometries_repaired", valid_after_repair=geometrias_validas_post)
        
        # 5. CONECTAR Y CARGAR A POSTGRESQL
        logger.info("connecting_to_postgresql")
        engine = create_engine(DatabaseConfig.get_connection_url())
        
        # Verificar conexión y extensiones
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
            
            # Verificar PostGIS
            postgis_version = conn.execute(text("SELECT PostGIS_Version()")).scalar()
            logger.info("postgis_verified", version=postgis_version)
        
        # 6. CARGAR CON to_postgis OPTIMIZADO
        logger.info("loading_to_postgis", records=len(gdf))
        
        gdf.to_postgis(
            'unidades_territoriales',
            engine,
            if_exists='replace',
            index=False,
            chunksize=1000  # Chunks optimizados para PostGIS
        )
        
        # 7. POST-PROCESAMIENTO Y OPTIMIZACIONES
        logger.info("applying_post_processing_optimizations")
        
        with engine.connect() as conn:
            # Crear índices espaciales críticos
            indices_espaciales = [
                "CREATE INDEX IF NOT EXISTS idx_territorios_geom_optimizado ON unidades_territoriales USING GIST(geometria)",
                "CREATE INDEX IF NOT EXISTS idx_territorios_divipola_unique ON unidades_territoriales(codigo_divipola)",
                "CREATE INDEX IF NOT EXISTS idx_territorios_tipo_region ON unidades_territoriales(tipo, region)",
                "CREATE INDEX IF NOT EXISTS idx_territorios_municipio_lookup ON unidades_territoriales(codigo_municipio) WHERE tipo = 'municipio'"
            ]
            
            for sql in indices_espaciales:
                try:
                    conn.execute(text(sql))
                    conn.commit()
                except Exception as e:
                    logger.warning("index_creation_failed", sql=sql[:50], error=str(e))
            
            # Actualizar estadísticas espaciales
            conn.execute(text("ANALYZE unidades_territoriales"))
            conn.commit()
            
            logger.info("spatial_indexes_created")
        
        # 8. VERIFICAR CARGA Y GENERAR ESTADÍSTICAS
        logger.info("verifying_load_and_generating_statistics")
        
        with engine.connect() as conn:
            # Contar registros totales
            total_records = conn.execute(text("SELECT COUNT(*) FROM unidades_territoriales")).scalar()
            
            # Estadísticas por tipo
            stats_tipo = pd.read_sql(text("""
                SELECT 
                    tipo, 
                    COUNT(*) as cantidad,
                    ROUND(AVG(area_oficial_km2), 2) as area_promedio_km2
                FROM unidades_territoriales 
                WHERE area_oficial_km2 IS NOT NULL
                GROUP BY tipo 
                ORDER BY cantidad DESC
            """), conn)
            
            # Estadísticas por región
            stats_region = pd.read_sql(text("""
                SELECT 
                    region, 
                    COUNT(*) as cantidad
                FROM unidades_territoriales 
                WHERE region IS NOT NULL 
                GROUP BY region 
                ORDER BY cantidad DESC
            """), conn)
            
            # Verificar geometrías válidas en BD
            geom_validas = conn.execute(text("""
                SELECT COUNT(*) 
                FROM unidades_territoriales 
                WHERE geometria IS NOT NULL AND ST_IsValid(geometria)
            """)).scalar()
            
            # Verificar extensión geográfica
            bbox = conn.execute(text("""
                SELECT 
                    ST_XMin(ST_Extent(geometria)) as min_lon,
                    ST_YMin(ST_Extent(geometria)) as min_lat,
                    ST_XMax(ST_Extent(geometria)) as max_lon,
                    ST_YMax(ST_Extent(geometria)) as max_lat
                FROM unidades_territoriales
                WHERE geometria IS NOT NULL
            """)).fetchone()
            
            estadisticas_finales = {
                'total_records': total_records,
                'valid_geometries': geom_validas,
                'types_distribution': stats_tipo.to_dict('records'),
                'regions_distribution': stats_region.to_dict('records'),
                'geographic_extent': dict(bbox) if bbox else None
            }
            
            logger.info("territories_loading_completed", **estadisticas_finales)
        
        # 9. CREAR BACKUP OPCIONAL
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_file = FileConfig.BACKUPS_DIR / f"territories_backup_{timestamp}.gpkg"
        
        try:
            FileConfig.create_directories()
            gdf.to_file(backup_file, driver="GPKG")
            logger.info("backup_created", backup_file=str(backup_file))
        except Exception as e:
            logger.warning("backup_creation_failed", error=str(e))
        
        # 10. ESTADÍSTICAS FINALES
        duracion = datetime.now() - inicio
        
        logger.info("territories_process_completed",
                   duration_seconds=duracion.total_seconds(),
                   records_per_second=round(total_records / duracion.total_seconds(), 2))
        
        return True
        
    except Exception as e:
        logger.error("territories_loading_failed", error=str(e))
        return False

def verificar_integridad_territorial() -> bool:
    """
    Verifica integridad de datos territoriales cargados
    """
    logger.info("verifying_territorial_integrity")
    
    try:
        engine = create_engine(DatabaseConfig.get_connection_url())
        
        with engine.connect() as conn:
            # Verificaciones críticas
            verificaciones = {
                "total_unidades": "SELECT COUNT(*) FROM unidades_territoriales",
                "sin_codigo_divipola": "SELECT COUNT(*) FROM unidades_territoriales WHERE codigo_divipola IS NULL",
                "sin_geometria": "SELECT COUNT(*) FROM unidades_territoriales WHERE geometria IS NULL",
                "geometrias_invalidas": "SELECT COUNT(*) FROM unidades_territoriales WHERE NOT ST_IsValid(geometria)",
                "municipios_tolima": "SELECT COUNT(*) FROM unidades_territoriales WHERE tipo='municipio' AND codigo_divipola LIKE '73%'"
            }
            
            resultados = {}
            problemas_encontrados = []
            
            for nombre, query in verificaciones.items():
                try:
                    resultado = conn.execute(text(query)).scalar()
                    resultados[nombre] = resultado
                    
                    # Verificar problemas críticos
                    if nombre in ['sin_codigo_divipola', 'sin_geometria', 'geometrias_invalidas'] and resultado > 0:
                        problemas_encontrados.append(f"{nombre}: {resultado}")
                        
                except Exception as e:
                    logger.error("verification_query_failed", query=nombre, error=str(e))
                    resultados[nombre] = "ERROR"
            
            logger.info("territorial_integrity_results", **resultados)
            
            if problemas_encontrados:
                logger.warning("integrity_issues_found", issues=problemas_encontrados)
                return False
            
            # Verificar municipios esperados del Tolima
            municipios_principales = [
                "IBAGUÉ", "MARIQUITA", "ESPINAL", "HONDA", "FLANDES"
            ]
            
            municipios_encontrados = pd.read_sql(text("""
                SELECT UPPER(nombre) as nombre_upper 
                FROM unidades_territoriales 
                WHERE tipo = 'municipio' AND codigo_divipola LIKE '73%'
            """), conn)
            
            municipios_encontrados_list = municipios_encontrados['nombre_upper'].tolist()
            municipios_faltantes = set(municipios_principales) - set(municipios_encontrados_list)
            
            if municipios_faltantes:
                logger.warning("missing_expected_municipalities", missing=list(municipios_faltantes))
            else:
                logger.info("main_municipalities_verified")
        
        return len(problemas_encontrados) == 0
        
    except Exception as e:
        logger.error("territorial_integrity_verification_failed", error=str(e))
        return False

def generar_consultas_territoriales_muestra() -> bool:
    """
    Genera y ejecuta consultas de muestra para probar datos territoriales
    """
    logger.info("generating_sample_territorial_queries")
    
    consultas_muestra = {
        "resumen_por_tipo": """
            SELECT 
                tipo, 
                COUNT(*) as cantidad,
                ROUND(AVG(area_oficial_km2), 2) as area_promedio_km2,
                ROUND(SUM(area_oficial_km2), 2) as area_total_km2
            FROM unidades_territoriales 
            WHERE area_oficial_km2 IS NOT NULL
            GROUP BY tipo 
            ORDER BY cantidad DESC
        """,
        
        "municipios_por_region": """
            SELECT 
                region, 
                COUNT(*) as municipios,
                ROUND(SUM(area_oficial_km2), 2) as area_total_km2
            FROM unidades_territoriales 
            WHERE tipo = 'municipio' AND region IS NOT NULL
            GROUP BY region 
            ORDER BY area_total_km2 DESC
        """,
        
        "top_municipios_mas_grandes": """
            SELECT 
                nombre, 
                area_oficial_km2, 
                region
            FROM unidades_territoriales 
            WHERE tipo = 'municipio' AND area_oficial_km2 IS NOT NULL
            ORDER BY area_oficial_km2 DESC 
            LIMIT 10
        """,
        
        "verificacion_cobertura_geografica": """
            SELECT 
                ST_XMin(ST_Extent(geometria)) as min_longitud,
                ST_YMin(ST_Extent(geometria)) as min_latitud,
                ST_XMax(ST_Extent(geometria)) as max_longitud,
                ST_YMax(ST_Extent(geometria)) as max_latitud,
                ROUND(ST_Area(ST_Extent(geometria))::numeric, 6) as area_extent_grados
            FROM unidades_territoriales 
            WHERE geometria IS NOT NULL
        """
    }
    
    try:
        engine = create_engine(DatabaseConfig.get_connection_url())
        
        resultados_consultas = {}
        
        for nombre, query in consultas_muestra.items():
            try:
                resultado = pd.read_sql(text(query), engine)
                resultados_consultas[nombre] = resultado.to_dict('records')
                
                logger.info("sample_query_executed",
                           query_name=nombre,
                           rows_returned=len(resultado))
                
            except Exception as e:
                logger.error("sample_query_failed", 
                           query_name=nombre,
                           error=str(e))
                resultados_consultas[nombre] = "ERROR"
        
        logger.info("sample_queries_completed", results_summary=list(resultados_consultas.keys()))
        
        return True
        
    except Exception as e:
        logger.error("sample_queries_generation_failed", error=str(e))
        return False

# ================================
# FUNCIÓN PRINCIPAL
# ================================
if __name__ == "__main__":
    print("🗺️ CARGA UNIDADES TERRITORIALES OPTIMIZADA")
    print("=" * 55)
    
    archivo_default = FileConfig.TERRITORIOS_FILE
    
    if not archivo_default.exists():
        print(f"❌ ERROR: Archivo no encontrado: {archivo_default}")
        print("💡 Colocar archivo .gpkg en data/tolima_cabeceras_veredas.gpkg")
        sys.exit(1)
    
    # Ejecutar carga completa
    exito = cargar_unidades_territoriales_postgresql(archivo_default)
    
    if exito:
        print("✅ Unidades territoriales cargadas exitosamente")
        
        # Verificaciones adicionales
        print("\n🔧 Ejecutando verificaciones...")
        if verificar_integridad_territorial():
            print("✅ Integridad territorial verificada")
        else:
            print("⚠️ Problemas de integridad encontrados")
        
        print("\n📊 Generando consultas de muestra...")
        if generar_consultas_territoriales_muestra():
            print("✅ Consultas de muestra ejecutadas")
        
        print("\n🎯 Unidades territoriales listas para análisis geoespaciales")
    else:
        print("❌ Error en carga de unidades territoriales")