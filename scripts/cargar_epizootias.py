#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
cargar_epizootias.py - Epizootias → PostgreSQL OPTIMIZADO
LIMPIO: Contexto municipal, datos geoespaciales, sin campos calculados
"""

import pandas as pd
import numpy as np
from datetime import datetime, date
from sqlalchemy import create_engine, text
import structlog
import sys
from pathlib import Path

# Añadir path para imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from config import (
    DatabaseConfig, FileConfig,
    limpiar_fecha_robusta, buscar_codigo_vereda, 
    buscar_codigo_municipio, normalizar_nombre_territorio
)

logger = structlog.get_logger()

# ================================
# MAPEO LOCAL EPIZOOTIAS EXCEL (LIMPIO)
# ================================
MAPEO_EPIZOOTIAS_EXCEL = {
    # Identificación territorial
    'municipio': 'MUNICIPIO',
    'vereda': 'VEREDA',
    
    # Datos temporales
    'fecha_recoleccion': 'FECHA_RECOLECCION',
    'fecha_notificacion': 'FECHA_NOTIFICACION',
    'fecha_envio_muestra': 'FECHA_ENVIO_MUESTRA',
    'fecha_resultado_pcr': 'FECHA_RESULTADO_PCR',
    'fecha_resultado_histopatologia': 'FECHA_RESULTADO_HISTOPATOLOGIA',
    
    # Información del evento
    'informante': 'INFORMANTE',
    'descripcion': 'DESCRIPCION',
    'especie': 'ESPECIE',
    
    # Geolocalización
    'latitud': 'LATITUD',
    'longitud': 'LONGITUD',
    
    # Resultados laboratorio
    'resultado_pcr': 'RESULTADO_PCR',
    'resultado_histopatologia': 'RESULTADO_HISTOPATOLOGIA'
}

def procesar_epizootias_optimizado(archivo_excel: Path) -> pd.DataFrame:
    """
    Procesa epizootias con contexto municipal y geolocalización
    LIMPIO: Datos originales preservados, validaciones robustas
    """
    logger.info("epizootias_processing_started", file_path=str(archivo_excel))
    
    inicio = datetime.now()
    
    try:
        # 1. CARGAR ARCHIVO EXCEL
        if not archivo_excel.exists():
            raise FileNotFoundError(f"Archivo no encontrado: {archivo_excel}")
        
        df = pd.read_excel(archivo_excel, sheet_name=0, dtype=str)
        logger.info("epizootias_file_loaded",
                   initial_records=len(df),
                   columns_available=list(df.columns))
        
        # 2. MAPEAR COLUMNAS DISPONIBLES
        columnas_mapeadas = {}
        columnas_faltantes = []
        
        for nombre_bd, nombre_excel in MAPEO_EPIZOOTIAS_EXCEL.items():
            if nombre_excel in df.columns:
                columnas_mapeadas[nombre_excel] = nombre_bd
            else:
                columnas_faltantes.append(nombre_excel)
        
        if columnas_faltantes:
            logger.warning("missing_columns", missing_columns=columnas_faltantes)
        
        # Renombrar y filtrar columnas
        df = df.rename(columns=columnas_mapeadas)
        columnas_finales = list(columnas_mapeadas.values())
        df = df[columnas_finales].copy()
        
        logger.info("columns_mapped", mapped_columns=len(columnas_finales))
        
        # 3. NORMALIZAR TERRITORIOS
        logger.info("normalizing_territories")
        
        # Normalizar municipios
        if 'municipio' in df.columns:
            df['municipio'] = df['municipio'].apply(
                lambda x: normalizar_nombre_territorio(x).title() if pd.notna(x) else None
            )
            municipios_unicos = df['municipio'].nunique()
            logger.info("municipalities_normalized", unique_count=municipios_unicos)
        
        # Normalizar veredas
        if 'vereda' in df.columns:
            df['vereda'] = df['vereda'].apply(
                lambda x: normalizar_nombre_territorio(x).title() if pd.notna(x) else None
            )
            veredas_unicas = df['vereda'].nunique()
            logger.info("veredas_normalized", unique_count=veredas_unicas)
        
        # 4. PROCESAR FECHAS
        logger.info("processing_dates")
        
        campos_fecha = [
            'fecha_recoleccion', 'fecha_notificacion', 'fecha_envio_muestra',
            'fecha_resultado_pcr', 'fecha_resultado_histopatologia'
        ]
        
        for campo in campos_fecha:
            if campo in df.columns:
                df[campo] = df[campo].apply(limpiar_fecha_robusta)
        
        # 5. PROCESAR COORDENADAS GEOGRÁFICAS
        logger.info("processing_coordinates")
        
        def limpiar_coordenada(coord_val):
            """Limpia y valida coordenadas"""
            if pd.isna(coord_val):
                return None
            
            try:
                coord_str = str(coord_val).strip().replace(',', '.')
                coord_float = float(coord_str)
                return coord_float
            except (ValueError, TypeError):
                return None
        
        # Limpiar coordenadas
        if 'latitud' in df.columns:
            df['latitud'] = df['latitud'].apply(limpiar_coordenada)
            
        if 'longitud' in df.columns:
            df['longitud'] = df['longitud'].apply(limpiar_coordenada)
        
        # Validar coordenadas para Colombia
        if 'latitud' in df.columns and 'longitud' in df.columns:
            coords_completas = df[['latitud', 'longitud']].dropna()
            
            # Filtro coordenadas válidas Colombia
            coordenadas_validas = (
                (df['latitud'].between(-4.2, 12.6)) &
                (df['longitud'].between(-81.8, -66.9))
            )
            
            coords_validas_count = coordenadas_validas.sum()
            logger.info("coordinates_validated",
                       complete_coordinates=len(coords_completas),
                       valid_coordinates=coords_validas_count)
        
        # 6. ASIGNAR CÓDIGOS DIVIPOLA CON CONTEXTO MUNICIPAL
        logger.info("assigning_divipola_codes_with_municipal_context")
        
        # Asignar código municipal
        if 'municipio' in df.columns:
            df['codigo_municipio'] = df['municipio'].apply(buscar_codigo_municipio)
            codigos_municipales = df['codigo_municipio'].notna().sum()
            logger.info("municipal_codes_assigned", count=codigos_municipales)
        
        # CRÍTICO: Asignar código veredal CON CONTEXTO MUNICIPAL
        if 'vereda' in df.columns and 'municipio' in df.columns:
            logger.info("mapping_veredas_with_municipal_context")
            
            df['codigo_vereda'] = df.apply(
                lambda row: buscar_codigo_vereda(
                    row.get('vereda'),
                    row.get('municipio')  # Contexto municipal para reducir búsqueda
                ), axis=1
            )
            
            codigos_veredales = df['codigo_vereda'].notna().sum()
            logger.info("vereda_codes_assigned", count=codigos_veredales)
            
            # Estadísticas de mapeo por municipio
            if codigos_veredales > 0:
                mapeo_stats = df.groupby('municipio').agg({
                    'vereda': 'count',
                    'codigo_vereda': 'count'
                }).rename(columns={
                    'vereda': 'total_veredas',
                    'codigo_vereda': 'veredas_mapeadas'
                })
                mapeo_stats['porcentaje_mapeo'] = (
                    mapeo_stats['veredas_mapeadas'] / mapeo_stats['total_veredas'] * 100
                ).round(1)
                
                logger.info("vereda_mapping_by_municipality", 
                           mapping_stats=mapeo_stats.to_dict('index'))
        
        # 7. NORMALIZAR ESPECIES Y RESULTADOS
        logger.info("normalizing_species_and_results")
        
        # Normalizar especies
        if 'especie' in df.columns:
            df['especie'] = df['especie'].apply(
                lambda x: str(x).strip().title() if pd.notna(x) else None
            )
            
            if df['especie'].notna().sum() > 0:
                especies_comunes = df['especie'].value_counts().head(3)
                logger.info("common_species", species_counts=especies_comunes.to_dict())
        
        # Normalizar resultados PCR
        if 'resultado_pcr' in df.columns:
            df['resultado_pcr'] = df['resultado_pcr'].apply(
                lambda x: str(x).strip().upper() if pd.notna(x) else None
            )
            
            if df['resultado_pcr'].notna().sum() > 0:
                resultados_pcr = df['resultado_pcr'].value_counts()
                logger.info("pcr_results", results_counts=resultados_pcr.to_dict())
        
        # 8. VALIDACIONES FINALES
        logger.info("applying_final_validations")
        
        registros_iniciales = len(df)
        
        # Filtrar registros con municipio válido
        if 'municipio' in df.columns:
            df = df.dropna(subset=['municipio'])
        
        # Filtrar fechas válidas para recolección
        if 'fecha_recoleccion' in df.columns:
            fecha_min = date(2020, 1, 1)
            fecha_max = date.today()
            
            df = df[
                (df['fecha_recoleccion'].isna()) | 
                ((df['fecha_recoleccion'] >= fecha_min) & 
                 (df['fecha_recoleccion'] <= fecha_max))
            ]
        
        logger.info("final_validation",
                   records_before=registros_iniciales,
                   records_after=len(df),
                   filtered_count=registros_iniciales - len(df))
        
        # 9. MANTENER DATOS ORIGINALES (SIN CAMPOS CALCULADOS)
        logger.info("preserving_original_data")
        
        # Metadatos únicamente
        df['created_at'] = datetime.now()
        df['updated_at'] = datetime.now()
        
        # 10. ESTADÍSTICAS FINALES
        duracion = datetime.now() - inicio
        
        estadisticas = {
            'final_records': len(df),
            'duration_seconds': duracion.total_seconds(),
            'municipalities_affected': df['municipio'].nunique() if 'municipio' in df.columns else 0,
            'species_reported': df['especie'].nunique() if 'especie' in df.columns else 0,
            'with_coordinates': len(df[['latitud', 'longitud']].dropna()) if all(col in df.columns for col in ['latitud', 'longitud']) else 0,
            'pcr_results': df['resultado_pcr'].notna().sum() if 'resultado_pcr' in df.columns else 0
        }
        
        # PCR positivos críticos
        if 'resultado_pcr' in df.columns:
            positivos = (df['resultado_pcr'] == 'POSITIVO').sum()
            if positivos > 0:
                estadisticas['pcr_positivos_criticos'] = positivos
        
        logger.info("epizootias_processing_completed", **estadisticas)
        
        return df
        
    except Exception as e:
        logger.error("epizootias_processing_failed", error=str(e))
        raise

def cargar_epizootias_postgresql_optimizado(df_epizootias: pd.DataFrame) -> bool:
    """
    Carga epizootias a PostgreSQL con soporte geoespacial optimizado
    """
    if df_epizootias is None or len(df_epizootias) == 0:
        logger.warning("no_epizootias_to_load")
        return False
    
    logger.info("epizootias_loading_to_postgresql", records=len(df_epizootias))
    
    try:
        engine = create_engine(DatabaseConfig.get_connection_url())
        
        # Verificar conexión
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        
        # Cargar datos principales
        df_epizootias.to_sql(
            'epizootias',
            engine,
            if_exists='replace',
            index=False,
            chunksize=100
        )
        
        # Optimizaciones PostGIS automáticas (trigger se encarga de punto_geografico)
        with engine.connect() as conn:
            # Crear índices optimizados
            indices_sql = [
                "CREATE INDEX IF NOT EXISTS idx_epizootias_municipio_optimizado ON epizootias(codigo_municipio)",
                "CREATE INDEX IF NOT EXISTS idx_epizootias_vereda_optimizado ON epizootias(codigo_vereda)",
                "CREATE INDEX IF NOT EXISTS idx_epizootias_fecha_optimizado ON epizootias(fecha_recoleccion)",
                "CREATE INDEX IF NOT EXISTS idx_epizootias_pcr_optimizado ON epizootias(resultado_pcr) WHERE resultado_pcr IS NOT NULL"
            ]
            
            for sql in indices_sql:
                try:
                    conn.execute(text(sql))
                    conn.commit()
                except Exception as e:
                    logger.warning("index_creation_failed", sql=sql[:50], error=str(e))
            
            # Estadísticas post-carga
            stats = conn.execute(text("""
                SELECT 
                    COUNT(*) as total_records,
                    COUNT(DISTINCT codigo_municipio) as municipios,
                    COUNT(DISTINCT especie) as especies,
                    COUNT(*) FILTER (WHERE punto_geografico IS NOT NULL) as con_coordenadas,
                    COUNT(*) FILTER (WHERE resultado_pcr IS NOT NULL) as con_pcr,
                    COUNT(*) FILTER (WHERE resultado_pcr = 'POSITIVO') as pcr_positivos,
                    COUNT(*) FILTER (WHERE codigo_vereda IS NOT NULL) as con_codigo_veredal,
                    MIN(fecha_recoleccion) as fecha_min,
                    MAX(fecha_recoleccion) as fecha_max
                FROM epizootias
            """)).fetchone()
            
            if stats:
                s = dict(stats)
                logger.info("epizootias_loading_completed", **s)
                
                # Alerta PCR positivos
                if s['pcr_positivos'] > 0:
                    logger.warning("pcr_positivos_detected", count=s['pcr_positivos'])
        
        return True
        
    except Exception as e:
        logger.error("epizootias_loading_failed", error=str(e))
        return False

def procesar_epizootias_completo(archivo_excel: Path = None) -> bool:
    """
    Proceso completo optimizado: Excel → Procesamiento → PostgreSQL
    """
    logger.info("epizootias_complete_process_started")
    
    if archivo_excel is None:
        archivo_excel = FileConfig.EPIZOOTIAS_FILE
    
    try:
        # 1. Procesar epizootias
        df_epizootias = procesar_epizootias_optimizado(archivo_excel)
        
        if df_epizootias is None or len(df_epizootias) == 0:
            logger.error("epizootias_processing_returned_empty")
            return False
        
        # 2. Cargar a PostgreSQL
        exito = cargar_epizootias_postgresql_optimizado(df_epizootias)
        
        if exito:
            logger.info("epizootias_complete_process_successful")
            
            # 3. Crear backup
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            backup_file = FileConfig.BACKUPS_DIR / f"epizootias_backup_{timestamp}.csv"
            FileConfig.create_directories()
            
            df_epizootias.to_csv(backup_file, index=False, encoding='utf-8-sig')
            logger.info("epizootias_backup_created", backup_file=str(backup_file))
        
        return exito
        
    except Exception as e:
        logger.error("epizootias_complete_process_failed", error=str(e))
        return False

def generar_reporte_epizootias() -> bool:
    """
    Genera reporte epidemiológico de epizootias
    """
    logger.info("generating_epizootias_report")
    
    try:
        engine = create_engine(DatabaseConfig.get_connection_url())
        
        with engine.connect() as conn:
            # Resumen epizootias
            resumen = conn.execute(text("""
                SELECT 
                    COUNT(*) as total_epizootias,
                    COUNT(DISTINCT municipio) as municipios_afectados,
                    COUNT(DISTINCT especie) as especies_afectadas,
                    COUNT(*) FILTER (WHERE punto_geografico IS NOT NULL) as con_geolocalizacion,
                    COUNT(*) FILTER (WHERE resultado_pcr = 'POSITIVO') as pcr_positivos,
                    COUNT(*) FILTER (WHERE codigo_vereda IS NOT NULL) as con_codigo_veredal,
                    MIN(fecha_recoleccion) as primera_epizooti,
                    MAX(fecha_recoleccion) as ultima_epizooti
                FROM epizootias
            """)).fetchone()
            
            if resumen:
                r = dict(resumen)
                logger.info("epizootias_epidemiological_report", **r)
        
        return True
        
    except Exception as e:
        logger.error("epizootias_report_failed", error=str(e))
        return False

# ================================
# FUNCIÓN PRINCIPAL
# ================================
if __name__ == "__main__":
    print("🐒 CARGA EPIZOOTIAS OPTIMIZADA")
    print("=" * 40)
    
    archivo_default = FileConfig.EPIZOOTIAS_FILE
    
    if not archivo_default.exists():
        print(f"❌ ERROR: Archivo no encontrado: {archivo_default}")
        print("💡 Colocar archivo en data/epizootias.xlsx")
        sys.exit(1)
    
    # Ejecutar proceso completo
    exito = procesar_epizootias_completo(archivo_default)
    
    if exito:
        print("✅ Epizootias cargadas exitosamente")
        generar_reporte_epizootias()
        print("\n🎯 Epizootias listas para vigilancia epidemiológica")
    else:
        print("❌ Error en carga de epizootias")