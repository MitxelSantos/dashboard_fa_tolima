#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
cargar_casos.py - Casos Fiebre Amarilla → PostgreSQL OPTIMIZADO
LIMPIO: Municipio procedencia (nmun_proce), edad con fecha actual, mapeos locales
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
    clasificar_grupo_etario, calcular_edad_en_meses,
    limpiar_fecha_robusta, buscar_codigo_vereda, 
    buscar_codigo_municipio, normalizar_nombre_territorio
)

logger = structlog.get_logger()

# =======================
# MAPEO LOCAL CASOS EXCEL
# =======================
MAPEO_CASOS_EXCEL = {
    # Identificación temporal
    'fecha_notificacion': 'fec_not',
    'semana_epidemiologica': 'semana',
    'fecha_inicio_sintomas': 'ini_sin_',
    'fecha_consulta': 'fec_con_',
    
    # Identificación personal
    'primer_nombre': 'pri_nom_',
    'primer_apellido': 'pri_ape_',
    'tipo_documento': 'tip_ide_',
    'numero_documento': 'num_ide_',
    'edad': 'edad_',
    'sexo': 'sexo_',
    'fecha_nacimiento': 'fecha_nto_',
    
    # Geolocalización CORREGIDA
    'municipio_procedencia': 'nmun_proce',    # DONDE SE INFECTÓ (CRÍTICO)
    'vereda_infeccion': 'vereda_',            # Vereda donde se infectó
    'municipio_residencia': 'nmun_resi',      # Donde vive
    'municipio_notificacion': 'nmun_notif',   # Donde se notificó
    
    # Datos clínicos críticos
    'clasificacion_inicial': 'tip_cas_',
    'condicion_final': 'con_fin_',
    'fecha_defuncion': 'fec_def_',
    'hospitalizado': 'pac_hos_',
    'fecha_hospitalizacion': 'fec_hos_',
    
    # Antecedentes vacunación
    'vacunado_previo': 'carne_vacu',
    'fecha_vacunacion_previa': 'fec_fa1_',
    
    # Síntomas principales (críticos para vigilancia)
    'fiebre': 'fiebre',
    'ictericia': 'ictericia',
    'sangrado': 'sfaget'
}

def procesar_casos_fiebre_amarilla_optimizado(archivo_excel: Path) -> pd.DataFrame:
    """
    Procesa casos de fiebre amarilla con mapeo optimizado
    LIMPIO: Municipio procedencia, edad actual, validaciones robustas
    """
    logger.info("casos_processing_started", file_path=str(archivo_excel))
    
    inicio = datetime.now()
    
    try:
        # 1. CARGAR ARCHIVO EXCEL
        if not archivo_excel.exists():
            raise FileNotFoundError(f"Archivo no encontrado: {archivo_excel}")
        
        # Leer primera hoja Excel
        df = pd.read_excel(archivo_excel, sheet_name=0, dtype=str)
        logger.info("casos_file_loaded", 
                   initial_records=len(df),
                   columns_count=len(df.columns))
        
        # 2. MAPEAR COLUMNAS DISPONIBLES
        columnas_mapeadas = {}
        columnas_faltantes = []
        
        for nombre_bd, nombre_excel in MAPEO_CASOS_EXCEL.items():
            if nombre_excel in df.columns:
                columnas_mapeadas[nombre_excel] = nombre_bd
            else:
                columnas_faltantes.append(nombre_excel)
        
        if columnas_faltantes:
            logger.warning("missing_columns", missing_columns=columnas_faltantes[:5])
        
        # Renombrar y filtrar columnas
        df = df.rename(columns=columnas_mapeadas)
        columnas_finales = list(columnas_mapeadas.values())
        df = df[columnas_finales].copy()
        
        logger.info("columns_mapped", mapped_columns=len(columnas_finales))
        
        # 3. PROCESAR FECHAS CRÍTICAS
        logger.info("processing_dates")
        
        campos_fecha = [
            'fecha_notificacion', 'fecha_inicio_sintomas', 'fecha_consulta',
            'fecha_defuncion', 'fecha_hospitalizacion', 'fecha_vacunacion_previa',
            'fecha_nacimiento'
        ]
        
        for campo in campos_fecha:
            if campo in df.columns:
                df[campo] = df[campo].apply(limpiar_fecha_robusta)
        
        # Filtrar registros sin fecha notificación
        df = df.dropna(subset=['fecha_notificacion'])
        logger.info("date_filtering", records_after_date_filter=len(df))
        
        # 4. CALCULAR EDAD CON FECHA ACTUAL (CORREGIDO)
        logger.info("calculating_age_with_current_date")
        
        if 'fecha_nacimiento' in df.columns:
            fecha_actual = date.today()
            
            # Calcular edad usando SOLO fecha actual
            df['edad_meses_calculada'] = df['fecha_nacimiento'].apply(
                lambda x: calcular_edad_en_meses(x, fecha_actual) if pd.notna(x) else None
            )
            df['edad_anos_calculada'] = df['edad_meses_calculada'] / 12
            
            # Usar edad calculada o edad del archivo
            df['edad_anos'] = df['edad_anos_calculada'].fillna(
                pd.to_numeric(df.get('edad', np.nan), errors='coerce')
            )
            
            logger.info("age_calculated", reference_date=str(fecha_actual))
        else:
            df['edad_anos'] = pd.to_numeric(df.get('edad', np.nan), errors='coerce')
        
        # 5. CLASIFICAR GRUPOS ETARIOS
        logger.info("classifying_age_groups")
        
        df['edad_meses_para_clasificacion'] = df['edad_anos'] * 12
        df['grupo_etario'] = df['edad_meses_para_clasificacion'].apply(clasificar_grupo_etario)
        
        # 6. PROCESAR GEOLOCALIZACIÓN CON MUNICIPIO PROCEDENCIA
        logger.info("processing_geolocation_with_procedencia")
        
        # Normalizar nombres municipios
        for campo_mun in ['municipio_procedencia', 'municipio_residencia', 'municipio_notificacion']:
            if campo_mun in df.columns:
                df[campo_mun] = df[campo_mun].apply(
                    lambda x: normalizar_nombre_territorio(x).title() if pd.notna(x) else None
                )
        
        # Asignar códigos municipales
        if 'municipio_procedencia' in df.columns:
            df['codigo_municipio_procedencia'] = df['municipio_procedencia'].apply(buscar_codigo_municipio)
        
        if 'municipio_residencia' in df.columns:
            df['codigo_municipio_residencia'] = df['municipio_residencia'].apply(buscar_codigo_municipio)
        
        if 'municipio_notificacion' in df.columns:
            df['codigo_municipio_notificacion'] = df['municipio_notificacion'].apply(buscar_codigo_municipio)
        
        # CRÍTICO: Mapear vereda con contexto municipio procedencia
        if 'vereda_infeccion' in df.columns and 'municipio_procedencia' in df.columns:
            logger.info("mapping_veredas_with_procedencia_context")
            
            df['codigo_vereda_infeccion'] = df.apply(
                lambda row: buscar_codigo_vereda(
                    row.get('vereda_infeccion'),
                    row.get('municipio_procedencia')  # Contexto municipal donde se infectó
                ), axis=1
            )
            
            veredas_mapeadas = df['codigo_vereda_infeccion'].notna().sum()
            logger.info("veredas_mapped", count=veredas_mapeadas)
        
        # 7. NORMALIZAR CAMPOS CATEGÓRICOS
        logger.info("normalizing_categorical_fields")
        
        # Normalizar condición final
        if 'condicion_final' in df.columns:
            df['condicion_final'] = df['condicion_final'].map({
                1: 'Vivo', '1': 'Vivo',
                2: 'Muerto', '2': 'Muerto'
            }).fillna(df['condicion_final'])
        
        # Normalizar vacunación previa
        if 'vacunado_previo' in df.columns:
            df['vacunado_previo'] = df['vacunado_previo'].map({
                1: True, '1': True,
                2: False, '2': False
            }).fillna(None)
        
        # Normalizar hospitalización
        if 'hospitalizado' in df.columns:
            df['hospitalizado'] = df['hospitalizado'].map({
                1: True, '1': True,
                2: False, '2': False
            }).fillna(None)
        
        # Normalizar síntomas (1=Sí, 2=No)
        sintomas = ['fiebre', 'ictericia', 'sangrado']
        for sintoma in sintomas:
            if sintoma in df.columns:
                df[sintoma] = df[sintoma].map({
                    1: True, '1': True,
                    2: False, '2': False
                }).fillna(None)
        
        # 8. VALIDACIONES FINALES
        logger.info("applying_final_validations")
        
        registros_iniciales = len(df)
        
        # Filtrar fechas coherentes
        fecha_min = date(2020, 1, 1)
        fecha_max = date.today()
        
        df = df[
            (df['fecha_notificacion'] >= fecha_min) &
            (df['fecha_notificacion'] <= fecha_max)
        ]
        
        # Filtrar edades válidas
        df = df[
            (df['edad_anos'].isna()) | 
            ((df['edad_anos'] >= 0) & (df['edad_anos'] <= 120))
        ]
        
        logger.info("final_validation", 
                   records_before=registros_iniciales,
                   records_after=len(df),
                   filtered_count=registros_iniciales - len(df))
        
        # 9. CAMPOS CALCULADOS FINALES
        if 'fecha_notificacion' in df.columns:
            df['año'] = df['fecha_notificacion'].dt.year
            df['semana_epidemiologica'] = df['fecha_notificacion'].dt.isocalendar().week
        
        # Metadatos
        df['created_at'] = datetime.now()
        df['updated_at'] = datetime.now()
        
        # 10. ESTADÍSTICAS FINALES
        duracion = datetime.now() - inicio
        
        logger.info("casos_processing_completed",
                   final_records=len(df),
                   duration_seconds=duracion.total_seconds(),
                   municipalities_procedencia=df['municipio_procedencia'].nunique() if 'municipio_procedencia' in df.columns else 0,
                   years_span=sorted(df['año'].dropna().unique()) if 'año' in df.columns else [])
        
        return df
        
    except Exception as e:
        logger.error("casos_processing_failed", error=str(e))
        raise

def cargar_casos_postgresql_optimizado(df_casos: pd.DataFrame) -> bool:
    """
    Carga casos a PostgreSQL con validaciones optimizadas
    """
    if df_casos is None or len(df_casos) == 0:
        logger.warning("no_casos_to_load")
        return False
    
    logger.info("casos_loading_to_postgresql", records=len(df_casos))
    
    try:
        engine = create_engine(DatabaseConfig.get_connection_url())
        
        # Verificar conexión
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        
        # Cargar datos
        df_casos.to_sql(
            'casos_fiebre_amarilla',
            engine,
            if_exists='replace',
            index=False,
            chunksize=500
        )
        
        # Verificar carga y generar estadísticas
        with engine.connect() as conn:
            total_cargado = conn.execute(text("SELECT COUNT(*) FROM casos_fiebre_amarilla")).scalar()
            
            # Estadísticas críticas
            stats = conn.execute(text("""
                SELECT 
                    COUNT(DISTINCT municipio_procedencia) as municipios_procedencia,
                    COUNT(DISTINCT año) as años_casos,
                    COUNT(*) FILTER (WHERE condicion_final = 'Muerto') as defunciones,
                    COUNT(*) FILTER (WHERE codigo_vereda_infeccion IS NOT NULL) as con_codigo_veredal,
                    MIN(fecha_notificacion) as fecha_min,
                    MAX(fecha_notificacion) as fecha_max
                FROM casos_fiebre_amarilla
                WHERE fecha_notificacion IS NOT NULL
            """)).fetchone()
            
            if stats:
                s = dict(stats)
                logger.info("casos_loading_completed",
                           total_loaded=total_cargado,
                           **s)
            
        return True
        
    except Exception as e:
        logger.error("casos_loading_failed", error=str(e))
        return False

def procesar_casos_completo(archivo_excel: Path = None) -> bool:
    """
    Proceso completo optimizado: Excel → Procesamiento → PostgreSQL
    """
    logger.info("casos_complete_process_started")
    
    if archivo_excel is None:
        archivo_excel = FileConfig.CASOS_FILE
    
    try:
        # 1. Procesar casos
        df_casos = procesar_casos_fiebre_amarilla_optimizado(archivo_excel)
        
        if df_casos is None or len(df_casos) == 0:
            logger.error("casos_processing_returned_empty")
            return False
        
        # 2. Cargar a PostgreSQL
        exito = cargar_casos_postgresql_optimizado(df_casos)
        
        if exito:
            logger.info("casos_complete_process_successful")
            
            # 3. Crear backup
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            backup_file = FileConfig.BACKUPS_DIR / f"casos_backup_{timestamp}.csv"
            FileConfig.create_directories()
            
            df_casos.to_csv(backup_file, index=False, encoding='utf-8-sig')
            logger.info("casos_backup_created", backup_file=str(backup_file))
        
        return exito
        
    except Exception as e:
        logger.error("casos_complete_process_failed", error=str(e))
        return False

def generar_reporte_casos() -> bool:
    """
    Genera reporte epidemiológico de casos
    """
    logger.info("generating_casos_report")
    
    try:
        engine = create_engine(DatabaseConfig.get_connection_url())
        
        with engine.connect() as conn:
            # Resumen epidemiológico
            resumen = conn.execute(text("""
                SELECT 
                    COUNT(*) as total_casos,
                    COUNT(DISTINCT municipio_procedencia) as municipios_afectados,
                    COUNT(*) FILTER (WHERE condicion_final = 'Muerto') as defunciones,
                    COUNT(*) FILTER (WHERE vacunado_previo = true) as vacunados_previos,
                    MIN(fecha_notificacion) as primer_caso,
                    MAX(fecha_notificacion) as ultimo_caso
                FROM casos_fiebre_amarilla
                WHERE fecha_notificacion IS NOT NULL
            """)).fetchone()
            
            if resumen:
                r = dict(resumen)
                letalidad = (r['defunciones'] / r['total_casos'] * 100) if r['total_casos'] > 0 else 0
                
                logger.info("casos_epidemiological_report",
                           **r,
                           letalidad_porcentaje=round(letalidad, 1))
        
        return True
        
    except Exception as e:
        logger.error("casos_report_failed", error=str(e))
        return False

# ================================
# FUNCIÓN PRINCIPAL
# ================================
if __name__ == "__main__":
    print("🦠 CARGA CASOS FIEBRE AMARILLA OPTIMIZADA")
    print("=" * 50)
    
    archivo_default = FileConfig.CASOS_FILE
    
    if not archivo_default.exists():
        print(f"❌ ERROR: Archivo no encontrado: {archivo_default}")
        print("💡 Colocar archivo en data/casos.xlsx")
        sys.exit(1)
    
    # Ejecutar proceso completo
    exito = procesar_casos_completo(archivo_default)
    
    if exito:
        print("✅ Casos cargados exitosamente")
        generar_reporte_casos()
        print("\n🎯 Casos listos para vigilancia epidemiológica")
    else:
        print("❌ Error en carga de casos")