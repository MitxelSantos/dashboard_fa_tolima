#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
cargar_poblacion.py - Procesamiento Integrado Población SISBEN → PostgreSQL
Integra la lógica completa de poblacion.py + carga directa a PostgreSQL
"""

import pandas as pd
import numpy as np
from datetime import datetime, date
from dateutil.relativedelta import relativedelta
from sqlalchemy import create_engine, text
import warnings
import os
import sys

# Importar configuración centralizada
from config import (
    DATABASE_URL, MAPEO_POBLACION_SISBEN,
    clasificar_grupo_etario, calcular_edad_en_meses, 
    determinar_ubicacion_urbano_rural, limpiar_fecha_robusta,
    buscar_codigo_municipio, normalizar_nombre_territorio
)

warnings.filterwarnings('ignore')

def cargar_poblacion_sisben_sin_headers(archivo_csv):
    """
    Carga CSV de población SISBEN sin headers
    """
    print("👥 PROCESANDO POBLACIÓN SISBEN DESDE CSV SIN HEADERS")
    print("=" * 60)
    
    inicio = datetime.now()
    
    try:
        # 1. CARGAR CSV SIN HEADERS
        print(f"📂 Cargando: {archivo_csv}")
        
        df = pd.read_csv(archivo_csv, header=None)
        print(f"📊 Registros iniciales: {len(df):,}")
        print(f"📋 Columnas detectadas: {len(df.columns)}")
        
        # 2. ASIGNAR NOMBRES A COLUMNAS
        df.columns = [f"col_{i}" for i in range(df.shape[1])]
        
        # Mapear columnas usando configuración centralizada
        columnas_mapeadas = {}
        for nombre_bd, indice in MAPEO_POBLACION_SISBEN.items():
            columna_excel = f"col_{indice}"
            if columna_excel in df.columns:
                columnas_mapeadas[columna_excel] = nombre_bd
                print(f"   ✅ col_{indice} → {nombre_bd}")
        
        df = df.rename(columns=columnas_mapeadas)
        
        # Verificar que tenemos las columnas esenciales
        columnas_esenciales = ['codigo_municipio', 'municipio', 'fecha_nacimiento', 'documento']
        columnas_faltantes = [col for col in columnas_esenciales if col not in df.columns]
        
        if columnas_faltantes:
            print(f"❌ ERROR: Columnas esenciales faltantes: {columnas_faltantes}")
            return None
        
        # 3. VALIDAR Y LIMPIAR FECHAS DE NACIMIENTO
        print("📅 Validando fechas de nacimiento...")
        
        df['fecha_nacimiento'] = df['fecha_nacimiento'].apply(limpiar_fecha_robusta)
        
        fechas_nulas = df['fecha_nacimiento'].isna().sum()
        print(f"   Fechas nulas/inválidas: {fechas_nulas:,}")
        
        # Filtrar registros con fecha válida
        df_limpio = df.dropna(subset=['fecha_nacimiento'])
        
        # Validar fechas coherentes
        fecha_actual = pd.Timestamp.now()
        fechas_futuras = df_limpio[df_limpio['fecha_nacimiento'] > fecha_actual.date()]
        
        if len(fechas_futuras) > 0:
            print(f"   ⚠️ Excluidas {len(fechas_futuras)} fechas futuras")
            df_limpio = df_limpio[df_limpio['fecha_nacimiento'] <= fecha_actual.date()]
        
        print(f"   ✅ Registros con fechas válidas: {len(df_limpio):,}")
        
        # 4. CALCULAR EDADES Y APLICAR FILTROS
        print("🔢 Calculando edades...")
        
        fecha_referencia = fecha_actual.date()
        
        def calcular_edad_detallada(fecha_nac):
            if pd.isna(fecha_nac):
                return None, None
            return calcular_edad_en_meses(fecha_nac, fecha_referencia), \
                   relativedelta(fecha_referencia, fecha_nac).years
        
        # Calcular edades
        edades_data = df_limpio['fecha_nacimiento'].apply(calcular_edad_detallada)
        df_limpio['edad_meses'] = [x[0] if x else None for x in edades_data]
        df_limpio['edad_anos'] = [x[1] if x else None for x in edades_data]
        
        # Filtros de edad (criterios originales)
        registros_antes_filtros = len(df_limpio)
        
        # Excluir edades negativas y mayores a 90 años
        df_limpio = df_limpio[
            (df_limpio['edad_anos'] >= 0) & 
            (df_limpio['edad_anos'] <= 90)
        ]
        
        print(f"   Registros después filtros edad: {len(df_limpio):,}")
        print(f"   Excluidos por edad: {registros_antes_filtros - len(df_limpio):,}")
        
        # 5. CLASIFICAR GRUPOS ETARIOS
        print("👥 Clasificando grupos etarios...")
        
        df_limpio['grupo_etario'] = df_limpio['edad_meses'].apply(clasificar_grupo_etario)
        df_limpio['fuera_grupos_etarios'] = df_limpio['grupo_etario'].isna()
        
        # 6. NORMALIZAR MUNICIPIOS
        print("🏙️ Normalizando municipios...")
        
        df_limpio['municipio'] = df_limpio['municipio'].apply(
            lambda x: normalizar_nombre_territorio(x).title() if pd.notna(x) else None
        )
        
        # 7. DETERMINAR UBICACIÓN URBANO/RURAL
        print("📍 Determinando ubicación urbano/rural...")
        
        df_limpio['tipo_ubicacion'] = df_limpio.apply(
            lambda row: determinar_ubicacion_urbano_rural(
                row.get('vereda'), row.get('corregimiento'), row.get('barrio')
            ), axis=1
        )
        
        # 8. ELIMINAR DUPLICADOS POR DOCUMENTO
        print("🔍 Eliminando duplicados...")
        
        registros_inicial = len(df_limpio)
        df_limpio = df_limpio.sort_values('fecha_nacimiento', ascending=False)
        df_limpio = df_limpio.drop_duplicates(subset=['documento'], keep='first')
        
        duplicados_removidos = registros_inicial - len(df_limpio)
        if duplicados_removidos > 0:
            print(f"   Duplicados eliminados: {duplicados_removidos:,}")
        
        # 9. CREAR CONTEO POBLACIONAL AGREGADO
        print("📊 Creando conteo poblacional agregado...")
        
        # Filtrar solo registros en grupos etarios definidos
        df_para_agregacion = df_limpio[
            (~df_limpio['fuera_grupos_etarios']) & 
            (df_limpio['grupo_etario'] != 'Sin datos') &
            (df_limpio['grupo_etario'].notna())
        ].copy()
        
        print(f"   Registros para agregación: {len(df_para_agregacion):,}")
        
        if len(df_para_agregacion) == 0:
            print("❌ ERROR: No hay registros válidos para agregación")
            return None
        
        # Agregar por municipio, ubicación y grupo etario
        conteo_poblacional = df_para_agregacion.groupby([
            'codigo_municipio',
            'municipio',
            'tipo_ubicacion',
            'grupo_etario'
        ]).size().reset_index(name='poblacion_total')
        
        # Ordenar resultados
        conteo_poblacional = conteo_poblacional.sort_values([
            'codigo_municipio', 'tipo_ubicacion', 'grupo_etario'
        ]).reset_index(drop=True)
        
        # 10. ESTADÍSTICAS FINALES
        print(f"\n📊 ESTADÍSTICAS PROCESAMIENTO:")
        print(f"   Registros originales: {len(df):,}")
        print(f"   Registros válidos finales: {len(df_limpio):,}")
        print(f"   Registros agregados: {len(conteo_poblacional):,}")
        print(f"   Población total: {conteo_poblacional['poblacion_total'].sum():,}")
        
        # Distribución urbano/rural
        dist_ubicacion = conteo_poblacional.groupby('tipo_ubicacion')['poblacion_total'].sum()
        print(f"\n🏙️ DISTRIBUCIÓN URBANO/RURAL:")
        for ubicacion, poblacion in dist_ubicacion.items():
            porcentaje = (poblacion / conteo_poblacional['poblacion_total'].sum()) * 100
            print(f"   {ubicacion}: {poblacion:,} ({porcentaje:.1f}%)")
        
        # Distribución por grupos etarios
        dist_grupos = conteo_poblacional.groupby('grupo_etario')['poblacion_total'].sum()
        print(f"\n👥 DISTRIBUCIÓN GRUPOS ETARIOS:")
        for grupo, poblacion in dist_grupos.items():
            porcentaje = (poblacion / conteo_poblacional['poblacion_total'].sum()) * 100
            print(f"   {grupo}: {poblacion:,} ({porcentaje:.1f}%)")
        
        print("✅ Procesamiento población completado exitosamente")
        
        return conteo_poblacional
        
    except Exception as e:
        print(f"❌ Error procesando población SISBEN: {e}")
        import traceback
        traceback.print_exc()
        return None

def cargar_poblacion_postgresql(df_poblacion, tabla="poblacion"):
    """
    Carga datos poblacionales agregados a PostgreSQL
    """
    if df_poblacion is None or len(df_poblacion) == 0:
        print("❌ No hay datos poblacionales para cargar")
        return False
    
    print(f"\n💾 CARGANDO {len(df_poblacion):,} REGISTROS A POSTGRESQL")
    print("=" * 55)
    
    try:
        engine = create_engine(DATABASE_URL)
        
        # Verificar conexión
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        print("✅ Conexión PostgreSQL exitosa")
        
        # Añadir metadatos
        df_poblacion['año'] = 2024  # Ajustar según sea necesario
        df_poblacion['fuente'] = 'SISBEN'
        df_poblacion['created_at'] = datetime.now()
        
        # Cargar datos
        df_poblacion.to_sql(
            tabla,
            engine,
            if_exists='replace',  # Reemplazar población completa
            index=False,
            chunksize=1000
        )
        
        # Verificar carga y generar estadísticas
        with engine.connect() as conn:
            total_cargado = conn.execute(text(f"SELECT COUNT(*) FROM {tabla}")).scalar()
            poblacion_total = conn.execute(text(f"SELECT SUM(poblacion_total) FROM {tabla}")).scalar()
            
            print(f"✅ {total_cargado:,} registros cargados exitosamente")
            print(f"👥 Población total: {poblacion_total:,} habitantes")
            
            # Estadísticas por ubicación
            stats_ubicacion = pd.read_sql(text(f"""
                SELECT tipo_ubicacion, 
                       COUNT(*) as registros,
                       SUM(poblacion_total) as poblacion
                FROM {tabla} 
                GROUP BY tipo_ubicacion 
                ORDER BY poblacion DESC
            """), conn)
            
            print(f"\n📍 ESTADÍSTICAS POR UBICACIÓN:")
            for _, row in stats_ubicacion.iterrows():
                porcentaje = (row['poblacion'] / poblacion_total) * 100
                print(f"   {row['tipo_ubicacion']}: {row['poblacion']:,} hab ({porcentaje:.1f}%)")
            
            # Top municipios más poblados
            top_municipios = pd.read_sql(text(f"""
                SELECT municipio, SUM(poblacion_total) as poblacion
                FROM {tabla} 
                GROUP BY municipio 
                ORDER BY poblacion DESC 
                LIMIT 10
            """), conn)
            
            print(f"\n🏆 TOP 10 MUNICIPIOS MÁS POBLADOS:")
            for i, row in top_municipios.iterrows():
                print(f"   {i+1:2d}. {row['municipio']}: {row['poblacion']:,} hab")
            
            # Verificar integridad referencial
            try:
                sin_referencia = conn.execute(text(f"""
                    SELECT COUNT(*) 
                    FROM {tabla} p
                    LEFT JOIN unidades_territoriales ut ON p.codigo_municipio = ut.codigo_divipola
                    WHERE ut.codigo_divipola IS NULL
                """)).scalar()
                
                if sin_referencia > 0:
                    print(f"⚠️ {sin_referencia} registros sin referencia territorial")
                else:
                    print("✅ Integridad referencial verificada")
                    
            except Exception as e:
                print(f"⚠️ No se pudo verificar integridad: {e}")
        
        return True
        
    except Exception as e:
        print(f"❌ Error cargando a PostgreSQL: {e}")
        import traceback
        traceback.print_exc()
        return False

def procesar_poblacion_completo(archivo_csv):
    """
    Proceso completo: CSV sin headers → Procesamiento → PostgreSQL
    """
    print("👥 PROCESAMIENTO COMPLETO POBLACIÓN SISBEN → POSTGRESQL")
    print("=" * 65)
    
    inicio = datetime.now()
    print(f"🚀 Iniciando: {inicio.strftime('%Y-%m-%d %H:%M:%S')}")
    
    try:
        # 1. Verificar archivo
        if not os.path.exists(archivo_csv):
            print(f"❌ ERROR: Archivo no encontrado: {archivo_csv}")
            return False
        
        print(f"📂 Archivo: {archivo_csv}")
        tamaño_mb = os.path.getsize(archivo_csv) / (1024*1024)
        print(f"📊 Tamaño: {tamaño_mb:.1f} MB")
        
        # 2. Procesar población SISBEN
        df_poblacion = cargar_poblacion_sisben_sin_headers(archivo_csv)
        
        if df_poblacion is None:
            print("❌ Error en procesamiento de población")
            return False
        
        # 3. Cargar a PostgreSQL
        exito = cargar_poblacion_postgresql(df_poblacion)
        
        # 4. Resumen final
        duracion = datetime.now() - inicio
        print(f"\n{'='*65}")
        print(" PROCESAMIENTO POBLACIÓN COMPLETADO ".center(65))
        print("=" * 65)
        
        if exito:
            print("🎉 ¡POBLACIÓN CARGADA EXITOSAMENTE!")
            print(f"📊 {len(df_poblacion):,} registros agregados procesados")
            print(f"👥 {df_poblacion['poblacion_total'].sum():,} habitantes totales")
            print("⚡ Denominadores listos para cálculo de coberturas")
        else:
            print("⚠️ Procesamiento con errores en carga BD")
        
        print(f"⏱️ Tiempo total: {duracion.total_seconds():.1f} segundos")
        print(f"📅 Finalizado: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        # 5. Generar backup CSV procesado
        if exito:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            backup_file = f"data/processed/poblacion_procesada_{timestamp}.csv"
            
            os.makedirs("data/processed", exist_ok=True)
            df_poblacion.to_csv(backup_file, index=False, encoding='utf-8-sig')
            print(f"💾 Backup procesado: {backup_file}")
        
        return exito
        
    except Exception as e:
        print(f"❌ Error crítico: {e}")
        import traceback
        traceback.print_exc()
        return False

def verificar_calidad_poblacion():
    """
    Verifica la calidad de los datos poblacionales cargados
    """
    print("\n🔍 VERIFICANDO CALIDAD DATOS POBLACIONALES")
    print("=" * 50)
    
    try:
        engine = create_engine(DATABASE_URL)
        
        with engine.connect() as conn:
            # Verificaciones básicas
            verificaciones = {
                "Total registros": "SELECT COUNT(*) FROM poblacion",
                "Sin código municipio": "SELECT COUNT(*) FROM poblacion WHERE codigo_municipio IS NULL",
                "Población <= 0": "SELECT COUNT(*) FROM poblacion WHERE poblacion_total <= 0",
                "Municipios únicos": "SELECT COUNT(DISTINCT codigo_municipio) FROM poblacion",
                "Grupos etarios únicos": "SELECT COUNT(DISTINCT grupo_etario) FROM poblacion"
            }
            
            print("📊 Verificaciones de calidad:")
            for nombre, query in verificaciones.items():
                try:
                    resultado = conn.execute(text(query)).scalar()
                    print(f"   {nombre}: {resultado:,}")
                except Exception as e:
                    print(f"   {nombre}: ERROR - {e}")
            
            # Verificar completitud por municipio
            completitud = pd.read_sql(text("""
                SELECT municipio,
                       COUNT(DISTINCT tipo_ubicacion) as ubicaciones,
                       COUNT(DISTINCT grupo_etario) as grupos,
                       SUM(poblacion_total) as poblacion
                FROM poblacion
                GROUP BY municipio
                HAVING COUNT(DISTINCT tipo_ubicacion) < 2 
                   OR COUNT(DISTINCT grupo_etario) < 3
                ORDER BY poblacion DESC
                LIMIT 5
            """), conn)
            
            if len(completitud) > 0:
                print(f"\n⚠️ Municipios con posible datos incompletos:")
                for _, row in completitud.iterrows():
                    print(f"   {row['municipio']}: {row['ubicaciones']} ubicaciones, "
                          f"{row['grupos']} grupos ({row['poblacion']:,} hab)")
            else:
                print("✅ Todos los municipios tienen datos completos")
        
        return True
        
    except Exception as e:
        print(f"❌ Error verificación: {e}")
        return False

# ================================
# FUNCIÓN PRINCIPAL
# ================================
if __name__ == "__main__":
    print("👥 PROCESADOR POBLACIÓN SISBEN → POSTGRESQL")
    print("=" * 50)
    
    # Archivo por defecto (CSV sin headers)
    archivo_default = "data/poblacion_veredas.csv"
    
    # Verificar archivo
    if not os.path.exists(archivo_default):
        print(f"❌ ERROR: No se encuentra '{archivo_default}'")
        print("\n💡 Opciones:")
        print("1. Colocar CSV SISBEN sin headers en 'data/poblacion_veredas.csv'")
        print("2. Modificar variable archivo_default")
        print("3. Llamar: procesar_poblacion_completo('ruta/archivo.csv')")
        sys.exit(1)
    
    # Ejecutar procesamiento completo
    exito = procesar_poblacion_completo(archivo_default)
    
    if exito:
        print("\n🔧 Ejecutando verificaciones de calidad...")
        verificar_calidad_poblacion()
        
        print("\n🎯 PRÓXIMOS PASOS:")
        print("1. Cargar datos de vacunación con cargar_vacunacion.py")
        print("2. Verificar vistas v_coberturas_dashboard en DBeaver")
        print("3. Calcular coberturas por municipio y grupo etario")
        print("4. ¡Análisis epidemiológicos completos! 🚀")
    else:
        print("\n❌ Procesamiento fallido. Revisar errores.")