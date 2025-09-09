#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
cargar_vacunacion.py - PAIweb → PostgreSQL
Procesamiento de datos de vacunación
CORREGIDO: Edad calculada con fecha actual, mapeos locales
Solo columnas necesarias, datos completamente anónimos
"""

import pandas as pd
import numpy as np
from datetime import datetime, date
from dateutil.relativedelta import relativedelta
import os
import warnings
from sqlalchemy import create_engine, text

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

# Importar configuración centralizada
from config import (
    DATABASE_URL, MAPEO_MUNICIPIOS_ESPECIALES,
    clasificar_grupo_etario, calcular_edad_en_meses,
    limpiar_fecha_robusta, cargar_primera_hoja_excel,
    buscar_codigo_municipio, normalizar_nombre_territorio
)

warnings.filterwarnings("ignore")

# ================================
# MAPEO LOCAL VACUNACIÓN PAIweb (Solo para este script)
# ================================
MAPEO_VACUNACION_EXCEL = {
    'departamento': 'Departamento',
    'municipio': 'Municipio',
    'institucion': 'Institucion',
    'fecha_aplicacion': 'fechaaplicacion',
    'fecha_nacimiento': 'FechaNacimiento',
    'tipo_ubicacion': 'TipoUbicación'
}

def procesar_paiweb_vacunacion(archivo_excel):
    """
    Procesa datos de vacunación PAIweb ELIMINANDO datos personales
    CORREGIDO: Edad calculada con fecha actual (no fecha aplicación)
    """
    print("💉 PROCESANDO VACUNACIÓN PAIweb → POSTGRESQL")
    print("=" * 55)
    
    inicio = datetime.now()
    
    try:
        # 1. CARGAR ARCHIVO EXCEL (primera hoja)
        print(f"📂 Cargando: {archivo_excel}")
        
        df, nombre_hoja = cargar_primera_hoja_excel(archivo_excel)
        if df is None:
            return None
        
        print(f"📊 Registros iniciales: {len(df):,}")
        print(f"📋 Columnas disponibles: {len(df.columns)}")
        
        # 2. MAPEAR SOLO COLUMNAS NECESARIAS (mapeo local)
        print("🔄 Mapeando columnas necesarias...")
        
        # Verificar y mapear columnas usando mapeo local específico
        columnas_mapeadas = {}
        columnas_faltantes = []
        
        for nombre_bd, nombre_excel in MAPEO_VACUNACION_EXCEL.items():
            if nombre_excel in df.columns:
                columnas_mapeadas[nombre_excel] = nombre_bd
                print(f"   ✅ {nombre_excel} → {nombre_bd}")
            else:
                columnas_faltantes.append(nombre_excel)
                print(f"   ❌ {nombre_excel} → NO ENCONTRADA")
        
        if columnas_faltantes:
            print(f"⚠️ Columnas faltantes: {columnas_faltantes}")
            print("⚠️ Se continuará con las columnas disponibles")
        
        # Renombrar columnas encontradas
        df = df.rename(columns=columnas_mapeadas)
        
        # Seleccionar solo columnas mapeadas (eliminar datos personales)
        columnas_disponibles = list(columnas_mapeadas.values())
        df = df[columnas_disponibles].copy()
        
        print(f"🔒 Datos personales completamente eliminados")
        print(f"📋 Columnas finales: {list(df.columns)}")
        
        # 3. LIMPIAR Y VALIDAR FECHAS
        print("📅 Procesando fechas...")
        
        # Limpiar fecha de aplicación
        if 'fecha_aplicacion' in df.columns:
            df['fecha_aplicacion'] = df['fecha_aplicacion'].apply(limpiar_fecha_robusta)
            fechas_app_nulas = df['fecha_aplicacion'].isna().sum()
            print(f"   Fechas aplicación nulas: {fechas_app_nulas:,}")
        
        # Limpiar fecha de nacimiento (CRÍTICO para cálculo edad)
        if 'fecha_nacimiento' in df.columns:
            df['fecha_nacimiento'] = df['fecha_nacimiento'].apply(limpiar_fecha_robusta)
            fechas_nac_nulas = df['fecha_nacimiento'].isna().sum()
            print(f"   Fechas nacimiento nulas: {fechas_nac_nulas:,}")
        else:
            print("❌ ERROR CRÍTICO: No se encontró columna FechaNacimiento")
            return None
        
        # 4. CALCULAR EDAD USANDO FECHA ACTUAL (CORREGIDO)
        print("🔢 Calculando edad con fecha ACTUAL como referencia...")
        
        fecha_referencia = date.today()  # CORREGIDO: Siempre fecha actual
        
        def calcular_edad_con_fecha_actual(fecha_nac):
            """Calcula edad usando SOLO fecha actual como referencia"""
            if pd.isna(fecha_nac):
                return None, None
            
            # SIEMPRE usar fecha actual, NO fecha aplicación
            edad_meses = calcular_edad_en_meses(fecha_nac, fecha_referencia)
            if edad_meses is not None:
                edad_anos = edad_meses / 12
                return edad_meses, edad_anos
            
            return None, None
        
        # Aplicar cálculo de edad con fecha actual
        edades_data = df['fecha_nacimiento'].apply(calcular_edad_con_fecha_actual)
        
        df['edad_meses'] = [x[0] if x else None for x in edades_data]
        df['edad_anos'] = [x[1] if x else None for x in edades_data]
        
        print(f"   ✅ Edades calculadas usando FECHA ACTUAL como referencia")
        print(f"   📅 Fecha referencia: {fecha_referencia}")
        
        # 5. CLASIFICAR GRUPOS ETARIOS
        print("👥 Clasificando grupos etarios...")
        
        df['grupo_etario'] = df['edad_meses'].apply(clasificar_grupo_etario)
        
        # Estadísticas de grupos etarios
        grupos_dist = df['grupo_etario'].value_counts()
        print(f"   Distribución grupos etarios:")
        for grupo, cantidad in grupos_dist.items():
            porcentaje = (cantidad / len(df)) * 100
            print(f"     {grupo}: {cantidad:,} ({porcentaje:.1f}%)")
        
        # 6. NORMALIZAR MUNICIPIOS
        print("🏙️ Normalizando municipios...")
        
        def normalizar_municipio_paiweb(municipio):
            if pd.isna(municipio):
                return None
            
            municipio = str(municipio).strip().upper()
            
            # Aplicar mapeos especiales desde config
            municipio = MAPEO_MUNICIPIOS_ESPECIALES.get(municipio, municipio)
            
            return municipio.title()
        
        if 'municipio' in df.columns:
            df['municipio'] = df['municipio'].apply(normalizar_municipio_paiweb)
        
        # 7. ASIGNAR CÓDIGOS DIVIPOLA
        print("🔢 Asignando códigos DIVIPOLA...")
        
        if 'municipio' in df.columns:
            df['codigo_municipio'] = df['municipio'].apply(buscar_codigo_municipio)
            
            # Estadísticas de mapeo
            codigos_asignados = df['codigo_municipio'].notna().sum()
            municipios_unicos = df['municipio'].nunique()
            print(f"   Códigos asignados: {codigos_asignados:,}/{len(df):,}")
            print(f"   Municipios únicos: {municipios_unicos}")
        
        # 8. NORMALIZAR UBICACIÓN
        print("📍 Normalizando tipo de ubicación...")
        
        def normalizar_ubicacion(tipo):
            if pd.isna(tipo) or str(tipo).strip() == "":
                return "Urbano"  # Por defecto urbano
            
            tipo_str = str(tipo).strip().lower()
            if any(keyword in tipo_str for keyword in ['rural', 'vereda', 'campo']):
                return "Rural"
            else:
                return "Urbano"
        
        if 'tipo_ubicacion' in df.columns:
            df['tipo_ubicacion'] = df['tipo_ubicacion'].apply(normalizar_ubicacion)
        
        # 9. VALIDACIONES Y FILTROS
        print("🔍 Aplicando validaciones...")
        
        registros_iniciales = len(df)
        
        # Filtrar registros con datos básicos válidos
        columnas_criticas = ['fecha_aplicacion', 'municipio', 'fecha_nacimiento']
        columnas_criticas_disponibles = [col for col in columnas_criticas if col in df.columns]
        
        df = df.dropna(subset=columnas_criticas_disponibles)
        
        # Filtrar fechas coherentes
        if 'fecha_aplicacion' in df.columns:
            fecha_min = date(2020, 1, 1)
            fecha_max = date.today()
            df = df[
                (df['fecha_aplicacion'] >= fecha_min) & 
                (df['fecha_aplicacion'] <= fecha_max)
            ]
        
        # Filtrar edades razonables (0-90 años)
        if 'edad_anos' in df.columns:
            df = df[
                (df['edad_anos'] >= 0) & 
                (df['edad_anos'] <= 90)
            ]
        
        print(f"   Registros después validaciones: {len(df):,}")
        print(f"   Registros excluidos: {registros_iniciales - len(df):,}")
        
        # 10. CAMPOS CALCULADOS AUTOMÁTICOS
        print("⚙️ Generando campos calculados...")
        
        if 'fecha_aplicacion' in df.columns:
            df['año'] = df['fecha_aplicacion'].dt.year
            df['mes'] = df['fecha_aplicacion'].dt.month
            df['semana_epidemiologica'] = df['fecha_aplicacion'].dt.isocalendar().week
        
        # 11. ELIMINAR FECHA DE NACIMIENTO (mantener solo edad calculada)
        print("🔒 Eliminando fecha nacimiento para anonimización...")
        
        if 'fecha_nacimiento' in df.columns:
            df = df.drop(columns=['fecha_nacimiento'])
            print("   ✅ Fecha nacimiento eliminada (solo edad conservada)")
        
        # 12. ESTADÍSTICAS FINALES
        print(f"\n📊 ESTADÍSTICAS FINALES - DATOS ANÓNIMOS:")
        print(f"   Total registros procesados: {len(df):,}")
        
        if 'municipio' in df.columns:
            print(f"   Municipios únicos: {df['municipio'].nunique()}")
            
        if 'institucion' in df.columns:
            print(f"   Instituciones únicas: {df['institucion'].nunique()}")
        
        if 'tipo_ubicacion' in df.columns:
            dist_ubicacion = df['tipo_ubicacion'].value_counts()
            print(f"   Distribución urbano/rural:")
            for ubicacion, cantidad in dist_ubicacion.items():
                porcentaje = (cantidad / len(df)) * 100
                print(f"     {ubicacion}: {cantidad:,} ({porcentaje:.1f}%)")
        
        if 'edad_anos' in df.columns:
            edad_stats = df['edad_anos'].describe()
            print(f"   Estadísticas edad (calculada con fecha actual):")
            print(f"     Mínima: {edad_stats['min']:.1f} años")
            print(f"     Máxima: {edad_stats['max']:.1f} años")
            print(f"     Promedio: {edad_stats['mean']:.1f} años")
        
        print("✅ Procesamiento PAIweb completado")
        print("🔒 CERO datos personales mantenidos")
        print("📅 Edad calculada con fecha actual (CORREGIDO)")
        
        return df
        
    except Exception as e:
        print(f"❌ Error procesando PAIweb: {e}")
        import traceback
        traceback.print_exc()
        return None

def cargar_vacunacion_postgresql(df_vacunacion, tabla="vacunacion_fiebre_amarilla"):
    """
    Carga datos de vacunación anónimos a PostgreSQL
    """
    if df_vacunacion is None or len(df_vacunacion) == 0:
        print("❌ No hay datos de vacunación para cargar")
        return False
    
    print(f"\n💾 CARGANDO {len(df_vacunacion):,} REGISTROS A POSTGRESQL")
    print("=" * 55)
    
    try:
        engine = create_engine(DATABASE_URL, pool_size=10, max_overflow=20)
        
        # Verificar conexión
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        print("✅ Conexión PostgreSQL exitosa")
        
        # Añadir metadatos
        df_vacunacion['fecha_carga'] = datetime.now()
        df_vacunacion['fuente'] = 'PAIweb'
        
        # Cargar datos con optimización por lotes
        start_time = datetime.now()
        
        df_vacunacion.to_sql(
            tabla,
            engine,
            if_exists='replace',  # Reemplazar datos históricos completos
            index=False,
            method='multi',
            chunksize=5000
        )
        
        load_time = datetime.now() - start_time
        print(f"⏱️ Carga completada en: {load_time.total_seconds():.1f} segundos")
        
        # Verificar carga y estadísticas
        with engine.connect() as conn:
            total_bd = conn.execute(text(f"SELECT COUNT(*) FROM {tabla}")).scalar()
            print(f"📊 Total registros en BD: {total_bd:,}")
            
            # Estadísticas de la carga
            stats = pd.read_sql(text(f"""
                SELECT 
                    COUNT(DISTINCT codigo_municipio) as municipios,
                    COUNT(DISTINCT institucion) as instituciones,
                    MIN(fecha_aplicacion) as fecha_min,
                    MAX(fecha_aplicacion) as fecha_max,
                    COUNT(DISTINCT año) as años_datos,
                    ROUND(AVG(edad_anos), 1) as edad_promedio
                FROM {tabla}
                WHERE fecha_aplicacion IS NOT NULL
            """), conn)
            
            if len(stats) > 0:
                s = stats.iloc[0]
                print(f"📍 Municipios: {s['municipios']}")
                print(f"🏥 Instituciones: {s['instituciones']}")
                print(f"📅 Período: {s['fecha_min']} a {s['fecha_max']}")
                print(f"📊 Años con datos: {s['años_datos']}")
                print(f"👥 Edad promedio: {s['edad_promedio']} años")
            
            # Verificar vista de coberturas
            try:
                cobertura_test = pd.read_sql(text("""
                    SELECT COUNT(*) as registros 
                    FROM v_coberturas_dashboard 
                    LIMIT 1
                """), conn)
                
                if len(cobertura_test) > 0:
                    print(f"📈 Vista coberturas: {cobertura_test.iloc[0]['registros']:,} registros")
                else:
                    print("📈 Vista coberturas: Funcional")
            except Exception as e:
                print(f"⚠️ Error vista coberturas: {e}")
        
        # Crear backup CSV
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_file = f"backups/vacunacion_backup_{timestamp}.csv"
        
        os.makedirs("backups", exist_ok=True)
        df_vacunacion.to_csv(backup_file, index=False, encoding='utf-8-sig')
        print(f"💾 Backup creado: {backup_file}")
        
        return True
        
    except Exception as e:
        print(f"❌ Error cargando a PostgreSQL: {e}")
        import traceback
        traceback.print_exc()
        return False

def procesar_vacunacion_completo(archivo_excel):
    """
    Proceso completo: Excel PAIweb → Procesamiento → PostgreSQL
    """
    print("💉 PROCESAMIENTO COMPLETO PAIweb → POSTGRESQL V2.0")
    print("=" * 60)
    
    inicio = datetime.now()
    print(f"🚀 Iniciando: {inicio.strftime('%Y-%m-%d %H:%M:%S')}")
    
    try:
        # 1. Verificar archivo
        if not os.path.exists(archivo_excel):
            print(f"❌ ERROR: Archivo no encontrado: {archivo_excel}")
            return False
        
        print(f"📂 Archivo: {archivo_excel}")
        tamaño_mb = os.path.getsize(archivo_excel) / (1024*1024)
        print(f"📊 Tamaño: {tamaño_mb:.1f} MB")
        
        # 2. Procesar datos PAIweb
        df_vacunacion = procesar_paiweb_vacunacion(archivo_excel)
        
        if df_vacunacion is None:
            print("❌ Error en procesamiento de vacunación")
            return False
        
        # 3. Cargar a PostgreSQL
        exito = cargar_vacunacion_postgresql(df_vacunacion)
        
        # 4. Resumen final
        duracion = datetime.now() - inicio
        print(f"\n{'='*60}")
        print(" PROCESAMIENTO VACUNACIÓN COMPLETADO ".center(60))
        print("=" * 60)
        
        if exito:
            print("🎉 ¡VACUNACIÓN CARGADA EXITOSAMENTE!")
            print(f"📊 {len(df_vacunacion):,} registros anónimos procesados")
            print("🔒 Cero datos personales almacenados")
            print("📅 Edad calculada con fecha actual (CORREGIDO)")
            print("📈 Vistas de coberturas actualizadas")
            print("⚡ Dashboard listo para conectarse")
        else:
            print("⚠️ Procesamiento con errores en carga BD")
        
        print(f"⏱️ Tiempo total: {duracion.total_seconds():.1f} segundos")
        print(f"📅 Finalizado: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        return exito
        
    except Exception as e:
        print(f"❌ Error crítico: {e}")
        import traceback
        traceback.print_exc()
        return False

def verificar_calidad_vacunacion():
    """
    Verifica la calidad de los datos de vacunación cargados
    """
    print("\n🔍 VERIFICANDO CALIDAD DATOS VACUNACIÓN")
    print("=" * 45)
    
    try:
        engine = create_engine(DATABASE_URL)
        
        with engine.connect() as conn:
            # Verificaciones básicas
            verificaciones = {
                "Total registros": "SELECT COUNT(*) FROM vacunacion_fiebre_amarilla",
                "Sin código municipio": "SELECT COUNT(*) FROM vacunacion_fiebre_amarilla WHERE codigo_municipio IS NULL",
                "Sin fecha aplicación": "SELECT COUNT(*) FROM vacunacion_fiebre_amarilla WHERE fecha_aplicacion IS NULL",
                "Sin institución": "SELECT COUNT(*) FROM vacunacion_fiebre_amarilla WHERE institucion IS NULL",
                "Edades inválidas": "SELECT COUNT(*) FROM vacunacion_fiebre_amarilla WHERE edad_anos < 0 OR edad_anos > 90",
                "Sin grupo etario": "SELECT COUNT(*) FROM vacunacion_fiebre_amarilla WHERE grupo_etario IS NULL"
            }
            
            print("📊 Verificaciones de calidad:")
            for nombre, query in verificaciones.items():
                try:
                    resultado = conn.execute(text(query)).scalar()
                    print(f"   {nombre}: {resultado:,}")
                except Exception as e:
                    print(f"   {nombre}: ERROR - {e}")
            
            # Top instituciones
            top_instituciones = pd.read_sql(text("""
                SELECT institucion, COUNT(*) as vacunas
                FROM vacunacion_fiebre_amarilla
                WHERE institucion IS NOT NULL
                GROUP BY institucion
                ORDER BY vacunas DESC
                LIMIT 5
            """), conn)
            
            if len(top_instituciones) > 0:
                print(f"\n🏥 Top 5 instituciones más activas:")
                for _, row in top_instituciones.iterrows():
                    print(f"   {row['institucion']}: {row['vacunas']:,} vacunas")
        
        return True
        
    except Exception as e:
        print(f"❌ Error verificación: {e}")
        return False

# ================================
# FUNCIÓN PRINCIPAL
# ================================
if __name__ == "__main__":
    print("💉 PROCESADOR VACUNACIÓN PAIweb → POSTGRESQL V2.0")
    print("=" * 55)
    
    # Archivo por defecto
    archivo_default = "data/paiweb.xlsx"
    
    # Verificar archivo
    if not os.path.exists(archivo_default):
        print(f"❌ ERROR: No se encuentra '{archivo_default}'")
        print("\n💡 Opciones:")
        print("1. Colocar archivo PAIweb en 'data/paiweb.xlsx'")
        print("2. Modificar variable archivo_default")
        print("3. Llamar: procesar_vacunacion_completo('ruta/archivo.xlsx')")
    else:
        # Ejecutar procesamiento completo
        exito = procesar_vacunacion_completo(archivo_default)
        
        if exito:
            print("\n🔧 Ejecutando verificaciones de calidad...")
            verificar_calidad_vacunacion()
            
            print("\n🎯 PRÓXIMOS PASOS:")
            print("1. Revisar datos en DBeaver: tabla 'vacunacion_fiebre_amarilla'")
            print("2. Consultar vistas: v_coberturas_dashboard, v_mapa_coberturas")
            print("3. Calcular coberturas por municipio y grupo etario")
            print("4. Conectar dashboard Streamlit")
            print("5. ¡Análisis epidemiológicos listos! 🚀")
        else:
            print("\n❌ Procesamiento fallido. Revisar errores.")