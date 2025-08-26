#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Script PAIweb Adaptado para PostgreSQL - SIN DATOS PERSONALES
Versión optimizada para Sistema Epidemiológico Tolima
Basado en tu script original pero eliminando datos personales completamente
"""

import pandas as pd
import numpy as np
from datetime import datetime, date
import re
import os
import warnings
from sqlalchemy import create_engine, text
import psycopg2
from psycopg2.extras import execute_values

warnings.filterwarnings("ignore")

# Configuración base de datos
DATABASE_URL = "postgresql://tolima_admin:tolima2025!@localhost:5432/epidemiologia_tolima"

def limpiar_paiweb_fiebre_amarilla_anonimo(archivo_excel, hoja="Vacunas"):
    """
    Limpia y procesa datos de PAIweb ELIMINANDO todos los datos personales
    Mantiene solo información epidemiológicamente relevante
    """

    print("🔄 Cargando archivo Excel PAIweb...")
    df = pd.read_excel(archivo_excel, sheet_name=hoja)
    print(f"📊 Registros iniciales: {len(df):,}")

    # ================================
    # 1. ELIMINAR COLUMNAS PERSONALES Y NO NECESARIAS
    # ================================
    columnas_eliminar = [
        "Departamento", "nombrebiologico", "dosis", "Actualizacion",
        # ELIMINAR TODOS LOS DATOS PERSONALES
        "PrimerNombre", "SegundoNombre", "PrimerApellido", "SegundoApellido",
        "Documento", "tipoDocumento", "FechaNacimiento"
    ]
    
    # Eliminar solo las columnas que existen
    df = df.drop(columns=[col for col in columnas_eliminar if col in df.columns])
    print(f"🗑️ Datos personales eliminados completamente")

    # ================================
    # 2. MAPEO Y NORMALIZACIÓN DE MUNICIPIOS
    # ================================
    def normalizar_municipio(municipio):
        if pd.isna(municipio):
            return None

        municipio = str(municipio).strip().upper()

        # Mapeo específico
        mapeo_municipios = {
            "SAN SEBASTIÁN DE MARIQUITA": "MARIQUITA",
            "SAN SEBASTIAN DE MARIQUITA": "MARIQUITA",
        }

        if municipio in mapeo_municipios:
            municipio = mapeo_municipios[municipio]

        # Convertir a Title Case para normalizar
        return municipio.title()

    df["Municipio"] = df["Municipio"].apply(normalizar_municipio)

    # ================================
    # 3. LIMPIEZA Y VALIDACIÓN DE FECHAS
    # ================================
    def limpiar_fecha(fecha_str):
        if pd.isna(fecha_str):
            return None

        # Convertir a string y limpiar
        fecha_str = str(fecha_str).strip()

        # Remover partes de tiempo si existen
        if " " in fecha_str:
            fecha_str = fecha_str.split(" ")[0]

        # Intentar diferentes formatos
        formatos = ["%d/%m/%Y", "%d/%m/%y", "%Y-%m-%d", "%m/%d/%Y"]

        for formato in formatos:
            try:
                return datetime.strptime(fecha_str, formato).date()
            except:
                continue

        return None

    df["fechaaplicacion"] = df["fechaaplicacion"].apply(limpiar_fecha)

    # ================================
    # 4. CALCULAR EDAD SIN FECHA DE NACIMIENTO
    # ================================
    # Nota: Si no tienes fecha de nacimiento en tu archivo, 
    # necesitarás usar edad directa del archivo
    fecha_corte = date.today()

    def procesar_edad_directa(row):
        """
        Procesa edad usando campos disponibles sin fecha nacimiento
        """
        # Si tienes campo edad directo, usarlo
        if 'edad_' in row.index and pd.notna(row.get('edad_')):
            return int(row['edad_'])
        
        # Si tienes unidad de medida de edad, calcular
        if 'uni_med_' in row.index and 'edad_' in row.index:
            edad_val = row.get('edad_', 0)
            unidad = str(row.get('uni_med_', 'años')).lower()
            
            if 'año' in unidad or 'anos' in unidad:
                return int(edad_val)
            elif 'mes' in unidad:
                return max(0, int(edad_val / 12))  # Convertir meses a años
            elif 'día' in unidad or 'dia' in unidad:
                return max(0, int(edad_val / 365))  # Convertir días a años
        
        return None

    # Aplicar procesamiento de edad
    df['edad_anos'] = df.apply(procesar_edad_directa, axis=1)

    # ================================
    # 5. CLASIFICACIÓN GRUPOS ETARIOS
    # ================================
    def clasificar_grupo_etario(edad):
        if pd.isna(edad) or edad is None:
            return "Sin datos"

        if edad < 0.75:  # Menor de 9 meses
            return "Menor de 9 meses"
        elif edad < 2:  # 09-23 meses
            return "09-23 meses"
        elif edad <= 19:  # 02-19 años
            return "02-19 años"
        elif edad <= 59:  # 20-59 años
            return "20-59 años"
        else:  # 60+ años
            return "60+ años"

    df["grupo_etario"] = df["edad_anos"].apply(clasificar_grupo_etario)

    # ================================
    # 6. NORMALIZAR UBICACIÓN
    # ================================
    def normalizar_tipo_ubicacion(tipo):
        if pd.isna(tipo) or str(tipo).strip() == "":
            return "Urbano"
        tipo_str = str(tipo).strip().lower()
        if 'rural' in tipo_str or 'vereda' in tipo_str:
            return "Rural"
        else:
            return "Urbano"

    df["TipoUbicación"] = df["TipoUbicación"].apply(normalizar_tipo_ubicacion)

    # ================================
    # 7. VALIDACIONES Y FILTRADO
    # ================================
    registros_iniciales = len(df)

    # Filtrar registros con datos básicos válidos
    df = df.dropna(subset=["fechaaplicacion", "Municipio"])

    # Filtrar fechas coherentes
    df = df[df["fechaaplicacion"] <= fecha_corte]
    df = df[df["fechaaplicacion"] >= date(2020, 1, 1)]  # Filtro fecha mínima

    # Filtrar edades razonables (tu filtro actual de >90 años)
    df = df[(df["edad_anos"] >= 0) & (df["edad_anos"] <= 90)]

    print(f"📊 Registros después de validaciones: {len(df):,}")
    print(f"📊 Registros excluidos: {registros_iniciales - len(df):,}")

    # ================================
    # 8. GENERAR CÓDIGO DE MUNICIPIO (tu función actual)
    # ================================
    def generar_codigo_municipio(municipio):
        if pd.isna(municipio):
            return None

        # Mapeo completo de municipios del Tolima
        codigos_municipios = {
            "Ibagué": "73001",
            "Mariquita": "73408", 
            "Armero (Guayabal)": "73055",
            "Armero Guayabal": "73055",
            "Armero": "73055",
            "Ambalema": "73024",
            "Anzoátegui": "73043",
            "Ataco": "73067",
            "Cajamarca": "73124",
            "Carmen De Apicalá": "73148",
            "Carmen De Apicala": "73148",
            "Casabianca": "73152",
            "Chaparral": "73168",
            "Coello": "73200",
            "Coyaima": "73217",
            "Cunday": "73226",
            "Dolores": "73236",
            "Espinal": "73268",
            "Falan": "73270",
            "Flandes": "73275",
            "Fresno": "73283",
            "Guamo": "73319",
            "Herveo": "73347",
            "Honda": "73349",
            "Icononzo": "73352",
            "Lérida": "73408",
            "Lerida": "73408",
            "Líbano": "73411",
            "Libano": "73411",
            "Melgar": "73449",
            "Murillo": "73461",
            "Natagaima": "73483",
            "Ortega": "73504",
            "Palocabildo": "73520",
            "Piedras": "73547",
            "Planadas": "73555",
            "Prado": "73563",
            "Purificación": "73585",
            "Purificacion": "73585",
            "Rioblanco": "73616",
            "Roncesvalles": "73622",
            "Rovira": "73624",
            "Saldaña": "73675",
            "Saldana": "73675",
            "San Antonio": "73678",
            "San Luis": "73686",
            "Santa Isabel": "73770",
            "Suárez": "73854",
            "Suarez": "73854",
            "Valle De San Juan": "73861",
            "Venadillo": "73873",
            "Villahermosa": "73870",
            "Villarrica": "73873",
        }

        # Buscar código exacto
        municipio_norm = str(municipio).strip().title()
        codigo = codigos_municipios.get(municipio_norm)

        # Si no encuentra exacto, buscar coincidencias parciales
        if codigo is None:
            municipio_clean = (
                municipio_norm.upper()
                .replace("Á", "A").replace("É", "E").replace("Í", "I")
                .replace("Ó", "O").replace("Ú", "U").replace("Ñ", "N")
            )

            for mun_mapa, cod_mapa in codigos_municipios.items():
                mun_clean = (
                    mun_mapa.upper()
                    .replace("Á", "A").replace("É", "E").replace("Í", "I")
                    .replace("Ó", "O").replace("Ú", "U").replace("Ñ", "N")
                )
                if municipio_clean in mun_clean or mun_clean in municipio_clean:
                    codigo = cod_mapa
                    break

        if codigo is None:
            print(f"⚠️ Municipio no encontrado: {municipio_norm}")
            codigo = "73999"  # Código genérico Tolima

        return codigo

    df["codigo_municipio"] = df["Municipio"].apply(generar_codigo_municipio)

    # ================================
    # 9. SELECCIONAR SOLO CAMPOS EPIDEMIOLÓGICOS ANÓNIMOS
    # ================================
    df_anonimo = df.rename(columns={
        "Municipio": "municipio",
        "Institucion": "institucion", 
        "fechaaplicacion": "fecha_aplicacion",
        "TipoUbicación": "tipo_ubicacion"
    })

    # Seleccionar SOLO campos epidemiológicos (SIN datos personales)
    columnas_finales = [
        "codigo_municipio",
        "municipio",
        "institucion",
        "fecha_aplicacion", 
        "tipo_ubicacion",
        "edad_anos",
        "grupo_etario"
    ]

    df_final = df_anonimo[columnas_finales].copy()

    # ================================
    # 10. CAMPOS CALCULADOS AUTOMÁTICOS
    # ================================
    df_final['año'] = df_final['fecha_aplicacion'].dt.year
    df_final['mes'] = df_final['fecha_aplicacion'].dt.month
    df_final['semana_epidemiologica'] = df_final['fecha_aplicacion'].dt.isocalendar().week

    # ================================
    # 11. ESTADÍSTICAS FINALES (SIN datos personales)
    # ================================
    print(f"\n{'='*60}")
    print("ESTADÍSTICAS FINALES - DATOS ANÓNIMOS")
    print("=" * 60)

    total_registros = len(df_final)
    print(f"📊 Total registros procesados: {total_registros:,}")
    print(f"📍 Municipios únicos: {df_final['municipio'].nunique()}")
    print(f"🏥 Instituciones únicas: {df_final['institucion'].nunique()}")
    
    print(f"\n🏙️ DISTRIBUCIÓN URBANO/RURAL:")
    dist_ubicacion = df_final["tipo_ubicacion"].value_counts()
    for ubicacion, cantidad in dist_ubicacion.items():
        porcentaje = (cantidad / total_registros) * 100
        print(f"  {ubicacion}: {cantidad:,} ({porcentaje:.1f}%)")

    print(f"\n👥 DISTRIBUCIÓN POR GRUPOS ETARIOS:")
    dist_grupos = df_final["grupo_etario"].value_counts()
    grupos_orden = [
        "Menor de 9 meses", "09-23 meses", "02-19 años", "20-59 años", "60+ años", "Sin datos"
    ]
    for grupo in grupos_orden:
        if grupo in dist_grupos.index:
            cantidad = dist_grupos[grupo]
            porcentaje = (cantidad / total_registros) * 100
            print(f"  {grupo}: {cantidad:,} ({porcentaje:.1f}%)")

    print(f"\n📅 ESTADÍSTICAS DE EDAD:")
    edad_valida = df_final['edad_anos'].dropna()
    if len(edad_valida) > 0:
        print(f"  Edad mínima: {edad_valida.min():.0f} años")
        print(f"  Edad máxima: {edad_valida.max():.0f} años") 
        print(f"  Edad promedio: {edad_valida.mean():.1f} años")

    print("✅ Procesamiento ANÓNIMO completado!")
    print("🔒 CERO datos personales mantenidos")

    return df_final


def cargar_postgresql_optimizado(df_anonimo, tabla="vacunacion_fiebre_amarilla"):
    """
    Carga datos anónimos a PostgreSQL de forma optimizada
    """
    print(f"\n💾 Cargando {len(df_anonimo):,} registros a PostgreSQL...")
    
    try:
        engine = create_engine(DATABASE_URL, pool_size=10, max_overflow=20)
        
        # Verificar conexión
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        print("✅ Conexión PostgreSQL exitosa")
        
        # Cargar datos (REPLACE completo para históricos)
        start_time = datetime.now()
        
        df_anonimo.to_sql(
            tabla,
            engine,
            if_exists='replace',  # Reemplaza todo (datos históricos completos)
            index=False,
            method='multi',  # Carga por lotes optimizada
            chunksize=5000   # 5K registros por lote
        )
        
        load_time = datetime.now() - start_time
        print(f"⏱️ Carga completada en: {load_time.total_seconds():.1f} segundos")
        
        # Verificar carga y generar estadísticas
        with engine.connect() as conn:
            # Contar registros
            total_bd = conn.execute(text(f"SELECT COUNT(*) FROM {tabla}")).scalar()
            print(f"📊 Total registros en BD: {total_bd:,}")
            
            # Estadísticas rápidas
            stats = conn.execute(text(f"""
                SELECT 
                    COUNT(DISTINCT codigo_municipio) as municipios,
                    COUNT(DISTINCT institucion) as instituciones,
                    MIN(fecha_aplicacion) as fecha_min,
                    MAX(fecha_aplicacion) as fecha_max,
                    COUNT(DISTINCT año) as años_datos
                FROM {tabla}
            """)).fetchone()
            
            print(f"📍 Municipios en BD: {stats[0]}")
            print(f"🏥 Instituciones en BD: {stats[1]}")
            print(f"📅 Rango fechas: {stats[2]} a {stats[3]}")
            print(f"📊 Años con datos: {stats[4]}")
            
        # Crear backup CSV
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_file = f"backups/vacunacion_backup_{timestamp}.csv"
        df_anonimo.to_csv(backup_file, index=False, encoding='utf-8-sig')
        print(f"💾 Backup CSV creado: {backup_file}")
        
        return True
        
    except Exception as e:
        print(f"❌ Error cargando a PostgreSQL: {e}")
        return False


def procesar_archivo_paiweb_completo(ruta_archivo, hoja="Vacunas"):
    """
    Función principal: Procesa archivo PAIweb completo a PostgreSQL
    """
    
    print("\n" + "=" * 80)
    print(" PROCESAMIENTO PAIweb → POSTGRESQL (ANÓNIMO) ".center(80))
    print("=" * 80)
    
    inicio = datetime.now()
    print(f"🚀 Iniciando: {inicio.strftime('%Y-%m-%d %H:%M:%S')}")
    print("-" * 80)

    try:
        # 1. VERIFICAR ARCHIVO
        if not os.path.exists(ruta_archivo):
            print(f"❌ ERROR: Archivo no encontrado: {ruta_archivo}")
            return None, False
            
        print(f"📂 Procesando: {ruta_archivo}")
        
        # 2. LIMPIAR Y PROCESAR (ANÓNIMO)
        df_limpio = limpiar_paiweb_fiebre_amarilla_anonimo(ruta_archivo, hoja)
        
        if df_limpio is None or len(df_limpio) == 0:
            print("❌ ERROR: No hay datos válidos para procesar")
            return None, False
        
        # 3. CARGAR A POSTGRESQL
        exito_carga = cargar_postgresql_optimizado(df_limpio)
        
        # 4. RESUMEN FINAL
        duracion = datetime.now() - inicio
        print(f"\n{'='*80}")
        print(" PROCESAMIENTO COMPLETADO ".center(80))
        print("=" * 80)
        
        if exito_carga:
            print("🎉 ¡ÉXITO TOTAL!")
            print(f"📊 {len(df_limpio):,} registros anónimos cargados")
            print("🔒 Cero datos personales almacenados")
            print("⚡ Dashboard puede conectarse a PostgreSQL")
        else:
            print("⚠️ Procesamiento completado con errores en BD")
            print("📁 Datos procesados disponibles en memoria")
        
        print(f"⏱️ Tiempo total: {duracion.total_seconds():.1f} segundos")
        print(f"📅 Finalizado: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        return df_limpio, exito_carga

    except Exception as e:
        print(f"❌ Error crítico: {str(e)}")
        import traceback
        traceback.print_exc()
        return None, False


def verificar_calidad_datos():
    """
    Verifica calidad de datos cargados en PostgreSQL
    """
    print("\n🔍 VERIFICANDO CALIDAD DE DATOS...")
    
    try:
        engine = create_engine(DATABASE_URL)
        
        with engine.connect() as conn:
            # Verificaciones básicas
            verificaciones = {
                "total_registros": "SELECT COUNT(*) FROM vacunacion_fiebre_amarilla",
                "registros_sin_municipio": "SELECT COUNT(*) FROM vacunacion_fiebre_amarilla WHERE codigo_municipio IS NULL",
                "registros_sin_fecha": "SELECT COUNT(*) FROM vacunacion_fiebre_amarilla WHERE fecha_aplicacion IS NULL",
                "edades_invalidas": "SELECT COUNT(*) FROM vacunacion_fiebre_amarilla WHERE edad_anos < 0 OR edad_anos > 90",
                "municipios_unicos": "SELECT COUNT(DISTINCT codigo_municipio) FROM vacunacion_fiebre_amarilla",
                "instituciones_unicas": "SELECT COUNT(DISTINCT institucion) FROM vacunacion_fiebre_amarilla"
            }
            
            print("📊 Resultados verificación:")
            for nombre, query in verificaciones.items():
                resultado = conn.execute(text(query)).scalar()
                print(f"   {nombre}: {resultado:,}")
            
            # Verificar vista de coberturas
            try:
                cobertura_sample = pd.read_sql(
                    "SELECT * FROM v_coberturas_dashboard LIMIT 5", conn
                )
                print(f"✅ Vista coberturas funcional: {len(cobertura_sample)} registros muestra")
            except Exception as e:
                print(f"⚠️ Error en vista coberturas: {e}")
        
        return True
        
    except Exception as e:
        print(f"❌ Error verificación: {e}")
        return False


# ================================
# EJECUCIÓN DEL SCRIPT
# ================================
if __name__ == "__main__":
    print("🐍 SCRIPT PAIweb → PostgreSQL ANÓNIMO")
    print("=" * 50)
    
    # Configurar archivo por defecto
    archivo_default = "data/paiweb.xlsx"
    
    # Verificar archivo
    if not os.path.exists(archivo_default):
        print(f"❌ ERROR: No se encuentra '{archivo_default}'")
        print("\n💡 Opciones:")
        print("1. Colocar archivo PAIweb en 'data/paiweb.xlsx'")
        print("2. Modificar variable archivo_default")
        print("3. Llamar función: procesar_archivo_paiweb_completo('ruta/archivo.xlsx')")
    else:
        # Ejecutar procesamiento completo
        df_resultado, exito = procesar_archivo_paiweb_completo(archivo_default)
        
        if exito:
            print("\n🔧 Ejecutando verificación de calidad...")
            verificar_calidad_datos()
            
            print("\n🎯 PRÓXIMOS PASOS:")
            print("1. Abrir DBeaver y conectar a PostgreSQL")
            print("2. Explorar tabla 'vacunacion_fiebre_amarilla'")  
            print("3. Revisar vistas: v_coberturas_dashboard, v_mapa_coberturas")
            print("4. Conectar dashboard Streamlit a PostgreSQL")
            print("5. ¡Disfrutar análisis optimizados! 🚀")