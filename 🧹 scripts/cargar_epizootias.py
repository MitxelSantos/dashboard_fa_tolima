#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Script: Procesar Epizootias → PostgreSQL
Procesa archivo Excel de epizootias (muertes animales) con geolocalización
"""

import pandas as pd
import numpy as np
from datetime import datetime, date
from sqlalchemy import create_engine, text
import warnings
import os
warnings.filterwarnings('ignore')

# Configuración BD
DATABASE_URL = "postgresql://tolima_admin:tolima2025!@localhost:5432/epidemiologia_tolima"

def procesar_epizootias(archivo_excel, hoja=None):
    """
    Procesa epizootias desde Excel a PostgreSQL
    """
    print("🐒 PROCESANDO EPIZOOTIAS")
    print("=" * 30)
    
    inicio = datetime.now()
    
    try:
        # 1. CARGAR ARCHIVO EXCEL
        print(f"📂 Cargando: {archivo_excel}")
        
        # Detectar hoja automáticamente
        if hoja is None:
            excel_file = pd.ExcelFile(archivo_excel)
            print(f"📋 Hojas disponibles: {excel_file.sheet_names}")
            
            # Buscar hoja con patrón de epizootias
            hoja = excel_file.sheet_names[0]
            for sheet in excel_file.sheet_names:
                if any(keyword in sheet.lower() for keyword in ['epizooti', 'animal', 'muerte']):
                    hoja = sheet
                    break
            print(f"📄 Usando hoja: {hoja}")
        
        df = pd.read_excel(archivo_excel, sheet_name=hoja)
        print(f"📊 Registros iniciales: {len(df):,}")
        print(f"📋 Columnas: {list(df.columns)}")
        
        # 2. NORMALIZAR NOMBRES DE COLUMNAS
        print("🔄 Normalizando columnas...")
        
        # Limpiar nombres de columnas
        df.columns = df.columns.str.strip().str.lower().str.replace(' ', '_')
        
        # Mapeo común de columnas
        mapeo_columnas = {
            "MUNICIPIO": "municipio",
            "VEREDA": "vereda",
            'FECHA_RECOLECCION': 'fecha_recoleccion',
            'FECHA_NOTIFICACION': 'fecha_notificacion', 
            'DESCRIPCION': 'descripcion',
            'INFORMANTE': 'informante',
            'ESPECIE': 'especie',
            'LATITUD': 'latitud',
            'LONGITUD': 'longitud',
            'FECHA_ENVIO_MUESTRA': 'fecha_envio_muestra',
            'RESULTADO_PCR': 'resultado_pcr',
            'FECHA_RESULTADO_PCR': 'fecha_resultado_pcr',
            'RESULTADO_HISTOPATOLOGIA': 'resultado_histopatologia',
            'FECHA_RESULTADO_HISTOPATOLOGIA': 'fecha_resultado_histopatologia'
        }
        
        # Aplicar mapeo
        for col_original, col_nueva in mapeo_columnas.items():
            if col_original in df.columns:
                df = df.rename(columns={col_original: col_nueva})
                print(f"   ✅ {col_original} → {col_nueva}")
        
        print(f"📋 Columnas después de mapeo: {list(df.columns)}")
        
        # 3. LIMPIEZA DE DATOS
        print("🧹 Limpiando datos...")
        
        # Limpiar municipios y veredas
        def limpiar_texto(texto):
            if pd.isna(texto):
                return None
            return str(texto).strip().title()
        
        if 'municipio' in df.columns:
            df['municipio'] = df['municipio'].apply(limpiar_texto)
            
        if 'vereda' in df.columns:
            df['vereda'] = df['vereda'].apply(limpiar_texto)
        
        # Limpiar fechas
        def limpiar_fecha(fecha_str):
            if pd.isna(fecha_str):
                return None
            try:
                fecha_str = str(fecha_str).strip()
                if " " in fecha_str:
                    fecha_str = fecha_str.split(" ")[0]
                
                for formato in ["%d/%m/%Y", "%Y-%m-%d", "%d-%m-%Y", "%m/%d/%Y"]:
                    try:
                        return datetime.strptime(fecha_str, formato).date()
                    except:
                        continue
                return None
            except:
                return None
        
        # Aplicar limpieza de fechas
        campos_fecha = [
            'fecha_recoleccion', 'fecha_notificacion', 'fecha_envio_muestra',
            'fecha_resultado_pcr', 'fecha_resultado_histopatologia'
        ]
        
        for campo in campos_fecha:
            if campo in df.columns:
                df[campo] = df[campo].apply(limpiar_fecha)
                print(f"   📅 {campo}: fechas procesadas")
        
        # 4. PROCESAMIENTO DE COORDENADAS
        print("📍 Procesando coordenadas geográficas...")
        
        def limpiar_coordenada(coord_str):
            if pd.isna(coord_str):
                return None
            try:
                # Convertir a float
                coord = float(str(coord_str).strip().replace(',', '.'))
                return coord
            except:
                return None
        
        if 'latitud' in df.columns:
            df['latitud'] = df['latitud'].apply(limpiar_coordenada)
            
        if 'longitud' in df.columns:
            df['longitud'] = df['longitud'].apply(limpiar_coordenada)
        
        # Validar coordenadas para Colombia
        if 'latitud' in df.columns and 'longitud' in df.columns:
            coords_validas = (
                (df['latitud'].between(-4.2, 12.6)) &  # Rango latitud Colombia
                (df['longitud'].between(-81.8, -66.9))  # Rango longitud Colombia
            )
            coords_invalidas = len(df) - coords_validas.sum()
            if coords_invalidas > 0:
                print(f"   ⚠️ {coords_invalidas} registros con coordenadas inválidas")
            
            # Mantener todas las coordenadas (incluso inválidas para revisión manual)
        
        # 5. ASIGNAR CÓDIGOS MUNICIPIO
        print("🏷️ Asignando códigos DIVIPOLA...")
        
        def asignar_codigo_municipio(nombre_municipio):
            if pd.isna(nombre_municipio):
                return None
                
            mapeo_municipios = {
                "IBAGUÉ": "73001",
                "MARIQUITA": "73408", 
                "ESPINAL": "73268",
                "HONDA": "73349",
                "FLANDES": "73275",
                "MELGAR": "73449",
                "LÍBANO": "73411",
                "LIBANO": "73411",
                "CHAPARRAL": "73168",
                "PURIFICACIÓN": "73585",
                "PURIFICACION": "73585",
                "GUAMO": "73319",
                "SALDAÑA": "73675",
                "SALDANA": "73675",
                "CAJAMARCA": "73124",
                "ROVIRA": "73624",
                "ORTEGA": "73504",
                "PLANADAS": "73555",
                "RIOBLANCO": "73616",
                "ATACO": "73067",
                "COYAIMA": "73217",
                "NATAGAIMA": "73483"
            }
            
            nombre_norm = str(nombre_municipio).strip().upper()
            
            # Buscar exacto
            codigo = mapeo_municipios.get(nombre_norm)
            
            # Buscar parcial si no encuentra exacto
            if codigo is None:
                for municipio, cod in mapeo_municipios.items():
                    if nombre_norm in municipio or municipio in nombre_norm:
                        codigo = cod
                        print(f"   📍 {nombre_municipio} → {municipio} ({codigo})")
                        break
            
            return codigo or "73999"
        
        if 'municipio' in df.columns:
            df['codigo_municipio'] = df['municipio'].apply(asignar_codigo_municipio)
        
        # 6. VALIDACIONES
        print("🔍 Aplicando validaciones...")
        
        registros_inicial = len(df)
        
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
        
        print(f"📊 Registros después de validaciones: {len(df):,}")
        print(f"📊 Registros filtrados: {registros_inicial - len(df):,}")
        
        # 7. ESTADÍSTICAS PRE-CARGA
        print(f"\n📊 ESTADÍSTICAS EPIZOOTIAS:")
        print(f"   Total registros: {len(df):,}")
        
        if 'municipio' in df.columns:
            municipios_unicos = df['municipio'].nunique()
            print(f"   Municipios únicos: {municipios_unicos}")
            
            # Top municipios con más epizootias
            top_municipios = df['municipio'].value_counts().head(5)
            print("   Top municipios:")
            for municipio, cantidad in top_municipios.items():
                print(f"     {municipio}: {cantidad}")
        
        if 'especie' in df.columns:
            especies = df['especie'].value_counts()
            print(f"   Especies reportadas: {len(especies)}")
            if len(especies) > 0:
                print(f"   Especie más común: {especies.index[0]} ({especies.iloc[0]} casos)")
        
        # Coordenadas válidas
        if 'latitud' in df.columns and 'longitud' in df.columns:
            coords_completas = df[['latitud', 'longitud']].dropna()
            print(f"   Registros con coordenadas: {len(coords_completas):,}")
        
        # Resultados de laboratorio
        if 'resultado_pcr' in df.columns:
            pcr_resultados = df['resultado_pcr'].value_counts()
            print(f"   Resultados PCR: {len(pcr_resultados)} diferentes")
        
        return df
        
    except Exception as e:
        print(f"❌ Error procesando epizootias: {e}")
        import traceback
        traceback.print_exc()
        return None

def cargar_epizootias_postgresql(df_epizootias, tabla="epizootias"):
    """
    Carga epizootias a PostgreSQL con soporte geoespacial
    """
    if df_epizootias is None or len(df_epizootias) == 0:
        print("❌ No hay datos de epizootias para cargar")
        return False
    
    print(f"\n💾 Cargando {len(df_epizootias):,} epizootias a PostgreSQL...")
    
    try:
        engine = create_engine(DATABASE_URL)
        
        # Verificar conexión
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        
        # Cargar datos (sin geometría primero)
        df_epizootias.to_sql(
            tabla,
            engine,
            if_exists='replace',
            index=False,
            chunksize=100
        )
        
        # Crear geometrías PostGIS para registros con coordenadas
        with engine.connect() as conn:
            print("🗺️ Creando geometrías PostGIS...")
            
            # Actualizar geometrías donde hay coordenadas válidas
            resultado = conn.execute(text(f"""
                UPDATE {tabla} 
                SET punto_geografico = ST_SetSRID(ST_MakePoint(longitud, latitud), 4326)
                WHERE latitud IS NOT NULL 
                AND longitud IS NOT NULL
                AND latitud BETWEEN -4.2 AND 12.6 
                AND longitud BETWEEN -81.8 AND -66.9
            """))
            
            filas_actualizadas = resultado.rowcount
            print(f"   ✅ {filas_actualizadas} puntos geográficos creados")
            
            # Estadísticas post-carga
            total = conn.execute(text(f"SELECT COUNT(*) FROM {tabla}")).scalar()
            print(f"✅ {total:,} epizootias cargadas exitosamente")
            
            if total > 0:
                stats = pd.read_sql(text(f"""
                    SELECT 
                        COUNT(DISTINCT codigo_municipio) as municipios,
                        COUNT(DISTINCT especie) as especies,
                        COUNT(*) FILTER (WHERE punto_geografico IS NOT NULL) as con_coordenadas,
                        COUNT(*) FILTER (WHERE resultado_pcr IS NOT NULL) as con_pcr,
                        MIN(fecha_recoleccion) as fecha_min,
                        MAX(fecha_recoleccion) as fecha_max
                    FROM {tabla}
                """), conn)
                
                if len(stats) > 0:
                    s = stats.iloc[0]
                    print(f"📊 Municipios: {s['municipios']}")
                    print(f"📊 Especies: {s['especies']}")
                    print(f"📍 Con coordenadas: {s['con_coordenadas']}")
                    print(f"🧪 Con resultados PCR: {s['con_pcr']}")
                    if s['fecha_min'] and s['fecha_max']:
                        print(f"📅 Período: {s['fecha_min']} a {s['fecha_max']}")
            
            conn.commit()
        
        return True
        
    except Exception as e:
        print(f"❌ Error cargando a PostgreSQL: {e}")
        import traceback
        traceback.print_exc()
        return False

def procesar_epizootias_completo(archivo_excel):
    """
    Proceso completo: Excel → Limpieza → PostgreSQL
    """
    print("🐒 PROCESAMIENTO COMPLETO EPIZOOTIAS")
    print("=" * 45)
    
    inicio = datetime.now()
    
    try:
        # 1. Verificar archivo
        if not os.path.exists(archivo_excel):
            print(f"❌ Archivo no encontrado: {archivo_excel}")
            return False
        
        # 2. Procesar epizootias
        df_epizootias = procesar_epizootias(archivo_excel)
        
        if df_epizootias is None:
            print("❌ Error en procesamiento de epizootias")
            return False
        
        # 3. Cargar a PostgreSQL
        exito = cargar_epizootias_postgresql(df_epizootias)
        
        # 4. Resumen final
        duracion = datetime.now() - inicio
        print(f"\n{'='*45}")
        print("PROCESAMIENTO EPIZOOTIAS COMPLETADO")
        print("=" * 45)
        
        if exito:
            print("🎉 ¡Epizootias cargadas exitosamente!")
            print(f"📊 {len(df_epizootias):,} registros procesados")
            print("🗺️ Datos geoespaciales listos para análisis")
        else:
            print("⚠️ Procesamiento con errores")
        
        print(f"⏱️ Tiempo total: {duracion.total_seconds():.1f} segundos")
        
        return exito
        
    except Exception as e:
        print(f"❌ Error crítico: {e}")
        return False

def generar_reporte_epizootias():
    """
    Genera reporte de epizootias cargadas
    """
    print("\n📊 GENERANDO REPORTE EPIZOOTIAS...")
    
    try:
        engine = create_engine(DATABASE_URL)
        
        with engine.connect() as conn:
            # Resumen general
            resumen = pd.read_sql(text("""
                SELECT 
                    COUNT(*) as total_epizootias,
                    COUNT(DISTINCT municipio) as municipios_afectados,
                    COUNT(DISTINCT especie) as especies_diferentes,
                    COUNT(*) FILTER (WHERE punto_geografico IS NOT NULL) as con_geolocalizacion,
                    COUNT(*) FILTER (WHERE resultado_pcr = 'POSITIVO') as pcr_positivos,
                    MIN(fecha_recoleccion) as primera_epizooti,
                    MAX(fecha_recoleccion) as ultima_epizooti
                FROM epizootias
            """), conn)
            
            if len(resumen) > 0:
                r = resumen.iloc[0]
                print(f"📋 RESUMEN EPIZOOTIAS:")
                print(f"   Total registros: {r['total_epizootias']:,}")
                print(f"   Municipios afectados: {r['municipios_afectados']}")
                print(f"   Especies diferentes: {r['especies_diferentes']}")
                print(f"   Con geolocalización: {r['con_geolocalizacion']:,}")
                print(f"   PCR positivos: {r['pcr_positivos']}")
                if r['primera_epizooti'] and r['ultima_epizooti']:
                    print(f"   Período: {r['primera_epizooti']} a {r['ultima_epizooti']}")
            
            # Top municipios afectados
            top_municipios = pd.read_sql(text("""
                SELECT municipio, COUNT(*) as casos
                FROM epizootias 
                GROUP BY municipio 
                ORDER BY casos DESC 
                LIMIT 5
            """), conn)
            
            if len(top_municipios) > 0:
                print(f"\n🏆 TOP MUNICIPIOS MÁS AFECTADOS:")
                for _, row in top_municipios.iterrows():
                    print(f"   {row['municipio']}: {row['casos']} casos")
            
            # Especies más afectadas
            especies = pd.read_sql(text("""
                SELECT especie, COUNT(*) as casos
                FROM epizootias 
                WHERE especie IS NOT NULL
                GROUP BY especie 
                ORDER BY casos DESC 
                LIMIT 5
            """), conn)
            
            if len(especies) > 0:
                print(f"\n🐒 ESPECIES MÁS AFECTADAS:")
                for _, row in especies.iterrows():
                    print(f"   {row['especie']}: {row['casos']} casos")
        
        return True
        
    except Exception as e:
        print(f"❌ Error generando reporte: {e}")
        return False

# ================================
# EJECUCIÓN PRINCIPAL
# ================================
if __name__ == "__main__":
    print("🐒 PROCESADOR EPIZOOTIAS")
    print("=" * 30)
    
    archivo_default = "data/epizootias.xlsx"
    
    if not os.path.exists(archivo_default):
        print(f"❌ ERROR: No se encuentra '{archivo_default}'")
        print("\n💡 Opciones:")
        print("1. Colocar archivo en 'data/epizootias.xlsx'")
        print("2. Modificar variable archivo_default")
        print("3. Llamar: procesar_epizootias_completo('ruta/archivo.xlsx')")
    else:
        exito = procesar_epizootias_completo(archivo_default)
        
        if exito:
            # Generar reporte adicional
            generar_reporte_epizootias()
            
            print("\n🎯 PRÓXIMOS PASOS:")
            print("1. Revisar datos en DBeaver: tabla 'epizootias'")
            print("2. Visualizar puntos geográficos en mapa")
            print("3. Analizar correlación con casos humanos")
            print("4. ¡Vigilancia epidemiológica completa! 🚀")