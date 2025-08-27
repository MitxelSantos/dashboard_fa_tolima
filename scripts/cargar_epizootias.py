#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
cargar_epizootias.py - Epizootias → PostgreSQL
Procesamiento de epizootias (muertes animales) con mapeo veredal desde .gpkg
CORREGIDO: Contexto municipal para veredas, mapeos locales, sin campos calculados
"""

import pandas as pd
import numpy as np
from datetime import datetime, date
from sqlalchemy import create_engine, text
import warnings
import os

# Importar configuración centralizada
from config import (
    DATABASE_URL,
    limpiar_fecha_robusta, cargar_primera_hoja_excel,
    buscar_codigo_vereda, buscar_codigo_municipio,
    normalizar_nombre_territorio
)

warnings.filterwarnings('ignore')

# ================================
# MAPEO LOCAL EPIZOOTIAS EXCEL (Solo para este script)
# ================================
MAPEO_EPIZOOTIAS_EXCEL = {
    'municipio': 'MUNICIPIO',
    'vereda': 'VEREDA',
    'fecha_recoleccion': 'FECHA_RECOLECCION',
    'informante': 'INFORMANTE',
    'descripcion': 'DESCRIPCION',
    'fecha_notificacion': 'FECHA_NOTIFICACION',
    'especie': 'ESPECIE',
    'latitud': 'LATITUD',
    'longitud': 'LONGITUD',
    'fecha_envio_muestra': 'FECHA_ENVIO_MUESTRA',
    'resultado_pcr': 'RESULTADO_PCR',
    'fecha_resultado_pcr': 'FECHA_RESULTADO_PCR',
    'resultado_histopatologia': 'RESULTADO_HISTOPATOLOGIA',
    'fecha_resultado_histopatologia': 'FECHA_RESULTADO_HISTOPATOLOGIA'
}

def procesar_epizootias(archivo_excel):
    """
    Procesa epizootias desde Excel con mapeo veredal completo
    CORREGIDO: Contexto municipal para búsqueda veredal, sin campos calculados
    """
    print("🐒 PROCESANDO EPIZOOTIAS")
    print("=" * 30)
    
    inicio = datetime.now()
    
    try:
        # 1. CARGAR ARCHIVO EXCEL (primera hoja)
        print(f"📂 Cargando: {archivo_excel}")
        
        df, nombre_hoja = cargar_primera_hoja_excel(archivo_excel)
        if df is None:
            return None
            
        print(f"📊 Registros iniciales: {len(df):,}")
        print(f"📋 Columnas originales: {list(df.columns)}")
        
        # 2. MAPEAR COLUMNAS USANDO MAPEO LOCAL ESPECÍFICO
        print("🔄 Mapeando columnas...")
        
        columnas_mapeadas = {}
        columnas_no_encontradas = []
        
        # Usar mapeo local específico para epizootias
        for nombre_bd, nombre_excel in MAPEO_EPIZOOTIAS_EXCEL.items():
            if nombre_excel in df.columns:
                columnas_mapeadas[nombre_excel] = nombre_bd
                print(f"   ✅ {nombre_excel} → {nombre_bd}")
            else:
                columnas_no_encontradas.append(nombre_excel)
                print(f"   ⚠️ {nombre_excel} → NO ENCONTRADA")
        
        # Renombrar columnas encontradas
        df = df.rename(columns=columnas_mapeadas)
        
        # Mantener todas las columnas mapeadas
        columnas_finales = list(columnas_mapeadas.values())
        df = df[columnas_finales].copy()
        
        print(f"✅ {len(columnas_finales)} columnas procesadas")
        print(f"📋 Columnas finales: {list(df.columns)}")
        
        # 3. NORMALIZAR MUNICIPIOS Y VEREDAS
        print("🏙️ Normalizando territorios...")
        
        # Normalizar municipios
        if 'municipio' in df.columns:
            df['municipio'] = df['municipio'].apply(
                lambda x: normalizar_nombre_territorio(x).title() if pd.notna(x) else None
            )
            municipios_unicos = df['municipio'].nunique()
            print(f"   Municipios únicos: {municipios_unicos}")
        
        # Normalizar veredas
        if 'vereda' in df.columns:
            df['vereda'] = df['vereda'].apply(
                lambda x: normalizar_nombre_territorio(x).title() if pd.notna(x) else None
            )
            veredas_unicas = df['vereda'].nunique()
            print(f"   Veredas únicas: {veredas_unicas}")
        
        # 4. LIMPIAR Y VALIDAR FECHAS
        print("📅 Procesando fechas...")
        
        campos_fecha = [
            'fecha_recoleccion', 'fecha_notificacion', 'fecha_envio_muestra',
            'fecha_resultado_pcr', 'fecha_resultado_histopatologia'
        ]
        
        fechas_procesadas = []
        for campo in campos_fecha:
            if campo in df.columns:
                df[campo] = df[campo].apply(limpiar_fecha_robusta)
                fechas_nulas = df[campo].isna().sum()
                print(f"   {campo}: {fechas_nulas:,} nulas")
                fechas_procesadas.append(campo)
        
        print(f"   ✅ {len(fechas_procesadas)} tipos de fecha procesados")
        
        # 5. PROCESAR COORDENADAS GEOGRÁFICAS
        print("📍 Procesando coordenadas...")
        
        def limpiar_coordenada(coord_val):
            """Limpia y valida coordenadas"""
            if pd.isna(coord_val):
                return None
            
            try:
                # Convertir a float, manejando diferentes formatos
                coord_str = str(coord_val).strip().replace(',', '.')
                coord_float = float(coord_str)
                return coord_float
            except (ValueError, TypeError):
                return None
        
        # Limpiar coordenadas
        coordenadas_procesadas = 0
        if 'latitud' in df.columns:
            df['latitud'] = df['latitud'].apply(limpiar_coordenada)
            coordenadas_procesadas += 1
            
        if 'longitud' in df.columns:
            df['longitud'] = df['longitud'].apply(limpiar_coordenada)
            coordenadas_procesadas += 1
        
        # Validar coordenadas para Colombia
        if 'latitud' in df.columns and 'longitud' in df.columns:
            # Rangos válidos para Colombia
            lat_validas = df['latitud'].between(-4.2, 12.6, na=True)
            lon_validas = df['longitud'].between(-81.8, -66.9, na=True)
            coords_validas = lat_validas & lon_validas
            
            coords_completas = df[['latitud', 'longitud']].dropna()
            coords_validas_count = coords_validas.sum()
            coords_invalidas = len(coords_completas) - coords_validas_count
            
            print(f"   Coordenadas completas: {len(coords_completas):,}")
            print(f"   Coordenadas válidas: {coords_validas_count:,}")
            if coords_invalidas > 0:
                print(f"   ⚠️ Coordenadas inválidas: {coords_invalidas:,}")
        
        # 6. ASIGNAR CÓDIGOS DIVIPOLA CON CONTEXTO MUNICIPAL
        print("🗺️ Asignando códigos DIVIPOLA con contexto municipal...")
        
        # Asignar código municipal
        if 'municipio' in df.columns:
            df['codigo_municipio'] = df['municipio'].apply(buscar_codigo_municipio)
            codigos_municipales = df['codigo_municipio'].notna().sum()
            print(f"   Códigos municipales: {codigos_municipales:,}")
        
        # CORREGIDO: Asignar código veredal CON CONTEXTO MUNICIPAL
        if 'vereda' in df.columns and 'municipio' in df.columns:
            print("   🗺️ Aplicando búsqueda veredal con contexto municipal...")
            
            def buscar_codigo_vereda_con_contexto(vereda, municipio_ctx):
                """Busca código veredal usando contexto municipal (CORREGIDO)"""
                if pd.isna(vereda):
                    return None
                # Usar contexto municipal para reducir búsqueda
                return buscar_codigo_vereda(vereda, municipio_ctx)
            
            # Aplicar búsqueda veredal con contexto municipal
            df['codigo_divipola_vereda'] = df.apply(
                lambda row: buscar_codigo_vereda_con_contexto(
                    row.get('vereda'),
                    row.get('municipio')  # Usar municipio como contexto
                ), axis=1
            )
            
            codigos_veredales = df['codigo_divipola_vereda'].notna().sum()
            print(f"   ✅ Códigos veredales con contexto municipal: {codigos_veredales:,}")
            
            # Mostrar estadísticas de mapeo por municipio
            if codigos_veredales > 0:
                mapeo_stats = df.groupby('municipio').agg({
                    'vereda': 'count',
                    'codigo_divipola_vereda': 'count'
                }).rename(columns={
                    'vereda': 'total_veredas',
                    'codigo_divipola_vereda': 'veredas_mapeadas'
                })
                mapeo_stats['porcentaje_mapeo'] = (mapeo_stats['veredas_mapeadas'] / mapeo_stats['total_veredas'] * 100).round(1)
                
                print(f"   📊 Mapeo veredal por municipio:")
                for municipio, row in mapeo_stats.head(10).iterrows():
                    if pd.notna(municipio):
                        print(f"     {municipio}: {row['veredas_mapeadas']}/{row['total_veredas']} ({row['porcentaje_mapeo']}%)")
        
        # 7. NORMALIZAR ESPECIES Y RESULTADOS
        print("🔬 Normalizando especies y resultados...")
        
        # Normalizar especies
        if 'especie' in df.columns:
            df['especie'] = df['especie'].apply(
                lambda x: str(x).strip().title() if pd.notna(x) else None
            )
            especies_unicas = df['especie'].nunique()
            print(f"   Especies únicas: {especies_unicas}")
            
            # Mostrar especies más comunes
            if especies_unicas > 0:
                top_especies = df['especie'].value_counts().head(3)
                print(f"   Especies más comunes:")
                for especie, cantidad in top_especies.items():
                    print(f"     {especie}: {cantidad:,}")
        
        # Normalizar resultados PCR
        if 'resultado_pcr' in df.columns:
            df['resultado_pcr'] = df['resultado_pcr'].apply(
                lambda x: str(x).strip().upper() if pd.notna(x) else None
            )
            
            # Estadísticas resultados PCR
            if df['resultado_pcr'].notna().sum() > 0:
                resultados_pcr = df['resultado_pcr'].value_counts()
                print(f"   Resultados PCR:")
                for resultado, cantidad in resultados_pcr.items():
                    print(f"     {resultado}: {cantidad:,}")
        
        # 8. VALIDACIONES Y FILTROS
        print("🔍 Aplicando validaciones...")
        
        registros_iniciales = len(df)
        
        # Filtrar registros con municipio válido
        if 'municipio' in df.columns:
            df = df.dropna(subset=['municipio'])
            print(f"   Filtro municipio: {len(df):,} registros")
        
        # Filtrar fechas válidas para recolección
        if 'fecha_recoleccion' in df.columns:
            fecha_min = date(2020, 1, 1)
            fecha_max = date.today()
            
            # Mantener registros con fecha válida o nula
            df = df[
                (df['fecha_recoleccion'].isna()) | 
                ((df['fecha_recoleccion'] >= fecha_min) & 
                 (df['fecha_recoleccion'] <= fecha_max))
            ]
            print(f"   Filtro fecha: {len(df):,} registros")
        
        print(f"   Registros excluidos: {registros_iniciales - len(df):,}")
        
        # 9. MANTENER DATOS ORIGINALES (CORREGIDO - Sin campos calculados)
        print("⚙️ Manteniendo datos originales...")
        print("   ✅ No se generan campos calculados - datos originales preservados")
        
        # 10. ESTADÍSTICAS FINALES
        print(f"\n📊 ESTADÍSTICAS EPIZOOTIAS PROCESADAS:")
        print(f"   Total registros: {len(df):,}")
        
        if 'municipio' in df.columns:
            municipios_afectados = df['municipio'].nunique()
            print(f"   Municipios afectados: {municipios_afectados}")
            
            # Top municipios con más epizootias
            top_municipios = df['municipio'].value_counts().head(5)
            print(f"   Top municipios:")
            for municipio, cantidad in top_municipios.items():
                print(f"     {municipio}: {cantidad:,}")
        
        if 'especie' in df.columns and df['especie'].notna().sum() > 0:
            especies_reportadas = df['especie'].nunique()
            print(f"   Especies reportadas: {especies_reportadas}")
            
            especie_comun = df['especie'].value_counts().iloc[0]
            nombre_especie = df['especie'].value_counts().index[0]
            print(f"   Especie más afectada: {nombre_especie} ({especie_comun} casos)")
        
        # Coordenadas válidas
        if 'latitud' in df.columns and 'longitud' in df.columns:
            coords_disponibles = df[['latitud', 'longitud']].dropna()
            print(f"   Con coordenadas: {len(coords_disponibles):,}")
        
        # Resultados de laboratorio
        if 'resultado_pcr' in df.columns:
            pcr_procesados = df['resultado_pcr'].notna().sum()
            print(f"   Resultados PCR: {pcr_procesados:,}")
            
            if pcr_procesados > 0:
                positivos = (df['resultado_pcr'] == 'POSITIVO').sum()
                if positivos > 0:
                    print(f"   ⚠️ PCR Positivos: {positivos:,}")
        
        # Mapeo veredal exitoso
        if 'codigo_divipola_vereda' in df.columns:
            veredas_mapeadas = df['codigo_divipola_vereda'].notna().sum()
            print(f"   🗺️ Veredas con código DIVIPOLA: {veredas_mapeadas:,}")
        
        print("✅ Procesamiento epizootias completado")
        print("🗺️ Códigos veredales asignados con contexto municipal (CORREGIDO)")
        
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
        print("❌ No hay epizootias para cargar")
        return False
    
    print(f"\n💾 CARGANDO {len(df_epizootias):,} EPIZOOTIAS A POSTGRESQL")
    print("=" * 55)
    
    try:
        engine = create_engine(DATABASE_URL)
        
        # Verificar conexión
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        print("✅ Conexión PostgreSQL exitosa")
        
        # Añadir metadatos
        df_epizootias['created_at'] = datetime.now()
        df_epizootias['updated_at'] = datetime.now()
        
        # Cargar datos (sin geometrías primero)
        df_epizootias.to_sql(
            tabla,
            engine,
            if_exists='replace',
            index=False,
            chunksize=100
        )
        
        # Crear geometrías PostGIS para registros con coordenadas válidas
        with engine.connect() as conn:
            print("🗺️ Creando geometrías PostGIS...")
            
            # Actualizar geometrías donde hay coordenadas válidas
            geometrias_creadas = conn.execute(text(f"""
                UPDATE {tabla} 
                SET punto_geografico = ST_SetSRID(ST_MakePoint(longitud, latitud), 4326)
                WHERE latitud IS NOT NULL 
                AND longitud IS NOT NULL
                AND latitud BETWEEN -4.2 AND 12.6 
                AND longitud BETWEEN -81.8 AND -66.9
            """))
            
            filas_geometria = geometrias_creadas.rowcount
            print(f"   ✅ {filas_geometria} puntos geográficos creados")
            
            # Estadísticas post-carga completas
            total_cargado = conn.execute(text(f"SELECT COUNT(*) FROM {tabla}")).scalar()
            print(f"✅ {total_cargado:,} epizootias cargadas exitosamente")
            
            if total_cargado > 0:
                stats = pd.read_sql(text(f"""
                    SELECT 
                        COUNT(DISTINCT codigo_municipio) as municipios,
                        COUNT(DISTINCT especie) as especies,
                        COUNT(*) FILTER (WHERE punto_geografico IS NOT NULL) as con_coordenadas,
                        COUNT(*) FILTER (WHERE resultado_pcr IS NOT NULL) as con_pcr,
                        COUNT(*) FILTER (WHERE resultado_pcr = 'POSITIVO') as pcr_positivos,
                        COUNT(*) FILTER (WHERE codigo_divipola_vereda IS NOT NULL) as con_codigo_veredal,
                        MIN(fecha_recoleccion) as fecha_min,
                        MAX(fecha_recoleccion) as fecha_max
                    FROM {tabla}
                """), conn)
                
                if len(stats) > 0:
                    s = stats.iloc[0]
                    print(f"📍 Municipios: {s['municipios']}")
                    print(f"🐒 Especies: {s['especies']}")
                    print(f"📍 Con coordenadas: {s['con_coordenadas']}")
                    print(f"🧪 Con PCR: {s['con_pcr']}")
                    print(f"⚠️ PCR Positivos: {s['pcr_positivos']}")
                    print(f"🗺️ Con código veredal: {s['con_codigo_veredal']}")
                    
                    if s['fecha_min'] and s['fecha_max']:
                        print(f"📅 Período: {s['fecha_min']} a {s['fecha_max']}")
            
            # Crear índices espaciales adicionales
            print("🔧 Creando índices...")
            try:
                conn.execute(text(f"""
                    CREATE INDEX IF NOT EXISTS idx_{tabla}_punto_geo 
                    ON {tabla} USING GIST(punto_geografico)
                """))
                conn.execute(text(f"""
                    CREATE INDEX IF NOT EXISTS idx_{tabla}_municipio 
                    ON {tabla}(codigo_municipio)
                """))
                conn.execute(text(f"""
                    CREATE INDEX IF NOT EXISTS idx_{tabla}_vereda 
                    ON {tabla}(codigo_divipola_vereda)
                """))
                conn.execute(text(f"""
                    CREATE INDEX IF NOT EXISTS idx_{tabla}_fecha 
                    ON {tabla}(fecha_recoleccion)
                """))
                conn.commit()
                print("✅ Índices creados/verificados")
            except Exception as e:
                print(f"⚠️ Error creando índices: {e}")
        
        return True
        
    except Exception as e:
        print(f"❌ Error cargando a PostgreSQL: {e}")
        import traceback
        traceback.print_exc()
        return False

def procesar_epizootias_completo(archivo_excel):
    """
    Proceso completo: Excel → Procesamiento → PostgreSQL
    """
    print("🐒 PROCESAMIENTO COMPLETO EPIZOOTIAS V2.0")
    print("=" * 45)
    
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
        print(" PROCESAMIENTO EPIZOOTIAS COMPLETADO ".center(45))
        print("=" * 45)
        
        if exito:
            print("🎉 ¡EPIZOOTIAS CARGADAS EXITOSAMENTE!")
            print(f"📊 {len(df_epizootias):,} registros procesados")
            print("🗺️ Códigos veredales con contexto municipal (CORREGIDO)")
            print("📍 Datos geoespaciales optimizados")
            print("🔬 Resultados laboratorio organizados")
            print("📈 Datos originales preservados")
        else:
            print("⚠️ Procesamiento con errores en carga BD")
        
        print(f"⏱️ Tiempo total: {duracion.total_seconds():.1f} segundos")
        print(f"📅 Finalizado: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        # 5. Crear backup CSV
        if exito:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            backup_file = f"backups/epizootias_backup_{timestamp}.csv"
            
            os.makedirs("backups", exist_ok=True)
            df_epizootias.to_csv(backup_file, index=False, encoding='utf-8-sig')
            print(f"💾 Backup creado: {backup_file}")
        
        return exito
        
    except Exception as e:
        print(f"❌ Error crítico: {e}")
        import traceback
        traceback.print_exc()
        return False

def generar_reporte_epizootias():
    """
    Genera reporte epidemiológico de epizootias
    """
    print("\n📊 GENERANDO REPORTE EPIDEMIOLÓGICO EPIZOOTIAS...")
    
    try:
        engine = create_engine(DATABASE_URL)
        
        with engine.connect() as conn:
            # Resumen general
            resumen = pd.read_sql(text("""
                SELECT 
                    COUNT(*) as total_epizootias,
                    COUNT(DISTINCT municipio) as municipios_afectados,
                    COUNT(DISTINCT especie) as especies_afectadas,
                    COUNT(*) FILTER (WHERE punto_geografico IS NOT NULL) as con_geolocalizacion,
                    COUNT(*) FILTER (WHERE resultado_pcr = 'POSITIVO') as pcr_positivos,
                    COUNT(*) FILTER (WHERE codigo_divipola_vereda IS NOT NULL) as con_codigo_veredal,
                    MIN(fecha_recoleccion) as primera_epizooti,
                    MAX(fecha_recoleccion) as ultima_epizooti
                FROM epizootias
            """), conn)
            
            if len(resumen) > 0:
                r = resumen.iloc[0]
                print(f"📋 RESUMEN EPIZOOTIAS:")
                print(f"   Total registros: {r['total_epizootias']:,}")
                print(f"   Municipios afectados: {r['municipios_afectados']}")
                print(f"   Especies afectadas: {r['especies_afectadas']}")
                print(f"   Con geolocalización: {r['con_geolocalizacion']:,}")
                print(f"   PCR positivos: {r['pcr_positivos']}")
                print(f"   Con código veredal: {r['con_codigo_veredal']:,}")
                
                if r['primera_epizooti'] and r['ultima_epizooti']:
                    print(f"   Período: {r['primera_epizooti']} a {r['ultima_epizooti']}")
            
            # Municipios más afectados
            municipios_afectados = pd.read_sql(text("""
                SELECT municipio, COUNT(*) as casos
                FROM epizootias 
                WHERE municipio IS NOT NULL
                GROUP BY municipio 
                ORDER BY casos DESC 
                LIMIT 5
            """), conn)
            
            if len(municipios_afectados) > 0:
                print(f"\n🏆 TOP 5 MUNICIPIOS MÁS AFECTADOS:")
                for _, row in municipios_afectados.iterrows():
                    print(f"   {row['municipio']}: {row['casos']} casos")
            
            # Especies más afectadas
            especies_afectadas = pd.read_sql(text("""
                SELECT especie, COUNT(*) as casos
                FROM epizootias 
                WHERE especie IS NOT NULL
                GROUP BY especie 
                ORDER BY casos DESC 
                LIMIT 5
            """), conn)
            
            if len(especies_afectadas) > 0:
                print(f"\n🐒 ESPECIES MÁS AFECTADAS:")
                for _, row in especies_afectadas.iterrows():
                    print(f"   {row['especie']}: {row['casos']} casos")
            
            # Análisis temporal si hay datos
            temporal = pd.read_sql(text("""
                SELECT EXTRACT(YEAR FROM fecha_recoleccion) as año, COUNT(*) as casos
                FROM epizootias 
                WHERE fecha_recoleccion IS NOT NULL
                GROUP BY EXTRACT(YEAR FROM fecha_recoleccion)
                ORDER BY año DESC
                LIMIT 5
            """), conn)
            
            if len(temporal) > 0:
                print(f"\n📅 DISTRIBUCIÓN TEMPORAL:")
                for _, row in temporal.iterrows():
                    if pd.notna(row['año']):
                        print(f"   {int(row['año'])}: {row['casos']} casos")
        
        return True
        
    except Exception as e:
        print(f"❌ Error generando reporte: {e}")
        return False

# ================================
# FUNCIÓN PRINCIPAL
# ================================
if __name__ == "__main__":
    print("🐒 PROCESADOR EPIZOOTIAS V2.0")
    print("=" * 30)
    
    # Archivo por defecto
    archivo_default = "data/epizootias.xlsx"
    
    # Verificar archivo
    if not os.path.exists(archivo_default):
        print(f"❌ ERROR: No se encuentra '{archivo_default}'")
        print("\n💡 Opciones:")
        print("1. Colocar archivo de epizootias en 'data/epizootias.xlsx'")
        print("2. Modificar variable archivo_default")
        print("3. Llamar: procesar_epizootias_completo('ruta/archivo.xlsx')")
    else:
        # Ejecutar procesamiento completo
        exito = procesar_epizootias_completo(archivo_default)
        
        if exito:
            print("\n📊 Generando reporte epidemiológico...")
            generar_reporte_epizootias()
            
            print("\n🎯 PRÓXIMOS PASOS:")
            print("1. Revisar datos en DBeaver: tabla 'epizootias'")
            print("2. Visualizar puntos geográficos en mapa")
            print("3. Analizar correlación espacial con casos humanos")
            print("4. Identificar clusters de mortalidad animal")
            print("5. ¡Vigilancia epidemiológica integrada! 🚀")
        else:
            print("\n❌ Procesamiento fallido. Revisar errores.")