#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Script: Cargar Población SISBEN a PostgreSQL
Carga denominadores poblacionales por municipio, ubicación y grupo etario
"""

import pandas as pd
from sqlalchemy import create_engine, text
import warnings
from datetime import datetime
import os
warnings.filterwarnings('ignore')

# Configuración BD
DATABASE_URL = "postgresql://tolima_admin:tolima2025!@localhost:5432/epidemiologia_tolima"

def cargar_poblacion_postgresql(archivo_csv):
    """
    Carga población desde CSV a PostgreSQL
    """
    print("👥 CARGANDO POBLACIÓN SISBEN A POSTGRESQL")
    print("=" * 50)
    
    inicio = datetime.now()
    
    try:
        # 1. VERIFICAR ARCHIVO
        if not os.path.exists(archivo_csv):
            print(f"❌ ERROR: Archivo no encontrado: {archivo_csv}")
            return False
            
        print(f"📂 Cargando desde: {archivo_csv}")
        
        # 2. CARGAR CSV
        print("🔄 Leyendo archivo CSV...")
        df_poblacion = pd.read_csv(archivo_csv, encoding='utf-8')
        print(f"📊 Registros leídos: {len(df_poblacion):,}")
        
        # 3. VERIFICAR ESTRUCTURA
        print("🔍 Verificando estructura...")
        print(f"   Columnas: {list(df_poblacion.columns)}")
        
        # Verificar columnas esperadas
        columnas_esperadas = ['codigo_municipio', 'municipio', 'tipo_ubicacion', 
                            'grupo_etario', 'poblacion_total']
        columnas_faltantes = set(columnas_esperadas) - set(df_poblacion.columns)
        
        if columnas_faltantes:
            print(f"⚠️ Columnas faltantes: {columnas_faltantes}")
            # Intentar mapear nombres alternativos
            mapeo_columnas = {
                'codigo_mpio': 'codigo_municipio',
                'nom_municipio': 'municipio', 
                'ubicacion': 'tipo_ubicacion',
                'grupo_edad': 'grupo_etario',
                'poblacion': 'poblacion_total',
                'total': 'poblacion_total'
            }
            
            for col_alt, col_std in mapeo_columnas.items():
                if col_alt in df_poblacion.columns and col_std in columnas_faltantes:
                    df_poblacion = df_poblacion.rename(columns={col_alt: col_std})
                    print(f"   ✅ Mapeado: {col_alt} → {col_std}")
        
        # 4. LIMPIEZA Y VALIDACIÓN
        print("🧹 Limpiando y validando datos...")
        
        # Eliminar registros con datos críticos nulos
        registros_inicial = len(df_poblacion)
        df_poblacion = df_poblacion.dropna(subset=['codigo_municipio', 'poblacion_total'])
        
        # Validar códigos municipio del Tolima
        df_poblacion = df_poblacion[df_poblacion['codigo_municipio'].astype(str).str.startswith('73')]
        
        # Validar población positiva
        df_poblacion = df_poblacion[df_poblacion['poblacion_total'] > 0]
        
        print(f"   📊 Registros válidos: {len(df_poblacion):,} de {registros_inicial:,}")
        
        # Normalizar campos
        df_poblacion['tipo_ubicacion'] = df_poblacion['tipo_ubicacion'].str.title()
        df_poblacion['municipio'] = df_poblacion['municipio'].str.title()
        
        # Añadir metadatos
        df_poblacion['año'] = 2024  # Ajustar según corresponda
        df_poblacion['fuente'] = 'SISBEN'
        
        # 5. ESTADÍSTICAS PRE-CARGA
        print("\n📊 ESTADÍSTICAS PRE-CARGA:")
        print(f"   Municipios únicos: {df_poblacion['codigo_municipio'].nunique()}")
        print(f"   Grupos etarios: {sorted(df_poblacion['grupo_etario'].unique())}")
        
        ubicacion_stats = df_poblacion['tipo_ubicacion'].value_counts()
        for ubicacion, cantidad in ubicacion_stats.items():
            print(f"   {ubicacion}: {cantidad:,} registros")
        
        poblacion_total = df_poblacion['poblacion_total'].sum()
        print(f"   Población total: {poblacion_total:,} habitantes")
        
        # 6. CONECTAR Y CARGAR A POSTGRESQL
        print("\n🐘 Conectando a PostgreSQL...")
        engine = create_engine(DATABASE_URL, pool_size=5, max_overflow=10)
        
        # Verificar conexión
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        print("✅ Conexión exitosa")
        
        # 7. CARGAR DATOS
        print(f"📥 Cargando {len(df_poblacion):,} registros a tabla 'poblacion'...")
        
        df_poblacion.to_sql(
            'poblacion',
            engine,
            if_exists='replace',  # Reemplaza todo (población se actualiza poco)
            index=False,
            chunksize=1000
        )
        
        # 8. VERIFICAR CARGA Y ESTADÍSTICAS
        with engine.connect() as conn:
            # Contar registros
            total = conn.execute(text("SELECT COUNT(*) FROM poblacion")).scalar()
            print(f"✅ Registros cargados: {total:,}")
            
            # Estadísticas por ubicación
            stats_ubicacion = pd.read_sql(text("""
                SELECT tipo_ubicacion, 
                       COUNT(*) as registros,
                       SUM(poblacion_total) as poblacion_total
                FROM poblacion 
                GROUP BY tipo_ubicacion 
                ORDER BY poblacion_total DESC
            """), conn)
            
            print("\n📊 ESTADÍSTICAS POR UBICACIÓN:")
            for _, row in stats_ubicacion.iterrows():
                print(f"   {row['tipo_ubicacion']}: {row['poblacion_total']:,} habitantes "
                      f"({row['registros']:,} registros)")
            
            # Estadísticas por grupo etario
            stats_grupo = pd.read_sql(text("""
                SELECT grupo_etario, 
                       COUNT(*) as registros,
                       SUM(poblacion_total) as poblacion_total
                FROM poblacion 
                GROUP BY grupo_etario 
                ORDER BY poblacion_total DESC
            """), conn)
            
            print("\n👥 ESTADÍSTICAS POR GRUPO ETARIO:")
            for _, row in stats_grupo.iterrows():
                print(f"   {row['grupo_etario']}: {row['poblacion_total']:,} habitantes "
                      f"({row['registros']:,} registros)")
            
            # Top 10 municipios más poblados
            top_municipios = pd.read_sql(text("""
                SELECT municipio, 
                       SUM(poblacion_total) as poblacion_total
                FROM poblacion 
                GROUP BY municipio 
                ORDER BY poblacion_total DESC 
                LIMIT 10
            """), conn)
            
            print("\n🏙️ TOP 10 MUNICIPIOS MÁS POBLADOS:")
            for _, row in top_municipios.iterrows():
                print(f"   {row['municipio']}: {row['poblacion_total']:,} habitantes")
            
            # Verificar integridad referencial con unidades territoriales
            try:
                municipios_sin_referencia = conn.execute(text("""
                    SELECT COUNT(*) 
                    FROM poblacion p
                    LEFT JOIN unidades_territoriales ut ON p.codigo_municipio = ut.codigo_divipola
                    WHERE ut.codigo_divipola IS NULL
                """)).scalar()
                
                if municipios_sin_referencia > 0:
                    print(f"⚠️ {municipios_sin_referencia} registros sin referencia territorial")
                else:
                    print("✅ Integridad referencial verificada")
                    
            except Exception as e:
                print(f"⚠️ No se pudo verificar integridad referencial: {e}")
        
        # 9. RESUMEN FINAL
        duracion = datetime.now() - inicio
        print(f"\n{'='*50}")
        print("✅ CARGA COMPLETADA EXITOSAMENTE")
        print("=" * 50)
        print(f"📊 Total registros: {total:,}")
        print(f"👥 Población total: {poblacion_total:,} habitantes")
        print(f"⏱️ Tiempo total: {duracion.total_seconds():.1f} segundos")
        print("🎯 Denominadores listos para cálculo coberturas!")
        
        return True
        
    except Exception as e:
        print(f"❌ Error cargando población: {e}")
        import traceback
        traceback.print_exc()
        return False


def verificar_integridad_poblacion():
    """
    Verifica integridad de datos poblacionales
    """
    print("\n🔍 VERIFICANDO INTEGRIDAD POBLACIONAL...")
    
    try:
        engine = create_engine(DATABASE_URL)
        
        with engine.connect() as conn:
            # Verificaciones básicas
            verificaciones = {
                "total_registros": "SELECT COUNT(*) FROM poblacion",
                "sin_codigo_municipio": "SELECT COUNT(*) FROM poblacion WHERE codigo_municipio IS NULL",
                "poblacion_negativa": "SELECT COUNT(*) FROM poblacion WHERE poblacion_total <= 0",
                "municipios_unicos": "SELECT COUNT(DISTINCT codigo_municipio) FROM poblacion",
                "grupos_etarios_unicos": "SELECT COUNT(DISTINCT grupo_etario) FROM poblacion"
            }
            
            print("📊 Verificaciones integridad:")
            for nombre, query in verificaciones.items():
                try:
                    resultado = conn.execute(text(query)).scalar()
                    print(f"   {nombre}: {resultado:,}")
                except Exception as e:
                    print(f"   {nombre}: ERROR - {e}")
            
            # Verificar completitud por municipio
            completitud = pd.read_sql(text("""
                SELECT p.municipio,
                       COUNT(DISTINCT p.tipo_ubicacion) as ubicaciones,
                       COUNT(DISTINCT p.grupo_etario) as grupos_etarios,
                       SUM(p.poblacion_total) as poblacion_total
                FROM poblacion p
                GROUP BY p.municipio
                HAVING COUNT(DISTINCT p.tipo_ubicacion) < 2 OR COUNT(DISTINCT p.grupo_etario) < 3
                ORDER BY poblacion_total DESC
                LIMIT 5
            """), conn)
            
            if len(completitud) > 0:
                print(f"\n⚠️ Municipios con datos incompletos:")
                print(completitud.to_string(index=False))
            else:
                print("✅ Completitud de datos verificada")
        
        return True
        
    except Exception as e:
        print(f"❌ Error verificación: {e}")
        return False


def generar_consultas_poblacion():
    """
    Genera consultas útiles con datos poblacionales
    """
    print("\n📝 CONSULTAS ÚTILES CON POBLACIÓN...")
    
    consultas = {
        "Resumen departamental": """
            SELECT 
                SUM(poblacion_total) as poblacion_total_tolima,
                COUNT(DISTINCT codigo_municipio) as municipios,
                COUNT(*) as registros_detalle
            FROM poblacion;
        """,
        
        "Distribución urbano-rural": """
            SELECT tipo_ubicacion,
                   SUM(poblacion_total) as poblacion,
                   ROUND(SUM(poblacion_total) * 100.0 / 
                         (SELECT SUM(poblacion_total) FROM poblacion), 2) as porcentaje
            FROM poblacion 
            GROUP BY tipo_ubicacion;
        """,
        
        "Estructura por edad": """
            SELECT grupo_etario,
                   SUM(poblacion_total) as poblacion,
                   ROUND(AVG(poblacion_total), 0) as promedio_por_municipio
            FROM poblacion 
            GROUP BY grupo_etario 
            ORDER BY poblacion DESC;
        """
    }
    
    try:
        engine = create_engine(DATABASE_URL)
        
        for nombre, query in consultas.items():
            print(f"\n🔎 {nombre}:")
            try:
                resultado = pd.read_sql(text(query), engine)
                print(resultado.to_string(index=False))
            except Exception as e:
                print(f"   ERROR: {e}")
        
        return True
        
    except Exception as e:
        print(f"❌ Error generando consultas: {e}")
        return False


# ================================
# FUNCIÓN PRINCIPAL
# ================================
if __name__ == "__main__":
    print("👥 CARGADOR POBLACIÓN SISBEN → POSTGRESQL")
    print("=" * 50)
    
    # Archivo por defecto
    archivo_csv_default = "data/poblacion_tolima_20250822.csv"
    
    # Verificar archivo
    if not os.path.exists(archivo_csv_default):
        print(f"❌ ERROR: No se encuentra '{archivo_csv_default}'")
        print("\n💡 Opciones:")
        print("1. Generar archivo usando tu script de población SISBEN")
        print("2. Colocar archivo CSV en 'data/poblacion_tolima_YYYYMMDD.csv'")
        print("3. Modificar variable archivo_csv_default")
        print("4. Llamar función: cargar_poblacion_postgresql('archivo.csv')")
    else:
        print(f"📂 Procesando: {archivo_csv_default}")
        
        # Ejecutar carga completa
        exito = cargar_poblacion_postgresql(archivo_csv_default)
        
        if exito:
            print("\n🔧 Ejecutando verificaciones...")
            verificar_integridad_poblacion()
            
            print("\n📊 Generando consultas útiles...")
            generar_consultas_poblacion()
            
            print(f"\n🎯 PRÓXIMOS PASOS:")
            print("1. Cargar datos de vacunación con script paiweb_postgresql.py")
            print("2. Verificar vista v_coberturas_dashboard en DBeaver")
            print("3. Calcular coberturas de vacunación por municipio")
            print("4. ¡Análisis epidemiológicos completos! 🚀")
        else:
            print("\n❌ Carga fallida. Revisar errores arriba.")