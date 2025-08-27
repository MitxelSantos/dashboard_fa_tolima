#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
cargar_casos.py - Casos Fiebre Amarilla → PostgreSQL
Procesamiento completo de casos con mapeo veredal desde .gpkg
CORREGIDO: Municipio procedencia (nmun_proce), edad con fecha actual, mapeos locales
"""

import pandas as pd
import numpy as np
from datetime import datetime, date
from dateutil.relativedelta import relativedelta
from sqlalchemy import create_engine, text
import warnings
import os

# Importar configuración centralizada
from config import (
    DATABASE_URL,
    clasificar_grupo_etario, calcular_edad_en_meses,
    limpiar_fecha_robusta, cargar_primera_hoja_excel,
    buscar_codigo_vereda, normalizar_nombre_territorio
)

warnings.filterwarnings('ignore')

# ================================
# MAPEO LOCAL CASOS EXCEL (Solo para este script)
# ================================
MAPEO_CASOS_EXCEL = {
    'fecha_notificacion': 'fec_not',
    'semana_epidemiologica': 'semana',
    'codigo_prestador': 'cod_pre',
    'primer_nombre': 'pri_nom_',
    'segundo_nombre': 'seg_nom_',
    'primer_apellido': 'pri_ape_',
    'segundo_apellido': 'seg_ape_',
    'tipo_documento': 'tip_ide_',
    'numero_documento': 'num_ide_',
    'edad': 'edad_',
    'sexo': 'sexo_',
    'area_residencia': 'area_',
    'vereda_infeccion': 'vereda_',  # CORREGIDO: vereda donde se infectó
    'ocupacion': 'ocupacion_',
    'tipo_seguridad_social': 'tip_ss_',
    'codigo_aseguradora': 'cod_ase_',
    'pertenencia_etnica': 'per_etn_',
    'estrato': 'estrato_',
    'grupo_discapacidad': 'gp_discapacidad',
    'grupo_desplazado': 'gp_desplazado',
    'grupo_migrante': 'gp_migrante',
    'grupo_carcelario': 'gp_carcela',
    'grupo_gestante': 'gp_gestante',
    'semanas_gestacion': 'sem_ges_',
    'grupo_indigena': 'gp_indigena',
    'poblacion_icbf': 'gp_pobicbf',
    'madres_comunitarias': 'gp_mad_com',
    'grupo_desmovilizados': 'gp_desmovim',
    'grupo_psiquiatricos': 'gp_psiquiatr',
    'victimas_violencia': 'gp_vic_viol',
    'grupo_otros': 'gp_otros',
    'fuente_informacion': 'fuente_',
    'fecha_consulta': 'fec_con_',
    'inicio_sintomas': 'ini_sin_',
    'tipo_caso': 'tip_cas_',
    'paciente_hospitalizado': 'pac_hos_',
    'fecha_hospitalizacion': 'fec_hos_',
    'condicion_final': 'con_fin_',
    'fecha_defuncion': 'fec_def_',
    'telefono': 'telefono_',
    'fecha_nacimiento': 'fecha_nto_',
    'certificado_defuncion': 'cer_def_',
    'carnet_vacunacion': 'carne_vacu',
    'fecha_vacunacion': 'fec_fa1_',
    'fiebre': 'fiebre',
    'mialgias': 'malgias',
    'artralgias': 'artralgias_',
    'cefalea': 'cefalea',
    'vomitos': 'vomito',
    'ictericia': 'ictericia',
    'sangrado': 'sfaget',
    'oliguria': 'oliguria',
    'shock': 'shock',
    'bradicardia': 'bradicardi',
    'falla_renal': 'falla_renal',
    'falla_hepatica': 'falla_hepa',
    'hepatomegalia': 'hepatomega',
    'hemoptisis': 'hemoptisis',
    'hiperemia': 'hipiremia',
    'hematemesis': 'hematemesi',
    'petequias': 'petequias',
    'metrorragia': 'metrorragi',
    'melenas': 'melenas',
    'equimosis': 'equimosis',
    'epistaxis': 'epistaxis',
    'hematuria': 'hematuria',
    'caso_familiar': 'cas_fam',
    'codigo_municipio_infeccion': 'codmuninfe',
    'nombre_upgd': 'nom_upgd',
    'pais_procedencia': 'npais_procen',
    'departamento_procedencia': 'ndep_proce',
    'municipio_procedencia': 'nmun_proce',  # CORREGIDO: municipio donde se infectó
    'pais_residencia': 'npais_resi',
    'departamento_residencia': 'ndep_resi',
    'municipio_residencia': 'nmun_resi',
    'departamento_notificacion': 'ndep_notif',
    'municipio_notificacion': 'nmun_notif'
}

def procesar_casos_fiebre_amarilla(archivo_excel):
    """
    Procesa casos de fiebre amarilla desde Excel con mapeo completo
    CORREGIDO: Municipio procedencia, edad con fecha actual
    """
    print("🦠 PROCESANDO CASOS FIEBRE AMARILLA")
    print("=" * 40)
    
    inicio = datetime.now()
    
    try:
        # 1. CARGAR ARCHIVO EXCEL (primera hoja)
        print(f"📂 Cargando: {archivo_excel}")
        
        df, nombre_hoja = cargar_primera_hoja_excel(archivo_excel)
        if df is None:
            return None
            
        print(f"📊 Registros iniciales: {len(df):,}")
        print(f"📋 Columnas originales: {len(df.columns)}")
        
        # 2. MAPEAR TODAS LAS COLUMNAS DISPONIBLES (mapeo local)
        print("🔄 Mapeando columnas disponibles...")
        
        columnas_mapeadas = {}
        columnas_no_encontradas = []
        
        # Usar mapeo local específico para casos
        for nombre_bd, nombre_excel in MAPEO_CASOS_EXCEL.items():
            if nombre_excel in df.columns:
                columnas_mapeadas[nombre_excel] = nombre_bd
                print(f"   ✅ {nombre_excel} → {nombre_bd}")
            else:
                columnas_no_encontradas.append(nombre_excel)
        
        # Renombrar columnas encontradas
        df = df.rename(columns=columnas_mapeadas)
        
        # Mantener todas las columnas mapeadas (no filtrar)
        columnas_finales = list(columnas_mapeadas.values())
        df = df[columnas_finales].copy()
        
        print(f"✅ {len(columnas_finales)} columnas mapeadas")
        if columnas_no_encontradas:
            print(f"⚠️ {len(columnas_no_encontradas)} columnas no encontradas")
        
        # 3. LIMPIAR Y VALIDAR FECHAS CRÍTICAS
        print("📅 Procesando fechas...")
        
        # Fecha de notificación
        if 'fecha_notificacion' in df.columns:
            df['fecha_notificacion'] = df['fecha_notificacion'].apply(limpiar_fecha_robusta)
            print(f"   Fecha notificación procesada")
        
        # Fecha inicio síntomas (CRÍTICA para epidemiología)
        if 'inicio_sintomas' in df.columns:
            df['inicio_sintomas'] = df['inicio_sintomas'].apply(limpiar_fecha_robusta)
            print(f"   Fecha inicio síntomas procesada")
        
        # Fecha de nacimiento (para cálculo edad)
        if 'fecha_nacimiento' in df.columns:
            df['fecha_nacimiento'] = df['fecha_nacimiento'].apply(limpiar_fecha_robusta)
            print(f"   Fecha nacimiento procesada")
        
        # Otras fechas importantes
        fechas_adicionales = [
            'fecha_consulta', 'fecha_hospitalizacion', 'fecha_defuncion',
            'fecha_vacunacion'
        ]
        
        for campo_fecha in fechas_adicionales:
            if campo_fecha in df.columns:
                df[campo_fecha] = df[campo_fecha].apply(limpiar_fecha_robusta)
        
        # 4. CALCULAR EDAD CON FECHA ACTUAL (CORREGIDO)
        print("👤 Calculando edad con fecha ACTUAL como referencia...")
        
        if 'fecha_nacimiento' in df.columns:
            fecha_referencia = date.today()  # CORREGIDO: Siempre fecha actual
            
            def calcular_edad_caso_actual(fecha_nac):
                """Calcula edad usando SOLO fecha actual como referencia"""
                if pd.isna(fecha_nac):
                    return None, None
                
                # SIEMPRE usar fecha actual, NO fecha inicio síntomas
                edad_meses = calcular_edad_en_meses(fecha_nac, fecha_referencia)
                if edad_meses is not None:
                    edad_anos = edad_meses / 12
                    return edad_anos, edad_meses
                
                return None, None
            
            # Calcular edad con fecha actual únicamente
            edades_data = df['fecha_nacimiento'].apply(calcular_edad_caso_actual)
            
            df['edad_calculada_anos'] = [x[0] if x else None for x in edades_data]
            df['edad_calculada_meses'] = [x[1] if x else None for x in edades_data]
            
            # Usar edad calculada o edad del archivo
            df['edad_final'] = df['edad_calculada_anos'].fillna(df.get('edad', np.nan))
            
            print(f"   ✅ Edad calculada con FECHA ACTUAL como referencia")
            print(f"   📅 Fecha referencia: {fecha_referencia}")
        else:
            # Si no hay fecha nacimiento, usar edad directa del archivo
            df['edad_final'] = df.get('edad', np.nan)
            print(f"   ⚠️ Usando edad directa del archivo (sin fecha nacimiento)")
        
        # 5. CLASIFICAR GRUPOS ETARIOS
        print("👥 Clasificando grupos etarios...")
        
        # Convertir edad a meses para clasificación
        df['edad_meses_clasificacion'] = df['edad_final'] * 12
        df['grupo_etario'] = df['edad_meses_clasificacion'].apply(clasificar_grupo_etario)
        
        grupos_dist = df['grupo_etario'].value_counts()
        print(f"   Distribución grupos etarios:")
        for grupo, cantidad in grupos_dist.head().items():
            print(f"     {grupo}: {cantidad:,}")
        
        # 6. MAPEAR CÓDIGO DIVIPOLA VEREDAL CON MUNICIPIO PROCEDENCIA
        print("🗺️ Mapeando códigos DIVIPOLA veredales con municipio procedencia...")
        
        if 'vereda_infeccion' in df.columns:
            # CORREGIDO: Usar municipio_procedencia como contexto (donde se infectó)
            municipio_contexto = df.get('municipio_procedencia')
            
            def buscar_codigo_vereda_caso(vereda, municipio_proc):
                """Busca código veredal usando municipio procedencia como contexto"""
                if pd.isna(vereda):
                    return None
                return buscar_codigo_vereda(vereda, municipio_proc)
            
            # Aplicar búsqueda veredal con contexto de municipio procedencia
            if municipio_contexto is not None:
                df['codigo_divipola_vereda'] = df.apply(
                    lambda row: buscar_codigo_vereda_caso(
                        row.get('vereda_infeccion'),
                        row.get('municipio_procedencia')  # CORREGIDO: usar procedencia
                    ), axis=1
                )
            else:
                df['codigo_divipola_vereda'] = df['vereda_infeccion'].apply(
                    lambda x: buscar_codigo_vereda_caso(x, None)
                )
            
            codigos_veredales_asignados = df['codigo_divipola_vereda'].notna().sum()
            print(f"   ✅ Códigos veredales asignados: {codigos_veredales_asignados:,}")
            print(f"   🗺️ Contexto: municipio procedencia (donde se infectó)")
        
        # 7. NORMALIZAR CAMPOS CATEGÓRICOS
        print("🔧 Normalizando campos categóricos...")
        
        # Normalizar nombres de municipios (procedencia, residencia, notificación)
        campos_municipio = [
            'municipio_procedencia',     # DONDE SE INFECTÓ (PRINCIPAL)
            'municipio_residencia',      # Donde vive
            'municipio_notificacion'     # Donde se notificó
        ]
        
        for campo in campos_municipio:
            if campo in df.columns:
                df[campo] = df[campo].apply(
                    lambda x: normalizar_nombre_territorio(x).title() if pd.notna(x) else None
                )
        
        # Normalizar condición final (1=Vivo, 2=Muerto)
        if 'condicion_final' in df.columns:
            df['condicion_final_texto'] = df['condicion_final'].map({
                1: 'Vivo',
                2: 'Muerto'
            }).fillna(df['condicion_final'])
        
        # Normalizar carnet vacunación (1=Sí, 2=No)  
        if 'carnet_vacunacion' in df.columns:
            df['vacunado_previo'] = df['carnet_vacunacion'].map({
                1: 'Sí',
                2: 'No'
            }).fillna(df['carnet_vacunacion'])
        
        # Normalizar hospitalización (1=Sí, 2=No)
        if 'paciente_hospitalizado' in df.columns:
            df['hospitalizado'] = df['paciente_hospitalizado'].map({
                1: 'Sí',
                2: 'No'
            }).fillna(df['paciente_hospitalizado'])
        
        # 8. PROCESAR SÍNTOMAS (1=Sí, 2=No para cada síntoma)
        print("🤒 Procesando síntomas...")
        
        sintomas_campos = [
            'fiebre', 'mialgias', 'artralgias', 'cefalea', 'vomitos', 'ictericia',
            'sangrado', 'oliguria', 'shock', 'bradicardia', 'falla_renal',
            'falla_hepatica', 'hepatomegalia', 'hemoptisis', 'hiperemia',
            'hematemesis', 'petequias', 'metrorragia', 'melenas', 'equimosis',
            'epistaxis', 'hematuria'
        ]
        
        sintomas_presentes = []
        for sintoma in sintomas_campos:
            if sintoma in df.columns:
                sintomas_presentes.append(sintoma)
                # Convertir 1/2 a Sí/No para mejor legibilidad
                df[f"{sintoma}_texto"] = df[sintoma].map({1: 'Sí', 2: 'No'}).fillna(df[sintoma])
        
        print(f"   ✅ {len(sintomas_presentes)} síntomas procesados")
        
        # 9. VALIDACIONES Y FILTROS
        print("🔍 Aplicando validaciones...")
        
        registros_iniciales = len(df)
        
        # Filtrar fechas de notificación válidas
        if 'fecha_notificacion' in df.columns:
            df = df.dropna(subset=['fecha_notificacion'])
        
        # Filtrar fechas coherentes (2020 en adelante)
        fecha_minima = date(2020, 1, 1)
        fecha_maxima = date.today()
        
        if 'fecha_notificacion' in df.columns:
            df = df[
                (df['fecha_notificacion'] >= fecha_minima) &
                (df['fecha_notificacion'] <= fecha_maxima)
            ]
        
        # Filtrar edades válidas
        if 'edad_final' in df.columns:
            df = df[
                (df['edad_final'].isna()) | 
                ((df['edad_final'] >= 0) & (df['edad_final'] <= 120))
            ]
        
        print(f"   Registros después validaciones: {len(df):,}")
        print(f"   Registros filtrados: {registros_iniciales - len(df):,}")
        
        # 10. CAMPOS CALCULADOS AUTOMÁTICOS
        print("⚙️ Generando campos calculados...")
        
        # Año y semana epidemiológica desde fecha notificación
        if 'fecha_notificacion' in df.columns:
            df['año'] = df['fecha_notificacion'].dt.year
            df['semana_epidemiologica'] = df['fecha_notificacion'].dt.isocalendar().week
        elif 'inicio_sintomas' in df.columns:
            # Usar fecha inicio síntomas como alternativa
            df['año'] = df['inicio_sintomas'].dt.year
            df['semana_epidemiologica'] = df['inicio_sintomas'].dt.isocalendar().week
        
        # 11. ESTADÍSTICAS PRE-CARGA
        print(f"\n📊 ESTADÍSTICAS CASOS PROCESADOS:")
        print(f"   Total casos: {len(df):,}")
        
        # CORREGIDO: Usar municipio_procedencia (donde se infectó)
        if 'municipio_procedencia' in df.columns:
            municipios_casos = df['municipio_procedencia'].nunique()
            print(f"   Municipios con casos (procedencia/infección): {municipios_casos}")
            
            # Top municipios con más casos (por procedencia)
            top_municipios = df['municipio_procedencia'].value_counts().head(5)
            print(f"   Top municipios procedencia (infección):")
            for municipio, casos in top_municipios.items():
                print(f"     {municipio}: {casos:,} casos")
        
        if 'año' in df.columns:
            años_casos = sorted(df['año'].dropna().unique())
            print(f"   Años con casos: {años_casos}")
        
        if 'condicion_final_texto' in df.columns:
            condiciones = df['condicion_final_texto'].value_counts()
            print(f"   Condición final:")
            for condicion, cantidad in condiciones.items():
                print(f"     {condicion}: {cantidad:,}")
        
        if 'vacunado_previo' in df.columns:
            vacunacion = df['vacunado_previo'].value_counts()
            print(f"   Vacunación previa:")
            for estado, cantidad in vacunacion.items():
                print(f"     {estado}: {cantidad:,}")
        
        print("✅ Procesamiento casos completado")
        print("📅 Edad calculada con fecha actual (CORREGIDO)")
        print("🗺️ Mapeo veredal con municipio procedencia (CORREGIDO)")
        
        return df
        
    except Exception as e:
        print(f"❌ Error procesando casos: {e}")
        import traceback
        traceback.print_exc()
        return None

def cargar_casos_postgresql(df_casos, tabla="casos_fiebre_amarilla"):
    """
    Carga casos de fiebre amarilla a PostgreSQL
    """
    if df_casos is None or len(df_casos) == 0:
        print("❌ No hay casos para cargar")
        return False
    
    print(f"\n💾 CARGANDO {len(df_casos):,} CASOS A POSTGRESQL")
    print("=" * 50)
    
    try:
        engine = create_engine(DATABASE_URL)
        
        # Verificar conexión
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        print("✅ Conexión PostgreSQL exitosa")
        
        # Añadir metadatos
        df_casos['created_at'] = datetime.now()
        df_casos['updated_at'] = datetime.now()
        
        # Cargar datos
        df_casos.to_sql(
            tabla,
            engine,
            if_exists='replace',  # Reemplazar casos completos
            index=False,
            chunksize=500  # Lotes más pequeños por la complejidad de los datos
        )
        
        # Verificar carga y estadísticas
        with engine.connect() as conn:
            total_cargado = conn.execute(text(f"SELECT COUNT(*) FROM {tabla}")).scalar()
            print(f"✅ {total_cargado:,} casos cargados exitosamente")
            
            # Estadísticas post-carga
            stats = pd.read_sql(text(f"""
                SELECT 
                    COUNT(DISTINCT municipio_procedencia) as municipios_procedencia,
                    COUNT(DISTINCT año) as años_casos,
                    COUNT(CASE WHEN condicion_final_texto = 'Muerto' THEN 1 END) as defunciones,
                    MIN(fecha_notificacion) as fecha_min,
                    MAX(fecha_notificacion) as fecha_max
                FROM {tabla}
                WHERE fecha_notificacion IS NOT NULL
            """), conn)
            
            if len(stats) > 0:
                s = stats.iloc[0]
                print(f"📍 Municipios procedencia: {s['municipios_procedencia']}")
                print(f"📊 Años con casos: {s['años_casos']}")
                print(f"☠️ Defunciones: {s['defunciones']}")
                if s['fecha_min'] and s['fecha_max']:
                    print(f"📅 Período: {s['fecha_min']} a {s['fecha_max']}")
            
            # Verificar códigos veredales asignados
            if 'codigo_divipola_vereda' in df_casos.columns:
                codigos_veredales = conn.execute(text(f"""
                    SELECT COUNT(*) FROM {tabla} WHERE codigo_divipola_vereda IS NOT NULL
                """)).scalar()
                print(f"🗺️ Casos con código veredal: {codigos_veredales:,}")
        
        return True
        
    except Exception as e:
        print(f"❌ Error cargando a PostgreSQL: {e}")
        import traceback
        traceback.print_exc()
        return False

def procesar_casos_completo(archivo_excel):
    """
    Proceso completo: Excel → Procesamiento → PostgreSQL
    """
    print("🦠 PROCESAMIENTO COMPLETO CASOS FIEBRE AMARILLA V2.0")
    print("=" * 55)
    
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
        
        # 2. Procesar casos
        df_casos = procesar_casos_fiebre_amarilla(archivo_excel)
        
        if df_casos is None:
            print("❌ Error en procesamiento de casos")
            return False
        
        # 3. Cargar a PostgreSQL
        exito = cargar_casos_postgresql(df_casos)
        
        # 4. Resumen final
        duracion = datetime.now() - inicio
        print(f"\n{'='*55}")
        print(" PROCESAMIENTO CASOS COMPLETADO ".center(55))
        print("=" * 55)
        
        if exito:
            print("🎉 ¡CASOS CARGADOS EXITOSAMENTE!")
            print(f"📊 {len(df_casos):,} casos procesados")
            print("📅 Edad calculada con fecha actual (CORREGIDO)")
            print("🗺️ Mapeo veredal con municipio procedencia (CORREGIDO)")
            print("🤒 Síntomas y datos epidemiológicos completos")
            print("📈 Listos para análisis de vigilancia")
        else:
            print("⚠️ Procesamiento con errores en carga BD")
        
        print(f"⏱️ Tiempo total: {duracion.total_seconds():.1f} segundos")
        print(f"📅 Finalizado: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        # 5. Crear backup CSV
        if exito:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            backup_file = f"backups/casos_backup_{timestamp}.csv"
            
            os.makedirs("backups", exist_ok=True)
            df_casos.to_csv(backup_file, index=False, encoding='utf-8-sig')
            print(f"💾 Backup creado: {backup_file}")
        
        return exito
        
    except Exception as e:
        print(f"❌ Error crítico: {e}")
        import traceback
        traceback.print_exc()
        return False

def generar_reporte_casos():
    """
    Genera reporte epidemiológico de casos cargados
    """
    print("\n📊 GENERANDO REPORTE EPIDEMIOLÓGICO...")
    
    try:
        engine = create_engine(DATABASE_URL)
        
        with engine.connect() as conn:
            # Resumen general
            resumen = pd.read_sql(text("""
                SELECT 
                    COUNT(*) as total_casos,
                    COUNT(DISTINCT municipio_procedencia) as municipios_afectados,
                    COUNT(CASE WHEN condicion_final_texto = 'Muerto' THEN 1 END) as defunciones,
                    COUNT(CASE WHEN vacunado_previo = 'Sí' THEN 1 END) as vacunados_previos,
                    MIN(fecha_notificacion) as primer_caso,
                    MAX(fecha_notificacion) as ultimo_caso
                FROM casos_fiebre_amarilla
                WHERE fecha_notificacion IS NOT NULL
            """), conn)
            
            if len(resumen) > 0:
                r = resumen.iloc[0]
                print(f"📋 RESUMEN EPIDEMIOLÓGICO:")
                print(f"   Total casos: {r['total_casos']:,}")
                print(f"   Municipios procedencia: {r['municipios_afectados']}")
                print(f"   Defunciones: {r['defunciones']} (Letalidad: {(r['defunciones']/r['total_casos']*100):.1f}%)")
                print(f"   Vacunados previos: {r['vacunados_previos']}")
                if r['primer_caso'] and r['ultimo_caso']:
                    print(f"   Período: {r['primer_caso']} a {r['ultimo_caso']}")
            
            # Casos por municipio procedencia
            casos_municipio = pd.read_sql(text("""
                SELECT municipio_procedencia, COUNT(*) as casos
                FROM casos_fiebre_amarilla
                WHERE municipio_procedencia IS NOT NULL
                GROUP BY municipio_procedencia
                ORDER BY casos DESC
                LIMIT 10
            """), conn)
            
            if len(casos_municipio) > 0:
                print(f"\n🏆 TOP 10 MUNICIPIOS PROCEDENCIA (INFECCIÓN):")
                for _, row in casos_municipio.iterrows():
                    print(f"   {row['municipio_procedencia']}: {row['casos']} casos")
            
            # Casos por grupo etario
            casos_edad = pd.read_sql(text("""
                SELECT grupo_etario, COUNT(*) as casos
                FROM casos_fiebre_amarilla
                WHERE grupo_etario IS NOT NULL
                GROUP BY grupo_etario
                ORDER BY casos DESC
            """), conn)
            
            if len(casos_edad) > 0:
                print(f"\n👥 CASOS POR GRUPO ETARIO:")
                for _, row in casos_edad.iterrows():
                    print(f"   {row['grupo_etario']}: {row['casos']} casos")
        
        return True
        
    except Exception as e:
        print(f"❌ Error generando reporte: {e}")
        return False

# ================================
# FUNCIÓN PRINCIPAL
# ================================
if __name__ == "__main__":
    print("🦠 PROCESADOR CASOS FIEBRE AMARILLA V2.0")
    print("=" * 40)
    
    # Archivo por defecto
    archivo_default = "data/casos.xlsx"
    
    # Verificar archivo
    if not os.path.exists(archivo_default):
        print(f"❌ ERROR: No se encuentra '{archivo_default}'")
        print("\n💡 Opciones:")
        print("1. Colocar archivo de casos en 'data/casos.xlsx'")
        print("2. Modificar variable archivo_default")
        print("3. Llamar: procesar_casos_completo('ruta/archivo.xlsx')")
    else:
        # Ejecutar procesamiento completo
        exito = procesar_casos_completo(archivo_default)
        
        if exito:
            print("\n📊 Generando reporte epidemiológico...")
            generar_reporte_casos()
            
            print("\n🎯 PRÓXIMOS PASOS:")
            print("1. Revisar datos en DBeaver: tabla 'casos_fiebre_amarilla'")
            print("2. Analizar distribución temporal y espacial")
            print("3. Correlacionar con datos de vacunación")
            print("4. Generar alertas epidemiológicas")
            print("5. ¡Vigilancia epidemiológica completa! 🚀")
        else:
            print("\n❌ Procesamiento fallido. Revisar errores.")