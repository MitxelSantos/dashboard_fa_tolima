#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
config.py - Configuración Centralizada Sistema Epidemiológico Tolima
Configuración única con mapeo centralizado de códigos DIVIPOLA desde .gpkg
"""

import os
import pandas as pd
import geopandas as gpd
from pathlib import Path
from dotenv import load_dotenv
from datetime import datetime, date
from dateutil.relativedelta import relativedelta
import warnings
warnings.filterwarnings('ignore')

# Cargar variables de entorno
load_dotenv()

# ================================
# CONFIGURACIÓN GRUPOS ETARIOS CENTRALIZADA
# ================================

GRUPOS_ETARIOS = {
    '09-23 meses': (9, 23),        # meses
    '02-19 años': (24, 239),       # meses  
    '20-59 años': (240, 719),      # meses
    '60+ años': (720, None)        # meses, None = sin límite superior
}

def clasificar_grupo_etario(edad_meses):
    """
    Función ÚNICA de clasificación de grupos etarios
    Usada por TODOS los scripts del sistema
    """
    if pd.isna(edad_meses):
        return 'Sin datos'
    
    for grupo, (min_meses, max_meses) in GRUPOS_ETARIOS.items():
        if max_meses is None:
            if edad_meses >= min_meses:
                return grupo
        else:
            if min_meses <= edad_meses <= max_meses:
                return grupo
    
    return None

def obtener_grupos_etarios_definidos():
    """Obtiene lista ordenada de grupos etarios definidos"""
    orden_preferido = ['09-23 meses', '02-19 años', '20-59 años', '60+ años', 'Sin datos']
    grupos_ordenados = [g for g in orden_preferido if g in GRUPOS_ETARIOS.keys() or g == 'Sin datos']
    return grupos_ordenados

def calcular_edad_en_meses(fecha_nacimiento, fecha_referencia):
    """Calcula edad en meses totales entre dos fechas"""
    if pd.isna(fecha_nacimiento) or pd.isna(fecha_referencia):
        return None
    
    if fecha_nacimiento > fecha_referencia:
        return None
    
    diferencia = relativedelta(fecha_referencia, fecha_nacimiento)
    edad_meses_total = diferencia.years * 12 + diferencia.months
    
    return edad_meses_total

# ================================
# CONFIGURACIÓN BASE DE DATOS
# ================================
class DatabaseConfig:
    """Configuración de base de datos PostgreSQL"""
    
    DEFAULT_HOST = "localhost"
    DEFAULT_PORT = "5432"
    DEFAULT_DATABASE = "epidemiologia_tolima"
    DEFAULT_USER = "tolima_admin"
    DEFAULT_PASSWORD = "tolima2025!"
    
    HOST = os.getenv("DB_HOST", DEFAULT_HOST)
    PORT = os.getenv("DB_PORT", DEFAULT_PORT)
    DATABASE = os.getenv("DB_NAME", DEFAULT_DATABASE)
    USER = os.getenv("DB_USER", DEFAULT_USER)
    PASSWORD = os.getenv("DB_PASSWORD", DEFAULT_PASSWORD)
    
    @classmethod
    def get_connection_url(cls):
        return f"postgresql://{cls.USER}:{cls.PASSWORD}@{cls.HOST}:{cls.PORT}/{cls.DATABASE}"

# ================================
# CONFIGURACIÓN DE ARCHIVOS
# ================================
class FileConfig:
    """Configuración de rutas de archivos"""
    
    BASE_DIR = Path(__file__).parent
    DATA_DIR = BASE_DIR / "data"
    PROCESSED_DIR = DATA_DIR / "processed"
    BACKUPS_DIR = BASE_DIR / "backups"
    LOGS_DIR = BASE_DIR / "logs"
    
    # Archivos específicos
    PAIWEB_FILE = DATA_DIR / "paiweb.xlsx"
    CASOS_FILE = DATA_DIR / "casos.xlsx"
    EPIZOOTIAS_FILE = DATA_DIR / "epizootias.xlsx"
    POBLACION_FILE = DATA_DIR / "poblacion_veredas.csv"
    TERRITORIOS_FILE = DATA_DIR / "tolima_cabeceras_veredas.gpkg"
    
    @classmethod
    def create_directories(cls):
        """Crea directorios necesarios"""
        for directory in [cls.DATA_DIR, cls.PROCESSED_DIR, cls.BACKUPS_DIR, cls.LOGS_DIR]:
            directory.mkdir(parents=True, exist_ok=True)

# ================================
# MAPEO CENTRALIZADO DE CÓDIGOS DIVIPOLA
# ================================

# Cache global para códigos DIVIPOLA
_CODIGOS_DIVIPOLA_CACHE = None
_GPKG_TIMESTAMP = None

def cargar_codigos_divipola_desde_gpkg(forzar_recarga=False):
    """
    Carga códigos DIVIPOLA desde .gpkg una sola vez
    Se recarga automáticamente si el archivo se actualiza
    """
    global _CODIGOS_DIVIPOLA_CACHE, _GPKG_TIMESTAMP
    
    gpkg_path = FileConfig.TERRITORIOS_FILE
    
    if not gpkg_path.exists():
        print(f"⚠️ Archivo .gpkg no encontrado: {gpkg_path}")
        return None
    
    # Verificar si necesita recarga
    current_timestamp = os.path.getmtime(gpkg_path)
    
    if not forzar_recarga and _CODIGOS_DIVIPOLA_CACHE is not None and _GPKG_TIMESTAMP == current_timestamp:
        return _CODIGOS_DIVIPOLA_CACHE
    
    try:
        print(f"📂 Cargando códigos DIVIPOLA desde {gpkg_path}...")
        gdf = gpd.read_file(gpkg_path)
        
        # Limpiar datos
        gdf = gdf.dropna(subset=['codigo_divipola', 'nombre'])
        gdf['nombre'] = gdf['nombre'].str.strip().str.upper()
        
        # Crear diccionarios de mapeo
        codigos_cache = {
            # Mapeo por nombre a código DIVIPOLA completo
            'nombre_a_codigo': dict(zip(gdf['nombre'], gdf['codigo_divipola'])),
            
            # Mapeo por tipo
            'municipios': gdf[gdf['tipo'] == 'municipio'][['nombre', 'codigo_divipola', 'codigo_municipio']].to_dict('records'),
            'veredas': gdf[gdf['tipo'] == 'vereda'][['nombre', 'codigo_divipola', 'municipio']].to_dict('records'),
            'cabeceras': gdf[gdf['tipo'] == 'cabecera'][['nombre', 'codigo_divipola', 'municipio']].to_dict('records'),
            
            # Índice por municipio padre para veredas
            'veredas_por_municipio': {},
            
            # DataFrame completo para consultas avanzadas
            'dataframe': gdf.copy()
        }
        
        # Crear índice de veredas por municipio
        for _, row in gdf[gdf['tipo'] == 'vereda'].iterrows():
            municipio = row['municipio']
            if municipio not in codigos_cache['veredas_por_municipio']:
                codigos_cache['veredas_por_municipio'][municipio] = {}
            codigos_cache['veredas_por_municipio'][municipio][row['nombre']] = row['codigo_divipola']
        
        # Actualizar cache
        _CODIGOS_DIVIPOLA_CACHE = codigos_cache
        _GPKG_TIMESTAMP = current_timestamp
        
        print(f"✅ Códigos DIVIPOLA cargados:")
        print(f"   - Municipios: {len(codigos_cache['municipios'])}")
        print(f"   - Veredas: {len(codigos_cache['veredas'])}")
        print(f"   - Cabeceras: {len(codigos_cache['cabeceras'])}")
        
        return codigos_cache
        
    except Exception as e:
        print(f"❌ Error cargando códigos DIVIPOLA: {e}")
        return None

def buscar_codigo_municipio(nombre_municipio):
    """
    Busca código DIVIPOLA de municipio por nombre
    """
    codigos = cargar_codigos_divipola_desde_gpkg()
    if not codigos:
        return None
    
    if pd.isna(nombre_municipio):
        return None
    
    nombre_norm = normalizar_nombre_territorio(nombre_municipio)
    
    # Buscar en municipios
    for mun in codigos['municipios']:
        if normalizar_nombre_territorio(mun['nombre']) == nombre_norm:
            return mun['codigo_municipio']
    
    # Búsqueda aproximada
    for mun in codigos['municipios']:
        if nombre_norm in normalizar_nombre_territorio(mun['nombre']) or \
           normalizar_nombre_territorio(mun['nombre']) in nombre_norm:
            print(f"🔍 Mapeo aproximado: {nombre_municipio} → {mun['nombre']} ({mun['codigo_municipio']})")
            return mun['codigo_municipio']
    
    print(f"⚠️ Municipio no encontrado: {nombre_municipio}")
    return "73999"  # Código genérico Tolima

def buscar_codigo_vereda(nombre_vereda, municipio_contexto=None):
    """
    Busca código DIVIPOLA completo de vereda por nombre
    Opcionalmente usa contexto de municipio para mejorar búsqueda
    """
    codigos = cargar_codigos_divipola_desde_gpkg()
    if not codigos:
        return None
    
    if pd.isna(nombre_vereda):
        return None
    
    nombre_norm = normalizar_nombre_territorio(nombre_vereda)
    
    # Si hay contexto de municipio, buscar primero ahí
    if municipio_contexto:
        municipio_norm = normalizar_nombre_territorio(municipio_contexto)
        veredas_municipio = codigos['veredas_por_municipio'].get(municipio_norm, {})
        
        for nombre_v, codigo_v in veredas_municipio.items():
            if normalizar_nombre_territorio(nombre_v) == nombre_norm:
                return codigo_v
    
    # Búsqueda general en todas las veredas
    for vereda in codigos['veredas']:
        if normalizar_nombre_territorio(vereda['nombre']) == nombre_norm:
            return vereda['codigo_divipola']
    
    # Búsqueda aproximada
    for vereda in codigos['veredas']:
        if nombre_norm in normalizar_nombre_territorio(vereda['nombre']) or \
           normalizar_nombre_territorio(vereda['nombre']) in nombre_norm:
            print(f"🔍 Mapeo aproximado vereda: {nombre_vereda} → {vereda['nombre']} ({vereda['codigo_divipola']})")
            return vereda['codigo_divipola']
    
    print(f"⚠️ Vereda no encontrada: {nombre_vereda}")
    return None

# ================================
# MAPEOS Y NORMALIZACIONES
# ================================

# Mapeo de nombres especiales de municipios
MAPEO_MUNICIPIOS_ESPECIALES = {
    "SAN SEBASTIÁN DE MARIQUITA": "MARIQUITA",
    "SAN SEBASTIAN DE MARIQUITA": "MARIQUITA",
    "ARMERO (GUAYABAL)": "ARMERO GUAYABAL",
    "CARMEN DE APICALÁ": "CARMEN DE APICALA",
    "VALLE DE SAN JUAN": "VALLE DE SAN JUAN",
}

def normalizar_nombre_territorio(nombre):
    """Normaliza nombres de territorios para comparación"""
    if pd.isna(nombre):
        return None
    
    nombre = str(nombre).strip().upper()
    
    # Aplicar mapeos especiales
    nombre = MAPEO_MUNICIPIOS_ESPECIALES.get(nombre, nombre)
    
    # Normalizar caracteres especiales
    nombre = (nombre
              .replace("Á", "A").replace("É", "E").replace("Í", "I")
              .replace("Ó", "O").replace("Ú", "U").replace("Ñ", "N")
              .replace("  ", " ").strip())
    
    return nombre

# ================================
# LIMPIEZA DE FECHAS CENTRALIZADA
# ================================

def limpiar_fecha_robusta(fecha_input):
    """
    Limpia y convierte fechas de múltiples formatos de manera robusta
    """
    if pd.isna(fecha_input):
        return None
    
    try:
        # Si ya es datetime, convertir a date
        if isinstance(fecha_input, (datetime, pd.Timestamp)):
            return fecha_input.date()
        
        # Convertir a string y limpiar
        fecha_str = str(fecha_input).strip()
        
        # Remover componente de tiempo si existe
        if " " in fecha_str:
            fecha_str = fecha_str.split(" ")[0]
        
        # Si está en formato timestamp de Excel
        if fecha_str.isdigit() and len(fecha_str) > 8:
            try:
                return pd.to_datetime(int(fecha_str), origin='1900-01-01', unit='D').date()
            except:
                pass
        
        # Probar diferentes formatos de fecha
        formatos_fecha = [
            "%d/%m/%Y",    # 15/01/2024
            "%d-%m-%Y",    # 15-01-2024
            "%Y-%m-%d",    # 2024-01-15
            "%m/%d/%Y",    # 01/15/2024
            "%d/%m/%y",    # 15/01/24
            "%d-%m-%y",    # 15-01-24
            "%Y/%m/%d",    # 2024/01/15
        ]
        
        for formato in formatos_fecha:
            try:
                return datetime.strptime(fecha_str, formato).date()
            except ValueError:
                continue
        
        # Si no funciona nada, intentar pandas
        try:
            return pd.to_datetime(fecha_str, dayfirst=True).date()
        except:
            pass
        
        return None
        
    except Exception:
        return None

# ================================
# MAPEOS DE COLUMNAS PARA ARCHIVOS EXCEL
# ================================

# CASOS - Mapeo correcto: nombre_bd: nombre_excel
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
    'vereda_infeccion': 'vereda_',
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
    'municipio_procedencia': 'nmun_proce',
    'pais_residencia': 'npais_resi',
    'departamento_residencia': 'ndep_resi',
    'municipio_residencia': 'nmun_resi',
    'departamento_notificacion': 'ndep_notif',
    'municipio_notificacion': 'nmun_notif'
}

# EPIZOOTIAS - Mapeo correcto: nombre_bd: nombre_excel
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

# VACUNACIÓN PAIweb - Solo columnas necesarias
MAPEO_VACUNACION_EXCEL = {
    'departamento': 'Departamento',
    'municipio': 'Municipio',
    'institucion': 'Institucion',
    'fecha_aplicacion': 'fechaaplicacion',
    'fecha_nacimiento': 'FechaNacimiento',
    'tipo_ubicacion': 'TipoUbicación'
}

# POBLACIÓN SISBEN - Por índice de columna (sin headers)
MAPEO_POBLACION_SISBEN = {
    'codigo_municipio': 1,     # col_1
    'municipio': 2,            # col_2
    'corregimiento': 6,        # col_6
    'vereda': 8,               # col_8
    'barrio': 10,              # col_10
    'documento': 17,           # col_17
    'fecha_nacimiento': 18     # col_18
}

# ================================
# FUNCIONES DE UTILIDAD
# ================================

def normalizar_texto_snake_case(texto):
    """Convierte texto a snake_case"""
    if pd.isna(texto) or texto == '':
        return None
    
    import re
    texto = str(texto).strip()
    texto = re.sub(r'[^\w\s-]', '', texto)  # Remover caracteres especiales
    texto = re.sub(r'[-\s]+', '_', texto)   # Espacios y guiones a underscore
    return texto.lower()

def cargar_primera_hoja_excel(archivo_path):
    """
    Carga la primera hoja de un archivo Excel sin especificar nombre
    """
    try:
        # Leer primera hoja disponible
        excel_file = pd.ExcelFile(archivo_path)
        primera_hoja = excel_file.sheet_names[0]
        
        print(f"📋 Hojas disponibles en {archivo_path}: {excel_file.sheet_names}")
        print(f"📄 Usando primera hoja: {primera_hoja}")
        
        df = pd.read_excel(archivo_path, sheet_name=primera_hoja)
        return df, primera_hoja
        
    except Exception as e:
        print(f"❌ Error cargando Excel: {e}")
        return None, None

def determinar_ubicacion_urbano_rural(vereda, corregimiento, barrio):
    """
    Determina si es urbano o rural basado en las reglas del sistema
    (Función original del script población.py)
    """
    # Normalizar valores
    vereda = str(vereda).strip().upper() if pd.notna(vereda) else "SIN VEREDA"
    corregimiento = str(corregimiento).strip().upper() if pd.notna(corregimiento) else "SIN CORREGIMIENTO"
    barrio = str(barrio).strip().upper() if pd.notna(barrio) else "SIN BARRIO"
    
    # REGLAS RURALES
    if vereda != "SIN VEREDA":
        return 'Rural'
    
    if vereda == "SIN VEREDA" and corregimiento not in ["SIN CORREGIMIENTO", "CABECERA MUNICIPAL"]:
        return 'Rural'
    
    # REGLAS URBANAS (por defecto)
    return 'Urbano'

# ================================
# VARIABLES GLOBALES DE CONVENIENCIA
# ================================
DATABASE_URL = DatabaseConfig.get_connection_url()
DATA_DIR = FileConfig.DATA_DIR
LOGS_DIR = FileConfig.LOGS_DIR

def validar_configuracion():
    """Valida que la configuración sea correcta"""
    print("⚙️ Validando configuración Sistema Epidemiológico Tolima")
    print("=" * 60)
    
    # Validar directorios
    FileConfig.create_directories()
    print(f"📁 Directorios creados/verificados")
    
    # Validar grupos etarios
    grupos = obtener_grupos_etarios_definidos()
    print(f"👥 Grupos etarios configurados: {len(grupos)}")
    for grupo in grupos:
        print(f"   - {grupo}")
    
    # Validar conexión BD
    print(f"🐘 Base de datos: {DatabaseConfig.HOST}:{DatabaseConfig.PORT}")
    
    # Cargar códigos DIVIPOLA
    codigos = cargar_codigos_divipola_desde_gpkg()
    if codigos:
        print(f"🗺️ Códigos DIVIPOLA cargados correctamente")
    else:
        print(f"⚠️ No se pudieron cargar códigos DIVIPOLA")
    
    print("✅ Configuración validada correctamente")

if __name__ == "__main__":
    validar_configuracion()