#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
config.py - Configuración Centralizada Sistema Epidemiológico Tolima
Configuración única de grupos etarios para todo el sistema
"""

import os
import pandas as pd
from pathlib import Path
from dotenv import load_dotenv

# Cargar variables de entorno
load_dotenv()

# ================================
# CONFIGURACIÓN GRUPOS ETARIOS CENTRALIZADA
# ================================

# CONFIGURACIÓN ÚNICA - MODIFICAR SOLO AQUÍ AFECTA TODO EL SISTEMA
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
    
    Args:
        edad_meses (float): Edad en meses totales
        
    Returns:
        str: Nombre del grupo etario o None si está fuera de grupos definidos
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
    
    # Fuera de grupos definidos
    return None

def obtener_grupos_etarios_definidos():
    """
    Obtiene lista ordenada de grupos etarios definidos
    Se auto-adapta cuando se modifican los grupos
    """
    # Orden lógico estándar
    orden_preferido = ['09-23 meses', '02-19 años', '20-59 años', '60+ años', 'Sin datos']
    grupos_ordenados = [g for g in orden_preferido if g in GRUPOS_ETARIOS.keys() or g == 'Sin datos']
    return grupos_ordenados

def calcular_edad_en_meses(fecha_nacimiento, fecha_referencia):
    """
    Calcula edad en meses totales entre dos fechas
    Función estándar para todo el sistema
    
    Args:
        fecha_nacimiento (date): Fecha de nacimiento
        fecha_referencia (date): Fecha de referencia (hoy)
        
    Returns:
        float: Edad en meses totales
    """
    if pd.isna(fecha_nacimiento) or pd.isna(fecha_referencia):
        return None
    
    if fecha_nacimiento > fecha_referencia:
        return None
    
    # Usar dateutil para cálculo preciso
    from dateutil.relativedelta import relativedelta
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
# CONFIGURACIÓN EPIDEMIOLÓGICA
# ================================
class EpidemiologicalConfig:
    """Configuraciones específicas epidemiológicas"""
    
    # Límites de edad válidos (en años)
    EDAD_MINIMA = 0
    EDAD_MAXIMA = 90
    
    # Fechas límite para datos
    FECHA_MINIMA = "2020-01-01"
    
    # Códigos DIVIPOLA Tolima
    CODIGO_DPTO_TOLIMA = "73"
    
    # Tolerancia para similitud de nombres (veredas, municipios)
    SIMILITUD_MINIMA = 0.75  # 75%
    
    # Coordenadas válidas para Colombia
    LAT_MIN = -4.2
    LAT_MAX = 12.6
    LON_MIN = -81.8
    LON_MAX = -66.9

# ================================
# MAPEOS DE COLUMNAS EXCEL
# ================================

# PAIweb - Mapeo por índice de columna
MAPEO_PAIWEB_EXCEL = {
    1: 'municipio',                 # B
    2: 'institucion',               # C
    12: 'fecha_nacimiento',         # M - Para calcular edad
    14: 'tipo_ubicacion'            # O - Espacios en blanco = Urbano
}

# Casos Fiebre Amarilla - Mapeo por índice de columna
MAPEO_CASOS_EXCEL = {
    1: 'fecha_notificacion',        # B
    2: 'semana_epidemiologica',     # C
    6: 'primer_nombre',             # G
    7: 'segundo_nombre',            # H
    8: 'primer_apellido',           # I
    9: 'segundo_apellido',          # J
    10: 'tipo_documento',           # K
    11: 'numero_documento',         # L
    12: 'edad',                     # M
    16: 'sexo',                     # Q
    27: 'vereda_residencia',        # AB
    32: 'ocupacion',                # AG
    33: 'tipo_seguridad_social',    # AH
    34: 'codigo_aseguradora',       # AI
    35: 'pertenencia_etnica',       # AJ
    37: 'estrato',                  # AL
    38: 'grupo_discapacidad',       # AM
    39: 'desplazado',               # AN
    40: 'grupo_migrante',           # AO
    41: 'grupo_carcelario',         # AP
    42: 'grupo_gestante',           # AQ
    43: 'semanas_gestacion',        # AR
    44: 'grupo_indigena',           # AS
    45: 'poblacion_icbf',           # AT
    46: 'madres_comunitarias',      # AU
    47: 'grupo_desmovilizados',     # AV
    48: 'grupo_psiquiatricos',      # AW
    49: 'victimas_violencia',       # AX
    51: 'fuente_informacion',       # AZ
    55: 'fecha_confirmacion_fa',    # BD
    56: 'fecha_inicio_sintomas',    # BE
    57: 'tipo_caso',                # BF
    58: 'hospitalizado',            # BG
    59: 'fecha_hospitalizacion',    # BH
    60: 'condicion_final',          # BI
    61: 'fecha_defuncion',          # BJ
    63: 'telefono',                 # BL
    78: 'carnet_vacunacion',        # CA
    80: 'fecha_vacunacion',         # CC
    
    # 23 síntomas CD-CZ (índices 81-103)
    81: 'fiebre',                   # CD
    82: 'malgias',                  # CE
    83: 'artralgia',                # CF
    84: 'cefalea',                  # CG
    85: 'vomito',                   # CH
    86: 'ictericia',                # CI
    87: 'sfaget',                   # CJ
    88: 'oliguria',                 # CK
    89: 'shock',                    # CL
    90: 'bradicardi',               # CM
    91: 'falla_rena',               # CN
    92: 'falla_hepa',               # CO
    93: 'hepatomega',               # CP
    94: 'hemoptisis',               # CQ
    95: 'hipiremia',                # CR
    96: 'hematemesi',               # CS
    97: 'petequias',                # CT
    98: 'metrorragi',               # CU
    99: 'melenas',                  # CV
    100: 'equimosis',               # CW
    101: 'epistaxis',               # CX
    102: 'hematuria',               # CY
    103: 'cas_fa',                  # CZ
    
    108: 'codigo_divipola_municipio_caso',  # DC
    110: 'nombre_upgd',             # DE
    111: 'pais_procedencia',        # DF
    112: 'departamento_procedencia', # DG
    113: 'municipio_procedencia',    # DH
    114: 'pais_residencia',         # DI
    115: 'departamento_residencia', # DJ
    116: 'municipio_residencia',    # DK
    117: 'departamento_notificacion', # DL
    118: 'municipio_notificacion'   # DM
}

# Población SISBEN - Mapeo sin headers por índice
MAPEO_POBLACION_SISBEN = {
    1: 'codigo_municipio',          # col_1
    2: 'municipio',                 # col_2
    6: 'corregimiento',             # col_6
    8: 'vereda',                    # col_8
    10: 'barrio',                   # col_10
    17: 'documento',                # col_17
    18: 'fecha_nacimiento'          # col_18
}

# Epizootias - Orden de columnas
COLUMNAS_EPIZOOTIAS = [
    'municipio',                    # 0
    'vereda',                       # 1
    'fecha_recoleccion',            # 2
    'informante',                   # 3
    'descripcion',                  # 4
    'fecha_notificacion',           # 5
    'especie',                      # 6
    'latitud',                      # 7
    'longitud',                     # 8
    'fecha_envio_muestra',          # 9
    'resultado_pcr',                # 10
    'fecha_resultado_pcr',          # 11
    'resultado_histopatologia',     # 12
    'fecha_resultado_histopatologia' # 13
]

# ================================
# FUNCIONES DE UTILIDAD
# ================================

def normalizar_texto_snake_case(texto):
    """Convierte texto a snake_case"""
    if pd.isna(texto) or texto == '':
        return None
    
    import re
    # Convertir a snake_case
    texto = str(texto).strip()
    texto = re.sub(r'[^\w\s-]', '', texto)  # Remover caracteres especiales
    texto = re.sub(r'[-\s]+', '_', texto)   # Espacios y guiones a underscore
    return texto.lower()

def validar_grupos_etarios_configuracion():
    """Valida que la configuración de grupos etarios sea consistente"""
    grupos = list(GRUPOS_ETARIOS.keys())
    print("Configuración actual de grupos etarios:")
    for grupo, (min_meses, max_meses) in GRUPOS_ETARIOS.items():
        if max_meses:
            print(f"  {grupo}: {min_meses}-{max_meses} meses")
        else:
            print(f"  {grupo}: {min_meses}+ meses")
    return grupos

# ================================
# VARIABLES GLOBALES DE CONVENIENCIA
# ================================
DATABASE_URL = DatabaseConfig.get_connection_url()
DATA_DIR = FileConfig.DATA_DIR
LOGS_DIR = FileConfig.LOGS_DIR

if __name__ == "__main__":
    # Verificar configuración
    FileConfig.create_directories()
    print("Configuración Sistema Epidemiológico Tolima")
    print("=" * 50)
    validar_grupos_etarios_configuracion()
    print(f"Base de datos: {DatabaseConfig.HOST}:{DatabaseConfig.PORT}")
    print(f"Directorio datos: {DATA_DIR}")
    print("Sistema configurado correctamente")