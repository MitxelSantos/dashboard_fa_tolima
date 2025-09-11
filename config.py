#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
config.py - Configuración Centralizada OPTIMIZADA Sistema Epidemiológico Tolima
CORREGIDO: Error fiona.path solucionado, configuración unificada, caché inteligente DIVIPOLA
"""

import os
import pandas as pd
import geopandas as gpd
from pathlib import Path
from dotenv import load_dotenv
from datetime import datetime, date
from dateutil.relativedelta import relativedelta
import warnings
from functools import lru_cache
from typing import Dict, Optional, Tuple, Any
import structlog

warnings.filterwarnings("ignore")

# Configurar logging estructurado
structlog.configure(
    processors=[
        structlog.stdlib.filter_by_level,
        structlog.stdlib.add_log_level,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.PositionalArgumentsFormatter(),
        structlog.processors.TimeStamper(fmt="ISO"),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
        structlog.processors.JSONRenderer()
    ],
    context_class=dict,
    logger_factory=structlog.stdlib.LoggerFactory(),
    wrapper_class=structlog.stdlib.BoundLogger,
    cache_logger_on_first_use=True,
)

logger = structlog.get_logger()

# Cargar variables de entorno
load_dotenv()

# ================================
# CONFIGURACIÓN UNIFICADA DE BASE DE DATOS
# ================================

class DatabaseConfig:
    """Configuración unificada de PostgreSQL - TODAS las contraseñas aquí"""
    
    # Configuración por defecto UNIFICADA
    DEFAULT_HOST = "localhost"
    DEFAULT_PORT = "5432"
    DEFAULT_DATABASE = "epidemiologia_tolima"
    DEFAULT_USER = "tolima_admin"
    DEFAULT_PASSWORD = "tolima2025"
    
    # Configuración desde variables de entorno
    HOST = os.getenv("DB_HOST", DEFAULT_HOST)
    PORT = os.getenv("DB_PORT", DEFAULT_PORT)
    DATABASE = os.getenv("DB_NAME", DEFAULT_DATABASE)
    USER = os.getenv("DB_USER", DEFAULT_USER)
    PASSWORD = os.getenv("DB_PASSWORD", DEFAULT_PASSWORD)
    
    @classmethod
    def get_connection_url(cls, include_password=True):
        """Genera URL de conexión consistente"""
        if include_password:
            return f"postgresql://{cls.USER}:{cls.PASSWORD}@{cls.HOST}:{cls.PORT}/{cls.DATABASE}"
        else:
            return f"postgresql://{cls.USER}:***@{cls.HOST}:{cls.PORT}/{cls.DATABASE}"
    
    @classmethod
    def get_connection_params(cls) -> Dict[str, str]:
        """Parámetros de conexión como diccionario"""
        return {
            'host': cls.HOST,
            'port': cls.PORT,
            'database': cls.DATABASE,
            'user': cls.USER,
            'password': cls.PASSWORD
        }
    
    @classmethod
    def log_connection_info(cls):
        """Log información de conexión (sin contraseña)"""
        logger.info("database_config", 
                   host=cls.HOST,
                   port=cls.PORT, 
                   database=cls.DATABASE,
                   user=cls.USER,
                   url=cls.get_connection_url(include_password=False))

# ================================
# CONFIGURACIÓN DE ARCHIVOS OPTIMIZADA
# ================================

class FileConfig:
    """Configuración de rutas con validación automática"""
    
    BASE_DIR = Path(__file__).parent
    DATA_DIR = BASE_DIR / "data"
    PROCESSED_DIR = DATA_DIR / "processed"
    BACKUPS_DIR = BASE_DIR / "backups"
    LOGS_DIR = BASE_DIR / "logs"
    
    # Archivos específicos con validación
    PAIWEB_FILE = DATA_DIR / "paiweb.xlsx"
    CASOS_FILE = DATA_DIR / "casos.xlsx"
    EPIZOOTIAS_FILE = DATA_DIR / "epizootias.xlsx"
    POBLACION_FILE = DATA_DIR / "poblacion_veredas.csv"
    TERRITORIOS_FILE = DATA_DIR / "tolima_cabeceras_veredas.gpkg"
    
    @classmethod
    def create_directories(cls):
        """Crea directorios necesarios si no existen"""
        for directory in [cls.DATA_DIR, cls.PROCESSED_DIR, cls.BACKUPS_DIR, cls.LOGS_DIR]:
            directory.mkdir(parents=True, exist_ok=True)
            logger.debug("directory_created", path=str(directory))
    
    @classmethod
    def validate_files(cls) -> Dict[str, bool]:
        """Valida que archivos críticos existan"""
        files_status = {}
        critical_files = {
            'paiweb': cls.PAIWEB_FILE,
            'casos': cls.CASOS_FILE,
            'epizootias': cls.EPIZOOTIAS_FILE,
            'poblacion': cls.POBLACION_FILE,
            'territorios': cls.TERRITORIOS_FILE
        }
        
        for name, path in critical_files.items():
            exists = path.exists()
            files_status[name] = exists
            if exists:
                size_mb = path.stat().st_size / (1024 * 1024)
                logger.info("file_validated", name=name, path=str(path), size_mb=round(size_mb, 2))
            else:
                logger.warning("file_missing", name=name, path=str(path))
        
        return files_status

# ================================
# CACHÉ INTELIGENTE DIVIPOLA (SINGLETON OPTIMIZADO) - CORREGIDO
# ================================

class DivipolaCache:
    """
    Caché singleton optimizado para códigos DIVIPOLA - CORREGIDO ERROR FIONA
    Se carga UNA sola vez y se reutiliza en toda la aplicación
    """
    _instance = None
    _cache_data = None
    _last_loaded = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(DivipolaCache, cls).__new__(cls)
            cls._instance._load_cache()
        return cls._instance
    
    def _load_cache(self):
        """Carga los códigos DIVIPOLA una sola vez - CORREGIDO"""
        if self._cache_data is not None:
            return  # Ya está cargado
        
        gpkg_path = FileConfig.TERRITORIOS_FILE
        
        if not gpkg_path.exists():
            logger.error("divipola_file_missing", path=str(gpkg_path))
            self._cache_data = self._create_empty_cache()
            return
        
        try:
            logger.info("divipola_loading_started", path=str(gpkg_path))
            
            # CORRECCIÓN: Cargar archivo geoespacial con manejo de errores fiona
            try:
                gdf = gpd.read_file(gpkg_path)
            except Exception as fiona_error:
                # Intentar con driver específico si fiona falla
                try:
                    gdf = gpd.read_file(gpkg_path, driver='GPKG')
                except Exception as backup_error:
                    logger.error("divipola_load_failed", 
                               error=str(fiona_error), 
                               backup_error=str(backup_error),
                               path=str(gpkg_path))
                    self._cache_data = self._create_empty_cache()
                    return
            
            gdf = gdf.dropna(subset=["codigo_divipola", "nombre"])
            gdf["nombre_normalizado"] = gdf["nombre"].apply(self._normalize_name)
            
            # Crear índices optimizados O(1)
            self._cache_data = {
                # Búsqueda rápida por nombre normalizado
                'municipios_by_name': {
                    row['nombre_normalizado']: row['codigo_municipio'] 
                    for _, row in gdf[gdf['tipo'] == 'municipio'].iterrows()
                },
                
                # Búsqueda de veredas por municipio
                'veredas_by_municipio': self._build_veredas_index(gdf),
                
                # DataFrame completo para consultas complejas
                'dataframe': gdf,
                
                # Estadísticas para logging
                'stats': {
                    'municipios': len(gdf[gdf['tipo'] == 'municipio']),
                    'veredas': len(gdf[gdf['tipo'] == 'vereda']),
                    'cabeceras': len(gdf[gdf['tipo'] == 'cabecera'])
                }
            }
            
            self._last_loaded = datetime.now()
            
            logger.info("divipola_loaded_successfully", 
                       municipios=self._cache_data['stats']['municipios'],
                       veredas=self._cache_data['stats']['veredas'],
                       cabeceras=self._cache_data['stats']['cabeceras'])
                       
        except Exception as e:
            logger.error("divipola_load_failed", error=str(e), path=str(gpkg_path))
            self._cache_data = self._create_empty_cache()
    
    def _build_veredas_index(self, gdf) -> Dict[str, Dict[str, str]]:
        """Construye índice optimizado para búsqueda de veredas"""
        veredas_index = {}
        
        for _, row in gdf[gdf['tipo'] == 'vereda'].iterrows():
            municipio_norm = self._normalize_name(row.get('municipio', ''))
            vereda_norm = self._normalize_name(row['nombre'])
            
            if municipio_norm not in veredas_index:
                veredas_index[municipio_norm] = {}
            
            veredas_index[municipio_norm][vereda_norm] = row['codigo_divipola']
        
        return veredas_index
    
    @staticmethod
    def _normalize_name(nombre: str) -> str:
        """Normalización optimizada de nombres"""
        if pd.isna(nombre) or not nombre:
            return ""
        
        # Mapeos especiales para Tolima
        MAPEOS_ESPECIALES = {
            "SAN SEBASTIÁN DE MARIQUITA": "MARIQUITA",
            "SAN SEBASTIAN DE MARIQUITA": "MARIQUITA", 
            "ARMERO (GUAYABAL)": "ARMERO GUAYABAL",
            "CARMEN DE APICALÁ": "CARMEN DE APICALA",
        }
        
        nombre = str(nombre).strip().upper()
        nombre = MAPEOS_ESPECIALES.get(nombre, nombre)
        
        # Remover acentos y normalizar
        replacements = {
            'Á': 'A', 'É': 'E', 'Í': 'I', 'Ó': 'O', 'Ú': 'U', 'Ñ': 'N'
        }
        for old, new in replacements.items():
            nombre = nombre.replace(old, new)
        
        return nombre.strip()
    
    def _create_empty_cache(self) -> Dict[str, Any]:
        """Crea caché vacío en caso de error"""
        return {
            'municipios_by_name': {},
            'veredas_by_municipio': {},
            'dataframe': gpd.GeoDataFrame(),
            'stats': {'municipios': 0, 'veredas': 0, 'cabeceras': 0}
        }
    
    def search_municipio_code(self, nombre_municipio: str) -> Optional[str]:
        """Búsqueda O(1) de código municipal"""
        if not nombre_municipio:
            return None
        
        nombre_norm = self._normalize_name(nombre_municipio)
        return self._cache_data['municipios_by_name'].get(nombre_norm, "73999")  # Código genérico Tolima
    
    def search_vereda_code(self, nombre_vereda: str, municipio_contexto: Optional[str] = None) -> Optional[str]:
        """Búsqueda optimizada de código veredal con contexto municipal"""
        if not nombre_vereda:
            return None
        
        vereda_norm = self._normalize_name(nombre_vereda)
        
        # Si hay contexto de municipio, buscar primero ahí
        if municipio_contexto:
            municipio_norm = self._normalize_name(municipio_contexto)
            municipio_veredas = self._cache_data['veredas_by_municipio'].get(municipio_norm, {})
            
            if vereda_norm in municipio_veredas:
                return municipio_veredas[vereda_norm]
        
        # Búsqueda general en todos los municipios
        for municipio_veredas in self._cache_data['veredas_by_municipio'].values():
            if vereda_norm in municipio_veredas:
                return municipio_veredas[vereda_norm]
        
        logger.warning("vereda_not_found", vereda=nombre_vereda, municipio=municipio_contexto)
        return None
    
    def get_stats(self) -> Dict[str, Any]:
        """Obtiene estadísticas del caché"""
        return {
            **self._cache_data['stats'],
            'last_loaded': self._last_loaded,
            'cache_size_mb': len(str(self._cache_data)) / (1024 * 1024)
        }

# Instancia global del caché (singleton)
divipola_cache = DivipolaCache()

# ================================
# FUNCIONES OPTIMIZADAS DE UTILIDAD
# ================================

# Configuración de grupos etarios centralizada
GRUPOS_ETARIOS = {
    "09-23 meses": (9, 23),
    "02-19 años": (24, 239), 
    "20-59 años": (240, 719),
    "60+ años": (720, None),
}

@lru_cache(maxsize=1000)  # Caché para funciones frecuentes
def clasificar_grupo_etario(edad_meses: Optional[float]) -> str:
    """Clasificación optimizada con caché"""
    if pd.isna(edad_meses) or edad_meses is None:
        return "Sin datos"
    
    for grupo, (min_meses, max_meses) in GRUPOS_ETARIOS.items():
        if max_meses is None:
            if edad_meses >= min_meses:
                return grupo
        else:
            if min_meses <= edad_meses <= max_meses:
                return grupo
    
    return "Sin datos"

def calcular_edad_en_meses(fecha_nacimiento: date, fecha_referencia: date) -> Optional[int]:
    """Cálculo optimizado de edad en meses"""
    if pd.isna(fecha_nacimiento) or pd.isna(fecha_referencia):
        return None
    
    if fecha_nacimiento > fecha_referencia:
        return None
    
    diferencia = relativedelta(fecha_referencia, fecha_nacimiento)
    return diferencia.years * 12 + diferencia.months

@lru_cache(maxsize=1000)
def limpiar_fecha_robusta(fecha_input: Any) -> Optional[date]:
    """Limpieza optimizada de fechas con caché"""
    if pd.isna(fecha_input):
        return None
    
    try:
        if isinstance(fecha_input, (datetime, pd.Timestamp)):
            return fecha_input.date()
        
        fecha_str = str(fecha_input).strip()
        
        if " " in fecha_str:
            fecha_str = fecha_str.split(" ")[0]
        
        # Probar formatos comunes
        formatos = ["%d/%m/%Y", "%d-%m-%Y", "%Y-%m-%d", "%m/%d/%Y"]
        
        for formato in formatos:
            try:
                return datetime.strptime(fecha_str, formato).date()
            except ValueError:
                continue
        
        # Último intento con pandas
        return pd.to_datetime(fecha_str, dayfirst=True).date()
        
    except Exception:
        return None

def determinar_ubicacion_urbano_rural(vereda: str, corregimiento: str, barrio: str) -> str:
    """Determina ubicación optimizada"""
    vereda = str(vereda).strip().upper() if pd.notna(vereda) else "SIN VEREDA"
    corregimiento = str(corregimiento).strip().upper() if pd.notna(corregimiento) else "SIN CORREGIMIENTO"
    
    # Reglas simplificadas
    if vereda != "SIN VEREDA":
        return "Rural"
    
    if vereda == "SIN VEREDA" and corregimiento not in ["SIN CORREGIMIENTO", "CABECERA MUNICIPAL"]:
        return "Rural"
    
    return "Urbano"

def normalizar_nombre_territorio(nombre: str) -> str:
    """Normalizar nombres de territorios"""
    if pd.isna(nombre) or not nombre:
        return ""
    return str(nombre).strip().upper()

# ================================
# FUNCIONES DE BÚSQUEDA OPTIMIZADAS
# ================================

def buscar_codigo_municipio(nombre_municipio: str) -> Optional[str]:
    """Función optimizada usando caché singleton"""
    return divipola_cache.search_municipio_code(nombre_municipio)

def buscar_codigo_vereda(nombre_vereda: str, municipio_contexto: Optional[str] = None) -> Optional[str]:
    """Función optimizada usando caché singleton"""
    return divipola_cache.search_vereda_code(nombre_vereda, municipio_contexto)

# ================================
# CONFIGURACIÓN DE ARCHIVOS DE CARGA
# ================================

class LoadingConfig:
    """Configuración para carga de archivos grandes"""
    
    # Configuración de chunks para archivos grandes
    POPULATION_CHUNK_SIZE = 10000  # Para archivo de 203MB
    VACCINATION_CHUNK_SIZE = 5000   # Para archivo de 67.5MB
    
    # Configuración de memoria
    MAX_MEMORY_USAGE_MB = 512  # Límite de memoria por proceso
    
    # Configuración de reintentos
    MAX_RETRIES = 3
    RETRY_DELAY_SECONDS = 5

# ================================
# VARIABLES GLOBALES DE CONVENIENCIA
# ================================

DATABASE_URL = DatabaseConfig.get_connection_url()
DATA_DIR = FileConfig.DATA_DIR
LOGS_DIR = FileConfig.LOGS_DIR

# ================================
# FUNCIÓN DE VALIDACIÓN COMPLETA
# ================================

def validar_configuracion_optimizada():
    """Validación completa optimizada del sistema"""
    logger.info("config_validation_started")
    
    # 1. Validar y crear directorios
    FileConfig.create_directories()
    
    # 2. Validar configuración de BD
    DatabaseConfig.log_connection_info()
    
    # 3. Validar archivos
    files_status = FileConfig.validate_files()
    missing_files = [name for name, exists in files_status.items() if not exists]
    
    if missing_files:
        logger.warning("files_missing", missing_files=missing_files)
    
    # 4. Validar caché DIVIPOLA
    stats = divipola_cache.get_stats()
    logger.info("divipola_cache_stats", **stats)
    
    # 5. Validar grupos etarios
    grupos = list(GRUPOS_ETARIOS.keys())
    logger.info("grupos_etarios_configured", grupos=grupos, total=len(grupos))
    
    logger.info("config_validation_completed", 
               missing_files_count=len(missing_files),
               divipola_municipios=stats['municipios'])
    
    return len(missing_files) == 0

if __name__ == "__main__":
    validar_configuracion_optimizada()