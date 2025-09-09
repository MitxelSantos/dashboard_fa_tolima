#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Sistema Epidemiológico Tolima V1.0
Pre-procesamiento local optimizado → PostgreSQL → Dashboard Streamlit Cloud
"""

__version__ = "1.0.0"
__author__ = "Sistema Epidemiológico Tolima"
__description__ = "Pre-procesamiento optimizado de datos epidemiológicos"

# Importaciones principales
try:
    from config import (
        DATABASE_URL,
        FileConfig,
        DatabaseConfig,
        clasificar_grupo_etario,
        calcular_edad_en_meses,
        limpiar_fecha_robusta,
        buscar_codigo_municipio,
        buscar_codigo_vereda,
        normalizar_nombre_territorio,
        validar_configuracion_optimizada
    )
    
    __all__ = [
        "DATABASE_URL",
        "FileConfig", 
        "DatabaseConfig",
        "clasificar_grupo_etario",
        "calcular_edad_en_meses",
        "limpiar_fecha_robusta",
        "buscar_codigo_municipio",
        "buscar_codigo_vereda",
        "normalizar_nombre_territorio",
        "validar_configuracion_optimizada"
    ]
    
except ImportError:
    # Importaciones opcionales para flexibilidad
    __all__ = []