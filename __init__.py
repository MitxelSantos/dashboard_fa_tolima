#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Sistema Epidemiológico Tolima V2.0
Sistema de vigilancia epidemiológica para fiebre amarilla en el departamento del Tolima
con configuración centralizada y mapeo automático de códigos DIVIPOLA.
"""

__version__ = "2.0.0"
__author__ = "Sistema Epidemiológico Tolima"
__description__ = "Sistema de vigilancia epidemiológica con configuración centralizada"

# Importaciones principales para uso externo
from scripts.config import (
    DATABASE_URL,
    FileConfig,
    DatabaseConfig,
    clasificar_grupo_etario,
    obtener_grupos_etarios_definidos,
    calcular_edad_en_meses,
    limpiar_fecha_robusta,
    buscar_codigo_municipio,
    buscar_codigo_vereda,
    normalizar_nombre_territorio,
    cargar_primera_hoja_excel,
    determiner_ubicacion_urbano_rural,
    verificar_actualizacion_archivos,
    validar_configuracion
)

__all__ = [
    "DATABASE_URL",
    "FileConfig", 
    "DatabaseConfig",
    "clasificar_grupo_etario",
    "obtener_grupos_etarios_definidos",
    "calcular_edad_en_meses",
    "limpiar_fecha_robusta",
    "buscar_codigo_municipio",
    "buscar_codigo_vereda",
    "normalizar_nombre_territorio",
    "cargar_primera_hoja_excel",
    "determiner_ubicacion_urbano_rural",
    "verificar_actualizacion_archivos",
    "validar_configuracion"
]