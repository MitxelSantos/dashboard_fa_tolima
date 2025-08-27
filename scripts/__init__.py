#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
scripts/__init__.py
Módulo de scripts de procesamiento Sistema Epidemiológico Tolima V1.0
"""

__version__ = "1.0.0"
__description__ = "Scripts de procesamiento de datos epidemiológicos"

# Importaciones principales para uso del módulo
from .cargar_poblacion import procesar_poblacion_completo
from .cargar_vacunacion import procesar_vacunacion_completo
from .cargar_casos import procesar_casos_completo
from .cargar_epizootias import procesar_epizootias_completo
from .cargar_geodata import cargar_unidades_territoriales_postgresql
from .sistema_coordinador import SistemaCoordinadorTolima
from .monitor_sistema import MonitorSistemaTolima

__all__ = [
    "procesar_poblacion_completo",
    "procesar_vacunacion_completo", 
    "procesar_casos_completo",
    "procesar_epizootias_completo",
    "cargar_unidades_territoriales_postgresql",
    "SistemaCoordinadorTolima",
    "MonitorSistemaTolima"
]