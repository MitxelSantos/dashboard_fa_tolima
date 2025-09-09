#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
scripts - Scripts de procesamiento de datos epidemiológicos
"""

__version__ = "1.0.0"

# Importaciones principales de scripts
try:
    from .cargar_poblacion import cargar_poblacion_optimizado
    from .cargar_vacunacion import cargar_vacunacion_optimizado
    from .cargar_casos import procesar_casos_completo
    from .cargar_epizootias import procesar_epizootias_completo
    from .cargar_geodata import cargar_unidades_territoriales_postgresql
    from .sistema_coordinador import run_optimized_coordinator
    from .monitor_sistema import MonitorSistemaTolima
    
    __all__ = [
        "cargar_poblacion_optimizado",
        "cargar_vacunacion_optimizado",
        "procesar_casos_completo",
        "procesar_epizootias_completo",
        "cargar_unidades_territoriales_postgresql",
        "run_optimized_coordinator",
        "MonitorSistemaTolima"
    ]
    
except ImportError:
    # Importaciones flexibles para desarrollo
    __all__ = []