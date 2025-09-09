#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
sql_init - Scripts SQL de inicialización PostgreSQL + PostGIS
"""

__version__ = "1.0.0"
__description__ = "Configuración optimizada PostgreSQL para datos epidemiológicos"

# Orden de ejecución SQL
SQL_EXECUTION_ORDER = [
    "01_extensions.sql",
    "02_schema.sql", 
    "03_views.sql"
]

__all__ = ["SQL_EXECUTION_ORDER"]