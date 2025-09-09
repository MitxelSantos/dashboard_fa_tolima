#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
core - Núcleo del sistema de procesamiento optimizado
"""

__version__ = "1.0.0"

try:
    from .processors import (
        MemoryMonitor,
        BaseStreamProcessor,
        PopulationStreamProcessor,
        VaccinationStreamProcessor,
        process_large_file_optimized
    )
    
    __all__ = [
        "MemoryMonitor",
        "BaseStreamProcessor", 
        "PopulationStreamProcessor",
        "VaccinationStreamProcessor",
        "process_large_file_optimized"
    ]
    
except ImportError:
    __all__ = []