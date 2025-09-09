#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
core/processors.py - Procesadores Optimizados para Archivos Grandes
PERFORMANCE CRÍTICO: Streaming de 203MB población + 67.5MB vacunación
"""

import pandas as pd
import numpy as np
from datetime import datetime, date
from sqlalchemy import create_engine, text
from pathlib import Path
import psutil
import structlog
from typing import Iterator, Dict, Any, Optional, List
from abc import ABC, abstractmethod
from contextlib import contextmanager
import time
import gc

# Importar configuración optimizada
from config import (
    DatabaseConfig, FileConfig, LoadingConfig,
    divipola_cache, clasificar_grupo_etario, 
    calcular_edad_en_meses, limpiar_fecha_robusta,
    determinar_ubicacion_urbano_rural
)

logger = structlog.get_logger()

# ================================
# MONITOR DE MEMORIA OPTIMIZADO
# ================================

class MemoryMonitor:
    """Monitor de memoria para prevenir desbordamientos"""
    
    def __init__(self, max_memory_mb: int = LoadingConfig.MAX_MEMORY_USAGE_MB):
        self.max_memory_mb = max_memory_mb
        self.process = psutil.Process()
        self.initial_memory = self.get_memory_usage_mb()
    
    def get_memory_usage_mb(self) -> float:
        """Obtiene uso actual de memoria en MB"""
        return self.process.memory_info().rss / 1024 / 1024
    
    def check_memory_limit(self) -> bool:
        """Verifica si se excede el límite de memoria"""
        current_memory = self.get_memory_usage_mb()
        return current_memory > self.max_memory_mb
    
    def log_memory_usage(self, operation: str):
        """Log del uso de memoria"""
        current_memory = self.get_memory_usage_mb()
        memory_delta = current_memory - self.initial_memory
        
        logger.info("memory_usage", 
                   operation=operation,
                   current_memory_mb=round(current_memory, 2),
                   delta_memory_mb=round(memory_delta, 2),
                   max_memory_mb=self.max_memory_mb)
    
    @contextmanager
    def monitor_operation(self, operation_name: str):
        """Context manager para monitorear operaciones"""
        start_memory = self.get_memory_usage_mb()
        start_time = time.time()
        
        logger.info("operation_started", 
                   operation=operation_name,
                   initial_memory_mb=round(start_memory, 2))
        
        try:
            yield self
            
            if self.check_memory_limit():
                logger.warning("memory_limit_exceeded", 
                             operation=operation_name,
                             current_memory_mb=round(self.get_memory_usage_mb(), 2),
                             limit_mb=self.max_memory_mb)
                # Forzar garbage collection
                gc.collect()
        
        finally:
            end_memory = self.get_memory_usage_mb()
            duration = time.time() - start_time
            
            logger.info("operation_completed",
                       operation=operation_name,
                       duration_seconds=round(duration, 2),
                       peak_memory_mb=round(end_memory, 2),
                       memory_delta_mb=round(end_memory - start_memory, 2))

# ================================
# PROCESADOR BASE ABSTRACTO
# ================================

class BaseStreamProcessor(ABC):
    """Procesador base optimizado para streaming de archivos grandes"""
    
    def __init__(self, file_path: Path, chunk_size: int = 10000):
        self.file_path = file_path
        self.chunk_size = chunk_size
        self.memory_monitor = MemoryMonitor()
        self.engine = create_engine(DatabaseConfig.get_connection_url())
        self.processed_records = 0
        self.error_records = 0
        
    @abstractmethod
    def get_column_mapping(self) -> Dict[str, Any]:
        """Implementar mapeo de columnas específico por tipo de archivo"""
        pass
    
    @abstractmethod
    def process_chunk(self, chunk: pd.DataFrame) -> pd.DataFrame:
        """Implementar procesamiento específico por chunk"""
        pass
    
    @abstractmethod
    def get_table_name(self) -> str:
        """Nombre de tabla destino"""
        pass
    
    def read_file_chunks(self) -> Iterator[pd.DataFrame]:
        """Lee archivo en chunks optimizados"""
        file_size_mb = self.file_path.stat().st_size / 1024 / 1024
        
        logger.info("file_reading_started",
                   file_path=str(self.file_path),
                   file_size_mb=round(file_size_mb, 2),
                   chunk_size=self.chunk_size)
        
        try:
            if self.file_path.suffix.lower() == '.csv':
                chunk_iter = pd.read_csv(
                    self.file_path,
                    chunksize=self.chunk_size,
                    dtype=str,  # Leer todo como string inicialmente
                    low_memory=True,
                    engine='c'  # Engine C más rápido
                )
            
            elif self.file_path.suffix.lower() in ['.xlsx', '.xls']:
                # Para Excel, necesitamos estrategia diferente
                chunk_iter = self._read_excel_chunks()
            
            else:
                raise ValueError(f"Formato de archivo no soportado: {self.file_path.suffix}")
            
            chunk_number = 0
            for chunk in chunk_iter:
                chunk_number += 1
                
                logger.debug("chunk_loaded",
                           chunk_number=chunk_number,
                           chunk_size=len(chunk),
                           memory_mb=round(self.memory_monitor.get_memory_usage_mb(), 2))
                
                yield chunk
                
                # Verificar memoria después de cada chunk
                if self.memory_monitor.check_memory_limit():
                    logger.warning("memory_limit_during_reading", chunk_number=chunk_number)
                    gc.collect()
        
        except Exception as e:
            logger.error("file_reading_failed", 
                        file_path=str(self.file_path),
                        error=str(e))
            raise
    
    def _read_excel_chunks(self) -> Iterator[pd.DataFrame]:
        """Lectura optimizada de Excel en chunks"""
        try:
            # Leer Excel completo pero procesarlo en chunks
            df = pd.read_excel(self.file_path, dtype=str)
            
            total_rows = len(df)
            logger.info("excel_loaded_for_chunking", 
                       total_rows=total_rows,
                       memory_mb=round(self.memory_monitor.get_memory_usage_mb(), 2))
            
            # Dividir en chunks
            for start_idx in range(0, total_rows, self.chunk_size):
                end_idx = min(start_idx + self.chunk_size, total_rows)
                chunk = df.iloc[start_idx:end_idx].copy()
                yield chunk
                
            # Liberar memoria del DataFrame completo
            del df
            gc.collect()
            
        except Exception as e:
            logger.error("excel_chunking_failed", error=str(e))
            raise
    
    def validate_chunk(self, chunk: pd.DataFrame) -> pd.DataFrame:
        """Validaciones básicas del chunk"""
        if chunk.empty:
            logger.warning("empty_chunk_detected")
            return chunk
        
        # Log estadísticas del chunk
        logger.debug("chunk_validation",
                    rows=len(chunk),
                    columns=len(chunk.columns),
                    null_percentage=round(chunk.isnull().sum().sum() / (len(chunk) * len(chunk.columns)) * 100, 2))
        
        return chunk
    
    def save_chunk_to_db(self, processed_chunk: pd.DataFrame, is_first_chunk: bool = False):
        """Guarda chunk procesado a base de datos"""
        if processed_chunk.empty:
            logger.warning("skipping_empty_processed_chunk")
            return
        
        try:
            # Determinar si hacer append o replace
            if_exists = 'replace' if is_first_chunk else 'append'
            
            processed_chunk.to_sql(
                self.get_table_name(),
                self.engine,
                if_exists=if_exists,
                index=False,
                method='multi',  # Inserción optimizada
                chunksize=1000
            )
            
            self.processed_records += len(processed_chunk)
            
            logger.debug("chunk_saved_to_db",
                        table=self.get_table_name(),
                        rows_saved=len(processed_chunk),
                        total_processed=self.processed_records,
                        if_exists=if_exists)
            
        except Exception as e:
            self.error_records += len(processed_chunk)
            logger.error("chunk_save_failed",
                        table=self.get_table_name(),
                        rows_failed=len(processed_chunk),
                        error=str(e))
            raise
    
    def process_file_streaming(self) -> Dict[str, Any]:
        """Procesa archivo completo usando streaming"""
        start_time = time.time()
        
        with self.memory_monitor.monitor_operation(f"stream_process_{self.file_path.name}"):
            
            logger.info("streaming_process_started",
                       file_path=str(self.file_path),
                       chunk_size=self.chunk_size,
                       table=self.get_table_name())
            
            is_first_chunk = True
            
            try:
                for chunk in self.read_file_chunks():
                    
                    # Validar chunk
                    validated_chunk = self.validate_chunk(chunk)
                    
                    if validated_chunk.empty:
                        continue
                    
                    # Procesar chunk específico
                    processed_chunk = self.process_chunk(validated_chunk)
                    
                    if not processed_chunk.empty:
                        # Guardar a BD
                        self.save_chunk_to_db(processed_chunk, is_first_chunk)
                        is_first_chunk = False
                    
                    # Log progreso
                    if self.processed_records % (self.chunk_size * 5) == 0:  # Cada 5 chunks
                        logger.info("streaming_progress",
                                   processed_records=self.processed_records,
                                   error_records=self.error_records,
                                   memory_mb=round(self.memory_monitor.get_memory_usage_mb(), 2))
                
                # Estadísticas finales
                duration = time.time() - start_time
                
                # Verificar total en BD
                with self.engine.connect() as conn:
                    total_in_db = conn.execute(text(f"SELECT COUNT(*) FROM {self.get_table_name()}")).scalar()
                
                result = {
                    'success': True,
                    'processed_records': self.processed_records,
                    'error_records': self.error_records,
                    'total_in_db': total_in_db,
                    'duration_seconds': round(duration, 2),
                    'records_per_second': round(self.processed_records / duration, 2),
                    'peak_memory_mb': round(self.memory_monitor.get_memory_usage_mb(), 2)
                }
                
                logger.info("streaming_process_completed", **result)
                return result
                
            except Exception as e:
                logger.error("streaming_process_failed",
                           file_path=str(self.file_path),
                           processed_records=self.processed_records,
                           error=str(e))
                
                return {
                    'success': False,
                    'error': str(e),
                    'processed_records': self.processed_records,
                    'error_records': self.error_records
                }

# ================================
# PROCESADOR ESPECÍFICO: POBLACIÓN (203MB)
# ================================

class PopulationStreamProcessor(BaseStreamProcessor):
    """Procesador optimizado para archivo de población de 203MB"""
    
    def __init__(self, file_path: Path = FileConfig.POBLACION_FILE):
        super().__init__(file_path, LoadingConfig.POPULATION_CHUNK_SIZE)
        
        # Mapeo específico de población
        self.column_mapping = {
            1: 'codigo_municipio',    # col_1
            2: 'municipio',           # col_2  
            6: 'corregimiento',       # col_6
            8: 'vereda',              # col_8
            10: 'barrio',             # col_10
            16: 'tipo_documento',     # col_16
            17: 'documento',          # col_17
            18: 'fecha_nacimiento'    # col_18
        }
    
    def get_column_mapping(self) -> Dict[str, Any]:
        return self.column_mapping
    
    def get_table_name(self) -> str:
        return "poblacion"
    
    def process_chunk(self, chunk: pd.DataFrame) -> pd.DataFrame:
        """Procesamiento optimizado de chunk de población"""
        
        try:
            # Renombrar columnas (CSV sin headers)
            if chunk.columns[0] == 0:  # Si son índices numéricos
                chunk.columns = [f"col_{i}" for i in range(len(chunk.columns))]
            
            # Mapear solo columnas necesarias
            mapped_cols = {}
            for col_idx, col_name in self.column_mapping.items():
                col_key = f"col_{col_idx}"
                if col_key in chunk.columns:
                    mapped_cols[col_key] = col_name
            
            chunk = chunk.rename(columns=mapped_cols)
            chunk = chunk[list(mapped_cols.values())].copy()
            
            # Limpiar y validar fechas de nacimiento
            chunk['fecha_nacimiento'] = chunk['fecha_nacimiento'].apply(limpiar_fecha_robusta)
            chunk = chunk.dropna(subset=['fecha_nacimiento'])
            
            if chunk.empty:
                return chunk
            
            # Calcular edad con fecha actual
            fecha_actual = date.today()
            edad_data = chunk['fecha_nacimiento'].apply(
                lambda x: calcular_edad_en_meses(x, fecha_actual) if pd.notna(x) else None
            )
            
            chunk['edad_meses'] = edad_data
            chunk['edad_anos'] = chunk['edad_meses'] / 12
            
            # Filtrar edades válidas
            chunk = chunk[
                (chunk['edad_anos'] >= 0) & 
                (chunk['edad_anos'] <= 90)
            ]
            
            if chunk.empty:
                return chunk
            
            # Clasificar grupos etarios
            chunk['grupo_etario'] = chunk['edad_meses'].apply(clasificar_grupo_etario)
            
            # Filtrar solo grupos válidos
            chunk = chunk[chunk['grupo_etario'] != 'Sin datos']
            
            if chunk.empty:
                return chunk
            
            # Procesar ubicación
            chunk['tipo_ubicacion'] = chunk.apply(
                lambda row: determinar_ubicacion_urbano_rural(
                    row.get('vereda'), row.get('corregimiento'), row.get('barrio')
                ), axis=1
            )
            
            # Procesar códigos DIVIPOLA
            chunk['codigo_municipio'] = chunk['codigo_municipio'].astype(str).str.zfill(5)
            
            # Validar códigos Tolima
            chunk = chunk[chunk['codigo_municipio'].str.startswith('73')]
            
            if chunk.empty:
                return chunk
            
            # Eliminar duplicados por documento + tipo
            chunk['clave_documento'] = chunk['tipo_documento'].astype(str) + '_' + chunk['documento'].astype(str)
            chunk = chunk.drop_duplicates(subset=['clave_documento'], keep='first')
            
            # Agregar por dimensiones epidemiológicas
            result = chunk.groupby([
                'codigo_municipio',
                'tipo_ubicacion', 
                'grupo_etario'
            ]).size().reset_index(name='poblacion_total')
            
            # Añadir metadatos
            result['año'] = 2024
            result['fuente'] = 'SISBEN'
            result['created_at'] = datetime.now()
            
            return result
            
        except Exception as e:
            logger.error("population_chunk_processing_failed", 
                        chunk_size=len(chunk),
                        error=str(e))
            return pd.DataFrame()

# ================================
# PROCESADOR ESPECÍFICO: VACUNACIÓN (67.5MB)
# ================================

class VaccinationStreamProcessor(BaseStreamProcessor):
    """Procesador optimizado para archivo de vacunación de 67.5MB"""
    
    def __init__(self, file_path: Path = FileConfig.PAIWEB_FILE):
        super().__init__(file_path, LoadingConfig.VACCINATION_CHUNK_SIZE)
        
        # Mapeo específico de vacunación
        self.column_mapping = {
            'Departamento': 'departamento',
            'Municipio': 'municipio',
            'Institucion': 'institucion',
            'fechaaplicacion': 'fecha_aplicacion',
            'FechaNacimiento': 'fecha_nacimiento',
            'TipoUbicación': 'tipo_ubicacion'
        }
    
    def get_column_mapping(self) -> Dict[str, Any]:
        return self.column_mapping
    
    def get_table_name(self) -> str:
        return "vacunacion_fiebre_amarilla"
    
    def process_chunk(self, chunk: pd.DataFrame) -> pd.DataFrame:
        """Procesamiento optimizado de chunk de vacunación"""
        
        try:
            # Mapear columnas
            available_mapping = {k: v for k, v in self.column_mapping.items() if k in chunk.columns}
            chunk = chunk.rename(columns=available_mapping)
            chunk = chunk[list(available_mapping.values())].copy()
            
            # Limpiar fechas
            if 'fecha_aplicacion' in chunk.columns:
                chunk['fecha_aplicacion'] = chunk['fecha_aplicacion'].apply(limpiar_fecha_robusta)
            
            if 'fecha_nacimiento' in chunk.columns:
                chunk['fecha_nacimiento'] = chunk['fecha_nacimiento'].apply(limpiar_fecha_robusta)
            
            # Filtrar registros con fechas válidas
            chunk = chunk.dropna(subset=['fecha_aplicacion', 'fecha_nacimiento'])
            
            if chunk.empty:
                return chunk
            
            # Calcular edad con fecha actual (CORREGIDO)
            fecha_actual = date.today()
            chunk['edad_meses'] = chunk['fecha_nacimiento'].apply(
                lambda x: calcular_edad_en_meses(x, fecha_actual) if pd.notna(x) else None
            )
            chunk['edad_anos'] = chunk['edad_meses'] / 12
            
            # Filtrar edades válidas
            chunk = chunk[
                (chunk['edad_anos'] >= 0) & 
                (chunk['edad_anos'] <= 90)
            ]
            
            if chunk.empty:
                return chunk
            
            # Clasificar grupos etarios
            chunk['grupo_etario'] = chunk['edad_meses'].apply(clasificar_grupo_etario)
            
            # Normalizar municipios
            if 'municipio' in chunk.columns:
                chunk['codigo_municipio'] = chunk['municipio'].apply(
                    lambda x: divipola_cache.search_municipio_code(x)
                )
            
            # Normalizar ubicación
            def normalizar_ubicacion(tipo):
                if pd.isna(tipo):
                    return "Urbano"
                tipo_str = str(tipo).strip().lower()
                return "Rural" if any(k in tipo_str for k in ['rural', 'vereda']) else "Urbano"
            
            if 'tipo_ubicacion' in chunk.columns:
                chunk['tipo_ubicacion'] = chunk['tipo_ubicacion'].apply(normalizar_ubicacion)
            
            # Calcular campos temporales
            if 'fecha_aplicacion' in chunk.columns:
                chunk['año'] = chunk['fecha_aplicacion'].dt.year
                chunk['mes'] = chunk['fecha_aplicacion'].dt.month
                chunk['semana_epidemiologica'] = chunk['fecha_aplicacion'].dt.isocalendar().week
            
            # Validaciones finales
            chunk = chunk.dropna(subset=['municipio', 'fecha_aplicacion'])
            
            # Filtrar fechas coherentes
            fecha_min = date(2020, 1, 1)
            fecha_max = date.today()
            chunk = chunk[
                (chunk['fecha_aplicacion'] >= fecha_min) & 
                (chunk['fecha_aplicacion'] <= fecha_max)
            ]
            
            # Eliminar fecha nacimiento para anonimización
            if 'fecha_nacimiento' in chunk.columns:
                chunk = chunk.drop(columns=['fecha_nacimiento'])
            
            # Añadir metadatos
            chunk['fecha_carga'] = datetime.now()
            chunk['fuente'] = 'PAIweb'
            
            return chunk
            
        except Exception as e:
            logger.error("vaccination_chunk_processing_failed",
                        chunk_size=len(chunk),
                        error=str(e))
            return pd.DataFrame()

# ================================
# FUNCIÓN DE UTILIDAD PRINCIPAL
# ================================

def process_large_file_optimized(file_type: str, file_path: Optional[Path] = None) -> Dict[str, Any]:
    """
    Procesa archivos grandes de manera optimizada según el tipo
    
    Args:
        file_type: 'population' o 'vaccination'
        file_path: Ruta opcional del archivo (usa por defecto si no se especifica)
    
    Returns:
        Diccionario con resultados del procesamiento
    """
    
    processors = {
        'population': PopulationStreamProcessor,
        'vaccination': VaccinationStreamProcessor
    }
    
    if file_type not in processors:
        raise ValueError(f"Tipo de archivo no soportado: {file_type}")
    
    processor_class = processors[file_type]
    
    if file_path:
        processor = processor_class(file_path)
    else:
        processor = processor_class()
    
    return processor.process_file_streaming()

# ================================
# TESTING Y VALIDACIÓN
# ================================

if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1:
        file_type = sys.argv[1]
        
        logger.info("test_processing_started", file_type=file_type)
        
        result = process_large_file_optimized(file_type)
        
        if result['success']:
            logger.info("test_processing_success", **result)
        else:
            logger.error("test_processing_failed", **result)
    else:
        print("Uso: python processors.py [population|vaccination]")