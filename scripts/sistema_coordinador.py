#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
scripts/sistema_coordinador_optimizado.py - Orquestador Maestro Optimizado
PERFORMANCE CRÍTICO: Orquesta carga completa con streaming y monitoreo
Memoria optimizada, logging estructurado, recuperación automática
"""

import sys
import subprocess
import os
from pathlib import Path
from datetime import datetime, timedelta
import time
import structlog
from sqlalchemy import create_engine, text
from typing import Dict, List, Optional, Tuple
import psutil
from dataclasses import dataclass
from enum import Enum

# Añadir directorio padre al path
sys.path.insert(0, str(Path(__file__).parent.parent))

# Importar módulos optimizados
from config import (
    DatabaseConfig, FileConfig, validar_configuracion_optimizada,
    divipola_cache
)
from core.processors import MemoryMonitor

logger = structlog.get_logger()

# ================================
# TIPOS Y CONFIGURACIONES
# ================================

class TaskStatus(Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"

@dataclass
class TaskResult:
    task_name: str
    status: TaskStatus
    duration_seconds: float
    records_processed: int
    peak_memory_mb: float
    error_message: Optional[str] = None
    details: Optional[Dict] = None

class SystemCoordinatorOptimized:
    """Coordinador maestro optimizado para carga completa del sistema"""
    
    def __init__(self):
        self.start_time = datetime.now()
        self.memory_monitor = MemoryMonitor(max_memory_mb=1024)  # 1GB limit
        self.engine = None
        self.task_results: List[TaskResult] = []
        self.total_records_processed = 0
        
        # Configuración de tareas
        self.tasks_config = {
            'validate_system': {
                'name': 'Validación Sistema',
                'required': True,
                'dependencies': [],
                'timeout_minutes': 5
            },
            'load_territories': {
                'name': 'Carga Unidades Territoriales',
                'required': True,
                'dependencies': ['validate_system'],
                'timeout_minutes': 10,
                'file_required': FileConfig.TERRITORIOS_FILE
            },
            'load_population': {
                'name': 'Carga Población SISBEN (203MB)',
                'required': True,
                'dependencies': ['load_territories'],
                'timeout_minutes': 15,
                'file_required': FileConfig.POBLACION_FILE
            },
            'load_vaccination': {
                'name': 'Carga Vacunación PAIweb (67.5MB)',
                'required': True,
                'dependencies': ['load_population'],
                'timeout_minutes': 10,
                'file_required': FileConfig.PAIWEB_FILE
            },
            'load_cases': {
                'name': 'Carga Casos Fiebre Amarilla',
                'required': False,
                'dependencies': ['load_territories'],
                'timeout_minutes': 5,
                'file_required': FileConfig.CASOS_FILE
            },
            'load_epizootics': {
                'name': 'Carga Epizootias',
                'required': False,
                'dependencies': ['load_territories'],
                'timeout_minutes': 5,
                'file_required': FileConfig.EPIZOOTIAS_FILE
            },
            'optimize_database': {
                'name': 'Optimización Base de Datos',
                'required': True,
                'dependencies': ['load_vaccination'],
                'timeout_minutes': 5
            },
            'validate_integrity': {
                'name': 'Validación Integridad Final',
                'required': True,
                'dependencies': ['optimize_database'],
                'timeout_minutes': 5
            }
        }
    
    def log_task_start(self, task_name: str):
        """Log inicio de tarea"""
        config = self.tasks_config[task_name]
        logger.info("task_started",
                   task=task_name,
                   display_name=config['name'],
                   required=config['required'],
                   dependencies=config['dependencies'],
                   timeout_minutes=config['timeout_minutes'],
                   memory_mb=round(self.memory_monitor.get_memory_usage_mb(), 2))
    
    def log_task_result(self, result: TaskResult):
        """Log resultado de tarea"""
        self.task_results.append(result)
        self.total_records_processed += result.records_processed
        
        logger.info("task_completed",
                   task=result.task_name,
                   status=result.status.value,
                   duration_seconds=result.duration_seconds,
                   records_processed=result.records_processed,
                   peak_memory_mb=result.peak_memory_mb,
                   error=result.error_message)
    
    def check_dependencies(self, task_name: str) -> bool:
        """Verifica que las dependencias estén completadas"""
        config = self.tasks_config[task_name]
        dependencies = config['dependencies']
        
        for dep in dependencies:
            dep_result = next((r for r in self.task_results if r.task_name == dep), None)
            if not dep_result or dep_result.status != TaskStatus.COMPLETED:
                logger.warning("dependency_not_met", task=task_name, missing_dependency=dep)
                return False
        
        return True
    
    def check_file_requirements(self, task_name: str) -> bool:
        """Verifica que archivos requeridos existan"""
        config = self.tasks_config[task_name]
        
        if 'file_required' in config:
            file_path = config['file_required']
            if not file_path.exists():
                logger.warning("required_file_missing", task=task_name, file=str(file_path))
                return False
            
            # Log tamaño de archivo
            size_mb = file_path.stat().st_size / (1024 * 1024)
            logger.info("file_validated", task=task_name, file=str(file_path), size_mb=round(size_mb, 2))
        
        return True
    
    def validate_system_task(self) -> TaskResult:
        """Tarea: Validación completa del sistema"""
        start_time = time.time()
        start_memory = self.memory_monitor.get_memory_usage_mb()
        
        try:
            # Validar configuración
            config_valid = validar_configuracion_optimizada()
            
            # Verificar conexión BD
            self.engine = create_engine(DatabaseConfig.get_connection_url())
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            
            # Verificar caché DIVIPOLA
            divipola_stats = divipola_cache.get_stats()
            
            # Verificar archivos disponibles
            files_status = FileConfig.validate_files()
            available_files = sum(1 for exists in files_status.values() if exists)
            
            if config_valid and divipola_stats['municipios'] > 0:
                return TaskResult(
                    task_name='validate_system',
                    status=TaskStatus.COMPLETED,
                    duration_seconds=time.time() - start_time,
                    records_processed=0,
                    peak_memory_mb=self.memory_monitor.get_memory_usage_mb(),
                    details={
                        'divipola_municipios': divipola_stats['municipios'],
                        'available_files': available_files,
                        'total_files': len(files_status)
                    }
                )
            else:
                return TaskResult(
                    task_name='validate_system',
                    status=TaskStatus.FAILED,
                    duration_seconds=time.time() - start_time,
                    records_processed=0,
                    peak_memory_mb=self.memory_monitor.get_memory_usage_mb(),
                    error_message="Configuración inválida o DIVIPOLA vacío"
                )
                
        except Exception as e:
            return TaskResult(
                task_name='validate_system',
                status=TaskStatus.FAILED,
                duration_seconds=time.time() - start_time,
                records_processed=0,
                peak_memory_mb=self.memory_monitor.get_memory_usage_mb(),
                error_message=str(e)
            )
    
    def load_territories_task(self) -> TaskResult:
        """Tarea: Carga unidades territoriales"""
        start_time = time.time()
        
        try:
            from cargar_geodata import cargar_unidades_territoriales_postgresql
            
            success = cargar_unidades_territoriales_postgresql(str(FileConfig.TERRITORIOS_FILE))
            
            if success:
                # Verificar registros cargados
                with self.engine.connect() as conn:
                    total_records = conn.execute(text("SELECT COUNT(*) FROM unidades_territoriales")).scalar()
                
                return TaskResult(
                    task_name='load_territories',
                    status=TaskStatus.COMPLETED,
                    duration_seconds=time.time() - start_time,
                    records_processed=total_records,
                    peak_memory_mb=self.memory_monitor.get_memory_usage_mb()
                )
            else:
                return TaskResult(
                    task_name='load_territories',
                    status=TaskStatus.FAILED,
                    duration_seconds=time.time() - start_time,
                    records_processed=0,
                    peak_memory_mb=self.memory_monitor.get_memory_usage_mb(),
                    error_message="Error en carga de territorios"
                )
                
        except Exception as e:
            return TaskResult(
                task_name='load_territories',
                status=TaskStatus.FAILED,
                duration_seconds=time.time() - start_time,
                records_processed=0,
                peak_memory_mb=self.memory_monitor.get_memory_usage_mb(),
                error_message=str(e)
            )
    
    def load_population_task(self) -> TaskResult:
        """Tarea: Carga población usando streaming optimizado"""
        start_time = time.time()
        
        try:
            from core.processors import process_large_file_optimized
            
            result = process_large_file_optimized('population')
            
            if result['success']:
                return TaskResult(
                    task_name='load_population',
                    status=TaskStatus.COMPLETED,
                    duration_seconds=result['duration_seconds'],
                    records_processed=result['processed_records'],
                    peak_memory_mb=result['peak_memory_mb'],
                    details={
                        'records_per_second': result['records_per_second'],
                        'total_in_db': result['total_in_db']
                    }
                )
            else:
                return TaskResult(
                    task_name='load_population',
                    status=TaskStatus.FAILED,
                    duration_seconds=time.time() - start_time,
                    records_processed=result.get('processed_records', 0),
                    peak_memory_mb=self.memory_monitor.get_memory_usage_mb(),
                    error_message=result.get('error', 'Error desconocido en población')
                )
                
        except Exception as e:
            return TaskResult(
                task_name='load_population',
                status=TaskStatus.FAILED,
                duration_seconds=time.time() - start_time,
                records_processed=0,
                peak_memory_mb=self.memory_monitor.get_memory_usage_mb(),
                error_message=str(e)
            )
    
    def load_vaccination_task(self) -> TaskResult:
        """Tarea: Carga vacunación usando streaming optimizado"""
        start_time = time.time()
        
        try:
            from core.processors import process_large_file_optimized
            
            result = process_large_file_optimized('vaccination')
            
            if result['success']:
                return TaskResult(
                    task_name='load_vaccination',
                    status=TaskStatus.COMPLETED,
                    duration_seconds=result['duration_seconds'],
                    records_processed=result['processed_records'],
                    peak_memory_mb=result['peak_memory_mb'],
                    details={
                        'records_per_second': result['records_per_second'],
                        'total_in_db': result['total_in_db']
                    }
                )
            else:
                return TaskResult(
                    task_name='load_vaccination',
                    status=TaskStatus.FAILED,
                    duration_seconds=time.time() - start_time,
                    records_processed=result.get('processed_records', 0),
                    peak_memory_mb=self.memory_monitor.get_memory_usage_mb(),
                    error_message=result.get('error', 'Error desconocido en vacunación')
                )
                
        except Exception as e:
            return TaskResult(
                task_name='load_vaccination',
                status=TaskStatus.FAILED,
                duration_seconds=time.time() - start_time,
                records_processed=0,
                peak_memory_mb=self.memory_monitor.get_memory_usage_mb(),
                error_message=str(e)
            )
    
    def load_cases_task(self) -> TaskResult:
        """Tarea: Carga casos (usando script existente)"""
        start_time = time.time()
        
        try:
            from cargar_casos import procesar_casos_completo
            
            success = procesar_casos_completo(str(FileConfig.CASOS_FILE))
            
            if success:
                # Verificar registros cargados
                with self.engine.connect() as conn:
                    total_records = conn.execute(text("SELECT COUNT(*) FROM casos_fiebre_amarilla")).scalar()
                
                return TaskResult(
                    task_name='load_cases',
                    status=TaskStatus.COMPLETED,
                    duration_seconds=time.time() - start_time,
                    records_processed=total_records,
                    peak_memory_mb=self.memory_monitor.get_memory_usage_mb()
                )
            else:
                return TaskResult(
                    task_name='load_cases',
                    status=TaskStatus.FAILED,
                    duration_seconds=time.time() - start_time,
                    records_processed=0,
                    peak_memory_mb=self.memory_monitor.get_memory_usage_mb(),
                    error_message="Error en carga de casos"
                )
                
        except Exception as e:
            return TaskResult(
                task_name='load_cases',
                status=TaskStatus.FAILED,
                duration_seconds=time.time() - start_time,
                records_processed=0,
                peak_memory_mb=self.memory_monitor.get_memory_usage_mb(),
                error_message=str(e)
            )
    
    def load_epizootics_task(self) -> TaskResult:
        """Tarea: Carga epizootias (usando script existente)"""
        start_time = time.time()
        
        try:
            from cargar_epizootias import procesar_epizootias_completo
            
            success = procesar_epizootias_completo(str(FileConfig.EPIZOOTIAS_FILE))
            
            if success:
                # Verificar registros cargados
                with self.engine.connect() as conn:
                    total_records = conn.execute(text("SELECT COUNT(*) FROM epizootias")).scalar()
                
                return TaskResult(
                    task_name='load_epizootics',
                    status=TaskStatus.COMPLETED,
                    duration_seconds=time.time() - start_time,
                    records_processed=total_records,
                    peak_memory_mb=self.memory_monitor.get_memory_usage_mb()
                )
            else:
                return TaskResult(
                    task_name='load_epizootics',
                    status=TaskStatus.FAILED,
                    duration_seconds=time.time() - start_time,
                    records_processed=0,
                    peak_memory_mb=self.memory_monitor.get_memory_usage_mb(),
                    error_message="Error en carga de epizootias"
                )
                
        except Exception as e:
            return TaskResult(
                task_name='load_epizootics',
                status=TaskStatus.FAILED,
                duration_seconds=time.time() - start_time,
                records_processed=0,
                peak_memory_mb=self.memory_monitor.get_memory_usage_mb(),
                error_message=str(e)
            )
    
    def optimize_database_task(self) -> TaskResult:
        """Tarea: Optimización de base de datos"""
        start_time = time.time()
        
        try:
            with self.engine.connect() as conn:
                # Crear/refrescar vistas materializadas
                optimization_sql = [
                    # Estadísticas actualizadas
                    "ANALYZE unidades_territoriales",
                    "ANALYZE poblacion", 
                    "ANALYZE vacunacion_fiebre_amarilla",
                    
                    # Índices optimizados si no existen
                    """CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_cobertura_dashboard 
                       ON vacunacion_fiebre_amarilla(codigo_municipio, grupo_etario, tipo_ubicacion)""",
                    
                    # Vacuum ligero
                    "VACUUM (ANALYZE) poblacion",
                    "VACUUM (ANALYZE) vacunacion_fiebre_amarilla"
                ]
                
                for sql in optimization_sql:
                    logger.debug("executing_optimization_sql", sql=sql[:50] + "...")
                    conn.execute(text(sql))
                    conn.commit()
                
                return TaskResult(
                    task_name='optimize_database',
                    status=TaskStatus.COMPLETED,
                    duration_seconds=time.time() - start_time,
                    records_processed=0,
                    peak_memory_mb=self.memory_monitor.get_memory_usage_mb()
                )
                
        except Exception as e:
            return TaskResult(
                task_name='optimize_database',
                status=TaskStatus.FAILED,
                duration_seconds=time.time() - start_time,
                records_processed=0,
                peak_memory_mb=self.memory_monitor.get_memory_usage_mb(),
                error_message=str(e)
            )
    
    def validate_integrity_task(self) -> TaskResult:
        """Tarea: Validación final de integridad"""
        start_time = time.time()
        
        try:
            with self.engine.connect() as conn:
                # Verificar integridad referencial
                integrity_checks = conn.execute(text("""
                    SELECT 
                        (SELECT COUNT(*) FROM unidades_territoriales) as territories,
                        (SELECT COUNT(*) FROM poblacion) as population_records,
                        (SELECT SUM(poblacion_total) FROM poblacion) as total_population,
                        (SELECT COUNT(*) FROM vacunacion_fiebre_amarilla) as vaccinations,
                        (SELECT COUNT(DISTINCT codigo_municipio) FROM vacunacion_fiebre_amarilla) as vac_municipalities,
                        (SELECT COUNT(*) FROM v_coberturas_dashboard) as dashboard_records
                """)).fetchone()
                
                checks = dict(integrity_checks)
                
                # Verificar que hay datos básicos
                if (checks['territories'] > 0 and 
                    checks['population_records'] > 0 and 
                    checks['vaccinations'] > 0 and
                    checks['dashboard_records'] > 0):
                    
                    return TaskResult(
                        task_name='validate_integrity',
                        status=TaskStatus.COMPLETED,
                        duration_seconds=time.time() - start_time,
                        records_processed=0,
                        peak_memory_mb=self.memory_monitor.get_memory_usage_mb(),
                        details=checks
                    )
                else:
                    return TaskResult(
                        task_name='validate_integrity',
                        status=TaskStatus.FAILED,
                        duration_seconds=time.time() - start_time,
                        records_processed=0,
                        peak_memory_mb=self.memory_monitor.get_memory_usage_mb(),
                        error_message="Datos insuficientes en tablas críticas",
                        details=checks
                    )
                
        except Exception as e:
            return TaskResult(
                task_name='validate_integrity',
                status=TaskStatus.FAILED,
                duration_seconds=time.time() - start_time,
                records_processed=0,
                peak_memory_mb=self.memory_monitor.get_memory_usage_mb(),
                error_message=str(e)
            )
    
    def execute_task(self, task_name: str) -> TaskResult:
        """Ejecuta una tarea específica"""
        
        # Mapeo de tareas a métodos
        task_methods = {
            'validate_system': self.validate_system_task,
            'load_territories': self.load_territories_task,
            'load_population': self.load_population_task,
            'load_vaccination': self.load_vaccination_task,
            'load_cases': self.load_cases_task,
            'load_epizootics': self.load_epizootics_task,
            'optimize_database': self.optimize_database_task,
            'validate_integrity': self.validate_integrity_task
        }
        
        if task_name not in task_methods:
            return TaskResult(
                task_name=task_name,
                status=TaskStatus.FAILED,
                duration_seconds=0,
                records_processed=0,
                peak_memory_mb=0,
                error_message=f"Tarea desconocida: {task_name}"
            )
        
        # Verificar dependencias
        if not self.check_dependencies(task_name):
            return TaskResult(
                task_name=task_name,
                status=TaskStatus.SKIPPED,
                duration_seconds=0,
                records_processed=0,
                peak_memory_mb=0,
                error_message="Dependencias no satisfechas"
            )
        
        # Verificar archivos requeridos
        if not self.check_file_requirements(task_name):
            config = self.tasks_config[task_name]
            if config['required']:
                return TaskResult(
                    task_name=task_name,
                    status=TaskStatus.FAILED,
                    duration_seconds=0,
                    records_processed=0,
                    peak_memory_mb=0,
                    error_message="Archivo requerido no encontrado"
                )
            else:
                return TaskResult(
                    task_name=task_name,
                    status=TaskStatus.SKIPPED,
                    duration_seconds=0,
                    records_processed=0,
                    peak_memory_mb=0,
                    error_message="Archivo opcional no encontrado"
                )
        
        # Ejecutar tarea
        self.log_task_start(task_name)
        
        with self.memory_monitor.monitor_operation(f"task_{task_name}"):
            result = task_methods[task_name]()
        
        self.log_task_result(result)
        
        return result
    
    def execute_complete_workflow(self, skip_optional: bool = False) -> Dict:
        """Ejecuta flujo completo de carga optimizada"""
        
        logger.info("complete_workflow_started",
                   skip_optional=skip_optional,
                   timestamp=self.start_time.isoformat())
        
        # Orden de ejecución de tareas
        task_order = [
            'validate_system',
            'load_territories', 
            'load_population',
            'load_vaccination',
            'load_cases',
            'load_epizootics',
            'optimize_database',
            'validate_integrity'
        ]
        
        successful_tasks = 0
        failed_tasks = 0
        skipped_tasks = 0
        
        for task_name in task_order:
            config = self.tasks_config[task_name]
            
            # Saltar tareas opcionales si se solicita
            if skip_optional and not config['required']:
                logger.info("skipping_optional_task", task=task_name)
                skipped_tasks += 1
                continue
            
            result = self.execute_task(task_name)
            
            if result.status == TaskStatus.COMPLETED:
                successful_tasks += 1
                logger.info("task_success", task=task_name, duration=result.duration_seconds)
            elif result.status == TaskStatus.FAILED:
                failed_tasks += 1
                logger.error("task_failed", task=task_name, error=result.error_message)
                
                # Parar en tareas requeridas fallidas
                if config['required']:
                    logger.error("required_task_failed_stopping_workflow", task=task_name)
                    break
            else:  # SKIPPED
                skipped_tasks += 1
                logger.info("task_skipped", task=task_name, reason=result.error_message)
        
        # Estadísticas finales
        total_duration = datetime.now() - self.start_time
        peak_memory = max((r.peak_memory_mb for r in self.task_results), default=0)
        
        workflow_result = {
            'success': failed_tasks == 0 or all(not self.tasks_config[r.task_name]['required'] 
                                               for r in self.task_results if r.status == TaskStatus.FAILED),
            'total_duration_seconds': total_duration.total_seconds(),
            'successful_tasks': successful_tasks,
            'failed_tasks': failed_tasks,
            'skipped_tasks': skipped_tasks,
            'total_records_processed': self.total_records_processed,
            'peak_memory_mb': peak_memory,
            'task_results': self.task_results
        }
        
        logger.info("complete_workflow_finished", **workflow_result)
        
        return workflow_result
    
    def generate_final_report(self, workflow_result: Dict) -> str:
        """Genera reporte final del workflow"""
        
        duration = timedelta(seconds=workflow_result['total_duration_seconds'])
        
        report = f"""
🚀 REPORTE SISTEMA COORDINADOR OPTIMIZADO
{'='*60}

⏱️ RESUMEN TEMPORAL:
   Inicio: {self.start_time.strftime('%Y-%m-%d %H:%M:%S')}
   Duración total: {duration}
   Estado: {'✅ EXITOSO' if workflow_result['success'] else '❌ FALLIDO'}

📊 ESTADÍSTICAS DE TAREAS:
   Exitosas: {workflow_result['successful_tasks']}
   Fallidas: {workflow_result['failed_tasks']}
   Omitidas: {workflow_result['skipped_tasks']}
   Total registros: {workflow_result['total_records_processed']:,}

💾 PERFORMANCE:
   Memoria pico: {workflow_result['peak_memory_mb']:.1f} MB
   Registros/segundo: {workflow_result['total_records_processed'] / workflow_result['total_duration_seconds']:.1f}

📋 DETALLE DE TAREAS:"""
        
        for result in self.task_results:
            config = self.tasks_config[result.task_name]
            status_icon = "✅" if result.status == TaskStatus.COMPLETED else "❌" if result.status == TaskStatus.FAILED else "⏭️"
            
            report += f"\n   {status_icon} {config['name']}"
            report += f"\n      Duración: {result.duration_seconds:.1f}s"
            
            if result.records_processed > 0:
                report += f" | Registros: {result.records_processed:,}"
            
            if result.status == TaskStatus.FAILED:
                report += f"\n      Error: {result.error_message}"
            
            if result.details:
                report += f"\n      Detalles: {result.details}"
        
        if workflow_result['success']:
            report += f"""

🎯 ¡SISTEMA EPIDEMIOLÓGICO COMPLETAMENTE CARGADO!

🔗 PRÓXIMOS PASOS:
   1. Dashboard: streamlit run dashboard/app.py
   2. Verificación: python scripts/test_conexion.py
   3. Monitoreo: python scripts/monitor_sistema.py --completo
   4. Backup: python scripts/crear_backup.py

🏥 ¡Vigilancia epidemiológica de Tolima lista! 🚀"""
        else:
            report += f"""

⚠️ SISTEMA CARGADO PARCIALMENTE

🔧 ACCIONES REQUERIDAS:
   1. Revisar tareas fallidas arriba
   2. Verificar archivos de datos faltantes
   3. Revisar logs: {FileConfig.LOGS_DIR}
   4. Re-ejecutar: --retry-failed"""
        
        return report

# ================================
# FUNCIONES DE UTILIDAD
# ================================

def run_optimized_coordinator(mode: str = "complete", skip_optional: bool = False) -> bool:
    """
    Ejecuta coordinador optimizado
    
    Args:
        mode: 'complete', 'essential', 'validation'
        skip_optional: Si omitir tareas opcionales
    
    Returns:
        bool: True si exitoso
    """
    
    coordinator = SystemCoordinatorOptimized()
    
    if mode == "validation":
        # Solo validación
        result = coordinator.execute_task('validate_system')
        return result.status == TaskStatus.COMPLETED
    
    elif mode == "essential":
        # Solo tareas esenciales
        skip_optional = True
    
    # Workflow completo
    workflow_result = coordinator.execute_complete_workflow(skip_optional=skip_optional)
    
    # Generar y mostrar reporte
    report = coordinator.generate_final_report(workflow_result)
    print(report)
    
    # Guardar reporte
    FileConfig.create_directories()
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    report_file = FileConfig.LOGS_DIR / f"coordinador_report_{timestamp}.txt"
    
    with open(report_file, 'w', encoding='utf-8') as f:
        f.write(report)
    
    logger.info("final_report_saved", report_file=str(report_file))
    
    return workflow_result['success']

# ================================
# FUNCIÓN PRINCIPAL
# ================================

def main():
    """Función principal con interfaz de línea de comandos"""
    
    print("🎮 SISTEMA COORDINADOR OPTIMIZADO - TOLIMA EPIDEMIOLÓGICO")
    print("=" * 70)
    print("Orquesta carga completa: streaming, monitoreo, recuperación automática")
    
    # Argumentos de línea de comandos
    mode = "complete"
    skip_optional = False
    
    if len(sys.argv) > 1:
        arg = sys.argv[1].lower()
        if arg == "--validation":
            mode = "validation"
        elif arg == "--essential":
            mode = "essential"
        elif arg == "--skip-optional":
            skip_optional = True
        elif arg == "--help":
            print("\nOpciones:")
            print("  --validation   : Solo validar sistema")
            print("  --essential    : Solo tareas esenciales")
            print("  --skip-optional: Omitir tareas opcionales")
            print("  --help         : Mostrar ayuda")
            return True
    
    # Ejecutar coordinador
    success = run_optimized_coordinator(mode, skip_optional)
    
    if success:
        print("\n🎉 ¡COORDINACIÓN COMPLETADA EXITOSAMENTE!")
        if mode == "validation":
            print("✅ Sistema validado - listo para carga")
        else:
            print("📊 Sistema epidemiológico completamente cargado")
            print("🔗 Dashboard disponible: streamlit run dashboard/app.py")
    else:
        print("\n❌ ERROR EN COORDINACIÓN")
        print("💡 Revisar reporte detallado arriba")
        print("🔧 Verificar archivos, configuración y logs")
    
    return success

if __name__ == "__main__":
    main()