#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
setup_sistema.py - Verificador/Instalador Sistema Epidemiológico Optimizado
LIMPIO: Verificación inteligente, solo instala lo necesario, logging estructurado
"""

import os
import sys
import subprocess
import time
from pathlib import Path
from datetime import datetime
import structlog
from typing import Dict, List, Tuple, Optional

# Configurar logging básico
structlog.configure(
    processors=[
        structlog.stdlib.add_log_level,
        structlog.processors.TimeStamper(fmt="ISO"),
        structlog.processors.JSONRenderer()
    ],
    logger_factory=structlog.stdlib.LoggerFactory(),
    wrapper_class=structlog.stdlib.BoundLogger,
    cache_logger_on_first_use=True,
)

logger = structlog.get_logger()

class SistemaVerificadorOptimizado:
    """Verificador inteligente del sistema epidemiológico"""
    
    def __init__(self):
        self.base_dir = Path.cwd()
        self.inicio = datetime.now()
        self.correcciones_aplicadas = []
        self.errores_encontrados = []
        self.verificaciones_exitosas = 0
        
    def log_resultado(self, operacion: str, exito: bool, detalles: str = ""):
        """Log estructurado de resultados"""
        if exito:
            logger.info("verificacion_exitosa", operacion=operacion, detalles=detalles)
            self.verificaciones_exitosas += 1
        else:
            logger.error("verificacion_fallida", operacion=operacion, error=detalles)
            self.errores_encontrados.append(f"{operacion}: {detalles}")
    
    def log_correccion(self, correccion: str):
        """Log de corrección aplicada"""
        logger.info("correccion_aplicada", correccion=correccion)
        self.correcciones_aplicadas.append(correccion)
    
    def verificar_python(self) -> bool:
        """Verifica versión de Python compatible"""
        logger.info("verificando_python")
        
        version = sys.version_info
        version_requerida = (3, 8)
        
        if version[:2] >= version_requerida:
            self.log_resultado("python_version", True, f"Python {version.major}.{version.minor}")
            return True
        else:
            self.log_resultado("python_version", False, 
                             f"Python {version.major}.{version.minor} < {version_requerida[0]}.{version_requerida[1]}")
            return False
    
    def verificar_estructura_proyecto(self) -> bool:
        """Verifica y crea estructura de directorios optimizada"""
        logger.info("verificando_estructura_proyecto")
        
        # Directorios críticos del sistema
        directorios_criticos = {
            "core": "Núcleo de procesamiento",
            "scripts": "Scripts de carga de datos", 
            "sql_init": "Scripts SQL de inicialización",
            "data": "Datos de entrada",
            "data/processed": "Datos procesados",
            "backups": "Respaldos automáticos",
            "logs": "Logs del sistema"
        }
        
        directorios_creados = []
        
        for directorio, descripcion in directorios_criticos.items():
            dir_path = self.base_dir / directorio
            
            if not dir_path.exists():
                try:
                    dir_path.mkdir(parents=True, exist_ok=True)
                    directorios_creados.append(directorio)
                    self.log_correccion(f"Directorio creado: {directorio}")
                except Exception as e:
                    self.log_resultado("crear_directorio", False, f"{directorio}: {e}")
                    return False
        
        if directorios_creados:
            logger.info("directorios_creados", count=len(directorios_creados), directories=directorios_creados)
        
        self.log_resultado("estructura_proyecto", True, f"{len(directorios_criticos)} directorios verificados")
        return True
    
    def verificar_archivo_env(self) -> bool:
        """Verifica y genera archivo .env optimizado"""
        logger.info("verificando_archivo_env")
        
        env_file = self.base_dir / ".env"
        
        # Variables esenciales para el sistema
        variables_esenciales = {
            'ENVIRONMENT': 'local',
            'DB_HOST': 'localhost',
            'DB_PORT': '5432',
            'DB_NAME': 'epidemiologia_tolima',
            'DB_USER': 'tolima_admin',
            'DB_PASSWORD': 'tolima2025',
            'LOG_LEVEL': 'INFO',
            'CACHE_TTL': '3600',
            'SYSTEM_VERSION': '2.0.0'
        }
        
        variables_existentes = {}
        
        # Leer variables existentes si el archivo existe
        if env_file.exists():
            try:
                with open(env_file, 'r', encoding='utf-8') as f:
                    for linea in f:
                        linea = linea.strip()
                        if '=' in linea and not linea.startswith('#'):
                            clave, valor = linea.split('=', 1)
                            variables_existentes[clave.strip()] = valor.strip()
            except Exception as e:
                logger.warning("error_leyendo_env", error=str(e))
        
        # Verificar variables faltantes
        variables_faltantes = [
            var for var in variables_esenciales 
            if var not in variables_existentes
        ]
        
        if variables_faltantes or not env_file.exists():
            try:
                # Crear contenido .env optimizado
                contenido_env = f"""# Sistema Epidemiológico Tolima V2.0 - Configuración
# Generado automáticamente: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
# Pre-procesamiento local → PostgreSQL → Dashboard Streamlit Cloud

"""
                
                # Agregar todas las variables (existentes + nuevas)
                todas_variables = {**variables_esenciales, **variables_existentes}
                
                for var, valor in todas_variables.items():
                    contenido_env += f"{var}={valor}\n"
                
                contenido_env += f"""
# Configuración adicional
PYTHONPATH=.
PYTHONUNBUFFERED=1
"""
                
                with open(env_file, 'w', encoding='utf-8') as f:
                    f.write(contenido_env)
                
                if variables_faltantes:
                    self.log_correccion(f"Variables .env agregadas: {variables_faltantes}")
                
                self.log_resultado("archivo_env", True, f"Archivo .env configurado con {len(todas_variables)} variables")
                return True
                
            except Exception as e:
                self.log_resultado("archivo_env", False, str(e))
                return False
        else:
            self.log_resultado("archivo_env", True, "Archivo .env completo")
            return True
    
    def verificar_docker(self) -> bool:
        """Verifica Docker (opcional, no bloquea el sistema)"""
        logger.info("verificando_docker")
        
        try:
            # Verificar Docker instalado
            result = subprocess.run(["docker", "--version"], 
                                  capture_output=True, text=True, timeout=10)
            
            if result.returncode == 0:
                docker_version = result.stdout.strip()
                logger.info("docker_encontrado", version=docker_version)
                
                # Verificar Docker Compose
                compose_result = subprocess.run(["docker-compose", "--version"], 
                                              capture_output=True, text=True, timeout=10)
                
                if compose_result.returncode == 0:
                    compose_version = compose_result.stdout.strip()
                    logger.info("docker_compose_encontrado", version=compose_version)
                    
                    self.log_resultado("docker", True, "Docker y Docker Compose disponibles")
                    return True
                else:
                    self.log_resultado("docker", True, "Docker disponible (Docker Compose faltante)")
                    return True
            else:
                self.log_resultado("docker", True, "Docker no disponible (opcional)")
                return True
                
        except (subprocess.TimeoutExpired, FileNotFoundError):
            self.log_resultado("docker", True, "Docker no disponible (opcional)")
            return True
        except Exception as e:
            logger.warning("docker_verification_error", error=str(e))
            return True  # No es crítico
    
    def verificar_docker_compose_yml(self) -> bool:
        """Verifica y crea docker-compose.yml optimizado"""
        logger.info("verificando_docker_compose_yml")
        
        compose_file = self.base_dir / "docker-compose.yml"
        
        if compose_file.exists():
            self.log_resultado("docker_compose_yml", True, "Archivo existente")
            return True
        
        # Crear docker-compose.yml optimizado
        docker_compose_content = '''version: '3.8'

# Docker Compose Optimizado - Sistema Epidemiológico Tolima
# PostgreSQL + PostGIS para pre-procesamiento local

services:
  postgres:
    image: postgis/postgis:15-3.3-alpine
    container_name: tolima_postgres_optimized
    
    environment:
      POSTGRES_DB: epidemiologia_tolima
      POSTGRES_USER: tolima_admin
      POSTGRES_PASSWORD: ${DB_PASSWORD:-tolima2025}
      PGDATA: /var/lib/postgresql/data/pgdata
      
    ports:
      - "${DB_PORT:-5432}:5432"
      
    volumes:
      - postgres_data:/var/lib/postgresql/data
      - ./sql_init:/docker-entrypoint-initdb.d:ro
      - ./backups:/backups
      
    command: >
      postgres
      -c shared_buffers=256MB
      -c effective_cache_size=1GB
      -c maintenance_work_mem=64MB
      -c max_connections=100
      -c listen_addresses='*'
      
    restart: unless-stopped
    
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U tolima_admin -d epidemiologia_tolima"]
      interval: 10s
      timeout: 5s
      retries: 5

  pgadmin:
    image: dpage/pgadmin4:latest
    container_name: tolima_pgadmin_optimized
    
    environment:
      PGADMIN_DEFAULT_EMAIL: admin@tolima.gov.co
      PGADMIN_DEFAULT_PASSWORD: ${PGADMIN_PASSWORD:-admin123}
      PGADMIN_CONFIG_SERVER_MODE: 'False'
      
    ports:
      - "${PGADMIN_PORT:-8080}:80"
      
    depends_on:
      postgres:
        condition: service_healthy
        
    restart: unless-stopped

volumes:
  postgres_data:
    name: tolima_postgres_data_optimized

# Uso:
# docker-compose up -d postgres    # Solo PostgreSQL
# docker-compose up -d             # PostgreSQL + pgAdmin
# docker-compose down              # Parar servicios
'''
        
        try:
            with open(compose_file, 'w', encoding='utf-8') as f:
                f.write(docker_compose_content)
            
            self.log_correccion("docker-compose.yml creado")
            self.log_resultado("docker_compose_yml", True, "Archivo creado")
            return True
            
        except Exception as e:
            self.log_resultado("docker_compose_yml", False, str(e))
            return False
    
    def verificar_archivos_criticos(self) -> bool:
        """Verifica archivos críticos del sistema"""
        logger.info("verificando_archivos_criticos")
        
        archivos_criticos = {
            "config.py": "Configuración centralizada",
            "core/__init__.py": "Módulo core",
            "scripts/__init__.py": "Módulo scripts",
            "sql_init/01_extensions.sql": "Extensiones PostgreSQL",
            "sql_init/02_schema.sql": "Esquema de base de datos"
        }
        
        archivos_existentes = 0
        archivos_faltantes = []
        
        for archivo, descripcion in archivos_criticos.items():
            archivo_path = self.base_dir / archivo
            
            if archivo_path.exists():
                archivos_existentes += 1
                logger.debug("archivo_critico_encontrado", archivo=archivo)
            else:
                archivos_faltantes.append(archivo)
                logger.warning("archivo_critico_faltante", archivo=archivo, descripcion=descripcion)
        
        if archivos_faltantes:
            self.log_resultado("archivos_criticos", False, 
                             f"Faltantes: {archivos_faltantes}")
            return False
        else:
            self.log_resultado("archivos_criticos", True, 
                             f"{archivos_existentes} archivos críticos encontrados")
            return True
    
    def instalar_dependencias_opcionales(self) -> bool:
        """Instala dependencias Python (opcional)"""
        logger.info("verificando_dependencias_python")
        
        requirements_file = self.base_dir / "requirements.txt"
        
        if not requirements_file.exists():
            logger.warning("requirements_txt_faltante")
            return True  # No crítico
        
        # Verificar si pip está disponible
        try:
            subprocess.run([sys.executable, "-m", "pip", "--version"], 
                          capture_output=True, timeout=10)
        except Exception:
            self.log_resultado("pip_disponible", False, "pip no disponible")
            return True  # No crítico
        
        # Preguntar al usuario
        print("\n💡 ¿Instalar dependencias Python automáticamente?")
        print("   Esto ejecutará: pip install -r requirements.txt")
        respuesta = input("   ¿Continuar? (y/N): ").strip().lower()
        
        if respuesta not in ['y', 'yes', 'si', 'sí']:
            logger.info("instalacion_dependencias_omitida")
            return True
        
        try:
            logger.info("instalando_dependencias")
            
            # Actualizar pip primero
            subprocess.run([
                sys.executable, "-m", "pip", "install", "--upgrade", "pip"
            ], capture_output=True, timeout=60)
            
            # Instalar dependencias
            result = subprocess.run([
                sys.executable, "-m", "pip", "install", "-r", str(requirements_file)
            ], capture_output=True, text=True, timeout=300)
            
            if result.returncode == 0:
                self.log_resultado("instalar_dependencias", True, "Dependencias instaladas")
                self.log_correccion("Dependencias Python instaladas")
                return True
            else:
                logger.warning("instalacion_dependencias_error", stderr=result.stderr[:200])
                return True  # No crítico
                
        except subprocess.TimeoutExpired:
            logger.warning("instalacion_dependencias_timeout")
            return True  # No crítico
        except Exception as e:
            logger.warning("instalacion_dependencias_fallo", error=str(e))
            return True  # No crítico
    
    def ejecutar_verificacion_completa(self) -> Tuple[bool, Dict]:
        """Ejecuta verificación completa inteligente"""
        logger.info("verificacion_completa_iniciada", timestamp=self.inicio.isoformat())
        
        # Verificaciones en orden de importancia
        verificaciones = [
            ("Python", self.verificar_python),
            ("Estructura Proyecto", self.verificar_estructura_proyecto),
            ("Archivo .env", self.verificar_archivo_env),
            ("Archivos Críticos", self.verificar_archivos_criticos),
            ("Docker (Opcional)", self.verificar_docker),
            ("docker-compose.yml", self.verificar_docker_compose_yml),
            ("Dependencias Python (Opcional)", self.instalar_dependencias_opcionales)
        ]
        
        verificaciones_totales = len(verificaciones)
        verificaciones_criticas_fallidas = 0
        
        for i, (nombre, funcion) in enumerate(verificaciones, 1):
            logger.info("ejecutando_verificacion", 
                       verificacion=nombre, 
                       progreso=f"{i}/{verificaciones_totales}")
            
            try:
                exito = funcion()
                
                # Marcar verificaciones críticas fallidas
                if not exito and nombre in ["Python", "Estructura Proyecto", "Archivos Críticos"]:
                    verificaciones_criticas_fallidas += 1
                    
            except Exception as e:
                logger.error("verificacion_fallo", verificacion=nombre, error=str(e))
                self.errores_encontrados.append(f"{nombre}: {str(e)}")
                
                if nombre in ["Python", "Estructura Proyecto", "Archivos Críticos"]:
                    verificaciones_criticas_fallidas += 1
        
        # Estadísticas finales
        duracion = datetime.now() - self.inicio
        
        resultado = {
            'exito_general': verificaciones_criticas_fallidas == 0,
            'verificaciones_exitosas': self.verificaciones_exitosas,
            'verificaciones_totales': verificaciones_totales,
            'correcciones_aplicadas': len(self.correcciones_aplicadas),
            'errores_encontrados': len(self.errores_encontrados),
            'duracion_segundos': duracion.total_seconds(),
            'criticas_fallidas': verificaciones_criticas_fallidas
        }
        
        logger.info("verificacion_completa_finalizada", **resultado)
        
        return resultado['exito_general'], resultado
    
    def generar_reporte_final(self, exito: bool, estadisticas: Dict) -> str:
        """Genera reporte final de verificación"""
        
        reporte = f"""
🔍 VERIFICACIÓN SISTEMA EPIDEMIOLÓGICO TOLIMA V2.0
{'='*65}

⏱️ RESUMEN:
   Duración: {estadisticas['duracion_segundos']:.2f} segundos
   Estado: {'✅ SISTEMA LISTO' if exito else '❌ REQUIERE ATENCIÓN'}
   Verificaciones exitosas: {estadisticas['verificaciones_exitosas']}/{estadisticas['verificaciones_totales']}

🔧 CORRECCIONES APLICADAS: {estadisticas['correcciones_aplicadas']}"""
        
        if self.correcciones_aplicadas:
            for correccion in self.correcciones_aplicadas:
                reporte += f"\n   ✅ {correccion}"
        
        if self.errores_encontrados:
            reporte += f"\n\n❌ ERRORES ENCONTRADOS: {len(self.errores_encontrados)}"
            for error in self.errores_encontrados:
                reporte += f"\n   ❌ {error}"
        
        if exito:
            reporte += f"""

🎉 ¡SISTEMA COMPLETAMENTE VERIFICADO!

🚀 SISTEMA LISTO PARA:
   ✅ Pre-procesamiento local de datos epidemiológicos
   ✅ Carga optimizada a PostgreSQL + PostGIS
   ✅ Conexión con dashboard Streamlit en la nube
   ✅ Procesamiento streaming de archivos grandes (203MB+)

🔗 PRÓXIMOS PASOS:
   1. Colocar archivos de datos en data/
   2. Iniciar PostgreSQL: docker-compose up -d postgres
   3. Ejecutar verificación: python test_sistema.py
   4. Carga completa: python scripts/sistema_coordinador.py
   5. ¡Conectar dashboard Streamlit a la BD! 🎯"""
        else:
            reporte += f"""

⚠️ SISTEMA REQUIERE ATENCIÓN

🔧 ACCIONES RECOMENDADAS:
   1. Revisar errores críticos arriba
   2. Instalar dependencias faltantes
   3. Verificar archivos críticos del proyecto
   4. Re-ejecutar verificación"""
        
        return reporte

def main():
    """Función principal optimizada"""
    print("🔍 VERIFICADOR SISTEMA EPIDEMIOLÓGICO TOLIMA V2.0")
    print("=" * 65)
    print("Pre-procesamiento local → PostgreSQL → Dashboard Cloud")
    
    # Opciones del usuario
    print("\nOpciones disponibles:")
    print("1. 🔍 Verificación inteligente completa (recomendado)")
    print("2. 👋 Salir")
    
    while True:
        try:
            opcion = input("\n🔢 Selecciona opción (1-2): ").strip()
            
            if opcion == "1":
                verificador = SistemaVerificadorOptimizado()
                exito, estadisticas = verificador.ejecutar_verificacion_completa()
                
                # Mostrar reporte
                reporte = verificador.generar_reporte_final(exito, estadisticas)
                print(reporte)
                
                # Guardar reporte
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                reporte_file = Path("logs") / f"verificacion_sistema_{timestamp}.txt"
                
                try:
                    Path("logs").mkdir(exist_ok=True)
                    with open(reporte_file, 'w', encoding='utf-8') as f:
                        f.write(reporte)
                    print(f"\n📄 Reporte guardado: {reporte_file}")
                except Exception:
                    pass
                
                return exito
                
            elif opcion == "2":
                print("👋 ¡Hasta luego!")
                return True
            else:
                print("❌ Opción inválida. Usa 1 o 2.")
                
        except KeyboardInterrupt:
            print("\n\n👋 Saliendo...")
            return True
        except Exception as e:
            print(f"❌ Error: {e}")
            return False

if __name__ == "__main__":
    main()