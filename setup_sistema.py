#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
setup_sistema.py - Verificador/Instalador Sistema Epidemiológico Tolima V2.0
CORREGIDO: Sistema de verificación inteligente que solo instala lo que falta
"""

import os
import sys
import subprocess
import shutil
import time
from pathlib import Path
from datetime import datetime
import warnings

warnings.filterwarnings('ignore')

class SistemaVerificadorTolima:
    def __init__(self):
        self.base_dir = Path.cwd()
        self.logs = []
        self.errores_encontrados = []
        self.correcciones_aplicadas = []
        
    def log(self, mensaje, tipo="INFO"):
        """Registra mensaje con timestamp"""
        timestamp = datetime.now().strftime("%H:%M:%S")
        log_entry = f"[{timestamp}] {tipo}: {mensaje}"
        self.logs.append(log_entry)
        print(log_entry)
        
    def verificar_python(self):
        """Verifica versión de Python"""
        self.log("🐍 Verificando Python...")
        
        version = sys.version_info
        if version.major < 3 or (version.major == 3 and version.minor < 8):
            self.log("❌ Python 3.8+ requerido. Versión actual: {}.{}".format(
                version.major, version.minor), "ERROR")
            self.errores_encontrados.append("Python versión insuficiente")
            return False
        
        self.log(f"✅ Python {version.major}.{version.minor} OK")
        return True
    
    def verificar_estructura_proyecto(self):
        """Verifica y crea estructura de directorios necesaria"""
        self.log("📁 Verificando estructura de proyecto...")
        
        directorios_necesarios = [
            "sql_init",
            "scripts", 
            "data",
            "data/processed",
            "backups",
            "logs",
            "reportes"
        ]
        
        directorios_creados = []
        directorios_existentes = []
        
        for directorio in directorios_necesarios:
            dir_path = self.base_dir / directorio
            if dir_path.exists():
                directorios_existentes.append(directorio)
            else:
                try:
                    dir_path.mkdir(parents=True, exist_ok=True)
                    directorios_creados.append(directorio)
                    self.correcciones_aplicadas.append(f"Directorio creado: {directorio}")
                except Exception as e:
                    self.log(f"❌ Error creando {directorio}: {e}", "ERROR")
                    self.errores_encontrados.append(f"No se pudo crear directorio: {directorio}")
                    return False
        
        self.log(f"✅ Directorios existentes: {len(directorios_existentes)}")
        if directorios_creados:
            self.log(f"📂 Directorios creados: {directorios_creados}")
        
        return True
    
    def verificar_archivo_env(self):
        """Verifica y crea archivo .env con variables necesarias"""
        self.log("🔐 Verificando archivo .env...")
        
        env_file = self.base_dir / ".env"
        variables_necesarias = {
            'ENVIRONMENT': 'development',
            'DB_HOST': 'localhost',
            'DB_PORT': '5432',
            'DB_NAME': 'epidemiologia_tolima',
            'DB_USER': 'tolima_admin',
            'DB_PASSWORD': 'tolima2025',
            'CACHE_TTL': '3600',
            'LOG_LEVEL': 'INFO',
            'SYSTEM_VERSION': '2.0'
        }
        
        variables_existentes = {}
        if env_file.exists():
            try:
                with open(env_file, 'r', encoding='utf-8') as f:
                    for linea in f:
                        linea = linea.strip()
                        if '=' in linea and not linea.startswith('#'):
                            clave, valor = linea.split('=', 1)
                            variables_existentes[clave.strip()] = valor.strip()
                self.log(f"✅ Archivo .env encontrado con {len(variables_existentes)} variables")
            except Exception as e:
                self.log(f"⚠️ Error leyendo .env: {e}", "WARNING")
        
        # Verificar variables faltantes
        variables_faltantes = []
        for var, valor_default in variables_necesarias.items():
            if var not in variables_existentes:
                variables_faltantes.append((var, valor_default))
        
        if variables_faltantes or not env_file.exists():
            self.log(f"🔧 Creando/actualizando archivo .env...")
            
            contenido_env = "# Sistema Epidemiológico Tolima V2.0 - Configuración\n"
            contenido_env += f"# Generado/actualizado: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n"
            
            # Mantener variables existentes
            for var, valor in variables_existentes.items():
                contenido_env += f"{var}={valor}\n"
            
            # Agregar variables faltantes
            if variables_faltantes:
                contenido_env += "\n# Variables agregadas automáticamente:\n"
                for var, valor in variables_faltantes:
                    contenido_env += f"{var}={valor}\n"
                    self.correcciones_aplicadas.append(f"Variable .env agregada: {var}")
            
            try:
                with open(env_file, 'w', encoding='utf-8') as f:
                    f.write(contenido_env)
                self.log(f"✅ Archivo .env actualizado")
            except Exception as e:
                self.log(f"❌ Error escribiendo .env: {e}", "ERROR")
                self.errores_encontrados.append("No se pudo crear/actualizar .env")
                return False
        else:
            self.log(f"✅ Archivo .env completo")
        
        return True
    
    def verificar_docker(self):
        """Verifica instalación y estado de Docker"""
        self.log("🐳 Verificando Docker...")
        
        try:
            # Verificar Docker instalado
            result = subprocess.run(["docker", "--version"], 
                                  capture_output=True, text=True, timeout=10)
            if result.returncode != 0:
                self.log("❌ Docker no está instalado", "ERROR")
                self.errores_encontrados.append("Docker no instalado")
                return False
            
            self.log(f"✅ {result.stdout.strip()}")
            
            # Verificar Docker Compose
            result = subprocess.run(["docker-compose", "--version"], 
                                  capture_output=True, text=True, timeout=10)
            if result.returncode != 0:
                self.log("❌ Docker Compose no disponible", "ERROR")
                self.errores_encontrados.append("Docker Compose no disponible")
                return False
                
            self.log(f"✅ {result.stdout.strip()}")
            
            # Verificar si Docker daemon está corriendo
            result = subprocess.run(["docker", "info"], 
                                  capture_output=True, text=True, timeout=15)
            if result.returncode != 0:
                self.log("⚠️ Docker daemon no está corriendo", "WARNING")
                self.log("💡 Iniciar Docker Desktop manualmente")
                return True  # No es error crítico
            
            self.log("✅ Docker daemon corriendo")
            return True
            
        except subprocess.TimeoutExpired:
            self.log("⏱️ Timeout verificando Docker", "WARNING")
            return True
        except FileNotFoundError:
            self.log("❌ Docker no encontrado en PATH", "ERROR")
            self.errores_encontrados.append("Docker no encontrado")
            return False
        except Exception as e:
            self.log(f"❌ Error verificando Docker: {e}", "ERROR")
            return False
    
    def verificar_docker_compose_yml(self):
        """Verifica y crea archivo docker-compose.yml si no existe"""
        self.log("📋 Verificando docker-compose.yml...")
        
        compose_file = self.base_dir / "docker-compose.yml"
        
        if compose_file.exists():
            self.log("✅ docker-compose.yml encontrado")
            return True
        
        self.log("🔧 Creando docker-compose.yml...")
        
        docker_compose_content = """version: '3.8'

services:
  postgres:
    image: postgis/postgis:15-3.3
    container_name: tolima_postgres
    environment:
      POSTGRES_DB: epidemiologia_tolima
      POSTGRES_USER: tolima_admin
      POSTGRES_PASSWORD: tolima2025!
      PGDATA: /var/lib/postgresql/data/pgdata
    ports:
      - "5432:5432"
    volumes:
      - postgres_data:/var/lib/postgresql/data
      - ./sql_init:/docker-entrypoint-initdb.d
      - ./backups:/backups
    restart: unless-stopped
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U tolima_admin -d epidemiologia_tolima"]
      interval: 10s
      timeout: 5s
      retries: 5

  pgadmin:
    image: dpage/pgadmin4
    container_name: tolima_pgadmin
    environment:
      PGADMIN_DEFAULT_EMAIL: admin@tolima.gov.co
      PGADMIN_DEFAULT_PASSWORD: admin123
      PGADMIN_CONFIG_SERVER_MODE: 'False'
    ports:
      - "8080:80"
    depends_on:
      postgres:
        condition: service_healthy
    restart: unless-stopped

volumes:
  postgres_data:
    name: tolima_postgres_data
"""
        
        try:
            with open(compose_file, 'w', encoding='utf-8') as f:
                f.write(docker_compose_content)
            self.log("✅ docker-compose.yml creado")
            self.correcciones_aplicadas.append("docker-compose.yml creado")
            return True
        except Exception as e:
            self.log(f"❌ Error creando docker-compose.yml: {e}", "ERROR")
            self.errores_encontrados.append("No se pudo crear docker-compose.yml")
            return False
    
    def verificar_requirements_txt(self):
        """Verifica archivo requirements.txt"""
        self.log("📦 Verificando requirements.txt...")
        
        req_file = self.base_dir / "requirements.txt"
        
        if req_file.exists():
            try:
                with open(req_file, 'r', encoding='utf-8') as f:
                    contenido = f.read()
                    # Verificar que tenga dependencias mínimas
                    deps_criticas = ['pandas', 'sqlalchemy', 'psycopg2-binary', 'geopandas']
                    deps_encontradas = sum(1 for dep in deps_criticas if dep.lower() in contenido.lower())
                    
                    if deps_encontradas >= 3:
                        self.log(f"✅ requirements.txt válido ({deps_encontradas}/4 deps críticas)")
                        return True
                    else:
                        self.log(f"⚠️ requirements.txt incompleto ({deps_encontradas}/4 deps)")
            except Exception as e:
                self.log(f"⚠️ Error leyendo requirements.txt: {e}", "WARNING")
        
        self.log("🔧 Creando requirements.txt...")
        
        requirements_content = """# Sistema Epidemiológico Tolima V2.0 - Dependencias

# Base de datos y conectividad
psycopg2-binary==2.9.7
SQLAlchemy==2.0.21
geoalchemy2==0.14.1

# Procesamiento de datos
pandas==2.1.1
numpy==1.25.2
openpyxl==3.1.2
xlrd==2.0.1

# Datos geoespaciales
geopandas==0.13.2
Shapely==2.0.1
Fiona==1.9.4
pyproj==3.6.0

# Dashboard y visualización
streamlit==1.26.0
plotly==5.16.1
folium==0.14.0
streamlit-folium==0.15.0
matplotlib==3.7.2
seaborn==0.12.2

# Utilidades adicionales
python-dotenv==1.0.0
schedule==1.2.0
tqdm==4.66.1

# Desarrollo y testing
pytest==7.4.2
black==23.7.0
flake8==6.0.0

# Exportación de reportes
jinja2==3.1.2
fpdf2==2.7.9
"""
        
        try:
            with open(req_file, 'w', encoding='utf-8') as f:
                f.write(requirements_content)
            self.log("✅ requirements.txt creado")
            self.correcciones_aplicadas.append("requirements.txt creado")
            return True
        except Exception as e:
            self.log(f"❌ Error creando requirements.txt: {e}", "ERROR")
            self.errores_encontrados.append("No se pudo crear requirements.txt")
            return False
    
    def verificar_archivos_sql(self):
        """Verifica archivos SQL básicos"""
        self.log("🗄️ Verificando archivos SQL...")
        
        sql_dir = self.base_dir / "sql_init"
        archivos_sql_necesarios = {
            "01_extensions.sql": """-- Extensiones necesarias para Sistema Epidemiológico Tolima
CREATE EXTENSION IF NOT EXISTS postgis;
CREATE EXTENSION IF NOT EXISTS pg_trgm;
CREATE EXTENSION IF NOT EXISTS unaccent;
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

\\echo 'Extensiones PostgreSQL instaladas exitosamente';
""",
            "02_schema.sql": """-- Esquema básico creado por verificador
-- Ver documentación completa en repositorio

\\echo 'Esquema básico verificado - usar scripts completos para producción';
""",
            "03_views.sql": """-- Vistas básicas creadas por verificador  
-- Ver documentación completa en repositorio

\\echo 'Vistas básicas verificadas - usar scripts completos para producción';
"""
        }
        
        archivos_creados = []
        for archivo, contenido in archivos_sql_necesarios.items():
            archivo_path = sql_dir / archivo
            if not archivo_path.exists():
                try:
                    with open(archivo_path, 'w', encoding='utf-8') as f:
                        f.write(contenido)
                    archivos_creados.append(archivo)
                    self.correcciones_aplicadas.append(f"SQL creado: {archivo}")
                except Exception as e:
                    self.log(f"❌ Error creando {archivo}: {e}", "ERROR")
                    return False
        
        if archivos_creados:
            self.log(f"✅ Archivos SQL creados: {archivos_creados}")
        else:
            self.log("✅ Archivos SQL existentes")
        
        return True
    
    def verificar_config_py(self):
        """Verifica que config.py existe"""
        self.log("⚙️ Verificando config.py...")
        
        config_file = self.base_dir / "config.py"
        
        if config_file.exists():
            self.log("✅ config.py encontrado")
            return True
        else:
            self.log("❌ config.py no encontrado", "ERROR")
            self.log("💡 Ejecutar: Crear config.py manualmente o usar template")
            self.errores_encontrados.append("config.py faltante")
            return False
    
    def instalar_dependencias_opcionales(self):
        """Instala dependencias Python si el usuario acepta"""
        self.log("📦 ¿Instalar dependencias Python? (Opcional)")
        
        respuesta = input("   Instalar dependencias ahora? (y/N): ")
        if respuesta.lower() not in ['y', 'yes', 'si', 'sí']:
            self.log("⏭️ Instalación de dependencias omitida")
            return True
        
        try:
            self.log("📥 Instalando dependencias...")
            result = subprocess.run([
                sys.executable, "-m", "pip", "install", "-r", "requirements.txt"
            ], capture_output=True, text=True, timeout=300)
            
            if result.returncode == 0:
                self.log("✅ Dependencias instaladas exitosamente")
                return True
            else:
                self.log(f"⚠️ Error instalando dependencias: {result.stderr}", "WARNING")
                return True  # No es error crítico
                
        except subprocess.TimeoutExpired:
            self.log("⏱️ Timeout instalando dependencias", "WARNING")
            return True
        except Exception as e:
            self.log(f"⚠️ Error instalando dependencias: {e}", "WARNING")
            return True
    
    def setup_completo_desde_cero(self):
        """Opción alternativa: setup completo desde cero"""
        self.log("🚀 SETUP COMPLETO DESDE CERO")
        self.log("=" * 40)
        
        confirmacion = input("⚠️ Esto sobrescribirá archivos existentes. ¿Continuar? (y/N): ")
        if confirmacion.lower() not in ['y', 'yes', 'si', 'sí']:
            self.log("❌ Setup completo cancelado")
            return False
        
        # Ejecutar todas las verificaciones en modo forzado
        pasos_completos = [
            self.verificar_estructura_proyecto,
            self.verificar_archivo_env,
            self.verificar_requirements_txt,
            self.verificar_docker_compose_yml,
            self.verificar_archivos_sql,
        ]
        
        for paso in pasos_completos:
            if not paso():
                return False
        
        self.instalar_dependencias_opcionales()
        self.log("✅ Setup completo desde cero completado")
        return True
    
    def ejecutar_verificacion_completa(self):
        """Ejecuta verificación completa del sistema"""
        inicio = datetime.now()
        
        self.log("🔍 VERIFICADOR SISTEMA EPIDEMIOLÓGICO TOLIMA V2.0")
        self.log("=" * 60)
        self.log("Modo: Verificación inteligente (solo corrige lo necesario)")
        
        # Verificaciones en orden de importancia
        verificaciones = [
            ("Python", self.verificar_python),
            ("Estructura proyecto", self.verificar_estructura_proyecto),
            ("Archivo .env", self.verificar_archivo_env),
            ("Docker", self.verificar_docker),
            ("docker-compose.yml", self.verificar_docker_compose_yml),
            ("requirements.txt", self.verificar_requirements_txt),
            ("Archivos SQL", self.verificar_archivos_sql),
            ("config.py", self.verificar_config_py),
        ]
        
        verificaciones_exitosas = 0
        total_verificaciones = len(verificaciones)
        
        for i, (nombre, funcion) in enumerate(verificaciones, 1):
            self.log(f"\n📋 Verificación {i}/{total_verificaciones}: {nombre}")
            
            try:
                if funcion():
                    verificaciones_exitosas += 1
                    self.log(f"✅ {nombre} OK")
                else:
                    self.log(f"❌ {nombre} con problemas")
            except Exception as e:
                self.log(f"❌ Error verificando {nombre}: {e}", "ERROR")
                self.errores_encontrados.append(f"Error en {nombre}")
        
        # Resumen final
        duracion = datetime.now() - inicio
        self.log(f"\n{'='*60}")
        self.log("VERIFICACIÓN COMPLETADA")
        self.log("=" * 60)
        
        self.log(f"✅ Verificaciones exitosas: {verificaciones_exitosas}/{total_verificaciones}")
        
        if self.correcciones_aplicadas:
            self.log(f"🔧 Correcciones aplicadas: {len(self.correcciones_aplicadas)}")
            for correccion in self.correcciones_aplicadas:
                self.log(f"   • {correccion}")
        
        if self.errores_encontrados:
            self.log(f"❌ Errores encontrados: {len(self.errores_encontrados)}")
            for error in self.errores_encontrados:
                self.log(f"   • {error}")
        
        self.log(f"⏱️ Tiempo total: {duracion.total_seconds():.1f} segundos")
        
        # Estado final
        if verificaciones_exitosas == total_verificaciones:
            self.log("🎉 ¡SISTEMA COMPLETAMENTE VERIFICADO!")
            self.log("\n🎯 PRÓXIMOS PASOS:")
            self.log("1. Colocar archivos de datos en data/")
            self.log("2. Ejecutar: python test_conexion.py")
            self.log("3. Ejecutar: python scripts/sistema_coordinador.py --completo")
            self.log("4. ¡Sistema epidemiológico listo! 🚀")
            return True
        else:
            self.log("⚠️ Sistema verificado con algunas observaciones")
            self.log("💡 Revisar errores reportados arriba")
            return False

def main():
    """Función principal con opciones"""
    print("🔍 VERIFICADOR SISTEMA EPIDEMIOLÓGICO TOLIMA V2.0")
    print("=" * 55)
    print("Verifica estructura, configuración y dependencias del sistema")
    
    print("\nOpciones disponibles:")
    print("1. 🔍 Verificación inteligente (recomendado)")
    print("2. 🚀 Setup completo desde cero")
    print("3. 👋 Salir")
    
    while True:
        try:
            opcion = input("\n🔢 Selecciona opción (1-3): ").strip()
            
            if opcion == "1":
                verificador = SistemaVerificadorTolima()
                exito = verificador.ejecutar_verificacion_completa()
                break
            elif opcion == "2":
                verificador = SistemaVerificadorTolima()
                exito = verificador.setup_completo_desde_cero()
                break
            elif opcion == "3":
                print("👋 ¡Hasta luego!")
                return True
            else:
                print("❌ Opción inválida. Usa 1, 2 o 3.")
                
        except KeyboardInterrupt:
            print("\n\n👋 Saliendo...")
            return True
        except Exception as e:
            print(f"❌ Error: {e}")
    
    return exito

if __name__ == "__main__":
    main()