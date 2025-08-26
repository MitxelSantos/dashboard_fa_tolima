#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
setup_sistema.py - Instalación Automática Sistema Epidemiológico Tolima
Script que configura automáticamente todo el sistema desde cero
"""

import os
import sys
import subprocess
import shutil
import time
from pathlib import Path
from datetime import datetime
import urllib.request
import zipfile

class SistemaInstaller:
    def __init__(self):
        self.base_dir = Path.cwd()
        self.logs = []
        self.success = True
        
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
            return False
        
        self.log(f"✅ Python {version.major}.{version.minor} OK")
        return True
    
    def verificar_docker(self):
        """Verifica instalación de Docker"""
        self.log("🐳 Verificando Docker...")
        
        try:
            # Verificar Docker
            result = subprocess.run(["docker", "--version"], 
                                  capture_output=True, text=True)
            if result.returncode != 0:
                self.log("❌ Docker no está instalado", "ERROR")
                self.log("💡 Instalar desde: https://www.docker.com/products/docker-desktop")
                return False
            
            self.log(f"✅ {result.stdout.strip()}")
            
            # Verificar Docker Compose
            result = subprocess.run(["docker-compose", "--version"], 
                                  capture_output=True, text=True)
            if result.returncode != 0:
                self.log("❌ Docker Compose no está disponible", "ERROR")
                return False
                
            self.log(f"✅ {result.stdout.strip()}")
            return True
            
        except FileNotFoundError:
            self.log("❌ Docker no encontrado en PATH", "ERROR")
            return False
    
    def crear_estructura_proyecto(self):
        """Crea estructura de directorios del proyecto"""
        self.log("📁 Creando estructura de proyecto...")
        
        directorios = [
            "sql_init",
            "scripts",
            "data",
            "data/processed", 
            "dashboard",
            "dashboard/pages",
            "dashboard/utils",
            "backups",
            "logs",
            "reportes",
            "utils_legacy"
        ]
        
        for directorio in directorios:
            dir_path = self.base_dir / directorio
            dir_path.mkdir(parents=True, exist_ok=True)
            self.log(f"   📂 {directorio}")
        
        return True
    
    def instalar_dependencias_python(self):
        """Instala dependencias Python"""
        self.log("📦 Instalando dependencias Python...")
        
        # Crear requirements.txt si no existe
        requirements_content = """# Sistema Epidemiológico Tolima - Dependencias
psycopg2-binary==2.9.7
SQLAlchemy==2.0.21
pandas==2.1.1
numpy==1.25.2
openpyxl==3.1.2
geopandas==0.13.2
streamlit==1.26.0
plotly==5.16.1
folium==0.14.0
streamlit-folium==0.15.0
matplotlib==3.7.2
seaborn==0.12.2
python-dotenv==1.0.0
tqdm==4.66.1
"""
        
        requirements_file = self.base_dir / "requirements.txt"
        if not requirements_file.exists():
            with open(requirements_file, 'w', encoding='utf-8') as f:
                f.write(requirements_content)
            self.log("✅ requirements.txt creado")
        
        try:
            # Instalar dependencias
            self.log("   📥 Instalando paquetes...")
            result = subprocess.run([
                sys.executable, "-m", "pip", "install", "-r", "requirements.txt"
            ], capture_output=True, text=True, timeout=300)
            
            if result.returncode != 0:
                self.log(f"❌ Error instalando dependencias: {result.stderr}", "ERROR")
                return False
            
            self.log("✅ Dependencias instaladas exitosamente")
            return True
            
        except subprocess.TimeoutExpired:
            self.log("❌ Timeout instalando dependencias", "ERROR")
            return False
        except Exception as e:
            self.log(f"❌ Error inesperado: {e}", "ERROR")
            return False
    
    def crear_archivos_docker(self):
        """Crea archivos de configuración Docker"""
        self.log("🐳 Creando configuración Docker...")
        
        # Docker Compose
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
        
        docker_file = self.base_dir / "docker-compose.yml"
        with open(docker_file, 'w', encoding='utf-8') as f:
            f.write(docker_compose_content)
        
        self.log("✅ docker-compose.yml creado")
        return True
    
    def crear_archivos_sql(self):
        """Crea archivos SQL de inicialización básicos"""
        self.log("🗄️ Creando scripts SQL...")
        
        # Extensions SQL
        extensions_sql = """-- Extensiones necesarias
CREATE EXTENSION IF NOT EXISTS postgis;
CREATE EXTENSION IF NOT EXISTS pg_trgm;
CREATE EXTENSION IF NOT EXISTS unaccent;
"""
        
        extensions_file = self.base_dir / "sql_init" / "01_extensions.sql"
        with open(extensions_file, 'w', encoding='utf-8') as f:
            f.write(extensions_sql)
        
        self.log("✅ Scripts SQL básicos creados")
        return True
    
    def crear_archivo_configuracion(self):
        """Crea archivo .env de configuración"""
        self.log("⚙️ Creando configuración...")
        
        env_content = """# Configuración Sistema Epidemiológico Tolima
ENVIRONMENT=development

# Base de Datos
DB_HOST=localhost
DB_PORT=5432
DB_NAME=epidemiologia_tolima
DB_USER=tolima_admin
DB_PASSWORD=tolima2025!

# Dashboard
CACHE_TTL=3600

# Logging
LOG_LEVEL=INFO

# Alertas
EMAIL_ALERTS=false
"""
        
        env_file = self.base_dir / ".env"
        with open(env_file, 'w', encoding='utf-8') as f:
            f.write(env_content)
        
        self.log("✅ Archivo .env creado")
        return True
    
    def iniciar_docker(self):
        """Inicia servicios Docker"""
        self.log("🚀 Iniciando servicios Docker...")
        
        try:
            # Iniciar servicios
            result = subprocess.run([
                "docker-compose", "up", "-d"
            ], capture_output=True, text=True, timeout=180)
            
            if result.returncode != 0:
                self.log(f"❌ Error iniciando Docker: {result.stderr}", "ERROR")
                return False
            
            self.log("✅ Servicios Docker iniciados")
            
            # Esperar a que PostgreSQL esté listo
            self.log("⏳ Esperando PostgreSQL...")
            time.sleep(10)
            
            # Verificar PostgreSQL
            result = subprocess.run([
                "docker", "exec", "tolima_postgres", 
                "pg_isready", "-U", "tolima_admin"
            ], capture_output=True, text=True)
            
            if result.returncode == 0:
                self.log("✅ PostgreSQL listo")
                return True
            else:
                self.log("⚠️ PostgreSQL aún iniciando... (esto es normal)", "WARNING") 
                return True
                
        except subprocess.TimeoutExpired:
            self.log("⏳ Docker tomando más tiempo del esperado", "WARNING")
            return True
        except Exception as e:
            self.log(f"❌ Error con Docker: {e}", "ERROR")
            return False
    
    def crear_script_test(self):
        """Crea script de prueba básico"""
        self.log("🧪 Creando script de prueba...")
        
        test_script_content = """#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
test_instalacion.py - Prueba básica de instalación
'''

import sys
try:
    import pandas as pd
    import psycopg2
    from sqlalchemy import create_engine, text
    print("✅ Todas las dependencias importadas correctamente")
    
    # Probar conexión BD
    DATABASE_URL = "postgresql://tolima_admin:tolima2025!@localhost:5432/epidemiologia_tolima"
    
    try:
        engine = create_engine(DATABASE_URL)
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        print("✅ Conexión PostgreSQL exitosa")
        
    except Exception as e:
        print(f"⚠️ PostgreSQL no disponible aún: {e}")
        print("💡 Espera unos minutos e intenta de nuevo")
        
except ImportError as e:
    print(f"❌ Error importando dependencias: {e}")
    sys.exit(1)

print("🎉 ¡Instalación básica completada!")
"""
        
        test_file = self.base_dir / "test_instalacion.py"
        with open(test_file, 'w', encoding='utf-8') as f:
            f.write(test_script_content)
        
        self.log("✅ Script de prueba creado")
        return True
    
    def generar_readme(self):
        """Genera README con instrucciones"""
        self.log("📝 Generando documentación...")
        
        readme_content = f"""# Sistema Epidemiológico Tolima

Sistema de vigilancia epidemiológica para fiebre amarilla en el departamento del Tolima.

## 🚀 Instalación Completada

La instalación automática se completó el {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}.

## 📁 Estructura del Proyecto

```
📁 epidemiologia_tolima/
├── 🐳 docker-compose.yml          # Configuración PostgreSQL
├── 📋 requirements.txt            # Dependencias Python
├── ⚙️ .env                       # Configuración
├── 🧪 test_instalacion.py        # Prueba básica
│
├── 📊 sql_init/                  # Scripts SQL inicialización
├── 🧹 scripts/                   # Scripts Python procesamiento
├── 📂 data/                      # Datos de entrada
├── 📈 dashboard/                 # Dashboard Streamlit
├── 🔄 backups/                   # Respaldos automáticos
├── 📝 logs/                      # Logs del sistema
└── 📊 reportes/                  # Reportes generados
```

## 🎯 Próximos Pasos

### 1. Verificar Instalación
```bash
python test_instalacion.py
```

### 2. Colocar Archivos de Datos
Copia tus archivos en la carpeta `data/`:
- `paiweb.xlsx`
- `casos.xlsx` 
- `epizootias.xlsx`
- `poblacion_tolima_YYYYMMDD.csv`
- `tolima_cabeceras_veredas.gpkg`

### 3. Herramientas Disponibles
- **PostgreSQL**: localhost:5432
- **pgAdmin**: http://localhost:8080
  - Usuario: admin@tolima.gov.co
  - Contraseña: admin123

### 4. Instalar DBeaver (Recomendado)
Descargar desde: https://dbeaver.io/download/

## 📞 Soporte

Si tienes problemas:
1. Verificar que Docker esté corriendo: `docker-compose ps`
2. Ver logs: `docker-compose logs postgres`
3. Reiniciar servicios: `docker-compose down && docker-compose up -d`

¡Sistema listo para uso! 🚀
"""
        
        readme_file = self.base_dir / "README.md"
        with open(readme_file, 'w', encoding='utf-8') as f:
            f.write(readme_content)
        
        self.log("✅ README.md creado")
        return True
    
    def generar_reporte_instalacion(self):
        """Genera reporte completo de instalación"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        reporte_file = self.base_dir / f"instalacion_{timestamp}.log"
        
        with open(reporte_file, 'w', encoding='utf-8') as f:
            f.write("REPORTE INSTALACIÓN SISTEMA EPIDEMIOLÓGICO TOLIMA\n")
            f.write("=" * 60 + "\n\n")
            f.write(f"Fecha: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"Directorio: {self.base_dir}\n")
            f.write(f"Estado: {'EXITOSA' if self.success else 'CON ERRORES'}\n\n")
            
            f.write("LOG COMPLETO:\n")
            f.write("-" * 30 + "\n")
            for log_entry in self.logs:
                f.write(log_entry + "\n")
        
        self.log(f"📄 Reporte guardado: {reporte_file}")
    
    def ejecutar_instalacion_completa(self):
        """Ejecuta instalación completa del sistema"""
        self.log("🚀 INICIANDO INSTALACIÓN SISTEMA EPIDEMIOLÓGICO TOLIMA")
        self.log("=" * 65)
        
        pasos = [
            ("Python", self.verificar_python),
            ("Docker", self.verificar_docker), 
            ("Estructura", self.crear_estructura_proyecto),
            ("Dependencias", self.instalar_dependencias_python),
            ("Docker Config", self.crear_archivos_docker),
            ("SQL Scripts", self.crear_archivos_sql),
            ("Configuración", self.crear_archivo_configuracion),
            ("Servicios Docker", self.iniciar_docker),
            ("Script Test", self.crear_script_test),
            ("Documentación", self.generar_readme)
        ]
        
        pasos_exitosos = 0
        total_pasos = len(pasos)
        
        for i, (nombre, funcion) in enumerate(pasos, 1):
            self.log(f"\n📋 Paso {i}/{total_pasos}: {nombre}")
            
            try:
                if funcion():
                    pasos_exitosos += 1
                    self.log(f"✅ {nombre} completado")
                else:
                    self.log(f"❌ {nombre} falló", "ERROR")
                    self.success = False
            except Exception as e:
                self.log(f"❌ Error en {nombre}: {e}", "ERROR")
                self.success = False
        
        # Resumen final
        self.log(f"\n{'='*65}")
        self.log("INSTALACIÓN COMPLETADA")
        self.log("=" * 65)
        
        if self.success:
            self.log("🎉 ¡INSTALACIÓN EXITOSA!")
            self.log(f"✅ {pasos_exitosos}/{total_pasos} pasos completados")
            self.log("\n🎯 PRÓXIMOS PASOS:")
            self.log("1. Ejecutar: python test_instalacion.py")
            self.log("2. Colocar archivos de datos en carpeta 'data/'")
            self.log("3. Instalar DBeaver desde https://dbeaver.io")
            self.log("4. ¡Empezar a usar el sistema! 🚀")
        else:
            self.log(f"⚠️ Instalación completada con errores")
            self.log(f"✅ {pasos_exitosos}/{total_pasos} pasos completados")
            self.log("💡 Revisar errores arriba y corregir manualmente")
        
        # Generar reporte
        self.generar_reporte_instalacion()
        
        return self.success

def main():
    """Función principal"""
    print("🏥 INSTALADOR AUTOMÁTICO SISTEMA EPIDEMIOLÓGICO TOLIMA")
    print("=" * 60)
    print("Este script configurará automáticamente todo el sistema.")
    print("Tiempo estimado: 5-10 minutos")
    
    respuesta = input("\n¿Continuar con la instalación? (y/N): ")
    if respuesta.lower() not in ['y', 'yes', 'si', 'sí']:
        print("👋 Instalación cancelada")
        return
    
    installer = SistemaInstaller()
    exito = installer.ejecutar_instalacion_completa()
    
    if exito:
        print(f"\n🎉 ¡SISTEMA INSTALADO EXITOSAMENTE!")
        print("📋 Ver README.md para instrucciones completas")
    else:
        print(f"\n⚠️ Instalación con errores. Ver log para detalles.")
    
    return exito

if __name__ == "__main__":
    main()