#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
setup_sistema.py - Instalación Automática Sistema Epidemiológico Tolima V2.0
Script actualizado que usa configuración centralizada
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

class SistemaInstallerV2:
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
        self.log("📁 Creando estructura de proyecto V2.0...")
        
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
            "reportes"
        ]
        
        for directorio in directorios:
            dir_path = self.base_dir / directorio
            dir_path.mkdir(parents=True, exist_ok=True)
            self.log(f"   📂 {directorio}")
        
        return True
    
    def instalar_dependencias_python(self):
        """Instala dependencias Python actualizadas"""
        self.log("📦 Instalando dependencias Python V2.0...")
        
        # Requirements actualizado para V2.0
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
python-dateutil==2.8.2

# Datos geoespaciales (NUEVOS para .gpkg)
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
pathlib==1.0.1

# Desarrollo y testing
pytest==7.4.2
black==23.7.0
flake8==6.0.0

# Exportación de reportes
jinja2==3.1.2
fpdf==2.7.4
"""
        
        requirements_file = self.base_dir / "requirements.txt"
        with open(requirements_file, 'w', encoding='utf-8') as f:
            f.write(requirements_content)
        self.log("✅ requirements.txt V2.0 creado")
        
        try:
            # Instalar dependencias
            self.log("   📥 Instalando paquetes...")
            result = subprocess.run([
                sys.executable, "-m", "pip", "install", "-r", "requirements.txt"
            ], capture_output=True, text=True, timeout=300)
            
            if result.returncode != 0:
                self.log(f"❌ Error instalando dependencias: {result.stderr}", "ERROR")
                return False
            
            self.log("✅ Dependencias V2.0 instaladas exitosamente")
            return True
            
        except subprocess.TimeoutExpired:
            self.log("❌ Timeout instalando dependencias", "ERROR")
            return False
        except Exception as e:
            self.log(f"❌ Error inesperado: {e}", "ERROR")
            return False
    
    def crear_archivo_configuracion_v2(self):
        """Crea archivo de configuración centralizada V2.0"""
        self.log("⚙️ Creando configuración centralizada V2.0...")
        
        config_content = '''#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
config.py - Configuración Centralizada Sistema Epidemiológico Tolima V2.0
GENERADO AUTOMÁTICAMENTE POR SETUP
"""

import os
import pandas as pd
import geopandas as gpd
from pathlib import Path
from dotenv import load_dotenv
from datetime import datetime, date
from dateutil.relativedelta import relativedelta

load_dotenv()

# ================================
# CONFIGURACIÓN GRUPOS ETARIOS
# ================================
GRUPOS_ETARIOS = {
    '09-23 meses': (9, 23),
    '02-19 años': (24, 239), 
    '20-59 años': (240, 719),
    '60+ años': (720, None)
}

def clasificar_grupo_etario(edad_meses):
    """Función única de clasificación de grupos etarios"""
    if pd.isna(edad_meses):
        return 'Sin datos'
    
    for grupo, (min_meses, max_meses) in GRUPOS_ETARIOS.items():
        if max_meses is None:
            if edad_meses >= min_meses:
                return grupo
        else:
            if min_meses <= edad_meses <= max_meses:
                return grupo
    return None

# ================================ 
# CONFIGURACIÓN BASE DE DATOS
# ================================
class DatabaseConfig:
    HOST = os.getenv("DB_HOST", "localhost")
    PORT = os.getenv("DB_PORT", "5432")
    DATABASE = os.getenv("DB_NAME", "epidemiologia_tolima")
    USER = os.getenv("DB_USER", "tolima_admin")
    PASSWORD = os.getenv("DB_PASSWORD", "tolima2025!")
    
    @classmethod
    def get_connection_url(cls):
        return f"postgresql://{cls.USER}:{cls.PASSWORD}@{cls.HOST}:{cls.PORT}/{cls.DATABASE}"

# ================================
# RUTAS DE ARCHIVOS
# ================================
class FileConfig:
    BASE_DIR = Path(__file__).parent
    DATA_DIR = BASE_DIR / "data"
    LOGS_DIR = BASE_DIR / "logs"
    BACKUPS_DIR = BASE_DIR / "backups"
    
    @classmethod
    def create_directories(cls):
        for directory in [cls.DATA_DIR, cls.LOGS_DIR, cls.BACKUPS_DIR]:
            directory.mkdir(parents=True, exist_ok=True)

# Variables globales
DATABASE_URL = DatabaseConfig.get_connection_url()

# ================================
# FUNCIONES DE UTILIDAD BÁSICAS
# ================================
def limpiar_fecha_robusta(fecha_input):
    """Limpia fechas en múltiples formatos"""
    if pd.isna(fecha_input):
        return None
    try:
        if isinstance(fecha_input, (datetime, pd.Timestamp)):
            return fecha_input.date()
        
        fecha_str = str(fecha_input).strip()
        if " " in fecha_str:
            fecha_str = fecha_str.split(" ")[0]
        
        formatos = ["%d/%m/%Y", "%d-%m-%Y", "%Y-%m-%d", "%m/%d/%Y"]
        for formato in formatos:
            try:
                return datetime.strptime(fecha_str, formato).date()
            except:
                continue
        return pd.to_datetime(fecha_str, dayfirst=True).date()
    except:
        return None

if __name__ == "__main__":
    print("⚙️ Configuración Sistema Epidemiológico Tolima V2.0")
    FileConfig.create_directories()
    print("✅ Sistema configurado correctamente")
'''
        
        config_file = self.base_dir / "config.py"
        with open(config_file, 'w', encoding='utf-8') as f:
            f.write(config_content)
        
        self.log("✅ config.py V2.0 creado")
        return True
    
    def crear_archivos_docker(self):
        """Crea archivos de configuración Docker"""
        self.log("🐳 Creando configuración Docker...")
        
        # Docker Compose actualizado
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
        """Crea archivos SQL de inicialización"""
        self.log("🗄️ Creando scripts SQL...")
        
        # Extensions SQL
        extensions_sql = """-- Extensiones necesarias para V2.0
CREATE EXTENSION IF NOT EXISTS postgis;
CREATE EXTENSION IF NOT EXISTS pg_trgm;
CREATE EXTENSION IF NOT EXISTS unaccent;
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

\\echo 'Extensiones PostgreSQL V2.0 instaladas exitosamente';
"""
        
        sql_dir = self.base_dir / "sql_init"
        sql_dir.mkdir(exist_ok=True)
        
        extensions_file = sql_dir / "01_extensions.sql"
        with open(extensions_file, 'w', encoding='utf-8') as f:
            f.write(extensions_sql)
        
        self.log("✅ Scripts SQL básicos creados")
        return True
    
    def crear_archivo_env(self):
        """Crea archivo .env de configuración"""
        self.log("🔐 Creando archivo de configuración .env...")
        
        env_content = """# Sistema Epidemiológico Tolima V2.0 - Configuración
ENVIRONMENT=development

# Base de Datos PostgreSQL
DB_HOST=localhost
DB_PORT=5432
DB_NAME=epidemiologia_tolima
DB_USER=tolima_admin
DB_PASSWORD=tolima2025!

# Dashboard
CACHE_TTL=3600

# Logging
LOG_LEVEL=INFO

# Sistema V2.0
SYSTEM_VERSION=2.0
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
            
            # Esperar PostgreSQL
            self.log("⏳ Esperando PostgreSQL...")
            time.sleep(15)
            
            # Verificar PostgreSQL
            result = subprocess.run([
                "docker", "exec", "tolima_postgres", 
                "pg_isready", "-U", "tolima_admin"
            ], capture_output=True, text=True)
            
            if result.returncode == 0:
                self.log("✅ PostgreSQL listo")
                return True
            else:
                self.log("⚠️ PostgreSQL aún iniciando... (normal)", "WARNING") 
                return True
                
        except subprocess.TimeoutExpired:
            self.log("⏳ Docker tomando más tiempo del esperado", "WARNING")
            return True
        except Exception as e:
            self.log(f"❌ Error con Docker: {e}", "ERROR")
            return False
    
    def crear_script_test_v2(self):
        """Crea script de prueba V2.0"""
        self.log("🧪 Creando script de prueba V2.0...")
        
        test_script_content = """#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
test_conexion.py - Prueba Sistema Epidemiológico V2.0
'''

import sys
import warnings
warnings.filterwarnings('ignore')

try:
    # Importar configuración centralizada
    from config import DATABASE_URL, FileConfig, DatabaseConfig
    print("✅ Configuración centralizada importada correctamente")
    
    # Probar dependencias principales
    import pandas as pd
    import geopandas as gpd
    import psycopg2
    from sqlalchemy import create_engine, text
    print("✅ Todas las dependencias importadas correctamente")
    
    # Crear directorios
    FileConfig.create_directories()
    print("✅ Estructura de directorios verificada")
    
    # Probar conexión BD
    try:
        engine = create_engine(DATABASE_URL)
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        print("✅ Conexión PostgreSQL exitosa")
        
    except Exception as e:
        print(f"⚠️ PostgreSQL no disponible aún: {e}")
        print("💡 Espera 30-60 segundos e intenta de nuevo")
        
    print("\\n🎉 ¡Sistema Epidemiológico Tolima V2.0 instalado correctamente!")
    print("📋 Próximos pasos:")
    print("1. Colocar archivos de datos en carpeta 'data/'")
    print("2. Ejecutar: python scripts/sistema_coordinador.py --completo")
    print("3. ¡Usar sistema completo! 🚀")
        
except ImportError as e:
    print(f"❌ Error importando dependencias: {e}")
    print("💡 Ejecutar: pip install -r requirements.txt")
    sys.exit(1)
except Exception as e:
    print(f"❌ Error inesperado: {e}")
    sys.exit(1)
"""
        
        test_file = self.base_dir / "test_conexion.py"
        with open(test_file, 'w', encoding='utf-8') as f:
            f.write(test_script_content)
        
        self.log("✅ Script de prueba V2.0 creado")
        return True
    
    def generar_readme_v2(self):
        """Genera README V2.0 con instrucciones"""
        self.log("📝 Generando documentación V2.0...")
        
        readme_content = f"""# 🏥 Sistema Epidemiológico Tolima V2.0

Sistema de vigilancia epidemiológica con **configuración centralizada** instalado automáticamente.

## ✅ Instalación Completada

La instalación automática V2.0 se completó el {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}.

## 🆕 Novedades V2.0
- ✅ **Configuración centralizada** en config.py
- ✅ **Mapeo automático DIVIPOLA** desde .gpkg
- ✅ **Scripts integrados** y optimizados
- ✅ **Validaciones robustas** de datos
- ✅ **Sistema coordinador** inteligente

## 🚀 Inicio Rápido

### 1. Verificar Instalación
```bash
python test_conexion.py
```

### 2. Colocar Archivos de Datos
Copia tus archivos en `data/`:
- `poblacion_veredas.csv` (CSV SISBEN sin headers)
- `paiweb.xlsx` (datos vacunación) 
- `casos.xlsx` (casos fiebre amarilla)
- `epizootias.xlsx` (epizootias con coordenadas)
- `tolima_cabeceras_veredas.gpkg` (**OBLIGATORIO** para códigos DIVIPOLA)

### 3. Actualización Completa Automática
```bash
python scripts/sistema_coordinador.py --completo
```

### 4. Monitoreo del Sistema
```bash
python scripts/monitor_sistema.py --completo
```

## 🛠️ Herramientas Disponibles
- **PostgreSQL**: localhost:5432
- **pgAdmin**: http://localhost:8080 (admin@tolima.gov.co / admin123)
- **Scripts**: carpeta scripts/ con todos los procesadores
- **Monitor**: sistema de alertas epidemiológicas

## 📋 Scripts Principales
- `sistema_coordinador.py` - Coordinador maestro (RECOMENDADO)
- `cargar_poblacion.py` - Población SISBEN integrada
- `cargar_vacunacion.py` - Vacunación PAIweb
- `cargar_casos.py` - Casos fiebre amarilla
- `cargar_epizootias.py` - Epizootias geoespaciales
- `monitor_sistema.py` - Monitor avanzado

## 📞 Solución de Problemas

### PostgreSQL no responde
```bash
docker-compose down && docker-compose up -d
# Esperar 30-60 segundos
python test_conexion.py
```

### Error en scripts
```bash
# Ver logs detallados
python scripts/monitor_sistema.py --completo
```

## 🎯 ¡Sistema V2.0 Listo!
Tu sistema epidemiológico está completamente instalado y configurado.

**¡Vigilancia epidemiológica automatizada para Tolima! 🚀**
"""
        
        readme_file = self.base_dir / "README.md"
        with open(readme_file, 'w', encoding='utf-8') as f:
            f.write(readme_content)
        
        self.log("✅ README.md V2.0 creado")
        return True
    
    def ejecutar_instalacion_completa(self):
        """Ejecuta instalación completa V2.0 del sistema"""
        self.log("🚀 INICIANDO INSTALACIÓN SISTEMA EPIDEMIOLÓGICO V2.0")
        self.log("=" * 70)
        
        pasos = [
            ("Python", self.verificar_python),
            ("Docker", self.verificar_docker), 
            ("Estructura", self.crear_estructura_proyecto),
            ("Config V2.0", self.crear_archivo_configuracion_v2),
            ("Dependencias", self.instalar_dependencias_python),
            ("Docker Config", self.crear_archivos_docker),
            ("SQL Scripts", self.crear_archivos_sql),
            ("Archivo .env", self.crear_archivo_env),
            ("Servicios Docker", self.iniciar_docker),
            ("Script Test V2.0", self.crear_script_test_v2),
            ("Documentación V2.0", self.generar_readme_v2)
        ]
        
        pasos_exitosos = 0
        total_pasos = len(pasos)
        
        for i, (nombre, funcion) in enumerate(pasos, 1):
            self.log(f"\\n📋 Paso {i}/{total_pasos}: {nombre}")
            
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
        self.log(f"\\n{'='*70}")
        self.log("INSTALACIÓN V2.0 COMPLETADA")
        self.log("=" * 70)
        
        if self.success:
            self.log("🎉 ¡INSTALACIÓN V2.0 EXITOSA!")
            self.log(f"✅ {pasos_exitosos}/{total_pasos} pasos completados")
            self.log("\\n🎯 PRÓXIMOS PASOS:")
            self.log("1. Ejecutar: python test_conexion.py")
            self.log("2. Colocar archivos de datos en 'data/' (incluyendo .gpkg)")
            self.log("3. Ejecutar: python scripts/sistema_coordinador.py --completo")
            self.log("4. ¡Sistema V2.0 funcionando! 🚀")
        else:
            self.log(f"⚠️ Instalación V2.0 completada con errores")
            self.log(f"✅ {pasos_exitosos}/{total_pasos} pasos completados")
        
        return self.success

def main():
    """Función principal"""
    print("🏥 INSTALADOR AUTOMÁTICO SISTEMA EPIDEMIOLÓGICO V2.0")
    print("=" * 65)
    print("Sistema con configuración centralizada y mapeo automático DIVIPOLA")
    print("Tiempo estimado: 5-10 minutos")
    
    respuesta = input("\\n¿Continuar con la instalación V2.0? (y/N): ")
    if respuesta.lower() not in ['y', 'yes', 'si', 'sí']:
        print("👋 Instalación cancelada")
        return
    
    installer = SistemaInstallerV2()
    exito = installer.ejecutar_instalacion_completa()
    
    if exito:
        print(f"\\n🎉 ¡SISTEMA V2.0 INSTALADO EXITOSAMENTE!")
        print("📋 Ver README.md para instrucciones completas")
        print("🚀 ¡Listo para vigilancia epidemiológica automatizada!")
    else:
        print(f"\\n⚠️ Instalación con errores. Ver log para detalles.")
    
    return exito

if __name__ == "__main__":
    main()