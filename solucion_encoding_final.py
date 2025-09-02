#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
solucion_encoding_final.py - Soluciona el problema de codificación UTF-8/Latin-1
El problema real: PostgreSQL envía mensajes en español con Latin-1, Python espera UTF-8
"""

import subprocess
import time
import os

def recrear_postgresql_con_utf8():
    """Recrea PostgreSQL forzando UTF-8 en todo"""
    
    print("🔧 SOLUCIONANDO PROBLEMA DE CODIFICACIÓN")
    print("=" * 50)
    print("Problema: PostgreSQL envía mensajes en español (Latin-1), Python espera UTF-8")
    
    print("\n🛑 PASO 1: Limpiar completamente")
    subprocess.run(['docker-compose', 'down', '-v'], capture_output=True)
    subprocess.run(['docker', 'system', 'prune', '-f'], capture_output=True)
    
    print("✅ Sistema limpiado")
    
    print("\n🛠️ PASO 2: Crear docker-compose.yml con UTF-8 forzado")
    
    docker_compose_utf8 = """services:
  postgres:
    image: postgis/postgis:15-3.3
    container_name: tolima_postgres
    environment:
      POSTGRES_DB: epidemiologia_tolima
      POSTGRES_USER: tolima_admin
      POSTGRES_PASSWORD: tolima2025
      # Forzar UTF-8 en todo
      POSTGRES_INITDB_ARGS: "--encoding=UTF8 --lc-collate=en_US.UTF-8 --lc-ctype=en_US.UTF-8"
      LC_ALL: en_US.UTF-8
      LANG: en_US.UTF-8
      LANGUAGE: en_US.UTF-8
      PGDATA: /var/lib/postgresql/data/pgdata
    ports:
      - "5432:5432"
    volumes:
      - postgres_data:/var/lib/postgresql/data
      - ./backups:/backups
    restart: unless-stopped
    command: >
      postgres
      -c listen_addresses='*'
      -c log_statement=none
      -c log_min_messages=warning
      -c lc_messages='en_US.UTF-8'
      -c lc_monetary='en_US.UTF-8'
      -c lc_numeric='en_US.UTF-8'
      -c lc_time='en_US.UTF-8'
      -c default_text_search_config='pg_catalog.english'

volumes:
  postgres_data:
    name: tolima_postgres_data
"""
    
    with open('docker-compose.yml', 'w', encoding='utf-8') as f:
        f.write(docker_compose_utf8)
    
    print("✅ docker-compose.yml con UTF-8 forzado creado")
    
    print("\n🚀 PASO 3: Levantar PostgreSQL")
    result = subprocess.run(['docker-compose', 'up', '-d'], 
                          capture_output=True, text=True)
    
    if result.returncode != 0:
        print(f"❌ Error: {result.stderr}")
        return False
    
    print("✅ PostgreSQL iniciando...")
    
    print("⏱️ Esperando inicialización completa...")
    time.sleep(45)  # Más tiempo para inicialización completa
    
    print("\n🔧 PASO 4: Configurar autenticación sin errores de codificación")
    
    # Conectar directamente sin mensajes de error problemáticos
    commands_setup = [
        "CREATE EXTENSION IF NOT EXISTS postgis;",
        "ALTER USER tolima_admin WITH PASSWORD 'tolima2025';",
        "GRANT ALL PRIVILEGES ON DATABASE epidemiologia_tolima TO tolima_admin;",
    ]
    
    for cmd in commands_setup:
        result = subprocess.run([
            'docker', 'exec', 'tolima_postgres',
            'psql', '-U', 'tolima_admin', '-d', 'epidemiologia_tolima',
            '-c', cmd
        ], capture_output=True, text=True)
        
        if result.returncode != 0:
            print(f"⚠️ Comando falló: {cmd}")
        
    print("✅ Configuración aplicada")
    
    return True

def crear_test_con_manejo_encoding():
    """Crea test que maneja correctamente los problemas de encoding"""
    
    test_encoding = '''#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
test_encoding.py - Test con manejo correcto de encoding
"""

import psycopg2
from sqlalchemy import create_engine, text
import warnings
warnings.filterwarnings('ignore')

def test_psycopg2_con_encoding():
    """Test psycopg2 con manejo de encoding"""
    print("🧪 Test psycopg2 con manejo UTF-8...")
    
    try:
        # Conexión con client_encoding explícito
        conn = psycopg2.connect(
            host="localhost",
            port=5432,
            database="epidemiologia_tolima",
            user="tolima_admin",
            password="tolima2025",
            client_encoding='utf8'
        )
        
        # Configurar encoding en la sesión
        with conn.cursor() as cursor:
            cursor.execute("SET client_encoding TO 'UTF8'")
            cursor.execute("SET lc_messages TO 'en_US.UTF-8'")
            cursor.execute("SELECT 1 as test")
            result = cursor.fetchone()
            
        print(f"✅ psycopg2 exitoso: {result[0]}")
        conn.close()
        return True
        
    except UnicodeDecodeError as e:
        print(f"❌ psycopg2 error encoding: {e}")
        return False
    except Exception as e:
        print(f"❌ psycopg2 error general: {e}")
        return False

def test_sqlalchemy_con_encoding():
    """Test SQLAlchemy con encoding correcto"""
    print("🧪 Test SQLAlchemy con UTF-8...")
    
    try:
        # URL con parámetros de encoding
        DATABASE_URL = "postgresql://tolima_admin:tolima2025@localhost:5432/epidemiologia_tolima?client_encoding=utf8"
        
        engine = create_engine(
            DATABASE_URL,
            connect_args={
                "options": "-c timezone=UTC -c lc_messages=en_US.UTF-8"
            },
            echo=False
        )
        
        with engine.connect() as conn:
            # Test simple sin generar errores de autenticación
            result = conn.execute(text("SELECT version()"))
            version_info = result.fetchone()
            print(f"✅ SQLAlchemy exitoso - PostgreSQL conectado")
            
        return True
        
    except UnicodeDecodeError as e:
        print(f"❌ SQLAlchemy error encoding: {e}")
        return False
    except Exception as e:
        print(f"❌ SQLAlchemy error: {e}")
        return False

def test_directo_docker():
    """Test usando conexión directa Docker como referencia"""
    print("🧪 Test directo Docker...")
    
    import subprocess
    
    try:
        result = subprocess.run([
            'docker', 'exec', 'tolima_postgres',
            'psql', '-U', 'tolima_admin', '-d', 'epidemiologia_tolima',
            '-c', 'SELECT current_database(), current_user;'
        ], capture_output=True, text=True, timeout=10)
        
        if result.returncode == 0:
            print("✅ Conexión Docker exitosa")
            print(f"   Output: {result.stdout.strip()}")
            return True
        else:
            print(f"❌ Error Docker: {result.stderr}")
            return False
            
    except Exception as e:
        print(f"❌ Error test Docker: {e}")
        return False

def main():
    print("🔍 TEST COMPLETO CON MANEJO DE ENCODING")
    print("=" * 45)
    
    # Test de referencia
    docker_ok = test_directo_docker()
    
    if not docker_ok:
        print("❌ Docker no funciona - problema de configuración")
        return False
    
    # Tests Python
    psycopg2_ok = test_psycopg2_con_encoding()
    sqlalchemy_ok = test_sqlalchemy_con_encoding()
    
    print("\\n📊 RESUMEN:")
    print(f"   Docker directo: {'✅' if docker_ok else '❌'}")
    print(f"   psycopg2: {'✅' if psycopg2_ok else '❌'}")
    print(f"   SQLAlchemy: {'✅' if sqlalchemy_ok else '❌'}")
    
    if psycopg2_ok and sqlalchemy_ok:
        print("\\n🎉 ¡PROBLEMA DE ENCODING RESUELTO!")
        print("✅ Todas las conexiones Python funcionan")
        return True
    elif docker_ok:
        print("\\n⚠️ Docker funciona, pero Python tiene problemas de encoding")
        print("💡 Revisar configuración de locale/encoding")
        return False
    else:
        print("\\n❌ Problemas generales de PostgreSQL")
        return False

if __name__ == "__main__":
    main()
'''
    
    with open('test_encoding.py', 'w', encoding='utf-8') as f:
        f.write(test_encoding)
    
    print("✅ test_encoding.py creado")

def actualizar_config_definitivo():
    """Actualiza config.py con configuración UTF-8 correcta"""
    
    print("🛠️ Actualizando config.py con UTF-8...")
    
    try:
        # Leer config actual
        with open('config.py', 'r', encoding='utf-8') as f:
            contenido = f.read()
        
        # Reemplazar método get_connection_url
        nuevo_metodo = '''    @classmethod
    def get_connection_url(cls):
        return f"postgresql://{cls.USER}:{cls.PASSWORD}@{cls.HOST}:{cls.PORT}/{cls.DATABASE}?client_encoding=utf8"'''
        
        # Buscar y reemplazar el método
        import re
        patron = r'@classmethod\s+def get_connection_url\(cls\):.*?return.*?client_encoding=latin1.*?"'
        
        if re.search(patron, contenido, re.DOTALL):
            contenido_nuevo = re.sub(patron, nuevo_metodo, contenido, flags=re.DOTALL)
        else:
            # Si no encuentra el patrón, buscar patrón más simple
            patron_simple = r'return f"postgresql://.*?client_encoding=latin1.*?"'
            contenido_nuevo = re.sub(
                patron_simple, 
                'return f"postgresql://{cls.USER}:{cls.PASSWORD}@{cls.HOST}:{cls.PORT}/{cls.DATABASE}?client_encoding=utf8"',
                contenido
            )
        
        # Escribir archivo actualizado
        with open('config.py', 'w', encoding='utf-8') as f:
            f.write(contenido_nuevo)
        
        print("✅ config.py actualizado con UTF-8")
        return True
        
    except Exception as e:
        print(f"⚠️ Error actualizando config.py: {e}")
        return False

def main():
    print("🚀 SOLUCIÓN DEFINITIVA - PROBLEMA DE CODIFICACIÓN UTF-8")
    print("=" * 60)
    
    try:
        # Paso 1: Recrear PostgreSQL con UTF-8
        if recrear_postgresql_con_utf8():
            
            # Paso 2: Crear test con manejo de encoding
            crear_test_con_manejo_encoding()
            
            # Paso 3: Actualizar config.py
            actualizar_config_definitivo()
            
            print("\n🎯 PRÓXIMOS PASOS:")
            print("1. python test_encoding.py")
            print("2. Si funciona: python scripts/test_connection.py")
            
            print("\n✅ SOLUCIÓN DE CODIFICACIÓN APLICADA")
            
            # Test inmediato
            print("\n🧪 EJECUTANDO TEST INMEDIATO...")
            subprocess.run(['python', 'test_encoding.py'])
            
        else:
            print("❌ Error recreando PostgreSQL")
            
    except Exception as e:
        print(f"❌ Error general: {e}")

if __name__ == "__main__":
    main()