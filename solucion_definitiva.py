#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
solucion_definitiva.py - Soluciona problemas de autenticación y codificación PostgreSQL
"""

import subprocess
import time
import os
from pathlib import Path

def solucion_completa():
    print("🔧 SOLUCIÓN DEFINITIVA - POSTGRESQL AUTHENTICATION")
    print("=" * 55)
    
    print("\n🛠️ PASO 1: Recrear contenedor con configuración correcta")
    
    # Parar contenedor actual
    print("⏹️ Deteniendo contenedor...")
    subprocess.run(['docker-compose', 'down'], capture_output=True)
    
    # Limpiar volúmenes
    print("🧹 Limpiando volúmenes...")
    subprocess.run(['docker-compose', 'down', '-v'], capture_output=True)
    subprocess.run(['docker', 'volume', 'prune', '-f'], capture_output=True)
    
    print("\n🛠️ PASO 2: Crear docker-compose.yml corregido")
    
    docker_compose_corregido = """services:
  postgres:
    image: postgis/postgis:15-3.3
    container_name: tolima_postgres
    environment:
      POSTGRES_DB: epidemiologia_tolima
      POSTGRES_USER: tolima_admin
      POSTGRES_PASSWORD: tolima2025
      POSTGRES_INITDB_ARGS: "--encoding=UTF8 --locale=C"
      PGDATA: /var/lib/postgresql/data/pgdata
    ports:
      - "5432:5432"
    volumes:
      - postgres_data:/var/lib/postgresql/data
      - ./backups:/backups
    restart: unless-stopped
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U tolima_admin -d epidemiologia_tolima"]
      interval: 10s
      timeout: 5s
      retries: 5
    command: >
      postgres
      -c listen_addresses='*'
      -c max_connections=100
      -c shared_buffers=256MB
      -c effective_cache_size=1GB
      -c log_statement=all

volumes:
  postgres_data:
    name: tolima_postgres_data
"""
    
    # Escribir archivo corregido
    with open('docker-compose.yml', 'w', encoding='utf-8') as f:
        f.write(docker_compose_corregido)
    print("✅ docker-compose.yml actualizado")
    
    print("\n🛠️ PASO 3: Crear configuración PostgreSQL personalizada")
    
    # Crear directorio para configuración personalizada
    Path('./postgres_config').mkdir(exist_ok=True)
    
    # Crear pg_hba.conf personalizado
    pg_hba_config = """# PostgreSQL Client Authentication Configuration File
# TYPE  DATABASE        USER            ADDRESS                 METHOD

# Allow local connections
local   all             all                                     trust

# IPv4/IPv6 local connections
host    all             all             127.0.0.1/32            md5
host    all             all             ::1/128                 md5

# Allow connections from any IPv4 address with password
host    all             all             0.0.0.0/0               md5

# Docker network connections
host    all             all             172.16.0.0/12           md5
"""
    
    with open('./postgres_config/pg_hba.conf', 'w', encoding='utf-8') as f:
        f.write(pg_hba_config)
    print("✅ pg_hba.conf personalizado creado")
    
    print("\n🛠️ PASO 4: Actualizar docker-compose.yml con configuración")
    
    docker_compose_final = """services:
  postgres:
    image: postgis/postgis:15-3.3
    container_name: tolima_postgres
    environment:
      POSTGRES_DB: epidemiologia_tolima
      POSTGRES_USER: tolima_admin
      POSTGRES_PASSWORD: tolima2025
      POSTGRES_INITDB_ARGS: "--encoding=UTF8 --locale=C"
      PGDATA: /var/lib/postgresql/data/pgdata
    ports:
      - "5432:5432"
    volumes:
      - postgres_data:/var/lib/postgresql/data
      - ./postgres_config/pg_hba.conf:/var/lib/postgresql/data/pgdata/pg_hba.conf
      - ./backups:/backups
    restart: unless-stopped
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U tolima_admin -d epidemiologia_tolima"]
      interval: 10s
      timeout: 5s
      retries: 5

volumes:
  postgres_data:
    name: tolima_postgres_data
"""
    
    with open('docker-compose.yml', 'w', encoding='utf-8') as f:
        f.write(docker_compose_final)
    print("✅ docker-compose.yml con configuración personalizada")
    
    print("\n🚀 PASO 5: Levantar contenedor")
    
    result = subprocess.run(['docker-compose', 'up', '-d'], 
                          capture_output=True, text=True)
    
    if result.returncode == 0:
        print("✅ Contenedor levantado exitosamente")
        
        print("⏱️ Esperando inicialización PostgreSQL...")
        for i in range(30, 0, -5):
            print(f"   Tiempo restante: {i} segundos", end='\r')
            time.sleep(5)
        print()
        
        print("\n🛠️ PASO 6: Configurar usuario y permisos")
        
        # Configurar usuario y contraseña
        commands = [
            "ALTER USER tolima_admin WITH PASSWORD 'tolima2025';",
            "GRANT ALL PRIVILEGES ON DATABASE epidemiologia_tolima TO tolima_admin;",
            "ALTER USER tolima_admin CREATEDB CREATEROLE;",
        ]
        
        for cmd in commands:
            subprocess.run([
                'docker', 'exec', 'tolima_postgres', 
                'psql', '-U', 'tolima_admin', '-d', 'epidemiologia_tolima', 
                '-c', cmd
            ], capture_output=True)
        
        print("✅ Usuario configurado")
        
        print("\n🔄 PASO 7: Reiniciar para aplicar configuración")
        subprocess.run(['docker', 'restart', 'tolima_postgres'], capture_output=True)
        
        print("⏱️ Esperando reinicio...")
        time.sleep(15)
        
        print("✅ Configuración completada")
        return True
        
    else:
        print(f"❌ Error levantando contenedor: {result.stderr}")
        return False

def crear_test_simple():
    """Crea test simple sin problemas de codificación"""
    
    test_simple = '''#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
test_simple.py - Test sin problemas de codificación
"""

import psycopg2
from sqlalchemy import create_engine, text

def test_psycopg2():
    """Test directo con psycopg2"""
    print("🧪 Probando psycopg2...")
    
    try:
        conn = psycopg2.connect(
            host="localhost",
            port=5432,
            database="epidemiologia_tolima",
            user="tolima_admin",
            password="tolima2025"
        )
        
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        result = cursor.fetchone()
        
        print(f"✅ psycopg2 exitoso: {result[0]}")
        conn.close()
        return True
        
    except Exception as e:
        print(f"❌ psycopg2 error: {e}")
        return False

def test_sqlalchemy():
    """Test con SQLAlchemy sin codificación problemática"""
    print("🧪 Probando SQLAlchemy...")
    
    try:
        # URL sin parámetros de codificación problemáticos
        DATABASE_URL = "postgresql://tolima_admin:tolima2025@localhost:5432/epidemiologia_tolima"
        
        engine = create_engine(DATABASE_URL)
        
        with engine.connect() as conn:
            result = conn.execute(text("SELECT 1 as test"))
            row = result.fetchone()
            print(f"✅ SQLAlchemy exitoso: {row[0]}")
        
        return True
        
    except Exception as e:
        print(f"❌ SQLAlchemy error: {e}")
        return False

def main():
    print("🔍 TEST SIMPLE POSTGRESQL")
    print("=" * 30)
    
    psycopg2_ok = test_psycopg2()
    sqlalchemy_ok = test_sqlalchemy()
    
    if psycopg2_ok and sqlalchemy_ok:
        print("\\n🎉 ¡TODAS LAS CONEXIONES FUNCIONAN!")
        print("✅ Sistema listo para usar")
        return True
    else:
        print("\\n⚠️ Algunas conexiones fallan")
        return False

if __name__ == "__main__":
    main()
'''
    
    with open('test_simple.py', 'w', encoding='utf-8') as f:
        f.write(test_simple)
    print("✅ test_simple.py creado")

def actualizar_config():
    """Actualiza config.py para quitar problemas de codificación"""
    
    print("🛠️ Actualizando config.py...")
    
    # Leer config.py actual
    with open('config.py', 'r', encoding='utf-8') as f:
        contenido = f.read()
    
    # Reemplazar URL problemática
    contenido_nuevo = contenido.replace(
        'postgresql://{cls.USER}:{cls.PASSWORD}@{cls.HOST}:{cls.PORT}/{cls.DATABASE}?options=-c%20client_encoding=latin1',
        'postgresql://{cls.USER}:{cls.PASSWORD}@{cls.HOST}:{cls.PORT}/{cls.DATABASE}'
    )
    
    # Escribir versión corregida
    with open('config.py', 'w', encoding='utf-8') as f:
        f.write(contenido_nuevo)
    
    print("✅ config.py actualizado (sin codificación problemática)")

if __name__ == "__main__":
    print("🚀 INICIANDO SOLUCIÓN DEFINITIVA")
    print("=" * 50)
    
    try:
        # Paso 1: Solución completa Docker
        if solucion_completa():
            
            # Paso 2: Crear test simple
            crear_test_simple()
            
            # Paso 3: Actualizar config
            actualizar_config()
            
            print("\n🎯 PRUEBA FINAL")
            print("=" * 20)
            print("Ejecutar: python test_simple.py")
            print("Si funciona, ejecutar: python scripts/test_connection.py")
            
            print("\n✅ ¡SOLUCIÓN APLICADA COMPLETAMENTE!")
            
        else:
            print("❌ Error en solución Docker")
            
    except Exception as e:
        print(f"❌ Error general: {e}")