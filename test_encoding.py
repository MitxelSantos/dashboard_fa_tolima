#!/usr/bin/env python
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
    
    print("\n📊 RESUMEN:")
    print(f"   Docker directo: {'✅' if docker_ok else '❌'}")
    print(f"   psycopg2: {'✅' if psycopg2_ok else '❌'}")
    print(f"   SQLAlchemy: {'✅' if sqlalchemy_ok else '❌'}")
    
    if psycopg2_ok and sqlalchemy_ok:
        print("\n🎉 ¡PROBLEMA DE ENCODING RESUELTO!")
        print("✅ Todas las conexiones Python funcionan")
        return True
    elif docker_ok:
        print("\n⚠️ Docker funciona, pero Python tiene problemas de encoding")
        print("💡 Revisar configuración de locale/encoding")
        return False
    else:
        print("\n❌ Problemas generales de PostgreSQL")
        return False

if __name__ == "__main__":
    main()
