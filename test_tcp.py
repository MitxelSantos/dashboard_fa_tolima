#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
test_tcp.py - Test específico para conexiones TCP PostgreSQL
"""

import psycopg2
from sqlalchemy import create_engine, text
import socket

def test_puerto_accesible():
    """Verifica que el puerto 5432 es accesible"""
    print("🔌 Verificando acceso a puerto 5432...")
    
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(5)
        result = sock.connect_ex(('localhost', 5432))
        sock.close()
        
        if result == 0:
            print("   ✅ Puerto 5432 accesible")
            return True
        else:
            print("   ❌ Puerto 5432 no accesible")
            return False
            
    except Exception as e:
        print(f"   ❌ Error verificando puerto: {e}")
        return False

def test_psycopg2_tcp():
    """Test psycopg2 con conexión TCP específica"""
    print("🧪 Test psycopg2 TCP...")
    
    try:
        # Conexión TCP explícita con configuración específica
        conn = psycopg2.connect(
            host="localhost",      # TCP explícito
            port=5432,
            database="epidemiologia_tolima",
            user="tolima_admin",
            password="tolima2025",
            connect_timeout=10,
            application_name="test_python"
        )
        
        # Test básico
        with conn.cursor() as cursor:
            cursor.execute("SELECT 1, current_user, current_database()")
            result = cursor.fetchone()
            
        print(f"   ✅ psycopg2 TCP exitoso: {result}")
        conn.close()
        return True
        
    except psycopg2.OperationalError as e:
        print(f"   ❌ psycopg2 error operacional: {e}")
        return False
    except Exception as e:
        print(f"   ❌ psycopg2 error: {e}")
        return False

def test_sqlalchemy_tcp():
    """Test SQLAlchemy con TCP"""
    print("🧪 Test SQLAlchemy TCP...")
    
    try:
        # URL TCP específica
        url = "postgresql://tolima_admin:tolima2025@localhost:5432/epidemiologia_tolima"
        
        engine = create_engine(
            url,
            connect_args={
                "connect_timeout": 10,
                "application_name": "test_sqlalchemy"
            }
        )
        
        with engine.connect() as conn:
            result = conn.execute(text("SELECT 1, current_user"))
            row = result.fetchone()
            print(f"   ✅ SQLAlchemy TCP exitoso: {row}")
            
        return True
        
    except Exception as e:
        print(f"   ❌ SQLAlchemy error: {e}")
        return False

def test_comparativo():
    """Test comparativo Docker vs TCP"""
    print("🔍 Test comparativo Docker vs TCP...")
    
    # Docker test
    import subprocess
    result_docker = subprocess.run([
        'docker', 'exec', 'tolima_postgres',
        'psql', '-U', 'tolima_admin', '-d', 'epidemiologia_tolima',
        '-c', 'SELECT current_user, version();'
    ], capture_output=True, text=True)
    
    docker_ok = result_docker.returncode == 0
    print(f"   Docker (Unix socket): {'✅' if docker_ok else '❌'}")
    
    # TCP test
    tcp_ok = test_psycopg2_tcp()
    print(f"   Python (TCP): {'✅' if tcp_ok else '❌'}")
    
    if docker_ok and not tcp_ok:
        print("   🔍 Problema: Docker funciona, TCP falla → Problema autenticación TCP")
    elif not docker_ok and not tcp_ok:
        print("   🔍 Problema: Ambos fallan → Problema PostgreSQL general")
    elif docker_ok and tcp_ok:
        print("   🎉 Ambos funcionan → Sistema OK")
    
    return tcp_ok

def main():
    print("🧪 TEST TCP ESPECÍFICO POSTGRESQL")
    print("=" * 35)
    
    puerto_ok = test_puerto_accesible()
    
    if not puerto_ok:
        print("❌ Puerto no accesible - revisar Docker")
        return False
    
    comparativo_ok = test_comparativo()
    psycopg2_ok = test_psycopg2_tcp()
    sqlalchemy_ok = test_sqlalchemy_tcp()
    
    print("\n📊 RESUMEN FINAL:")
    print(f"   Puerto 5432: {'✅' if puerto_ok else '❌'}")
    print(f"   Comparativo: {'✅' if comparativo_ok else '❌'}")
    print(f"   psycopg2: {'✅' if psycopg2_ok else '❌'}")
    print(f"   SQLAlchemy: {'✅' if sqlalchemy_ok else '❌'}")
    
    if psycopg2_ok and sqlalchemy_ok:
        print("\n🎉 ¡CONEXIONES TCP FUNCIONAN!")
        return True
    else:
        print("\n❌ Conexiones TCP siguen fallando")
        return False

if __name__ == "__main__":
    main()
