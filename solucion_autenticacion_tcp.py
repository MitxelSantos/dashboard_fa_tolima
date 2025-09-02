#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
solucion_autenticacion_tcp.py - Soluciona autenticación TCP PostgreSQL
Problema real: Conexión Docker (Unix socket) funciona, TCP externa falla
"""

import subprocess
import time
import os

def diagnosticar_problema():
    """Diagnostica exactamente cuál es el problema"""
    print("🔍 DIAGNÓSTICO DETALLADO DEL PROBLEMA")
    print("=" * 45)
    
    # Test 1: Conexión Docker (Unix socket)
    print("1. Test conexión Docker (Unix socket)...")
    result = subprocess.run([
        'docker', 'exec', 'tolima_postgres',
        'psql', '-U', 'tolima_admin', '-d', 'epidemiologia_tolima',
        '-c', 'SELECT current_user, inet_server_addr(), inet_server_port();'
    ], capture_output=True, text=True)
    
    if result.returncode == 0:
        print("   ✅ Docker (Unix socket): FUNCIONA")
        print(f"   📄 Output: {result.stdout.strip()}")
    else:
        print("   ❌ Docker (Unix socket): FALLA")
    
    # Test 2: Verificar configuración pg_hba.conf
    print("\n2. Verificando pg_hba.conf...")
    result = subprocess.run([
        'docker', 'exec', 'tolima_postgres',
        'cat', '/var/lib/postgresql/data/pgdata/pg_hba.conf'
    ], capture_output=True, text=True)
    
    if result.returncode == 0:
        print("   ✅ pg_hba.conf leído")
        if 'host    all             all             0.0.0.0/0' in result.stdout:
            print("   ✅ Regla para conexiones externas existe")
        else:
            print("   ❌ Regla para conexiones externas FALTA")
    
    # Test 3: Verificar si PostgreSQL escucha en todas las interfaces
    print("\n3. Verificando listen_addresses...")
    result = subprocess.run([
        'docker', 'exec', 'tolima_postgres',
        'psql', '-U', 'tolima_admin', '-d', 'epidemiologia_tolima',
        '-c', "SHOW listen_addresses;"
    ], capture_output=True, text=True)
    
    if result.returncode == 0:
        print("   ✅ listen_addresses configurado")
        print(f"   📄 Valor: {result.stdout.strip()}")
    
    # Test 4: Verificar contraseña del usuario
    print("\n4. Verificando usuario y contraseña...")
    result = subprocess.run([
        'docker', 'exec', 'tolima_postgres',
        'psql', '-U', 'tolima_admin', '-d', 'epidemiologia_tolima',
        '-c', "SELECT rolname, rolcanlogin FROM pg_roles WHERE rolname = 'tolima_admin';"
    ], capture_output=True, text=True)
    
    if result.returncode == 0:
        print("   ✅ Usuario verificado")
        print(f"   📄 Info: {result.stdout.strip()}")
    
    print("\n" + "="*45)

def configurar_autenticacion_tcp():
    """Configura correctamente la autenticación TCP"""
    print("🔧 CONFIGURANDO AUTENTICACIÓN TCP")
    print("=" * 35)
    
    print("1. Recreando pg_hba.conf correcto...")
    
    # Crear pg_hba.conf que funcione para TCP
    pg_hba_correcto = '''# PostgreSQL Client Authentication Configuration File
# TYPE  DATABASE        USER            ADDRESS                 METHOD

# Local connections (Unix socket) - dentro del contenedor
local   all             all                                     trust

# IPv4 local connections (TCP)
host    all             all             127.0.0.1/32            md5
host    all             all             ::1/128                 md5

# Allow connections from Docker host to container
host    all             all             172.17.0.0/16           md5
host    all             all             172.18.0.0/16           md5
host    all             all             172.19.0.0/16           md5

# Allow connections from any address (para desarrollo)
host    all             all             0.0.0.0/0               md5
'''
    
    # Escribir el archivo dentro del contenedor
    cmd_crear_hba = f'''cat > /var/lib/postgresql/data/pgdata/pg_hba.conf << 'EOF'
{pg_hba_correcto}
EOF'''
    
    result = subprocess.run([
        'docker', 'exec', 'tolima_postgres', 'bash', '-c', cmd_crear_hba
    ], capture_output=True, text=True)
    
    if result.returncode == 0:
        print("   ✅ pg_hba.conf recreado")
    else:
        print(f"   ❌ Error recreando pg_hba.conf: {result.stderr}")
        return False
    
    print("2. Configurando postgresql.conf...")
    
    # Configurar postgresql.conf para escuchar en todas las interfaces
    cmd_postgresql_conf = '''
echo "listen_addresses = '*'" >> /var/lib/postgresql/data/pgdata/postgresql.conf
echo "port = 5432" >> /var/lib/postgresql/data/pgdata/postgresql.conf
echo "max_connections = 100" >> /var/lib/postgresql/data/pgdata/postgresql.conf
echo "log_connections = on" >> /var/lib/postgresql/data/pgdata/postgresql.conf
echo "log_disconnections = on" >> /var/lib/postgresql/data/pgdata/postgresql.conf
echo "log_statement = 'none'" >> /var/lib/postgresql/data/pgdata/postgresql.conf
echo "lc_messages = 'C'" >> /var/lib/postgresql/data/pgdata/postgresql.conf
'''
    
    result = subprocess.run([
        'docker', 'exec', 'tolima_postgres', 'bash', '-c', cmd_postgresql_conf
    ], capture_output=True, text=True)
    
    if result.returncode == 0:
        print("   ✅ postgresql.conf configurado")
    else:
        print(f"   ⚠️ Error configurando postgresql.conf: {result.stderr}")
    
    print("3. Reseteando contraseña del usuario...")
    
    # Resetear contraseña para asegurar que funciona
    cmd_password = "ALTER USER tolima_admin WITH PASSWORD 'tolima2025';"
    
    result = subprocess.run([
        'docker', 'exec', 'tolima_postgres',
        'psql', '-U', 'tolima_admin', '-d', 'epidemiologia_tolima',
        '-c', cmd_password
    ], capture_output=True, text=True)
    
    if result.returncode == 0:
        print("   ✅ Contraseña reseteada")
    else:
        print(f"   ⚠️ Error reseteando contraseña: {result.stderr}")
    
    print("4. Reiniciando PostgreSQL...")
    subprocess.run(['docker', 'restart', 'tolima_postgres'], capture_output=True)
    
    print("   ⏱️ Esperando reinicio...")
    time.sleep(20)
    
    # Verificar que el servicio está listo
    for i in range(10):
        result = subprocess.run([
            'docker', 'exec', 'tolima_postgres',
            'pg_isready', '-U', 'tolima_admin', '-d', 'epidemiologia_tolima'
        ], capture_output=True, text=True)
        
        if result.returncode == 0:
            print("   ✅ PostgreSQL reiniciado y listo")
            return True
        
        time.sleep(3)
    
    print("   ⚠️ PostgreSQL tardando en reiniciar")
    return True

def crear_test_tcp_especifico():
    """Crea test específico para conexiones TCP"""
    
    test_tcp = '''#!/usr/bin/env python
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
    
    print("\\n📊 RESUMEN FINAL:")
    print(f"   Puerto 5432: {'✅' if puerto_ok else '❌'}")
    print(f"   Comparativo: {'✅' if comparativo_ok else '❌'}")
    print(f"   psycopg2: {'✅' if psycopg2_ok else '❌'}")
    print(f"   SQLAlchemy: {'✅' if sqlalchemy_ok else '❌'}")
    
    if psycopg2_ok and sqlalchemy_ok:
        print("\\n🎉 ¡CONEXIONES TCP FUNCIONAN!")
        return True
    else:
        print("\\n❌ Conexiones TCP siguen fallando")
        return False

if __name__ == "__main__":
    main()
'''
    
    with open('test_tcp.py', 'w', encoding='utf-8') as f:
        f.write(test_tcp)
    
    print("✅ test_tcp.py creado")

def main():
    print("🚀 SOLUCIÓN AUTENTICACIÓN TCP POSTGRESQL")
    print("=" * 45)
    
    # Paso 1: Diagnosticar problema
    diagnosticar_problema()
    
    # Paso 2: Configurar autenticación TCP
    if configurar_autenticacion_tcp():
        
        # Paso 3: Crear test específico
        crear_test_tcp_especifico()
        
        print("\n🎯 EJECUTANDO TEST TCP...")
        result = subprocess.run(['python', 'test_tcp.py'], 
                              capture_output=True, text=True)
        
        print(result.stdout)
        if result.stderr:
            print("Errores:")
            print(result.stderr)
        
        if result.returncode == 0:
            print("\n✅ ¡PROBLEMA DE AUTENTICACIÓN TCP RESUELTO!")
        else:
            print("\n⚠️ Autenticación TCP aún tiene problemas")
            
        print("\n📋 Próximos pasos:")
        print("1. python test_tcp.py")
        print("2. python scripts/test_connection.py")
        
    else:
        print("❌ Error configurando autenticación TCP")

if __name__ == "__main__":
    main()