#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
test.py - Prueba de Conexión PostgreSQL CORREGIDA
Contraseña y configuración corregidas según docker-compose.yml
"""

import pg8000
import psycopg2
from sqlalchemy import create_engine, text
import warnings

warnings.filterwarnings('ignore')

def test_pg8000():
    """Prueba conexión con pg8000"""
    print("🧪 Probando conexión con pg8000...")
    
    try:
        conn = pg8000.connect(
            host="localhost",
            port=5432,
            database="epidemiologia_tolima",
            user="tolima_admin",
            password="tolima2025",  # ✅ CONTRASEÑA CORREGIDA
        )
        
        print("✅ CONEXIÓN PG8000 EXITOSA!")
        
        cursor = conn.cursor()
        cursor.execute("SELECT 1 as test")
        resultado = cursor.fetchone()
        print(f"🎉 Consulta exitosa: {resultado}")
        
        conn.close()
        return True
        
    except Exception as e:
        print(f"❌ Error pg8000: {e}")
        return False

def test_psycopg2():
    """Prueba conexión con psycopg2"""
    print("\n🧪 Probando conexión con psycopg2...")
    
    try:
        conn = psycopg2.connect(
            host="localhost",
            port=5432,
            database="epidemiologia_tolima",
            user="tolima_admin",
            password="tolima2025",  # ✅ CONTRASEÑA CORREGIDA
        )
        
        print("✅ CONEXIÓN PSYCOPG2 EXITOSA!")
        
        cursor = conn.cursor()
        cursor.execute("SELECT version()")
        version = cursor.fetchone()
        print(f"🐘 PostgreSQL: {version[0]}")
        
        # Verificar extensiones
        cursor.execute("SELECT extname FROM pg_extension WHERE extname IN ('postgis', 'pg_trgm')")
        extensiones = cursor.fetchall()
        print(f"🔧 Extensiones: {[ext[0] for ext in extensiones]}")
        
        conn.close()
        return True
        
    except Exception as e:
        print(f"❌ Error psycopg2: {e}")
        return False

def test_sqlalchemy():
    """Prueba conexión con SQLAlchemy"""
    print("\n🧪 Probando conexión con SQLAlchemy...")
    
    try:
        # ✅ URL CORREGIDA con contraseña correcta
        DATABASE_URL = "postgresql://tolima_admin:tolima2025@localhost:5432/epidemiologia_tolima"
        
        engine = create_engine(DATABASE_URL)
        
        with engine.connect() as conn:
            result = conn.execute(text("SELECT current_database(), current_user"))
            db_info = result.fetchone()
            print(f"✅ CONEXIÓN SQLALCHEMY EXITOSA!")
            print(f"🗄️ Base de datos: {db_info[0]}")
            print(f"👤 Usuario: {db_info[1]}")
            
            # Verificar tablas del sistema
            tables_result = conn.execute(text("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name IN ('unidades_territoriales', 'poblacion', 'vacunacion_fiebre_amarilla')
                ORDER BY table_name
            """))
            
            tablas = tables_result.fetchall()
            if tablas:
                print(f"📊 Tablas encontradas: {[tabla[0] for tabla in tablas]}")
            else:
                print("📋 Base de datos vacía (normal en primera instalación)")
        
        return True
        
    except Exception as e:
        print(f"❌ Error SQLAlchemy: {e}")
        return False

def diagnostico_completo():
    """Diagnóstico completo del sistema"""
    print("🔍 DIAGNÓSTICO COMPLETO SISTEMA EPIDEMIOLÓGICO")
    print("=" * 55)
    
    # Verificar dependencias
    try:
        import pandas
        print(f"✅ pandas: {pandas.__version__}")
    except ImportError:
        print("❌ pandas: NO INSTALADO")
    
    try:
        import sqlalchemy
        print(f"✅ sqlalchemy: {sqlalchemy.__version__}")
    except ImportError:
        print("❌ sqlalchemy: NO INSTALADO")
    
    try:
        import geopandas
        print(f"✅ geopandas: {geopandas.__version__}")
    except ImportError:
        print("❌ geopandas: NO INSTALADO")
    
    print("\n" + "=" * 55)
    
    # Probar conexiones
    pruebas_exitosas = 0
    
    if test_psycopg2():
        pruebas_exitosas += 1
    
    if test_pg8000():
        pruebas_exitosas += 1
    
    if test_sqlalchemy():
        pruebas_exitosas += 1
    
    print(f"\n{'='*55}")
    print(f"📊 RESUMEN: {pruebas_exitosas}/3 pruebas exitosas")
    
    if pruebas_exitosas == 3:
        print("🎉 ¡SISTEMA COMPLETAMENTE FUNCIONAL!")
    elif pruebas_exitosas >= 1:
        print("⚠️ Sistema parcialmente funcional")
    else:
        print("❌ Sistema no funcional - revisar configuración")
    
    return pruebas_exitosas

if __name__ == "__main__":
    diagnostico_completo()