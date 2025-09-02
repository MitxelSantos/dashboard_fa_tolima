#!/usr/bin/env python
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
        print("\n🎉 ¡TODAS LAS CONEXIONES FUNCIONAN!")
        print("✅ Sistema listo para usar")
        return True
    else:
        print("\n⚠️ Algunas conexiones fallan")
        return False

if __name__ == "__main__":
    main()
