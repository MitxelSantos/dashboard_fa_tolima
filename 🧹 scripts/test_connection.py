#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
test_connection.py
Prueba de conexión a PostgreSQL
"""

import pandas as pd
import psycopg2
from sqlalchemy import create_engine
import warnings
warnings.filterwarnings("ignore")

# String de conexión
DATABASE_URL = "postgresql://tolima_admin:tolima2025!@localhost:5432/epidemiologia_tolima"

def test_postgresql_connection():
    """
    Prueba la conexión a PostgreSQL y verifica el esquema
    """
    print("🐘 Probando conexión a PostgreSQL...")
    
    try:
        # 1. Probar conexión básica
        engine = create_engine(DATABASE_URL)
        
        with engine.connect() as conn:
            print("✅ Conexión exitosa!")
            
            # 2. Verificar extensiones
            print("\n🔧 Extensiones instaladas:")
            extensiones = pd.read_sql("""
                SELECT extname, extversion 
                FROM pg_extension 
                WHERE extname IN ('postgis', 'pg_trgm', 'unaccent')
                ORDER BY extname
            """, conn)
            print(extensiones.to_string(index=False))
            
            # 3. Verificar tablas creadas
            print("\n📊 Tablas creadas:")
            tablas = pd.read_sql("""
                SELECT table_name, table_type 
                FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_type = 'BASE TABLE'
                ORDER BY table_name
            """, conn)
            print(tablas.to_string(index=False))
            
            # 4. Verificar vistas
            print("\n👁️ Vistas creadas:")
            vistas = pd.read_sql("""
                SELECT table_name 
                FROM information_schema.views 
                WHERE table_schema = 'public'
                ORDER BY table_name
            """, conn)
            print(vistas.to_string(index=False))
            
            # 5. Verificar funciones
            print("\n⚙️ Funciones personalizadas:")
            funciones = pd.read_sql("""
                SELECT routine_name, routine_type
                FROM information_schema.routines 
                WHERE routine_schema = 'public'
                AND routine_name LIKE '%validar%' OR routine_name LIKE '%calcular%'
                ORDER BY routine_name
            """, conn)
            print(funciones.to_string(index=False))
            
        return True
        
    except Exception as e:
        print(f"❌ Error de conexión: {e}")
        print("\n💡 Posibles soluciones:")
        print("1. Verificar que Docker esté corriendo: docker ps")
        print("2. Verificar logs: docker-compose logs postgres")
        print("3. Reiniciar contenedores: docker-compose down && docker-compose up -d")
        return False

def test_sample_data_insert():
    """
    Prueba insertar datos de muestra
    """
    if not test_postgresql_connection():
        return False
        
    print("\n🧪 Probando inserción de datos de muestra...")
    
    try:
        engine = create_engine(DATABASE_URL)
        
        # Datos de muestra: unidad territorial
        sample_territorio = pd.DataFrame([{
            'tipo': 'municipio',
            'codigo_divipola': '73001',
            'codigo_dpto': '73',
            'codigo_municipio': '73001',
            'nombre': 'Ibagué',
            'municipio': 'Ibagué',
            'region': 'CENTRO',
            'area_oficial_km2': 1498.0,
            'activo': True
        }])
        
        # Insertar muestra territorio
        sample_territorio.to_sql('unidades_territoriales', engine, 
                               if_exists='append', index=False)
        print("✅ Territorio de muestra insertado")
        
        # Datos de muestra: población
        sample_poblacion = pd.DataFrame([{
            'codigo_municipio': '73001',
            'municipio': 'Ibagué', 
            'tipo_ubicacion': 'Urbano',
            'grupo_etario': '20-59 años',
            'poblacion_total': 350000,
            'año': 2024
        }])
        
        sample_poblacion.to_sql('poblacion', engine, 
                              if_exists='append', index=False)
        print("✅ Población de muestra insertada")
        
        # Datos de muestra: vacunación
        sample_vacunacion = pd.DataFrame([{
            'codigo_municipio': '73001',
            'municipio': 'Ibagué',
            'tipo_ubicacion': 'Urbano', 
            'institucion': 'Hospital San Rafael',
            'fecha_aplicacion': '2024-01-15',
            'grupo_etario': '20-59 años',
            'edad_anos': 35
        }])
        
        sample_vacunacion.to_sql('vacunacion_fiebre_amarilla', engine,
                               if_exists='append', index=False)
        print("✅ Vacunación de muestra insertada")
        
        # Verificar vista de coberturas
        with engine.connect() as conn:
            cobertura_test = pd.read_sql("""
                SELECT municipio, vacunados, poblacion_total, cobertura_porcentaje
                FROM v_coberturas_dashboard 
                LIMIT 5
            """, conn)
            
            print("\n📈 Muestra vista coberturas:")
            print(cobertura_test.to_string(index=False))
        
        print("\n🎉 ¡Sistema PostgreSQL funcionando perfectamente!")
        print("💡 Listo para adaptar tu script paiweb.py")
        
        return True
        
    except Exception as e:
        print(f"❌ Error en pruebas: {e}")
        return False

if __name__ == "__main__":
    print("🧪 PRUEBA SISTEMA POSTGRESQL TOLIMA")
    print("=" * 50)
    
    # Probar conexión
    if test_postgresql_connection():
        # Probar datos de muestra
        test_sample_data_insert()
    
    print("\n🔗 URLs útiles:")
    print("• pgAdmin: http://localhost:8080")
    print("  Usuario: admin@tolima.gov.co")
    print("  Contraseña: admin123")
    print("\n• Conexión directa: postgresql://tolima_admin:tolima2025!@localhost:5432/epidemiologia_tolima")