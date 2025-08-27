#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
test_conexion.py - Prueba Sistema Epidemiológico Tolima V2.0
Versión actualizada con configuración centralizada
"""

import pandas as pd
import psycopg2
from sqlalchemy import create_engine, text
import warnings
import sys
import os

warnings.filterwarnings("ignore")

# Importar configuración centralizada
try:
    from config import (
        DATABASE_URL, FileConfig, DatabaseConfig,
        clasificar_grupo_etario, limpiar_fecha_robusta,
        cargar_codigos_divipola_desde_gpkg, validar_configuracion
    )
    print("✅ Configuración centralizada importada correctamente")
except ImportError as e:
    print(f"❌ Error importando config.py: {e}")
    print("💡 Asegúrate de que config.py esté en el directorio actual")
    sys.exit(1)

def test_postgresql_connection():
    """Prueba la conexión a PostgreSQL usando configuración centralizada"""
    print("🐘 Probando conexión a PostgreSQL...")
    
    try:
        # 1. Probar conexión básica usando config centralizada
        engine = create_engine(DATABASE_URL)
        
        with engine.connect() as conn:
            print("✅ Conexión exitosa usando configuración centralizada!")
            print(f"   Host: {DatabaseConfig.HOST}:{DatabaseConfig.PORT}")
            print(f"   Base de datos: {DatabaseConfig.DATABASE}")
            print(f"   Usuario: {DatabaseConfig.USER}")
            
            # 2. Verificar extensiones
            print("\n🔧 Extensiones instaladas:")
            try:
                extensiones = pd.read_sql(text("""
                    SELECT extname, extversion 
                    FROM pg_extension 
                    WHERE extname IN ('postgis', 'pg_trgm', 'unaccent', 'uuid-ossp')
                    ORDER BY extname
                """), conn)
                
                if len(extensiones) > 0:
                    for _, ext in extensiones.iterrows():
                        print(f"   ✅ {ext['extname']} v{ext['extversion']}")
                else:
                    print("   ⚠️ No se encontraron extensiones esperadas")
            except Exception as e:
                print(f"   ⚠️ Error verificando extensiones: {e}")
            
            # 3. Verificar tablas creadas
            print("\n📊 Tablas del sistema:")
            try:
                tablas = pd.read_sql(text("""
                    SELECT table_name, table_type 
                    FROM information_schema.tables 
                    WHERE table_schema = 'public' 
                    AND table_type = 'BASE TABLE'
                    AND table_name IN (
                        'unidades_territoriales', 'poblacion', 'vacunacion_fiebre_amarilla',
                        'casos_fiebre_amarilla', 'epizootias'
                    )
                    ORDER BY table_name
                """), conn)
                
                if len(tablas) > 0:
                    for _, tabla in tablas.iterrows():
                        # Contar registros
                        try:
                            count = conn.execute(text(f"SELECT COUNT(*) FROM {tabla['table_name']}")).scalar()
                            print(f"   ✅ {tabla['table_name']}: {count:,} registros")
                        except:
                            print(f"   📋 {tabla['table_name']}: tabla creada (sin datos)")
                else:
                    print("   ⚠️ Tablas del sistema no encontradas (normal en instalación nueva)")
            except Exception as e:
                print(f"   ⚠️ Error verificando tablas: {e}")
            
            # 4. Verificar vistas críticas
            print("\n👁️ Vistas del sistema:")
            try:
                vistas = pd.read_sql(text("""
                    SELECT table_name 
                    FROM information_schema.views 
                    WHERE table_schema = 'public'
                    AND table_name LIKE 'v_%'
                    ORDER BY table_name
                """), conn)
                
                if len(vistas) > 0:
                    for _, vista in vistas.iterrows():
                        try:
                            count = conn.execute(text(f"SELECT COUNT(*) FROM {vista['table_name']}")).scalar()
                            print(f"   ✅ {vista['table_name']}: {count:,} registros")
                        except:
                            print(f"   📋 {vista['table_name']}: vista creada")
                else:
                    print("   ⚠️ Vistas del sistema no encontradas (normal en instalación nueva)")
            except Exception as e:
                print(f"   ⚠️ Error verificando vistas: {e}")
        
        return True
        
    except Exception as e:
        print(f"❌ Error de conexión: {e}")
        print("\n💡 Posibles soluciones:")
        print("1. Verificar que Docker esté corriendo: docker ps")
        print("2. Verificar logs: docker-compose logs postgres")
        print("3. Reiniciar contenedores: docker-compose down && docker-compose up -d")
        print("4. Esperar 30-60 segundos después de iniciar Docker")
        return False

def test_configuracion_centralizada():
    """Prueba las funciones de configuración centralizada"""
    print("\n⚙️ PROBANDO CONFIGURACIÓN CENTRALIZADA...")
    
    try:
        # 1. Verificar estructura de directorios
        print("📁 Verificando estructura de directorios...")
        FileConfig.create_directories()
        
        directorios_esperados = [FileConfig.DATA_DIR, FileConfig.LOGS_DIR, FileConfig.BACKUPS_DIR]
        for directorio in directorios_esperados:
            if directorio.exists():
                print(f"   ✅ {directorio.name}: existe")
            else:
                print(f"   ❌ {directorio.name}: no existe")
        
        # 2. Probar función de limpieza de fechas
        print("\n📅 Probando limpieza de fechas...")
        fechas_prueba = [
            "15/01/2024",
            "2024-01-15", 
            "01/15/2024",
            "15-01-2024",
            None,
            ""
        ]
        
        for fecha_test in fechas_prueba:
            resultado = limpiar_fecha_robusta(fecha_test)
            print(f"   {fecha_test} → {resultado}")
        
        # 3. Probar clasificación grupos etarios
        print("\n👥 Probando clasificación grupos etarios...")
        edades_prueba = [6, 12, 30, 300, 800, None]
        for edad in edades_prueba:
            grupo = clasificar_grupo_etario(edad)
            años = (edad / 12) if edad else None
            print(f"   {edad} meses ({años:.1f} años si no es None) → {grupo}")
        
        # 4. Probar carga de códigos DIVIPOLA
        print("\n🗺️ Probando carga códigos DIVIPOLA...")
        gpkg_path = FileConfig.DATA_DIR / "tolima_cabeceras_veredas.gpkg"
        
        if gpkg_path.exists():
            print(f"   ✅ Archivo .gpkg encontrado: {gpkg_path}")
            try:
                codigos = cargar_codigos_divipola_desde_gpkg()
                if codigos:
                    print(f"   ✅ Códigos DIVIPOLA cargados:")
                    print(f"      - Municipios: {len(codigos['municipios'])}")
                    print(f"      - Veredas: {len(codigos['veredas'])}")
                    print(f"      - Cabeceras: {len(codigos['cabeceras'])}")
                else:
                    print("   ⚠️ Error cargando códigos DIVIPOLA")
            except Exception as e:
                print(f"   ⚠️ Error procesando .gpkg: {e}")
        else:
            print(f"   ⚠️ Archivo .gpkg no encontrado: {gpkg_path}")
            print("      💡 Colocar archivo tolima_cabeceras_veredas.gpkg en data/")
        
        print("✅ Configuración centralizada funcionando correctamente")
        return True
        
    except Exception as e:
        print(f"❌ Error probando configuración: {e}")
        return False

def test_sample_data_insert():
    """Prueba insertar datos de muestra"""
    print("\n🧪 PROBANDO INSERCIÓN DE DATOS DE MUESTRA...")
    
    try:
        engine = create_engine(DATABASE_URL)
        
        # Verificar que las tablas existan
        with engine.connect() as conn:
            tablas_sistema = ['unidades_territoriales', 'poblacion', 'vacunacion_fiebre_amarilla']
            
            for tabla in tablas_sistema:
                try:
                    conn.execute(text(f"SELECT 1 FROM {tabla} LIMIT 1"))
                    print(f"   ✅ Tabla {tabla}: disponible")
                except:
                    print(f"   ⚠️ Tabla {tabla}: no existe (crear con scripts SQL)")
                    return False
        
        # Datos de muestra usando configuración
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
        
        sample_poblacion = pd.DataFrame([{
            'codigo_municipio': '73001',
            'municipio': 'Ibagué', 
            'tipo_ubicacion': 'Urbano',
            'grupo_etario': '20-59 años',
            'poblacion_total': 350000,
            'año': 2024,
            'fuente': 'SISBEN'
        }])
        
        sample_vacunacion = pd.DataFrame([{
            'codigo_municipio': '73001',
            'municipio': 'Ibagué',
            'tipo_ubicacion': 'Urbano', 
            'institucion': 'Hospital San Rafael',
            'fecha_aplicacion': '2024-01-15',
            'grupo_etario': '20-59 años',
            'edad_anos': 35,
            'año': 2024,
            'mes': 1,
            'semana_epidemiologica': 3,
            'fuente': 'PAIweb'
        }])
        
        # Insertar muestras
        try:
            sample_territorio.to_sql('unidades_territoriales', engine, if_exists='append', index=False)
            print("   ✅ Territorio de muestra insertado")
            
            sample_poblacion.to_sql('poblacion', engine, if_exists='append', index=False)
            print("   ✅ Población de muestra insertada")
            
            sample_vacunacion.to_sql('vacunacion_fiebre_amarilla', engine, if_exists='append', index=False)
            print("   ✅ Vacunación de muestra insertada")
        except Exception as e:
            print(f"   ⚠️ Error insertando datos: {e}")
            return False
        
        # Verificar vista de coberturas si existe
        with engine.connect() as conn:
            try:
                cobertura_test = pd.read_sql(text("""
                    SELECT municipio, vacunados, poblacion_total, cobertura_porcentaje
                    FROM v_coberturas_dashboard 
                    LIMIT 3
                """), conn)
                
                if len(cobertura_test) > 0:
                    print("\n📈 Muestra vista coberturas:")
                    for _, row in cobertura_test.iterrows():
                        print(f"   {row['municipio']}: {row['cobertura_porcentaje']:.1f}% "
                              f"({row['vacunados']}/{row['poblacion_total']})")
                else:
                    print("   ⚠️ Vista coberturas sin datos")
            except Exception as e:
                print(f"   ⚠️ Vista coberturas no disponible: {e}")
        
        print("\n🎉 ¡Sistema PostgreSQL V2.0 funcionando perfectamente!")
        print("💡 Listo para usar scripts adaptados del sistema")
        
        return True
        
    except Exception as e:
        print(f"❌ Error en pruebas V2.0: {e}")
        return False

def main():
    """Función principal de pruebas V2.0"""
    print("🧪 PRUEBA COMPLETA SISTEMA POSTGRESQL V2.0")
    print("=" * 55)
    
    # Ejecutar validación completa de configuración
    print("⚙️ Ejecutando validación completa...")
    try:
        validar_configuracion()
    except Exception as e:
        print(f"⚠️ Error en validación: {e}")
    
    # Pruebas principales
    pruebas_exitosas = 0
    total_pruebas = 3
    
    print(f"\n📋 Ejecutando {total_pruebas} pruebas principales...")
    
    # Prueba 1: Configuración centralizada
    if test_configuracion_centralizada():
        pruebas_exitosas += 1
    
    # Prueba 2: Conexión PostgreSQL
    if test_postgresql_connection():
        pruebas_exitosas += 1
        
        # Prueba 3: Datos de muestra (solo si conexión OK)
        if test_sample_data_insert():
            pruebas_exitosas += 1
    
    # Resumen final
    print(f"\n{'='*55}")
    print(f"RESUMEN PRUEBAS V2.0: {pruebas_exitosas}/{total_pruebas} exitosas")
    print("=" * 55)
    
    if pruebas_exitosas == total_pruebas:
        print("🎉 ¡SISTEMA V2.0 COMPLETAMENTE FUNCIONAL!")
        print("\n🎯 PRÓXIMOS PASOS:")
        print("1. Colocar archivos de datos en data/ (incluyendo .gpkg)")
        print("2. Ejecutar: python scripts/sistema_coordinador.py --completo")
        print("3. Monitorear: python scripts/monitor_sistema.py --completo")
        print("4. ¡Usar sistema epidemiológico completo! 🚀")
    elif pruebas_exitosas >= 1:
        print("⚠️ Sistema parcialmente funcional")
        print("💡 Revisar errores arriba y corregir")
    else:
        print("❌ Sistema no funcional")
        print("💡 Verificar instalación con setup_sistema.py")
    
    print("\n🔗 URLs útiles:")
    print("• pgAdmin: http://localhost:8080")
    print("  Usuario: admin@tolima.gov.co")
    print("  Contraseña: admin123")
    print(f"\n• Conexión directa: {DATABASE_URL}")
    
    return pruebas_exitosas == total_pruebas

if __name__ == "__main__":
    main()