#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
test_conexion.py - Prueba Sistema Epidemiológico Tolima V2.0
CORREGIDO: Sin generación de datos de prueba, solo verificaciones reales
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
        cargar_codigos_divipola_desde_gpkg, validar_configuracion,
        verificar_actualizacion_archivos
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
        
        # 5. Probar sistema de alertas
        print("\n🚨 Probando sistema de alertas...")
        try:
            alertas = verificar_actualizacion_archivos()
            if alertas:
                print(f"   ⚠️ {len(alertas)} alertas generadas")
            else:
                print("   ✅ Sistema de alertas funcionando (sin alertas)")
        except Exception as e:
            print(f"   ⚠️ Error en sistema de alertas: {e}")
        
        print("✅ Configuración centralizada funcionando correctamente")
        return True
        
    except Exception as e:
        print(f"❌ Error probando configuración: {e}")
        return False

def test_integridad_sistema():
    """Verifica integridad general del sistema SIN generar datos de prueba"""
    print("\n🔍 VERIFICANDO INTEGRIDAD DEL SISTEMA...")
    print("(Solo verificaciones - sin generar datos de prueba)")
    
    try:
        engine = create_engine(DATABASE_URL)
        
        with engine.connect() as conn:
            # 1. Verificar que PostgreSQL responde
            conn.execute(text("SELECT 1"))
            print("   ✅ PostgreSQL responde correctamente")
            
            # 2. Verificar extensiones críticas
            ext_postgis = conn.execute(text(
                "SELECT 1 FROM pg_extension WHERE extname = 'postgis'"
            )).fetchone()
            
            if ext_postgis:
                print("   ✅ PostGIS disponible para datos geoespaciales")
            else:
                print("   ⚠️ PostGIS no encontrado")
            
            # 3. Verificar capacidad de crear tablas temporales
            try:
                conn.execute(text("""
                    CREATE TEMP TABLE test_temp (
                        id SERIAL PRIMARY KEY,
                        nombre VARCHAR(50),
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """))
                conn.execute(text("DROP TABLE test_temp"))
                print("   ✅ Capacidad de crear tablas verificada")
            except Exception as e:
                print(f"   ⚠️ Error creando tablas temporales: {e}")
            
            # 4. Verificar zona horaria
            timezone = conn.execute(text("SHOW timezone")).scalar()
            print(f"   ℹ️ Zona horaria PostgreSQL: {timezone}")
            
            # 5. Verificar espacio disponible (si es posible)
            try:
                tamano_bd = conn.execute(text(
                    f"SELECT pg_size_pretty(pg_database_size('{DatabaseConfig.DATABASE}'))"
                )).scalar()
                print(f"   📊 Tamaño base de datos: {tamano_bd}")
            except Exception as e:
                print(f"   ℹ️ No se pudo obtener tamaño BD: {e}")
        
        print("✅ Integridad del sistema verificada")
        return True
        
    except Exception as e:
        print(f"❌ Error verificación integridad: {e}")
        return False

def main():
    """Función principal de pruebas V2.0 - SIN datos de prueba"""
    print("🧪 PRUEBA COMPLETA SISTEMA POSTGRESQL V2.0")
    print("=" * 55)
    print("MODO: Solo verificaciones - sin generar datos de prueba")
    
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
    print("\n" + "="*50)
    print("PRUEBA 1: CONFIGURACIÓN CENTRALIZADA")
    print("="*50)
    if test_configuracion_centralizada():
        pruebas_exitosas += 1
    
    # Prueba 2: Conexión PostgreSQL
    print("\n" + "="*50)
    print("PRUEBA 2: CONEXIÓN POSTGRESQL")
    print("="*50)
    if test_postgresql_connection():
        pruebas_exitosas += 1
        
        # Prueba 3: Integridad (solo si conexión OK)
        print("\n" + "="*50)
        print("PRUEBA 3: INTEGRIDAD SISTEMA")
        print("="*50)
        if test_integridad_sistema():
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
    
    print(f"\n📞 IMPORTANTE:")
    print("• Este test NO genera datos de prueba")
    print("• Solo funciona con datos reales del sistema")
    print("• Para generar data, usar scripts de carga específicos")
    
    return pruebas_exitosas == total_pruebas

if __name__ == "__main__":
    main()