#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
diagnostico_completo.py - Diagnóstico Total Sistema Epidemiológico Tolima
Identifica y soluciona TODOS los problemas automáticamente
"""

import subprocess
import sys
import os
import time
from pathlib import Path
from datetime import datetime
import warnings

warnings.filterwarnings('ignore')

class DiagnosticoCompletoTolima:
    def __init__(self):
        self.problemas_encontrados = []
        self.soluciones_aplicadas = []
        self.verificaciones_exitosas = 0
        self.verificaciones_totales = 0
        
    def log(self, mensaje, tipo="INFO"):
        """Log con timestamp"""
        timestamp = datetime.now().strftime("%H:%M:%S")
        print(f"[{timestamp}] {tipo}: {mensaje}")
    
    def verificar_dependencias_python(self):
        """Verifica y corrige dependencias Python"""
        self.log("🐍 Verificando dependencias Python...")
        self.verificaciones_totales += 1
        
        modulos_criticos = {
            'psycopg2': 'psycopg2-binary==2.9.9',
            'sqlalchemy': 'SQLAlchemy==2.0.25',
            'pandas': 'pandas>=2.2.0',
            'dotenv': 'python-dotenv>=1.0.1',
            'pg8000': 'pg8000>=1.30.0'
        }
        
        modulos_faltantes = []
        
        for modulo, paquete in modulos_criticos.items():
            try:
                __import__(modulo)
                self.log(f"✅ {modulo}: disponible")
            except ImportError:
                self.log(f"❌ {modulo}: FALTANTE")
                modulos_faltantes.append(paquete)
        
        if modulos_faltantes:
            self.problemas_encontrados.append(f"Dependencias faltantes: {', '.join(modulos_faltantes)}")
            
            # Intentar instalar automáticamente
            respuesta = input(f"\n🔧 ¿Instalar {len(modulos_faltantes)} dependencias faltantes? (Y/n): ")
            if respuesta.lower() != 'n':
                self.log("📦 Instalando dependencias...")
                
                for paquete in modulos_faltantes:
                    try:
                        self.log(f"⬇️ Instalando {paquete}...")
                        result = subprocess.run([
                            sys.executable, '-m', 'pip', 'install', paquete
                        ], capture_output=True, text=True, timeout=120)
                        
                        if result.returncode == 0:
                            self.log(f"✅ {paquete} instalado")
                            self.soluciones_aplicadas.append(f"Instalado: {paquete}")
                        else:
                            self.log(f"❌ Error instalando {paquete}: {result.stderr[:100]}")
                    except Exception as e:
                        self.log(f"❌ Error: {e}")
                
                # Verificar después de instalación
                self.log("🔍 Re-verificando dependencias...")
                todas_instaladas = True
                for modulo in modulos_criticos.keys():
                    try:
                        __import__(modulo)
                    except ImportError:
                        todas_instaladas = False
                
                if todas_instaladas:
                    self.verificaciones_exitosas += 1
                    self.log("✅ Todas las dependencias disponibles")
                else:
                    self.log("⚠️ Algunas dependencias siguen faltando")
            else:
                self.log("⏭️ Instalación de dependencias omitida")
        else:
            self.verificaciones_exitosas += 1
            self.log("✅ Todas las dependencias Python disponibles")
    
    def verificar_docker(self):
        """Verifica Docker y PostgreSQL"""
        self.log("🐳 Verificando Docker y PostgreSQL...")
        self.verificaciones_totales += 1
        
        try:
            # Verificar Docker
            result = subprocess.run(['docker', '--version'], capture_output=True, text=True)
            if result.returncode != 0:
                self.problemas_encontrados.append("Docker no instalado")
                self.log("❌ Docker no está instalado")
                return False
            
            self.log(f"✅ {result.stdout.strip()}")
            
            # Verificar estado de contenedores
            result = subprocess.run(['docker-compose', 'ps'], capture_output=True, text=True)
            
            if 'tolima_postgres' in result.stdout and 'Up' in result.stdout:
                self.log("✅ PostgreSQL contenedor corriendo")
            else:
                self.log("⚠️ PostgreSQL contenedor no está corriendo")
                self.problemas_encontrados.append("PostgreSQL contenedor parado")
                
                # Intentar levantar automáticamente
                respuesta = input("🚀 ¿Levantar servicios Docker? (Y/n): ")
                if respuesta.lower() != 'n':
                    self.log("🚀 Levantando servicios Docker...")
                    
                    # Parar primero para limpiar
                    subprocess.run(['docker-compose', 'down'], capture_output=True)
                    time.sleep(2)
                    
                    # Levantar servicios
                    result = subprocess.run(['docker-compose', 'up', '-d'], 
                                          capture_output=True, text=True)
                    
                    if result.returncode == 0:
                        self.log("✅ Servicios Docker levantados")
                        self.soluciones_aplicadas.append("Docker services iniciados")
                        
                        # Esperar inicialización
                        self.log("⏱️ Esperando inicialización PostgreSQL (30 segundos)...")
                        for i in range(30, 0, -5):
                            print(f"   Espera restante: {i} segundos", end='\r')
                            time.sleep(5)
                        print()  # Nueva línea
                        
                        self.verificaciones_exitosas += 1
                    else:
                        self.log(f"❌ Error levantando Docker: {result.stderr}")
                        return False
                else:
                    self.log("⏭️ Servicios Docker no levantados")
                    return False
            
            # Verificar logs de PostgreSQL
            try:
                result = subprocess.run(['docker-compose', 'logs', '--tail=10', 'postgres'], 
                                      capture_output=True, text=True, timeout=10)
                if 'ready to accept connections' in result.stdout:
                    self.log("✅ PostgreSQL listo para conexiones")
                    if 'tolima_postgres' not in result.stdout or 'Up' not in str(result):
                        self.verificaciones_exitosas += 1
                elif 'FATAL' in result.stdout or 'ERROR' in result.stdout:
                    self.log("❌ Errores en logs PostgreSQL")
                    self.problemas_encontrados.append("Errores PostgreSQL en logs")
                else:
                    self.log("ℹ️ PostgreSQL iniciando...")
            except:
                self.log("⚠️ No se pudieron verificar logs PostgreSQL")
            
            return True
            
        except FileNotFoundError:
            self.problemas_encontrados.append("Docker no encontrado en PATH")
            self.log("❌ Docker no encontrado")
            return False
        except Exception as e:
            self.problemas_encontrados.append(f"Error Docker: {e}")
            self.log(f"❌ Error verificando Docker: {e}")
            return False
    
    def verificar_configuracion(self):
        """Verifica archivos de configuración"""
        self.log("⚙️ Verificando configuración...")
        self.verificaciones_totales += 1
        
        archivos_config = {
            '.env': self.verificar_archivo_env,
            'docker-compose.yml': self.verificar_docker_compose,
            'config.py': self.verificar_config_py
        }
        
        configs_correctas = 0
        
        for archivo, verificador in archivos_config.items():
            if verificador():
                configs_correctas += 1
        
        if configs_correctas == len(archivos_config):
            self.verificaciones_exitosas += 1
            self.log("✅ Configuración completa")
        else:
            self.log(f"⚠️ Configuración parcial: {configs_correctas}/{len(archivos_config)}")
    
    def verificar_archivo_env(self):
        """Verifica archivo .env"""
        env_path = Path('.env')
        
        if not env_path.exists():
            self.log("❌ .env no existe")
            self.problemas_encontrados.append("Archivo .env faltante")
            return self.crear_archivo_env()
        
        # Leer y verificar contenido
        try:
            with open(env_path, 'r', encoding='utf-8') as f:
                contenido = f.read()
            
            variables_necesarias = ['DB_PASSWORD', 'DB_USER', 'DB_NAME', 'DB_HOST']
            variables_faltantes = []
            
            for var in variables_necesarias:
                if var not in contenido:
                    variables_faltantes.append(var)
            
            if variables_faltantes:
                self.log(f"⚠️ .env falta variables: {variables_faltantes}")
                self.problemas_encontrados.append(f".env incompleto: {variables_faltantes}")
                return self.crear_archivo_env()
            
            # Verificar contraseña correcta
            if 'DB_PASSWORD=tolima2025' in contenido:
                self.log("✅ .env configurado correctamente")
                return True
            elif 'admin123' in contenido:
                self.log("⚠️ .env con contraseña incorrecta")
                self.problemas_encontrados.append(".env contraseña incorrecta")
                return self.crear_archivo_env()
            else:
                self.log("✅ .env existe")
                return True
                
        except Exception as e:
            self.log(f"❌ Error leyendo .env: {e}")
            return False
    
    def crear_archivo_env(self):
        """Crea archivo .env corregido"""
        respuesta = input("🔧 ¿Crear/corregir archivo .env? (Y/n): ")
        if respuesta.lower() == 'n':
            return False
        
        contenido_env = """# Sistema Epidemiológico Tolima V2.0 - CONFIGURACIÓN CORREGIDA
DB_HOST=localhost
DB_PORT=5432
DB_NAME=epidemiologia_tolima
DB_USER=tolima_admin
DB_PASSWORD=tolima2025

ENVIRONMENT=development
SYSTEM_VERSION=2.0
CACHE_TTL=3600
LOG_LEVEL=INFO
"""
        
        try:
            with open('.env', 'w', encoding='utf-8') as f:
                f.write(contenido_env)
            self.log("✅ .env creado/corregido")
            self.soluciones_aplicadas.append(".env creado con configuración correcta")
            return True
        except Exception as e:
            self.log(f"❌ Error creando .env: {e}")
            return False
    
    def verificar_docker_compose(self):
        """Verifica docker-compose.yml"""
        compose_path = Path('docker-compose.yml')
        
        if not compose_path.exists():
            self.problemas_encontrados.append("docker-compose.yml faltante")
            self.log("❌ docker-compose.yml no existe")
            return False
        
        try:
            with open(compose_path, 'r', encoding='utf-8') as f:
                contenido = f.read()
            
            if 'POSTGRES_PASSWORD: tolima2025' in contenido:
                self.log("✅ docker-compose.yml configurado correctamente")
                return True
            else:
                self.log("⚠️ docker-compose.yml configuración inconsistente")
                return True  # No crítico
                
        except Exception as e:
            self.log(f"❌ Error leyendo docker-compose.yml: {e}")
            return False
    
    def verificar_config_py(self):
        """Verifica config.py"""
        config_path = Path('config.py')
        
        if config_path.exists():
            self.log("✅ config.py existe")
            return True
        else:
            self.problemas_encontrados.append("config.py faltante")
            self.log("❌ config.py no existe")
            return False
    
    def probar_conexiones(self):
        """Prueba conexiones a PostgreSQL"""
        self.log("🔗 Probando conexiones PostgreSQL...")
        self.verificaciones_totales += 1
        
        # Configuración de conexión corregida
        config_conexion = {
            'host': 'localhost',
            'port': 5432,
            'database': 'epidemiologia_tolima',
            'user': 'tolima_admin',
            'password': 'tolima2025'
        }
        
        conexiones_exitosas = 0
        
        # Probar psycopg2
        try:
            import psycopg2
            conn = psycopg2.connect(**config_conexion)
            conn.close()
            self.log("✅ Conexión psycopg2 exitosa")
            conexiones_exitosas += 1
        except ImportError:
            self.log("❌ psycopg2 no disponible")
        except Exception as e:
            self.log(f"❌ Error conexión psycopg2: {e}")
            self.problemas_encontrados.append(f"Conexión psycopg2 falló: {e}")
        
        # Probar pg8000
        try:
            import pg8000
            conn = pg8000.connect(**config_conexion)
            conn.close()
            self.log("✅ Conexión pg8000 exitosa")
            conexiones_exitosas += 1
        except ImportError:
            self.log("❌ pg8000 no disponible")
        except Exception as e:
            self.log(f"❌ Error conexión pg8000: {e}")
            self.problemas_encontrados.append(f"Conexión pg8000 falló: {e}")
        
        # Probar SQLAlchemy
        try:
            from sqlalchemy import create_engine, text
            DATABASE_URL = "postgresql://tolima_admin:tolima2025@localhost:5432/epidemiologia_tolima"
            engine = create_engine(DATABASE_URL)
            
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            
            self.log("✅ Conexión SQLAlchemy exitosa")
            conexiones_exitosas += 1
        except ImportError:
            self.log("❌ SQLAlchemy no disponible")
        except Exception as e:
            self.log(f"❌ Error conexión SQLAlchemy: {e}")
            self.problemas_encontrados.append(f"Conexión SQLAlchemy falló: {e}")
        
        if conexiones_exitosas > 0:
            self.verificaciones_exitosas += 1
            self.log(f"✅ {conexiones_exitosas}/3 conexiones exitosas")
            return True
        else:
            self.log("❌ Ninguna conexión exitosa")
            return False
    
    def generar_reporte_final(self):
        """Genera reporte final del diagnóstico"""
        print("\n" + "=" * 70)
        print(" REPORTE DIAGNÓSTICO SISTEMA EPIDEMIOLÓGICO TOLIMA ".center(70))
        print("=" * 70)
        
        # Estado general
        porcentaje_exito = (self.verificaciones_exitosas / max(self.verificaciones_totales, 1)) * 100
        
        print(f"📊 VERIFICACIONES: {self.verificaciones_exitosas}/{self.verificaciones_totales} ({porcentaje_exito:.1f}%)")
        
        if porcentaje_exito >= 80:
            print("🎉 ESTADO GENERAL: SISTEMA FUNCIONAL")
        elif porcentaje_exito >= 60:
            print("⚠️ ESTADO GENERAL: SISTEMA PARCIALMENTE FUNCIONAL")
        else:
            print("❌ ESTADO GENERAL: SISTEMA CON PROBLEMAS CRÍTICOS")
        
        # Problemas encontrados
        if self.problemas_encontrados:
            print(f"\n❌ PROBLEMAS ENCONTRADOS ({len(self.problemas_encontrados)}):")
            for problema in self.problemas_encontrados:
                print(f"   • {problema}")
        
        # Soluciones aplicadas
        if self.soluciones_aplicadas:
            print(f"\n✅ SOLUCIONES APLICADAS ({len(self.soluciones_aplicadas)}):")
            for solucion in self.soluciones_aplicadas:
                print(f"   • {solucion}")
        
        # Próximos pasos
        print(f"\n🎯 PRÓXIMOS PASOS:")
        if porcentaje_exito >= 80:
            print("   1. Colocar archivos de datos en data/")
            print("   2. Ejecutar: python test.py")
            print("   3. Ejecutar: python scripts/test_connection.py")
            print("   4. ¡Sistema listo para usar! 🚀")
        elif porcentaje_exito >= 60:
            print("   1. Revisar problemas arriba")
            print("   2. Ejecutar nuevamente este diagnóstico")
            print("   3. Probar: python test.py")
        else:
            print("   1. Instalar Docker Desktop")
            print("   2. Instalar dependencias: pip install psycopg2-binary")
            print("   3. Ejecutar: docker-compose up -d")
            print("   4. Re-ejecutar este diagnóstico")
    
    def ejecutar_diagnostico_completo(self):
        """Ejecuta diagnóstico completo paso a paso"""
        print("🔍 DIAGNÓSTICO COMPLETO SISTEMA EPIDEMIOLÓGICO TOLIMA")
        print("=" * 65)
        print("Identifica y soluciona automáticamente problemas del sistema")
        
        # Ejecutar verificaciones
        self.verificar_dependencias_python()
        print()
        
        self.verificar_docker()
        print()
        
        self.verificar_configuracion()
        print()
        
        self.probar_conexiones()
        print()
        
        # Reporte final
        self.generar_reporte_final()
        
        return self.verificaciones_exitosas >= (self.verificaciones_totales * 0.8)

def main():
    """Función principal"""
    diagnostico = DiagnosticoCompletoTolima()
    
    try:
        exito = diagnostico.ejecutar_diagnostico_completo()
        return exito
    except KeyboardInterrupt:
        print("\n\n👋 Diagnóstico interrumpido por el usuario")
        return False
    except Exception as e:
        print(f"\n❌ Error durante diagnóstico: {e}")
        return False

if __name__ == "__main__":
    main()