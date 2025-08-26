#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Sistema Coordinador Maestro
Orquesta actualizaciones completas del Sistema Epidemiológico Tolima
"""

import subprocess
import os
import sys
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine, text
import warnings
warnings.filterwarnings('ignore')

# Configuración
DATABASE_URL = "postgresql://tolima_admin:tolima2025!@localhost:5432/epidemiologia_tolima"

class SistemaCoordinadorTolima:
    def __init__(self):
        self.inicio = datetime.now()
        self.logs = []
        self.engine = None
        
    def log(self, mensaje, tipo="INFO"):
        timestamp = datetime.now().strftime("%H:%M:%S")
        log_entry = f"[{timestamp}] {tipo}: {mensaje}"
        self.logs.append(log_entry)
        print(log_entry)
    
    def verificar_docker(self):
        """Verifica que Docker PostgreSQL esté corriendo"""
        self.log("🐳 Verificando Docker PostgreSQL...")
        
        try:
            # Verificar si docker-compose está disponible
            result = subprocess.run(["docker-compose", "--version"], 
                                  capture_output=True, text=True, check=True)
            self.log(f"Docker Compose: {result.stdout.strip()}")
            
            # Levantar servicios si no están corriendo
            self.log("Iniciando servicios Docker...")
            subprocess.run(["docker-compose", "up", "-d"], check=True)
            
            # Esperar un momento para que PostgreSQL inicie
            import time
            time.sleep(5)
            
            # Verificar conexión PostgreSQL
            self.engine = create_engine(DATABASE_URL)
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            
            self.log("✅ PostgreSQL funcionando correctamente")
            return True
            
        except subprocess.CalledProcessError as e:
            self.log(f"❌ Error Docker: {e}", "ERROR")
            return False
        except Exception as e:
            self.log(f"❌ Error conexión PostgreSQL: {e}", "ERROR")
            return False
    
    def verificar_archivos_entrada(self):
        """Verifica que los archivos de entrada existan"""
        self.log("📂 Verificando archivos de entrada...")
        
        archivos = {
            "Unidades Territoriales": "data/tolima_cabeceras_veredas.gpkg",
            "Población SISBEN": "data/poblacion_tolima_20250822.csv", 
            "Vacunación PAIweb": "data/paiweb.xlsx",
            "Casos Fiebre Amarilla": "data/casos.xlsx",
            "Epizootias": "data/epizootias.xlsx"
        }
        
        archivos_faltantes = []
        archivos_encontrados = []
        
        for nombre, ruta in archivos.items():
            if os.path.exists(ruta):
                tamaño = os.path.getsize(ruta) / (1024*1024)  # MB
                self.log(f"✅ {nombre}: {ruta} ({tamaño:.1f} MB)")
                archivos_encontrados.append(nombre)
            else:
                self.log(f"❌ {nombre}: {ruta} NO ENCONTRADO", "ERROR")
                archivos_faltantes.append(nombre)
        
        if archivos_faltantes:
            self.log(f"⚠️ Archivos faltantes: {', '.join(archivos_faltantes)}", "WARNING")
            return False, archivos_encontrados, archivos_faltantes
        
        return True, archivos_encontrados, []
    
    def cargar_unidades_territoriales(self):
        """Carga unidades territoriales"""
        self.log("🗺️ Cargando unidades territoriales...")
        
        try:
            from cargar_unidades_territoriales import cargar_unidades_territoriales_postgresql
            resultado = cargar_unidades_territoriales_postgresql("data/tolima_cabeceras_veredas.gpkg")
            
            if resultado:
                self.log("✅ Unidades territoriales cargadas exitosamente")
                return True
            else:
                self.log("❌ Error cargando unidades territoriales", "ERROR")
                return False
                
        except Exception as e:
            self.log(f"❌ Error crítico unidades territoriales: {e}", "ERROR")
            return False
    
    def cargar_poblacion(self):
        """Carga población SISBEN"""
        self.log("👥 Cargando población SISBEN...")
        
        try:
            from cargar_poblacion import cargar_poblacion_postgresql
            resultado = cargar_poblacion_postgresql("data/poblacion_tolima_20250822.csv")
            
            if resultado:
                self.log("✅ Población cargada exitosamente")
                return True
            else:
                self.log("❌ Error cargando población", "ERROR")
                return False
                
        except Exception as e:
            self.log(f"❌ Error crítico población: {e}", "ERROR")
            return False
    
    def cargar_vacunacion(self):
        """Carga datos de vacunación (anónimos)"""
        self.log("💉 Cargando vacunación PAIweb...")
        
        try:
            from paiweb_postgresql import procesar_archivo_paiweb_completo
            df_resultado, exito = procesar_archivo_paiweb_completo("data/paiweb.xlsx")
            
            if exito:
                self.log("✅ Vacunación cargada exitosamente")
                return True
            else:
                self.log("❌ Error cargando vacunación", "ERROR") 
                return False
                
        except Exception as e:
            self.log(f"❌ Error crítico vacunación: {e}", "ERROR")
            return False
    
    def cargar_casos(self):
        """Carga casos de fiebre amarilla"""
        self.log("🦠 Cargando casos fiebre amarilla...")
        
        try:
            # Implementar script específico para casos
            self.log("⚠️ Carga de casos pendiente de implementar", "WARNING")
            return True  # Por ahora
            
        except Exception as e:
            self.log(f"❌ Error casos: {e}", "ERROR")
            return False
    
    def cargar_epizootias(self):
        """Carga epizootias"""
        self.log("🐒 Cargando epizootias...")
        
        try:
            # Implementar script específico para epizootias
            self.log("⚠️ Carga de epizootias pendiente de implementar", "WARNING")
            return True  # Por ahora
            
        except Exception as e:
            self.log(f"❌ Error epizootias: {e}", "ERROR")
            return False
    
    def verificar_integridad_sistema(self):
        """Verifica integridad del sistema completo"""
        self.log("🔍 Verificando integridad del sistema...")
        
        try:
            verificaciones = {
                "unidades_territoriales": "SELECT COUNT(*) FROM unidades_territoriales",
                "poblacion": "SELECT COUNT(*) FROM poblacion", 
                "vacunacion": "SELECT COUNT(*) FROM vacunacion_fiebre_amarilla",
                "casos": "SELECT COUNT(*) FROM casos_fiebre_amarilla",
                "epizootias": "SELECT COUNT(*) FROM epizootias"
            }
            
            with self.engine.connect() as conn:
                self.log("📊 Resumen de datos cargados:")
                totales = {}
                
                for tabla, query in verificaciones.items():
                    try:
                        total = conn.execute(text(query)).scalar()
                        totales[tabla] = total
                        self.log(f"   {tabla}: {total:,} registros")
                    except Exception as e:
                        totales[tabla] = 0
                        self.log(f"   {tabla}: ERROR - {e}")
                
                # Verificar vistas críticas
                try:
                    cobertura_test = pd.read_sql(text("""
                        SELECT COUNT(*) as registros FROM v_coberturas_dashboard
                    """), conn)
                    self.log(f"   Vista coberturas: {cobertura_test.iloc[0]['registros']:,} registros")
                except Exception as e:
                    self.log(f"   Vista coberturas: ERROR - {e}", "ERROR")
                
                return totales
                
        except Exception as e:
            self.log(f"❌ Error verificación integridad: {e}", "ERROR")
            return {}
    
    def generar_reporte_final(self, totales):
        """Genera reporte final de la actualización"""
        duracion = datetime.now() - self.inicio
        
        print(f"\n{'='*80}")
        print(" REPORTE FINAL ACTUALIZACIÓN SISTEMA ".center(80))
        print("=" * 80)
        
        print(f"🕐 Inicio: {self.inicio.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"🕐 Fin: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"⏱️ Duración total: {duracion.total_seconds():.1f} segundos")
        
        print(f"\n📊 DATOS CARGADOS:")
        for tabla, total in totales.items():
            print(f"   {tabla.replace('_', ' ').title()}: {total:,} registros")
        
        # Calcular población total
        if totales.get('poblacion', 0) > 0:
            try:
                with self.engine.connect() as conn:
                    poblacion_total = conn.execute(text(
                        "SELECT SUM(poblacion_total) FROM poblacion"
                    )).scalar()
                    print(f"\n👥 Población total Tolima: {poblacion_total:,} habitantes")
            except:
                pass
        
        print(f"\n🎯 PRÓXIMOS PASOS:")
        print("1. Abrir DBeaver/pgAdmin para explorar datos")
        print("2. Conectar dashboard Streamlit a PostgreSQL")
        print("3. Generar análisis epidemiológicos")
        print("4. Configurar alertas automáticas")
        
        # Guardar log completo
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_file = f"logs/actualizacion_sistema_{timestamp}.txt"
        
        try:
            os.makedirs("logs", exist_ok=True)
            with open(log_file, 'w', encoding='utf-8') as f:
                for log_entry in self.logs:
                    f.write(log_entry + "\n")
            print(f"\n📝 Log guardado: {log_file}")
        except Exception as e:
            print(f"⚠️ No se pudo guardar log: {e}")
    
    def actualizar_sistema_completo(self, modo="completo"):
        """
        Actualización completa del sistema
        Modos: completo, solo_datos, solo_verificacion
        """
        self.log("🚀 INICIANDO ACTUALIZACIÓN SISTEMA EPIDEMIOLÓGICO TOLIMA")
        self.log("=" * 60)
        
        # 1. Verificar Docker
        if not self.verificar_docker():
            self.log("❌ No se pudo conectar a PostgreSQL. Abortando.", "ERROR")
            return False
        
        # 2. Verificar archivos
        archivos_ok, encontrados, faltantes = self.verificar_archivos_entrada()
        
        if modo == "solo_verificacion":
            self.log("✅ Modo verificación completado")
            return True
        
        if not archivos_ok and modo == "completo":
            respuesta = input("⚠️ Faltan archivos. ¿Continuar con los disponibles? (y/N): ")
            if respuesta.lower() != 'y':
                self.log("❌ Actualización cancelada por usuario", "WARNING")
                return False
        
        # 3. Cargas de datos (en orden de dependencias)
        cargas_exitosas = 0
        total_cargas = 0
        
        if "Unidades Territoriales" in encontrados:
            total_cargas += 1
            if self.cargar_unidades_territoriales():
                cargas_exitosas += 1
        
        if "Población SISBEN" in encontrados:
            total_cargas += 1
            if self.cargar_poblacion():
                cargas_exitosas += 1
        
        if "Vacunación PAIweb" in encontrados:
            total_cargas += 1
            if self.cargar_vacunacion():
                cargas_exitosas += 1
        
        # Casos y epizootias (implementación futura)
        # if "Casos Fiebre Amarilla" in encontrados:
        #     total_cargas += 1
        #     if self.cargar_casos():
        #         cargas_exitosas += 1
        
        # if "Epizootias" in encontrados:
        #     total_cargas += 1
        #     if self.cargar_epizootias():
        #         cargas_exitosas += 1
        
        # 4. Verificación final
        totales = self.verificar_integridad_sistema()
        
        # 5. Reporte final
        self.generar_reporte_final(totales)
        
        # 6. Resultado
        if cargas_exitosas == total_cargas:
            self.log("🎉 ¡ACTUALIZACIÓN COMPLETADA EXITOSAMENTE!")
            return True
        else:
            self.log(f"⚠️ Actualización parcial: {cargas_exitosas}/{total_cargas} exitosas", "WARNING")
            return False


def menu_interactivo():
    """Menú interactivo para el sistema coordinador"""
    print("🎛️ SISTEMA COORDINADOR EPIDEMIOLÓGICO TOLIMA")
    print("=" * 50)
    print("1. Actualización completa del sistema")
    print("2. Solo verificar archivos y conexiones")
    print("3. Solo cargar unidades territoriales")
    print("4. Solo cargar población")
    print("5. Solo cargar vacunación")
    print("6. Verificar integridad sistema")
    print("0. Salir")
    
    while True:
        try:
            opcion = input("\n🔢 Selecciona opción: ")
            
            coordinador = SistemaCoordinadorTolima()
            
            if opcion == "1":
                coordinador.actualizar_sistema_completo("completo")
                break
            elif opcion == "2":
                coordinador.actualizar_sistema_completo("solo_verificacion")
                break
            elif opcion == "3":
                if coordinador.verificar_docker():
                    coordinador.cargar_unidades_territoriales()
                break
            elif opcion == "4":
                if coordinador.verificar_docker():
                    coordinador.cargar_poblacion()
                break
            elif opcion == "5":
                if coordinador.verificar_docker():
                    coordinador.cargar_vacunacion()
                break
            elif opcion == "6":
                if coordinador.verificar_docker():
                    coordinador.verificar_integridad_sistema()
                break
            elif opcion == "0":
                print("👋 ¡Hasta luego!")
                break
            else:
                print("❌ Opción inválida. Intenta de nuevo.")
                
        except KeyboardInterrupt:
            print("\n\n👋 Saliendo...")
            break
        except Exception as e:
            print(f"❌ Error: {e}")


# ================================
# FUNCIÓN PRINCIPAL
# ================================
if __name__ == "__main__":
    print("🎮 SISTEMA COORDINADOR TOLIMA")
    print("=" * 40)
    
    # Verificar argumentos de línea de comandos
    if len(sys.argv) > 1:
        if sys.argv[1] == "--completo":
            coordinador = SistemaCoordinadorTolima()
            coordinador.actualizar_sistema_completo("completo")
        elif sys.argv[1] == "--verificar":
            coordinador = SistemaCoordinadorTolima()
            coordinador.actualizar_sistema_completo("solo_verificacion")
        elif sys.argv[1] == "--menu":
            menu_interactivo()
        else:
            print("❌ Argumento inválido")
            print("Opciones: --completo, --verificar, --menu")
    else:
        # Menú interactivo por defecto
        menu_interactivo()