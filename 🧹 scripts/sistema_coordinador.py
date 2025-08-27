#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Sistema Coordinador Maestro - ACTUALIZADO
Orquesta actualizaciones completas del Sistema Epidemiológico Tolima
Usa los scripts adaptados con configuración centralizada
"""

import subprocess
import os
import sys
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine, text
import warnings

# Importar configuración centralizada
from config import (
    DATABASE_URL, FileConfig, validar_configuracion,
    cargar_codigos_divipola_desde_gpkg
)

warnings.filterwarnings('ignore')

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
    
    def verificar_configuracion_sistema(self):
        """Verifica configuración del sistema"""
        self.log("⚙️ Verificando configuración del sistema...")
        
        try:
            # Ejecutar validación de configuración
            validar_configuracion()
            
            # Cargar códigos DIVIPOLA
            codigos = cargar_codigos_divipola_desde_gpkg(forzar_recarga=True)
            if codigos:
                municipios = len(codigos['municipios'])
                veredas = len(codigos['veredas'])
                self.log(f"✅ Códigos DIVIPOLA: {municipios} municipios, {veredas} veredas")
            else:
                self.log("⚠️ No se pudieron cargar códigos DIVIPOLA", "WARNING")
            
            return True
            
        except Exception as e:
            self.log(f"❌ Error verificando configuración: {e}", "ERROR")
            return False
    
    def verificar_archivos_entrada(self):
        """Verifica que los archivos de entrada existan"""
        self.log("📂 Verificando archivos de entrada...")
        
        archivos = {
            "Unidades Territoriales": FileConfig.TERRITORIOS_FILE,
            "Población SISBEN": FileConfig.POBLACION_FILE, 
            "Vacunación PAIweb": FileConfig.PAIWEB_FILE,
            "Casos Fiebre Amarilla": FileConfig.CASOS_FILE,
            "Epizootias": FileConfig.EPIZOOTIAS_FILE
        }
        
        archivos_faltantes = []
        archivos_encontrados = []
        
        for nombre, ruta in archivos.items():
            if ruta.exists():
                tamaño = ruta.stat().st_size / (1024*1024)  # MB
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
        """Carga unidades territoriales usando script adaptado"""
        self.log("🗺️ Cargando unidades territoriales...")
        
        try:
            from cargar_geodata import cargar_unidades_territoriales_postgresql
            resultado = cargar_unidades_territoriales_postgresql(str(FileConfig.TERRITORIOS_FILE))
            
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
        """Carga población SISBEN usando script adaptado integrado"""
        self.log("👥 Cargando población SISBEN...")
        
        try:
            from cargar_poblacion import procesar_poblacion_completo
            resultado = procesar_poblacion_completo(str(FileConfig.POBLACION_FILE))
            
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
        """Carga datos de vacunación usando script adaptado"""
        self.log("💉 Cargando vacunación PAIweb...")
        
        try:
            from cargar_vacunacion import procesar_vacunacion_completo
            resultado = procesar_vacunacion_completo(str(FileConfig.PAIWEB_FILE))
            
            if resultado:
                self.log("✅ Vacunación cargada exitosamente")
                return True
            else:
                self.log("❌ Error cargando vacunación", "ERROR") 
                return False
                
        except Exception as e:
            self.log(f"❌ Error crítico vacunación: {e}", "ERROR")
            return False
    
    def cargar_casos(self):
        """Carga casos de fiebre amarilla usando script adaptado"""
        self.log("🦠 Cargando casos fiebre amarilla...")
        
        try:
            from cargar_casos import procesar_casos_completo
            resultado = procesar_casos_completo(str(FileConfig.CASOS_FILE))
            
            if resultado:
                self.log("✅ Casos cargados exitosamente")
                return True
            else:
                self.log("❌ Error cargando casos", "ERROR")
                return False
                
        except Exception as e:
            self.log(f"❌ Error crítico casos: {e}", "ERROR")
            return False
    
    def cargar_epizootias(self):
        """Carga epizootias usando script adaptado"""
        self.log("🐒 Cargando epizootias...")
        
        try:
            from cargar_epizootias import procesar_epizootias_completo
            resultado = procesar_epizootias_completo(str(FileConfig.EPIZOOTIAS_FILE))
            
            if resultado:
                self.log("✅ Epizootias cargadas exitosamente")
                return True
            else:
                self.log("❌ Error cargando epizootias", "ERROR")
                return False
                
        except Exception as e:
            self.log(f"❌ Error crítico epizootias: {e}", "ERROR")
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
                        self.log(f"   {tabla.replace('_', ' ').title()}: {total:,} registros")
                    except Exception as e:
                        totales[tabla] = 0
                        self.log(f"   {tabla.replace('_', ' ').title()}: ERROR - {e}")
                
                # Verificar vistas críticas
                vistas_criticas = [
                    "v_coberturas_dashboard",
                    "v_mapa_coberturas", 
                    "v_indicadores_clave"
                ]
                
                for vista in vistas_criticas:
                    try:
                        test_vista = conn.execute(text(f"SELECT COUNT(*) FROM {vista}")).scalar()
                        self.log(f"   Vista {vista}: {test_vista:,} registros")
                    except Exception as e:
                        self.log(f"   Vista {vista}: ERROR - {e}")
                
                return totales
                
        except Exception as e:
            self.log(f"❌ Error verificación integridad: {e}", "ERROR")
            return {}
    
    def generar_reporte_final(self, totales, cargas_exitosas, total_cargas):
        """Genera reporte final de la actualización"""
        duracion = datetime.now() - self.inicio
        
        print(f"\n{'='*80}")
        print(" REPORTE FINAL ACTUALIZACIÓN SISTEMA EPIDEMIOLÓGICO TOLIMA ".center(80))
        print("=" * 80)
        
        print(f"🕐 Inicio: {self.inicio.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"🕐 Fin: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"⏱️ Duración total: {duracion.total_seconds():.1f} segundos")
        
        # Estado general
        if cargas_exitosas == total_cargas:
            print(f"🎉 ESTADO: ACTUALIZACIÓN EXITOSA ({cargas_exitosas}/{total_cargas})")
        else:
            print(f"⚠️ ESTADO: ACTUALIZACIÓN PARCIAL ({cargas_exitosas}/{total_cargas})")
        
        print(f"\n📊 DATOS CARGADOS:")
        total_registros = 0
        for tabla, total in totales.items():
            print(f"   {tabla.replace('_', ' ').title()}: {total:,} registros")
            total_registros += total
        
        print(f"   📈 TOTAL GENERAL: {total_registros:,} registros")
        
        # Calcular estadísticas epidemiológicas si hay datos
        if totales.get('poblacion', 0) > 0 and totales.get('vacunacion', 0) > 0:
            try:
                with self.engine.connect() as conn:
                    # Población total
                    poblacion_total = conn.execute(text(
                        "SELECT SUM(poblacion_total) FROM poblacion"
                    )).scalar()
                    
                    # Cobertura aproximada
                    vacunados_total = totales.get('vacunacion', 0)
                    cobertura_aprox = (vacunados_total / poblacion_total * 100) if poblacion_total > 0 else 0
                    
                    print(f"\n📊 INDICADORES EPIDEMIOLÓGICOS:")
                    print(f"   👥 Población total Tolima: {poblacion_total:,} habitantes")
                    print(f"   💉 Total vacunados: {vacunados_total:,}")
                    print(f"   📈 Cobertura aproximada: {cobertura_aprox:.1f}%")
                    
                    if totales.get('casos', 0) > 0:
                        print(f"   🦠 Casos registrados: {totales['casos']:,}")
                        
                    if totales.get('epizootias', 0) > 0:
                        print(f"   🐒 Epizootias registradas: {totales['epizootias']:,}")
            except:
                pass
        
        print(f"\n🎯 SISTEMA LISTO PARA:")
        print("   1. 📊 Dashboard epidemiológico en tiempo real")
        print("   2. 🗺️ Análisis geoespaciales avanzados")
        print("   3. 📈 Cálculo de coberturas por municipio/grupo etario")
        print("   4. 🚨 Generación de alertas epidemiológicas")
        print("   5. 📋 Reportes automatizados")
        
        print(f"\n🔗 HERRAMIENTAS DISPONIBLES:")
        print("   • DBeaver: Análisis de datos SQL")
        print("   • pgAdmin: http://localhost:8080")
        print("   • Scripts de monitoreo en /scripts/")
        
        # Guardar log completo
        self.guardar_log_completo()
    
    def guardar_log_completo(self):
        """Guarda log completo de la actualización"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_file = FileConfig.LOGS_DIR / f"actualizacion_sistema_{timestamp}.txt"
        
        try:
            FileConfig.create_directories()
            with open(log_file, 'w', encoding='utf-8') as f:
                f.write("SISTEMA EPIDEMIOLÓGICO TOLIMA - LOG ACTUALIZACIÓN\n")
                f.write("=" * 60 + "\n\n")
                f.write(f"Fecha: {self.inicio.strftime('%Y-%m-%d %H:%M:%S')}\n")
                f.write(f"Duración: {(datetime.now() - self.inicio).total_seconds():.1f}s\n\n")
                
                for log_entry in self.logs:
                    f.write(log_entry + "\n")
            
            self.log(f"📝 Log completo guardado: {log_file}")
        except Exception as e:
            self.log(f"⚠️ No se pudo guardar log: {e}")
    
    def actualizar_sistema_completo(self, modo="completo"):
        """
        Actualización completa del sistema epidemiológico
        Modos: completo, solo_datos, solo_verificacion, rapido
        """
        self.log("🚀 INICIANDO ACTUALIZACIÓN SISTEMA EPIDEMIOLÓGICO TOLIMA")
        self.log("=" * 70)
        self.log(f"Modo: {modo.upper()}")
        
        # 1. Verificar Docker y configuración
        if not self.verificar_docker():
            self.log("❌ No se pudo conectar a PostgreSQL. Abortando.", "ERROR")
            return False
        
        if not self.verificar_configuracion_sistema():
            self.log("❌ Error en configuración del sistema. Abortando.", "ERROR")
            return False
        
        # 2. Verificar archivos
        archivos_ok, encontrados, faltantes = self.verificar_archivos_entrada()
        
        if modo == "solo_verificacion":
            self.log("✅ Modo verificación completado")
            return True
        
        if not archivos_ok and modo == "completo":
            respuesta = input("⚠️ Faltan archivos. ¿Continuar con los disponibles? (y/N): ")
            if respuesta.lower() not in ['y', 'yes', 'si', 'sí']:
                self.log("❌ Actualización cancelada por usuario", "WARNING")
                return False
        
        # 3. Cargas de datos (en orden de dependencias)
        cargas_exitosas = 0
        total_cargas = 0
        
        # Definir orden de carga por dependencias
        cargas_programadas = []
        
        if "Unidades Territoriales" in encontrados:
            cargas_programadas.append(("Unidades Territoriales", self.cargar_unidades_territoriales))
        
        if "Población SISBEN" in encontrados:
            cargas_programadas.append(("Población SISBEN", self.cargar_poblacion))
        
        if "Vacunación PAIweb" in encontrados:
            cargas_programadas.append(("Vacunación PAIweb", self.cargar_vacunacion))
        
        if "Casos Fiebre Amarilla" in encontrados:
            cargas_programadas.append(("Casos Fiebre Amarilla", self.cargar_casos))
        
        if "Epizootias" in encontrados:
            cargas_programadas.append(("Epizootias", self.cargar_epizootias))
        
        # Ejecutar cargas
        total_cargas = len(cargas_programadas)
        self.log(f"📋 Cargas programadas: {total_cargas}")
        
        for i, (nombre, funcion_carga) in enumerate(cargas_programadas, 1):
            self.log(f"📊 Carga {i}/{total_cargas}: {nombre}")
            
            if funcion_carga():
                cargas_exitosas += 1
                self.log(f"✅ {nombre} completado exitosamente")
            else:
                self.log(f"❌ Error en {nombre}", "ERROR")
                
                if modo == "completo":
                    continuar = input(f"⚠️ ¿Continuar con el resto de cargas? (y/N): ")
                    if continuar.lower() not in ['y', 'yes', 'si', 'sí']:
                        break
        
        # 4. Verificación final de integridad
        totales = self.verificar_integridad_sistema()
        
        # 5. Reporte final completo
        self.generar_reporte_final(totales, cargas_exitosas, total_cargas)
        
        # 6. Resultado final
        if cargas_exitosas == total_cargas:
            self.log("🎉 ¡ACTUALIZACIÓN COMPLETADA EXITOSAMENTE!")
            return True
        else:
            self.log(f"⚠️ Actualización parcial: {cargas_exitosas}/{total_cargas} exitosas", "WARNING")
            return cargas_exitosas > 0  # True si al menos una carga fue exitosa

def menu_interactivo():
    """Menú interactivo para el sistema coordinador"""
    print("🎛️ SISTEMA COORDINADOR EPIDEMIOLÓGICO TOLIMA - V2.0")
    print("=" * 60)
    print("1. 🚀 Actualización completa del sistema")
    print("2. 🔍 Solo verificar archivos y conexiones")
    print("3. ⚡ Actualización rápida (sin confirmaciones)")
    print("4. 🗺️ Solo cargar unidades territoriales")
    print("5. 👥 Solo cargar población")
    print("6. 💉 Solo cargar vacunación")
    print("7. 🦠 Solo cargar casos")
    print("8. 🐒 Solo cargar epizootias")
    print("9. 🔧 Verificar integridad sistema")
    print("0. 👋 Salir")
    
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
                coordinador.actualizar_sistema_completo("rapido")
                break
            elif opcion == "4":
                if coordinador.verificar_docker() and coordinador.verificar_configuracion_sistema():
                    coordinador.cargar_unidades_territoriales()
                break
            elif opcion == "5":
                if coordinador.verificar_docker() and coordinador.verificar_configuracion_sistema():
                    coordinador.cargar_poblacion()
                break
            elif opcion == "6":
                if coordinador.verificar_docker() and coordinador.verificar_configuracion_sistema():
                    coordinador.cargar_vacunacion()
                break
            elif opcion == "7":
                if coordinador.verificar_docker() and coordinador.verificar_configuracion_sistema():
                    coordinador.cargar_casos()
                break
            elif opcion == "8":
                if coordinador.verificar_docker() and coordinador.verificar_configuracion_sistema():
                    coordinador.cargar_epizootias()
                break
            elif opcion == "9":
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
    print("🎮 SISTEMA COORDINADOR TOLIMA - V2.0")
    print("=" * 45)
    
    # Verificar argumentos de línea de comandos
    if len(sys.argv) > 1:
        coordinador = SistemaCoordinadorTolima()
        
        if sys.argv[1] == "--completo":
            coordinador.actualizar_sistema_completo("completo")
        elif sys.argv[1] == "--rapido":
            coordinador.actualizar_sistema_completo("rapido")
        elif sys.argv[1] == "--verificar":
            coordinador.actualizar_sistema_completo("solo_verificacion")
        elif sys.argv[1] == "--menu":
            menu_interactivo()
        else:
            print("❌ Argumento inválido")
            print("Opciones: --completo, --rapido, --verificar, --menu")
    else:
        # Menú interactivo por defecto
        menu_interactivo()