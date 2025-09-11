#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
diagnostico_sistema_completo.py - Diagnóstico y Corrección de Problemas del Sistema
ANALIZA: PostgreSQL, PostGIS, Esquema SQL, Datos
CORRIGE: Problemas detectados automáticamente
"""

import sys
import subprocess
import os
import time
from pathlib import Path
from datetime import datetime
from sqlalchemy import create_engine, text
import structlog
from typing import Dict, List, Tuple, Optional

# Configurar logging
structlog.configure(
    processors=[
        structlog.stdlib.add_log_level,
        structlog.processors.TimeStamper(fmt="ISO"),
        structlog.processors.JSONRenderer()
    ],
    logger_factory=structlog.stdlib.LoggerFactory(),
    wrapper_class=structlog.stdlib.BoundLogger,
    cache_logger_on_first_use=True,
)

logger = structlog.get_logger()

class DiagnosticoSistemaCompleto:
    """Diagnóstico completo y corrección de problemas del sistema"""
    
    def __init__(self):
        self.base_dir = Path.cwd()
        self.inicio = datetime.now()
        self.problemas_encontrados = []
        self.correcciones_aplicadas = []
        self.errores_criticos = []
        
    def log_problema(self, categoria: str, descripcion: str, critico: bool = False):
        """Registra un problema encontrado"""
        problema = {
            'categoria': categoria,
            'descripcion': descripcion,
            'critico': critico,
            'timestamp': datetime.now().isoformat()
        }
        self.problemas_encontrados.append(problema)
        
        if critico:
            self.errores_criticos.append(problema)
            logger.error("problema_critico", **problema)
        else:
            logger.warning("problema_detectado", **problema)
    
    def log_correccion(self, descripcion: str):
        """Registra una corrección aplicada"""
        correccion = {
            'descripcion': descripcion,
            'timestamp': datetime.now().isoformat()
        }
        self.correcciones_aplicadas.append(correccion)
        logger.info("correccion_aplicada", **correccion)
    
    def verificar_docker_postgres(self) -> Dict:
        """Verifica estado de Docker y PostgreSQL"""
        print("\n🐳 VERIFICANDO DOCKER Y POSTGRESQL...")
        
        try:
            # Verificar Docker
            result = subprocess.run(["docker", "--version"], 
                                  capture_output=True, text=True, timeout=10)
            if result.returncode != 0:
                self.log_problema("docker", "Docker no está instalado o no funciona", True)
                return {"docker_ok": False}
            
            # Verificar contenedor PostgreSQL
            result = subprocess.run([
                "docker", "ps", "--format", "table {{.Names}}\t{{.Status}}", 
                "--filter", "name=tolima_postgres"
            ], capture_output=True, text=True, timeout=10)
            
            postgres_running = "tolima_postgres" in result.stdout and "Up" in result.stdout
            
            if not postgres_running:
                self.log_problema("postgresql", "Contenedor PostgreSQL no está corriendo", True)
                print("❌ PostgreSQL no está corriendo")
                
                # Intentar reiniciar
                print("🔄 Intentando reiniciar PostgreSQL...")
                restart_result = subprocess.run([
                    "docker-compose", "down"
                ], capture_output=True, text=True, timeout=30)
                
                time.sleep(5)
                
                restart_result = subprocess.run([
                    "docker-compose", "up", "-d", "postgres"
                ], capture_output=True, text=True, timeout=60)
                
                if restart_result.returncode == 0:
                    self.log_correccion("PostgreSQL reiniciado")
                    print("✅ PostgreSQL reiniciado")
                    time.sleep(15)  # Esperar inicialización
                else:
                    self.log_problema("postgresql", f"Error reiniciando: {restart_result.stderr}", True)
                    return {"docker_ok": True, "postgres_ok": False}
            else:
                print("✅ PostgreSQL está corriendo")
            
            return {"docker_ok": True, "postgres_ok": True}
            
        except Exception as e:
            self.log_problema("docker", f"Error verificando Docker: {e}", True)
            return {"docker_ok": False}
    
    def verificar_conexion_bd(self) -> Optional[object]:
        """Verifica conexión a la base de datos"""
        print("\n🔌 VERIFICANDO CONEXIÓN A BASE DE DATOS...")
        
        try:
            # Intentar conexión
            engine = create_engine("postgresql://tolima_admin:tolima2025@localhost:5432/epidemiologia_tolima")
            
            with engine.connect() as conn:
                # Test básico
                conn.execute(text("SELECT 1"))
                print("✅ Conexión establecida")
                
                # Verificar versión PostgreSQL
                version = conn.execute(text("SELECT version()")).scalar()
                print(f"📋 PostgreSQL: {version.split()[1]}")
                
                return engine
                
        except Exception as e:
            self.log_problema("conexion", f"Error conectando a BD: {e}", True)
            print(f"❌ Error de conexión: {e}")
            return None
    
    def verificar_postgis(self, engine) -> bool:
        """Verifica instalación de PostGIS"""
        print("\n🗺️ VERIFICANDO POSTGIS...")
        
        try:
            with engine.connect() as conn:
                # Intentar verificar PostGIS
                try:
                    version = conn.execute(text("SELECT PostGIS_Version()")).scalar()
                    print(f"✅ PostGIS disponible: {version}")
                    return True
                except Exception:
                    self.log_problema("postgis", "PostGIS no está instalado", True)
                    print("❌ PostGIS no disponible")
                    
                    # Intentar instalar PostGIS
                    print("🔄 Intentando instalar PostGIS...")
                    try:
                        conn.execute(text("CREATE EXTENSION IF NOT EXISTS postgis"))
                        conn.commit()
                        
                        # Verificar instalación
                        version = conn.execute(text("SELECT PostGIS_Version()")).scalar()
                        print(f"✅ PostGIS instalado: {version}")
                        self.log_correccion("PostGIS instalado")
                        return True
                    except Exception as e:
                        self.log_problema("postgis", f"Error instalando PostGIS: {e}", True)
                        print(f"❌ Error instalando PostGIS: {e}")
                        return False
                        
        except Exception as e:
            self.log_problema("postgis", f"Error verificando PostGIS: {e}", True)
            return False
    
    def verificar_esquema_bd(self, engine) -> Dict:
        """Verifica esquema de base de datos"""
        print("\n📋 VERIFICANDO ESQUEMA DE BASE DE DATOS...")
        
        esquema_info = {
            'tablas_principales': {},
            'vistas': {},
            'funciones': {},
            'extensiones': {}
        }
        
        try:
            with engine.connect() as conn:
                # Verificar tablas principales
                tablas_esperadas = [
                    'unidades_territoriales',
                    'poblacion',
                    'vacunacion_fiebre_amarilla', 
                    'casos_fiebre_amarilla',
                    'epizootias'
                ]
                
                for tabla in tablas_esperadas:
                    try:
                        count = conn.execute(text(f"SELECT COUNT(*) FROM {tabla}")).scalar()
                        esquema_info['tablas_principales'][tabla] = {
                            'existe': True,
                            'registros': count
                        }
                        print(f"✅ {tabla}: {count:,} registros")
                    except Exception:
                        esquema_info['tablas_principales'][tabla] = {
                            'existe': False,
                            'registros': 0
                        }
                        print(f"❌ {tabla}: No existe")
                        self.log_problema("esquema", f"Tabla {tabla} no existe", True)
                
                # Verificar vistas principales
                vistas_esperadas = [
                    'mv_dashboard_principal',
                    'v_indicadores_tiempo_real',
                    'v_mapa_municipios',
                    'v_alertas_dashboard'
                ]
                
                for vista in vistas_esperadas:
                    try:
                        count = conn.execute(text(f"SELECT COUNT(*) FROM {vista}")).scalar()
                        esquema_info['vistas'][vista] = {
                            'existe': True,
                            'registros': count
                        }
                        print(f"✅ Vista {vista}: {count:,} registros")
                    except Exception:
                        esquema_info['vistas'][vista] = {
                            'existe': False,
                            'registros': 0
                        }
                        print(f"❌ Vista {vista}: No existe")
                        self.log_problema("vistas", f"Vista {vista} no existe", False)
                
                # Verificar extensiones
                extensiones = conn.execute(text("""
                    SELECT extname, extversion 
                    FROM pg_extension 
                    WHERE extname IN ('postgis', 'pg_trgm', 'unaccent', 'uuid-ossp')
                """)).fetchall()
                
                for ext in extensiones:
                    esquema_info['extensiones'][ext[0]] = ext[1]
                    print(f"✅ Extensión {ext[0]}: {ext[1]}")
                
                return esquema_info
                
        except Exception as e:
            self.log_problema("esquema", f"Error verificando esquema: {e}", True)
            return esquema_info
    
    def corregir_scripts_sql(self):
        """Corrige errores en scripts SQL"""
        print("\n🔧 CORRIGIENDO SCRIPTS SQL...")
        
        # Corregir 01_extensions.sql
        ext_file = self.base_dir / "sql_init" / "01_extensions.sql"
        if ext_file.exists():
            content = ext_file.read_text(encoding='utf-8')
            
            # Corregir verificación PostGIS
            new_content = content.replace(
                "SELECT PostGIS_Version();",
                """-- Crear PostGIS si no existe
CREATE EXTENSION IF NOT EXISTS postgis;

-- Verificar PostGIS
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'postgis') THEN
        RAISE NOTICE 'PostGIS Version: %', PostGIS_Version();
    ELSE
        RAISE NOTICE 'PostGIS no está disponible';
    END IF;
END $$;"""
            )
            
            ext_file.write_text(new_content, encoding='utf-8')
            self.log_correccion("Script 01_extensions.sql corregido")
            print("✅ 01_extensions.sql corregido")
    
    def corregir_vistas_sql(self):
        """Corrige errores en vistas SQL"""
        print("\n🔧 CORRIGIENDO VISTAS SQL...")
        
        views_file = self.base_dir / "sql_init" / "03_views.sql"
        if views_file.exists():
            content = views_file.read_text(encoding='utf-8')
            
            # Corregir problema ORDER BY en UNION
            content = content.replace(
                """ORDER BY 
    CASE severidad 
        WHEN 'ALTA' THEN 1 
        WHEN 'MEDIA' THEN 2 
        WHEN 'BAJA' THEN 3 
        ELSE 4 
    END,
    valor_metrica DESC;""",
                """ORDER BY 1, 7 DESC;"""  # Usar números de columna en lugar de nombres
            )
            
            # Corregir problema CAST AS decimal
            content = content.replace(
                "CAST(ROUND(COUNT(*) FILTER (WHERE c.condicion_final = 'Muerto') * 100.0 / \n                NULLIF(COUNT(*)::numeric, 0) AS decimal), 2\n            ) as letalidad_porcentaje",
                "ROUND(COUNT(*) FILTER (WHERE c.condicion_final = 'Muerto') * 100.0 / \n                NULLIF(COUNT(*), 0)::numeric, 2) as letalidad_porcentaje"
            )
            
            # Corregir funciones ROUND con CAST
            content = content.replace(
                "CAST(CAST(CAST(ROUND(AVG(area_oficial_km2)::numeric::numeric::numeric, 2) AS decimal) AS decimal) AS decimal)",
                "ROUND(AVG(area_oficial_km2)::numeric, 2)"
            )
            
            # Corregir CAST múltiples para cobertura
            content = content.replace(
                "CAST(ROUND(total_vacunados * 100.0 / poblacion_total::numeric, 2) AS decimal)",
                "ROUND(total_vacunados * 100.0 / poblacion_total::numeric, 2)"
            )
            
            views_file.write_text(content, encoding='utf-8')
            self.log_correccion("Vistas SQL corregidas")
            print("✅ Vistas SQL corregidas")
    
    def reinicializar_bd(self, engine):
        """Reinicializa completamente la base de datos"""
        print("\n🔄 REINICIALIZANDO BASE DE DATOS...")
        
        try:
            with engine.connect() as conn:
                # Eliminar todo el esquema existente
                print("🗑️ Limpiando esquema existente...")
                
                cleanup_sql = [
                    "DROP MATERIALIZED VIEW IF EXISTS mv_dashboard_principal CASCADE",
                    "DROP VIEW IF EXISTS v_indicadores_tiempo_real CASCADE",
                    "DROP VIEW IF EXISTS v_mapa_municipios CASCADE", 
                    "DROP VIEW IF EXISTS v_alertas_dashboard CASCADE",
                    "DROP VIEW IF EXISTS v_casos_dashboard CASCADE",
                    "DROP VIEW IF EXISTS v_epizootias_dashboard CASCADE",
                    "DROP TABLE IF EXISTS casos_fiebre_amarilla CASCADE",
                    "DROP TABLE IF EXISTS epizootias CASCADE",
                    "DROP TABLE IF EXISTS vacunacion_fiebre_amarilla CASCADE", 
                    "DROP TABLE IF EXISTS poblacion CASCADE",
                    "DROP TABLE IF EXISTS unidades_territoriales CASCADE"
                ]
                
                for sql in cleanup_sql:
                    try:
                        conn.execute(text(sql))
                        conn.commit()
                    except Exception:
                        pass
                
                print("✅ Esquema limpiado")
                
        except Exception as e:
            self.log_problema("reinicializacion", f"Error limpiando esquema: {e}", True)
            return False
        
        # Ejecutar scripts SQL corregidos
        print("📝 Ejecutando scripts SQL...")
        
        sql_scripts = [
            "01_extensions.sql",
            "02_schema.sql", 
            "03_views.sql"
        ]
        
        for script in sql_scripts:
            script_path = self.base_dir / "sql_init" / script
            if script_path.exists():
                try:
                    print(f"▶️ Ejecutando {script}...")
                    
                    # Leer y ejecutar script
                    with open(script_path, 'r', encoding='utf-8') as f:
                        sql_content = f.read()
                    
                    # Dividir por statements (aproximado)
                    statements = [stmt.strip() for stmt in sql_content.split(';') if stmt.strip()]
                    
                    with engine.connect() as conn:
                        for stmt in statements:
                            if stmt and not stmt.startswith('--') and not stmt.startswith('\\echo'):
                                try:
                                    conn.execute(text(stmt))
                                    conn.commit()
                                except Exception as e:
                                    if "already exists" not in str(e).lower():
                                        print(f"⚠️ Error en statement: {str(e)[:100]}")
                    
                    print(f"✅ {script} ejecutado")
                    self.log_correccion(f"Script {script} ejecutado")
                    
                except Exception as e:
                    self.log_problema("sql", f"Error ejecutando {script}: {e}", True)
                    print(f"❌ Error en {script}: {e}")
            else:
                self.log_problema("sql", f"Script {script} no encontrado", True)
        
        return True
    
    def verificar_archivos_datos(self) -> Dict:
        """Verifica archivos de datos"""
        print("\n📂 VERIFICANDO ARCHIVOS DE DATOS...")
        
        archivos_info = {}
        
        archivos_criticos = {
            'tolima_cabeceras_veredas.gpkg': 'data/tolima_cabeceras_veredas.gpkg',
            'poblacion_veredas.csv': 'data/poblacion_veredas.csv', 
            'paiweb.xlsx': 'data/paiweb.xlsx',
            'casos.xlsx': 'data/casos.xlsx',
            'epizootias.xlsx': 'data/epizootias.xlsx'
        }
        
        for nombre, ruta in archivos_criticos.items():
            archivo_path = self.base_dir / ruta
            if archivo_path.exists():
                size_mb = archivo_path.stat().st_size / (1024 * 1024)
                archivos_info[nombre] = {
                    'existe': True,
                    'size_mb': round(size_mb, 2),
                    'path': str(archivo_path)
                }
                print(f"✅ {nombre}: {size_mb:.1f} MB")
            else:
                archivos_info[nombre] = {
                    'existe': False,
                    'size_mb': 0,
                    'path': str(archivo_path)
                }
                print(f"❌ {nombre}: No encontrado")
                self.log_problema("datos", f"Archivo {nombre} no encontrado", False)
        
        return archivos_info
    
    def ejecutar_diagnostico_completo(self) -> Dict:
        """Ejecuta diagnóstico completo del sistema"""
        print("🔍 DIAGNÓSTICO COMPLETO DEL SISTEMA EPIDEMIOLÓGICO")
        print("=" * 65)
        
        resultado = {
            'timestamp': self.inicio.isoformat(),
            'docker_status': {},
            'bd_status': {},
            'esquema_status': {},
            'archivos_status': {},
            'problemas_count': 0,
            'correcciones_count': 0
        }
        
        # 1. Verificar Docker y PostgreSQL
        resultado['docker_status'] = self.verificar_docker_postgres()
        
        if not resultado['docker_status'].get('postgres_ok', False):
            print("\n❌ PostgreSQL no está funcionando. No se puede continuar.")
            return resultado
        
        # 2. Verificar conexión BD
        engine = self.verificar_conexion_bd()
        if not engine:
            print("\n❌ No se puede conectar a la base de datos.")
            return resultado
        
        resultado['bd_status']['conexion_ok'] = True
        
        # 3. Verificar PostGIS
        postgis_ok = self.verificar_postgis(engine)
        resultado['bd_status']['postgis_ok'] = postgis_ok
        
        # 4. Corregir scripts SQL si hay problemas
        if not postgis_ok or len(self.problemas_encontrados) > 0:
            self.corregir_scripts_sql()
            self.corregir_vistas_sql()
        
        # 5. Verificar esquema
        resultado['esquema_status'] = self.verificar_esquema_bd(engine)
        
        # 6. Si hay problemas críticos en esquema, reinicializar
        tablas_criticas = ['unidades_territoriales', 'poblacion', 'vacunacion_fiebre_amarilla']
        tablas_faltantes = [t for t in tablas_criticas 
                           if not resultado['esquema_status']['tablas_principales'].get(t, {}).get('existe', False)]
        
        if tablas_faltantes:
            print(f"\n⚠️ Faltan tablas críticas: {tablas_faltantes}")
            print("🔄 Iniciando reinicialización completa...")
            self.reinicializar_bd(engine)
            
            # Verificar esquema después de reinicialización
            print("\n📋 Verificando esquema después de reinicialización...")
            resultado['esquema_status'] = self.verificar_esquema_bd(engine)
        
        # 7. Verificar archivos de datos
        resultado['archivos_status'] = self.verificar_archivos_datos()
        
        # 8. Estadísticas finales
        resultado['problemas_count'] = len(self.problemas_encontrados)
        resultado['correcciones_count'] = len(self.correcciones_aplicadas)
        resultado['errores_criticos_count'] = len(self.errores_criticos)
        
        return resultado
    
    def generar_reporte_diagnostico(self, resultado: Dict) -> str:
        """Genera reporte final del diagnóstico"""
        
        duracion = datetime.now() - self.inicio
        
        reporte = f"""
🔍 DIAGNÓSTICO COMPLETO SISTEMA EPIDEMIOLÓGICO TOLIMA
{'='*70}

⏱️ RESUMEN:
   Duración: {duracion.total_seconds():.1f} segundos
   Problemas encontrados: {resultado['problemas_count']}
   Correcciones aplicadas: {resultado['correcciones_count']}
   Errores críticos: {resultado['errores_criticos_count']}

🐳 DOCKER Y POSTGRESQL:
   Docker OK: {'✅' if resultado['docker_status'].get('docker_ok') else '❌'}
   PostgreSQL OK: {'✅' if resultado['docker_status'].get('postgres_ok') else '❌'}

🔌 BASE DE DATOS:
   Conexión OK: {'✅' if resultado['bd_status'].get('conexion_ok') else '❌'}
   PostGIS OK: {'✅' if resultado['bd_status'].get('postgis_ok') else '❌'}

📋 ESQUEMA:"""
        
        # Tablas principales
        for tabla, info in resultado['esquema_status']['tablas_principales'].items():
            status = "✅" if info['existe'] else "❌"
            registros = f" ({info['registros']:,} registros)" if info['existe'] else ""
            reporte += f"\n   {status} {tabla}{registros}"
        
        # Vistas
        vistas_ok = sum(1 for v in resultado['esquema_status']['vistas'].values() if v['existe'])
        total_vistas = len(resultado['esquema_status']['vistas'])
        reporte += f"\n   📊 Vistas: {vistas_ok}/{total_vistas} funcionando"
        
        # Archivos de datos
        reporte += "\n\n📂 ARCHIVOS DE DATOS:"
        for archivo, info in resultado['archivos_status'].items():
            status = "✅" if info['existe'] else "❌"
            size_info = f" ({info['size_mb']} MB)" if info['existe'] else ""
            reporte += f"\n   {status} {archivo}{size_info}"
        
        # Problemas encontrados
        if self.problemas_encontrados:
            reporte += f"\n\n❌ PROBLEMAS ENCONTRADOS ({len(self.problemas_encontrados)}):"
            for problema in self.problemas_encontrados[:10]:  # Mostrar solo primeros 10
                critico = "🔴 CRÍTICO" if problema['critico'] else "🟡"
                reporte += f"\n   {critico} {problema['categoria']}: {problema['descripcion']}"
        
        # Correcciones aplicadas
        if self.correcciones_aplicadas:
            reporte += f"\n\n✅ CORRECCIONES APLICADAS ({len(self.correcciones_aplicadas)}):"
            for correccion in self.correcciones_aplicadas:
                reporte += f"\n   ✅ {correccion['descripcion']}"
        
        # Recomendaciones finales
        if resultado['errores_criticos_count'] == 0:
            reporte += f"""

🎉 DIAGNÓSTICO EXITOSO - SISTEMA LISTO

🚀 PRÓXIMOS PASOS:
   1. Cargar datos: python scripts/cargar_geodata.py
   2. Luego población: python scripts/cargar_poblacion.py  
   3. Luego vacunación: python scripts/cargar_vacunacion.py
   4. Verificar: python test_sistema.py

✅ Base de datos preparada y lista para recibir datos!"""
        else:
            reporte += f"""

⚠️ QUEDAN PROBLEMAS CRÍTICOS POR RESOLVER

🔧 ACCIONES REQUERIDAS:
   1. Revisar problemas críticos arriba
   2. Ejecutar: docker-compose down && docker-compose up -d
   3. Re-ejecutar diagnóstico
   4. Verificar archivos de datos en data/"""
        
        return reporte

def main():
    """Función principal de diagnóstico"""
    diagnostico = DiagnosticoSistemaCompleto()
    
    print("Iniciando diagnóstico completo del sistema...")
    print("Esto puede tomar varios minutos...\n")
    
    # Ejecutar diagnóstico
    resultado = diagnostico.ejecutar_diagnostico_completo()
    
    # Generar reporte
    reporte = diagnostico.generar_reporte_diagnostico(resultado)
    print(reporte)
    
    # Guardar reporte
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    reporte_file = Path("logs") / f"diagnostico_completo_{timestamp}.txt"
    
    try:
        Path("logs").mkdir(exist_ok=True)
        with open(reporte_file, 'w', encoding='utf-8') as f:
            f.write(reporte)
        print(f"\n📄 Reporte guardado: {reporte_file}")
    except Exception:
        pass
    
    return resultado['errores_criticos_count'] == 0

if __name__ == "__main__":
    main()