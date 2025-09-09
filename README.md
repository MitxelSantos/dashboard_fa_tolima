# 🏥 Sistema Epidemiológico Tolima - V2.0 CORREGIDO

Sistema de vigilancia epidemiológica para fiebre amarilla en el departamento del Tolima con **configuración centralizada** y **mapeo automático de códigos DIVIPOLA** desde archivos geoespaciales.

## 🔧 Correcciones Principales V2.0

### ✅ **Configuración Centralizada Optimizada**
- ❌ **Removido:** Mapeos específicos de columnas Excel de `config.py`
- ✅ **Mantenido:** Solo elementos globales (grupos etarios, códigos DIVIPOLA, funciones compartidas)
- ✅ **Cada script:** Tiene sus propios mapeos de columnas específicos

### ✅ **Cálculo de Edad Corregido**
- ❌ **Antes:** Edad calculada entre fecha nacimiento y fecha aplicación/síntomas
- ✅ **Ahora:** Edad SIEMPRE calculada entre fecha nacimiento y **fecha actual**
- 📅 **Aplicado en:** `cargar_vacunacion.py` y `cargar_casos.py`

### ✅ **Población SISBEN Mejorada**
- ✅ **Solo código DIVIPOLA:** Opción A implementada (más confiable)
- ✅ **Duplicados mejorados:** Considerando `tipo_documento` + `numero_documento`
- ✅ **Mapeos locales:** Específicos en el script, no en config global

### ✅ **Casos Fiebre Amarilla Corregidos**
- ✅ **Municipio procedencia:** Campo `nmun_proce` (donde se infectó) para mapeo veredal
- ✅ **Contexto veredal:** Vereda `vereda_` con contexto municipio procedencia
- ✅ **Edad actual:** Calculada con fecha de hoy, no fecha síntomas

### ✅ **Epizootias Optimizadas**
- ✅ **Contexto municipal:** Búsqueda veredal con municipio como contexto
- ✅ **Sin campos calculados:** Datos originales preservados
- ✅ **Mapeos locales:** Específicos del script

### ✅ **Sistema de Alertas Enfocado**
- ✅ **Solo actualización archivos:** No coberturas ni datos internos
- ✅ **Alertas diarias:** Archivos desactualizados por tipo de criticidad
- ✅ **Automatización:** Sistema programable con `schedule`

### ✅ **Setup Sistema Inteligente**
- ✅ **Verificador/Instalador:** Solo instala lo que falta
- ✅ **Verificación estructura:** Directorios, .env, Docker, dependencias
- ✅ **Modo inteligente:** Detecta y corrige solo lo necesario

### ✅ **Test Conexión Sin Datos Prueba**
- ❌ **Removido:** Generación de datos de prueba
- ✅ **Solo verificaciones:** Funciona únicamente con datos reales
- ✅ **Verificaciones robustas:** PostgreSQL, extensiones, tablas, vistas

### ✅ **Dependencias Actualizadas**
- ✅ **fpdf2==2.7.9:** Versión corregida y estable
- ✅ **Sin fpdf 2.7.4:** Versión inexistente corregida

## 📁 Estructura Corregida del Proyecto

```
📁 epidemiologia_tolima/
├── 🐳 docker-compose.yml              # PostgreSQL + PostGIS
├── 📋 requirements.txt                # Dependencias corregidas
├── ⚙️ config.py                      # 🆕 SOLO elementos globales
├── 🧪 test_conexion.py               # 🆕 Sin datos de prueba
├── 🔧 setup_sistema.py               # 🆕 Verificador inteligente
├── 📝 __init__.py                    # Módulo principal
│
├── 📊 sql_init/                      # Scripts SQL inicialización
│   ├── 01_extensions.sql
│   ├── 02_schema.sql
│   └── 03_views.sql
│
├── 🧹 scripts/                       # 🆕 SCRIPTS CORREGIDOS V2.0
│   ├── __init__.py                   # 🆕 Módulo scripts
│   ├── cargar_poblacion.py           # 🆕 Solo código DIVIPOLA, duplicados mejorados
│   ├── cargar_vacunacion.py          # 🆕 Edad con fecha actual
│   ├── cargar_casos.py               # 🆕 Municipio procedencia, edad actual
│   ├── cargar_epizootias.py          # 🆕 Contexto municipal, sin calculados
│   ├── cargar_geodata.py             # Unidades territoriales
│   ├── sistema_coordinador.py        # Coordinador maestro
│   ├── monitor_sistema.py            # Monitor avanzado
│   └── alertas_diarias.py            # 🆕 Sistema alertas automatizado
│
├── 📂 data/                          # Datos de entrada
│   ├── poblacion_veredas.csv         # CSV SISBEN sin headers
│   ├── paiweb.xlsx                   # Datos vacunación
│   ├── casos.xlsx                    # Casos fiebre amarilla
│   ├── epizootias.xlsx               # Muertes animales
│   └── tolima_cabeceras_veredas.gpkg # 🔴 OBLIGATORIO códigos DIVIPOLA
│
├── 🔄 backups/                       # Respaldos automáticos
├── 📝 logs/                          # Logs del sistema + alertas diarias
└── 📊 reportes/                      # Reportes generados
```

## 🚀 Instalación Corregida V2.0

### 1. Setup Inteligente (RECOMENDADO)
```bash
git clone <repositorio>
cd epidemiologia_tolima

# Verificador inteligente - solo instala lo que falta
python setup_sistema.py
```

### 2. Manual (Alternativo)
```bash
# Instalar dependencias corregidas
python -m pip install -r requirements.txt

# Iniciar PostgreSQL
docker-compose up -d

# Esperar inicialización
sleep 30

# Verificar sistema
python test_conexion.py
```

## 🎯 Uso del Sistema Corregido

### 1. Verificación Sistema
```bash
# Verificación completa sin datos de prueba
python test_conexion.py
```

### 2. Colocar Archivos Datos
```bash
# Copiar archivos en data/
cp tu_poblacion.csv data/poblacion_veredas.csv
cp tu_paiweb.xlsx data/paiweb.xlsx
cp tu_casos.xlsx data/casos.xlsx
cp tu_epizootias.xlsx data/epizootias.xlsx
cp tu_territorios.gpkg data/tolima_cabeceras_veredas.gpkg  # OBLIGATORIO
```

### 3. Carga Automática Completa
```bash
# Sistema coordinador maestro
python scripts/sistema_coordinador.py --completo
```

### 4. Cargas Individuales (Alternativo)
```bash
# Población con código DIVIPOLA únicamente
python scripts/cargar_poblacion.py

# Vacunación con edad calculada fecha actual
python scripts/cargar_vacunacion.py  

# Casos con municipio procedencia y edad actual
python scripts/cargar_casos.py

# Epizootias con contexto municipal
python scripts/cargar_epizootias.py
```

### 5. Sistema de Alertas Diarias
```bash
# Verificación inmediata
python scripts/alertas_diarias.py

# Programar alertas automáticas diarias
python scripts/alertas_diarias.py
# Seleccionar opción 2: "Programar alertas automáticas"
```

### 6. Monitoreo Sistema
```bash
# Monitor completo
python scripts/monitor_sistema.py --completo

# Solo alertas generales
python scripts/monitor_sistema.py --alertas
```

## 📊 Mapeos de Datos Corregidos

### Población SISBEN (CSV sin headers)
```python
# Mapeos locales en cargar_poblacion.py
MAPEO_POBLACION_SISBEN = {
    'codigo_municipio': 1,    # col_1 - Código DIVIPOLA (PRINCIPAL)
    'municipio': 2,           # col_2 - Nombre municipio  
    'tipo_documento': 16,     # col_16 - Tipo documento (CC, TI, CE, etc.)
    'documento': 17,          # col_17 - Número documento
    'fecha_nacimiento': 18    # col_18 - Para calcular edad
}
```
- ✅ **Solo código DIVIPOLA** como identificador municipal
- ✅ **Duplicados por:** `tipo_documento` + `numero_documento` 
- ✅ **Edad:** Calculada con fecha actual

### Vacunación PAIweb
```python
# Mapeos locales en cargar_vacunacion.py
MAPEO_VACUNACION_EXCEL = {
    'municipio': 'Municipio',
    'fecha_aplicacion': 'fechaaplicacion',
    'fecha_nacimiento': 'FechaNacimiento',  # Para calcular edad
    'tipo_ubicacion': 'TipoUbicación'
}
```
- ✅ **Edad calculada:** Entre `FechaNacimiento` y **fecha actual**
- ✅ **Datos anónimos:** FechaNacimiento eliminada post-cálculo

### Casos Fiebre Amarilla  
```python
# Mapeos locales en cargar_casos.py
MAPEO_CASOS_EXCEL = {
    'municipio_procedencia': 'nmun_proce',  # ← CORREGIDO: Donde se infectó
    'vereda_infeccion': 'vereda_',          # ← Vereda infección
    'fecha_nacimiento': 'fecha_nto_',       # Para calcular edad
    'municipio_residencia': 'nmun_resi',    # Donde vive
    'municipio_notificacion': 'nmun_notif'  # Donde se notificó
}
```
- ✅ **Municipio procedencia:** Campo `nmun_proce` para contexto veredal
- ✅ **Mapeo veredal:** Con contexto municipio procedencia
- ✅ **Edad actual:** Calculada con fecha de hoy

### Epizootias
```python
# Mapeos locales en cargar_epizootias.py  
MAPEO_EPIZOOTIAS_EXCEL = {
    'municipio': 'MUNICIPIO',
    'vereda': 'VEREDA',
    'latitud': 'LATITUD',
    'longitud': 'LONGITUD',
    'fecha_recoleccion': 'FECHA_RECOLECCION',
    'resultado_pcr': 'RESULTADO_PCR'
}
```
- ✅ **Contexto municipal:** Búsqueda veredal con municipio como contexto
- ✅ **Datos originales:** Sin campos calculados adicionales

## 🚨 Sistema de Alertas Diarias Automatizado

### Tipos de Alertas por Archivo

| Archivo | Criticidad | Umbral | Descripción |
|---------|------------|--------|-------------|
| Casos FA | **CRÍTICA** | 3 días | Vigilancia epidemiológica urgente |
| Vacunación | **ALTA** | 7 días | Actualización semanal esperada |
| Población | **ALTA** | 30 días | Base de denominadores (mensual) |  
| Epizootias | **MEDIA** | 14 días | Vigilancia animal (quincenal) |
| Territorios | **BAJA** | 90 días | Códigos DIVIPOLA (trimestral) |

### Configuración Alertas
```bash
# Archivo: scripts/alertas_diarias.py
python scripts/alertas_diarias.py

# Opciones:
# 1. Verificación inmediata
# 2. Programar automáticas (8:00 AM diario)
# 3. Solo verificar archivos  
# 4. Solo verificar base datos
```

### Logs de Alertas
- **Ubicación:** `logs/alertas_diarias_YYYYMMDD_HHMMSS.txt`
- **Frecuencia:** Diaria automática + manual cuando se necesite
- **Contenido:** Estado archivos + base datos + resumen ejecutivo

## 🔧 Herramientas de Verificación

### Setup Sistema (Nuevo)
```bash
python setup_sistema.py

# Opciones:
# 1. 🔍 Verificación inteligente (recomendado)
# 2. 🚀 Setup completo desde cero  
# 3. 👋 Salir
```

### Test Conexión (Corregido)
```bash
python test_conexion.py
# - NO genera datos de prueba
# - Solo verificaciones reales
# - Funciona únicamente con datos originales
```

### Monitor Sistema
```bash
# Completo con estadísticas avanzadas
python scripts/monitor_sistema.py --completo

# Solo resumen
python scripts/monitor_sistema.py --resumen

# Solo alertas generales (no archivos)
python scripts/monitor_sistema.py --alertas
```

## 📈 Análisis de Datos

### Vistas Disponibles
- `v_coberturas_dashboard` - Coberturas por municipio/grupo/ubicación
- `v_mapa_coberturas` - Datos agregados para mapas
- `v_indicadores_clave` - Indicadores departamentales  
- `v_casos_dashboard` - Casos epidemiológicos

### Consultas Ejemplo
```sql
-- Cobertura por municipio (solo código DIVIPOLA)
SELECT codigo_municipio, cobertura_porcentaje, vacunados, poblacion_total
FROM v_coberturas_dashboard
WHERE cobertura_porcentaje < 70
ORDER BY cobertura_porcentaje ASC;

-- Casos por municipio procedencia (donde se infectaron)
SELECT municipio_procedencia, COUNT(*) as casos
FROM casos_fiebre_amarilla  
WHERE municipio_procedencia IS NOT NULL
GROUP BY municipio_procedencia
ORDER BY casos DESC;
```

## 🛠️ Herramientas Disponibles

### PostgreSQL
- **Servidor:** localhost:5432
- **BD:** epidemiologia_tolima  
- **Usuario:** tolima_admin
- **Contraseña:** tolima2025!

### pgAdmin  
- **URL:** http://localhost:8080
- **Usuario:** admin@tolima.gov.co
- **Contraseña:** admin123

### Archivos de Configuración
- **Docker:** `docker-compose.yml`
- **Variables:** `.env` (generado automáticamente)
- **Dependencias:** `requirements.txt` (fpdf2 corregido)

## 📋 Resolución de Problemas

### Error: Archivo .gpkg no encontrado
```bash
# Verificar archivo obligatorio
ls -la data/tolima_cabeceras_veredas.gpkg

# Si no existe, conseguir archivo .gpkg con:
# - Campo 'tipo': municipio, vereda, cabecera
# - Campo 'codigo_divipola': Código completo  
# - Campo 'municipio': Para contexto veredal
```

### Error: Columnas no mapeadas
```bash
# Verificar estructura Excel
python -c "import pandas as pd; print(pd.read_excel('data/casos.xlsx').columns.tolist())"

# Los mapeos están en cada script individual
# Verificar MAPEO_*_EXCEL en el script correspondiente
```

### PostgreSQL no responde
```bash
# Reiniciar servicios
docker-compose down && docker-compose up -d
sleep 30

# Verificar conexión
python test_conexion.py
```

### Sistema de alertas no funciona
```bash
# Verificar archivos críticos
python scripts/alertas_diarias.py
# Seleccionar opción 3: "Solo verificar archivos"

# Ver logs de alertas
ls -la logs/alertas_diarias_*.txt
```

## 🎯 Flujo de Trabajo Recomendado

### 1. Setup Inicial
```bash
python setup_sistema.py           # Verificar/instalar componentes
python test_conexion.py          # Verificar sin datos prueba
```

### 2. Preparar Datos
```bash
# Colocar archivos en data/
# OBLIGATORIO: tolima_cabeceras_veredas.gpkg
```

### 3. Carga Completa  
```bash
python scripts/sistema_coordinador.py --completo
```

### 4. Verificación Diaria
```bash
python scripts/alertas_diarias.py    # Configurar alertas automáticas
python scripts/monitor_sistema.py --completo  # Monitor general
```

### 5. Análisis
- **DBeaver:** Para consultas SQL avanzadas
- **pgAdmin:** Para administración visual
- **Vistas:** Para datos agregados listos

## 📞 Notas Importantes V2.0

### ✅ **Correcciones Aplicadas:**
1. **Config centralizada:** Solo elementos globales, mapeos locales en scripts
2. **Edad actual:** Siempre calculada con fecha de hoy  
3. **Población optimizada:** Solo código DIVIPOLA, duplicados mejorados
4. **Casos corregidos:** Municipio procedencia, contexto veredal correcto
5. **Epizootias mejoradas:** Contexto municipal, datos originales
6. **Alertas enfocadas:** Solo actualización archivos, no datos internos
7. **Setup inteligente:** Solo instala lo necesario  
8. **Test real:** Sin datos de prueba, solo verificaciones
9. **Dependencias actualizadas:** fpdf2 versión correcta

### 🎉 **Sistema V2.0 Listo!**

Tu Sistema Epidemiológico Tolima V2.0 está completamente **CORREGIDO** con:
- ✅ Configuración centralizada optimizada
- ✅ Cálculos de edad corregidos
- ✅ Mapeos específicos localizados  
- ✅ Sistema de alertas automatizado
- ✅ Verificaciones inteligentes
- ✅ Datos procesados correctamente

**¡Vigilancia epidemiológica de Tolima corregida y lista para usar!** 🚀