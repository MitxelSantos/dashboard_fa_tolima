# 🏥 Sistema Epidemiológico Tolima - V2.0

Sistema de vigilancia epidemiológica para fiebre amarilla en el departamento del Tolima con **configuración centralizada** y **mapeo automático de códigos DIVIPOLA** desde archivos geoespaciales.

## 🚀 Novedades V2.0

- ✅ **Configuración centralizada** en `config.py` para todos los scripts
- ✅ **Mapeo automático de códigos DIVIPOLA** desde archivo `.gpkg`
- ✅ **Búsqueda inteligente de veredas** con contexto municipal
- ✅ **Scripts integrados** con procesamiento completo
- ✅ **Validación robusta de fechas** en múltiples formatos
- ✅ **Carga de primera hoja automática** (sin especificar nombres)
- ✅ **Anonimización completa** de datos personales
- ✅ **Sistema coordinador inteligente** para actualizaciones masivas

## 📁 Estructura del Proyecto

```
📁 epidemiologia_tolima/
├── 🐳 docker-compose.yml              # PostgreSQL + PostGIS
├── 📋 requirements.txt                # Dependencias Python
├── ⚙️ config.py                      # 🆕 CONFIGURACIÓN CENTRALIZADA
├── 🧪 test_conexion.py               # Pruebas de conexión
│
├── 📊 sql_init/                      # Scripts SQL inicialización
│   ├── 01_extensions.sql
│   ├── 02_schema.sql
│   └── 03_views.sql
│
├── 🧹 scripts/                       # 🆕 SCRIPTS ADAPTADOS V2.0
│   ├── cargar_poblacion.py           # 🆕 Integrado con poblacion.py
│   ├── cargar_vacunacion.py          # 🆕 Con FechaNacimiento
│   ├── cargar_casos.py               # 🆕 Con mapeo veredal
│   ├── cargar_epizootias.py          # 🆕 Con geolocalización
│   ├── cargar_geodata.py             # Unidades territoriales
│   ├── sistema_coordinador.py        # 🆕 Coordinador maestro
│   └── monitor_sistema.py            # 🆕 Monitor avanzado
│
├── 📂 data/                          # Datos de entrada
│   ├── poblacion_veredas.csv         # CSV SISBEN sin headers
│   ├── paiweb.xlsx                   # Datos vacunación
│   ├── casos.xlsx                    # Casos fiebre amarilla
│   ├── epizootias.xlsx               # Muertes animales
│   └── tolima_cabeceras_veredas.gpkg # 🆕 CÓDIGOS DIVIPOLA
│
├── 🗺️ dashboard/                     # Dashboard Streamlit
├── 🔄 backups/                       # Respaldos automáticos
├── 📝 logs/                          # Logs del sistema
└── 📊 reportes/                      # Reportes generados
```

## 🎯 Instalación Rápida

### 1. Clonar y Preparar Entorno
```bash
git clone <repositorio>
cd epidemiologia_tolima
python -m pip install -r requirements.txt
```

### 2. Iniciar PostgreSQL
```bash
docker-compose up -d
# Esperar ~30 segundos para inicialización completa
```

### 3. Verificar Instalación
```bash
python test_conexion.py
```

### 4. Colocar Archivos de Datos
Copiar archivos en `data/`:
- `poblacion_veredas.csv` - CSV SISBEN **sin headers**
- `paiweb.xlsx` - Datos PAIweb con columnas requeridas
- `casos.xlsx` - Casos fiebre amarilla
- `epizootias.xlsx` - Epizootias con coordenadas
- `tolima_cabeceras_veredas.gpkg` - **OBLIGATORIO** para códigos DIVIPOLA

## ⚙️ Configuración Centralizada

El archivo `config.py` centraliza toda la configuración del sistema:

### Códigos DIVIPOLA Automáticos
```python
from config import buscar_codigo_municipio, buscar_codigo_vereda

# Buscar código municipal
codigo = buscar_codigo_municipio("Ibagué")  # Retorna: "73001"

# Buscar código veredal con contexto
codigo_vereda = buscar_codigo_vereda("La Esperanza", "Ibagué")
```

### Grupos Etarios Centralizados
```python
from config import clasificar_grupo_etario, obtener_grupos_etarios_definidos

# Clasificar edad en meses
grupo = clasificar_grupo_etario(30)  # "02-19 años"

# Obtener todos los grupos
grupos = obtener_grupos_etarios_definidos()
```

### Validación de Fechas
```python
from config import limpiar_fecha_robusta

fecha = limpiar_fecha_robusta("15/01/2024")  # Maneja múltiples formatos
```

## 🔧 Scripts Principales V2.0

### Sistema Coordinador (RECOMENDADO)
```bash
# Actualización completa automática
python scripts/sistema_coordinador.py --completo

# Modo interactivo
python scripts/sistema_coordinador.py --menu

# Solo verificar archivos
python scripts/sistema_coordinador.py --verificar
```

### Scripts Individuales

#### 1. Cargar Población (Integrado)
```bash
python scripts/cargar_poblacion.py
```
- ✅ **Procesa CSV SISBEN sin headers automáticamente**
- ✅ Ejecuta lógica completa de `poblacion.py`
- ✅ Asigna códigos DIVIPOLA desde `.gpkg`
- ✅ Genera conteos agregados por municipio/ubicación/grupo etario

#### 2. Cargar Vacunación (Corregido)
```bash
python scripts/cargar_vacunacion.py
```
- ✅ **Usa FechaNacimiento para calcular edad correctamente**
- ✅ Solo columnas necesarias: `Departamento`, `Municipio`, `Institucion`, `fechaaplicacion`, `FechaNacimiento`, `TipoUbicación`
- ✅ **Datos completamente anónimos** (elimina fecha nacimiento post-cálculo)
- ✅ Asigna códigos municipales automáticamente

#### 3. Cargar Casos (Mapeo Veredal)
```bash
python scripts/cargar_casos.py
```
- ✅ **Mapeo completo de todas las columnas disponibles**
- ✅ **Búsqueda automática de códigos veredales** desde `.gpkg`
- ✅ Procesa síntomas, condiciones finales, vacunación previa
- ✅ Calcula edad desde fecha nacimiento cuando disponible

#### 4. Cargar Epizootias (Geoespacial)
```bash
python scripts/cargar_epizootias.py
```
- ✅ **Mapeo veredal con coordenadas geográficas**
- ✅ Validación de coordenadas para Colombia
- ✅ **Geometrías PostGIS automáticas**
- ✅ Procesamiento de resultados de laboratorio

#### 5. Monitor Avanzado
```bash
# Monitoreo completo
python scripts/monitor_sistema.py --completo

# Modo interactivo  
python scripts/monitor_sistema.py

# Solo alertas
python scripts/monitor_sistema.py --alertas
```

## 📊 Mapeo de Columnas

### PAIweb (Vacunación) - Solo Necesarias
| Campo BD | Columna Excel | Descripción |
|----------|---------------|-------------|
| `departamento` | `Departamento` | Departamento |
| `municipio` | `Municipio` | Municipio aplicación |
| `institucion` | `Institucion` | IPS aplicadora |
| `fecha_aplicacion` | `fechaaplicacion` | Fecha aplicación |
| `fecha_nacimiento` | `FechaNacimiento` | **Para cálculo edad** |
| `tipo_ubicacion` | `TipoUbicación` | Urbano/Rural |

### Casos Fiebre Amarilla - Completo
| Campo BD | Columna Excel | Descripción |
|----------|---------------|-------------|
| `fecha_notificacion` | `fec_not` | Fecha notificación |
| `vereda_infeccion` | `vereda_` | **Vereda donde ocurrió caso** |
| `inicio_sintomas` | `ini_sin_` | **Fecha inicio síntomas** |
| `fecha_nacimiento` | `fecha_nto_` | Para calcular edad |
| `condicion_final` | `con_fin_` | 1=Vivo, 2=Muerto |
| `carnet_vacunacion` | `carne_vacu` | 1=Sí, 2=No |
| `codigo_municipio_infeccion` | `codmuninfe` | Código DIVIPOLA municipal |

*+ 50+ columnas adicionales de síntomas, datos epidemiológicos, etc.*

### Epizootias - Geoespacial
| Campo BD | Columna Excel | Descripción |
|----------|---------------|-------------|
| `municipio` | `MUNICIPIO` | Municipio |
| `vereda` | `VEREDA` | **Vereda para mapeo DIVIPOLA** |
| `fecha_recoleccion` | `FECHA_RECOLECCION` | Fecha recolección muestra |
| `latitud` | `LATITUD` | Coordenada Y |
| `longitud` | `LONGITUD` | Coordenada X |
| `especie` | `ESPECIE` | Especie animal |
| `resultado_pcr` | `RESULTADO_PCR` | Resultado laboratorio |

### Población SISBEN - Por Índice (Sin Headers)
| Campo BD | Índice Columna | Descripción |
|----------|----------------|-------------|
| `codigo_municipio` | `col_1` | Código DIVIPOLA |
| `municipio` | `col_2` | Nombre municipio |
| `corregimiento` | `col_6` | Corregimiento |
| `vereda` | `col_8` | Vereda |
| `barrio` | `col_10` | Barrio |
| `documento` | `col_17` | Número documento |
| `fecha_nacimiento` | `col_18` | Fecha nacimiento |

## 🗺️ Códigos DIVIPOLA Automáticos

El sistema usa el archivo `.gpkg` para asignar códigos automáticamente:

### Estructura `.gpkg` Requerida
```sql
-- Campos obligatorios en tolima_cabeceras_veredas.gpkg
tipo                -- 'departamento', 'municipio', 'vereda', 'cabecera'
codigo_divipola     -- Código completo (ej: "7300101001" para vereda)
codigo_municipio    -- Código municipal (ej: "73001")
nombre              -- Nombre territorio
municipio           -- Municipio padre (para veredas)
geometria           -- Geometría MultiPolygon
```

### Búsqueda Inteligente
- **Municipios**: Búsqueda exacta + similitud fonética
- **Veredas**: Búsqueda con contexto municipal para mayor precisión
- **Normalización**: Maneja acentos, mayúsculas, espacios
- **Mapeos especiales**: "San Sebastián de Mariquita" → "Mariquita"

## 🔄 Flujo de Trabajo Recomendado

### Actualización Completa
```bash
# 1. Colocar archivos actualizados en data/
# 2. Ejecutar coordinador completo
python scripts/sistema_coordinador.py --completo

# 3. Verificar resultados
python scripts/monitor_sistema.py --completo

# 4. Conectar dashboard/análisis
```

### Actualización Parcial
```bash
# Solo población actualizada
python scripts/cargar_poblacion.py

# Solo vacunación nueva
python scripts/cargar_vacunacion.py

# Verificar integridad
python scripts/monitor_sistema.py --alertas
```

## 📈 Análisis y Consultas

### Vistas Principales Disponibles
- `v_coberturas_dashboard` - Coberturas por municipio/grupo/ubicación
- `v_mapa_coberturas` - Datos agregados para mapas
- `v_indicadores_clave` - Indicadores departamentales
- `v_casos_dashboard` - Casos epidemiológicos

### Consultas Ejemplo
```sql
-- Cobertura por municipio
SELECT municipio, cobertura_porcentaje, vacunados, poblacion_total
FROM v_coberturas_dashboard
WHERE cobertura_porcentaje < 70
ORDER BY cobertura_porcentaje ASC;

-- Casos por vereda (con código DIVIPOLA)
SELECT municipio_residencia, vereda_infeccion, codigo_divipola_vereda,
       COUNT(*) as casos
FROM casos_fiebre_amarilla
WHERE codigo_divipola_vereda IS NOT NULL
GROUP BY municipio_residencia, vereda_infeccion, codigo_divipola_vereda;
```

## 🚨 Sistema de Alertas

El monitor genera alertas automáticas:

- 🔴 **Críticas**: Municipios sin vacunación, cobertura <50%
- ⚠️ **Atención**: Datos desactualizados, instituciones inactivas  
- 📅 **Info**: Métricas temporales, actualizaciones

## 🛠️ Herramientas Disponibles

### Interfaces de Base de Datos
- **pgAdmin**: http://localhost:8080 (admin@tolima.gov.co / admin123)
- **DBeaver**: Conexión PostgreSQL recomendada
- **Conexión directa**: `postgresql://tolima_admin:tolima2025!@localhost:5432/epidemiologia_tolima`

### Monitoreo y Reportes
```bash
# Reportes HTML automáticos
python scripts/monitor_sistema.py --completo

# Logs detallados en logs/
# Backups automáticos en backups/
```

## 📋 Validaciones del Sistema

### Criterios de Exclusión Automáticos
1. ❌ Fechas nacimiento nulas/futuras
2. ❌ Edades <0 o >90 años  
3. ❌ Registros duplicados por documento
4. ❌ Municipios fuera del Tolima
5. ❌ Coordenadas fuera de Colombia

### Criterios de Inclusión
1. ✅ Solo grupos etarios definidos en CSV final
2. ✅ Códigos DIVIPOLA válidos desde `.gpkg`
3. ✅ Fechas dentro de rangos epidemiológicos
4. ✅ Datos anonimizados completamente

## 🔧 Solución de Problemas

### Error: "Archivo .gpkg no encontrado"
```bash
# Verificar que existe data/tolima_cabeceras_veredas.gpkg
ls -la data/tolima_cabeceras_veredas.gpkg

# Recargar códigos DIVIPOLA
python -c "from config import cargar_codigos_divipola_desde_gpkg; cargar_codigos_divipola_desde_gpkg(True)"
```

### Error: "No hay columns mapeadas"
```bash
# Verificar estructura Excel
python -c "import pandas as pd; print(pd.read_excel('data/casos.xlsx').columns.tolist())"

# El mapeo está en config.py - verificar nombres exactos
```

### Error: PostgreSQL conexión
```bash
# Reiniciar servicios
docker-compose down && docker-compose up -d

# Verificar logs
docker-compose logs postgres

# Probar conexión
python test_conexion.py
```

## 📞 Soporte Técnico

### Logs del Sistema
- `logs/actualizacion_sistema_*.txt` - Logs coordinador
- `logs/reporte_avanzado_*.html` - Reportes HTML
- `backups/*_backup_*.csv` - Respaldos automáticos

### Verificaciones de Integridad
```bash
# Verificar todos los componentes
python scripts/sistema_coordinador.py --verificar

# Monitor completo con alertas
python scripts/monitor_sistema.py --completo
```

---

## 🎉 ¡Sistema Listo!

Tu Sistema Epidemiológico Tolima V2.0 está configurado con:
- ✅ **Configuración centralizada** para fácil mantenimiento
- ✅ **Mapeo automático** de códigos DIVIPOLA
- ✅ **Búsqueda inteligente** de veredas y municipios
- ✅ **Validaciones robustas** de datos
- ✅ **Anonimización completa** para protección de datos
- ✅ **Sistema coordinador** para actualizaciones masivas
- ✅ **Monitor avanzado** con alertas epidemiológicas

**¡Vigilancia epidemiológica de Tolima automatizada y lista para usar!** 🚀