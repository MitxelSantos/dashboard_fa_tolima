-- 02_schema.sql
-- Esquema completo Sistema Epidemiológico Tolima

\echo 'Creando esquema de base de datos...'

-- =====================================================
-- 1. TABLA MAESTRA: Unidades Territoriales
-- =====================================================
CREATE TABLE unidades_territoriales (
    id SERIAL PRIMARY KEY,
    tipo VARCHAR(20) NOT NULL CHECK (tipo IN ('departamento', 'municipio', 'vereda', 'cabecera')),
    codigo_divipola VARCHAR(11) UNIQUE NOT NULL,
    codigo_dpto VARCHAR(2) NOT NULL DEFAULT '73',
    codigo_municipio VARCHAR(5),
    nombre VARCHAR(100) NOT NULL,
    municipio VARCHAR(50),
    region VARCHAR(20) CHECK (region IN ('CENTRO', 'NEVADOS', 'SUR', 'SUR ORIENTE', 'NORTE', 'ORIENTE', 'TODAS')),
    area_oficial_km2 DECIMAL(10,4),
    area_geometrica_km2 DECIMAL(10,4),
    perimetro_km DECIMAL(10,2),
    geometria GEOMETRY(MultiPolygon, 4326),
    activo BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Índices espaciales y funcionales
CREATE INDEX idx_unidades_territoriales_geom ON unidades_territoriales USING GIST(geometria);
CREATE INDEX idx_unidades_territoriales_divipola ON unidades_territoriales(codigo_divipola);
CREATE INDEX idx_unidades_territoriales_municipio ON unidades_territoriales(codigo_municipio);
CREATE INDEX idx_unidades_territoriales_tipo ON unidades_territoriales(tipo);
CREATE INDEX idx_unidades_territoriales_region ON unidades_territoriales(region);

\echo 'Tabla unidades_territoriales creada'

-- =====================================================
-- 2. TABLA: Población (Denominadores)
-- =====================================================
CREATE TABLE poblacion (
    id SERIAL PRIMARY KEY,
    codigo_municipio VARCHAR(5) NOT NULL,
    municipio VARCHAR(50) NOT NULL,
    tipo_ubicacion VARCHAR(10) NOT NULL CHECK (tipo_ubicacion IN ('Urbano', 'Rural')),
    grupo_etario VARCHAR(20) NOT NULL,
    poblacion_total INTEGER NOT NULL CHECK (poblacion_total >= 0),
    año INTEGER DEFAULT 2024,
    fuente VARCHAR(50) DEFAULT 'SISBEN',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Foreign key
    FOREIGN KEY (codigo_municipio) REFERENCES unidades_territoriales(codigo_divipola)
);

CREATE INDEX idx_poblacion_municipio ON poblacion(codigo_municipio);
CREATE INDEX idx_poblacion_grupo_etario ON poblacion(grupo_etario);
CREATE INDEX idx_poblacion_ubicacion ON poblacion(tipo_ubicacion);
CREATE INDEX idx_poblacion_año ON poblacion(año);

\echo 'Tabla poblacion creada'

-- =====================================================
-- 3. TABLA: Vacunación Fiebre Amarilla (ANÓNIMA)
-- =====================================================
CREATE TABLE vacunacion_fiebre_amarilla (
    id SERIAL PRIMARY KEY,
    
    -- Geolocalización
    codigo_municipio VARCHAR(5) NOT NULL,
    municipio VARCHAR(50) NOT NULL,
    tipo_ubicacion VARCHAR(10) NOT NULL CHECK (tipo_ubicacion IN ('Urbano', 'Rural')),
    
    -- Institución prestadora
    institucion VARCHAR(100) NOT NULL,
    
    -- Datos temporales PRECISOS
    fecha_aplicacion DATE NOT NULL,
    año INTEGER NOT NULL,
    mes INTEGER NOT NULL,
    semana_epidemiologica INTEGER NOT NULL,
    
    -- Datos demográficos epidemiológicos (SIN identidad personal)
    grupo_etario VARCHAR(20) NOT NULL CHECK (grupo_etario IN (
        'Menor de 9 meses', '09-23 meses', '02-19 años', '20-59 años', '60+ años'
    )),
    edad_anos INTEGER CHECK (edad_anos BETWEEN 0 AND 90),
    
    -- Metadatos de control
    fecha_carga TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    fuente VARCHAR(20) DEFAULT 'PAIweb',
    
    -- Foreign Keys
    FOREIGN KEY (codigo_municipio) REFERENCES unidades_territoriales(codigo_divipola)
);

-- Índices optimizados para consultas frecuentes
CREATE INDEX idx_vacunacion_municipio_fecha ON vacunacion_fiebre_amarilla(codigo_municipio, fecha_aplicacion);
CREATE INDEX idx_vacunacion_ubicacion_grupo ON vacunacion_fiebre_amarilla(tipo_ubicacion, grupo_etario);
CREATE INDEX idx_vacunacion_temporal ON vacunacion_fiebre_amarilla(año, mes, semana_epidemiologica);
CREATE INDEX idx_vacunacion_institucion ON vacunacion_fiebre_amarilla(institucion);

-- Índice compuesto para dashboard (consulta más frecuente)
CREATE INDEX idx_vacunacion_dashboard ON vacunacion_fiebre_amarilla(
    codigo_municipio, tipo_ubicacion, grupo_etario, año, mes
);

\echo 'Tabla vacunacion_fiebre_amarilla creada'

-- =====================================================
-- 4. TABLA: Casos Fiebre Amarilla
-- =====================================================
CREATE TABLE casos_fiebre_amarilla (
    id SERIAL PRIMARY KEY,
    codigo_evento INTEGER,
    fecha_notificacion DATE NOT NULL,
    semana_epidemiologica INTEGER,
    año INTEGER,
    codigo_prestador BIGINT,
    codigo_subred INTEGER,
    
    -- Datos personales (mantener para vigilancia epidemiológica)
    primer_nombre VARCHAR(50),
    segundo_nombre VARCHAR(50),
    primer_apellido VARCHAR(50),
    segundo_apellido VARCHAR(50),
    tipo_documento VARCHAR(10),
    numero_documento VARCHAR(20),
    edad INTEGER,
    unidad_medida_edad VARCHAR(10),
    nacionalidad VARCHAR(3),
    
    -- Geolocalización
    codigo_municipio_residencia VARCHAR(5),
    municipio_residencia VARCHAR(50),
    codigo_municipio_ocurrencia VARCHAR(5),
    municipio_ocurrencia VARCHAR(50),
    vereda_residencia VARCHAR(100),
    tipo_ubicacion_residencia VARCHAR(10),
    
    -- Datos epidemiológicos
    fecha_inicio_sintomas DATE,
    fecha_consulta DATE,
    fecha_hospitalizacion DATE,
    clasificacion_inicial VARCHAR(20),
    clasificacion_final VARCHAR(20),
    condicion_final VARCHAR(20),
    fecha_defuncion DATE,
    
    -- Laboratorio
    fecha_toma_muestra DATE,
    tipo_muestra VARCHAR(50),
    resultado_laboratorio VARCHAR(50),
    fecha_resultado DATE,
    
    -- Antecedentes
    vacuna_fiebre_amarilla BOOLEAN,
    fecha_vacunacion DATE,
    viaje_area_endemica BOOLEAN,
    municipios_visitados TEXT,
    
    -- Metadatos
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Foreign keys
    FOREIGN KEY (codigo_municipio_residencia) REFERENCES unidades_territoriales(codigo_divipola),
    FOREIGN KEY (codigo_municipio_ocurrencia) REFERENCES unidades_territoriales(codigo_divipola)
);

-- Índices importantes
CREATE INDEX idx_casos_fa_fecha_notif ON casos_fiebre_amarilla(fecha_notificacion);
CREATE INDEX idx_casos_fa_municipio_res ON casos_fiebre_amarilla(codigo_municipio_residencia);
CREATE INDEX idx_casos_fa_año ON casos_fiebre_amarilla(año);
CREATE INDEX idx_casos_fa_semana ON casos_fiebre_amarilla(semana_epidemiologica, año);

\echo 'Tabla casos_fiebre_amarilla creada'

-- =====================================================
-- 5. TABLA: Epizootias (Muertes Animales)
-- =====================================================
CREATE TABLE epizootias (
    id SERIAL PRIMARY KEY,
    municipio VARCHAR(50) NOT NULL,
    vereda VARCHAR(100),
    fecha_recoleccion DATE,
    informante VARCHAR(100),
    descripcion TEXT,
    fecha_notificacion DATE,
    especie VARCHAR(50),
    
    -- Geolocalización
    latitud DECIMAL(10, 8),
    longitud DECIMAL(11, 8),
    punto_geografico GEOMETRY(Point, 4326),
    codigo_municipio VARCHAR(5),
    
    -- Laboratorio
    fecha_envio_muestra DATE,
    resultado_pcr VARCHAR(50),
    fecha_resultado_pcr DATE,
    resultado_histopatologia VARCHAR(100),
    fecha_resultado_histopatologia DATE,
    
    -- Metadatos
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Foreign key
    FOREIGN KEY (codigo_municipio) REFERENCES unidades_territoriales(codigo_divipola)
);

-- Índices espaciales y funcionales
CREATE INDEX idx_epizootias_geom ON epizootias USING GIST(punto_geografico);
CREATE INDEX idx_epizootias_fecha_recoleccion ON epizootias(fecha_recoleccion);
CREATE INDEX idx_epizootias_municipio ON epizootias(codigo_municipio);

\echo 'Tabla epizootias creada'

-- =====================================================
-- 6. FUNCIONES DE VALIDACIÓN
-- =====================================================

-- Función para validar municipio por nombre
CREATE OR REPLACE FUNCTION validar_municipio_nombre(nombre_input TEXT) 
RETURNS VARCHAR(5) AS $$
DECLARE
    codigo_encontrado VARCHAR(5);
    nombre_limpio TEXT;
BEGIN
    -- Limpiar nombre input
    nombre_limpio := TRIM(UPPER(UNACCENT(nombre_input)));
    
    -- Buscar exacto primero
    SELECT codigo_divipola INTO codigo_encontrado 
    FROM unidades_territoriales 
    WHERE tipo = 'municipio' 
    AND UPPER(UNACCENT(nombre)) = nombre_limpio;
    
    -- Si no encuentra, buscar por similitud
    IF codigo_encontrado IS NULL THEN
        SELECT codigo_divipola INTO codigo_encontrado
        FROM unidades_territoriales 
        WHERE tipo = 'municipio'
        AND SIMILARITY(UPPER(UNACCENT(nombre)), nombre_limpio) > 0.8
        ORDER BY SIMILARITY(UPPER(UNACCENT(nombre)), nombre_limpio) DESC
        LIMIT 1;
    END IF;
    
    RETURN codigo_encontrado;
END;
$$ LANGUAGE plpgsql;

-- Función para calcular semana epidemiológica
CREATE OR REPLACE FUNCTION calcular_semana_epi(fecha_input DATE)
RETURNS INTEGER AS $$
BEGIN
    RETURN EXTRACT(WEEK FROM fecha_input);
END;
$$ LANGUAGE plpgsql;

\echo 'Funciones de validación creadas'

-- =====================================================
-- 7. TRIGGERS AUTOMÁTICOS
-- =====================================================

-- Trigger para actualizar campos calculados en vacunación
CREATE OR REPLACE FUNCTION trigger_campos_calculados_vacunacion() 
RETURNS TRIGGER AS $$
BEGIN
    -- Calcular campos temporales automáticamente
    NEW.año := EXTRACT(YEAR FROM NEW.fecha_aplicacion);
    NEW.mes := EXTRACT(MONTH FROM NEW.fecha_aplicacion);
    NEW.semana_epidemiologica := calcular_semana_epi(NEW.fecha_aplicacion);
    
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER tr_campos_calculados_vacunacion 
BEFORE INSERT OR UPDATE ON vacunacion_fiebre_amarilla 
FOR EACH ROW EXECUTE FUNCTION trigger_campos_calculados_vacunacion();

\echo 'Triggers creados'

-- =====================================================
-- 8. CONFIGURACIONES DE RENDIMIENTO
-- =====================================================

-- Configurar PostgreSQL para mejor rendimiento con datos masivos
ALTER SYSTEM SET shared_preload_libraries = 'pg_stat_statements';
ALTER SYSTEM SET max_connections = '100';
ALTER SYSTEM SET shared_buffers = '256MB';
ALTER SYSTEM SET effective_cache_size = '1GB';
ALTER SYSTEM SET maintenance_work_mem = '64MB';
ALTER SYSTEM SET checkpoint_completion_target = '0.9';
ALTER SYSTEM SET wal_buffers = '16MB';
ALTER SYSTEM SET default_statistics_target = '100';

\echo 'Configuraciones de rendimiento aplicadas'
\echo 'Esquema de base de datos creado exitosamente!'