-- =====================================================
-- 02_schema_optimized.sql - Esquema Limpio Sistema Epidemiológico Tolima
-- OPTIMIZADO: Performance, índices inteligentes, constraints robustos
-- =====================================================

\echo 'Creando esquema optimizado Sistema Epidemiológico Tolima V1.0...'

-- =====================================================
-- 1. CONFIGURACIÓN INICIAL OPTIMIZADA
-- =====================================================

-- Configurar timezone para Colombia
SET timezone = 'America/Bogota';

-- Configurar memoria para operaciones grandes
SET work_mem = '256MB';
SET maintenance_work_mem = '512MB';

-- =====================================================
-- 2. TABLA MAESTRA: Unidades Territoriales (OPTIMIZADA)
-- =====================================================

DROP TABLE IF EXISTS unidades_territoriales CASCADE;

CREATE TABLE unidades_territoriales (
    id SERIAL PRIMARY KEY,
    
    -- Identificación territorial
    tipo VARCHAR(20) NOT NULL CHECK (tipo IN ('departamento', 'municipio', 'vereda', 'cabecera')),
    codigo_divipola VARCHAR(11) UNIQUE NOT NULL,
    codigo_dpto VARCHAR(2) NOT NULL DEFAULT '73',
    codigo_municipio VARCHAR(5),
    
    -- Información descriptiva
    nombre VARCHAR(100) NOT NULL,
    municipio VARCHAR(50),
    region VARCHAR(20) CHECK (region IN ('CENTRO', 'NEVADOS', 'SUR', 'SUR ORIENTE', 'NORTE', 'ORIENTE')),
    
    -- Métricas territoriales
    area_oficial_km2 DECIMAL(10,4),
    perimetro_km DECIMAL(10,2),
    
    -- Geometría optimizada
    geometria GEOMETRY(MultiPolygon, 4326),
    
    -- Control y auditoría
    activo BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Índices optimizados unidades territoriales
CREATE INDEX idx_territorios_geom ON unidades_territoriales USING GIST(geometria);
CREATE INDEX idx_territorios_divipola ON unidades_territoriales(codigo_divipola);
CREATE INDEX idx_territorios_tipo_region ON unidades_territoriales(tipo, region);
CREATE INDEX idx_territorios_municipio_lookup ON unidades_territoriales(codigo_municipio) WHERE tipo = 'municipio';

-- Constraint territorial Tolima
ALTER TABLE unidades_territoriales ADD CONSTRAINT chk_codigo_tolima 
CHECK (codigo_divipola LIKE '73%' OR tipo = 'departamento');

\echo 'Tabla unidades_territoriales optimizada creada'

-- =====================================================
-- 3. TABLA: Población SISBEN (OPTIMIZADA)
-- =====================================================

DROP TABLE IF EXISTS poblacion CASCADE;

CREATE TABLE poblacion (
    id SERIAL PRIMARY KEY,
    
    -- Identificación territorial
    codigo_municipio VARCHAR(5) NOT NULL,
    municipio VARCHAR(50) NOT NULL,
    
    -- Clasificación epidemiológica
    tipo_ubicacion VARCHAR(10) NOT NULL CHECK (tipo_ubicacion IN ('Urbano', 'Rural')),
    grupo_etario VARCHAR(20) NOT NULL CHECK (grupo_etario IN (
        '09-23 meses', '02-19 años', '20-59 años', '60+ años'
    )),
    
    -- Datos poblacionales
    poblacion_total INTEGER NOT NULL CHECK (poblacion_total > 0),
    
    -- Metadatos
    año INTEGER NOT NULL DEFAULT 2024,
    fuente VARCHAR(50) DEFAULT 'SISBEN',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Constraint única por dimensión
    UNIQUE(codigo_municipio, tipo_ubicacion, grupo_etario, año),
    
    -- Foreign key optimizada
    FOREIGN KEY (codigo_municipio) REFERENCES unidades_territoriales(codigo_divipola)
);

-- Índices optimizados población
CREATE INDEX idx_poblacion_lookup_completo ON poblacion(codigo_municipio, grupo_etario, tipo_ubicacion);
CREATE INDEX idx_poblacion_agregacion ON poblacion(tipo_ubicacion, grupo_etario) INCLUDE (poblacion_total);
CREATE INDEX idx_poblacion_temporal ON poblacion(año);

\echo 'Tabla poblacion optimizada creada'

-- =====================================================
-- 4. TABLA: Vacunación Fiebre Amarilla (ANÓNIMA + OPTIMIZADA)
-- =====================================================

DROP TABLE IF EXISTS vacunacion_fiebre_amarilla CASCADE;

CREATE TABLE vacunacion_fiebre_amarilla (
    id SERIAL PRIMARY KEY,
    
    -- Identificación territorial
    codigo_municipio VARCHAR(5) NOT NULL,
    municipio VARCHAR(50) NOT NULL,
    tipo_ubicacion VARCHAR(10) NOT NULL CHECK (tipo_ubicacion IN ('Urbano', 'Rural')),
    
    -- Institución prestadora
    institucion VARCHAR(100) NOT NULL,
    
    -- Datos temporales PRECISOS
    fecha_aplicacion DATE NOT NULL,
    año INTEGER NOT NULL,
    mes INTEGER NOT NULL,
    semana_epidemiologica INTEGER NOT NULL CHECK (semana_epidemiologica BETWEEN 1 AND 53),
    
    -- Clasificación epidemiológica (ANÓNIMA)
    grupo_etario VARCHAR(20) NOT NULL CHECK (grupo_etario IN (
        '09-23 meses', '02-19 años', '20-59 años', '60+ años'
    )),
    edad_anos DECIMAL(4,1) CHECK (edad_anos BETWEEN 0 AND 90),
    
    -- Metadatos de control
    fecha_carga TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    fuente VARCHAR(20) DEFAULT 'PAIweb',
    
    -- Constraints temporales
    CONSTRAINT chk_fecha_valida CHECK (fecha_aplicacion >= '2020-01-01' AND fecha_aplicacion <= CURRENT_DATE),
    CONSTRAINT chk_consistencia_temporal CHECK (
        año = EXTRACT(YEAR FROM fecha_aplicacion) AND
        mes = EXTRACT(MONTH FROM fecha_aplicacion)
    ),
    
    -- Foreign Key
    FOREIGN KEY (codigo_municipio) REFERENCES unidades_territoriales(codigo_divipola)
);

-- Índices CRÍTICOS para dashboard (consultas <2s)
CREATE INDEX idx_vacunacion_dashboard_principal ON vacunacion_fiebre_amarilla(
    codigo_municipio, grupo_etario, tipo_ubicacion, año, mes
);

CREATE INDEX idx_vacunacion_temporal_completo ON vacunacion_fiebre_amarilla(
    fecha_aplicacion, año, mes
) INCLUDE (codigo_municipio, grupo_etario);

CREATE INDEX idx_vacunacion_institucion_activa ON vacunacion_fiebre_amarilla(
    institucion, fecha_aplicacion
) WHERE institucion IS NOT NULL;

CREATE INDEX idx_vacunacion_ubicacion_edad ON vacunacion_fiebre_amarilla(
    tipo_ubicacion, grupo_etario
) INCLUDE (codigo_municipio);

\echo 'Tabla vacunacion_fiebre_amarilla optimizada creada'

-- =====================================================
-- 5. TABLA: Casos Fiebre Amarilla (VIGILANCIA)
-- =====================================================

DROP TABLE IF EXISTS casos_fiebre_amarilla CASCADE;

CREATE TABLE casos_fiebre_amarilla (
    id SERIAL PRIMARY KEY,
    
    -- Identificación del caso
    codigo_evento INTEGER,
    numero_documento VARCHAR(20),
    tipo_documento VARCHAR(10),
    
    -- Datos temporales críticos
    fecha_notificacion DATE NOT NULL,
    fecha_inicio_sintomas DATE,
    fecha_consulta DATE,
    semana_epidemiologica INTEGER CHECK (semana_epidemiologica BETWEEN 1 AND 53),
    año INTEGER,
    
    -- Datos personales (vigilancia epidemiológica)
    primer_nombre VARCHAR(50),
    primer_apellido VARCHAR(50),
    edad_anos INTEGER CHECK (edad_anos BETWEEN 0 AND 120),
    sexo VARCHAR(1) CHECK (sexo IN ('M', 'F')),
    grupo_etario VARCHAR(20),
    
    -- Geolocalización CORREGIDA
    municipio_procedencia VARCHAR(50),          -- DONDE SE INFECTÓ
    codigo_municipio_procedencia VARCHAR(5),    -- Código del municipio procedencia
    vereda_infeccion VARCHAR(100),              -- Vereda donde se infectó
    codigo_vereda_infeccion VARCHAR(11),        -- Código DIVIPOLA vereda
    
    municipio_residencia VARCHAR(50),           -- Donde vive
    codigo_municipio_residencia VARCHAR(5),     -- Código residencia
    
    municipio_notificacion VARCHAR(50),         -- Donde se notificó
    codigo_municipio_notificacion VARCHAR(5),   -- Código notificación
    
    -- Datos clínicos
    clasificacion_inicial VARCHAR(20),
    clasificacion_final VARCHAR(20),
    condicion_final VARCHAR(20) CHECK (condicion_final IN ('Vivo', 'Muerto')),
    fecha_defuncion DATE,
    hospitalizado BOOLEAN,
    fecha_hospitalizacion DATE,
    
    -- Antecedentes vacunación
    vacunado_previo BOOLEAN,
    fecha_vacunacion_previa DATE,
    
    -- Síntomas principales (campos críticos)
    fiebre BOOLEAN,
    ictericia BOOLEAN,
    sangrado BOOLEAN,
    
    -- Metadatos
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Constraints validación
    CONSTRAINT chk_fecha_notif_valida CHECK (fecha_notificacion >= '2020-01-01'),
    CONSTRAINT chk_fecha_coherente CHECK (
        fecha_inicio_sintomas IS NULL OR 
        fecha_notificacion >= fecha_inicio_sintomas - INTERVAL '30 days'
    ),
    
    -- Foreign Keys
    FOREIGN KEY (codigo_municipio_procedencia) REFERENCES unidades_territoriales(codigo_divipola),
    FOREIGN KEY (codigo_municipio_residencia) REFERENCES unidades_territoriales(codigo_divipola),
    FOREIGN KEY (codigo_municipio_notificacion) REFERENCES unidades_territoriales(codigo_divipola)
);

-- Índices casos epidemiológicos
CREATE INDEX idx_casos_temporal_principal ON casos_fiebre_amarilla(fecha_notificacion, año, semana_epidemiologica);
CREATE INDEX idx_casos_procedencia ON casos_fiebre_amarilla(codigo_municipio_procedencia, municipio_procedencia);
CREATE INDEX idx_casos_vereda_infeccion ON casos_fiebre_amarilla(codigo_vereda_infeccion);
CREATE INDEX idx_casos_residencia ON casos_fiebre_amarilla(codigo_municipio_residencia);
CREATE INDEX idx_casos_condicion ON casos_fiebre_amarilla(condicion_final, fecha_defuncion);

\echo 'Tabla casos_fiebre_amarilla optimizada creada'

-- =====================================================
-- 6. TABLA: Epizootias (GEOESPACIAL OPTIMIZADA)
-- =====================================================

DROP TABLE IF EXISTS epizootias CASCADE;

CREATE TABLE epizootias (
    id SERIAL PRIMARY KEY,
    
    -- Identificación territorial
    municipio VARCHAR(50) NOT NULL,
    codigo_municipio VARCHAR(5),
    vereda VARCHAR(100),
    codigo_vereda VARCHAR(11),
    
    -- Datos temporales
    fecha_recoleccion DATE,
    fecha_notificacion DATE,
    
    -- Información del evento
    informante VARCHAR(100),
    descripcion TEXT,
    especie VARCHAR(50),
    
    -- Geolocalización precisa
    latitud DECIMAL(10, 8),
    longitud DECIMAL(11, 8),
    punto_geografico GEOMETRY(Point, 4326),
    
    -- Resultados laboratorio
    fecha_envio_muestra DATE,
    resultado_pcr VARCHAR(50),
    fecha_resultado_pcr DATE,
    resultado_histopatologia VARCHAR(100),
    fecha_resultado_histopatologia DATE,
    
    -- Metadatos
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Constraints geográficos Colombia
    CONSTRAINT chk_coordenadas_colombia CHECK (
        (latitud IS NULL AND longitud IS NULL) OR
        (latitud BETWEEN -4.2 AND 12.6 AND longitud BETWEEN -81.8 AND -66.9)
    ),
    
    -- Foreign Key
    FOREIGN KEY (codigo_municipio) REFERENCES unidades_territoriales(codigo_divipola)
);

-- Índices espaciales optimizados
CREATE INDEX idx_epizootias_geom ON epizootias USING GIST(punto_geografico);
CREATE INDEX idx_epizootias_temporal ON epizootias(fecha_recoleccion, fecha_notificacion);
CREATE INDEX idx_epizootias_municipio_vereda ON epizootias(codigo_municipio, codigo_vereda);
CREATE INDEX idx_epizootias_laboratorio ON epizootias(resultado_pcr, fecha_resultado_pcr);

\echo 'Tabla epizootias optimizada creada'

-- =====================================================
-- 7. FUNCIONES OPTIMIZADAS DE UTILIDAD
-- =====================================================

-- Función para crear punto geográfico automáticamente
CREATE OR REPLACE FUNCTION crear_punto_geografico()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.latitud IS NOT NULL AND NEW.longitud IS NOT NULL THEN
        NEW.punto_geografico := ST_SetSRID(ST_MakePoint(NEW.longitud, NEW.latitud), 4326);
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Trigger para epizootias
CREATE TRIGGER tr_epizootias_punto_geografico
    BEFORE INSERT OR UPDATE ON epizootias
    FOR EACH ROW
    EXECUTE FUNCTION crear_punto_geografico();

-- Función para campos calculados vacunación
CREATE OR REPLACE FUNCTION calcular_campos_temporales_vacunacion()
RETURNS TRIGGER AS $$
BEGIN
    NEW.año := EXTRACT(YEAR FROM NEW.fecha_aplicacion);
    NEW.mes := EXTRACT(MONTH FROM NEW.fecha_aplicacion);
    NEW.semana_epidemiologica := EXTRACT(WEEK FROM NEW.fecha_aplicacion);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Trigger para vacunación
CREATE TRIGGER tr_vacunacion_campos_temporales
    BEFORE INSERT OR UPDATE ON vacunacion_fiebre_amarilla
    FOR EACH ROW
    EXECUTE FUNCTION calcular_campos_temporales_vacunacion();

-- Función para campos calculados casos
CREATE OR REPLACE FUNCTION calcular_campos_temporales_casos()
RETURNS TRIGGER AS $$
BEGIN
    NEW.año := EXTRACT(YEAR FROM NEW.fecha_notificacion);
    NEW.semana_epidemiologica := EXTRACT(WEEK FROM NEW.fecha_notificacion);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Trigger para casos
CREATE TRIGGER tr_casos_campos_temporales
    BEFORE INSERT OR UPDATE ON casos_fiebre_amarilla
    FOR EACH ROW
    EXECUTE FUNCTION calcular_campos_temporales_casos();

\echo 'Funciones y triggers optimizados creados'

-- =====================================================
-- 8. CONFIGURACIÓN DE PERFORMANCE
-- =====================================================

-- Configurar autovacuum optimizado para tablas grandes
ALTER TABLE vacunacion_fiebre_amarilla SET (
    autovacuum_vacuum_scale_factor = 0.05,
    autovacuum_analyze_scale_factor = 0.02,
    autovacuum_vacuum_cost_delay = 10
);

ALTER TABLE poblacion SET (
    autovacuum_vacuum_scale_factor = 0.1,
    autovacuum_analyze_scale_factor = 0.05
);

-- Configurar fillfactor para tablas de solo inserción
ALTER TABLE vacunacion_fiebre_amarilla SET (fillfactor = 100);
ALTER TABLE casos_fiebre_amarilla SET (fillfactor = 90);

-- Estadísticas iniciales
ANALYZE unidades_territoriales;
ANALYZE poblacion;
ANALYZE vacunacion_fiebre_amarilla;
ANALYZE casos_fiebre_amarilla;
ANALYZE epizootias;

\echo 'Configuración de performance aplicada'

-- =====================================================
-- 9. COMENTARIOS DOCUMENTACIÓN
-- =====================================================

COMMENT ON TABLE unidades_territoriales IS 'Tabla maestra territorial optimizada para Tolima';
COMMENT ON TABLE poblacion IS 'Denominadores poblacionales SISBEN agregados por dimensiones epidemiológicas';
COMMENT ON TABLE vacunacion_fiebre_amarilla IS 'Registro ANÓNIMO de vacunación fiebre amarilla optimizado para dashboard';
COMMENT ON TABLE casos_fiebre_amarilla IS 'Vigilancia epidemiológica casos fiebre amarilla con geolocalización corregida';
COMMENT ON TABLE epizootias IS 'Vigilancia epizootias con geolocalización precisa';

COMMENT ON COLUMN casos_fiebre_amarilla.municipio_procedencia IS 'CRÍTICO: Municipio donde se infectó (para mapeo veredal)';
COMMENT ON COLUMN casos_fiebre_amarilla.codigo_vereda_infeccion IS 'Código DIVIPOLA vereda donde ocurrió infección';

\echo 'Esquema optimizado Sistema Epidemiológico Tolima V2.0 creado exitosamente!'
\echo 'Performance: Índices especializados, constraints robustos, triggers automáticos'
\echo 'Listo para carga optimizada de datos masivos'