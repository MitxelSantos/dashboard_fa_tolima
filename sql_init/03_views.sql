-- 03_views_optimized.sql
-- Vistas SQL OPTIMIZADAS para Dashboard Sistema Epidemiológico Tolima
-- PERFORMANCE CRÍTICO: Consultas <2s, índices optimizados, vistas materializadas

\echo 'Creando vistas optimizadas para dashboard de alto rendimiento...'

-- =====================================================
-- 1. VISTA MATERIALIZADA: Dashboard Principal (CRÍTICA)
-- =====================================================

-- Eliminar vista materializada si existe
DROP MATERIALIZED VIEW IF EXISTS mv_dashboard_principal CASCADE;

CREATE MATERIALIZED VIEW mv_dashboard_principal AS
SELECT 
    v.codigo_municipio,
    v.municipio,
    COALESCE(ut.region, 'SIN REGION') as region,
    v.tipo_ubicacion,
    v.grupo_etario,
    
    -- Métricas de vacunación
    COUNT(*) as total_vacunados,
    COUNT(DISTINCT v.institucion) as instituciones_activas,
    MIN(v.fecha_aplicacion) as primera_vacuna,
    MAX(v.fecha_aplicacion) as ultima_vacuna,
    
    -- Métricas poblacionales
    COALESCE(p.poblacion_total, 0) as poblacion_total,
    
    -- Cobertura optimizada
    CASE 
        WHEN COALESCE(p.poblacion_total, 0) > 0 
        THEN ROUND(COUNT(*) * 100.0 / p.poblacion_total, 2)
        ELSE 0 
    END as cobertura_porcentaje,
    
    -- Clasificación de cobertura
    CASE 
        WHEN COALESCE(p.poblacion_total, 0) = 0 THEN 'SIN_DATOS'
        WHEN COUNT(*) * 100.0 / p.poblacion_total >= 95 THEN 'EXCELENTE'
        WHEN COUNT(*) * 100.0 / p.poblacion_total >= 85 THEN 'BUENA'
        WHEN COUNT(*) * 100.0 / p.poblacion_total >= 70 THEN 'REGULAR'
        WHEN COUNT(*) * 100.0 / p.poblacion_total >= 50 THEN 'BAJA'
        ELSE 'CRITICA'
    END as categoria_cobertura,
    
    -- Métricas temporales
    EXTRACT(YEAR FROM v.fecha_aplicacion) as año,
    EXTRACT(MONTH FROM v.fecha_aplicacion) as mes,
    
    -- Timestamp de actualización
    NOW() as actualizado_en

FROM vacunacion_fiebre_amarilla v
LEFT JOIN unidades_territoriales ut ON v.codigo_municipio = ut.codigo_divipola
LEFT JOIN poblacion p ON (
    v.codigo_municipio = p.codigo_municipio AND
    v.grupo_etario = p.grupo_etario AND
    v.tipo_ubicacion = p.tipo_ubicacion
)
WHERE v.fecha_aplicacion IS NOT NULL
GROUP BY 
    v.codigo_municipio, v.municipio, ut.region, v.tipo_ubicacion, 
    v.grupo_etario, p.poblacion_total,
    EXTRACT(YEAR FROM v.fecha_aplicacion),
    EXTRACT(MONTH FROM v.fecha_aplicacion)
ORDER BY v.municipio, v.grupo_etario;

-- Índices optimizados para vista materializada
CREATE INDEX idx_mv_dashboard_cobertura ON mv_dashboard_principal(categoria_cobertura, region);
CREATE INDEX idx_mv_dashboard_municipio ON mv_dashboard_principal(codigo_municipio, grupo_etario);
CREATE INDEX idx_mv_dashboard_temporal ON mv_dashboard_principal(año, mes);

\echo 'Vista materializada mv_dashboard_principal creada con índices optimizados'

-- =====================================================
-- 2. VISTA RÁPIDA: Indicadores Clave Tiempo Real
-- =====================================================

CREATE OR REPLACE VIEW v_indicadores_tiempo_real AS
SELECT 
    -- Métricas principales
    (SELECT COUNT(*) FROM vacunacion_fiebre_amarilla) as total_vacunados,
    (SELECT COUNT(DISTINCT codigo_municipio) FROM vacunacion_fiebre_amarilla) as municipios_activos,
    (SELECT COUNT(DISTINCT institucion) FROM vacunacion_fiebre_amarilla) as instituciones_activas,
    
    -- Población y cobertura
    (SELECT COALESCE(SUM(poblacion_total), 0) FROM poblacion) as poblacion_total,
    ROUND(
        (SELECT COUNT(*) FROM vacunacion_fiebre_amarilla) * 100.0 / 
        NULLIF((SELECT SUM(poblacion_total) FROM poblacion), 0), 2
    ) as cobertura_general,
    
    -- Actividad reciente (últimos 30 días)
    (SELECT COUNT(*) 
     FROM vacunacion_fiebre_amarilla 
     WHERE fecha_aplicacion >= CURRENT_DATE - INTERVAL '30 days') as vacunados_ultimos_30d,
    
    -- Última actividad
    (SELECT MAX(fecha_aplicacion) FROM vacunacion_fiebre_amarilla) as ultima_vacunacion,
    
    -- Distribución urbano/rural
    (SELECT COUNT(*) FROM vacunacion_fiebre_amarilla WHERE tipo_ubicacion = 'Urbano') as vacunados_urbano,
    (SELECT COUNT(*) FROM vacunacion_fiebre_amarilla WHERE tipo_ubicacion = 'Rural') as vacunados_rural,
    
    -- Casos epidemiológicos
    (SELECT COUNT(*) FROM casos_fiebre_amarilla) as total_casos,
    (SELECT COUNT(*) FROM epizootias) as total_epizootias,
    
    -- Timestamp
    NOW() as consultado_en;

\echo 'Vista v_indicadores_tiempo_real creada'

-- =====================================================
-- 3. VISTA OPTIMIZADA: Mapa Geográfico
-- =====================================================

CREATE OR REPLACE VIEW v_mapa_optimizado AS
SELECT 
    m.codigo_municipio,
    m.municipio,
    m.region,
    m.total_vacunados,
    m.poblacion_total,
    m.cobertura_porcentaje,
    m.categoria_cobertura,
    
    -- Distribución por ubicación
    SUM(CASE WHEN m.tipo_ubicacion = 'Urbano' THEN m.total_vacunados ELSE 0 END) as vacunados_urbano,
    SUM(CASE WHEN m.tipo_ubicacion = 'Rural' THEN m.total_vacunados ELSE 0 END) as vacunados_rural,
    
    -- Distribución por grupo etario
    SUM(CASE WHEN m.grupo_etario = '09-23 meses' THEN m.total_vacunados ELSE 0 END) as vacunados_9_23m,
    SUM(CASE WHEN m.grupo_etario = '02-19 años' THEN m.total_vacunados ELSE 0 END) as vacunados_2_19a,
    SUM(CASE WHEN m.grupo_etario = '20-59 años' THEN m.total_vacunados ELSE 0 END) as vacunados_20_59a,
    SUM(CASE WHEN m.grupo_etario = '60+ años' THEN m.total_vacunados ELSE 0 END) as vacunados_60mas,
    
    -- Actividad temporal
    MIN(m.primera_vacuna) as primera_vacuna_municipio,
    MAX(m.ultima_vacuna) as ultima_vacuna_municipio,
    
    -- Geometría para mapa
    ut.geometria,
    
    -- Cobertura agregada por municipio
    ROUND(
        SUM(m.total_vacunados) * 100.0 / NULLIF(SUM(m.poblacion_total), 0), 2
    ) as cobertura_municipal_agregada

FROM mv_dashboard_principal m
LEFT JOIN unidades_territoriales ut ON m.codigo_municipio = ut.codigo_divipola
WHERE ut.tipo = 'municipio'
GROUP BY 
    m.codigo_municipio, m.municipio, m.region,
    m.total_vacunados, m.poblacion_total, m.cobertura_porcentaje, m.categoria_cobertura,
    ut.geometria
ORDER BY cobertura_municipal_agregada DESC;

\echo 'Vista v_mapa_optimizado creada'

-- =====================================================
-- 4. VISTA TEMPORAL: Tendencias Optimizada
-- =====================================================

CREATE OR REPLACE VIEW v_tendencias_optimizada AS
WITH tendencias_base AS (
    SELECT 
        año,
        mes,
        region,
        tipo_ubicacion,
        grupo_etario,
        SUM(total_vacunados) as vacunados_periodo,
        SUM(poblacion_total) as poblacion_periodo,
        COUNT(DISTINCT codigo_municipio) as municipios_activos
    FROM mv_dashboard_principal
    GROUP BY año, mes, region, tipo_ubicacion, grupo_etario
),
tendencias_con_ventana AS (
    SELECT 
        *,
        -- Promedio móvil 3 meses
        AVG(vacunados_periodo) OVER (
            PARTITION BY region, tipo_ubicacion, grupo_etario 
            ORDER BY año, mes 
            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        ) as promedio_movil_3m,
        
        -- Crecimiento mensual
        LAG(vacunados_periodo) OVER (
            PARTITION BY region, tipo_ubicacion, grupo_etario 
            ORDER BY año, mes
        ) as vacunados_mes_anterior,
        
        -- Acumulado
        SUM(vacunados_periodo) OVER (
            PARTITION BY region, tipo_ubicacion, grupo_etario 
            ORDER BY año, mes
        ) as vacunados_acumulado
    FROM tendencias_base
)
SELECT 
    *,
    -- Cobertura periodo
    ROUND(vacunados_periodo * 100.0 / NULLIF(poblacion_periodo, 0), 2) as cobertura_periodo,
    
    -- Crecimiento porcentual
    CASE 
        WHEN vacunados_mes_anterior > 0 
        THEN ROUND((vacunados_periodo - vacunados_mes_anterior) * 100.0 / vacunados_mes_anterior, 1)
        ELSE NULL 
    END as crecimiento_porcentual,
    
    -- Período formateado
    CONCAT(año, '-', LPAD(mes::text, 2, '0')) as periodo_formato

FROM tendencias_con_ventana
ORDER BY año DESC, mes DESC, region, tipo_ubicacion, grupo_etario;

\echo 'Vista v_tendencias_optimizada creada'

-- =====================================================
-- 5. VISTA ALERTAS: Dashboard de Alertas
-- =====================================================

CREATE OR REPLACE VIEW v_alertas_dashboard AS
SELECT 
    'COBERTURA_BAJA' as tipo_alerta,
    'CRÍTICA' as severidad,
    municipio,
    region,
    CONCAT('Cobertura ', cobertura_porcentaje, '% en ', municipio) as mensaje,
    codigo_municipio,
    cobertura_porcentaje as valor_metrica,
    'cobertura_critica' as categoria,
    NOW() as generada_en
FROM mv_dashboard_principal
WHERE categoria_cobertura IN ('CRITICA', 'BAJA')
  AND poblacion_total > 500  -- Solo municipios significativos

UNION ALL

SELECT 
    'INACTIVIDAD_INSTITUCIONAL' as tipo_alerta,
    'ALTA' as severidad,
    municipio,
    region,
    CONCAT('Sin actividad en ', municipio, ' últimos 30 días') as mensaje,
    codigo_municipio,
    EXTRACT(DAYS FROM NOW() - ultima_vacuna) as valor_metrica,
    'inactividad' as categoria,
    NOW() as generada_en
FROM mv_dashboard_principal
WHERE ultima_vacuna < CURRENT_DATE - INTERVAL '30 days'
  AND poblacion_total > 1000

UNION ALL

SELECT 
    'MUNICIPIO_SIN_DATOS' as tipo_alerta,
    'MEDIA' as severidad,
    ut.nombre as municipio,
    ut.region,
    CONCAT('Municipio ', ut.nombre, ' sin datos de vacunación') as mensaje,
    ut.codigo_divipola as codigo_municipio,
    0 as valor_metrica,
    'sin_datos' as categoria,
    NOW() as generada_en
FROM unidades_territoriales ut
LEFT JOIN mv_dashboard_principal m ON ut.codigo_divipola = m.codigo_municipio
WHERE ut.tipo = 'municipio' 
  AND m.codigo_municipio IS NULL

ORDER BY 
    CASE severidad 
        WHEN 'CRÍTICA' THEN 1 
        WHEN 'ALTA' THEN 2 
        WHEN 'MEDIA' THEN 3 
        ELSE 4 
    END,
    valor_metrica DESC;

\echo 'Vista v_alertas_dashboard creada'

-- =====================================================
-- 6. VISTA ANÁLISIS: Instituciones Performance
-- =====================================================

CREATE OR REPLACE VIEW v_instituciones_performance AS
SELECT 
    v.institucion,
    COUNT(*) as total_vacunas,
    COUNT(DISTINCT v.codigo_municipio) as municipios_atendidos,
    COUNT(DISTINCT CONCAT(v.año, '-', LPAD(v.mes::text, 2, '0'))) as meses_activos,
    
    -- Distribución geográfica
    STRING_AGG(DISTINCT v.municipio, ', ' ORDER BY v.municipio) as municipios_lista,
    MODE() WITHIN GROUP (ORDER BY ut.region) as region_principal,
    
    -- Distribución por grupo etario
    COUNT(*) FILTER (WHERE v.grupo_etario = '09-23 meses') as vacunas_9_23m,
    COUNT(*) FILTER (WHERE v.grupo_etario = '02-19 años') as vacunas_2_19a,
    COUNT(*) FILTER (WHERE v.grupo_etario = '20-59 años') as vacunas_20_59a,
    COUNT(*) FILTER (WHERE v.grupo_etario = '60+ años') as vacunas_60mas,
    
    -- Actividad temporal
    MIN(v.fecha_aplicacion) as inicio_actividad,
    MAX(v.fecha_aplicacion) as ultima_actividad,
    
    -- Métricas de performance
    ROUND(COUNT(*) / COUNT(DISTINCT CONCAT(v.año, '-', LPAD(v.mes::text, 2, '0')))::decimal, 1) as promedio_mensual,
    
    -- Concentración geográfica
    ROUND(COUNT(DISTINCT v.codigo_municipio) * 100.0 / 47, 1) as cobertura_territorial_pct

FROM vacunacion_fiebre_amarilla v
LEFT JOIN unidades_territoriales ut ON v.codigo_municipio = ut.codigo_divipola
WHERE v.institucion IS NOT NULL 
  AND v.institucion != ''
GROUP BY v.institucion
HAVING COUNT(*) >= 10  -- Solo instituciones con actividad significativa
ORDER BY total_vacunas DESC;

\echo 'Vista v_instituciones_performance creada'

-- =====================================================
-- 7. FUNCIONES DE ACTUALIZACIÓN AUTOMÁTICA
-- =====================================================

-- Función para refrescar vista materializada
CREATE OR REPLACE FUNCTION refresh_dashboard_views()
RETURNS void AS $$
BEGIN
    -- Refrescar vista materializada principal
    REFRESH MATERIALIZED VIEW mv_dashboard_principal;
    
    -- Log de actualización
    INSERT INTO dashboard_refresh_log (refreshed_at, records_count) 
    SELECT NOW(), COUNT(*) FROM mv_dashboard_principal
    ON CONFLICT DO NOTHING;
    
    -- Analizar estadísticas
    ANALYZE mv_dashboard_principal;
    
    RAISE NOTICE 'Dashboard views refreshed at %', NOW();
END;
$$ LANGUAGE plpgsql;

-- Tabla para log de refrescos (opcional)
CREATE TABLE IF NOT EXISTS dashboard_refresh_log (
    id SERIAL PRIMARY KEY,
    refreshed_at TIMESTAMP DEFAULT NOW(),
    records_count INTEGER,
    duration_ms INTEGER
);

\echo 'Función refresh_dashboard_views() creada'

-- =====================================================
-- 8. ÍNDICES ADICIONALES PARA PERFORMANCE
-- =====================================================

-- Índices para consultas de dashboard frecuentes
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_vacunacion_dashboard_lookup 
ON vacunacion_fiebre_amarilla(codigo_municipio, grupo_etario, tipo_ubicacion, fecha_aplicacion);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_vacunacion_institucion_activa
ON vacunacion_fiebre_amarilla(institucion, fecha_aplicacion) 
WHERE institucion IS NOT NULL;

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_poblacion_lookup_completo
ON poblacion(codigo_municipio, grupo_etario, tipo_ubicacion) 
INCLUDE (poblacion_total);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_territorios_mapa
ON unidades_territoriales(codigo_divipola, tipo, region) 
INCLUDE (nombre);

\echo 'Índices adicionales para performance creados'

-- =====================================================
-- 9. VISTA DE MONITOREO: Performance Sistema
-- =====================================================

CREATE OR REPLACE VIEW v_monitoreo_sistema AS
SELECT 
    'database_size' as metrica,
    pg_size_pretty(pg_database_size(current_database())) as valor,
    'Tamaño total base de datos' as descripcion
    
UNION ALL

SELECT 
    'table_vacunacion_size' as metrica,
    pg_size_pretty(pg_total_relation_size('vacunacion_fiebre_amarilla')) as valor,
    'Tamaño tabla vacunación' as descripcion
    
UNION ALL

SELECT 
    'table_poblacion_size' as metrica,
    pg_size_pretty(pg_total_relation_size('poblacion')) as valor,
    'Tamaño tabla población' as descripcion
    
UNION ALL

SELECT 
    'mv_dashboard_size' as metrica,
    pg_size_pretty(pg_total_relation_size('mv_dashboard_principal')) as valor,
    'Tamaño vista materializada dashboard' as descripcion
    
UNION ALL

SELECT 
    'last_vacuum_vacunacion' as metrica,
    COALESCE(last_vacuum::text, 'Nunca') as valor,
    'Último vacuum tabla vacunación' as descripcion
FROM pg_stat_user_tables 
WHERE relname = 'vacunacion_fiebre_amarilla'

UNION ALL

SELECT 
    'dashboard_refresh_needed' as metrica,
    CASE 
        WHEN EXISTS(
            SELECT 1 FROM vacunacion_fiebre_amarilla v
            LEFT JOIN mv_dashboard_principal m ON v.codigo_municipio = m.codigo_municipio
            WHERE v.created_at > m.actualizado_en OR m.codigo_municipio IS NULL
        ) THEN 'SÍ'
        ELSE 'NO'
    END as valor,
    'Vista materializada necesita actualización' as descripcion;

\echo 'Vista v_monitoreo_sistema creada'

-- =====================================================
-- 10. CONFIGURACIÓN FINAL Y ESTADÍSTICAS
-- =====================================================

-- Actualizar estadísticas de todas las tablas
ANALYZE unidades_territoriales;
ANALYZE poblacion;
ANALYZE vacunacion_fiebre_amarilla;
ANALYZE mv_dashboard_principal;

-- Configurar auto-vacuum más agresivo para tablas grandes
ALTER TABLE vacunacion_fiebre_amarilla SET (
    autovacuum_vacuum_scale_factor = 0.1,
    autovacuum_analyze_scale_factor = 0.05
);

ALTER TABLE poblacion SET (
    autovacuum_vacuum_scale_factor = 0.2,
    autovacuum_analyze_scale_factor = 0.1
);

-- Grant permisos (ajustar según necesidades)
GRANT SELECT ON ALL TABLES IN SCHEMA public TO tolima_admin;
GRANT SELECT ON mv_dashboard_principal TO tolima_admin;

\echo 'Configuración de performance aplicada'
\echo 'Vistas optimizadas creadas exitosamente!'
\echo ''
\echo '🚀 PERFORMANCE OPTIMIZADO:'
\echo '   • Vista materializada: mv_dashboard_principal (consultas <2s)'
\echo '   • Índices especializados para dashboard'
\echo '   • Vistas de tiempo real para métricas'
\echo '   • Sistema de alertas automático'
\echo '   • Monitoreo de performance integrado'
\echo ''
\echo '📊 Para actualizar dashboard: SELECT refresh_dashboard_views();'
\echo '🔍 Para monitorear: SELECT * FROM v_monitoreo_sistema;'