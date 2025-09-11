-- =====================================================
-- 03_views_optimized.sql - Vistas SQL OPTIMIZADAS Dashboard
-- CRÍTICO: Consultas <2s, índices especializados, vistas materializadas
-- Pre-procesamiento local → PostgreSQL → Dashboard Streamlit Cloud
-- =====================================================

\echo 'Creando vistas optimizadas para dashboard de alto rendimiento...'

-- =====================================================
-- 1. VISTA MATERIALIZADA PRINCIPAL - Dashboard Core (CRÍTICA)
-- =====================================================

-- Eliminar vista materializada si existe
DROP MATERIALIZED VIEW IF EXISTS mv_dashboard_principal CASCADE;

CREATE MATERIALIZED VIEW mv_dashboard_principal AS
WITH coberturas_base AS (
    SELECT 
        v.codigo_municipio,
        v.municipio,
        COALESCE(ut.region, 'SIN_REGION') as region,
        v.tipo_ubicacion,
        v.grupo_etario,
        v.año,
        v.mes,
        
        -- Métricas vacunación
        COUNT(*) as total_vacunados,
        COUNT(DISTINCT v.institucion) as instituciones_activas,
        MIN(v.fecha_aplicacion) as primera_vacuna,
        MAX(v.fecha_aplicacion) as ultima_vacuna,
        
        -- Población objetivo
        COALESCE(p.poblacion_total, 0) as poblacion_total
        
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
        v.grupo_etario, v.año, v.mes, p.poblacion_total
)
SELECT 
    *,
    -- Cobertura optimizada
    CASE 
        WHEN poblacion_total > 0 
        THEN CAST(ROUND(total_vacunados * 100.0 / poblacion_total::numeric, 2) AS decimal)
        ELSE 0 
    END as cobertura_porcentaje,
    
    -- Clasificación cobertura para dashboard
    CASE 
        WHEN poblacion_total = 0 THEN 'SIN_DATOS'
        WHEN total_vacunados * 100.0 / poblacion_total >= 95 THEN 'EXCELENTE'
        WHEN total_vacunados * 100.0 / poblacion_total >= 85 THEN 'BUENA'
        WHEN total_vacunados * 100.0 / poblacion_total >= 70 THEN 'REGULAR'
        WHEN total_vacunados * 100.0 / poblacion_total >= 50 THEN 'BAJA'
        ELSE 'CRITICA'
    END as categoria_cobertura,
    
    -- Timestamp actualización
    NOW() as actualizado_en

FROM coberturas_base
ORDER BY municipio, grupo_etario, año DESC, mes DESC;

-- Índices CRÍTICOS para vista materializada (performance <2s)
CREATE INDEX idx_mv_dashboard_lookup ON mv_dashboard_principal(codigo_municipio, grupo_etario, tipo_ubicacion);
CREATE INDEX idx_mv_dashboard_cobertura ON mv_dashboard_principal(categoria_cobertura, region);
CREATE INDEX idx_mv_dashboard_temporal ON mv_dashboard_principal(año, mes);
CREATE INDEX idx_mv_dashboard_municipio ON mv_dashboard_principal(municipio);

\echo 'Vista materializada mv_dashboard_principal creada con índices críticos'

-- =====================================================
-- 2. VISTA TIEMPO REAL - Indicadores Clave (RÁPIDA)
-- =====================================================

CREATE OR REPLACE VIEW v_indicadores_tiempo_real AS
WITH metricas_base AS (
    SELECT 
        COUNT(*) as total_vacunados,
        COUNT(DISTINCT codigo_municipio) as municipios_activos,
        COUNT(DISTINCT institucion) as instituciones_activas,
        MAX(fecha_aplicacion) as ultima_vacunacion,
        COUNT(*) FILTER (WHERE fecha_aplicacion >= CURRENT_DATE - INTERVAL '30 days') as vacunados_30d
    FROM vacunacion_fiebre_amarilla
),
poblacion_base AS (
    SELECT COALESCE(SUM(poblacion_total), 0) as poblacion_total
    FROM poblacion
),
otros_datos AS (
    SELECT 
        (SELECT COUNT(*) FROM casos_fiebre_amarilla) as total_casos,
        (SELECT COUNT(*) FROM epizootias) as total_epizootias,
        (SELECT COUNT(*) FROM unidades_territoriales WHERE tipo = 'municipio') as municipios_totales
)
SELECT 
    -- Métricas principales
    m.total_vacunados,
    m.municipios_activos,
    m.instituciones_activas,
    m.ultima_vacunacion,
    m.vacunados_30d,
    
    -- Población y cobertura
    p.poblacion_total,
    CAST(ROUND(CASE 
            WHEN p.poblacion_total > 0 
            THEN m.total_vacunados * 100.0 / p.poblacion_total 
            ELSE 0 
        END::numeric, 2) AS decimal) as cobertura_general,
    
    -- Otros indicadores
    o.total_casos,
    o.total_epizootias,
    o.municipios_totales,
    
    -- Distribución urbano/rural
    (SELECT COUNT(*) FROM vacunacion_fiebre_amarilla WHERE tipo_ubicacion = 'Urbano') as vacunados_urbano,
    (SELECT COUNT(*) FROM vacunacion_fiebre_amarilla WHERE tipo_ubicacion = 'Rural') as vacunados_rural,
    
    -- Timestamp
    NOW() as consultado_en
    
FROM metricas_base m
CROSS JOIN poblacion_base p
CROSS JOIN otros_datos o;

\echo 'Vista v_indicadores_tiempo_real creada'

-- =====================================================
-- 3. VISTA MAPA - Datos Agregados por Municipio (OPTIMIZADA)
-- =====================================================

CREATE OR REPLACE VIEW v_mapa_municipios AS
SELECT 
    m.codigo_municipio,
    m.municipio,
    m.region,
    
    -- Agregaciones por municipio
    SUM(m.total_vacunados) as total_vacunados,
    SUM(m.poblacion_total) as poblacion_total,
    COUNT(DISTINCT m.grupo_etario) as grupos_etarios,
    COUNT(DISTINCT m.tipo_ubicacion) as tipos_ubicacion,
    
    -- Cobertura municipal agregada
    CAST(ROUND(CASE 
            WHEN SUM(m.poblacion_total) > 0 
            THEN SUM(m.total_vacunados) * 100.0 / SUM(m.poblacion_total)
            ELSE 0 
        END::numeric, 2) AS decimal) as cobertura_municipal,
    
    -- Distribución urbano/rural
    SUM(CASE WHEN m.tipo_ubicacion = 'Urbano' THEN m.total_vacunados ELSE 0 END) as vacunados_urbano,
    SUM(CASE WHEN m.tipo_ubicacion = 'Rural' THEN m.total_vacunados ELSE 0 END) as vacunados_rural,
    
    -- Actividad temporal
    MIN(m.primera_vacuna) as primera_vacuna_municipal,
    MAX(m.ultima_vacuna) as ultima_vacuna_municipal,
    
    -- Geometría para mapa (JOIN con unidades territoriales)
    ut.geometria,
    
    -- Clasificación para colores en mapa
    CASE 
        WHEN SUM(m.poblacion_total) = 0 THEN 'SIN_DATOS'
        WHEN SUM(m.total_vacunados) * 100.0 / SUM(m.poblacion_total) >= 95 THEN 'EXCELENTE'
        WHEN SUM(m.total_vacunados) * 100.0 / SUM(m.poblacion_total) >= 85 THEN 'BUENA'
        WHEN SUM(m.total_vacunados) * 100.0 / SUM(m.poblacion_total) >= 70 THEN 'REGULAR'
        WHEN SUM(m.total_vacunados) * 100.0 / SUM(m.poblacion_total) >= 50 THEN 'BAJA'
        ELSE 'CRITICA'
    END as categoria_cobertura

FROM mv_dashboard_principal m
LEFT JOIN unidades_territoriales ut ON m.codigo_municipio = ut.codigo_divipola
WHERE ut.tipo = 'municipio'
GROUP BY 
    m.codigo_municipio, m.municipio, m.region, ut.geometria
ORDER BY cobertura_municipal DESC;

\echo 'Vista v_mapa_municipios creada'

-- =====================================================
-- 4. VISTA TENDENCIAS - Análisis Temporal (OPTIMIZADA)
-- =====================================================

CREATE OR REPLACE VIEW v_tendencias_temporales AS
WITH tendencias_mensual AS (
    SELECT 
        año,
        mes,
        region,
        tipo_ubicacion,
        grupo_etario,
        SUM(total_vacunados) as vacunados_mes,
        SUM(poblacion_total) as poblacion_mes,
        COUNT(DISTINCT codigo_municipio) as municipios_activos_mes
    FROM mv_dashboard_principal
    GROUP BY año, mes, region, tipo_ubicacion, grupo_etario
),
tendencias_con_calculo AS (
    SELECT 
        *,
        -- Cobertura mensual
        CAST(ROUND(CASE 
                WHEN poblacion_mes > 0 
                THEN vacunados_mes * 100.0 / poblacion_mes 
                ELSE 0 
            END::numeric, 2) AS decimal) as cobertura_mes,
        
        -- Promedio móvil 3 meses
        AVG(vacunados_mes) OVER (
            PARTITION BY region, tipo_ubicacion, grupo_etario 
            ORDER BY año, mes 
            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        ) as promedio_movil_3m,
        
        -- Vacunados mes anterior
        LAG(vacunados_mes) OVER (
            PARTITION BY region, tipo_ubicacion, grupo_etario 
            ORDER BY año, mes
        ) as vacunados_mes_anterior,
        
        -- Acumulado
        SUM(vacunados_mes) OVER (
            PARTITION BY region, tipo_ubicacion, grupo_etario 
            ORDER BY año, mes
        ) as vacunados_acumulado
    FROM tendencias_mensual
)
SELECT 
    *,
    -- Crecimiento mensual
    CASE 
        WHEN vacunados_mes_anterior > 0 
        THEN CAST(ROUND((vacunados_mes - vacunados_mes_anterior) * 100.0 / vacunados_mes_anterior::numeric, 1) AS decimal)
        ELSE NULL 
    END as crecimiento_porcentual,
    
    -- Período formateado para dashboard
    CONCAT(año, '-', LPAD(mes::text, 2, '0')) as periodo_texto,
    
    -- Promedio móvil redondeado
    CAST(ROUND(promedio_movil_3m::numeric, 0) AS decimal) as promedio_movil_3m_redondeado

FROM tendencias_con_calculo
ORDER BY año DESC, mes DESC, region, tipo_ubicacion, grupo_etario;

\echo 'Vista v_tendencias_temporales creada'

-- =====================================================
-- 5. VISTA ALERTAS - Sistema de Alertas Automático
-- =====================================================

CREATE OR REPLACE VIEW v_alertas_dashboard AS
-- Alertas cobertura crítica
SELECT 
    'COBERTURA_CRITICA' as tipo_alerta,
    'ALTA' as severidad,
    municipio,
    region,
    CONCAT('Cobertura crítica ', cobertura_porcentaje, '% en ', municipio) as mensaje,
    codigo_municipio,
    cobertura_porcentaje as valor_metrica,
    'cobertura' as categoria,
    NOW() as generada_en
FROM mv_dashboard_principal
WHERE categoria_cobertura = 'CRITICA'
  AND poblacion_total > 500
GROUP BY codigo_municipio, municipio, region, cobertura_porcentaje

UNION ALL

-- Alertas inactividad municipal
SELECT 
    'INACTIVIDAD_MUNICIPAL' as tipo_alerta,
    'MEDIA' as severidad,
    municipio,
    region,
    CONCAT('Sin actividad reciente en ', municipio) as mensaje,
    codigo_municipio,
    EXTRACT(DAYS FROM NOW() - ultima_vacuna) as valor_metrica,
    'inactividad' as categoria,
    NOW() as generada_en
FROM mv_dashboard_principal
WHERE ultima_vacuna < CURRENT_DATE - INTERVAL '30 days'
  AND poblacion_total > 1000
GROUP BY codigo_municipio, municipio, region, ultima_vacuna

UNION ALL

-- Alertas municipios sin datos
SELECT 
    'MUNICIPIO_SIN_DATOS' as tipo_alerta,
    'BAJA' as severidad,
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
        WHEN 'ALTA' THEN 1 
        WHEN 'MEDIA' THEN 2 
        WHEN 'BAJA' THEN 3 
        ELSE 4 
    END,
    valor_metrica DESC;

\echo 'Vista v_alertas_dashboard creada'

-- =====================================================
-- 6. VISTA INSTITUCIONES - Performance Institucional
-- =====================================================

CREATE OR REPLACE VIEW v_instituciones_performance AS
SELECT 
    v.institucion,
    COUNT(*) as total_vacunas_aplicadas,
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
    
    -- Distribución urbano/rural
    COUNT(*) FILTER (WHERE v.tipo_ubicacion = 'Urbano') as vacunas_urbano,
    COUNT(*) FILTER (WHERE v.tipo_ubicacion = 'Rural') as vacunas_rural,
    
    -- Actividad temporal
    MIN(v.fecha_aplicacion) as inicio_actividad,
    MAX(v.fecha_aplicacion) as ultima_actividad,
    
    -- Métricas performance
    ROUND(
        COUNT(*) / NULLIF(COUNT(DISTINCT CONCAT(v.año, '-', LPAD(v.mes::text, 2, '0'))), 0)::decimal, 1
    ) as promedio_vacunas_mensuales,
    
    -- Cobertura territorial (porcentaje municipios Tolima atendidos)
    CAST(ROUND(COUNT(DISTINCT v.codigo_municipio) * 100.0 / 47::numeric, 1) AS decimal) as cobertura_territorial_pct

FROM vacunacion_fiebre_amarilla v
LEFT JOIN unidades_territoriales ut ON v.codigo_municipio = ut.codigo_divipola
WHERE v.institucion IS NOT NULL 
  AND v.institucion != ''
GROUP BY v.institucion
HAVING COUNT(*) >= 10  -- Solo instituciones con actividad significativa
ORDER BY total_vacunas_aplicadas DESC;

\echo 'Vista v_instituciones_performance creada'

-- =====================================================
-- 7. VISTA CASOS EPIDEMIOLÓGICOS - Vigilancia
-- =====================================================

CREATE OR REPLACE VIEW v_casos_dashboard AS
SELECT 
    c.codigo_municipio_procedencia as codigo_municipio,
    c.municipio_procedencia as municipio,
    c.año,
    c.semana_epidemiologica,
    c.grupo_etario,
    
    -- Métricas casos
    COUNT(*) as total_casos,
    COUNT(*) FILTER (WHERE c.condicion_final = 'Muerto') as defunciones,
    COUNT(*) FILTER (WHERE c.vacunado_previo = true) as casos_vacunados_previos,
    COUNT(*) FILTER (WHERE c.hospitalizado = true) as casos_hospitalizados,
    
    -- Distribución temporal
    MIN(c.fecha_notificacion) as primer_caso,
    MAX(c.fecha_notificacion) as ultimo_caso,
    
    -- Síntomas principales
    COUNT(*) FILTER (WHERE c.fiebre = true) as casos_con_fiebre,
    COUNT(*) FILTER (WHERE c.ictericia = true) as casos_con_ictericia,
    COUNT(*) FILTER (WHERE c.sangrado = true) as casos_con_sangrado,
    
    -- Letalidad
    CAST(ROUND(COUNT(*) FILTER (WHERE c.condicion_final = 'Muerto') * 100.0 / 
        NULLIF(COUNT(*)::numeric, 0) AS decimal), 2
    ) as letalidad_porcentaje

FROM casos_fiebre_amarilla c
WHERE c.fecha_notificacion IS NOT NULL
  AND c.municipio_procedencia IS NOT NULL
GROUP BY 
    c.codigo_municipio_procedencia, c.municipio_procedencia, 
    c.año, c.semana_epidemiologica, c.grupo_etario
ORDER BY c.año DESC, c.semana_epidemiologica DESC, total_casos DESC;

\echo 'Vista v_casos_dashboard creada'

-- =====================================================
-- 8. VISTA EPIZOOTIAS - Vigilancia Animal
-- =====================================================

CREATE OR REPLACE VIEW v_epizootias_dashboard AS
SELECT 
    e.codigo_municipio,
    e.municipio,
    EXTRACT(YEAR FROM e.fecha_recoleccion) as año,
    EXTRACT(MONTH FROM e.fecha_recoleccion) as mes,
    e.especie,
    
    -- Métricas epizootias
    COUNT(*) as total_epizootias,
    COUNT(*) FILTER (WHERE e.punto_geografico IS NOT NULL) as con_coordenadas,
    COUNT(*) FILTER (WHERE e.resultado_pcr IS NOT NULL) as con_resultado_pcr,
    COUNT(*) FILTER (WHERE e.resultado_pcr = 'POSITIVO') as pcr_positivos,
    
    -- Distribución temporal
    MIN(e.fecha_recoleccion) as primera_epizooti,
    MAX(e.fecha_recoleccion) as ultima_epizooti,
    
    -- Coordenadas promedio (para centroide en mapa)
    CAST(CAST(ROUND(AVG(e.latitud)::numeric::numeric, 6) AS decimal) AS decimal) as latitud_promedio,
    CAST(CAST(ROUND(AVG(e.longitud)::numeric::numeric, 6) AS decimal) AS decimal) as longitud_promedio

FROM epizootias e
WHERE e.fecha_recoleccion IS NOT NULL
  AND e.municipio IS NOT NULL
GROUP BY 
    e.codigo_municipio, e.municipio, 
    EXTRACT(YEAR FROM e.fecha_recoleccion),
    EXTRACT(MONTH FROM e.fecha_recoleccion),
    e.especie
ORDER BY año DESC, mes DESC, total_epizootias DESC;

\echo 'Vista v_epizootias_dashboard creada'

-- =====================================================
-- 9. FUNCIÓN REFRESCAR VISTAS MATERIALIZADAS
-- =====================================================

CREATE OR REPLACE FUNCTION refresh_dashboard_views()
RETURNS TABLE(
    vista_actualizada text,
    registros_count bigint,
    duracion_ms integer
) AS $$
DECLARE
    start_time timestamp;
    end_time timestamp;
    record_count bigint;
BEGIN
    -- Refrescar vista materializada principal
    start_time := clock_timestamp();
    
    REFRESH MATERIALIZED VIEW CONCURRENTLY mv_dashboard_principal;
    
    end_time := clock_timestamp();
    
    -- Contar registros
    SELECT COUNT(*) INTO record_count FROM mv_dashboard_principal;
    
    -- Retornar resultado
    RETURN QUERY SELECT 
        'mv_dashboard_principal'::text,
        record_count,
        EXTRACT(MILLISECONDS FROM (end_time - start_time))::integer;
    
    -- Actualizar estadísticas
    ANALYZE mv_dashboard_principal;
    
    -- Log de actualización (opcional)
    INSERT INTO dashboard_refresh_log (vista_name, refreshed_at, records_count, duration_ms) 
    VALUES (
        'mv_dashboard_principal', 
        NOW(), 
        record_count, 
        EXTRACT(MILLISECONDS FROM (end_time - start_time))::integer
    );
    
END;
$$ LANGUAGE plpgsql;

-- Tabla log refrescos (opcional)
CREATE TABLE IF NOT EXISTS dashboard_refresh_log (
    id SERIAL PRIMARY KEY,
    vista_name VARCHAR(100),
    refreshed_at TIMESTAMP DEFAULT NOW(),
    records_count BIGINT,
    duration_ms INTEGER
);

\echo 'Función refresh_dashboard_views() creada'

-- =====================================================
-- 10. CONFIGURACIÓN PERFORMANCE FINAL
-- =====================================================

-- Crear índices adicionales para vistas
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_vacunacion_dashboard_performance 
ON vacunacion_fiebre_amarilla(codigo_municipio, grupo_etario, tipo_ubicacion, año, mes);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_poblacion_dashboard_performance
ON poblacion(codigo_municipio, grupo_etario, tipo_ubicacion) 
INCLUDE (poblacion_total);

-- Estadísticas actualizadas
ANALYZE mv_dashboard_principal;
ANALYZE vacunacion_fiebre_amarilla;
ANALYZE poblacion;
ANALYZE unidades_territoriales;

-- Configurar refresh automático vista materializada (opcional)
-- Esto se puede programar con cron o desde la aplicación

\echo 'Índices de performance adicionales creados'
\echo ''
\echo '🚀 VISTAS OPTIMIZADAS DASHBOARD COMPLETADAS!'
\echo '   • Vista materializada principal: mv_dashboard_principal'
\echo '   • Indicadores tiempo real: v_indicadores_tiempo_real'
\echo '   • Mapa municipios: v_mapa_municipios'
\echo '   • Tendencias temporales: v_tendencias_temporales'
\echo '   • Alertas automáticas: v_alertas_dashboard'
\echo '   • Performance institucional: v_instituciones_performance'
\echo '   • Casos epidemiológicos: v_casos_dashboard'
\echo '   • Vigilancia epizootias: v_epizootias_dashboard'
\echo ''
\echo '📊 Dashboard Streamlit listo para conexión a PostgreSQL!'
\echo '🔄 Para actualizar: SELECT * FROM refresh_dashboard_views();'