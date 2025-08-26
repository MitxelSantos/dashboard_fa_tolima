-- 03_views.sql
-- Vistas optimizadas para Dashboard Sistema Epidemiológico Tolima

\echo 'Creando vistas optimizadas para dashboard...'

-- =====================================================
-- 1. VISTA PRINCIPAL: Coberturas de Vacunación
-- =====================================================
CREATE VIEW v_coberturas_dashboard AS
SELECT 
    v.codigo_municipio,
    v.municipio,
    ut.region,
    v.tipo_ubicacion,
    v.grupo_etario,
    v.año,
    v.mes,
    v.institucion,
    
    -- Métricas principales
    COUNT(*) as vacunados,
    COALESCE(p.poblacion_total, 0) as poblacion_total,
    CASE 
        WHEN COALESCE(p.poblacion_total, 0) > 0 
        THEN ROUND(COUNT(*) * 100.0 / p.poblacion_total, 2)
        ELSE 0 
    END as cobertura_porcentaje,
    
    -- Métricas adicionales
    COUNT(DISTINCT v.institucion) as instituciones_activas,
    ROUND(AVG(v.edad_anos), 1) as edad_promedio,
    MIN(v.fecha_aplicacion) as primera_vacuna,
    MAX(v.fecha_aplicacion) as ultima_vacuna,
    
    -- Para filtros temporales
    CONCAT(v.año, '-', LPAD(v.mes::text, 2, '0')) as periodo_mes

FROM vacunacion_fiebre_amarilla v
JOIN unidades_territoriales ut ON v.codigo_municipio = ut.codigo_divipola
LEFT JOIN poblacion p ON (
    v.codigo_municipio = p.codigo_municipio AND
    v.grupo_etario = p.grupo_etario AND
    v.tipo_ubicacion = p.tipo_ubicacion
)
WHERE ut.tipo = 'municipio'
GROUP BY 
    v.codigo_municipio, v.municipio, ut.region, v.tipo_ubicacion, 
    v.grupo_etario, v.año, v.mes, v.institucion, p.poblacion_total
ORDER BY v.municipio, v.año, v.mes, v.grupo_etario;

\echo 'Vista v_coberturas_dashboard creada'

-- =====================================================
-- 2. VISTA MAPA: Coberturas por Municipio Agregadas
-- =====================================================
CREATE VIEW v_mapa_coberturas AS
SELECT 
    v.codigo_municipio,
    v.municipio,
    ut.region,
    ut.geometria,
    
    -- Totales generales
    COUNT(*) as total_vacunados,
    COALESCE(SUM(p.poblacion_total), 0) as poblacion_total,
    CASE 
        WHEN COALESCE(SUM(p.poblacion_total), 0) > 0 
        THEN ROUND(COUNT(*) * 100.0 / SUM(p.poblacion_total), 2)
        ELSE 0 
    END as cobertura_general,
    
    -- Por ubicación
    COUNT(CASE WHEN v.tipo_ubicacion = 'Urbano' THEN 1 END) as vacunados_urbano,
    COUNT(CASE WHEN v.tipo_ubicacion = 'Rural' THEN 1 END) as vacunados_rural,
    
    -- Por grupo etario
    COUNT(CASE WHEN v.grupo_etario = 'Menor de 9 meses' THEN 1 END) as vacunados_menores_9m,
    COUNT(CASE WHEN v.grupo_etario = '09-23 meses' THEN 1 END) as vacunados_9_23m,
    COUNT(CASE WHEN v.grupo_etario = '02-19 años' THEN 1 END) as vacunados_2_19a,
    COUNT(CASE WHEN v.grupo_etario = '20-59 años' THEN 1 END) as vacunados_20_59a,
    COUNT(CASE WHEN v.grupo_etario = '60+ años' THEN 1 END) as vacunados_60mas,
    
    -- Temporal
    MIN(v.fecha_aplicacion) as primera_vacuna_municipio,
    MAX(v.fecha_aplicacion) as ultima_vacuna_municipio,
    COUNT(DISTINCT v.institucion) as instituciones_total

FROM vacunacion_fiebre_amarilla v
JOIN unidades_territoriales ut ON v.codigo_municipio = ut.codigo_divipola
LEFT JOIN poblacion p ON (
    v.codigo_municipio = p.codigo_municipio AND
    v.grupo_etario = p.grupo_etario AND
    v.tipo_ubicacion = p.tipo_ubicacion
)
WHERE ut.tipo = 'municipio'
GROUP BY v.codigo_municipio, v.municipio, ut.region, ut.geometria
ORDER BY cobertura_general DESC;

\echo 'Vista v_mapa_coberturas creada'

-- =====================================================
-- 3. VISTA ANÁLISIS: Rendimiento por Institución
-- =====================================================
CREATE VIEW v_instituciones_rendimiento AS
SELECT 
    v.institucion,
    v.municipio,
    ut.region,
    v.tipo_ubicacion,
    v.año,
    
    -- Métricas de rendimiento
    COUNT(*) as vacunas_aplicadas,
    COUNT(DISTINCT v.codigo_municipio) as municipios_atendidos,
    COUNT(DISTINCT CONCAT(v.año, '-', v.mes)) as meses_activos,
    
    -- Distribución por grupo etario
    COUNT(CASE WHEN v.grupo_etario = 'Menor de 9 meses' THEN 1 END) as menores_9m,
    COUNT(CASE WHEN v.grupo_etario = '09-23 meses' THEN 1 END) as entre_9_23m,
    COUNT(CASE WHEN v.grupo_etario = '02-19 años' THEN 1 END) as entre_2_19a,
    COUNT(CASE WHEN v.grupo_etario = '20-59 años' THEN 1 END) as entre_20_59a,
    COUNT(CASE WHEN v.grupo_etario = '60+ años' THEN 1 END) as mayores_60a,
    
    -- Temporal
    MIN(v.fecha_aplicacion) as inicio_actividad,
    MAX(v.fecha_aplicacion) as ultima_actividad,
    ROUND(COUNT(*)::DECIMAL / 12, 1) as promedio_mensual  -- Asumiendo año completo
    
FROM vacunacion_fiebre_amarilla v
JOIN unidades_territoriales ut ON v.codigo_municipio = ut.codigo_divipola
WHERE ut.tipo = 'municipio'
GROUP BY v.institucion, v.municipio, ut.region, v.tipo_ubicacion, v.año
ORDER BY vacunas_aplicadas DESC;

\echo 'Vista v_instituciones_rendimiento creada'

-- =====================================================
-- 4. VISTA ALERTAS: Municipios con Baja Cobertura
-- =====================================================
CREATE VIEW v_alertas_cobertura_baja AS
SELECT 
    municipio,
    region,
    tipo_ubicacion,
    grupo_etario,
    vacunados,
    poblacion_total,
    cobertura_porcentaje,
    
    -- Clasificación de alerta
    CASE 
        WHEN cobertura_porcentaje < 50 THEN 'CRÍTICA'
        WHEN cobertura_porcentaje < 70 THEN 'BAJA'
        WHEN cobertura_porcentaje < 85 THEN 'REGULAR'
        ELSE 'ADECUADA'
    END as nivel_alerta,
    
    -- Brecha de vacunación
    CASE 
        WHEN poblacion_total > 0 
        THEN poblacion_total - vacunados
        ELSE 0 
    END as faltantes_para_100,
    
    periodo_mes

FROM v_coberturas_dashboard
WHERE cobertura_porcentaje < 85
ORDER BY cobertura_porcentaje ASC, poblacion_total DESC;

\echo 'Vista v_alertas_cobertura_baja creada'

-- =====================================================
-- 5. VISTA TEMPORAL: Tendencias de Vacunación
-- =====================================================
CREATE VIEW v_tendencias_vacunacion AS
SELECT 
    año,
    mes,
    semana_epidemiologica,
    municipio,
    region,
    tipo_ubicacion,
    grupo_etario,
    
    -- Conteos temporales
    COUNT(*) as vacunados_periodo,
    COUNT(DISTINCT institucion) as instituciones_activas,
    
    -- Acumulados (window function)
    SUM(COUNT(*)) OVER (
        PARTITION BY municipio, grupo_etario, tipo_ubicacion 
        ORDER BY año, mes
    ) as vacunados_acumulado,
    
    -- Promedio móvil 3 meses
    AVG(COUNT(*)) OVER (
        PARTITION BY municipio, grupo_etario, tipo_ubicacion 
        ORDER BY año, mes 
        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    ) as promedio_movil_3m,
    
    -- Comparación mes anterior
    LAG(COUNT(*)) OVER (
        PARTITION BY municipio, grupo_etario, tipo_ubicacion 
        ORDER BY año, mes
    ) as vacunados_mes_anterior

FROM vacunacion_fiebre_amarilla v
JOIN unidades_territoriales ut ON v.codigo_municipio = ut.codigo_divipola
WHERE ut.tipo = 'municipio'
GROUP BY año, mes, semana_epidemiologica, municipio, region, tipo_ubicacion, grupo_etario
ORDER BY año, mes, municipio, grupo_etario;

\echo 'Vista v_tendencias_vacunacion creada'

-- =====================================================
-- 6. VISTA RESUMEN: Indicadores Clave
-- =====================================================
CREATE VIEW v_indicadores_clave AS
SELECT 
    'Tolima' as departamento,
    COUNT(DISTINCT v.codigo_municipio) as municipios_con_vacunacion,
    COUNT(*) as total_vacunados_historico,
    COUNT(DISTINCT v.institucion) as instituciones_total,
    
    -- Por ubicación
    COUNT(CASE WHEN v.tipo_ubicacion = 'Urbano' THEN 1 END) as vacunados_urbano,
    COUNT(CASE WHEN v.tipo_ubicacion = 'Rural' THEN 1 END) as vacunados_rural,
    ROUND(
        COUNT(CASE WHEN v.tipo_ubicacion = 'Rural' THEN 1 END) * 100.0 / COUNT(*), 1
    ) as porcentaje_rural,
    
    -- Por grupo etario
    COUNT(CASE WHEN v.grupo_etario = 'Menor de 9 meses' THEN 1 END) as menores_9m,
    COUNT(CASE WHEN v.grupo_etario = '09-23 meses' THEN 1 END) as entre_9_23m,
    COUNT(CASE WHEN v.grupo_etario = '02-19 años' THEN 1 END) as entre_2_19a,
    COUNT(CASE WHEN v.grupo_etario = '20-59 años' THEN 1 END) as entre_20_59a,
    COUNT(CASE WHEN v.grupo_etario = '60+ años' THEN 1 END) as mayores_60a,
    
    -- Temporal
    MIN(v.fecha_aplicacion) as primera_vacuna_departamental,
    MAX(v.fecha_aplicacion) as ultima_vacuna_departamental,
    
    -- Cobertura general aproximada
    COALESCE(SUM(p.poblacion_total), 0) as poblacion_total_estimada,
    CASE 
        WHEN COALESCE(SUM(p.poblacion_total), 0) > 0 
        THEN ROUND(COUNT(*) * 100.0 / SUM(p.poblacion_total), 2)
        ELSE 0 
    END as cobertura_general_estimada

FROM vacunacion_fiebre_amarilla v
JOIN unidades_territoriales ut ON v.codigo_municipio = ut.codigo_divipola
LEFT JOIN poblacion p ON (
    v.codigo_municipio = p.codigo_municipio AND
    v.grupo_etario = p.grupo_etario AND
    v.tipo_ubicacion = p.tipo_ubicacion
)
WHERE ut.tipo = 'municipio';

\echo 'Vista v_indicadores_clave creada'

-- =====================================================
-- 7. VISTA CASOS: Dashboard Casos Fiebre Amarilla
-- =====================================================
CREATE OR REPLACE VIEW v_casos_dashboard AS
SELECT 
    c.codigo_municipio_residencia as codigo_municipio,
    c.municipio_residencia as municipio,
    ut.region,
    c.tipo_ubicacion_residencia as tipo_ubicacion,
    c.semana_epidemiologica,
    c.año,
    EXTRACT(MONTH FROM c.fecha_notificacion) as mes,
    
    -- Conteos principales
    COUNT(*) as numero_casos,
    COUNT(CASE WHEN c.condicion_final = 'Muerto' THEN 1 END) as defunciones,
    COUNT(CASE WHEN c.clasificacion_final = 'Confirmado' THEN 1 END) as confirmados,
    COUNT(CASE WHEN c.clasificacion_final = 'Probable' THEN 1 END) as probables,
    COUNT(CASE WHEN c.clasificacion_final = 'Sospechoso' THEN 1 END) as sospechosos,
    
    -- Indicadores
    CASE 
        WHEN COUNT(*) > 0 
        THEN ROUND(COUNT(CASE WHEN c.condicion_final = 'Muerto' THEN 1 END) * 100.0 / COUNT(*), 2)
        ELSE 0 
    END as letalidad_porcentaje,
    
    -- Temporal
    MIN(c.fecha_notificacion) as primer_caso,
    MAX(c.fecha_notificacion) as ultimo_caso

FROM casos_fiebre_amarilla c
LEFT JOIN unidades_territoriales ut ON c.codigo_municipio_residencia = ut.codigo_divipola
WHERE ut.tipo = 'municipio' OR ut.tipo IS NULL
GROUP BY 
    c.codigo_municipio_residencia, c.municipio_residencia, ut.region,
    c.tipo_ubicacion_residencia, c.semana_epidemiologica, c.año,
    EXTRACT(MONTH FROM c.fecha_notificacion)
ORDER BY c.año DESC, c.semana_epidemiologica DESC, c.municipio_residencia;

\echo 'Vista v_casos_dashboard creada'

-- =====================================================
-- 8. ÍNDICES ADICIONALES PARA VISTAS
-- =====================================================

-- Índice para mejores performance en vistas con GROUP BY temporal
CREATE INDEX IF NOT EXISTS idx_vacunacion_grupo_temporal 
ON vacunacion_fiebre_amarilla(municipio, grupo_etario, tipo_ubicacion, año, mes);

-- Índice para análisis por institución
CREATE INDEX IF NOT EXISTS idx_vacunacion_institucion_municipio 
ON vacunacion_fiebre_amarilla(institucion, municipio, año);

\echo 'Índices adicionales creados'

-- =====================================================
-- 9. PERMISOS Y CONFIGURACIONES FINALES
-- =====================================================

-- Configurar zona horaria
SET timezone = 'America/Bogota';

-- Estadísticas para optimizador
ANALYZE unidades_territoriales;
ANALYZE poblacion;
ANALYZE vacunacion_fiebre_amarilla;

\echo 'Configuraciones finales aplicadas'
\echo 'Vistas optimizadas creadas exitosamente!'
\echo 'Sistema listo para recibir datos y conectar dashboard'