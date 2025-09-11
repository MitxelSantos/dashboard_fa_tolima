-- 01_extensions.sql - Extensiones PostgreSQL + PostGIS
-- CORREGIDO: Para imagen postgis/postgis:15-3.4-alpine

-- PostGIS ya viene instalado en la imagen postgis/postgis
-- Solo verificamos y configuramos

-- Verificar PostGIS
SELECT PostGIS_Version();

-- Extensiones para análisis de texto  
CREATE EXTENSION IF NOT EXISTS pg_trgm;
CREATE EXTENSION IF NOT EXISTS unaccent;
CREATE EXTENSION IF NOT EXISTS fuzzystrmatch;

-- Extensión para UUIDs
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Configurar timezone Colombia
SET timezone = 'America/Bogota';

-- Verificar funciones básicas
SELECT round(123.456, 2) as test_round;

\echo '✅ Extensiones PostgreSQL instaladas exitosamente'
