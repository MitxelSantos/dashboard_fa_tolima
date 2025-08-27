-- 01_extensions.sql
-- Extensiones necesarias para Sistema Epidemiológico Tolima

-- PostGIS para manejo de datos geoespaciales
CREATE EXTENSION IF NOT EXISTS postgis;
CREATE EXTENSION IF NOT EXISTS postgis_topology;

-- Extensiones para búsqueda y análisis de texto
CREATE EXTENSION IF NOT EXISTS pg_trgm;    -- Similitud de texto (para validación nombres)
CREATE EXTENSION IF NOT EXISTS unaccent;  -- Remover acentos
CREATE EXTENSION IF NOT EXISTS fuzzystrmatch; -- Búsqueda aproximada

-- Extensión para UUIDs (útil para IDs únicos)
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Confirmar extensiones instaladas
\echo 'Extensiones PostgreSQL instaladas exitosamente'