-- =========================================
-- SCHEMAS
-- =========================================

CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS mart;
CREATE SCHEMA IF NOT EXISTS monitoring;


-- =========================================
-- TABLE : CITIES (RAW)
-- =========================================

CREATE TABLE IF NOT EXISTS raw.cities_raw (
    city_id SERIAL PRIMARY KEY,
    city_name TEXT NOT NULL,
    department TEXT,
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


-- =========================================
-- TABLE : WEATHER RAW
-- =========================================

CREATE TABLE IF NOT EXISTS raw.weather_raw (
    weather_id SERIAL PRIMARY KEY,
    city_name TEXT NOT NULL,
    weather_date DATE NOT NULL,
    temperature_min DOUBLE PRECISION,
    temperature_max DOUBLE PRECISION,
    temperature_avg DOUBLE PRECISION,
    humidity DOUBLE PRECISION,
    precipitation DOUBLE PRECISION,
    wind_speed DOUBLE PRECISION,
    weather_code INTEGER,
    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


-- =========================================
-- TABLE : MONITORING PIPELINE
-- =========================================

CREATE TABLE IF NOT EXISTS monitoring.pipeline_run_log (
    run_id SERIAL PRIMARY KEY,
    job_name TEXT,
    run_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    status TEXT,
    rows_inserted INTEGER,
    rows_transformed INTEGER,
    error_message TEXT
);


-- =========================================
-- TABLE : WEATHER ALERTS
-- =========================================

CREATE TABLE IF NOT EXISTS monitoring.weather_alerts (
    alert_id SERIAL PRIMARY KEY,
    city_name TEXT,
    weather_date DATE,
    alert_type TEXT,
    alert_value DOUBLE PRECISION,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


-- =========================================
-- INDEXES (OPTIONNEL MAIS PROPRE)
-- =========================================

CREATE INDEX IF NOT EXISTS idx_weather_date
ON raw.weather_raw(weather_date);

CREATE INDEX IF NOT EXISTS idx_city_name
ON raw.weather_raw(city_name);


-- =========================================
-- COMMENTAIRES (OPTIONNEL MAIS PRO)
-- =========================================

COMMENT ON SCHEMA raw IS 'Données brutes issues des APIs';
COMMENT ON SCHEMA staging IS 'Données nettoyées et transformées (dbt)';
COMMENT ON SCHEMA mart IS 'Données analytiques pour Power BI';
COMMENT ON SCHEMA monitoring IS 'Suivi et logs du pipeline';
