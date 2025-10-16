-- COVID-19 ETL Pipeline - Redshift Table Schemas
-- Amazon Redshift table creation scripts

-- Global COVID-19 statistics table
CREATE TABLE IF NOT EXISTS public.covid_global (
    cases BIGINT,
    deaths BIGINT,
    recovered BIGINT,
    active BIGINT,
    critical BIGINT,
    today_cases BIGINT,
    today_deaths BIGINT,
    today_recovered BIGINT,
    cases_per_one_million DECIMAL(10,2),
    deaths_per_one_million DECIMAL(10,2),
    tests BIGINT,
    tests_per_one_million DECIMAL(10,2),
    population BIGINT,
    one_case_per_people DECIMAL(10,2),
    one_death_per_people DECIMAL(10,2),
    one_test_per_people DECIMAL(10,2),
    active_per_one_million DECIMAL(10,2),
    recovered_per_one_million DECIMAL(10,2),
    critical_per_one_million DECIMAL(10,2),
    affected_countries INTEGER,
    updated TIMESTAMP,
    extraction_date TIMESTAMP,
    data_source VARCHAR(50),
    data_type VARCHAR(50),
    processed_at TIMESTAMP,
    data_freshness_hours DECIMAL(10,2),
    mortality_rate DECIMAL(10,6),
    recovery_rate DECIMAL(10,6),
    active_rate DECIMAL(10,6),
    region_type VARCHAR(50),
    region_name VARCHAR(100)
)
DISTSTYLE AUTO
SORTKEY (updated, extraction_date);

-- Countries COVID-19 statistics table
CREATE TABLE IF NOT EXISTS public.covid_countries (
    country VARCHAR(100),
    country_iso2 VARCHAR(2),
    country_iso3 VARCHAR(3),
    country_id INTEGER,
    country_lat DECIMAL(10,6),
    country_long DECIMAL(10,6),
    country_flag VARCHAR(500),
    cases BIGINT,
    deaths BIGINT,
    recovered BIGINT,
    active BIGINT,
    critical BIGINT,
    today_cases BIGINT,
    today_deaths BIGINT,
    today_recovered BIGINT,
    cases_per_one_million DECIMAL(10,2),
    deaths_per_one_million DECIMAL(10,2),
    tests BIGINT,
    tests_per_one_million DECIMAL(10,2),
    population BIGINT,
    continent VARCHAR(50),
    one_case_per_people DECIMAL(10,2),
    one_death_per_people DECIMAL(10,2),
    one_test_per_people DECIMAL(10,2),
    active_per_one_million DECIMAL(10,2),
    recovered_per_one_million DECIMAL(10,2),
    critical_per_one_million DECIMAL(10,2),
    updated TIMESTAMP,
    extraction_date TIMESTAMP,
    data_source VARCHAR(50),
    data_type VARCHAR(50),
    processed_at TIMESTAMP,
    data_freshness_hours DECIMAL(10,2),
    mortality_rate DECIMAL(10,6),
    recovery_rate DECIMAL(10,6),
    active_rate DECIMAL(10,6),
    cases_per_million DECIMAL(10,2),
    deaths_per_million DECIMAL(10,2),
    region_type VARCHAR(50),
    region_name VARCHAR(100)
)
DISTSTYLE KEY
DISTKEY (country)
SORTKEY (country, updated, extraction_date);

-- Continents COVID-19 statistics table
CREATE TABLE IF NOT EXISTS public.covid_continents (
    continent VARCHAR(50),
    cases BIGINT,
    deaths BIGINT,
    recovered BIGINT,
    active BIGINT,
    critical BIGINT,
    today_cases BIGINT,
    today_deaths BIGINT,
    today_recovered BIGINT,
    cases_per_one_million DECIMAL(10,2),
    deaths_per_one_million DECIMAL(10,2),
    tests BIGINT,
    tests_per_one_million DECIMAL(10,2),
    population BIGINT,
    active_per_one_million DECIMAL(10,2),
    recovered_per_one_million DECIMAL(10,2),
    critical_per_one_million DECIMAL(10,2),
    countries VARCHAR(2000),
    updated TIMESTAMP,
    extraction_date TIMESTAMP,
    data_source VARCHAR(50),
    data_type VARCHAR(50),
    processed_at TIMESTAMP,
    data_freshness_hours DECIMAL(10,2),
    mortality_rate DECIMAL(10,6),
    recovery_rate DECIMAL(10,6),
    active_rate DECIMAL(10,6),
    cases_per_million DECIMAL(10,2),
    deaths_per_million DECIMAL(10,2),
    region_type VARCHAR(50),
    region_name VARCHAR(100)
)
DISTSTYLE AUTO
SORTKEY (continent, updated, extraction_date);

-- US States COVID-19 statistics table
CREATE TABLE IF NOT EXISTS public.covid_states (
    state VARCHAR(100),
    cases BIGINT,
    deaths BIGINT,
    recovered BIGINT,
    active BIGINT,
    today_cases BIGINT,
    today_deaths BIGINT,
    today_recovered BIGINT,
    cases_per_one_million DECIMAL(10,2),
    deaths_per_one_million DECIMAL(10,2),
    tests BIGINT,
    tests_per_one_million DECIMAL(10,2),
    population BIGINT,
    updated TIMESTAMP,
    extraction_date TIMESTAMP,
    data_source VARCHAR(50),
    data_type VARCHAR(50),
    processed_at TIMESTAMP,
    data_freshness_hours DECIMAL(10,2),
    mortality_rate DECIMAL(10,6),
    recovery_rate DECIMAL(10,6),
    active_rate DECIMAL(10,6),
    cases_per_million DECIMAL(10,2),
    deaths_per_million DECIMAL(10,2),
    region_type VARCHAR(50),
    country VARCHAR(50),
    region_name VARCHAR(100)
)
DISTSTYLE KEY
DISTKEY (state)
SORTKEY (state, updated, extraction_date);

-- Vaccine data table
CREATE TABLE IF NOT EXISTS public.covid_vaccines (
    country VARCHAR(100),
    timeline VARCHAR(10000),
    extraction_date TIMESTAMP,
    data_source VARCHAR(50),
    data_type VARCHAR(50),
    processed_at TIMESTAMP,
    data_category VARCHAR(50)
)
DISTSTYLE KEY
DISTKEY (country)
SORTKEY (country, extraction_date);

-- Historical COVID-19 data table
CREATE TABLE IF NOT EXISTS public.covid_historical (
    date DATE,
    metric VARCHAR(50),
    value BIGINT,
    country VARCHAR(100),
    extraction_date TIMESTAMP,
    data_source VARCHAR(50),
    data_type VARCHAR(50),
    processed_at TIMESTAMP,
    year INTEGER,
    month INTEGER,
    day_of_week INTEGER,
    week_of_year INTEGER,
    daily_change BIGINT,
    daily_change_pct DECIMAL(10,6),
    value_7day_avg DECIMAL(15,2),
    daily_change_7day_avg DECIMAL(15,2)
)
DISTSTYLE KEY
DISTKEY (country)
SORTKEY (country, metric, date);

-- Data quality metrics table
CREATE TABLE IF NOT EXISTS public.covid_data_quality (
    table_name VARCHAR(100),
    validation_timestamp TIMESTAMP,
    record_count BIGINT,
    expected_count BIGINT,
    count_match BOOLEAN,
    is_valid BOOLEAN,
    quality_score DECIMAL(5,2),
    errors VARCHAR(2000),
    warnings VARCHAR(2000),
    null_percentages VARCHAR(2000),
    duplicate_percentage DECIMAL(10,6),
    memory_usage_mb DECIMAL(10,2),
    extraction_date TIMESTAMP
)
DISTSTYLE AUTO
SORTKEY (table_name, validation_timestamp);

-- Pipeline execution log table
CREATE TABLE IF NOT EXISTS public.covid_pipeline_log (
    execution_id VARCHAR(100),
    execution_date TIMESTAMP,
    task_name VARCHAR(100),
    status VARCHAR(50),
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    duration_seconds INTEGER,
    records_processed BIGINT,
    error_message VARCHAR(2000),
    created_at TIMESTAMP DEFAULT GETDATE()
)
DISTSTYLE AUTO
SORTKEY (execution_date, task_name);

-- Create indexes for better query performance
CREATE INDEX IF NOT EXISTS idx_countries_country_date ON public.covid_countries (country, extraction_date);
CREATE INDEX IF NOT EXISTS idx_historical_country_metric_date ON public.covid_historical (country, metric, date);
CREATE INDEX IF NOT EXISTS idx_states_state_date ON public.covid_states (state, extraction_date);
CREATE INDEX IF NOT EXISTS idx_continents_continent_date ON public.covid_continents (continent, extraction_date);

-- Grant permissions (adjust as needed for your environment)
GRANT SELECT ON ALL TABLES IN SCHEMA public TO GROUP analysts;
GRANT ALL ON ALL TABLES IN SCHEMA public TO GROUP etl_users;
