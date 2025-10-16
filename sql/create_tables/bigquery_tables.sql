-- COVID-19 ETL Pipeline - BigQuery Table Schemas
-- Google BigQuery table creation scripts

-- Create dataset (run this first)
CREATE SCHEMA IF NOT EXISTS `covid_data`
OPTIONS(
  description="COVID-19 ETL Pipeline Data Warehouse",
  location="US"
);

-- Global COVID-19 statistics table
CREATE OR REPLACE TABLE `covid_data.covid_global` (
  cases INT64,
  deaths INT64,
  recovered INT64,
  active INT64,
  critical INT64,
  today_cases INT64,
  today_deaths INT64,
  today_recovered INT64,
  cases_per_one_million FLOAT64,
  deaths_per_one_million FLOAT64,
  tests INT64,
  tests_per_one_million FLOAT64,
  population INT64,
  one_case_per_people FLOAT64,
  one_death_per_people FLOAT64,
  one_test_per_people FLOAT64,
  active_per_one_million FLOAT64,
  recovered_per_one_million FLOAT64,
  critical_per_one_million FLOAT64,
  affected_countries INT64,
  updated TIMESTAMP,
  extraction_date TIMESTAMP,
  data_source STRING,
  data_type STRING,
  processed_at TIMESTAMP,
  data_freshness_hours FLOAT64,
  mortality_rate FLOAT64,
  recovery_rate FLOAT64,
  active_rate FLOAT64,
  region_type STRING,
  region_name STRING
)
PARTITION BY DATE(extraction_date)
CLUSTER BY region_type, updated
OPTIONS(
  description="Global COVID-19 statistics",
  labels=[("source", "disease_sh"), ("type", "global")]
);

-- Countries COVID-19 statistics table
CREATE OR REPLACE TABLE `covid_data.covid_countries` (
  country STRING,
  country_iso2 STRING,
  country_iso3 STRING,
  country_id INT64,
  country_lat FLOAT64,
  country_long FLOAT64,
  country_flag STRING,
  cases INT64,
  deaths INT64,
  recovered INT64,
  active INT64,
  critical INT64,
  today_cases INT64,
  today_deaths INT64,
  today_recovered INT64,
  cases_per_one_million FLOAT64,
  deaths_per_one_million FLOAT64,
  tests INT64,
  tests_per_one_million FLOAT64,
  population INT64,
  continent STRING,
  one_case_per_people FLOAT64,
  one_death_per_people FLOAT64,
  one_test_per_people FLOAT64,
  active_per_one_million FLOAT64,
  recovered_per_one_million FLOAT64,
  critical_per_one_million FLOAT64,
  updated TIMESTAMP,
  extraction_date TIMESTAMP,
  data_source STRING,
  data_type STRING,
  processed_at TIMESTAMP,
  data_freshness_hours FLOAT64,
  mortality_rate FLOAT64,
  recovery_rate FLOAT64,
  active_rate FLOAT64,
  cases_per_million FLOAT64,
  deaths_per_million FLOAT64,
  region_type STRING,
  region_name STRING
)
PARTITION BY DATE(extraction_date)
CLUSTER BY country, continent, updated
OPTIONS(
  description="Country-level COVID-19 statistics",
  labels=[("source", "disease_sh"), ("type", "countries")]
);

-- Continents COVID-19 statistics table
CREATE OR REPLACE TABLE `covid_data.covid_continents` (
  continent STRING,
  cases INT64,
  deaths INT64,
  recovered INT64,
  active INT64,
  critical INT64,
  today_cases INT64,
  today_deaths INT64,
  today_recovered INT64,
  cases_per_one_million FLOAT64,
  deaths_per_one_million FLOAT64,
  tests INT64,
  tests_per_one_million FLOAT64,
  population INT64,
  active_per_one_million FLOAT64,
  recovered_per_one_million FLOAT64,
  critical_per_one_million FLOAT64,
  countries STRING,
  updated TIMESTAMP,
  extraction_date TIMESTAMP,
  data_source STRING,
  data_type STRING,
  processed_at TIMESTAMP,
  data_freshness_hours FLOAT64,
  mortality_rate FLOAT64,
  recovery_rate FLOAT64,
  active_rate FLOAT64,
  cases_per_million FLOAT64,
  deaths_per_million FLOAT64,
  region_type STRING,
  region_name STRING
)
PARTITION BY DATE(extraction_date)
CLUSTER BY continent, updated
OPTIONS(
  description="Continent-level COVID-19 statistics",
  labels=[("source", "disease_sh"), ("type", "continents")]
);

-- US States COVID-19 statistics table
CREATE OR REPLACE TABLE `covid_data.covid_states` (
  state STRING,
  cases INT64,
  deaths INT64,
  recovered INT64,
  active INT64,
  today_cases INT64,
  today_deaths INT64,
  today_recovered INT64,
  cases_per_one_million FLOAT64,
  deaths_per_one_million FLOAT64,
  tests INT64,
  tests_per_one_million FLOAT64,
  population INT64,
  updated TIMESTAMP,
  extraction_date TIMESTAMP,
  data_source STRING,
  data_type STRING,
  processed_at TIMESTAMP,
  data_freshness_hours FLOAT64,
  mortality_rate FLOAT64,
  recovery_rate FLOAT64,
  active_rate FLOAT64,
  cases_per_million FLOAT64,
  deaths_per_million FLOAT64,
  region_type STRING,
  country STRING,
  region_name STRING
)
PARTITION BY DATE(extraction_date)
CLUSTER BY state, updated
OPTIONS(
  description="US state-level COVID-19 statistics",
  labels=[("source", "disease_sh"), ("type", "states")]
);

-- Vaccine data table
CREATE OR REPLACE TABLE `covid_data.covid_vaccines` (
  country STRING,
  timeline STRING,
  extraction_date TIMESTAMP,
  data_source STRING,
  data_type STRING,
  processed_at TIMESTAMP,
  data_category STRING
)
PARTITION BY DATE(extraction_date)
CLUSTER BY country
OPTIONS(
  description="COVID-19 vaccination data",
  labels=[("source", "disease_sh"), ("type", "vaccines")]
);

-- Historical COVID-19 data table
CREATE OR REPLACE TABLE `covid_data.covid_historical` (
  date DATE,
  metric STRING,
  value INT64,
  country STRING,
  extraction_date TIMESTAMP,
  data_source STRING,
  data_type STRING,
  processed_at TIMESTAMP,
  year INT64,
  month INT64,
  day_of_week INT64,
  week_of_year INT64,
  daily_change INT64,
  daily_change_pct FLOAT64,
  value_7day_avg FLOAT64,
  daily_change_7day_avg FLOAT64
)
PARTITION BY date
CLUSTER BY country, metric
OPTIONS(
  description="Historical COVID-19 time series data",
  labels=[("source", "disease_sh"), ("type", "historical")]
);

-- Data quality metrics table
CREATE OR REPLACE TABLE `covid_data.covid_data_quality` (
  table_name STRING,
  validation_timestamp TIMESTAMP,
  record_count INT64,
  expected_count INT64,
  count_match BOOL,
  is_valid BOOL,
  quality_score FLOAT64,
  errors STRING,
  warnings STRING,
  null_percentages STRING,
  duplicate_percentage FLOAT64,
  memory_usage_mb FLOAT64,
  extraction_date TIMESTAMP
)
PARTITION BY DATE(validation_timestamp)
CLUSTER BY table_name
OPTIONS(
  description="Data quality validation results",
  labels=[("type", "quality_metrics")]
);

-- Pipeline execution log table
CREATE OR REPLACE TABLE `covid_data.covid_pipeline_log` (
  execution_id STRING,
  execution_date TIMESTAMP,
  task_name STRING,
  status STRING,
  start_time TIMESTAMP,
  end_time TIMESTAMP,
  duration_seconds INT64,
  records_processed INT64,
  error_message STRING,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
PARTITION BY DATE(execution_date)
CLUSTER BY task_name, status
OPTIONS(
  description="ETL pipeline execution logs",
  labels=[("type", "pipeline_logs")]
);

-- Create views for common queries

-- Latest country statistics view
CREATE OR REPLACE VIEW `covid_data.latest_country_stats` AS
SELECT 
  country,
  continent,
  cases,
  deaths,
  recovered,
  active,
  mortality_rate,
  recovery_rate,
  cases_per_million,
  deaths_per_million,
  population,
  updated,
  extraction_date
FROM `covid_data.covid_countries`
WHERE extraction_date = (
  SELECT MAX(extraction_date) 
  FROM `covid_data.covid_countries`
)
ORDER BY cases DESC;

-- Daily global trends view
CREATE OR REPLACE VIEW `covid_data.global_trends` AS
SELECT 
  date,
  SUM(CASE WHEN metric = 'cases' THEN value END) as total_cases,
  SUM(CASE WHEN metric = 'deaths' THEN value END) as total_deaths,
  SUM(CASE WHEN metric = 'recovered' THEN value END) as total_recovered,
  AVG(CASE WHEN metric = 'cases' THEN daily_change END) as avg_daily_cases,
  AVG(CASE WHEN metric = 'deaths' THEN daily_change END) as avg_daily_deaths
FROM `covid_data.covid_historical`
WHERE country = 'Global'
GROUP BY date
ORDER BY date DESC;

-- Top affected countries view
CREATE OR REPLACE VIEW `covid_data.top_affected_countries` AS
SELECT 
  country,
  continent,
  cases,
  deaths,
  mortality_rate,
  cases_per_million,
  deaths_per_million,
  population,
  RANK() OVER (ORDER BY cases DESC) as cases_rank,
  RANK() OVER (ORDER BY deaths DESC) as deaths_rank,
  RANK() OVER (ORDER BY mortality_rate DESC) as mortality_rank
FROM `covid_data.latest_country_stats`
WHERE cases > 1000  -- Filter out countries with very low case counts
ORDER BY cases DESC
LIMIT 50;
