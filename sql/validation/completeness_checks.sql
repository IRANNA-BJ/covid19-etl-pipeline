-- COVID-19 ETL Pipeline - Data Completeness Validation Queries
-- SQL queries for validating data completeness and coverage

-- =============================================================================
-- TEMPORAL COMPLETENESS CHECKS
-- =============================================================================

-- Check for missing dates in historical data
WITH date_series AS (
    SELECT generate_series(
        (SELECT MIN(date) FROM covid_historical),
        (SELECT MAX(date) FROM covid_historical),
        '1 day'::interval
    )::date as expected_date
),
missing_dates AS (
    SELECT 
        ds.expected_date,
        COUNT(h.date) as records_count
    FROM date_series ds
    LEFT JOIN covid_historical h ON ds.expected_date = h.date
    GROUP BY ds.expected_date
    HAVING COUNT(h.date) = 0
)
SELECT 
    'temporal_completeness' as check_category,
    'missing_dates_historical' as check_name,
    COUNT(*) as missing_dates_count,
    CASE WHEN COUNT(*) > 7 THEN 'FAIL' ELSE 'PASS' END as status,
    'Historical data should have daily records' as description
FROM missing_dates;

-- Check for countries missing recent data
WITH recent_countries AS (
    SELECT DISTINCT country
    FROM covid_historical
    WHERE date >= CURRENT_DATE - INTERVAL '7 days'
),
all_countries AS (
    SELECT DISTINCT country
    FROM covid_countries
    WHERE cases > 1000  -- Focus on countries with significant cases
)
SELECT 
    'temporal_completeness' as check_category,
    'countries_missing_recent_data' as check_name,
    COUNT(*) as missing_countries_count,
    CASE WHEN COUNT(*) > 10 THEN 'FAIL' ELSE 'PASS' END as status,
    'Major countries should have recent historical data' as description
FROM all_countries ac
LEFT JOIN recent_countries rc ON ac.country = rc.country
WHERE rc.country IS NULL;

-- =============================================================================
-- GEOGRAPHICAL COMPLETENESS CHECKS
-- =============================================================================

-- Check for missing continents
WITH expected_continents AS (
    SELECT continent_name FROM (
        VALUES ('Asia'), ('Europe'), ('North America'), 
               ('South America'), ('Africa'), ('Oceania')
    ) AS t(continent_name)
),
actual_continents AS (
    SELECT DISTINCT continent as continent_name
    FROM covid_countries
    WHERE continent IS NOT NULL AND continent != ''
)
SELECT 
    'geographical_completeness' as check_category,
    'missing_continents' as check_name,
    COUNT(*) as missing_continents_count,
    CASE WHEN COUNT(*) > 0 THEN 'FAIL' ELSE 'PASS' END as status,
    'All major continents should be represented' as description
FROM expected_continents ec
LEFT JOIN actual_continents ac ON ec.continent_name = ac.continent_name
WHERE ac.continent_name IS NULL;

-- Check for major countries presence
WITH major_countries AS (
    SELECT country_name FROM (
        VALUES ('United States'), ('China'), ('India'), ('Brazil'), 
               ('Russia'), ('France'), ('Germany'), ('United Kingdom'),
               ('Italy'), ('Spain'), ('Iran'), ('South Korea'), ('Japan')
    ) AS t(country_name)
),
actual_countries AS (
    SELECT DISTINCT country as country_name
    FROM covid_countries
)
SELECT 
    'geographical_completeness' as check_category,
    'missing_major_countries' as check_name,
    COUNT(*) as missing_countries_count,
    CASE WHEN COUNT(*) > 2 THEN 'FAIL' ELSE 'PASS' END as status,
    'Major countries should be present in the data' as description
FROM major_countries mc
LEFT JOIN actual_countries ac ON mc.country_name = ac.country_name
WHERE ac.country_name IS NULL;

-- Check US states completeness
WITH expected_states AS (
    SELECT state_name FROM (
        VALUES ('California'), ('Texas'), ('Florida'), ('New York'),
               ('Pennsylvania'), ('Illinois'), ('Ohio'), ('Georgia'),
               ('North Carolina'), ('Michigan'), ('New Jersey'), ('Virginia')
    ) AS t(state_name)
),
actual_states AS (
    SELECT DISTINCT state as state_name
    FROM covid_states
)
SELECT 
    'geographical_completeness' as check_category,
    'missing_major_states' as check_name,
    COUNT(*) as missing_states_count,
    CASE WHEN COUNT(*) > 3 THEN 'FAIL' ELSE 'PASS' END as status,
    'Major US states should be present' as description
FROM expected_states es
LEFT JOIN actual_states ast ON es.state_name = ast.state_name
WHERE ast.state_name IS NULL;

-- =============================================================================
-- METRIC COMPLETENESS CHECKS
-- =============================================================================

-- Check for missing essential metrics in countries data
SELECT 
    'metric_completeness' as check_category,
    'countries_missing_cases' as check_name,
    COUNT(*) as missing_count,
    CASE WHEN COUNT(*) > 5 THEN 'FAIL' ELSE 'PASS' END as status,
    'Countries should have cases data' as description
FROM covid_countries
WHERE cases IS NULL;

SELECT 
    'metric_completeness' as check_category,
    'countries_missing_deaths' as check_name,
    COUNT(*) as missing_count,
    CASE WHEN COUNT(*) > 10 THEN 'FAIL' ELSE 'PASS' END as status,
    'Most countries should have deaths data' as description
FROM covid_countries
WHERE deaths IS NULL AND cases > 100;

-- Check for missing population data (important for per-capita calculations)
SELECT 
    'metric_completeness' as check_category,
    'countries_missing_population' as check_name,
    COUNT(*) as missing_count,
    CASE WHEN COUNT(*) > 20 THEN 'FAIL' ELSE 'PASS' END as status,
    'Countries should have population data for per-capita metrics' as description
FROM covid_countries
WHERE population IS NULL OR population = 0;

-- Check historical data metric completeness
WITH metric_completeness AS (
    SELECT 
        country,
        COUNT(DISTINCT CASE WHEN metric = 'cases' THEN date END) as cases_days,
        COUNT(DISTINCT CASE WHEN metric = 'deaths' THEN date END) as deaths_days,
        COUNT(DISTINCT CASE WHEN metric = 'recovered' THEN date END) as recovered_days,
        COUNT(DISTINCT date) as total_days
    FROM covid_historical
    WHERE country != 'Global'
      AND date >= CURRENT_DATE - INTERVAL '30 days'
    GROUP BY country
)
SELECT 
    'metric_completeness' as check_category,
    'historical_metrics_incomplete' as check_name,
    COUNT(*) as incomplete_countries,
    CASE WHEN COUNT(*) > 50 THEN 'FAIL' ELSE 'PASS' END as status,
    'Countries should have complete historical metrics' as description
FROM metric_completeness
WHERE cases_days < (total_days * 0.8) OR deaths_days < (total_days * 0.8);

-- =============================================================================
-- DATA COVERAGE ANALYSIS
-- =============================================================================

-- Analyze data coverage by continent
SELECT 
    'coverage_analysis' as check_category,
    'continent_coverage' as check_name,
    continent,
    COUNT(*) as countries_count,
    SUM(CASE WHEN cases > 0 THEN 1 ELSE 0 END) as countries_with_cases,
    ROUND(SUM(CASE WHEN cases > 0 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as coverage_percentage,
    CASE WHEN SUM(CASE WHEN cases > 0 THEN 1 ELSE 0 END) * 100.0 / COUNT(*) >= 80 
         THEN 'PASS' ELSE 'FAIL' END as status
FROM covid_countries
WHERE continent IS NOT NULL AND continent != ''
GROUP BY continent
ORDER BY coverage_percentage DESC;

-- Check data recency across different data types
SELECT 
    'coverage_analysis' as check_category,
    'data_recency' as check_name,
    data_source,
    data_type,
    MAX(extraction_date) as latest_extraction,
    COUNT(*) as record_count,
    EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - MAX(extraction_date))) / 3600 as hours_old,
    CASE WHEN EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - MAX(extraction_date))) / 3600 <= 24 
         THEN 'PASS' ELSE 'FAIL' END as status
FROM (
    SELECT data_source, data_type, extraction_date FROM covid_countries
    UNION ALL
    SELECT data_source, data_type, extraction_date FROM covid_global
    UNION ALL
    SELECT data_source, data_type, extraction_date FROM covid_continents
    UNION ALL
    SELECT data_source, data_type, extraction_date FROM covid_states
) all_data
GROUP BY data_source, data_type
ORDER BY latest_extraction DESC;

-- =============================================================================
-- COMPLETENESS SCORING
-- =============================================================================

-- Calculate overall completeness score
WITH completeness_metrics AS (
    -- Countries completeness
    SELECT 
        'countries' as data_type,
        COUNT(*) as total_records,
        SUM(CASE WHEN country IS NOT NULL AND country != '' THEN 1 ELSE 0 END) as complete_country_names,
        SUM(CASE WHEN cases IS NOT NULL THEN 1 ELSE 0 END) as complete_cases,
        SUM(CASE WHEN deaths IS NOT NULL THEN 1 ELSE 0 END) as complete_deaths,
        SUM(CASE WHEN population IS NOT NULL AND population > 0 THEN 1 ELSE 0 END) as complete_population
    FROM covid_countries
    
    UNION ALL
    
    -- Historical completeness
    SELECT 
        'historical' as data_type,
        COUNT(*) as total_records,
        COUNT(*) as complete_country_names,  -- All should have country names
        SUM(CASE WHEN value IS NOT NULL THEN 1 ELSE 0 END) as complete_cases,
        COUNT(*) as complete_deaths,  -- Using count as placeholder
        COUNT(*) as complete_population  -- Using count as placeholder
    FROM covid_historical
    WHERE date >= CURRENT_DATE - INTERVAL '7 days'
),
completeness_scores AS (
    SELECT 
        data_type,
        total_records,
        ROUND(complete_country_names * 100.0 / total_records, 2) as country_completeness,
        ROUND(complete_cases * 100.0 / total_records, 2) as cases_completeness,
        ROUND(complete_deaths * 100.0 / total_records, 2) as deaths_completeness,
        ROUND(complete_population * 100.0 / total_records, 2) as population_completeness
    FROM completeness_metrics
)
SELECT 
    'completeness_scoring' as check_category,
    'overall_completeness' as check_name,
    data_type,
    total_records,
    country_completeness,
    cases_completeness,
    deaths_completeness,
    population_completeness,
    ROUND((country_completeness + cases_completeness + deaths_completeness + population_completeness) / 4, 2) as avg_completeness,
    CASE 
        WHEN (country_completeness + cases_completeness + deaths_completeness + population_completeness) / 4 >= 95 THEN 'EXCELLENT'
        WHEN (country_completeness + cases_completeness + deaths_completeness + population_completeness) / 4 >= 90 THEN 'GOOD'
        WHEN (country_completeness + cases_completeness + deaths_completeness + population_completeness) / 4 >= 80 THEN 'FAIR'
        ELSE 'POOR'
    END as completeness_grade
FROM completeness_scores;

-- =============================================================================
-- COMPLETENESS SUMMARY REPORT
-- =============================================================================

-- Generate completeness summary for monitoring dashboard
WITH summary_stats AS (
    SELECT 
        COUNT(DISTINCT country) as total_countries,
        COUNT(DISTINCT continent) as total_continents,
        MAX(extraction_date) as latest_data,
        MIN(extraction_date) as earliest_data,
        COUNT(*) as total_country_records
    FROM covid_countries
    
    UNION ALL
    
    SELECT 
        COUNT(DISTINCT country) as total_countries,
        0 as total_continents,
        MAX(extraction_date) as latest_data,
        MIN(extraction_date) as earliest_data,
        COUNT(*) as total_records
    FROM covid_historical
    WHERE date >= CURRENT_DATE - INTERVAL '30 days'
)
SELECT 
    'completeness_summary' as report_type,
    CURRENT_TIMESTAMP as report_timestamp,
    'Data completeness validation completed' as status,
    json_build_object(
        'total_countries', MAX(total_countries),
        'total_continents', MAX(total_continents),
        'latest_data', MAX(latest_data),
        'data_age_hours', EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - MAX(latest_data))) / 3600,
        'total_records', SUM(total_country_records)
    ) as summary_metrics
FROM summary_stats;
