-- COVID-19 ETL Pipeline - Data Quality Validation Queries
-- SQL queries for validating data quality and completeness

-- =============================================================================
-- GLOBAL DATA QUALITY CHECKS
-- =============================================================================

-- Check for duplicate records in countries table
SELECT 
    'covid_countries' as table_name,
    'duplicate_countries' as check_type,
    COUNT(*) as duplicate_count,
    CASE WHEN COUNT(*) > 0 THEN 'FAIL' ELSE 'PASS' END as status
FROM (
    SELECT country, extraction_date, COUNT(*) as cnt
    FROM covid_countries
    GROUP BY country, extraction_date
    HAVING COUNT(*) > 1
) duplicates;

-- Check for null values in critical columns
SELECT 
    'covid_countries' as table_name,
    'null_country_names' as check_type,
    COUNT(*) as null_count,
    CASE WHEN COUNT(*) > 0 THEN 'FAIL' ELSE 'PASS' END as status
FROM covid_countries
WHERE country IS NULL OR country = '';

-- Check for negative values in case counts
SELECT 
    'covid_countries' as table_name,
    'negative_cases' as check_type,
    COUNT(*) as negative_count,
    CASE WHEN COUNT(*) > 0 THEN 'FAIL' ELSE 'PASS' END as status
FROM covid_countries
WHERE cases < 0 OR deaths < 0 OR recovered < 0;

-- Check data freshness (data should be updated within last 48 hours)
SELECT 
    'covid_countries' as table_name,
    'data_freshness' as check_type,
    COUNT(*) as stale_records,
    CASE WHEN COUNT(*) > 0 THEN 'FAIL' ELSE 'PASS' END as status
FROM covid_countries
WHERE updated < CURRENT_TIMESTAMP - INTERVAL '48 hours';

-- =============================================================================
-- BUSINESS RULE VALIDATIONS
-- =============================================================================

-- Validate that total cases >= deaths + recovered
SELECT 
    'covid_countries' as table_name,
    'cases_consistency' as check_type,
    COUNT(*) as inconsistent_records,
    CASE WHEN COUNT(*) > 0 THEN 'FAIL' ELSE 'PASS' END as status
FROM covid_countries
WHERE cases < (deaths + recovered) AND cases > 0;

-- Check for unrealistic mortality rates (> 20%)
SELECT 
    'covid_countries' as table_name,
    'mortality_rate_check' as check_type,
    COUNT(*) as high_mortality_count,
    CASE WHEN COUNT(*) > 100 THEN 'FAIL' ELSE 'PASS' END as status
FROM covid_countries
WHERE mortality_rate > 0.20 AND cases > 1000;

-- Validate active cases calculation
SELECT 
    'covid_countries' as table_name,
    'active_cases_calculation' as check_type,
    COUNT(*) as calculation_errors,
    CASE WHEN COUNT(*) > 0 THEN 'FAIL' ELSE 'PASS' END as status
FROM covid_countries
WHERE ABS(active - (cases - deaths - recovered)) > (cases * 0.01) -- Allow 1% tolerance
  AND cases > 100;

-- =============================================================================
-- HISTORICAL DATA VALIDATIONS
-- =============================================================================

-- Check for gaps in historical data
WITH date_gaps AS (
    SELECT 
        country,
        metric,
        date,
        LAG(date) OVER (PARTITION BY country, metric ORDER BY date) as prev_date,
        date - LAG(date) OVER (PARTITION BY country, metric ORDER BY date) as gap_days
    FROM covid_historical
    WHERE country != 'Global'
)
SELECT 
    'covid_historical' as table_name,
    'date_gaps' as check_type,
    COUNT(*) as gap_count,
    CASE WHEN COUNT(*) > 100 THEN 'FAIL' ELSE 'PASS' END as status
FROM date_gaps
WHERE gap_days > 1;

-- Check for negative daily changes that are too large (potential data errors)
SELECT 
    'covid_historical' as table_name,
    'large_negative_changes' as check_type,
    COUNT(*) as large_negative_count,
    CASE WHEN COUNT(*) > 50 THEN 'FAIL' ELSE 'PASS' END as status
FROM covid_historical
WHERE daily_change < -10000 AND metric = 'cases';

-- Validate historical data completeness for major countries
WITH major_countries AS (
    SELECT DISTINCT country
    FROM covid_countries
    WHERE cases > 100000
),
historical_completeness AS (
    SELECT 
        mc.country,
        COUNT(DISTINCT h.date) as days_with_data,
        DATEDIFF(day, MIN(h.date), MAX(h.date)) + 1 as expected_days
    FROM major_countries mc
    LEFT JOIN covid_historical h ON mc.country = h.country
    WHERE h.metric = 'cases'
    GROUP BY mc.country
)
SELECT 
    'covid_historical' as table_name,
    'completeness_major_countries' as check_type,
    COUNT(*) as incomplete_countries,
    CASE WHEN COUNT(*) > 5 THEN 'FAIL' ELSE 'PASS' END as status
FROM historical_completeness
WHERE (days_with_data * 1.0 / expected_days) < 0.9; -- Less than 90% complete

-- =============================================================================
-- CROSS-TABLE CONSISTENCY CHECKS
-- =============================================================================

-- Compare latest historical data with current country data
WITH latest_historical AS (
    SELECT 
        country,
        MAX(CASE WHEN metric = 'cases' THEN value END) as hist_cases,
        MAX(CASE WHEN metric = 'deaths' THEN value END) as hist_deaths,
        MAX(date) as latest_date
    FROM covid_historical
    WHERE date >= CURRENT_DATE - INTERVAL '7 days'
    GROUP BY country
),
country_comparison AS (
    SELECT 
        c.country,
        c.cases as current_cases,
        h.hist_cases,
        c.deaths as current_deaths,
        h.hist_deaths,
        ABS(c.cases - h.hist_cases) as cases_diff,
        ABS(c.deaths - h.hist_deaths) as deaths_diff
    FROM covid_countries c
    JOIN latest_historical h ON c.country = h.country
    WHERE c.cases > 1000
)
SELECT 
    'cross_table_consistency' as table_name,
    'historical_vs_current' as check_type,
    COUNT(*) as inconsistent_records,
    CASE WHEN COUNT(*) > 20 THEN 'FAIL' ELSE 'PASS' END as status
FROM country_comparison
WHERE cases_diff > (current_cases * 0.1) OR deaths_diff > (current_deaths * 0.1);

-- =============================================================================
-- DATA VOLUME AND COMPLETENESS CHECKS
-- =============================================================================

-- Check minimum record counts
SELECT 
    table_name,
    record_count,
    minimum_expected,
    CASE WHEN record_count >= minimum_expected THEN 'PASS' ELSE 'FAIL' END as status
FROM (
    SELECT 'covid_countries' as table_name, COUNT(*) as record_count, 190 as minimum_expected FROM covid_countries
    UNION ALL
    SELECT 'covid_continents' as table_name, COUNT(*) as record_count, 6 as minimum_expected FROM covid_continents
    UNION ALL
    SELECT 'covid_states' as table_name, COUNT(*) as record_count, 50 as minimum_expected FROM covid_states
    UNION ALL
    SELECT 'covid_global' as table_name, COUNT(*) as record_count, 1 as minimum_expected FROM covid_global
) volume_checks;

-- Check for recent data (within last 24 hours)
SELECT 
    table_name,
    latest_extraction,
    hours_since_extraction,
    CASE WHEN hours_since_extraction <= 24 THEN 'PASS' ELSE 'FAIL' END as status
FROM (
    SELECT 'covid_countries' as table_name, 
           MAX(extraction_date) as latest_extraction,
           EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - MAX(extraction_date))) / 3600 as hours_since_extraction
    FROM covid_countries
    UNION ALL
    SELECT 'covid_global' as table_name, 
           MAX(extraction_date) as latest_extraction,
           EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - MAX(extraction_date))) / 3600 as hours_since_extraction
    FROM covid_global
) freshness_checks;

-- =============================================================================
-- SUMMARY DATA QUALITY REPORT
-- =============================================================================

-- Generate overall data quality summary
WITH all_checks AS (
    -- Combine all the above checks into a single result set
    SELECT 'duplicate_countries' as check_name, 
           CASE WHEN COUNT(*) > 0 THEN 'FAIL' ELSE 'PASS' END as status
    FROM (SELECT country, extraction_date FROM covid_countries GROUP BY country, extraction_date HAVING COUNT(*) > 1) d
    
    UNION ALL
    
    SELECT 'null_country_names' as check_name,
           CASE WHEN COUNT(*) > 0 THEN 'FAIL' ELSE 'PASS' END as status
    FROM covid_countries WHERE country IS NULL OR country = ''
    
    UNION ALL
    
    SELECT 'negative_values' as check_name,
           CASE WHEN COUNT(*) > 0 THEN 'FAIL' ELSE 'PASS' END as status
    FROM covid_countries WHERE cases < 0 OR deaths < 0 OR recovered < 0
    
    UNION ALL
    
    SELECT 'minimum_record_count' as check_name,
           CASE WHEN COUNT(*) >= 190 THEN 'PASS' ELSE 'FAIL' END as status
    FROM covid_countries
)
SELECT 
    COUNT(*) as total_checks,
    SUM(CASE WHEN status = 'PASS' THEN 1 ELSE 0 END) as passed_checks,
    SUM(CASE WHEN status = 'FAIL' THEN 1 ELSE 0 END) as failed_checks,
    ROUND(SUM(CASE WHEN status = 'PASS' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as pass_percentage,
    CASE 
        WHEN SUM(CASE WHEN status = 'FAIL' THEN 1 ELSE 0 END) = 0 THEN 'EXCELLENT'
        WHEN SUM(CASE WHEN status = 'FAIL' THEN 1 ELSE 0 END) <= 2 THEN 'GOOD'
        WHEN SUM(CASE WHEN status = 'FAIL' THEN 1 ELSE 0 END) <= 5 THEN 'FAIR'
        ELSE 'POOR'
    END as overall_quality
FROM all_checks;
