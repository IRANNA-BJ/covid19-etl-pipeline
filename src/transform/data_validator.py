"""
Data validation utilities for COVID-19 ETL Pipeline.
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Any, Tuple, Optional
from datetime import datetime, timedelta
import re

from ..utils.config import config
from ..utils.logger import get_logger


class DataValidator:
    """Data validation and quality checks for COVID-19 data."""
    
    def __init__(self):
        """Initialize validator."""
        self.logger = get_logger(__name__)
        self.quality_config = config.get_data_quality_config()
        
        # Validation thresholds
        self.max_null_percentage = self.quality_config.get('max_null_percentage', 0.1)
        self.min_record_count = self.quality_config.get('min_record_count', 100)
    
    def validate_dataframe(self, df: pd.DataFrame, data_type: str) -> Dict[str, Any]:
        """
        Comprehensive validation of DataFrame.
        
        Args:
            df: DataFrame to validate
            data_type: Type of data (global, countries, etc.)
            
        Returns:
            Validation results dictionary
        """
        self.logger.info(f"Validating {data_type} data with {len(df)} records")
        
        validation_results = {
            'data_type': data_type,
            'validation_timestamp': datetime.now().isoformat(),
            'record_count': len(df),
            'column_count': len(df.columns),
            'is_valid': True,
            'errors': [],
            'warnings': [],
            'quality_metrics': {}
        }
        
        try:
            # Basic structure validation
            self._validate_structure(df, validation_results)
            
            # Data type specific validation
            if data_type == 'global':
                self._validate_global_data(df, validation_results)
            elif data_type == 'countries':
                self._validate_countries_data(df, validation_results)
            elif data_type == 'historical':
                self._validate_historical_data(df, validation_results)
            elif data_type == 'vaccines':
                self._validate_vaccine_data(df, validation_results)
            
            # Common data quality checks
            self._validate_data_quality(df, validation_results)
            
            # Calculate quality score
            validation_results['quality_score'] = self._calculate_quality_score(validation_results)
            
            self.logger.info(f"Validation completed for {data_type}. "
                           f"Quality score: {validation_results['quality_score']:.2f}")
            
            return validation_results
            
        except Exception as e:
            self.logger.error(f"Validation failed for {data_type}: {str(e)}")
            validation_results['is_valid'] = False
            validation_results['errors'].append(f"Validation error: {str(e)}")
            return validation_results
    
    def _validate_structure(self, df: pd.DataFrame, results: Dict[str, Any]) -> None:
        """Validate basic DataFrame structure."""
        # Check if DataFrame is empty
        if df.empty:
            results['errors'].append("DataFrame is empty")
            results['is_valid'] = False
            return
        
        # Check minimum record count
        if len(df) < self.min_record_count:
            results['warnings'].append(
                f"Record count ({len(df)}) below minimum threshold ({self.min_record_count})"
            )
        
        # Check for duplicate rows
        duplicate_count = df.duplicated().sum()
        if duplicate_count > 0:
            results['warnings'].append(f"Found {duplicate_count} duplicate rows")
        
        results['quality_metrics']['duplicate_percentage'] = duplicate_count / len(df)
    
    def _validate_global_data(self, df: pd.DataFrame, results: Dict[str, Any]) -> None:
        """Validate global COVID-19 data."""
        required_columns = ['cases', 'deaths', 'recovered', 'active']
        
        # Check required columns
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            results['errors'].append(f"Missing required columns: {missing_columns}")
            results['is_valid'] = False
        
        # Validate numeric columns
        numeric_columns = ['cases', 'deaths', 'recovered', 'active', 'todayCases', 'todayDeaths']
        for col in numeric_columns:
            if col in df.columns:
                if not pd.api.types.is_numeric_dtype(df[col]):
                    results['errors'].append(f"Column {col} should be numeric")
                    results['is_valid'] = False
                
                # Check for negative values (except for some cases where it might be valid)
                if col in ['cases', 'deaths', 'recovered'] and (df[col] < 0).any():
                    results['warnings'].append(f"Found negative values in {col}")
    
    def _validate_countries_data(self, df: pd.DataFrame, results: Dict[str, Any]) -> None:
        """Validate countries COVID-19 data."""
        required_columns = ['country', 'cases', 'deaths']
        
        # Check required columns
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            results['errors'].append(f"Missing required columns: {missing_columns}")
            results['is_valid'] = False
        
        # Validate country names
        if 'country' in df.columns:
            null_countries = df['country'].isnull().sum()
            if null_countries > 0:
                results['errors'].append(f"Found {null_countries} null country names")
                results['is_valid'] = False
            
            # Check for duplicate countries
            duplicate_countries = df['country'].duplicated().sum()
            if duplicate_countries > 0:
                results['warnings'].append(f"Found {duplicate_countries} duplicate countries")
        
        # Validate ISO codes if present
        if 'country_iso2' in df.columns:
            invalid_iso2 = df[df['country_iso2'].notna()]['country_iso2'].str.len() != 2
            if invalid_iso2.any():
                results['warnings'].append("Found invalid ISO2 codes")
        
        if 'country_iso3' in df.columns:
            invalid_iso3 = df[df['country_iso3'].notna()]['country_iso3'].str.len() != 3
            if invalid_iso3.any():
                results['warnings'].append("Found invalid ISO3 codes")
    
    def _validate_historical_data(self, df: pd.DataFrame, results: Dict[str, Any]) -> None:
        """Validate historical COVID-19 data."""
        required_columns = ['date', 'metric', 'value', 'country']
        
        # Check required columns
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            results['errors'].append(f"Missing required columns: {missing_columns}")
            results['is_valid'] = False
        
        # Validate date column
        if 'date' in df.columns:
            try:
                pd.to_datetime(df['date'])
            except Exception:
                results['errors'].append("Invalid date format in date column")
                results['is_valid'] = False
            
            # Check date range
            if not df.empty:
                min_date = pd.to_datetime(df['date']).min()
                max_date = pd.to_datetime(df['date']).max()
                
                # COVID-19 started around December 2019
                if min_date < pd.Timestamp('2019-12-01'):
                    results['warnings'].append(f"Dates before COVID-19 outbreak: {min_date}")
                
                # Check if dates are too far in the future
                if max_date > pd.Timestamp.now() + timedelta(days=1):
                    results['warnings'].append(f"Future dates found: {max_date}")
        
        # Validate metrics
        if 'metric' in df.columns:
            valid_metrics = ['cases', 'deaths', 'recovered']
            invalid_metrics = df[~df['metric'].isin(valid_metrics)]['metric'].unique()
            if len(invalid_metrics) > 0:
                results['warnings'].append(f"Unknown metrics found: {invalid_metrics}")
    
    def _validate_vaccine_data(self, df: pd.DataFrame, results: Dict[str, Any]) -> None:
        """Validate vaccine data."""
        # Check for reasonable vaccination numbers
        if 'timeline' in df.columns:
            # This would need more specific validation based on actual vaccine data structure
            pass
    
    def _validate_data_quality(self, df: pd.DataFrame, results: Dict[str, Any]) -> None:
        """Perform general data quality checks."""
        # Check null percentages
        null_percentages = df.isnull().sum() / len(df)
        high_null_columns = null_percentages[null_percentages > self.max_null_percentage]
        
        if not high_null_columns.empty:
            results['warnings'].append(
                f"High null percentage in columns: {high_null_columns.to_dict()}"
            )
        
        results['quality_metrics']['null_percentages'] = null_percentages.to_dict()
        
        # Check data types
        results['quality_metrics']['data_types'] = df.dtypes.astype(str).to_dict()
        
        # Memory usage
        results['quality_metrics']['memory_usage_mb'] = df.memory_usage(deep=True).sum() / 1024 / 1024
        
        # Check for outliers in numeric columns
        numeric_columns = df.select_dtypes(include=[np.number]).columns
        outlier_info = {}
        
        for col in numeric_columns:
            if col in df.columns and not df[col].empty:
                Q1 = df[col].quantile(0.25)
                Q3 = df[col].quantile(0.75)
                IQR = Q3 - Q1
                lower_bound = Q1 - 1.5 * IQR
                upper_bound = Q3 + 1.5 * IQR
                
                outliers = df[(df[col] < lower_bound) | (df[col] > upper_bound)]
                outlier_percentage = len(outliers) / len(df)
                
                outlier_info[col] = {
                    'count': len(outliers),
                    'percentage': outlier_percentage,
                    'lower_bound': lower_bound,
                    'upper_bound': upper_bound
                }
        
        results['quality_metrics']['outliers'] = outlier_info
    
    def _calculate_quality_score(self, results: Dict[str, Any]) -> float:
        """Calculate overall data quality score (0-100)."""
        score = 100.0
        
        # Deduct points for errors
        error_count = len(results['errors'])
        score -= error_count * 20  # 20 points per error
        
        # Deduct points for warnings
        warning_count = len(results['warnings'])
        score -= warning_count * 5  # 5 points per warning
        
        # Deduct points for high null percentages
        null_percentages = results['quality_metrics'].get('null_percentages', {})
        for col, null_pct in null_percentages.items():
            if null_pct > self.max_null_percentage:
                score -= (null_pct - self.max_null_percentage) * 50
        
        # Deduct points for high duplicate percentage
        duplicate_pct = results['quality_metrics'].get('duplicate_percentage', 0)
        if duplicate_pct > 0.05:  # More than 5% duplicates
            score -= duplicate_pct * 30
        
        return max(0.0, min(100.0, score))
    
    def validate_business_rules(self, df: pd.DataFrame, data_type: str) -> List[str]:
        """
        Validate business-specific rules for COVID-19 data.
        
        Args:
            df: DataFrame to validate
            data_type: Type of data
            
        Returns:
            List of business rule violations
        """
        violations = []
        
        try:
            if data_type in ['global', 'countries']:
                # Cases should be >= deaths + recovered
                if all(col in df.columns for col in ['cases', 'deaths', 'recovered']):
                    invalid_totals = df[df['cases'] < (df['deaths'] + df['recovered'])]
                    if not invalid_totals.empty:
                        violations.append(
                            f"Found {len(invalid_totals)} records where cases < deaths + recovered"
                        )
                
                # Active cases should equal cases - deaths - recovered (approximately)
                if all(col in df.columns for col in ['cases', 'deaths', 'recovered', 'active']):
                    calculated_active = df['cases'] - df['deaths'] - df['recovered']
                    difference = abs(df['active'] - calculated_active)
                    
                    # Allow some tolerance for data inconsistencies
                    tolerance = df['cases'] * 0.01  # 1% tolerance
                    significant_differences = difference > tolerance
                    
                    if significant_differences.any():
                        violations.append(
                            f"Found {significant_differences.sum()} records with "
                            f"inconsistent active case calculations"
                        )
                
                # Death rate should be reasonable (typically < 10%)
                if all(col in df.columns for col in ['cases', 'deaths']):
                    death_rate = df['deaths'] / df['cases']
                    high_death_rate = death_rate > 0.1  # More than 10%
                    
                    if high_death_rate.any():
                        violations.append(
                            f"Found {high_death_rate.sum()} records with "
                            f"unusually high death rates (>10%)"
                        )
            
            return violations
            
        except Exception as e:
            self.logger.error(f"Error validating business rules: {str(e)}")
            return [f"Business rule validation error: {str(e)}"]
    
    def generate_validation_report(self, validation_results: List[Dict[str, Any]]) -> str:
        """
        Generate a comprehensive validation report.
        
        Args:
            validation_results: List of validation results for different datasets
            
        Returns:
            Formatted validation report
        """
        report_lines = [
            "COVID-19 Data Validation Report",
            "=" * 40,
            f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            ""
        ]
        
        total_datasets = len(validation_results)
        valid_datasets = sum(1 for r in validation_results if r['is_valid'])
        
        report_lines.extend([
            f"Total Datasets: {total_datasets}",
            f"Valid Datasets: {valid_datasets}",
            f"Invalid Datasets: {total_datasets - valid_datasets}",
            ""
        ])
        
        for result in validation_results:
            report_lines.extend([
                f"Dataset: {result['data_type']}",
                "-" * 20,
                f"Records: {result['record_count']:,}",
                f"Columns: {result['column_count']}",
                f"Valid: {'Yes' if result['is_valid'] else 'No'}",
                f"Quality Score: {result.get('quality_score', 0):.1f}/100",
                ""
            ])
            
            if result['errors']:
                report_lines.append("Errors:")
                for error in result['errors']:
                    report_lines.append(f"  - {error}")
                report_lines.append("")
            
            if result['warnings']:
                report_lines.append("Warnings:")
                for warning in result['warnings']:
                    report_lines.append(f"  - {warning}")
                report_lines.append("")
            
            report_lines.append("")
        
        return "\n".join(report_lines)
