"""
Data transformation and cleaning for COVID-19 ETL Pipeline.
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple
import re
from pathlib import Path

from .data_validator import DataValidator
from ..utils.config import config
from ..utils.logger import get_logger


class CovidDataTransformer:
    """Main class for transforming and cleaning COVID-19 data."""
    
    def __init__(self):
        """Initialize transformer."""
        self.logger = get_logger(__name__)
        self.validator = DataValidator()
        self.storage_config = config.get_storage_config()
        
        # Country code mappings for standardization
        self.country_mappings = self._load_country_mappings()
    
    def transform_all_data(self, raw_data: Dict[str, pd.DataFrame]) -> Dict[str, pd.DataFrame]:
        """
        Transform all extracted COVID-19 data.
        
        Args:
            raw_data: Dictionary of raw DataFrames
            
        Returns:
            Dictionary of transformed DataFrames
        """
        self.logger.info("Starting data transformation for all datasets")
        
        transformed_data = {}
        
        try:
            for data_type, df in raw_data.items():
                if df.empty:
                    self.logger.warning(f"Skipping empty dataset: {data_type}")
                    continue
                
                self.logger.info(f"Transforming {data_type} data ({len(df)} records)")
                
                # Apply common transformations
                df_transformed = self._apply_common_transformations(df, data_type)
                
                # Apply data-type specific transformations
                if data_type == 'global':
                    df_transformed = self.transform_global_data(df_transformed)
                elif data_type == 'countries':
                    df_transformed = self.transform_countries_data(df_transformed)
                elif data_type == 'continents':
                    df_transformed = self.transform_continents_data(df_transformed)
                elif data_type == 'states':
                    df_transformed = self.transform_states_data(df_transformed)
                elif data_type == 'vaccines':
                    df_transformed = self.transform_vaccine_data(df_transformed)
                elif data_type == 'historical':
                    df_transformed = self.transform_historical_data(df_transformed)
                
                # Validate transformed data
                validation_result = self.validator.validate_dataframe(df_transformed, data_type)
                if not validation_result['is_valid']:
                    self.logger.error(f"Validation failed for {data_type}: {validation_result['errors']}")
                
                transformed_data[data_type] = df_transformed
                self.logger.info(f"Successfully transformed {data_type} data")
            
            self.logger.info(f"Completed transformation for {len(transformed_data)} datasets")
            return transformed_data
            
        except Exception as e:
            self.logger.error(f"Error during data transformation: {str(e)}")
            raise
    
    def _apply_common_transformations(self, df: pd.DataFrame, data_type: str) -> pd.DataFrame:
        """Apply common transformations to all datasets."""
        df = df.copy()
        
        # Standardize column names
        df = self._standardize_column_names(df)
        
        # Handle missing values
        df = self._handle_missing_values(df, data_type)
        
        # Convert data types
        df = self._convert_data_types(df)
        
        # Add derived columns
        df = self._add_derived_columns(df, data_type)
        
        # Remove duplicates
        df = self._remove_duplicates(df, data_type)
        
        return df
    
    def _standardize_column_names(self, df: pd.DataFrame) -> pd.DataFrame:
        """Standardize column names."""
        df = df.copy()
        
        # Convert to lowercase and replace spaces/special chars with underscores
        df.columns = df.columns.str.lower().str.replace(r'[^a-zA-Z0-9]', '_', regex=True)
        
        # Remove multiple consecutive underscores
        df.columns = df.columns.str.replace(r'_+', '_', regex=True)
        
        # Remove leading/trailing underscores
        df.columns = df.columns.str.strip('_')
        
        return df
    
    def _handle_missing_values(self, df: pd.DataFrame, data_type: str) -> pd.DataFrame:
        """Handle missing values based on data type and column characteristics."""
        df = df.copy()
        
        # Numeric columns: fill with 0 for count-based metrics
        numeric_cols = df.select_dtypes(include=[np.number]).columns
        count_columns = [col for col in numeric_cols if any(keyword in col.lower() 
                        for keyword in ['cases', 'deaths', 'recovered', 'tests', 'population'])]
        
        for col in count_columns:
            df[col] = df[col].fillna(0)
        
        # Rate/percentage columns: fill with median or 0
        rate_columns = [col for col in numeric_cols if any(keyword in col.lower() 
                       for keyword in ['rate', 'per', 'ratio', 'percentage'])]
        
        for col in rate_columns:
            if col in df.columns:
                df[col] = df[col].fillna(df[col].median())
        
        # String columns: fill with 'Unknown' or appropriate default
        string_cols = df.select_dtypes(include=['object']).columns
        for col in string_cols:
            if 'country' in col.lower():
                df[col] = df[col].fillna('Unknown')
            elif 'continent' in col.lower():
                df[col] = df[col].fillna('Unknown')
            else:
                df[col] = df[col].fillna('N/A')
        
        return df
    
    def _convert_data_types(self, df: pd.DataFrame) -> pd.DataFrame:
        """Convert columns to appropriate data types."""
        df = df.copy()
        
        # Convert timestamp columns
        timestamp_cols = [col for col in df.columns if 'updated' in col.lower() or 'date' in col.lower()]
        for col in timestamp_cols:
            if col in df.columns:
                try:
                    if df[col].dtype == 'object':
                        df[col] = pd.to_datetime(df[col], errors='coerce')
                    elif pd.api.types.is_numeric_dtype(df[col]):
                        # Assume Unix timestamp in milliseconds
                        df[col] = pd.to_datetime(df[col], unit='ms', errors='coerce')
                except Exception as e:
                    self.logger.warning(f"Could not convert {col} to datetime: {str(e)}")
        
        # Convert numeric columns
        numeric_candidates = [col for col in df.columns if any(keyword in col.lower() 
                             for keyword in ['cases', 'deaths', 'recovered', 'tests', 'population', 
                                           'active', 'critical', 'today'])]
        
        for col in numeric_candidates:
            if col in df.columns and df[col].dtype == 'object':
                try:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
                except Exception as e:
                    self.logger.warning(f"Could not convert {col} to numeric: {str(e)}")
        
        return df
    
    def _add_derived_columns(self, df: pd.DataFrame, data_type: str) -> pd.DataFrame:
        """Add derived columns for analysis."""
        df = df.copy()
        
        # Add processing metadata
        df['processed_at'] = datetime.now()
        df['data_freshness_hours'] = None
        
        if 'updated' in df.columns:
            df['data_freshness_hours'] = (datetime.now() - df['updated']).dt.total_seconds() / 3600
        
        # Add calculated metrics for case data
        if data_type in ['global', 'countries', 'continents', 'states']:
            # Mortality rate
            if all(col in df.columns for col in ['deaths', 'cases']):
                df['mortality_rate'] = np.where(df['cases'] > 0, df['deaths'] / df['cases'], 0)
            
            # Recovery rate
            if all(col in df.columns for col in ['recovered', 'cases']):
                df['recovery_rate'] = np.where(df['cases'] > 0, df['recovered'] / df['cases'], 0)
            
            # Active case rate
            if all(col in df.columns for col in ['active', 'cases']):
                df['active_rate'] = np.where(df['cases'] > 0, df['active'] / df['cases'], 0)
            
            # Cases per million (if population data available)
            if all(col in df.columns for col in ['cases', 'population']):
                df['cases_per_million'] = np.where(df['population'] > 0, 
                                                 (df['cases'] / df['population']) * 1000000, 0)
            
            # Deaths per million
            if all(col in df.columns for col in ['deaths', 'population']):
                df['deaths_per_million'] = np.where(df['population'] > 0, 
                                                  (df['deaths'] / df['population']) * 1000000, 0)
        
        return df
    
    def _remove_duplicates(self, df: pd.DataFrame, data_type: str) -> pd.DataFrame:
        """Remove duplicate records."""
        initial_count = len(df)
        
        if data_type == 'countries':
            # Remove duplicates based on country name
            df = df.drop_duplicates(subset=['country'], keep='last')
        elif data_type == 'historical':
            # Remove duplicates based on date, country, and metric
            df = df.drop_duplicates(subset=['date', 'country', 'metric'], keep='last')
        else:
            # General duplicate removal
            df = df.drop_duplicates(keep='last')
        
        removed_count = initial_count - len(df)
        if removed_count > 0:
            self.logger.info(f"Removed {removed_count} duplicate records from {data_type}")
        
        return df
    
    def transform_global_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transform global COVID-19 data."""
        df = df.copy()
        
        # Add global identifier
        df['region_type'] = 'global'
        df['region_name'] = 'World'
        
        # Ensure required columns exist
        required_columns = ['cases', 'deaths', 'recovered', 'active']
        for col in required_columns:
            if col not in df.columns:
                df[col] = 0
        
        return df
    
    def transform_countries_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transform countries COVID-19 data."""
        df = df.copy()
        
        # Standardize country names
        df = self._standardize_country_names(df)
        
        # Add region type
        df['region_type'] = 'country'
        
        # Extract and clean country info
        if 'country' in df.columns:
            df['region_name'] = df['country']
        
        # Handle country-specific data cleaning
        df = self._clean_country_specific_data(df)
        
        return df
    
    def transform_continents_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transform continents COVID-19 data."""
        df = df.copy()
        
        # Add region type
        df['region_type'] = 'continent'
        
        if 'continent' in df.columns:
            df['region_name'] = df['continent']
        
        return df
    
    def transform_states_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transform US states COVID-19 data."""
        df = df.copy()
        
        # Add region type
        df['region_type'] = 'state'
        df['country'] = 'USA'
        
        if 'state' in df.columns:
            df['region_name'] = df['state']
        
        return df
    
    def transform_vaccine_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transform vaccine data."""
        df = df.copy()
        
        # Add data type identifier
        df['data_category'] = 'vaccination'
        
        return df
    
    def transform_historical_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transform historical COVID-19 data."""
        df = df.copy()
        
        # Ensure date column is properly formatted
        if 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date'])
            
            # Add time-based features
            df['year'] = df['date'].dt.year
            df['month'] = df['date'].dt.month
            df['day_of_week'] = df['date'].dt.dayofweek
            df['week_of_year'] = df['date'].dt.isocalendar().week
        
        # Calculate daily changes if we have time series data
        if all(col in df.columns for col in ['date', 'country', 'metric', 'value']):
            df = df.sort_values(['country', 'metric', 'date'])
            df['daily_change'] = df.groupby(['country', 'metric'])['value'].diff()
            df['daily_change_pct'] = df.groupby(['country', 'metric'])['value'].pct_change()
            
            # Calculate rolling averages
            df['value_7day_avg'] = df.groupby(['country', 'metric'])['value'].rolling(
                window=7, min_periods=1).mean().reset_index(0, drop=True)
            
            df['daily_change_7day_avg'] = df.groupby(['country', 'metric'])['daily_change'].rolling(
                window=7, min_periods=1).mean().reset_index(0, drop=True)
        
        return df
    
    def _standardize_country_names(self, df: pd.DataFrame) -> pd.DataFrame:
        """Standardize country names using mappings."""
        if 'country' not in df.columns:
            return df
        
        df = df.copy()
        
        # Apply country name mappings
        df['country'] = df['country'].replace(self.country_mappings)
        
        # Clean country names
        df['country'] = df['country'].str.strip()
        df['country'] = df['country'].str.title()
        
        return df
    
    def _clean_country_specific_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean country-specific data issues."""
        df = df.copy()
        
        # Handle specific data quality issues
        # Remove countries with invalid data
        if 'country' in df.columns:
            invalid_countries = ['', 'null', 'undefined', 'N/A']
            df = df[~df['country'].isin(invalid_countries)]
        
        # Fix negative values that shouldn't be negative
        numeric_cols = ['cases', 'deaths', 'recovered', 'active']
        for col in numeric_cols:
            if col in df.columns:
                # Set negative values to 0 (or previous valid value)
                df[col] = df[col].clip(lower=0)
        
        return df
    
    def _load_country_mappings(self) -> Dict[str, str]:
        """Load country name mappings for standardization."""
        # Common country name variations and their standard names
        mappings = {
            'US': 'United States',
            'USA': 'United States',
            'United States of America': 'United States',
            'UK': 'United Kingdom',
            'Britain': 'United Kingdom',
            'Great Britain': 'United Kingdom',
            'South Korea': 'Korea, South',
            'North Korea': 'Korea, North',
            'Russia': 'Russian Federation',
            'Iran': 'Iran, Islamic Republic of',
            'Syria': 'Syrian Arab Republic',
            'Venezuela': 'Venezuela, Bolivarian Republic of',
            'Bolivia': 'Bolivia, Plurinational State of',
            'Tanzania': 'Tanzania, United Republic of',
            'Moldova': 'Moldova, Republic of',
            'Macedonia': 'North Macedonia',
            'Czech Republic': 'Czechia',
            'Myanmar': 'Myanmar',
            'Burma': 'Myanmar',
            'Congo (Kinshasa)': 'Congo, Democratic Republic of the',
            'Congo (Brazzaville)': 'Congo',
            'Ivory Coast': "Cote d'Ivoire",
        }
        
        return mappings
    
    def calculate_growth_rates(self, df: pd.DataFrame) -> pd.DataFrame:
        """Calculate growth rates for time series data."""
        if 'historical' not in df.get('data_type', ''):
            return df
        
        df = df.copy()
        
        if all(col in df.columns for col in ['date', 'country', 'metric', 'value']):
            df = df.sort_values(['country', 'metric', 'date'])
            
            # Calculate various growth rates
            df['growth_rate_1day'] = df.groupby(['country', 'metric'])['value'].pct_change()
            df['growth_rate_7day'] = df.groupby(['country', 'metric'])['value'].pct_change(periods=7)
            df['growth_rate_14day'] = df.groupby(['country', 'metric'])['value'].pct_change(periods=14)
            
            # Calculate doubling time (days for value to double at current growth rate)
            df['doubling_time_days'] = np.where(
                df['growth_rate_1day'] > 0,
                np.log(2) / np.log(1 + df['growth_rate_1day']),
                np.inf
            )
        
        return df
    
    def add_population_metrics(self, df: pd.DataFrame, population_data: Optional[pd.DataFrame] = None) -> pd.DataFrame:
        """Add population-based metrics if population data is available."""
        if population_data is None or 'population' not in df.columns:
            return df
        
        df = df.copy()
        
        # Calculate per capita metrics
        if 'cases' in df.columns and 'population' in df.columns:
            df['cases_per_100k'] = (df['cases'] / df['population']) * 100000
        
        if 'deaths' in df.columns and 'population' in df.columns:
            df['deaths_per_100k'] = (df['deaths'] / df['population']) * 100000
        
        if 'tests' in df.columns and 'population' in df.columns:
            df['tests_per_100k'] = (df['tests'] / df['population']) * 100000
        
        return df
    
    def save_transformed_data(self, data: Dict[str, pd.DataFrame],
                            output_dir: str = "data/processed") -> Dict[str, str]:
        """
        Save transformed data to files.
        
        Args:
            data: Dictionary of transformed DataFrames
            output_dir: Output directory path
            
        Returns:
            Dictionary mapping data type to file path
        """
        try:
            output_path = Path(output_dir)
            output_path.mkdir(parents=True, exist_ok=True)
            
            saved_files = {}
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            
            for data_type, df in data.items():
                if df.empty:
                    self.logger.warning(f"Skipping empty dataset: {data_type}")
                    continue
                
                # Determine file format
                file_format = self.storage_config.get('file_format', 'parquet')
                
                if file_format == 'parquet':
                    filename = f"covid_{data_type}_processed_{timestamp}.parquet"
                    filepath = output_path / filename
                    df.to_parquet(filepath, index=False)
                else:
                    filename = f"covid_{data_type}_processed_{timestamp}.csv"
                    filepath = output_path / filename
                    df.to_csv(filepath, index=False)
                
                saved_files[data_type] = str(filepath)
                self.logger.info(f"Saved processed {data_type} data to {filepath}")
            
            return saved_files
            
        except Exception as e:
            self.logger.error(f"Error saving transformed data: {str(e)}")
            raise
