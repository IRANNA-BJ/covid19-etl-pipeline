"""
COVID-19 data extractor using disease.sh API.
"""

import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple
import json
from pathlib import Path

from .api_client import CovidAPIClient
from ..utils.config import config
from ..utils.logger import get_logger


class CovidDataExtractor:
    """Main class for extracting COVID-19 data from disease.sh API."""
    
    def __init__(self):
        """Initialize the extractor."""
        self.logger = get_logger(__name__)
        self.api_client = CovidAPIClient()
        self.storage_config = config.get_storage_config()
        
    def extract_all_data(self, include_historical: bool = True,
                        historical_days: int = 30) -> Dict[str, pd.DataFrame]:
        """
        Extract all COVID-19 data types.
        
        Args:
            include_historical: Whether to include historical data
            historical_days: Number of days of historical data
            
        Returns:
            Dictionary of DataFrames with extracted data
        """
        self.logger.info("Starting complete COVID-19 data extraction")
        
        extracted_data = {}
        
        try:
            # Extract global data
            self.logger.info("Extracting global data")
            global_data = self.extract_global_data()
            extracted_data['global'] = global_data
            
            # Extract countries data
            self.logger.info("Extracting countries data")
            countries_data = self.extract_countries_data()
            extracted_data['countries'] = countries_data
            
            # Extract continents data
            self.logger.info("Extracting continents data")
            continents_data = self.extract_continents_data()
            extracted_data['continents'] = continents_data
            
            # Extract states data (USA)
            self.logger.info("Extracting US states data")
            states_data = self.extract_states_data()
            extracted_data['states'] = states_data
            
            # Extract vaccine data
            self.logger.info("Extracting vaccine data")
            vaccine_data = self.extract_vaccine_data()
            extracted_data['vaccines'] = vaccine_data
            
            # Extract historical data if requested
            if include_historical:
                self.logger.info(f"Extracting historical data ({historical_days} days)")
                historical_data = self.extract_historical_data(days=historical_days)
                extracted_data['historical'] = historical_data
            
            self.logger.info(f"Successfully extracted {len(extracted_data)} datasets")
            return extracted_data
            
        except Exception as e:
            self.logger.error(f"Error during data extraction: {str(e)}")
            raise
    
    def extract_global_data(self) -> pd.DataFrame:
        """
        Extract global COVID-19 statistics.
        
        Returns:
            DataFrame with global data
        """
        try:
            data = self.api_client.get_global_data()
            
            # Convert to DataFrame
            df = pd.DataFrame([data])
            
            # Add extraction metadata
            df['extraction_date'] = datetime.now()
            df['data_source'] = 'disease.sh'
            df['data_type'] = 'global'
            
            # Convert timestamp fields
            if 'updated' in df.columns:
                df['updated'] = pd.to_datetime(df['updated'], unit='ms')
            
            self.logger.info(f"Extracted global data with {len(df)} records")
            return df
            
        except Exception as e:
            self.logger.error(f"Error extracting global data: {str(e)}")
            raise
    
    def extract_countries_data(self) -> pd.DataFrame:
        """
        Extract COVID-19 data for all countries.
        
        Returns:
            DataFrame with countries data
        """
        try:
            data = self.api_client.get_countries_data()
            
            # Convert to DataFrame
            df = pd.DataFrame(data)
            
            # Add extraction metadata
            df['extraction_date'] = datetime.now()
            df['data_source'] = 'disease.sh'
            df['data_type'] = 'countries'
            
            # Convert timestamp fields
            if 'updated' in df.columns:
                df['updated'] = pd.to_datetime(df['updated'], unit='ms')
            
            # Flatten nested country info
            if 'countryInfo' in df.columns:
                country_info = pd.json_normalize(df['countryInfo'])
                country_info.columns = ['country_' + col for col in country_info.columns]
                df = pd.concat([df.drop('countryInfo', axis=1), country_info], axis=1)
            
            self.logger.info(f"Extracted countries data with {len(df)} records")
            return df
            
        except Exception as e:
            self.logger.error(f"Error extracting countries data: {str(e)}")
            raise
    
    def extract_continents_data(self) -> pd.DataFrame:
        """
        Extract COVID-19 data by continent.
        
        Returns:
            DataFrame with continents data
        """
        try:
            data = self.api_client.get_continents_data()
            
            # Convert to DataFrame
            df = pd.DataFrame(data)
            
            # Add extraction metadata
            df['extraction_date'] = datetime.now()
            df['data_source'] = 'disease.sh'
            df['data_type'] = 'continents'
            
            # Convert timestamp fields
            if 'updated' in df.columns:
                df['updated'] = pd.to_datetime(df['updated'], unit='ms')
            
            self.logger.info(f"Extracted continents data with {len(df)} records")
            return df
            
        except Exception as e:
            self.logger.error(f"Error extracting continents data: {str(e)}")
            raise
    
    def extract_states_data(self) -> pd.DataFrame:
        """
        Extract COVID-19 data for US states.
        
        Returns:
            DataFrame with states data
        """
        try:
            data = self.api_client.get_states_data()
            
            # Convert to DataFrame
            df = pd.DataFrame(data)
            
            # Add extraction metadata
            df['extraction_date'] = datetime.now()
            df['data_source'] = 'disease.sh'
            df['data_type'] = 'states'
            
            # Convert timestamp fields
            if 'updated' in df.columns:
                df['updated'] = pd.to_datetime(df['updated'], unit='ms')
            
            self.logger.info(f"Extracted states data with {len(df)} records")
            return df
            
        except Exception as e:
            self.logger.error(f"Error extracting states data: {str(e)}")
            raise
    
    def extract_vaccine_data(self) -> pd.DataFrame:
        """
        Extract COVID-19 vaccine data.
        
        Returns:
            DataFrame with vaccine data
        """
        try:
            data = self.api_client.get_vaccine_data()
            
            # Convert to DataFrame
            df = pd.DataFrame(data)
            
            # Add extraction metadata
            df['extraction_date'] = datetime.now()
            df['data_source'] = 'disease.sh'
            df['data_type'] = 'vaccines'
            
            self.logger.info(f"Extracted vaccine data with {len(df)} records")
            return df
            
        except Exception as e:
            self.logger.error(f"Error extracting vaccine data: {str(e)}")
            raise
    
    def extract_historical_data(self, days: int = 30) -> pd.DataFrame:
        """
        Extract historical COVID-19 data.
        
        Args:
            days: Number of days of historical data
            
        Returns:
            DataFrame with historical data
        """
        try:
            # Get global historical data
            data = self.api_client.get_historical_data(days=days)
            
            # Convert nested time series data to DataFrame
            records = []
            
            if 'cases' in data:
                for date_str, value in data['cases'].items():
                    records.append({
                        'date': date_str,
                        'metric': 'cases',
                        'value': value,
                        'country': 'Global'
                    })
            
            if 'deaths' in data:
                for date_str, value in data['deaths'].items():
                    records.append({
                        'date': date_str,
                        'metric': 'deaths',
                        'value': value,
                        'country': 'Global'
                    })
            
            if 'recovered' in data:
                for date_str, value in data['recovered'].items():
                    records.append({
                        'date': date_str,
                        'metric': 'recovered',
                        'value': value,
                        'country': 'Global'
                    })
            
            df = pd.DataFrame(records)
            
            if not df.empty:
                # Convert date column
                df['date'] = pd.to_datetime(df['date'])
                
                # Add extraction metadata
                df['extraction_date'] = datetime.now()
                df['data_source'] = 'disease.sh'
                df['data_type'] = 'historical'
            
            self.logger.info(f"Extracted historical data with {len(df)} records")
            return df
            
        except Exception as e:
            self.logger.error(f"Error extracting historical data: {str(e)}")
            raise
    
    def extract_country_historical_data(self, countries: List[str],
                                      days: int = 30) -> pd.DataFrame:
        """
        Extract historical data for specific countries.
        
        Args:
            countries: List of country names
            days: Number of days of historical data
            
        Returns:
            DataFrame with country historical data
        """
        try:
            all_records = []
            
            for country in countries:
                self.logger.info(f"Extracting historical data for {country}")
                
                try:
                    data = self.api_client.get_historical_data(country=country, days=days)
                    
                    # Handle different response formats
                    if isinstance(data, list) and len(data) > 0:
                        data = data[0]  # Take first item if it's a list
                    
                    timeline = data.get('timeline', {})
                    
                    # Process each metric
                    for metric in ['cases', 'deaths', 'recovered']:
                        if metric in timeline:
                            for date_str, value in timeline[metric].items():
                                all_records.append({
                                    'date': date_str,
                                    'metric': metric,
                                    'value': value,
                                    'country': country
                                })
                
                except Exception as e:
                    self.logger.warning(f"Failed to extract data for {country}: {str(e)}")
                    continue
            
            df = pd.DataFrame(all_records)
            
            if not df.empty:
                # Convert date column
                df['date'] = pd.to_datetime(df['date'])
                
                # Add extraction metadata
                df['extraction_date'] = datetime.now()
                df['data_source'] = 'disease.sh'
                df['data_type'] = 'country_historical'
            
            self.logger.info(f"Extracted country historical data with {len(df)} records")
            return df
            
        except Exception as e:
            self.logger.error(f"Error extracting country historical data: {str(e)}")
            raise
    
    def save_raw_data(self, data: Dict[str, pd.DataFrame],
                     output_dir: str = "data/raw") -> Dict[str, str]:
        """
        Save raw extracted data to files.
        
        Args:
            data: Dictionary of DataFrames to save
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
                    filename = f"covid_{data_type}_{timestamp}.parquet"
                    filepath = output_path / filename
                    df.to_parquet(filepath, index=False)
                else:
                    filename = f"covid_{data_type}_{timestamp}.csv"
                    filepath = output_path / filename
                    df.to_csv(filepath, index=False)
                
                saved_files[data_type] = str(filepath)
                self.logger.info(f"Saved {data_type} data to {filepath}")
            
            return saved_files
            
        except Exception as e:
            self.logger.error(f"Error saving raw data: {str(e)}")
            raise
    
    def get_extraction_summary(self, data: Dict[str, pd.DataFrame]) -> Dict[str, Any]:
        """
        Generate summary of extracted data.
        
        Args:
            data: Dictionary of extracted DataFrames
            
        Returns:
            Summary statistics
        """
        summary = {
            'extraction_timestamp': datetime.now().isoformat(),
            'total_datasets': len(data),
            'datasets': {}
        }
        
        for data_type, df in data.items():
            summary['datasets'][data_type] = {
                'record_count': len(df),
                'columns': list(df.columns),
                'memory_usage_mb': df.memory_usage(deep=True).sum() / 1024 / 1024,
                'is_empty': df.empty
            }
        
        return summary
