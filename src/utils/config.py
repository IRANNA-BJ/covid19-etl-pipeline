"""
Configuration management for COVID-19 ETL Pipeline.
"""

import os
import yaml
from typing import Dict, Any, Optional
from pathlib import Path


class Config:
    """Configuration manager for the ETL pipeline."""
    
    def __init__(self, config_path: Optional[str] = None):
        """
        Initialize configuration.
        
        Args:
            config_path: Path to configuration file. If None, uses default.
        """
        if config_path is None:
            config_path = os.path.join(
                Path(__file__).parent.parent.parent, 
                'config', 
                'config.yaml'
            )
        
        self.config_path = config_path
        self._config = self._load_config()
    
    def _load_config(self) -> Dict[str, Any]:
        """Load configuration from YAML file."""
        try:
            with open(self.config_path, 'r') as file:
                config = yaml.safe_load(file)
            
            # Override with environment variables if they exist
            config = self._override_with_env_vars(config)
            return config
            
        except FileNotFoundError:
            # Return default configuration if file not found
            return self._get_default_config()
    
    def _override_with_env_vars(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """Override configuration with environment variables."""
        # API Configuration
        if os.getenv('COVID_API_BASE_URL'):
            config['api']['base_url'] = os.getenv('COVID_API_BASE_URL')
        if os.getenv('COVID_API_RATE_LIMIT'):
            config['api']['rate_limit'] = int(os.getenv('COVID_API_RATE_LIMIT'))
        
        # Storage Configuration
        if os.getenv('STORAGE_PROVIDER'):
            config['storage']['provider'] = os.getenv('STORAGE_PROVIDER')
        if os.getenv('STORAGE_BUCKET'):
            config['storage']['bucket'] = os.getenv('STORAGE_BUCKET')
        
        # AWS Configuration
        if os.getenv('AWS_ACCESS_KEY_ID'):
            config['aws']['access_key_id'] = os.getenv('AWS_ACCESS_KEY_ID')
        if os.getenv('AWS_SECRET_ACCESS_KEY'):
            config['aws']['secret_access_key'] = os.getenv('AWS_SECRET_ACCESS_KEY')
        if os.getenv('AWS_REGION'):
            config['aws']['region'] = os.getenv('AWS_REGION')
        
        # GCP Configuration
        if os.getenv('GOOGLE_APPLICATION_CREDENTIALS'):
            config['gcp']['credentials_path'] = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
        if os.getenv('GCP_PROJECT_ID'):
            config['gcp']['project_id'] = os.getenv('GCP_PROJECT_ID')
        
        # Database Configuration
        if os.getenv('DB_HOST'):
            config['database']['host'] = os.getenv('DB_HOST')
        if os.getenv('DB_PORT'):
            config['database']['port'] = int(os.getenv('DB_PORT'))
        if os.getenv('DB_NAME'):
            config['database']['name'] = os.getenv('DB_NAME')
        if os.getenv('DB_USER'):
            config['database']['user'] = os.getenv('DB_USER')
        if os.getenv('DB_PASSWORD'):
            config['database']['password'] = os.getenv('DB_PASSWORD')
        
        return config
    
    def _get_default_config(self) -> Dict[str, Any]:
        """Return default configuration."""
        return {
            'api': {
                'base_url': 'https://disease.sh/v3/covid-19',
                'rate_limit': 100,
                'timeout': 30,
                'retry_attempts': 3,
                'retry_delay': 1
            },
            'storage': {
                'provider': 'aws',  # or 'gcp'
                'bucket': 'covid-etl-data',
                'raw_data_prefix': 'raw/',
                'processed_data_prefix': 'processed/',
                'file_format': 'parquet'
            },
            'aws': {
                'region': 'us-east-1',
                'access_key_id': '',
                'secret_access_key': ''
            },
            'gcp': {
                'project_id': '',
                'credentials_path': ''
            },
            'database': {
                'provider': 'redshift',  # or 'bigquery'
                'host': '',
                'port': 5439,
                'name': 'covid_data',
                'user': '',
                'password': '',
                'schema': 'public'
            },
            'airflow': {
                'dag_id': 'covid_etl_pipeline',
                'schedule_interval': '@daily',
                'start_date': '2024-01-01',
                'catchup': False,
                'max_active_runs': 1
            },
            'logging': {
                'level': 'INFO',
                'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                'file_path': 'logs/covid_etl.log'
            },
            'data_quality': {
                'enable_validation': True,
                'max_null_percentage': 0.1,
                'min_record_count': 100,
                'enable_alerts': True
            }
        }
    
    def get(self, key: str, default: Any = None) -> Any:
        """
        Get configuration value by key.
        
        Args:
            key: Configuration key (supports dot notation, e.g., 'api.base_url')
            default: Default value if key not found
            
        Returns:
            Configuration value
        """
        keys = key.split('.')
        value = self._config
        
        try:
            for k in keys:
                value = value[k]
            return value
        except (KeyError, TypeError):
            return default
    
    def set(self, key: str, value: Any) -> None:
        """
        Set configuration value by key.
        
        Args:
            key: Configuration key (supports dot notation)
            value: Value to set
        """
        keys = key.split('.')
        config = self._config
        
        for k in keys[:-1]:
            if k not in config:
                config[k] = {}
            config = config[k]
        
        config[keys[-1]] = value
    
    def get_api_config(self) -> Dict[str, Any]:
        """Get API configuration."""
        return self.get('api', {})
    
    def get_storage_config(self) -> Dict[str, Any]:
        """Get storage configuration."""
        return self.get('storage', {})
    
    def get_database_config(self) -> Dict[str, Any]:
        """Get database configuration."""
        return self.get('database', {})
    
    def get_aws_config(self) -> Dict[str, Any]:
        """Get AWS configuration."""
        return self.get('aws', {})
    
    def get_gcp_config(self) -> Dict[str, Any]:
        """Get GCP configuration."""
        return self.get('gcp', {})
    
    def get_airflow_config(self) -> Dict[str, Any]:
        """Get Airflow configuration."""
        return self.get('airflow', {})
    
    def get_logging_config(self) -> Dict[str, Any]:
        """Get logging configuration."""
        return self.get('logging', {})
    
    def get_data_quality_config(self) -> Dict[str, Any]:
        """Get data quality configuration."""
        return self.get('data_quality', {})
    
    def to_dict(self) -> Dict[str, Any]:
        """Return configuration as dictionary."""
        return self._config.copy()


# Global configuration instance
config = Config()
