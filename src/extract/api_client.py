"""
API client for disease.sh COVID-19 data extraction.
"""

import requests
import time
from typing import Dict, List, Any, Optional, Union
from datetime import datetime, timedelta
import json
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from ..utils.config import config
from ..utils.logger import get_logger


class CovidAPIClient:
    """Client for interacting with disease.sh COVID-19 API."""
    
    def __init__(self):
        """Initialize API client."""
        self.logger = get_logger(__name__)
        self.api_config = config.get_api_config()
        
        self.base_url = self.api_config.get('base_url', 'https://disease.sh/v3/covid-19')
        self.rate_limit = self.api_config.get('rate_limit', 100)
        self.timeout = self.api_config.get('timeout', 30)
        self.retry_attempts = self.api_config.get('retry_attempts', 3)
        self.retry_delay = self.api_config.get('retry_delay', 1)
        
        # Setup session with retry strategy
        self.session = self._setup_session()
        
        # Rate limiting
        self.last_request_time = 0
        self.min_request_interval = 60 / self.rate_limit  # seconds between requests
    
    def _setup_session(self) -> requests.Session:
        """Setup requests session with retry strategy."""
        session = requests.Session()
        
        # Retry strategy
        retry_strategy = Retry(
            total=self.retry_attempts,
            backoff_factor=self.retry_delay,
            status_forcelist=[429, 500, 502, 503, 504],
        )
        
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        
        # Set headers
        session.headers.update({
            'User-Agent': 'COVID-ETL-Pipeline/1.0',
            'Accept': 'application/json',
            'Content-Type': 'application/json'
        })
        
        return session
    
    def _rate_limit(self) -> None:
        """Implement rate limiting."""
        current_time = time.time()
        time_since_last_request = current_time - self.last_request_time
        
        if time_since_last_request < self.min_request_interval:
            sleep_time = self.min_request_interval - time_since_last_request
            self.logger.debug(f"Rate limiting: sleeping for {sleep_time:.2f} seconds")
            time.sleep(sleep_time)
        
        self.last_request_time = time.time()
    
    def _make_request(self, endpoint: str, params: Optional[Dict] = None) -> Dict[str, Any]:
        """
        Make HTTP request to API endpoint.
        
        Args:
            endpoint: API endpoint
            params: Query parameters
            
        Returns:
            JSON response data
            
        Raises:
            requests.RequestException: If request fails
        """
        self._rate_limit()
        
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        
        self.logger.debug(f"Making request to: {url}")
        
        try:
            response = self.session.get(
                url,
                params=params,
                timeout=self.timeout
            )
            response.raise_for_status()
            
            data = response.json()
            self.logger.debug(f"Successfully retrieved data from {endpoint}")
            
            return data
            
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Request failed for {endpoint}: {str(e)}")
            raise
        except json.JSONDecodeError as e:
            self.logger.error(f"Failed to decode JSON response from {endpoint}: {str(e)}")
            raise
    
    def get_global_data(self) -> Dict[str, Any]:
        """
        Get global COVID-19 statistics.
        
        Returns:
            Global COVID-19 data
        """
        self.logger.info("Fetching global COVID-19 data")
        return self._make_request('/all')
    
    def get_countries_data(self, yesterday: bool = False, 
                          two_days_ago: bool = False) -> List[Dict[str, Any]]:
        """
        Get COVID-19 data for all countries.
        
        Args:
            yesterday: Get data from yesterday
            two_days_ago: Get data from two days ago
            
        Returns:
            List of country data
        """
        self.logger.info("Fetching countries COVID-19 data")
        
        params = {}
        if yesterday:
            params['yesterday'] = 'true'
        elif two_days_ago:
            params['twoDaysAgo'] = 'true'
        
        return self._make_request('/countries', params)
    
    def get_country_data(self, country: str, yesterday: bool = False,
                        two_days_ago: bool = False) -> Dict[str, Any]:
        """
        Get COVID-19 data for specific country.
        
        Args:
            country: Country name, ISO2, ISO3, or country ID
            yesterday: Get data from yesterday
            two_days_ago: Get data from two days ago
            
        Returns:
            Country COVID-19 data
        """
        self.logger.info(f"Fetching COVID-19 data for country: {country}")
        
        params = {}
        if yesterday:
            params['yesterday'] = 'true'
        elif two_days_ago:
            params['twoDaysAgo'] = 'true'
        
        return self._make_request(f'/countries/{country}', params)
    
    def get_historical_data(self, country: Optional[str] = None,
                           days: Union[int, str] = 30) -> Dict[str, Any]:
        """
        Get historical COVID-19 data.
        
        Args:
            country: Country name (None for global data)
            days: Number of days or 'all' for all data
            
        Returns:
            Historical COVID-19 data
        """
        if country:
            endpoint = f'/historical/{country}'
            self.logger.info(f"Fetching historical data for country: {country}")
        else:
            endpoint = '/historical/all'
            self.logger.info("Fetching global historical data")
        
        params = {'lastdays': str(days)}
        return self._make_request(endpoint, params)
    
    def get_vaccine_data(self, country: Optional[str] = None) -> Union[Dict, List]:
        """
        Get COVID-19 vaccine data.
        
        Args:
            country: Country name (None for all countries)
            
        Returns:
            Vaccine data
        """
        if country:
            endpoint = f'/vaccine/coverage/countries/{country}'
            self.logger.info(f"Fetching vaccine data for country: {country}")
        else:
            endpoint = '/vaccine/coverage/countries'
            self.logger.info("Fetching vaccine data for all countries")
        
        return self._make_request(endpoint)
    
    def get_states_data(self, country: str = 'USA') -> List[Dict[str, Any]]:
        """
        Get COVID-19 data for states/provinces.
        
        Args:
            country: Country code (default: USA)
            
        Returns:
            List of state/province data
        """
        self.logger.info(f"Fetching states data for country: {country}")
        return self._make_request(f'/states')
    
    def get_continents_data(self) -> List[Dict[str, Any]]:
        """
        Get COVID-19 data by continent.
        
        Returns:
            List of continent data
        """
        self.logger.info("Fetching continents COVID-19 data")
        return self._make_request('/continents')
    
    def get_jhucsse_data(self) -> List[Dict[str, Any]]:
        """
        Get data from Johns Hopkins CSSE.
        
        Returns:
            JHU CSSE data
        """
        self.logger.info("Fetching JHU CSSE data")
        return self._make_request('/jhucsse')
    
    def health_check(self) -> bool:
        """
        Check if API is accessible.
        
        Returns:
            True if API is healthy, False otherwise
        """
        try:
            self._make_request('/all')
            self.logger.info("API health check passed")
            return True
        except Exception as e:
            self.logger.error(f"API health check failed: {str(e)}")
            return False
    
    def get_api_info(self) -> Dict[str, Any]:
        """
        Get API information and status.
        
        Returns:
            API information
        """
        try:
            # Try to get global data to check API status
            global_data = self.get_global_data()
            
            return {
                'status': 'healthy',
                'base_url': self.base_url,
                'last_updated': global_data.get('updated'),
                'rate_limit': self.rate_limit,
                'timeout': self.timeout
            }
        except Exception as e:
            return {
                'status': 'unhealthy',
                'base_url': self.base_url,
                'error': str(e),
                'rate_limit': self.rate_limit,
                'timeout': self.timeout
            }
