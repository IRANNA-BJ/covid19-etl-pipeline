"""
Logging utilities for COVID-19 ETL Pipeline.
"""

import logging
import sys
from pathlib import Path
from typing import Optional
from .config import config


class ETLLogger:
    """Custom logger for ETL pipeline."""
    
    def __init__(self, name: str, log_file: Optional[str] = None):
        """
        Initialize logger.
        
        Args:
            name: Logger name
            log_file: Optional log file path
        """
        self.name = name
        self.logger = logging.getLogger(name)
        
        # Prevent duplicate handlers
        if not self.logger.handlers:
            self._setup_logger(log_file)
    
    def _setup_logger(self, log_file: Optional[str] = None) -> None:
        """Setup logger configuration."""
        # Get logging configuration
        log_config = config.get_logging_config()
        level = getattr(logging, log_config.get('level', 'INFO').upper())
        format_str = log_config.get('format', 
                                   '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        
        self.logger.setLevel(level)
        
        # Create formatter
        formatter = logging.Formatter(format_str)
        
        # Console handler
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(level)
        console_handler.setFormatter(formatter)
        self.logger.addHandler(console_handler)
        
        # File handler
        if log_file is None:
            log_file = log_config.get('file_path', 'logs/covid_etl.log')
        
        if log_file:
            # Create log directory if it doesn't exist
            log_path = Path(log_file)
            log_path.parent.mkdir(parents=True, exist_ok=True)
            
            file_handler = logging.FileHandler(log_file)
            file_handler.setLevel(level)
            file_handler.setFormatter(formatter)
            self.logger.addHandler(file_handler)
    
    def debug(self, message: str, **kwargs) -> None:
        """Log debug message."""
        self.logger.debug(message, **kwargs)
    
    def info(self, message: str, **kwargs) -> None:
        """Log info message."""
        self.logger.info(message, **kwargs)
    
    def warning(self, message: str, **kwargs) -> None:
        """Log warning message."""
        self.logger.warning(message, **kwargs)
    
    def error(self, message: str, **kwargs) -> None:
        """Log error message."""
        self.logger.error(message, **kwargs)
    
    def critical(self, message: str, **kwargs) -> None:
        """Log critical message."""
        self.logger.critical(message, **kwargs)
    
    def exception(self, message: str, **kwargs) -> None:
        """Log exception with traceback."""
        self.logger.exception(message, **kwargs)


def get_logger(name: str, log_file: Optional[str] = None) -> ETLLogger:
    """
    Get logger instance.
    
    Args:
        name: Logger name
        log_file: Optional log file path
        
    Returns:
        ETLLogger instance
    """
    return ETLLogger(name, log_file)
