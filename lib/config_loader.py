"""
Configuration loader module for Splunk Duplicate Remover
"""

import configparser
import os

class ConfigLoader:
    """
    Handles loading configuration from either INI or JSON files
    Default configuration path is configs/config.ini
    """
    
    def __init__(self):
        """
        Initialize with the default configuration path (configs/config.ini)
        """
        self.config_file = os.path.join('configs', 'config.ini')
        
    def load(self):
        """
        Load and parse the configuration file
        
        Returns:
            configparser.ConfigParser: Configuration object
            
        Raises:
            FileNotFoundError: If configs/config.ini is not found
        """
        if not os.path.exists(self.config_file):
            raise FileNotFoundError(f"Configuration file not found: {self.config_file}")
        return self._load_ini()
    
    def _load_ini(self):
        """Load configuration from INI file"""
        config = configparser.ConfigParser()
        config.read(self.config_file)
        return config