"""
Configuration loader module for Splunk Duplicate Remover
"""

import configparser
import json
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
        
        if self.config_file.endswith('.json'):
            return self._load_json()
        else:  # Assume INI file
            return self._load_ini()
    
    def _load_json(self):
        """Load configuration from JSON file"""
        with open(self.config_file, 'r') as f:
            json_config = json.load(f)
            
            # Convert JSON to ConfigParser object for consistent interface
            config = configparser.ConfigParser()
            for section, values in json_config.items():
                config[section] = values
                
            return config
    
    def _load_ini(self):
        """Load configuration from INI file"""
        config = configparser.ConfigParser()
        config.read(self.config_file)
        return config