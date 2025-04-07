"""
Configuration loader module for Splunk Duplicate Remover
"""

import configparser
import json
import os

class ConfigLoader:
    """
    Handles loading configuration from either INI or JSON files
    """
    
    def __init__(self, config_file):
        """
        Initialize with the path to a configuration file
        
        Args:
            config_file (str): Path to configuration file (INI or JSON)
        """
        self.config_file = config_file
        
    def load(self):
        """
        Load and parse the configuration file
        
        Returns:
            configparser.ConfigParser: Configuration object
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