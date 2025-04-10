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
        # Get absolute path for clearer error reporting
        abs_config_path = os.path.abspath(self.config_file)
        
        if not os.path.exists(self.config_file):
            error_msg = f"Configuration file not found: {abs_config_path}"
            # We can't log since logger isn't set up yet
            raise FileNotFoundError(error_msg)
        
        # Log will happen in _load_ini since we're just proxying to it
        return self._load_ini()

    def _load_ini(self):
        """Load configuration from INI file"""
        try:
            print(f"Loading configuration from {self.config_file}")  # Basic output since logger isn't available yet
            config = configparser.ConfigParser()
            config.read(self.config_file)
            
            # Basic validation of required sections
            required_sections = ['general', 'splunk', 'search']
            missing_sections = [section for section in required_sections if section not in config]
            
            if missing_sections:
                error_msg = f"Missing required sections in config: {', '.join(missing_sections)}"
                print(f"ERROR: {error_msg}")
                raise ValueError(error_msg)
                
            print(f"Successfully loaded configuration with sections: {list(config.sections())}")
            return config
        except Exception as e:
            error_msg = f"Error loading configuration: {type(e).__name__} - {str(e)}"
            print(f"ERROR: {error_msg}")
            raise