"""
Splunk authentication module
"""

import requests
import urllib3

# Disable SSL warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

class SplunkAuthenticator:
    """
    Handles authentication to Splunk Cloud instance
    """
    
    def __init__(self, config, logger):
        """
        Initialize with configuration and logger
        
        Args:
            config (configparser.ConfigParser): Configuration with Splunk settings
            logger (logging.Logger): Logger instance
        """
        self.config = config
        self.logger = logger
    
    def authenticate(self):
        """
        Authenticate to Splunk Cloud using JWT token
        
        Returns:
            requests.Session: Authenticated session or None if failed
        """
        try:
            # Create a new session
            session = requests.Session()
            
            # Get JWT token from config
            jwt_token = self.config['splunk']['jwt_token']
            
            # Set authorization header with JWT token
            session.headers.update({
                'Authorization': f'Bearer {jwt_token}',
                'Content-Type': 'application/json'
            })
            
            # Set SSL verification based on config
            verify_ssl = self.config.getboolean('splunk', 'verify_ssl', fallback=False)
            session.verify = verify_ssl
            
            # Test authentication by making a simple API call
            test_url = f"{self.config['splunk']['url']}/services/search/jobs/export"
            response = session.post(
                test_url, 
                data={
                    'search': 'search index=_internal | head 1',
                    'output_mode': 'json',
                    'earliest_time': '-1m'
                    },
            )
            response.raise_for_status()
            
            self.logger.info("Successfully authenticated to Splunk Cloud using JWT token")
            return session
        except Exception as e:
            self.logger.error(f"Authentication failed: {str(e)}")
            return None