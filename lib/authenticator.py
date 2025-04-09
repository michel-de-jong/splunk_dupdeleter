"""
Splunk authentication module with performance optimizations
"""

import requests
import urllib3

# Disable SSL warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

class SplunkAuthenticator:
    """
    Handles authentication to Splunk Cloud instance with performance optimizations
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
        self.session = None  # Store the authenticated session
    
    def authenticate(self):
        """
        Authenticate to Splunk Cloud using JWT token
        Only performs the authentication test once and stores the session
        
        Returns:
            requests.Session: Authenticated session or None if failed
        """
        if self.session is not None:
            # Return the existing authenticated session if we already have one
            return self.session
            
        try:
            # PERFORMANCE IMPROVEMENT: Configure the requests session for better performance
            session = requests.Session()
            
            # Configure connection pooling
            adapter = requests.adapters.HTTPAdapter(
                pool_connections=10,  # Number of connection pools to cache
                pool_maxsize=20,      # Number of connections to save in the pool
                max_retries=3,        # Retry failed requests
                pool_block=False      # Don't block when pool is depleted
            )
            
            # Add the adapter to both HTTP and HTTPS 
            session.mount('http://', adapter)
            session.mount('https://', adapter)
            
            # Get JWT token from config
            jwt_token = self.config['splunk']['jwt_token']
            
            # Set authorization header with JWT token
            session.headers.update({
                'Authorization': f'Bearer {jwt_token}',
                'Content-Type': 'application/json',
                'Connection': 'keep-alive',         # Keep connection alive for better performance
                'Accept-Encoding': 'gzip, deflate'  # Accept compressed responses
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
                    'earliest_time': '-1m',
                    'exec_mode': 'oneshot'  # Use oneshot for quick authentication test
                },
                timeout=30  # Set a reasonable timeout
            )
            response.raise_for_status()
            
            self.logger.info("Successfully authenticated to Splunk Cloud using JWT token")
            
            # Store the authenticated session for future use
            self.session = session
            return session
        except Exception as e:
            self.logger.error(f"Authentication failed: {str(e)}")
            self.session = None
            return None