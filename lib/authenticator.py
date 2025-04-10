"""
Splunk authentication module with performance optimizations
"""

import requests
import urllib3
import logging
from lib.logger import mask_credentials

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
        self.logger.debug("SplunkAuthenticator initialized")
    
    def authenticate(self):
        """
        Authenticate to Splunk Cloud using JWT token
        Only performs the authentication test once and stores the session
        
        Returns:
            requests.Session: Authenticated session or None if failed
        """
        if self.session is not None:
            # Return the existing authenticated session if we already have one
            self.logger.debug("Using existing authenticated session")
            return self.session
            
        try:
            self.logger.debug("Creating new authenticated session")
            
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
            self.logger.debug("Configured HTTP adapter with pool_connections=10, pool_maxsize=20")
            
            # Get JWT token from config
            jwt_token = self.config['splunk']['jwt_token']
            self.logger.debug("Retrieved JWT token from config")
            
            # Safe debug log with masked credentials
            masked_debug = mask_credentials(f"Setting Authorization header with token: {jwt_token}")
            self.logger.debug(masked_debug)
            
            # Set authorization header with JWT token
            session.headers.update({
                'Authorization': f'Bearer {jwt_token}',
                'Content-Type': 'application/json',
                'Connection': 'keep-alive',         # Keep connection alive for better performance
                'Accept-Encoding': 'gzip, deflate'  # Accept compressed responses
            })
            self.logger.debug("Set session headers with Content-Type, Connection, and Accept-Encoding")
            
            # Set SSL verification based on config
            verify_ssl = self.config.getboolean('splunk', 'verify_ssl', fallback=True)
            session.verify = verify_ssl
            self.logger.debug(f"SSL verification set to: {verify_ssl}")
            
            # Test authentication by making a simple API call
            test_url = f"{self.config['splunk']['url']}/services/search/jobs/export"
            self.logger.debug(f"Testing authentication with URL: {test_url}")
            
            search_query = 'search index=_internal | head 1'
            self.logger.debug(f"Authentication test search query: {search_query}")
            
            response = session.post(
                test_url, 
                data={
                    'search': search_query,
                    'output_mode': 'json',
                    'earliest_time': '-1m',
                    'exec_mode': 'oneshot'  # Use oneshot for quick authentication test
                },
                timeout=30  # Set a reasonable timeout
            )
            
            self.logger.debug(f"Authentication test response status code: {response.status_code}")
            response.raise_for_status()
            
            self.logger.info("Successfully authenticated to Splunk Cloud using JWT token")
            self.logger.debug(f"Authentication test response headers: {response.headers}")
            
            # Store the authenticated session for future use
            self.session = session
            return session
        except Exception as e:
            self.logger.error(f"Authentication failed: {str(e)}")
            self.logger.debug(f"Authentication failure details: {type(e).__name__} - {str(e)}")
            self.session = None
            return None