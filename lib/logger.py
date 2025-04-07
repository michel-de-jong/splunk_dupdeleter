"""
Logging configuration for Splunk Duplicate Remover
"""

import logging

def setup_logger(config, debug=False):
    """
    Configure and return a logger
    
    Args:
        config (configparser.ConfigParser): Configuration with logging settings
        debug (bool, optional): Whether to enable debug logging. Defaults to False.
    
    Returns:
        logging.Logger: Configured logger instance
    """
    # Get logger
    logger = logging.getLogger('splunk_duplicate_remover')
    
    # Set logging level
    level = logging.DEBUG if debug else logging.INFO
    logger.setLevel(level)
    
    # Create handlers
    log_file = config.get('general', 'log_file', fallback='splunk_duplicate_remover.log')
    
    # Clear any existing handlers to avoid duplicates
    if logger.handlers:
        logger.handlers.clear()
    
    # Add handlers
    console_handler = logging.StreamHandler()
    file_handler = logging.FileHandler(log_file)
    
    # Set format
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    console_handler.setFormatter(formatter)
    file_handler.setFormatter(formatter)
    
    # Add handlers to logger
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)
    
    return logger