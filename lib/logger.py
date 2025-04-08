"""
Logging configuration for Splunk Duplicate Remover
"""

import logging
from logging.handlers import RotatingFileHandler
import os

def setup_logger(config, debug=False):
    """
    Configure and return a logger with rotation and compression
    
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
    
    # Create log directory if it doesn't exist
    log_dir = 'log'
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
        
    # Create handlers with log file in the log directory
    log_file = config.get('general', 'log_file', fallback='splunk_duplicate_remover.log')
    log_path = os.path.join(log_dir, log_file)
    
    # Clear any existing handlers to avoid duplicates
    if logger.handlers:
        logger.handlers.clear()
    
    # Add handlers
    console_handler = logging.StreamHandler()
    
    # Create rotating file handler (10MB max size, unlimited backup files)
    max_bytes = 10 * 1024 * 1024  # 10MB in bytes
    file_handler = RotatingFileHandler(
        filename=log_path,
        maxBytes=max_bytes,
        backupCount=0,  # 0 means unlimited backups
        encoding='utf-8'
    )
    
    # Set format
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    console_handler.setFormatter(formatter)
    file_handler.setFormatter(formatter)
    
    # Add handlers to logger
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)
    
    # Log initial message with file size info
    if os.path.exists(log_path):
        current_size = os.path.getsize(log_path)
        logger.info(f"Current log file size: {current_size / (1024*1024):.2f} MB")
    
    return logger