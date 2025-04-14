"""
Logging configuration for Splunk Duplicate Remover
"""

import logging
from logging.handlers import RotatingFileHandler
import os
import datetime

class MessageTruncatingFilter(logging.Filter):
    """Filter that truncates long log messages"""
    
    def __init__(self, max_length=750):
        super().__init__()
        self.max_length = max_length
        
    def filter(self, record):
        if len(record.msg) > self.max_length:
            record.msg = record.msg[:self.max_length] + f"... (truncated, full length: {len(record.msg)} chars)"
        return True

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
    
    # Create unique log filename based on index and time range
    index = config.get('search', 'index', fallback='unknown')
    start_time = config.get('search', 'start_time', fallback='unknown')
    end_time = config.get('search', 'end_time', fallback='unknown')
    
    # Clean up timestamps for filename (replace characters that might be invalid in filenames)
    start_time = start_time.replace(':', '-').replace(' ', '_')
    end_time = end_time.replace(':', '-').replace(' ', '_')
    
    # Format the log filename
    base_log_name = f"splunk_duplicate_remover-{index}_{start_time}_{end_time}"
    log_file = config.get('general', 'log_file', fallback=f'{base_log_name}.log')
    
    # If the original log_file doesn't contain the index and time pattern, replace it
    if 'splunk_duplicate_remover-' not in log_file:
        log_file = f'{base_log_name}.log'
        
    log_path = os.path.join(log_dir, log_file)
    
    # Clear any existing handlers to avoid duplicates
    if logger.handlers:
        logger.handlers.clear()
    
    # Add handlers
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)  # Console only shows INFO and above
    
    # Create rotating file handler (10MB max size, unlimited backup files)
    max_bytes = 10 * 1024 * 1024  # 10MB in bytes
    file_handler = RotatingFileHandler(
        filename=log_path,
        maxBytes=max_bytes,
        backupCount=0,  # 0 means unlimited backups
        encoding='utf-8'
    )
    file_handler.setLevel(logging.INFO)  # Main log file shows INFO and above
    
    # Set format
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    console_handler.setFormatter(formatter)
    file_handler.setFormatter(formatter)
    
    # Add handlers to logger
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)
    
    # Create debug handler if debug mode is enabled
    if debug:
        # Create unique debug log filename with timestamp and index
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        debug_log_file = f"debug_{base_log_name}_{timestamp}.log"
        debug_log_path = os.path.join(log_dir, debug_log_file)
        
        # Create debug file handler
        debug_handler = RotatingFileHandler(
            filename=debug_log_path,
            maxBytes=max_bytes * 2,  # 20MB for debug logs
            backupCount=0,  # 0 means unlimited backups
            encoding='utf-8'
        )
        debug_handler.setLevel(logging.DEBUG)  # Debug file shows all levels
        
        # Create more detailed formatter for debug logs
        debug_formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(funcName)s - %(message)s'
        )
        debug_handler.setFormatter(debug_formatter)
        
        # Add debug handler to logger
        logger.addHandler(debug_handler)
        
        # Only apply message truncation filter in debug mode
        truncate_filter = MessageTruncatingFilter(max_length=500)
        logger.addFilter(truncate_filter)
        
        logger.debug(f"Debug logging enabled to {debug_log_path}")
        logger.debug(f"Long message truncation enabled (max 500 chars)")
    
    # Log initial message with file size info
    if os.path.exists(log_path):
        current_size = os.path.getsize(log_path)
        logger.info(f"Current log file size: {current_size / (1024*1024):.2f} MB")
    
    return logger

def mask_credentials(message):
    """
    Mask sensitive credentials in log messages
    
    Args:
        message (str): The log message to be masked
        
    Returns:
        str: The masked log message
    """
    # List of patterns to mask
    patterns = [
        "jwt_token",
        "token=",
        "password=",
        "auth=",
        "Authorization: Bearer ",
        "apikey=",
        "api_key=",
        "secret="
    ]
    
    # Perform the masking
    masked_message = message
    for pattern in patterns:
        if pattern in masked_message:
            # Find the position of the pattern
            pos = masked_message.find(pattern)
            
            # Find where the value ends (next space, comma, quote, etc.)
            end_chars = [' ', ',', ';', '"', "'", '}', ')', '\n', '\r']
            end_pos = len(masked_message)
            
            for char in end_chars:
                next_char_pos = masked_message.find(char, pos + len(pattern))
                if next_char_pos != -1 and next_char_pos < end_pos:
                    end_pos = next_char_pos
            
            # Replace the actual value with MASKED_CREDENTIALS
            value_start = pos + len(pattern)
            masked_message = masked_message[:value_start] + "MASKED_CREDENTIALS" + masked_message[end_pos:]
    
    return masked_message

def truncate_search_query(message, max_length=300):
    """
    Truncate search queries in log messages
    
    Args:
        message (str): The log message that might contain a search query
        max_length (int): Maximum length to keep before truncating
        
    Returns:
        str: The message with truncated search query if applicable
    """
    # Patterns that might indicate a search query
    query_indicators = [
        "search=",
        "search query:",
        "search:",
        "query:",
        "| search ",
        "search index="
    ]
    
    truncated_message = message
    for indicator in query_indicators:
        if indicator in truncated_message:
            # Find position of the search query
            pos = truncated_message.find(indicator) + len(indicator)
            
            # Check if there's enough text after the indicator to warrant truncation
            if len(truncated_message) > pos + max_length:
                # Truncate and add ellipsis
                truncated_message = truncated_message[:pos + max_length] + "... (truncated)"
                break
    
    return truncated_message