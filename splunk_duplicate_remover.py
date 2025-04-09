#!/usr/bin/env python3
"""
Splunk Duplicate Event Finder and Remover
Main script to handle duplicate event detection and removal in Splunk Cloud
"""

# Check required modules first before importing any other modules
from lib.module_checker import check_modules
check_modules()

import argparse
import os
import concurrent.futures
import time
from lib.config_loader import ConfigLoader
from lib.logger import setup_logger
from lib.authenticator import SplunkAuthenticator
from lib.duplicate_finder import DuplicateFinder
from lib.duplicate_remover import DuplicateRemover
from lib.file_processor import FileProcessor
from lib.stats_tracker import StatsTracker

__name__ = "splunk_duplicate_remover.py"
__author__ = "Michel de Jong"

def main():
    """Main entry point for the application"""
    parser = argparse.ArgumentParser(description='Splunk Duplicate Event Finder and Remover')
    parser.add_argument('--debug', action='store_true', help='Enable debug logging')
    
    # Add optional command-line arguments that override config.ini values
    parser.add_argument('--max_workers', type=int, help='Maximum number of concurrent searches')
    parser.add_argument('--batch_size', type=int, help='Batch size for processing events')
    parser.add_argument('--url', help='Splunk Cloud URL')
    parser.add_argument('--jwt_token', help='JWT token for Splunk authentication')
    parser.add_argument('--start_time', help='Start time for search window (ISO format)')
    parser.add_argument('--end_time', help='End time for search window (ISO format)')
    parser.add_argument('--verify_ssl', type=lambda x: (str(x).lower() == 'true'), 
                        help='Whether to verify SSL certificates (true/false)')
    parser.add_argument('--index', help='Splunk index name to search')
    
    args = parser.parse_args()
    
    try:
        # Load configuration from default path (configs/config.ini)
        config = ConfigLoader().load()
        
        # Override config with command-line arguments if provided
        update_config_from_args(config, args)
        
    except FileNotFoundError as e:
        print(f"Error: {str(e)}")
        print("Please ensure configs/config.ini exists and is properly configured.")
        return False
    
    # Setup logging
    logger = setup_logger(config, args.debug)
    logger.info("Starting Splunk Duplicate Remover")
    
    # Log configuration settings
    logger.debug(f"Configuration: max_workers={config['general'].get('max_workers')}, "
                 f"batch_size={config['general'].get('batch_size')}, "
                 f"url={config['splunk'].get('url')}, "
                 f"verify_ssl={config['splunk'].get('verify_ssl')}, "
                 f"index={config['search'].get('index')}, "
                 f"start_time={config['search'].get('start_time')}, "
                 f"end_time={config['search'].get('end_time')}")
    
    # Initialize components
    stats_tracker = StatsTracker()
    authenticator = SplunkAuthenticator(config, logger)
    duplicate_finder = DuplicateFinder(config, logger, stats_tracker)
    duplicate_remover = DuplicateRemover(config, logger, stats_tracker)
    file_processor = FileProcessor(config, logger)
    
    # Create output directories if they don't exist
    csv_dir = config.get('general', 'csv_dir', fallback='csv_output')
    processed_dir = config.get('general', 'processed_dir', fallback='processed_csv')
    os.makedirs(csv_dir, exist_ok=True)
    os.makedirs(processed_dir, exist_ok=True)
    
    # Authenticate to Splunk
    session = authenticator.authenticate()
    if not session:
        logger.error("Authentication failed. Exiting.")
        return False
    
    # Get search parameters
    index = config['search']['index']
    start_time = config['search']['start_time']
    end_time = config['search']['end_time']
    
    # Generate time windows for searches
    time_windows = duplicate_finder.generate_timespan_windows(start_time, end_time)
    
    # Run integrated process to find and remove duplicates in each time window
    logger.info(f"Starting integrated search and remove process for {len(time_windows)} time windows")
    run_parallelized_process(duplicate_finder, duplicate_remover, file_processor, session, index, time_windows, logger)
    
    logger.info("Completed processing all time windows")
    return True

def update_config_from_args(config, args):
    """
    Update configuration with command-line arguments
    
    Args:
        config (configparser.ConfigParser): Configuration object
        args (argparse.Namespace): Command-line arguments
    """
    # Update general section
    if args.max_workers is not None:
        config['general']['max_workers'] = str(args.max_workers)
    if args.batch_size is not None:
        config['general']['batch_size'] = str(args.batch_size)
    
    # Update splunk section
    if args.url is not None:
        config['splunk']['url'] = args.url
    if args.jwt_token is not None:
        config['splunk']['jwt_token'] = args.jwt_token
    if args.verify_ssl is not None:
        config['splunk']['verify_ssl'] = str(args.verify_ssl)
    
    # Update search section
    if args.index is not None:
        config['search']['index'] = args.index
    if args.start_time is not None:
        config['search']['start_time'] = args.start_time
    if args.end_time is not None:
        config['search']['end_time'] = args.end_time

def run_parallelized_process(duplicate_finder, duplicate_remover, file_processor, session, index, time_windows, logger):
    """Run integrated search and delete process in parallel batches"""
    max_workers = int(duplicate_finder.config['general'].get('max_workers', 1))  # Default to 1 if not configured
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        for i in range(0, len(time_windows), max_workers):
            batch = time_windows[i:i+max_workers]
            
            # Submit batch of searches
            batch_futures = [
                executor.submit(process_time_window, duplicate_finder, duplicate_remover, file_processor, session, index, start, end)
                for start, end in batch
            ]
            
            # Wait for current batch to complete before starting next
            for future in concurrent.futures.as_completed(batch_futures):
                try:
                    future.result()
                except Exception as e:
                    logger.error(f"Error in search execution: {str(e)}")
            
            # Sleep briefly between batches to avoid overwhelming Splunk
            time.sleep(2)

def process_time_window(duplicate_finder, duplicate_remover, file_processor, session, index, start_time, end_time):
    """Process a single time window to find and delete duplicates"""
    try:
        # Find duplicates for this time window with initial iteration=1
        csv_file = duplicate_finder.find_duplicates_integrated(
            session, 
            index, 
            start_time, 
            end_time, 
            duplicate_remover, 
            file_processor,
            iteration=1  # Explicitly start with iteration 1
        )
        
        return True
    except Exception as e:
        duplicate_finder.logger.error(f"Error processing time window {start_time} to {end_time}: {str(e)}")
        return False

if __name__ == "splunk_duplicate_remover.py":
    main()