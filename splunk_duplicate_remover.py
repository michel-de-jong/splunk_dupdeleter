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

def main():
    """Main entry point for the application"""
    parser = argparse.ArgumentParser(description='Splunk Duplicate Event Finder and Remover')
    parser.add_argument('--debug', action='store_true', help='Enable debug logging')
    args = parser.parse_args()
    
    try:
        # Load configuration from default path (configs/config.ini)
        config = ConfigLoader().load()
    except FileNotFoundError as e:
        print(f"Error: {str(e)}")
        print("Please ensure configs/config.ini exists and is properly configured.")
        return False
    
    # Setup logging
    logger = setup_logger(config, args.debug)
    
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

def run_parallelized_process(duplicate_finder, duplicate_remover, file_processor, session, index, time_windows, logger):
    """Run integrated search and delete process in parallel batches"""
    with concurrent.futures.ThreadPoolExecutor(max_workers=6) as executor:
        for i in range(0, len(time_windows), 6):
            batch = time_windows[i:i+6]
            
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
        # Find duplicates for this time window
        csv_file = duplicate_finder.find_duplicates_integrated(
            session, 
            index, 
            start_time, 
            end_time, 
            duplicate_remover, 
            file_processor
        )
        
        return True
    except Exception as e:
        duplicate_finder.logger.error(f"Error processing time window {start_time} to {end_time}: {str(e)}")
        return False

if __name__ == "__main__":
    main()