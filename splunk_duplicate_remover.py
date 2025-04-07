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
    parser.add_argument('--config', required=True, help='Path to configuration file (INI or JSON)')
    parser.add_argument('--debug', action='store_true', help='Enable debug logging')
    args = parser.parse_args()
    
    # Load configuration
    config = ConfigLoader(args.config).load()
    
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
    
    # Phase 1: Run searches to identify duplicates
    logger.info(f"Starting Phase 1: Finding duplicate events in {len(time_windows)} time windows")
    run_parallelized_searches(duplicate_finder, session, index, time_windows, logger)
    
    # Phase 2: Process CSV files and delete duplicates
    logger.info("Starting Phase 2: Processing CSV files and deleting duplicates")
    unprocessed_files = file_processor.get_unprocessed_csv_files()
    
    if not unprocessed_files:
        logger.info("No unprocessed CSV files found.")
        return True
    
    logger.info(f"Found {len(unprocessed_files)} unprocessed CSV files to process")
    
    # Process each CSV file
    for csv_file in unprocessed_files:
        logger.info(f"Processing {csv_file}")
        process_file(csv_file, duplicate_remover, file_processor, session, logger)
    
    logger.info("Completed processing all CSV files")
    return True

def run_parallelized_searches(duplicate_finder, session, index, time_windows, logger):
    """Run searches in parallel batches"""
    with concurrent.futures.ThreadPoolExecutor(max_workers=6) as executor:
        for i in range(0, len(time_windows), 6):
            batch = time_windows[i:i+6]
            
            # Submit batch of searches
            batch_futures = [
                executor.submit(duplicate_finder.find_duplicates, session, index, start, end)
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

def process_file(csv_file, duplicate_remover, file_processor, session, logger):
    """Process a single CSV file to delete duplicates"""
    try:
        # Extract metadata from CSV file
        metadata = file_processor.extract_metadata_from_filename(csv_file)
        if not metadata:
            return False
        
        # Process events from CSV
        events = file_processor.read_events_from_csv(csv_file)
        
        # Delete duplicates
        success = duplicate_remover.remove_duplicates(session, events, metadata)
        
        # Mark as processed if successful
        if success:
            file_processor.mark_as_processed(csv_file)
        
        return success
    except Exception as e:
        logger.error(f"Error processing file {csv_file}: {str(e)}")
        return False

if __name__ == "__main__":
    main()