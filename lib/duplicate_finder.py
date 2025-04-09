"""
Module for finding duplicate events in Splunk with performance optimizations
"""

from datetime import datetime, timedelta
import os
import time
import csv

class DuplicateFinder:
    """
    Handles finding duplicate events in Splunk with performance optimizations
    """
    
    def __init__(self, config, logger, stats_tracker):
        """
        Initialize with configuration and logger
        
        Args:
            config (configparser.ConfigParser): Configuration
            logger (logging.Logger): Logger instance
            stats_tracker (StatsTracker): Statistics tracker
        """
        self.config = config
        self.logger = logger
        self.stats_tracker = stats_tracker
        self.csv_dir = config.get('general', 'csv_dir', fallback='csv_output')
    
    def generate_timespan_windows(self, start_time, end_time, window_minutes=5):
        """
        Generate time windows for searches
        
        Args:
            start_time (str): Start time in ISO format
            end_time (str): End time in ISO format
            window_minutes (int, optional): Size of each window in minutes. Defaults to 5.
        
        Returns:
            list: List of (start, end) tuples for each time window
        """
        start_dt = datetime.fromisoformat(start_time) if isinstance(start_time, str) else start_time
        end_dt = datetime.fromisoformat(end_time) if isinstance(end_time, str) else end_time
        
        current = start_dt
        windows = []
        
        while current < end_dt:
            window_end = min(current + timedelta(minutes=window_minutes), end_dt)
            windows.append((current, window_end))
            current = window_end
        
        self.logger.info(f"Generated {len(windows)} search windows")
        return windows

    def find_duplicates_integrated(self, session, index, earliest, latest, duplicate_remover, file_processor, iteration=1):
        """
        Find duplicates and immediately process them for removal
        
        Args:
            session (requests.Session): Authenticated Splunk session
            index (str): Splunk index name
            earliest (datetime): Start time for search window
            latest (datetime): End time for search window
            duplicate_remover (DuplicateRemover): Instance for removing duplicates
            file_processor (FileProcessor): Instance for processing CSV files
            iteration (int, optional): Current iteration number for recursive searches. Defaults to 1.
            
        Returns:
            str: Path to final CSV file or None if no duplicates found
        """
        try:
            # Convert to epoch timestamps for Splunk query
            earliest_epoch = int(earliest.timestamp())
            latest_epoch = int(latest.timestamp())
            
            # Format times in Splunk format for the API call
            earliest_time = f"{earliest_epoch}"  # Epoch format for Splunk
            latest_time = f"{latest_epoch}"      # Epoch format for Splunk
            
            self.logger.info(f"Starting integrated find/remove for timespan {earliest} to {latest} (iteration {iteration})")
            
            # Optimized search query to ensure _cd is correctly captured
            # Use subsearches and avoid extra steps when possible
            search_query = f"""
            search index={index} earliest={earliest_time} latest={latest_time}
            | fields host source sourcetype _time _raw _cd
            | eval eventID=md5(host.source.sourcetype._time._raw), cd=_cd
            | search 
                [| search index={index} earliest={earliest_time} latest={latest_time}
                | fields host source sourcetype _time _raw _cd
                | eval eventID=md5(host.source.sourcetype._time._raw)
                | stats first(_cd) as cd count by eventID
                | where count>1 
                | fields eventID cd]
            | fields eventID cd
            """
            
            
            # PERFORMANCE IMPROVEMENT: Use oneshot search with export directly instead of normal job creation
            # This avoids the overhead of job creation, monitoring, and results fetching
            url = f"{self.config['splunk']['url']}/services/search/jobs/export"
            payload = {
                'search': search_query,
                'output_mode': 'csv',
                'earliest_time': earliest_time,
                'latest_time': latest_time,
                'adhoc_search_level': 'fast',
                'timeout': self.config['splunk'].get('ttl', '180'),  # Get TTL from config, default to 180
                'exec_mode': 'oneshot',      # Use oneshot mode for faster execution
            }
            
            self.logger.debug(f"Executing oneshot search for timespan {earliest} to {latest} (iteration {iteration})")
            
            # Execute the search with optimized connection settings
            response = session.post(
                url, 
                data=payload,
                stream=True   # Stream the response to handle large result sets
            )
            response.raise_for_status()
            
            # Create CSV filename with index, timespan info and iteration number
            file_name = f"{index}_{earliest_epoch}_{latest_epoch}_iter{iteration}.csv"
            file_path = os.path.join(self.csv_dir, file_name)
            
            # Check if directory exists
            if not os.path.exists(self.csv_dir):
                os.makedirs(self.csv_dir)
                
            # Process the response directly to a file
            with open(file_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
            
            # Check if we got any results
            with open(file_path, 'r') as f:
                # Read first line (header)
                header = f.readline().strip()
                # Read second line to see if we have any results
                first_data = f.readline().strip()
                
                if not first_data:
                    # No results found
                    os.remove(file_path)  # Clean up empty file
                    self.logger.info(f"No duplicate events found in timespan {earliest} to {latest} (iteration {iteration})")
                    return None
            
            self.logger.info(f"Found duplicates in timespan {earliest} to {latest} (iteration {iteration}), processing now")
            
            # Check if we hit the result limit
            hit_limit = self._hit_result_limit(file_path)
            
            # Process and remove duplicates
            metadata = file_processor.extract_metadata_from_filename(file_path)
            if not metadata:
                return None
            
            events = file_processor.read_events_from_csv(file_path)
            success = duplicate_remover.remove_duplicates(session, events, metadata)
            
            if success:
                file_processor.mark_as_processed(file_path)
                
                # If we hit the limit, start next iteration
                if hit_limit:
                    self.logger.info(f"Hit result limit, running additional search for same timespan (iteration {iteration + 1})")
                    return self.find_duplicates_integrated(
                        session, index, earliest, latest, duplicate_remover, file_processor, iteration + 1
                    )
            else:
                self.logger.warning(f"Failed to remove duplicates for {file_path}")
            
            self.stats_tracker.increment_search_success()
            return file_path
            
        except Exception as e:
            self.logger.error(f"Error in integrated find/remove: {str(e)}")
            self.stats_tracker.increment_search_failure()
            return None

    def _hit_result_limit(self, csv_filepath):
        """Check if we hit the results limit"""
        try:
            with open(csv_filepath, 'r') as f:
                row_count = sum(1 for _ in csv.reader(f))
                # Subtract 1 for header row
                return (row_count - 1) >= int(self.config['general'].get('batch_size', 5000))
        except Exception as e:
            self.logger.error(f"Error checking result limit: {str(e)}")
            return False