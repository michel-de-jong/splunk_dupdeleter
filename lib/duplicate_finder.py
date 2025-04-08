"""
Module for finding duplicate events in Splunk
"""

from datetime import datetime, timedelta
import os
import time
import csv

class DuplicateFinder:
    """
    Handles finding duplicate events in Splunk
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
            
            # Modified search query to ensure _cd is correctly captured
            search_query = f"""
            search index={index} earliest={earliest_time} latest={latest_time}
            | eval eventID=md5(host.source.sourcetype._time._raw), cd=_cd
            | search 
                [| search index={index} earliest={earliest_time} latest={latest_time}
                | eval eventID=md5(host.source.sourcetype._time._raw), cd=_cd
                | stats first(_cd) as cd count by eventID
                | search count>1 
                | table eventID cd]
            | table eventID cd
            """
            
            # Run search and get results
            url = f"{self.config['splunk']['url']}/services/search/jobs"
            payload = {
                'search': search_query,
                'output_mode': 'json',
                'exec_mode': 'normal',
                'ttl': self.config['splunk'].get('ttl', '20')  # Get TTL from config, default to 20
            }
            
            response = session.post(url, data=payload)
            response.raise_for_status()
            job_id = response.json()['sid']
            
            self.logger.debug(f"Search job submitted: {job_id} for timespan {earliest} to {latest} (iteration {iteration})")
            
            # Wait for job completion and handle results
            csv_filepath = self._wait_for_job_and_export_results(
                session, job_id, index, earliest, latest, 
                earliest_epoch, latest_epoch, iteration
            )
            
            if csv_filepath:
                self.logger.info(f"Found duplicates in timespan {earliest} to {latest} (iteration {iteration}), processing now")
                
                # Check if we hit the result limit BEFORE processing
                hit_limit = self._hit_result_limit(csv_filepath)
                
                # Process and remove duplicates
                metadata = file_processor.extract_metadata_from_filename(csv_filepath)
                if not metadata:
                    return None
                
                events = file_processor.read_events_from_csv(csv_filepath)
                success = duplicate_remover.remove_duplicates(session, events, metadata)
                
                if success:
                    file_processor.mark_as_processed(csv_filepath)
                    
                    # If we hit the limit, start next iteration
                    if hit_limit:
                        self.logger.info(f"Hit 10000 result limit, running additional search for same timespan (iteration {iteration + 1})")
                        return self.find_duplicates_integrated(
                            session, index, earliest, latest, duplicate_remover, file_processor, iteration + 1
                        )
                else:
                    self.logger.warning(f"Failed to remove duplicates for {csv_filepath}")
            else:
                self.logger.info(f"No duplicate events found in timespan {earliest} to {latest} (iteration {iteration})")
            
            self.stats_tracker.increment_search_success()
            return csv_filepath
            
        except Exception as e:
            self.logger.error(f"Error in integrated find/remove: {str(e)}")
            self.stats_tracker.increment_search_failure()
            return None

    def _wait_for_job_and_export_results(self, session, job_id, index, earliest, latest, earliest_epoch, latest_epoch, iteration):
        """
        Wait for a search job to complete and export results to CSV
        """
        import os
        import time
        
        try:
            is_done = False
            status_url = f"{self.config['splunk']['url']}/services/search/jobs/{job_id}"
            
            while not is_done:
                response = session.get(status_url, params={'output_mode': 'json'})
                response.raise_for_status()
                status = response.json()['entry'][0]['content']
                
                if status['isDone']:
                    is_done = True
                else:
                    progress = round(float(status['doneProgress']) * 100, 2)
                    self.logger.debug(f"Job {job_id} in progress: {progress}%")
                    time.sleep(5)
            
            # Once job is done, get results
            if int(status['resultCount']) > 0:
                results_url = f"{self.config['splunk']['url']}/services/search/jobs/{job_id}/results"
                response = session.get(
                    results_url,
                    params={
                        'output_mode': 'csv',
                        'count': 0  # get all results
                    }
                )
                response.raise_for_status()
                
                # Create CSV filename with index, timespan info and iteration number
                file_name = f"{index}_{earliest_epoch}_{latest_epoch}_iter{iteration}.csv"
                file_path = os.path.join(self.csv_dir, file_name)
                
                self.logger.debug(f"Attempting to write results to: {file_path}")
                
                # Verify directory exists and is writable
                if not os.path.exists(self.csv_dir):
                    self.logger.error(f"CSV directory does not exist: {self.csv_dir}")
                    return None
                    
                if not os.access(self.csv_dir, os.W_OK):
                    self.logger.error(f"CSV directory is not writable: {self.csv_dir}")
                    return None
                
                # Save results to CSV with error handling
                try:
                    with open(file_path, 'w', newline='', encoding='utf-8') as csvfile:
                        csvfile.write(response.text)
                    
                    # Verify file was created
                    if os.path.exists(file_path):
                        self.logger.info(f"Successfully saved {status['resultCount']} duplicate events to {file_path}")
                        return file_path
                    else:
                        self.logger.error(f"Failed to create file: {file_path}")
                        return None
                        
                except IOError as e:
                    self.logger.error(f"IOError while writing CSV file {file_path}: {str(e)}")
                    return None
                    
            else:
                self.logger.info(f"No duplicate events found in timespan {earliest} to {latest}")
                return None
                
        except Exception as e:
            self.logger.error(f"Error in _wait_for_job_and_export_results: {str(e)}")
            return None

    def _hit_result_limit(self, csv_filepath):
        """Check if we hit the 10000 result limit"""
        with open(csv_filepath, 'r') as f:
            return sum(1 for _ in csv.reader(f)) > 10000