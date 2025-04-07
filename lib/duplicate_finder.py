"""
Module for finding duplicate events in Splunk
"""

from datetime import datetime, timedelta

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
    
    def generate_timespan_windows(self, start_time, end_time, window_minutes=10):
        """
        Generate time windows for searches
        
        Args:
            start_time (str): Start time in ISO format
            end_time (str): End time in ISO format
            window_minutes (int, optional): Size of each window in minutes. Defaults to 10.
        
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
    
    def find_duplicates(self, session, index, earliest, latest):
        """
        Submit a search to find duplicate events in a specific time window
        
        Args:
            session (requests.Session): Authenticated Splunk session
            index (str): Splunk index to search
            earliest (datetime): Start time for search window
            latest (datetime): End time for search window
        
        Returns:
            str: Path to CSV file with results or None if failed/no results
        """
        try:
            earliest_epoch = int(earliest.timestamp())
            latest_epoch = int(latest.timestamp())
            
            search_query = f"""
            search index={index} earliest={earliest_epoch} latest={latest_epoch}
            | eval eventID=md5(host.source.sourcetype._time._raw), cd=_cd
            | search 
                [| search index={index} earliest={earliest_epoch} latest={latest_epoch}
                | eval eventID=md5(host.source.sourcetype._time._raw), cd=_cd
                | stats first(cd) as cd count by eventID
                | search count>1 
                | table cd eventID]
            | fields host source sourcetype _time _raw eventID cd
            """

            url = f"{self.config['splunk']['url']}/services/search/jobs"
            payload = {
                'search': search_query,
                'output_mode': 'json',
                'exec_mode': 'normal'
            }
            
            response = session.post(url, data=payload)
            response.raise_for_status()
            job_id = response.json()['sid']
            
            self.logger.debug(f"Search job submitted: {job_id} for timespan {earliest} to {latest}")
            
            # Wait for job completion
            csv_filepath = self._wait_for_job_and_export_results(session, job_id, index, earliest, latest, earliest_epoch, latest_epoch)
            
            self.stats_tracker.increment_search_success()
            return csv_filepath
        
        except Exception as e:
            self.logger.error(f"Error submitting search: {str(e)}")
            self.stats_tracker.increment_search_failure()
            return None
    
    def _wait_for_job_and_export_results(self, session, job_id, index, earliest, latest, earliest_epoch, latest_epoch):
        """
        Wait for a search job to complete and export results to CSV
        
        Args:
            session (requests.Session): Authenticated Splunk session
            job_id (str): Splunk search job ID
            index (str): Splunk index name
            earliest (datetime): Start time for search window
            latest (datetime): End time for search window
            earliest_epoch (int): Start time in epoch format
            latest_epoch (int): End time in epoch format
        
        Returns:
            str: Path to CSV file with results or None if no results
        """
        import os
        import time
        
        is_done = False
        status_url = f"{self.config['splunk']['url']}/services/search/jobs/{job_id}"
        
        while not is_done:
            response = session.get(f"{status_url}", params={'output_mode': 'json'})
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
            
            # Create CSV filename with index and timespan info
            file_name = f"{index}_{earliest.strftime('%Y%m%d%H%M')}_{latest.strftime('%Y%m%d%H%M')}_{earliest_epoch}_{latest_epoch}.csv"
            file_path = os.path.join(self.csv_dir, file_name)
            
            # Save results to CSV
            with open(file_path, 'w', newline='') as csvfile:
                csvfile.write(response.text)
                
            self.logger.info(f"Saved {status['resultCount']} duplicate events to {file_path}")
            return file_path
        else:
            self.logger.info(f"No duplicate events found in timespan {earliest} to {latest}")
            return None