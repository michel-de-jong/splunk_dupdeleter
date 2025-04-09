"""
Module for removing duplicate events from Splunk with enhanced performance
"""

import time
import json
from concurrent.futures import ThreadPoolExecutor

class DuplicateRemover:
    """
    Handles removing duplicate events from Splunk with optimized API calls
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
        # Read timeouts from config or use defaults
        self.job_status_interval = int(self.config.get('splunk', 'status_interval', fallback=5))
        self.job_timeout = int(self.config.get('splunk', 'ttl', fallback=180))
    
    def remove_duplicates(self, session, events, metadata):
        """
        Remove duplicate events from Splunk
        
        Args:
            session (requests.Session): Authenticated Splunk session
            events (list): List of event dictionaries from CSV
            metadata (dict): Metadata extracted from CSV filename
        
        Returns:
            bool: True if all duplicates were deleted, False otherwise
        """
        if not events:
            self.logger.info("No events to process")
            return True
        
        # Extract eventIDs and CDs directly from events
        event_ids_to_delete = []
        cds_to_delete = []
        
        for event in events:
            if 'eventID' in event and 'cd' in event:
                event_ids_to_delete.append(event['eventID'])
                cds_to_delete.append(event['cd'])
        
        if not event_ids_to_delete:
            self.logger.info("No events found with required fields")
            return True
        
        self.logger.info(f"Processing {len(event_ids_to_delete)} duplicate events")
        
        # Execute bulk deletion
        return self.delete_duplicate_events_bulk(
            session=session,
            index=metadata['index'],
            event_ids=event_ids_to_delete,
            cds=cds_to_delete,
            earliest=metadata['earliest_epoch'],
            latest=metadata['latest_epoch']
        )

    def delete_duplicate_events_bulk(self, session, index, event_ids, cds, earliest, latest):
        """
        Delete multiple duplicate events from Splunk in a single query with optimized API calls
        """
        try:
            # Get batch size from config, default to 5000 if not specified
            batch_size = int(self.config['general'].get('batch_size', 5000))
            total_batches = (len(event_ids) + batch_size - 1) // batch_size
            
            self.logger.info(f"Splitting deletion into {total_batches} batches (max {batch_size} events per batch)")
            
            # Maximum concurrent deletion jobs
            max_concurrent_jobs = min(5, int(self.config['general'].get('max_workers', 1)))
            
            # Using ThreadPoolExecutor to parallelize deletion jobs
            with ThreadPoolExecutor(max_workers=max_concurrent_jobs) as executor:
                # Submit all batches to the executor
                futures = []
                
                for batch_num in range(total_batches):
                    start_idx = batch_num * batch_size
                    end_idx = min(start_idx + batch_size, len(event_ids))
                    
                    # Get current batch of both eventIDs and CDs
                    batch_event_ids = event_ids[start_idx:end_idx]
                    batch_cds = cds[start_idx:end_idx]
                    
                    # IMPORTANT: Maintain exact cd-eventID pairs as they appeared in CSV
                    # Build individual conditions for each specific pair
                    pair_conditions = []
                    for i in range(len(batch_cds)):
                        pair_conditions.append(f'(eventID="{batch_event_ids[i]}" AND cd="{batch_cds[i]}")')
                    
                    # Join the pair conditions with OR
                    search_condition = ' OR '.join(pair_conditions)
                    
                    # Construct the delete query using both eventID and cd pairs
                    delete_query = f"""
                    search index={index} earliest={earliest} latest={latest} 
                    | eval eventID=md5(host.source.sourcetype._time._raw), cd=_cd
                    | search ({search_condition})
                    | delete
                    | where deleted>0
                    """
                    
                    self.logger.info(f"Submitting delete batch {batch_num+1}/{total_batches} with {len(batch_cds)} events")
                    
                    # Submit this batch to the executor
                    futures.append(
                        executor.submit(
                            self._execute_delete_job,
                            session=session,
                            delete_query=delete_query,
                            batch_num=batch_num,
                            total_batches=total_batches,
                            event_count=len(batch_cds)
                        )
                    )
                
                # Process results as they complete
                success_count = 0
                for future in futures:
                    if future.result():
                        success_count += 1
                
                # Check if all batches were successful
                return success_count == total_batches
            
        except Exception as e:
            self.logger.error(f"Error in bulk deletion: {str(e)}")
            self.stats_tracker.increment_delete_failure()
            return False

    def _execute_delete_job(self, session, delete_query, batch_num, total_batches, event_count):
        """
        Execute a single delete job with optimized API calls
        
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            self.logger.debug(f"Executing delete query for batch {batch_num+1}/{total_batches}")
            
            # PERFORMANCE IMPROVEMENT: Optimize the API request
            url = f"{self.config['splunk']['url']}/services/search/jobs"
            
            # PERFORMANCE IMPROVEMENT: Use JSON payload instead of form data
            payload = {
                'search': delete_query,
                'output_mode': 'json',
                'exec_mode': 'normal',
                'adhoc_search_level': 'fast',
                'timeout': self.config['splunk'].get('ttl', '180'),  # Get TTL from config, default to 180
            }
            
            # PERFORMANCE IMPROVEMENT: Send job as JSON instead of form-encoded for better performance
            headers = {
                'Content-Type': 'application/json'
            }
            
            response = session.post(url, data=json.dumps(payload), headers=headers)
            response.raise_for_status()
            job_id = response.json()['sid']
            
            self.logger.info(f"Batch {batch_num+1} delete job submitted: {job_id}")
            
            # PERFORMANCE IMPROVEMENT: Implement exponential backoff for status checks
            # Start with a small interval but increase it as we wait longer
            is_done = False
            status_url = f"{self.config['splunk']['url']}/services/search/jobs/{job_id}"
            check_interval = self.job_status_interval  # Start with configured value
            max_check_interval = 30  # Cap at 30 seconds
            total_wait_time = 0
            
            while not is_done and total_wait_time < self.job_timeout:
                # Sleep first to give the job time to start
                time.sleep(check_interval)
                total_wait_time += check_interval
                
                # PERFORMANCE IMPROVEMENT: Request only the fields we need
                response = session.get(
                    status_url, 
                    params={
                        'output_mode': 'json',
                        'fields': 'isDone,isFailed,doneProgress,messages'  # Only request fields we need
                    }
                )
                response.raise_for_status()
                status = response.json()['entry'][0]['content']
                
                if status['isDone']:
                    is_done = True
                    # Check if the job was successful
                    if status.get('isFailed', False):
                        self.logger.error(f"Delete job {job_id} failed: {status.get('messages', 'No details')}")
                        return False
                    else:
                        # PERFORMANCE IMPROVEMENT: Only get results if job succeeded
                        return self._get_delete_results(session, job_id, batch_num, event_count)
                else:
                    progress = round(float(status['doneProgress']) * 100, 2)
                    self.logger.debug(f"Delete job {job_id} in progress: {progress}%")
                    
                    # Increase check interval with exponential backoff, capped at max_check_interval
                    check_interval = min(check_interval * 1.5, max_check_interval)
            
            # If we're here, we timed out
            if not is_done:
                self.logger.error(f"Delete job {job_id} timed out after {total_wait_time} seconds")
                return False
                
            return True
            
        except Exception as e:
            self.logger.error(f"Error executing delete job for batch {batch_num+1}: {str(e)}")
            self.stats_tracker.increment_delete_failure()
            return False

    def _get_delete_results(self, session, job_id, batch_num, event_count):
        """
        Get results from a completed delete job with optimized API calls
        
        Returns:
            bool: True if got results successfully, False otherwise
        """
        try:
            # PERFORMANCE IMPROVEMENT: Use count=0 to get all results in one request
            results_url = f"{self.config['splunk']['url']}/services/search/jobs/{job_id}/results"
            results_response = session.get(
                results_url,
                params={
                    'output_mode': 'json', 
                    'count': 0,
                    'f': 'deleted,index'  # Only fetch the fields we need
                }
            )
            results_response.raise_for_status()
            results_json = results_response.json()
            
            deleted_count = sum(
                int(result.get('deleted', 0)) 
                for result in results_json.get('results', [])
                if result.get('index') == '__ALL__'
            )
            
            self.logger.info(f"Batch {batch_num+1}: Deleted {deleted_count} events")
            
            # Increment stats counter for each deleted event
            # Only count actual deleted events, not the events we attempted to delete
            for _ in range(min(deleted_count, event_count)):
                self.stats_tracker.increment_delete_success()
                
            return True
        except Exception as e:
            self.logger.warning(f"Error getting deletion results for job {job_id}: {str(e)}")
            return False