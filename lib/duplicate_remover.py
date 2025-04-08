"""
Module for removing duplicate events from Splunk
"""

import time

class DuplicateRemover:
    """
    Handles removing duplicate events from Splunk
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
        Delete multiple duplicate events from Splunk in a single query
        
        Args:
            session (requests.Session): Authenticated Splunk session
            index (str): Splunk index name
            event_ids (list): List of event IDs to delete
            cds (list): List of CD values corresponding to event IDs
            earliest (int): Start time in epoch format
            latest (int): End time in epoch format
        
        Returns:
            bool: True if deletion was successful, False otherwise
        """
        try:
            # For better performance, separate into smaller batches of 1000 events
            batch_size = 1000
            total_batches = (len(event_ids) + batch_size - 1) // batch_size
            
            self.logger.info(f"Splitting deletion into {total_batches} batches (max {batch_size} events per batch)")
            
            for batch_num in range(total_batches):
                start_idx = batch_num * batch_size
                end_idx = min(start_idx + batch_size, len(event_ids))
                
                # Get current batch of CDs - we use these directly as they're the most efficient way to identify events
                batch_cds = cds[start_idx:end_idx]
                
                # Build combined conditions for batch using only CD values
                pair_conditions = []
                for i in range(len(batch_cds)):
                    pair_conditions.append(f'(_cd="{batch_cds[i]}")')
                
                # Join the pair conditions with OR
                search_condition = ' OR '.join(pair_conditions)
                
                # Construct the delete query using _cd directly and proper epoch format
                delete_query = f"""
                search index={index} earliest={earliest} latest={latest} ({search_condition})
                | delete
                """
                
                self.logger.info(f"Deleting batch {batch_num+1}/{total_batches} with {len(batch_cds)} events")
                self.logger.debug(f"Delete query: {delete_query}")
                
                url = f"{self.config['splunk']['url']}/services/search/jobs"
                payload = {
                    'search': delete_query,
                    'output_mode': 'json',
                    'exec_mode': 'normal',
                    'ttl': '300'  # Set TTL to 5 minutes (300 seconds)
                }
                
                response = session.post(url, data=payload)
                response.raise_for_status()
                job_id = response.json()['sid']
                
                self.logger.info(f"Bulk delete job submitted: {job_id}")
                
                # Wait for delete job completion
                is_done = False
                status_url = f"{self.config['splunk']['url']}/services/search/jobs/{job_id}"
                
                while not is_done:
                    response = session.get(status_url, params={'output_mode': 'json'})
                    response.raise_for_status()
                    status = response.json()['entry'][0]['content']
                    
                    if status['isDone']:
                        is_done = True
                        # Check if the job was successful
                        if status.get('isFailed', False):
                            self.logger.error(f"Delete job {job_id} failed: {status.get('messages', 'No details')}")
                            return False
                        else:
                            # Check actual events deleted from the job results
                            results_url = f"{self.config['splunk']['url']}/services/search/jobs/{job_id}/results"
                            try:
                                results_response = session.get(
                                    results_url,
                                    params={'output_mode': 'json', 'count': 0}
                                )
                                results_response.raise_for_status()
                                results_json = results_response.json()
                                deleted_count = len(results_json.get('results', []))
                                self.logger.info(f"Batch {batch_num+1}: Deleted {deleted_count} events")
                            except Exception as res_e:
                                self.logger.warning(f"Couldn't get deletion results: {str(res_e)}")
                    else:
                        progress = round(float(status['doneProgress']) * 100, 2)
                        self.logger.debug(f"Delete job {job_id} in progress: {progress}%")
                        time.sleep(2)
            
                # Increment stats counter for each deleted event in this batch
                for _ in range(len(batch_cds)):
                    self.stats_tracker.increment_delete_success()
            
            return True
            
        except Exception as e:
            self.logger.error(f"Error in bulk deletion: {str(e)}")
            self.stats_tracker.increment_delete_failure()
            return False