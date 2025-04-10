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
        self.logger.debug(f"Starting remove_duplicates with {len(events) if events else 0} events")
        self.logger.debug(f"Metadata: index={metadata.get('index')}, earliest={metadata.get('earliest_epoch')}, latest={metadata.get('latest_epoch')}")
        
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
            self.logger.debug("Missing eventID or cd fields in all events")
            return True
        
        self.logger.info(f"Processing {len(event_ids_to_delete)} duplicate events")
        self.logger.debug(f"First eventID: {event_ids_to_delete[0] if event_ids_to_delete else 'None'}, first cd: {cds_to_delete[0] if cds_to_delete else 'None'}")
        
        # Execute bulk deletion
        result = self.delete_duplicate_events_bulk(
            session=session,
            index=metadata['index'],
            event_ids=event_ids_to_delete,
            cds=cds_to_delete,
            earliest=metadata['earliest_epoch'],
            latest=metadata['latest_epoch']
        )
        
        self.logger.debug(f"delete_duplicate_events_bulk result: {result}")
        return result

    def delete_duplicate_events_bulk(self, session, index, event_ids, cds, earliest, latest):
        """
        Delete multiple duplicate events from Splunk in a single query
        """
        try:
            # Get batch size from config, default to 5000 if not specified
            batch_size = int(self.config['general'].get('batch_size', 5000))
            total_batches = (len(event_ids) + batch_size - 1) // batch_size
            
            self.logger.info(f"Splitting deletion into {total_batches} batches (max {batch_size} events per batch)")
            
            for batch_num in range(total_batches):
                start_idx = batch_num * batch_size
                end_idx = min(start_idx + batch_size, len(event_ids))
                
                # Get current batch of both eventIDs and CDs
                batch_event_ids = event_ids[start_idx:end_idx]
                batch_cds = cds[start_idx:end_idx]
                
                # Build combined conditions for batch using both eventID and cd pairs
                pair_conditions = []
                for i in range(len(batch_cds)):
                    pair_conditions.append(f'(eventID="{batch_event_ids[i]}" AND cd="{batch_cds[i]}")')
                
                # Join the pair conditions with OR
                search_condition = ' OR '.join(pair_conditions)
                
                # Construct the delete query using both eventID and cd
                delete_query = f"""
                search index={index} earliest={earliest} latest={latest} 
                | eval eventID=md5(host.source.sourcetype._time._raw), cd=_cd
                | search ({search_condition})
                | delete
                | where deleted>0
                """
                
                self.logger.info(f"Deleting batch {batch_num+1}/{total_batches} with {len(batch_cds)} events")
                self.logger.debug(f"Delete query: {delete_query}")
                
                url = f"{self.config['splunk']['url']}/services/search/jobs"
                payload = {
                    'search': delete_query,
                    'output_mode': 'json',
                    'exec_mode': 'normal',
                    'adhoc_search_level': 'fast',
                    'timeout': self.config['splunk'].get('ttl', '180')  # Get TTL from config, default to 180
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
                                deleted_count = sum(
                                    int(result.get('deleted', 0)) 
                                    for result in results_json.get('results', [])
                                    if result.get('index') == '__ALL__'
                                )
                                self.logger.info(f"Batch {batch_num+1}: Deleted {deleted_count} events")
                            except Exception as res_e:
                                self.logger.warning(f"Couldn't get deletion results: {str(res_e)}")
                    else:
                        progress = round(float(status['doneProgress']) * 100, 2)
                        self.logger.debug(f"Delete job {job_id} in progress: {progress}%")
                        time.sleep(5)
            
                # Increment stats counter for each deleted event in this batch
                for _ in range(len(batch_cds)):
                    self.stats_tracker.increment_delete_success()
            
            return True
            
        except Exception as e:
            self.logger.error(f"Error in bulk deletion: {str(e)}")
            self.stats_tracker.increment_delete_failure()
            return False