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
        
        # Group events by eventID
        event_groups = {}
        for event in events:
            if 'eventID' in event and 'cd' in event:
                event_id = event['eventID']
                if event_id not in event_groups:
                    event_groups[event_id] = []
                event_groups[event_id].append(event)
        
        # Build lists for eventIDs and CDs to be used in bulk deletion
        # Keep the first event of each eventID group, delete the rest
        event_ids_to_delete = []
        cds_to_delete = []
        
        for event_id, events in event_groups.items():
            if len(events) <= 1:
                continue  # Skip if not actually a duplicate
                
            # Skip the first event (keep it), add the rest to delete list
            for i, event in enumerate(events):
                if i == 0:
                    continue  # Keep the first one
                    
                event_ids_to_delete.append(event_id)
                cds_to_delete.append(event['cd'])
        
        if not event_ids_to_delete:
            self.logger.info("No duplicate events to delete after filtering")
            return True
            
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
            # Build OR conditions for eventIDs and CDs
            event_id_conditions = ' OR '.join([f'eventID="{event_id}"' for event_id in event_ids])
            cd_conditions = ' OR '.join([f'cd="{cd}"' for cd in cds])
            
            # Construct the delete query with all conditions
            delete_query = f"""
            search index={index} earliest={earliest} latest={latest}
            | eval eventID=md5(host.source.sourcetype._time._raw), cd=_cd
            | search ({event_id_conditions}) ({cd_conditions})
            | delete
            """
            
            self.logger.info(f"Deleting {len(event_ids)} duplicate events in bulk")
            
            url = f"{self.config['splunk']['url']}/services/search/jobs"
            payload = {
                'search': delete_query,
                'output_mode': 'json',
                'exec_mode': 'normal'
            }
            
            response = session.post(url, data=payload)
            response.raise_for_status()
            job_id = response.json()['sid']
            
            self.logger.debug(f"Bulk delete job submitted: {job_id}")
            
            # Wait for delete job completion
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
                    self.logger.debug(f"Delete job {job_id} in progress: {progress}%")
                    time.sleep(2)
            
            # Increment stats counter for each deleted event
            for _ in range(len(event_ids)):
                self.stats_tracker.increment_delete_success()
                
            return True
            
        except Exception as e:
            self.logger.error(f"Error in bulk deletion: {str(e)}")
            # Increment failure counter only once for the bulk operation
            self.stats_tracker.increment_delete_failure()
            return False