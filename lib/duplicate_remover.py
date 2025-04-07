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
        
        success = True
        # Process each group of duplicates
        for event_id, events in event_groups.items():
            if len(events) <= 1:
                continue  # Skip if not actually a duplicate
            
            # Keep first event, delete the rest
            for i, event in enumerate(events):
                if i == 0:
                    continue  # Keep the first one
                
                # Delete duplicate event
                if not self.delete_duplicate_event(
                    session=session,
                    index=metadata['index'],
                    event_id=event_id,
                    cd=event['cd'],
                    earliest=metadata['earliest_epoch'],
                    latest=metadata['latest_epoch']
                ):
                    self.logger.warning(f"Failed to delete duplicate event: {event_id}")
                    success = False
        
        return success
    
    def delete_duplicate_event(self, session, index, event_id, cd, earliest, latest):
        """
        Delete a duplicate event from Splunk
        
        Args:
            session (requests.Session): Authenticated Splunk session
            index (str): Splunk index name
            event_id (str): Event ID to delete
            cd (str): CD value for the event
            earliest (int): Start time in epoch format
            latest (int): End time in epoch format
        
        Returns:
            bool: True if deletion was successful, False otherwise
        """
        try:
            delete_query = f"""
            search index={index} earliest={earliest} latest={latest}
            | eval eventID=md5(host.source.sourcetype._time._raw), cd=_cd
            | search eventID="{event_id}" cd="{cd}"
            | delete
            """
            
            url = f"{self.config['splunk']['url']}/services/search/jobs"
            payload = {
                'search': delete_query,
                'output_mode': 'json',
                'exec_mode': 'normal'
            }
            
            response = session.post(url, data=payload)
            response.raise_for_status()
            job_id = response.json()['sid']
            
            self.logger.debug(f"Delete job submitted: {job_id} for event {event_id}")
            
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
                    time.sleep(2)
            
            self.stats_tracker.increment_delete_success()
            return True
            
        except Exception as e:
            self.logger.error(f"Error deleting duplicate event: {str(e)}")
            self.stats_tracker.increment_delete_failure()
            return False