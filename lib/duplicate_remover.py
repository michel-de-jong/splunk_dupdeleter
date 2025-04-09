"""
Module for removing duplicate events from Splunk
"""

import time
from typing import List, Dict, Tuple

class DuplicateRemover:
    """
    Handles removing duplicate events from Splunk
    """
    
    def __init__(self, config, logger, stats_tracker):
        """Initialize with configuration and logger"""
        self.config = config
        self.logger = logger
        self.stats_tracker = stats_tracker
        # Cache frequently used config values
        self.batch_size = int(config['general'].get('batch_size', 5000))
        self.splunk_url = config['splunk']['url']
        self.ttl = config['splunk'].get('ttl', '180')

    def remove_duplicates(self, session, events: List[Dict], metadata: Dict) -> bool:
        """Remove duplicate events from Splunk"""
        if not events:
            self.logger.info("No events to process")
            return True
        
        # Use list comprehension for better performance
        event_pairs = [(e['eventID'], e['cd']) for e in events 
                      if 'eventID' in e and 'cd' in e]
        
        if not event_pairs:
            self.logger.info("No events found with required fields")
            return True

        # Unzip the pairs for better memory efficiency
        event_ids_to_delete, cds_to_delete = zip(*event_pairs)
        self.logger.info(f"Processing {len(event_ids_to_delete)} duplicate events")
        
        return self.delete_duplicate_events_bulk(
            session=session,
            index=metadata['index'],
            event_ids=event_ids_to_delete,
            cds=cds_to_delete,
            earliest=metadata['earliest_epoch'],
            latest=metadata['latest_epoch']
        )

    def delete_duplicate_events_bulk(self, session, index: str, event_ids: Tuple[str], 
                                   cds: Tuple[str], earliest: int, latest: int) -> bool:
        """Delete multiple duplicate events from Splunk in a single query"""
        try:
            total_batches = (len(event_ids) + self.batch_size - 1) // self.batch_size
            self.logger.info(f"Splitting deletion into {total_batches} batches (max {self.batch_size} events per batch)")
            
            for batch_num in range(total_batches):
                start_idx = batch_num * self.batch_size
                end_idx = min(start_idx + self.batch_size, len(event_ids))
                
                # Process current batch
                batch_event_ids = event_ids[start_idx:end_idx]
                batch_cds = cds[start_idx:end_idx]
                
                # Build search condition using join() instead of list append
                pair_conditions = [
                    f'(eventID="{eid}" AND cd="{cd}")' 
                    for eid, cd in zip(batch_event_ids, batch_cds)
                ]
                search_condition = ' OR '.join(pair_conditions)
                
                # Use f-string for query construction
                delete_query = (
                    f"search index={index} earliest={earliest} latest={latest} "
                    f"| eval eventID=md5(host.source.sourcetype._time._raw), cd=_cd "
                    f"| search ({search_condition}) "
                    "| delete "
                    "| where deleted>0"
                )
                
                self.logger.info(f"Deleting batch {batch_num+1}/{total_batches} with {len(batch_cds)} events")
                
                # Submit delete job
                job_id = self._submit_delete_job(session, delete_query)
                if not job_id:
                    return False
                
                # Monitor job completion
                if not self._monitor_delete_job(session, job_id, batch_num, len(batch_cds)):
                    return False
                
                # Update stats
                self.stats_tracker.increment_delete_success(len(batch_cds))
            
            return True
            
        except Exception as e:
            self.logger.error(f"Error in bulk deletion: {str(e)}")
            self.stats_tracker.increment_delete_failure()
            return False

    def _submit_delete_job(self, session, delete_query: str) -> str:
        """Submit delete job to Splunk"""
        try:
            payload = {
                'search': delete_query,
                'output_mode': 'json',
                'exec_mode': 'normal',
                'adhoc_search_level': 'fast',
                'timeout': self.ttl
            }
            
            response = session.post(f"{self.splunk_url}/services/search/jobs", data=payload)
            response.raise_for_status()
            return response.json()['sid']
            
        except Exception as e:
            self.logger.error(f"Error submitting delete job: {str(e)}")
            return None

    def _monitor_delete_job(self, session, job_id: str, batch_num: int, batch_size: int) -> bool:
        """Monitor delete job completion and results"""
        status_url = f"{self.splunk_url}/services/search/jobs/{job_id}"
        
        while True:
            try:
                response = session.get(status_url, params={'output_mode': 'json'})
                response.raise_for_status()
                status = response.json()['entry'][0]['content']
                
                if status['isDone']:
                    if status.get('isFailed', False):
                        self.logger.error(f"Delete job {job_id} failed: {status.get('messages', 'No details')}")
                        return False
                        
                    deleted_count = self._get_deletion_results(session, job_id)
                    self.logger.info(f"Batch {batch_num+1}: Deleted {deleted_count} events")
                    return True
                    
                time.sleep(5)
                
            except Exception as e:
                self.logger.error(f"Error monitoring delete job: {str(e)}")
                return False

    def _get_deletion_results(self, session, job_id: str) -> int:
        """Get deletion results count"""
        try:
            results_response = session.get(
                f"{self.splunk_url}/services/search/jobs/{job_id}/results",
                params={'output_mode': 'json', 'count': 0}
            )
            results_response.raise_for_status()
            results = results_response.json().get('results', [])
            return sum(int(r.get('deleted', 0)) for r in results if r.get('index') == '__ALL__')
            
        except Exception as e:
            self.logger.warning(f"Couldn't get deletion results: {str(e)}")
            return 0