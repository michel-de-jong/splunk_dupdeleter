# splunk_dupdeleter
Deleting duplicate events in Splunk
<br/><br/>
- Create your own config.ini based on the sample file located in configs/
- It's possible the script requires a long time to complete, depending on the amount of duplicates
    - Take this into account and use screen or tmux if needed
- The script checks for completion of search jobs every 5 seconds
- The script will store the results in a locally created CSV when the find search is ready
- The script will use the "eventID" and "cd" fields in the CSV to create the deletion searches, and polls every 5 seconds if the deletion search is finished. The script will log amount of deleted events per search.
<br/><br/>
The following settings are tweakable via the config.ini file:
- max_workes (max concurrent Splunk searches)
    - Defaults to 1
- Batch (amount of duplicated events being deleted at once) (defines how big the OR statement in the search will be)
    - Defaults to 5000
- TTL 
    - The value is used for both the find and delete searches
    - Defaults to 180 seconds (to prevent hitting quota limits as much as possible)
<br/><br/>
- Syntax: python3 splunk_duplicate_remover.py
- Optional: 
    -h, --help                                              help<br/>
    --debug                                                 Enable debug logging<br/>
    --max_workers <MAX_WORKERS>                             Maximum number of concurrent searches<br/>
    --batch_size <BATCH_SIZE>                               Batch size for processing events<br/>
    --url URL                                               Splunk Cloud URL<br/>
    --jwt_token <JWT_TOKEN>                                 JWT token for Splunk authentication<br/>
    --start_time <START_TIME>                               Start time for search window (ISO format)<br/>
    --end_time <END_TIME>                                   End time for search window (ISO format)<br/>
    --verify_ssl <VERIFY_SSL>                               Whether to verify SSL certificates (true/false)<br/>
    --index <INDEX>                                         Splunk index name to search<br/>
    --ttl                                                   Time-to-live value for completed Splunk searches after in seconds<br/>
    --compression_threshold_mb <COMPRESSION_THRESHOLD_MB>   Size threshold in MB for compressing directories<br/>
    --max_storage_mb <MAX_STORAGE_MB>                       Maximum storage size in MB before cleanup<br/>
<br/><br/>
