# splunk_dupdeleter
Deleting duplicate events in Splunk
<br/><br/>
- Create your own config.ini based on the sample file located in configs/
- It's possible the script requires a long time to complete, depending on the amount of duplicates
    - Take this into account and use screen or tmux if needed
- The script checks for completion of search jobs every 5 seconds
- The script will store the results in a locally created CSV when the find search is ready
- The script will use the "eventID" and "cd" fields in the CSV to create the deletion searches, and polls every 5  seconds if the deletion search is finished. The script will log amount of deleted events per search.
<br/><br/>
The following settings are tweakable via the config.ini file:
- max_workes (max concurrent Splunk searches)
    - Defaults to 6
- Batch (amount of duplicated events being deleted at once) (defines how big the OR statement in the search will be)
    - Defaults to 5000
- TTL 
    - The value is used for both the find and delete searches
    - Defaults to 180 seconds (to prevent hitting quota limits as much as possible)
<br/><br/>
- Syntax: python3 splunk_duplicate_remover.py
- Optional: -h (help)
- Optional: --debug (Enable debug mode, create an extra log file with all debug logs)
<br/><br/>
