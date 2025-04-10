# Splunk DupDeleter

A tool for deleting duplicate events in Splunk.

## Setup

1. Create your own `config.ini` based on the sample file located in `configs/` directory
2. Note that script execution time depends on the number of duplicates
   - Use `screen` or `tmux` for long-running processes if needed

## How it Works

- The script checks for completion of search jobs every 5 seconds
- Results are stored in a locally created CSV when the find search is complete
- Uses "eventID" and "cd" fields from the CSV to create deletion searches
- Polls every 5 seconds to check if deletion searches are finished
- Logs the number of deleted events per search

## Configuration

The performance can be tweaked with the following settings in `config.ini`:

| Setting | Description | Default |
|---------|-------------|---------|
| `max_workers` | Maximum concurrent Splunk searches | 1 |
| `batch` | Number of duplicated events to delete at once | 5000 |
| `TTL` | Time-to-live for find and delete searches (seconds) | 180 |

## Usage

Basic syntax:
```bash
python3 splunk_duplicate_remover.py
```

### Optional Arguments

| Argument | Description |
|----------|-------------|
| `-h, --help` | Show help message |
| `--debug` | Enable debug logging |
| `--max_workers <MAX_WORKERS>` | Maximum number of concurrent searches |
| `--batch_size <BATCH_SIZE>` | Batch size for processing events |
| `--url URL` | Splunk Cloud URL |
| `--jwt_token <JWT_TOKEN>` | JWT token for Splunk authentication |
| `--start_time <START_TIME>` | Start time for search window (ISO format) |
| `--end_time <END_TIME>` | End time for search window (ISO format) |
| `--verify_ssl <VERIFY_SSL>` | Whether to verify SSL certificates (true/false) |
| `--index <INDEX>` | Splunk index name to search |
| `--ttl` | Time-to-live value for completed Splunk searches (seconds) |
| `--compression_threshold_mb <COMPRESSION_THRESHOLD_MB>` | Size threshold in MB for compressing directories |
| `--max_storage_mb <MAX_STORAGE_MB>` | Maximum storage size in MB before cleanup |
