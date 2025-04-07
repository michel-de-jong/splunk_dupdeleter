"""
File processing module for handling CSV files
"""

import os
import csv
import tarfile

class FileProcessor:
    """
    Handles CSV file operations
    """
    
    def __init__(self, config, logger):
        """
        Initialize with configuration and logger
        
        Args:
            config (configparser.ConfigParser): Configuration
            logger (logging.Logger): Logger instance
        """
        self.config = config
        self.logger = logger
        self.csv_dir = config.get('general', 'csv_dir', fallback='csv_output')
        self.processed_dir = config.get('general', 'processed_dir', fallback='processed_csv')
        
        # Create directories if they don't exist
        self._ensure_directories_exist()
    
    def _ensure_directories_exist(self):
        """
        Create required directories if they don't exist
        """
        for directory in [self.csv_dir, self.processed_dir]:
            if not os.path.exists(directory):
                os.makedirs(directory)
                self.logger.info(f"Created directory: {directory}")
    
    def get_unprocessed_csv_files(self):
        """
        Get list of unprocessed CSV files
        
        Returns:
            list: List of paths to unprocessed CSV files
        """
        if not os.path.exists(self.csv_dir):
            return []
        
        return [os.path.join(self.csv_dir, f) for f in os.listdir(self.csv_dir) 
                if f.endswith('.csv')]
    
    def extract_metadata_from_filename(self, csv_file):
        """
        Extract metadata from CSV filename
        
        Args:
            csv_file (str): Path to CSV file
        
        Returns:
            dict: Metadata from filename or None if invalid
        """
        try:
            filename = os.path.basename(csv_file)
            parts = filename.split('_')
            if len(parts) >= 5:
                return {
                    'index': parts[0],
                    'start_time': parts[1],
                    'end_time': parts[2],
                    'earliest_epoch': int(parts[3]),
                    'latest_epoch': int(parts[4].split('.')[0])  # Remove .csv extension
                }
            else:
                self.logger.error(f"Invalid CSV filename format: {filename}")
                return None
        except Exception as e:
            self.logger.error(f"Error extracting metadata from filename: {str(e)}")
            return None
    
    def read_events_from_csv(self, csv_file):
        """
        Read events from CSV file
        
        Args:
            csv_file (str): Path to CSV file
        
        Returns:
            list: List of event dictionaries
        """
        events = []
        try:
            with open(csv_file, 'r', newline='') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    events.append(row)
            return events
        except Exception as e:
            self.logger.error(f"Error reading CSV file: {str(e)}")
            return []
    
    def mark_as_processed(self, csv_file):
        """
        Mark a CSV file as processed by compressing and moving it
        
        Args:
            csv_file (str): Path to CSV file
        
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            # Ensure processed directory exists
            if not os.path.exists(self.processed_dir):
                os.makedirs(self.processed_dir)
                self.logger.info(f"Created directory: {self.processed_dir}")
                
            filename = os.path.basename(csv_file)
            tar_filename = filename.replace('.csv', '.tgz')
            tar_path = os.path.join(self.processed_dir, tar_filename)
            
            # Create tar.gz file
            with tarfile.open(tar_path, "w:gz") as tar:
                tar.add(csv_file, arcname=filename)
            
            # Remove original CSV file
            os.remove(csv_file)
            self.logger.info(f"Marked CSV as processed: {csv_file} -> {tar_path}")
            return True
        except Exception as e:
            self.logger.error(f"Error marking CSV as processed: {str(e)}")
            return False