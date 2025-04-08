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
            
            # Find the position of 'iter' part to determine where the index name ends
            iter_position = -1
            for i, part in enumerate(parts):
                if part.startswith('iter') or part.endswith('.csv'):
                    iter_position = i
                    break
            
            if iter_position >= 3:  # Need at least: [index_parts..., epoch1, epoch2, iter]
                # Everything before the last 3 parts is the index
                index_parts = parts[:(iter_position - 2)]
                index = '_'.join(index_parts)
                
                # Get epoch timestamps (always 2nd and 3rd from last before iter)
                earliest_epoch = int(parts[iter_position - 2])
                latest_epoch = int(parts[iter_position - 1])
                
                # Extract iteration number
                iteration_part = parts[iter_position].split('.')[0]  # Strip .csv if present
                if iteration_part.startswith('iter'):
                    iteration = int(iteration_part[4:])  # Get number after 'iter'
                else:
                    iteration = 1  # Default to 1 if no iteration found
                
                return {
                    'index': index,
                    'earliest_epoch': earliest_epoch,
                    'latest_epoch': latest_epoch,
                    'iteration': iteration
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
        Mark a CSV file as processed by compressing and moving it to a time-based subdirectory
        
        Args:
            csv_file (str): Path to CSV file
        
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            # Extract metadata to get timestamp info
            metadata = self.extract_metadata_from_filename(csv_file)
            if not metadata:
                self.logger.error(f"Could not extract metadata from filename: {csv_file}")
                return False

            # Create subdirectory based on epoch timestamp range
            subdir_name = f"{metadata['earliest_epoch']}_{metadata['latest_epoch']}"
            target_dir = os.path.join(self.processed_dir, subdir_name)
            
            # Create subdirectory if it doesn't exist
            if not os.path.exists(target_dir):
                os.makedirs(target_dir)
                self.logger.info(f"Created subdirectory: {target_dir}")

            filename = os.path.basename(csv_file)
            tar_filename = filename.replace('.csv', '.tgz')
            tar_path = os.path.join(target_dir, tar_filename)
            
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