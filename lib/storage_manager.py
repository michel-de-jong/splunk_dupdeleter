"""
Storage management module for maintaining processed_csv directory size
"""

import os
import tarfile
import shutil
import logging
from datetime import datetime

class StorageManager:
    """
    Manages the size of processed_csv directory with compression and cleanup
    """
    
    def __init__(self, config, main_logger=None):
        """
        Initialize with configuration and logger
        
        Args:
            config (configparser.ConfigParser): Configuration
            main_logger (logging.Logger, optional): Main logger instance
        """
        self.config = config
        self.processed_dir = config.get('general', 'processed_dir', fallback='processed_csv')
        
        # Get size thresholds from config (in MB)
        self.compression_threshold_mb = float(config.get('storage', 'compression_threshold_mb', fallback='50'))
        self.max_storage_mb = float(config.get('storage', 'max_storage_mb', fallback='500'))
        
        # Setup dedicated logger for storage operations
        self.log_dir = 'log'
        self.log_file = config.get('storage', 'log_file', fallback='storage_manager.log')
        self.logger = self._setup_logger() if main_logger is None else main_logger
    
    def _setup_logger(self):
        """Setup dedicated logger for storage operations"""
        if not os.path.exists(self.log_dir):
            os.makedirs(self.log_dir)
            
        logger = logging.getLogger('storage_manager')
        logger.setLevel(logging.INFO)
        
        # Clear any existing handlers to avoid duplicates
        if logger.handlers:
            logger.handlers.clear()
        
        # Create file handler
        log_path = os.path.join(self.log_dir, self.log_file)
        file_handler = logging.FileHandler(log_path)
        
        # Set format
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(formatter)
        
        # Add handler to logger
        logger.addHandler(file_handler)
        
        return logger
    
    def check_storage(self):
        """
        Check storage size and perform maintenance if needed
        
        Returns:
            bool: True if operations were successful
        """
        try:
            self.logger.debug("Starting storage maintenance check")
            
            # Check if directory exists
            if not os.path.exists(self.processed_dir):
                self.logger.info(f"Processed directory does not exist: {self.processed_dir}")
                self.logger.debug(f"Cannot perform maintenance on non-existent directory: {self.processed_dir}")
                return True
            
            # Get the current size of the processed directory
            current_size_mb = self._get_directory_size_mb(self.processed_dir)
            self.logger.info(f"Current processed directory size: {current_size_mb:.2f} MB")
            self.logger.debug(f"Compression threshold: {self.compression_threshold_mb} MB, Max storage: {self.max_storage_mb} MB")
            
            # First check: Compress subdirectories if total size exceeds threshold
            if current_size_mb > self.compression_threshold_mb:
                self.logger.info(f"Size exceeds compression threshold ({self.compression_threshold_mb} MB), compressing subdirectories")
                self.logger.debug(f"Size before compression: {current_size_mb:.2f} MB")
                self._compress_subdirectories()
                
                # Recalculate size after compression
                current_size_mb = self._get_directory_size_mb(self.processed_dir)
                self.logger.info(f"Size after compression: {current_size_mb:.2f} MB")
                self.logger.debug(f"Compression reduced size by {(current_size_mb - self._get_directory_size_mb(self.processed_dir)):.2f} MB")
            else:
                self.logger.debug(f"Size ({current_size_mb:.2f} MB) is below compression threshold ({self.compression_threshold_mb} MB)")
            
            # Second check: Delete oldest subdirectories if still over max storage
            if current_size_mb > self.max_storage_mb:
                self.logger.info(f"Size exceeds maximum threshold ({self.max_storage_mb} MB), cleaning up oldest subdirectories")
                self.logger.debug(f"Size before cleanup: {current_size_mb:.2f} MB")
                self._cleanup_oldest_subdirectories(current_size_mb)
                
                # Log final size
                final_size_mb = self._get_directory_size_mb(self.processed_dir)
                self.logger.info(f"Final size after cleanup: {final_size_mb:.2f} MB")
                self.logger.debug(f"Cleanup reduced size by {(current_size_mb - final_size_mb):.2f} MB")
            else:
                self.logger.debug(f"Size ({current_size_mb:.2f} MB) is below maximum threshold ({self.max_storage_mb} MB)")
            
            self.logger.debug("Storage maintenance check completed successfully")
            return True
        except Exception as e:
            self.logger.error(f"Error during storage check: {str(e)}")
            self.logger.debug(f"Storage check exception details: {type(e).__name__} - {str(e)}")
            return False
    
    def _get_directory_size_mb(self, directory):
        """
        Calculate the total size of a directory in megabytes
        
        Args:
            directory (str): Path to directory
            
        Returns:
            float: Size in megabytes
        """
        total_size = 0
        for dirpath, _, filenames in os.walk(directory):
            for f in filenames:
                fp = os.path.join(dirpath, f)
                if not os.path.islink(fp):  # Skip symbolic links
                    total_size += os.path.getsize(fp)
        
        return total_size / (1024 * 1024)  # Convert to MB
    
    def _get_subdirectories_info(self):
        """
        Get information about subdirectories including timestamps and compression status
        
        Returns:
            list: List of dictionaries with subdirectory information
        """
        subdirs = []
        
        if not os.path.exists(self.processed_dir):
            return subdirs
            
        for item in os.listdir(self.processed_dir):
            subdir_path = os.path.join(self.processed_dir, item)
            if os.path.isdir(subdir_path):
                try:
                    # Parse timestamps from directory name (expects format earliest_latest)
                    timestamps = item.split('_')
                    if len(timestamps) >= 2:
                        # Use earliest timestamp for sorting
                        timestamp = int(timestamps[0])
                    else:
                        # Fallback to directory creation time
                        timestamp = os.path.getctime(subdir_path)
                    
                    # Check if directory contains any uncompressed contents
                    contains_uncompressed = any(
                        not f.endswith('.tgz') 
                        for f in os.listdir(subdir_path) 
                        if os.path.isfile(os.path.join(subdir_path, f))
                    )
                    
                    size_mb = self._get_directory_size_mb(subdir_path)
                    
                    subdirs.append({
                        'path': subdir_path,
                        'name': item,
                        'timestamp': timestamp,
                        'contains_uncompressed': contains_uncompressed,
                        'size_mb': size_mb
                    })
                except Exception as e:
                    self.logger.warning(f"Error processing subdirectory {item}: {str(e)}")
        
        # Sort by timestamp (oldest first)
        subdirs.sort(key=lambda x: x['timestamp'])
        
        return subdirs
    
    def _compress_subdirectories(self):
        """
        Compress uncompressed files in subdirectories except the 2 newest ones
        """
        subdirs = self._get_subdirectories_info()
        
        # Skip the two newest directories
        dirs_to_compress = subdirs[:-2] if len(subdirs) > 2 else []
        
        compressed_count = 0
        for subdir in dirs_to_compress:
            if subdir['contains_uncompressed']:
                try:
                    self._compress_directory(subdir['path'])
                    compressed_count += 1
                except Exception as e:
                    self.logger.error(f"Error compressing directory {subdir['path']}: {str(e)}")
        
        self.logger.info(f"Compressed {compressed_count} subdirectories")
    
    def _compress_directory(self, directory):
        """
        Compress all uncompressed files in a directory
        
        Args:
            directory (str): Path to directory
        """
        # Find all files that are not already compressed
        uncompressed_files = [
            f for f in os.listdir(directory) 
            if os.path.isfile(os.path.join(directory, f)) and not f.endswith('.tgz')
        ]
        
        if not uncompressed_files:
            return
            
        self.logger.info(f"Compressing {len(uncompressed_files)} files in {directory}")
        
        for file in uncompressed_files:
            file_path = os.path.join(directory, file)
            tar_path = file_path + '.tgz'
            
            # Create tar.gz file
            with tarfile.open(tar_path, "w:gz") as tar:
                tar.add(file_path, arcname=os.path.basename(file_path))
            
            # Remove original file
            os.remove(file_path)
            self.logger.debug(f"Compressed {file_path} to {tar_path}")
    
    def _cleanup_oldest_subdirectories(self, current_size_mb):
        """
        Delete oldest subdirectories until size is below max_storage_mb
        
        Args:
            current_size_mb (float): Current size of processed_csv directory in MB
        """
        subdirs = self._get_subdirectories_info()
        
        # Target size to reach after deletion
        target_size_mb = self.max_storage_mb
        
        # Keep track of deleted directories and removed size
        deleted_count = 0
        removed_size_mb = 0
        
        # Delete oldest directories until we're under the limit
        for subdir in subdirs:
            if current_size_mb - removed_size_mb <= target_size_mb:
                break
                
            try:
                dir_size_mb = subdir['size_mb']
                self.logger.info(f"Deleting directory: {subdir['path']} ({dir_size_mb:.2f} MB)")
                
                # Remove the directory
                shutil.rmtree(subdir['path'])
                
                # Update tracking variables
                removed_size_mb += dir_size_mb
                deleted_count += 1
            except Exception as e:
                self.logger.error(f"Error deleting directory {subdir['path']}: {str(e)}")
        
        self.logger.info(f"Deleted {deleted_count} oldest subdirectories (removed approximately {removed_size_mb:.2f} MB)")