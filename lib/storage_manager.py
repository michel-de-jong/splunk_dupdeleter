"""
Storage management module for maintaining processed_csv directory size
with thread-safe operations
"""

import os
import tarfile
import shutil
import logging
import fcntl
import time
import errno
from datetime import datetime

class StorageManager:
    """
    Manages the size of processed_csv directory with compression and cleanup
    Thread-safe implementation using file locks
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
        
        # Path for the lock file
        self.lock_file = os.path.join(os.path.dirname(self.processed_dir), '.storage_manager.lock')
        
        # Lock timeout in seconds
        self.lock_timeout = float(config.get('storage', 'lock_timeout', fallback='300'))  # 5 minutes default
    
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
    
    def _acquire_lock(self):
        """
        Acquire a lock file to ensure thread-safe operation
        
        Returns:
            file object or None: Lock file object if successful, None otherwise
        """
        # Make sure the parent directory exists
        lock_dir = os.path.dirname(self.lock_file)
        if lock_dir and not os.path.exists(lock_dir):
            os.makedirs(lock_dir, exist_ok=True)
            
        try:
            # Open the lock file
            lock_fd = open(self.lock_file, 'w')
            
            # Try to acquire an exclusive lock, non-blocking
            fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            
            # Write PID and timestamp to the lock file
            lock_fd.write(f"PID: {os.getpid()}\nTimestamp: {datetime.now().isoformat()}\n")
            lock_fd.flush()
            
            self.logger.debug(f"Lock acquired by PID {os.getpid()}")
            return lock_fd
            
        except IOError as e:
            # Check if it's a "resource temporarily unavailable" error (meaning the file is locked)
            if e.errno == errno.EAGAIN:
                self.logger.debug("Lock already held by another process")
                # Attempt to read the lock file to see who has it
                try:
                    with open(self.lock_file, 'r') as f:
                        lock_info = f.read()
                        self.logger.debug(f"Current lock info: {lock_info}")
                except:
                    pass
            else:
                self.logger.error(f"Error acquiring lock: {str(e)}")
                
            # Check if lock file is stale (older than lock_timeout)
            try:
                if os.path.exists(self.lock_file):
                    file_age = time.time() - os.path.getmtime(self.lock_file)
                    if file_age > self.lock_timeout:
                        self.logger.warning(f"Found stale lock file (age: {file_age:.1f}s). Breaking lock.")
                        os.remove(self.lock_file)
                        # Try again after breaking the lock
                        return self._acquire_lock()
            except Exception as ex:
                self.logger.error(f"Error checking lock file age: {str(ex)}")
                
            return None
            
        except Exception as e:
            self.logger.error(f"Unexpected error acquiring lock: {str(e)}")
            return None
    
    def _release_lock(self, lock_fd):
        """
        Release the lock file
        
        Args:
            lock_fd (file object): Lock file descriptor
        """
        if lock_fd:
            try:
                fcntl.flock(lock_fd, fcntl.LOCK_UN)
                lock_fd.close()
                self.logger.debug(f"Lock released by PID {os.getpid()}")
            except Exception as e:
                self.logger.error(f"Error releasing lock: {str(e)}")
    
    def check_storage(self):
        """
        Check storage size and perform maintenance if needed
        
        Returns:
            bool: True if operations were successful
        """
        # Acquire lock for thread safety
        lock_fd = self._acquire_lock()
        if not lock_fd:
            self.logger.info("Could not acquire lock - skipping storage maintenance")
            return False
            
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
            else:
                self.logger.debug(f"Size ({current_size_mb:.2f} MB) is below compression threshold ({self.compression_threshold_mb} MB)")
            
            # Second check: Delete oldest subdirectories if still over max storage
            if current_size_mb > self.max_storage_mb:
                self.logger.info(f"Size exceeds maximum threshold ({self.max_storage_mb} MB), cleaning up oldest items")
                self.logger.debug(f"Size before cleanup: {current_size_mb:.2f} MB")
                self._cleanup_oldest_items(current_size_mb)
                
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
        finally:
            # Always release the lock, even if an error occurred
            self._release_lock(lock_fd)
    
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
    
    def _get_items_info(self):
        """
        Get information about all items (directories and compressed archives) in the processed directory
        with their timestamps and sizes
        
        Returns:
            list: List of dictionaries with item information
        """
        items = []
        
        if not os.path.exists(self.processed_dir):
            return items
            
        for item_name in os.listdir(self.processed_dir):
            item_path = os.path.join(self.processed_dir, item_name)
            
            try:
                # Extract timestamps from name (for both dirs and archives)
                base_name = item_name.split('.')[0] if item_name.endswith('.tgz') else item_name
                timestamps = base_name.split('_')
                
                if len(timestamps) >= 2 and timestamps[0].isdigit() and timestamps[1].isdigit():
                    # Use earliest timestamp for sorting
                    timestamp = int(timestamps[0])
                else:
                    # Fallback to item creation time
                    timestamp = os.path.getctime(item_path)
                
                # Calculate size
                if os.path.isdir(item_path):
                    size_mb = self._get_directory_size_mb(item_path)
                    item_type = 'directory'
                elif os.path.isfile(item_path) and item_name.endswith('.tgz'):
                    size_mb = os.path.getsize(item_path) / (1024 * 1024)  # Convert to MB
                    item_type = 'archive'
                else:
                    # Skip other file types
                    continue
                
                items.append({
                    'path': item_path,
                    'name': item_name,
                    'timestamp': timestamp,
                    'size_mb': size_mb,
                    'type': item_type
                })
            except Exception as e:
                self.logger.warning(f"Error processing item {item_name}: {str(e)}")
        
        # Sort by timestamp (oldest first)
        items.sort(key=lambda x: x['timestamp'])
        
        return items
    
    def _compress_subdirectories(self):
        """
        Compress entire subdirectories as .tgz archives except the 2 newest ones
        """
        items = self._get_items_info()
        
        # Filter only directories (not already compressed)
        dirs = [item for item in items if item['type'] == 'directory']
        
        # Skip the two newest directories
        dirs_to_compress = dirs[:-2] if len(dirs) > 2 else []
        
        compressed_count = 0
        for dir_item in dirs_to_compress:
            try:
                dir_path = dir_item['path']
                dir_name = dir_item['name']
                tar_path = os.path.join(self.processed_dir, f"{dir_name}.tgz")
                
                # Skip if target archive already exists (could happen with concurrent operations)
                if os.path.exists(tar_path):
                    self.logger.warning(f"Target archive already exists: {tar_path}, skipping compression")
                    continue
                
                self.logger.info(f"Compressing entire directory: {dir_path} to {tar_path}")
                
                # Create tar.gz file of the entire directory
                with tarfile.open(tar_path, "w:gz") as tar:
                    tar.add(dir_path, arcname=dir_name)
                
                # Verify the archive was created successfully
                if os.path.exists(tar_path):
                    # Remove original directory after successful compression
                    shutil.rmtree(dir_path)
                    compressed_count += 1
                else:
                    self.logger.error(f"Failed to create archive: {tar_path}")
                
            except Exception as e:
                self.logger.error(f"Error compressing directory {dir_item['path']}: {str(e)}")
        
        self.logger.info(f"Compressed {compressed_count} subdirectories")
    
    def _cleanup_oldest_items(self, current_size_mb):
        """
        Delete oldest items (compressed archives first, then directories if needed)
        until size is below max_storage_mb
        
        Args:
            current_size_mb (float): Current size of processed_csv directory in MB
        """
        items = self._get_items_info()
        
        # Target size to reach after deletion
        target_size_mb = self.max_storage_mb * 0.9  # Aim for 90% of max to avoid frequent cleanups
        
        # Keep track of deleted items and removed size
        deleted_count = 0
        removed_size_mb = 0
        
        # Delete oldest items until we're under the limit
        for item in items:
            # Always keep at least one item (the newest)
            if len(items) - deleted_count <= 1:
                self.logger.info("Keeping the newest item regardless of size constraints")
                break
                
            # Stop if we're below target size
            if current_size_mb - removed_size_mb <= target_size_mb:
                break
                
            try:
                item_size_mb = item['size_mb']
                item_path = item['path']
                
                # Skip if item no longer exists (could have been deleted by another process)
                if not os.path.exists(item_path):
                    self.logger.warning(f"Item no longer exists: {item_path}, skipping")
                    continue
                
                self.logger.info(f"Deleting {item['type']}: {item_path} ({item_size_mb:.2f} MB)")
                
                # Remove the item (directory or archive)
                if item['type'] == 'directory':
                    shutil.rmtree(item_path)
                else:  # archive
                    os.remove(item_path)
                
                # Update tracking variables
                removed_size_mb += item_size_mb
                deleted_count += 1
            except Exception as e:
                self.logger.error(f"Error deleting {item['type']} {item['path']}: {str(e)}")
        
        self.logger.info(f"Deleted {deleted_count} oldest items (removed approximately {removed_size_mb:.2f} MB)")