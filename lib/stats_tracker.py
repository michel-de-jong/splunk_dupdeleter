"""
Statistics tracking module with dynamic terminal display
"""

import sys

class StatsTracker:
    """
    Tracks API call statistics and displays them dynamically
    """
    
    def __init__(self):
        """Initialize statistics counters and setup display"""
        self.stats = {
            'search_api_calls': {'success': 0, 'failure': 0},
            'delete_api_calls': {'success': 0, 'failure': 0}
        }
        self.display_initialized = False
    
    def increment_search_success(self):
        """Increment successful search API calls counter"""
        self.stats['search_api_calls']['success'] += 1
        self.update_search_display()
    
    def increment_search_failure(self):
        """Increment failed search API calls counter"""
        self.stats['search_api_calls']['failure'] += 1
        self.update_search_display()
    
    def increment_delete_success(self):
        """Increment successful delete API calls counter"""
        self.stats['delete_api_calls']['success'] += 1
        self.update_delete_display()
    
    def increment_delete_failure(self):
        """Increment failed delete API calls counter"""
        self.stats['delete_api_calls']['failure'] += 1
        self.update_delete_display()
    
    def initialize_display(self):
        """Initialize the stats display area"""
        if not self.display_initialized:
            print("=== Splunk Duplicate Remover Statistics ===")
            self.display_initialized = True
            self.update_search_display()
            self.update_delete_display()
    
    def update_search_display(self):
        """Update search statistics"""
        try:
            if not self.display_initialized:
                self.initialize_display()
            
            # Print all stats on the same line with carriage return
            print(f"\rSearch API Calls: Success: {self.stats['search_api_calls']['success']} | Failures: {self.stats['search_api_calls']['failure']}")
            #sys.stdout.flush()
        except Exception:
            # Fallback to non-dynamic display if terminal doesn't support ANSI codes
            pass
    
    def update_delete_display(self):
        """Update delete statistics"""
        try:
            if not self.display_initialized:
                self.initialize_display()
            
            # Print all stats on the same line with carriage return
            print(f"\rDelete API Calls: Success: {self.stats['delete_api_calls']['success']} | Failures: {self.stats['delete_api_calls']['failure']}")
            #sys.stdout.flush()
        except Exception:
            # Fallback to non-dynamic display if terminal doesn't support ANSI codes
            pass