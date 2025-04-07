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
        self.initialized_display = False
    
    def increment_search_success(self):
        """Increment successful search API calls counter"""
        self.stats['search_api_calls']['success'] += 1
        self.update_stats_display()
    
    def increment_search_failure(self):
        """Increment failed search API calls counter"""
        self.stats['search_api_calls']['failure'] += 1
        self.update_stats_display()
    
    def increment_delete_success(self):
        """Increment successful delete API calls counter"""
        self.stats['delete_api_calls']['success'] += 1
        self.update_stats_display()
    
    def increment_delete_failure(self):
        """Increment failed delete API calls counter"""
        self.stats['delete_api_calls']['failure'] += 1
        self.update_stats_display()
    
    def initialize_display(self):
        """Initialize the stats display area"""
        if not self.initialized_display:
            # Print the initial stats section with placeholders
            print("=== Splunk Duplicate Remover Statistics ===")
            print(f"Search API Calls: Success: 0 | Failures: 0")
            print(f"Delete API Calls: Success: 0 | Failures: 0")
            print("=========================================")
            # Move cursor back up to the stats section
            sys.stdout.write("\033[3A")  # Move cursor up 3 lines
            sys.stdout.flush()
            self.initialized_display = True
    
    def update_stats_display(self):
        """Update statistics display without clearing the screen"""
        try:
            if not self.initialized_display:
                self.initialize_display()
            
            # Move to the search stats line
            sys.stdout.write("\033[1B")  # Move down 1 line from current position
            # Clear line and write updated search stats
            sys.stdout.write("\r\033[K")  # Clear current line
            sys.stdout.write(f"Search API Calls: Success: {self.stats['search_api_calls']['success']} | Failures: {self.stats['search_api_calls']['failure']}")
            
            # Move to the delete stats line
            sys.stdout.write("\033[1B")  # Move down 1 line from current position
            # Clear line and write updated delete stats
            sys.stdout.write("\r\033[K")  # Clear current line
            sys.stdout.write(f"Delete API Calls: Success: {self.stats['delete_api_calls']['success']} | Failures: {self.stats['delete_api_calls']['failure']}")
            
            # Move back to the top of the stats section
            sys.stdout.write("\033[2A")  # Move cursor up 2 lines
            sys.stdout.flush()
        except Exception:
            # Fallback to non-dynamic display if terminal doesn't support ANSI codes
            pass