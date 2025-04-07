"""
Statistics tracking module
"""

class StatsTracker:
    """
    Tracks API call statistics and displays them
    """
    
    def __init__(self):
        """Initialize statistics counters"""
        self.stats = {
            'search_api_calls': {'success': 0, 'failure': 0},
            'delete_api_calls': {'success': 0, 'failure': 0}
        }
    
    def increment_search_success(self):
        """Increment successful search API calls counter"""
        self.stats['search_api_calls']['success'] += 1
        self.print_stats()
    
    def increment_search_failure(self):
        """Increment failed search API calls counter"""
        self.stats['search_api_calls']['failure'] += 1
        self.print_stats()
    
    def increment_delete_success(self):
        """Increment successful delete API calls counter"""
        self.stats['delete_api_calls']['success'] += 1
        self.print_stats()
    
    def increment_delete_failure(self):
        """Increment failed delete API calls counter"""
        self.stats['delete_api_calls']['failure'] += 1
        self.print_stats()
    
    def print_stats(self):
        """Print statistics to terminal"""
        print("\033[H\033[J")  # Clear screen
        print("=== Splunk Duplicate Remover Statistics ===")
        print(f"Search API Calls: Success: {self.stats['search_api_calls']['success']} | Failures: {self.stats['search_api_calls']['failure']}")
        print(f"Delete API Calls: Success: {self.stats['delete_api_calls']['success']} | Failures: {self.stats['delete_api_calls']['failure']}")
        print("=========================================")