"""
Module checker for Splunk Duplicate Remover

Verifies that all required Python packages are installed
and offers to install missing dependencies
"""

import importlib
import subprocess
import sys

def check_modules():
    """
    Check that all required modules are installed
    Offer to install them if they're missing
    """
    required_modules = [
        'requests',           # HTTP requests for Splunk API
        'urllib3',            # Used for disabling SSL warnings
        'configparser',       # For configuration handling
        'concurrent.futures', # For parallelization of searches
        'argparse',           # For command-line arguments
        'tarfile',            # For compressing processed CSV files
        'csv',                # For CSV processing
        'datetime',           # For time window calculations
        'logging',            # For logging functionality
        'os',                 # For file operations
        'sys',                # For system interactions
        'time',               # For sleep and timing functions
        'shutil'              # For directory operations
    ]
    
    missing_modules = []
    
    for module in required_modules:
        try:
            importlib.import_module(module)
        except ImportError:
            missing_modules.append(module)
    
    if missing_modules:
        print("The following required modules are not installed:")
        for module in missing_modules:
            print(f"  - {module}")
        
        decision = input("\nDo you want to install all missing modules? (y/n): ")
        if decision.lower() == "y":
            print("Installing missing modules...")
            for module in missing_modules:
                try:
                    print(f"Installing {module}...")
                    subprocess.check_call([sys.executable, '-m', 'pip', 'install', module])
                    print(f"{module} has been successfully installed.")
                except Exception as e:
                    print(f"Failed to install {module}: {e}")
                    print("Exiting the script")
                    sys.exit(1)
        else:
            print("Required modules are missing. Exiting the script.")
            sys.exit(1)
    
    print("All required modules are installed.")
    return True

if __name__ == "__main__":
    check_modules()