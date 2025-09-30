"""
Debug script to test rolling dates in stop reports.
This script helps diagnose why stop JSON files aren't being generated for rolling dates.
"""
import os
import json
import tempfile
from datetime import datetime

# Mock the process flow
def test_rolling_stops_generation():
    """Test that rolling dates generate stop JSON files"""
    
    # Simulate the rolling config
    rolling_mappings = {
        "2025-09-30": "2025-09-24",
        "2025-10-01": "2025-09-25",
        "2025-10-02": "2025-09-26"
    }
    
    # Simulate initial date_list (user requested 2025-09-24 to 2025-09-26)
    initial_date_list = ["2025-09-24", "2025-09-25", "2025-09-26"]
    
    # Simulate expansion (what orchestrator does now)
    rolling_dates = list(rolling_mappings.keys())
    expanded_date_list = sorted(set(initial_date_list + rolling_dates))
    
    print(f"Initial date list: {initial_date_list}")
    print(f"Rolling dates from config: {rolling_dates}")
    print(f"Expanded date list: {expanded_date_list}")
    print()
    
    # Simulate grouping (what orchestrator does)
    source_to_targets = {}
    normal_dates = []
    
    for date in expanded_date_list:
        source_date = rolling_mappings.get(date)
        if source_date:
            if source_date not in source_to_targets:
                source_to_targets[source_date] = []
            source_to_targets[source_date].append(date)
        else:
            normal_dates.append(date)
    
    print(f"Normal dates: {normal_dates}")
    print(f"Source to targets mapping: {source_to_targets}")
    print()
    
    # Simulate process_args building
    process_args = []
    
    for date in normal_dates:
        process_args.append((date, None))  # (target_date, source_date_or_none)
    
    for source_date, target_dates in source_to_targets.items():
        for target_date in target_dates:
            process_args.append((target_date, source_date))
    
    print("Process args (target_date, source_date):")
    for args in process_args:
        print(f"  {args}")
    print()
    
    # Simulate process_stop_date return values
    print("Simulated process_stop_date returns:")
    for target_date, source_date in process_args:
        # Mock some stop arrivals data
        stop_arrivals = {
            "1234": [{"line": {"name": "L1"}, "arrival_time": "08:00:00"}]
        }
        
        is_rolling = source_date is not None
        status = f"(rolling from {source_date})" if is_rolling else "(normal)"
        print(f"  Returns: ('{target_date}', {len(stop_arrivals)} stops) {status}")
        
        # This is what should create the directory
        output_dir = tempfile.gettempdir()
        date_dir = os.path.join(output_dir, "stops", target_date)
        print(f"    Would create: {date_dir}")
    print()
    
    # Summary
    print(f"Total dates to process: {len(process_args)}")
    print(f"Expected stop directories: {len(expanded_date_list)}")
    print(f"  - Normal dates: {len(normal_dates)}")
    print(f"  - Rolling dates: {sum(len(targets) for targets in source_to_targets.values())}")

if __name__ == "__main__":
    test_rolling_stops_generation()
