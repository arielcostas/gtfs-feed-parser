#!/usr/bin/env python3

"""Test script to verify next-day trip handling."""

import os
import sys
sys.path.append('.')

from src.orchestrators import process_stop_date

def test_nextday_logic():
    """Test the next-day logic on a single date."""
    feed_dir = "feed"
    test_date = "2025-08-02"  # Pick a date that might have next-day trips
    numeric_stop_code = False
    
    print(f"Testing next-day logic for date: {test_date}")
    
    # Process the date
    args = (feed_dir, test_date, numeric_stop_code)
    date, stop_arrivals = process_stop_date(args)
    
    print(f"Processed {len(stop_arrivals)} stops for {date}")
    
    # Look for any early morning trips that might be next-day trips
    early_morning_trips = []
    for stop_code, arrivals in stop_arrivals.items():
        for arrival in arrivals:
            arrival_time = arrival['arrival_time']
            # Check for trips between 00:00 and 06:00 (likely next-day trips)
            if arrival_time and arrival_time.startswith(('00:', '01:', '02:', '03:', '04:', '05:')):
                early_morning_trips.append({
                    'stop_code': stop_code,
                    'time': arrival_time,
                    'trip_id': arrival['trip']['id'],
                    'service_id': arrival['trip']['service_id']
                })
    
    print(f"Found {len(early_morning_trips)} early morning trips (potential next-day trips)")
    
    # Show a few examples
    for i, trip in enumerate(early_morning_trips[:5]):
        print(f"  {i+1}. Stop {trip['stop_code']} at {trip['time']} - Trip: {trip['trip_id']}")
    
    if len(early_morning_trips) > 5:
        print(f"  ... and {len(early_morning_trips) - 5} more")

if __name__ == "__main__":
    test_nextday_logic()
