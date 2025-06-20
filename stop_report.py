"""
Script for generating stop-based JSON reports from GTFS data.
"""
import os
import shutil
import sys
import traceback
import argparse
import json
from datetime import datetime, timedelta
from typing import List, Dict, Any

from src.download import download_feed_from_url
from src.logger import get_logger
from src.stops import get_all_stops
from src.services import get_active_services
from src.trips import get_trips_for_services
from src.stop_times import get_stops_for_trips
from src.routes import load_routes

logger = get_logger("stop_report")


def date_range(start: str, end: str):
    start_dt = datetime.strptime(start, "%Y-%m-%d")
    end_dt = datetime.strptime(end, "%Y-%m-%d")
    while start_dt <= end_dt:
        yield start_dt.strftime("%Y-%m-%d")
        start_dt += timedelta(days=1)


def get_all_feed_dates(feed_dir: str) -> List[str]:
    """
    Returns all dates the feed is valid for, using calendar.txt if present, else calendar_dates.txt.
    """
    import csv
    import os
    calendar_path = os.path.join(feed_dir, 'calendar.txt')
    calendar_dates_path = os.path.join(feed_dir, 'calendar_dates.txt')
    # Try calendar.txt first
    if os.path.exists(calendar_path):
        with open(calendar_path, encoding='utf-8') as f:
            reader = csv.DictReader(f)
            start_dates: List[str] = []
            end_dates: List[str] = []
            for row in reader:
                if row.get('start_date') and row.get('end_date'):
                    start_dates.append(row['start_date'])
                    end_dates.append(row['end_date'])
            if start_dates and end_dates:
                min_date = min(start_dates)
                max_date = max(end_dates)
                # Convert YYYYMMDD to YYYY-MM-DD
                start = datetime.strptime(min_date, '%Y%m%d')
                end = datetime.strptime(max_date, '%Y%m%d')
                result: List[str] = []
                while start <= end:
                    result.append(start.strftime('%Y-%m-%d'))
                    start += timedelta(days=1)
                return result
    # Fallback: use calendar_dates.txt
    if os.path.exists(calendar_dates_path):
        with open(calendar_dates_path, encoding='utf-8') as f:
            reader = csv.DictReader(f)
            dates: set[str] = set()
            for row in reader:
                if row.get('exception_type') == '1' and row.get('date'):
                    # Convert YYYYMMDD to YYYY-MM-DD
                    d = row['date']
                    dates.add(f"{d[:4]}-{d[4:6]}-{d[6:]}")
            return sorted(dates)
    return []


def parse_args():
    parser = argparse.ArgumentParser(
        description="Generate stop-based JSON reports for a date or date range.")
    parser.add_argument('--start-date', type=str,
                        help='Start date (YYYY-MM-DD)')
    parser.add_argument('--end-date', type=str,
                        help='End date (YYYY-MM-DD, inclusive)')
    parser.add_argument('--all-dates', action='store_true', help='Process all dates in the feed')
    parser.add_argument('--output-dir', type=str, default="./output/", help='Directory to write reports to (default: ./output/)')
    parser.add_argument('--feed-dir', type=str, help="Path to the feed directory")
    parser.add_argument('--feed-url', type=str, help="URL to download the GTFS feed from (if not using local feed directory)")
    parser.add_argument('--numeric-stop-code', action='store_true', 
                        help="Strip non-numeric characters from stop codes (e.g., 'P001400' becomes '1400')")
    parser.add_argument('--pretty', action='store_true', 
                        help="Pretty-print JSON output (default is compact JSON without spaces)")
    args = parser.parse_args()

    if not args.all_dates and not args.start_date:
        parser.error('--start-date is required unless --all-dates is specified')
    if not args.all_dates and not args.end_date:
        parser.error('--end-date is required unless --all-dates is specified')
    if args.feed_dir and args.feed_url:
        parser.error("Specify either --feed-dir or --feed-url, not both.")
    if not args.feed_dir and not args.feed_url:
        parser.error("You must specify either a path to the existing feed (unzipped) or a URL to download the GTFS feed from.")
    if args.feed_dir and not os.path.exists(args.feed_dir):
        parser.error(f"Feed directory does not exist: {args.feed_dir}")
    return args


def time_to_seconds(time_str: str) -> int:
    """Convert HH:MM:SS to seconds since midnight."""
    parts = time_str.split(':')
    if len(parts) != 3:
        return 0
    hours, minutes, seconds = map(int, parts)
    return hours * 3600 + minutes * 60 + seconds


def get_stop_arrivals(feed_dir: str, date: str, numeric_stop_code: bool = False) -> Dict[str, List[Dict[str, Any]]]:
    """
    Process trips for the given date and organize stop arrivals.
    
    Args:
        feed_dir: Path to the GTFS feed directory
        date: Date in YYYY-MM-DD format
        numeric_stop_code: If True, strip non-numeric characters from stop codes
        
    Returns:
        Dictionary mapping stop_code to lists of arrival information.
    """
    stops = get_all_stops(feed_dir)
    logger.info(f"Found {len(stops)} stops in the feed.")
    
    active_services = get_active_services(feed_dir, date)
    if not active_services:
        logger.info("No active services found for the given date.")
        return {}
    
    logger.info(f"Found {len(active_services)} active services for date {date}.")
    
    trips = get_trips_for_services(feed_dir, active_services)
    total_trip_count = sum(len(trip_list) for trip_list in trips.values())
    logger.info(f"Found {total_trip_count} trips for active services.")
    
    # Get all trip IDs
    all_trip_ids = [trip.trip_id for trip_list in trips.values() for trip in trip_list]
    
    # Get stops for all trips
    stops_for_all_trips = get_stops_for_trips(feed_dir, all_trip_ids)
    logger.info(f"Precomputed stops for {len(stops_for_all_trips)} trips.")
    
    # Load routes information
    routes = load_routes(feed_dir)
    logger.info(f"Loaded {len(routes)} routes from feed.")    # Create a reverse lookup from stop_id to stop_code
    stop_id_to_code = {}
    for stop_id, stop in stops.items():
        if stop.stop_code:
            stop_code = stop.stop_code
            # Apply numeric-only transformation if requested
            if numeric_stop_code:
                # First strip non-numeric characters
                numeric_code = ''.join(c for c in stop_code if c.isdigit())
                # Then convert to integer and back to string to remove leading zeros
                stop_code = str(int(numeric_code)) if numeric_code else ""
            stop_id_to_code[stop_id] = stop_code
    
    # Organize data by stop_code
    stop_arrivals = {}
    
    for service_id, trip_list in trips.items():
        for trip in trip_list:
            # Get route information
            route_info = routes.get(trip.route_id, {})
            route_short_name = route_info.get('route_short_name', '')
            route_color = route_info.get('route_color', '')
              # Get stop times for this trip
            trip_stops = stops_for_all_trips.get(trip.trip_id, [])
            
            for i, stop_time in enumerate(trip_stops):
                stop_id = stop_time.stop_id
                stop_code = stop_id_to_code.get(stop_id)
                
                if not stop_code:
                    continue  # Skip stops without a code
                
                if stop_code not in stop_arrivals:
                    stop_arrivals[stop_code] = []
                
                # Get stop information
                stop = stops.get(stop_id)
                stop_name = stop.stop_name if stop else "Unknown Stop"
                  # Get the next stops in the trip (from current position to the end)
                next_stops = []
                if i < len(trip_stops) - 1:
                    for next_stop_time in trip_stops[i+1:]:
                        next_stop_id = next_stop_time.stop_id
                        next_stop = stops.get(next_stop_id)
                        next_stop_code = stop_id_to_code.get(next_stop_id, "")
                        next_stop_name = next_stop.stop_name if next_stop else "Unknown Stop"
                        
                        next_stops.append({
                            "stop_code": next_stop_code,
                            "stop_name": next_stop_name,
                            "arrival_time": next_stop_time.arrival_time,
                            "departure_time": next_stop_time.departure_time,
                            "stop_sequence": next_stop_time.stop_sequence,
                        })
                  # Convert times to seconds for sorting
                arrival_seconds = time_to_seconds(stop_time.arrival_time)
                stop_arrivals[stop_code].append({
                    "trip_id": trip.trip_id,
                    "route_id": trip.route_id,
                    "line": route_short_name,
                    "color": route_color,
                    "headsign": trip.headsign,
                    "arrival_time": stop_time.arrival_time,
                    "departure_time": stop_time.departure_time,
                    "stop_sequence": stop_time.stop_sequence,
                    "stop_name": stop_name,
                    "service_id": service_id,
                    "shape_dist_traveled": stop_time.shape_dist_traveled,
                    "next_stops": next_stops,
                    "arrival_seconds": arrival_seconds
                })
    
    # Sort each stop's arrivals by arrival time
    for stop_code in stop_arrivals:
        stop_arrivals[stop_code].sort(key=lambda x: x["arrival_seconds"])
        # Remove the temporary sorting key
        for item in stop_arrivals[stop_code]:
            item.pop("arrival_seconds", None)
    
    return stop_arrivals


def write_stop_json(output_dir: str, date: str, stop_code: str, arrivals: List[Dict[str, Any]], pretty: bool):
    """Write the stop arrivals to a JSON file."""
    # Create the stops directory for this date
    date_dir = os.path.join(output_dir, "stops", date)
    os.makedirs(date_dir, exist_ok=True)
    
    # Create the JSON file
    file_path = os.path.join(date_dir, f"{stop_code}.json")
    
    with open(file_path, 'w', encoding='utf-8') as f:
        json.dump(arrivals, f, indent=2 if pretty else None, separators=(",", ":") if not pretty else None, ensure_ascii=False)
    
    logger.debug(f"Written {len(arrivals)} arrivals to {file_path}")


def write_index_json(output_dir: str, stops_data: Dict[str, Dict[str, Any]], pretty: bool):
    """Write index JSON files for easy navigation."""
    # Create the stops directory
    stops_dir = os.path.join(output_dir, "stops")
    os.makedirs(stops_dir, exist_ok=True)
    
    # Write main index with all available dates
    dates = list(stops_data.keys())
    with open(os.path.join(stops_dir, "index.json"), 'w', encoding='utf-8') as f:
        json.dump({"dates": dates}, f, indent=2 if pretty else None, separators=(",", ":") if not pretty else None)
    
    # Write per-date indexes with all stops for that date
    for date, stops in stops_data.items():
        date_dir = os.path.join(stops_dir, date)
        os.makedirs(date_dir, exist_ok=True)
        
        # Create a list of stops with basic info for the index
        stop_list = [{"stop_code": code, "count": len(data)} for code, data in stops.items()]
        
        with open(os.path.join(date_dir, "index.json"), 'w', encoding='utf-8') as f:
            json.dump({"date": date, "stops": stop_list}, f, indent=2 if pretty else None, separators=(",", ":") if not pretty else None)


def main():
    args = parse_args()
    output_dir = args.output_dir
    feed_url = args.feed_url
    numeric_stop_code = args.numeric_stop_code
    pretty = args.pretty
    
    if not feed_url:
        feed_dir = args.feed_dir
    else:
        logger.info(f"Downloading GTFS feed from {feed_url}...")
        feed_dir = download_feed_from_url(feed_url)

    if args.all_dates:
        all_dates = get_all_feed_dates(feed_dir)
        if not all_dates:
            logger.error('No valid dates found in feed.')
            return
        date_list = all_dates
    else:
        start_date = args.start_date
        end_date = args.end_date or args.start_date
        date_list = list(date_range(start_date, end_date))

    # Dictionary to store data for index files
    all_stops_data = {}

    # Ensure date_list is not empty before processing
    if not date_list:
        logger.error("No valid dates to process.")
        return    # Process each date in date_list
    for current_date in date_list:
        logger.info(f"Starting stop report generation for date {current_date}")
        
        # Get all stop arrivals for the current date
        stop_arrivals = get_stop_arrivals(feed_dir, current_date, numeric_stop_code)
        
        if not stop_arrivals:
            logger.warning(f"No stop arrivals found for date {current_date}")
            continue
        
        # Store the data for this date
        all_stops_data[current_date] = {}
        
        # Write individual stop JSON files
        for stop_code, arrivals in stop_arrivals.items():
            write_stop_json(output_dir, current_date, stop_code, arrivals, pretty)
            all_stops_data[current_date][stop_code] = arrivals
        
        logger.info(f"Processed {len(stop_arrivals)} stops for date {current_date}")

    # Write index files
    write_index_json(output_dir, all_stops_data, pretty)
    
    logger.info("Stop report generation completed.")

    if feed_url:
        logger.debug("Cleaning up temporary feed directory...")
        if os.path.exists(feed_dir):
            shutil.rmtree(feed_dir)
            logger.info(f"Removed temporary feed directory: {feed_dir}")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.critical(f"An unexpected error occurred: {e}", exc_info=True)
        traceback.print_exc()
        sys.exit(1)
