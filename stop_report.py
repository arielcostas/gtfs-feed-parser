import os
import shutil
import sys
import traceback
import argparse
import json
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
import multiprocessing
from multiprocessing import Pool, cpu_count

from src.download import download_feed_from_url
from src.logger import get_logger
from src.common import get_all_feed_dates, date_range, time_to_seconds
from src.stops import get_all_stops
from src.services import get_active_services
from src.street_name import get_street_name
from src.trips import get_trips_for_services
from src.stop_times import get_stops_for_trips
from src.routes import load_routes

logger = get_logger("stop_report")


def parse_args():
    parser = argparse.ArgumentParser(
        description="Generate stop-based JSON reports for a date or date range.")
    parser.add_argument('--start-date', type=str,
                        help='Start date (YYYY-MM-DD)')
    parser.add_argument('--end-date', type=str,
                        help='End date (YYYY-MM-DD, inclusive)')
    parser.add_argument('--all-dates', action='store_true',
                        help='Process all dates in the feed')
    parser.add_argument('--output-dir', type=str, default="./output/",
                        help='Directory to write reports to (default: ./output/)')
    parser.add_argument('--feed-dir', type=str,
                        help="Path to the feed directory")
    parser.add_argument('--feed-url', type=str,
                        help="URL to download the GTFS feed from (if not using local feed directory)")
    parser.add_argument('--numeric-stop-code', action='store_true',
                        help="Strip non-numeric characters from stop codes (e.g., 'P001400' becomes '1400')")
    parser.add_argument('--pretty', action='store_true',
                        help="Pretty-print JSON output (default is compact JSON without spaces)")
    parser.add_argument('--jobs', type=int, default=0,
                        help="Number of parallel processes to use (default: 0). Set to 0 for automatic detection.")
    parser.add_argument('--force-download', action='store_true', 
                        help="Force download even if the feed hasn't been modified (only applies when using --feed-url)")
    args = parser.parse_args()

    if not args.all_dates and not args.start_date:
        parser.error(
            '--start-date is required unless --all-dates is specified')
    if not args.all_dates and not args.end_date:
        parser.error('--end-date is required unless --all-dates is specified')
    if args.feed_dir and args.feed_url:
        parser.error("Specify either --feed-dir or --feed-url, not both.")
    if not args.feed_dir and not args.feed_url:
        parser.error(
            "You must specify either a path to the existing feed (unzipped) or a URL to download the GTFS feed from.")
    if args.feed_dir and not os.path.exists(args.feed_dir):
        parser.error(f"Feed directory does not exist: {args.feed_dir}")
    return args


def time_to_seconds(time_str: str) -> int:
    """Convert HH:MM:SS to seconds since midnight."""
    parts = time_str.split(':')
    if len(parts) != 3:
        return 0

def get_stop_arrivals(
    feed_dir: str,
        date: str,
        numeric_stop_code: bool = False
) -> Dict[str, List[Dict[str, Any]]]:
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

    logger.info(
        f"Found {len(active_services)} active services for date {date}.")

    trips = get_trips_for_services(feed_dir, active_services)
    total_trip_count = sum(len(trip_list) for trip_list in trips.values())
    logger.info(f"Found {total_trip_count} trips for active services.")

    # Get all trip IDs
    all_trip_ids = [trip.trip_id for trip_list in trips.values()
                    for trip in trip_list]

    # Get stops for all trips
    stops_for_all_trips = get_stops_for_trips(feed_dir, all_trip_ids)
    logger.info(f"Precomputed stops for {len(stops_for_all_trips)} trips.")

    # Load routes information
    routes = load_routes(feed_dir)
    logger.info(f"Loaded {len(routes)} routes from feed.")

    # Create a reverse lookup from stop_id to stop_code
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

                next_stop_names = []
                if i < len(trip_stops) - 1:
                    for next_stop_time in trip_stops[i+1:]:
                        next_stop = stops.get(next_stop_time.stop_id)
                        next_stop_names.append(next_stop.stop_name if next_stop else "N/A")

                next_streets = []
                _street_cursor = None
                for stop_name in next_stop_names:
                    street_name = get_street_name(stop_name)
                    if _street_cursor != street_name:
                        next_streets.append(street_name)
                        _street_cursor = street_name

                # Convert times to seconds for sorting
                arrival_seconds = time_to_seconds(stop_time.arrival_time)
                stop_arrivals[stop_code].append({
                    "line": {
                        "name": route_short_name,
                        "colour": f"#{route_color}" if route_color else "#FFFFFF",
                    },
                    "trip": {
                        "id": trip.trip_id,
                        "service_id": service_id,
                        "headsign": trip.headsign,
                        "direction_id": "OUTBOUND" if trip.direction_id == 0 else "INBOUND" if trip.direction_id == 1 else "UNKNOWN",
                    },
                    "route_id": trip.route_id,
                    "departure_time": stop_time.departure_time,
                    "arrival_time": stop_time.arrival_time,
                    "stop_sequence": stop_time.stop_sequence,
                    "shape_dist_traveled": stop_time.shape_dist_traveled,
                    "next_streets": next_streets,
                    "arrival_seconds": arrival_seconds,
                })

    # Sort each stop's arrivals by arrival time
    for stop_code in stop_arrivals:
        # Filter out entries with None arrival_seconds
        stop_arrivals[stop_code] = [item for item in stop_arrivals[stop_code] if item["arrival_seconds"] is not None]
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
        json.dump(arrivals, f, indent=2 if pretty else None, separators=(
            ",", ":") if not pretty else None, ensure_ascii=False)


def write_index_json(output_dir: str, stops_summary: Dict[str, Dict[str, int]], pretty: bool):
    """
    Write index JSON files with stop counts.

    Args:
        stops_summary: Dictionary mapping dates to dictionaries of stop_code: count
    """
    # Create the stops directory
    stops_dir = os.path.join(output_dir, "stops")
    os.makedirs(stops_dir, exist_ok=True)


def process_date(
    feed_dir: str,
    date: str,
    output_dir: str,
    numeric_stop_code: bool,
    pretty: bool
) -> tuple[str, Dict[str, int]]:
    """
    Process a single date and write its stop JSON files.
    Returns summary data for index generation.
    """
    try:
        logger = get_logger(f"stop_report_{date}")
        logger.info(f"Starting stop report generation for date {date}")

        # Get all stop arrivals for the current date
        stop_arrivals = get_stop_arrivals(
            feed_dir, date, numeric_stop_code
        )

        if not stop_arrivals:
            logger.warning(f"No stop arrivals found for date {date}")
            return date, {}

        # Write individual stop JSON files
        for stop_code, arrivals in stop_arrivals.items():
            write_stop_json(output_dir, date, stop_code, arrivals, pretty)

        # Create summary for index
        stop_summary = {stop_code: len(arrivals)
                        for stop_code, arrivals in stop_arrivals.items()}
        logger.info(f"Processed {len(stop_arrivals)} stops for date {date}")

        return date, stop_summary
    except Exception as e:
        logger.error(f"Error processing date {date}: {e}")
        raise


def main():
    args = parse_args()
    output_dir = args.output_dir
    feed_url = args.feed_url
    numeric_stop_code = args.numeric_stop_code
    pretty = args.pretty
    jobs = args.jobs

    if not feed_url:
        feed_dir = args.feed_dir
    else:
        logger.info(f"Downloading GTFS feed from {feed_url}...")
        feed_dir = download_feed_from_url(feed_url, output_dir, args.force_download)
        if feed_dir is None:
            logger.info("Download was skipped (feed not modified). Exiting.")
            return

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

    # Ensure date_list is not empty before processing
    if not date_list:
        logger.error("No valid dates to process.")
        return

    # Determine number of jobs for parallel processing
    if jobs <= 0:
        jobs = cpu_count()
    logger.info(f"Processing {len(date_list)} dates with {jobs} jobs")

    # Dictionary to store summary data for index files
    all_stops_summary = {}

    if jobs > 1 and len(date_list) > 1:
        # Parallel processing
        try:
            with Pool(processes=jobs) as pool:
                tasks = [
                    (feed_dir, date, output_dir,
                     numeric_stop_code, pretty)
                    for date in date_list
                ]
                results = pool.starmap(process_date, tasks)

                for date, stop_summary in results:
                    all_stops_summary[date] = stop_summary
        except Exception as e:
            logger.error(f"Error in parallel processing: {e}")
            # Fallback to sequential processing
            for date in date_list:
                _, stop_summary = process_date(
                    feed_dir, date, output_dir, numeric_stop_code, pretty)
                all_stops_summary[date] = stop_summary
    else:
        # Sequential processing
        for date in date_list:
            _, stop_summary = process_date(
                feed_dir, date, output_dir, numeric_stop_code, pretty)
            all_stops_summary[date] = stop_summary

    # Write index files
    write_index_json(output_dir, all_stops_summary, pretty)

    logger.info("Stop report generation completed.")

    if feed_url:
        if os.path.exists(feed_dir):
            shutil.rmtree(feed_dir)
            logger.info(f"Removed temporary feed directory: {feed_dir}")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger = get_logger("stop_report")
        logger.critical(f"An unexpected error occurred: {e}", exc_info=True)
        traceback.print_exc()
        sys.exit(1)
