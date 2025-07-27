"""
Main script for generating service reports from GTFS data.
"""
import os
import shutil
import sys
import traceback
import argparse
from datetime import datetime, timedelta
from typing import List, Dict, Any

from src.download import download_feed_from_url
from src.logger import get_logger
from src.common import get_all_feed_dates, date_range
from src.stops import get_all_stops
from src.services import get_active_services
from src.trips import get_trips_for_services
from src.report_writer import write_service_html
from src.stop_times import get_stops_for_trips
from src.routes import load_routes
from src.report_data import get_service_report_data
# Service extractor imports
from src.service_extractor.default import DefaultServiceExtractor
from src.service_extractor.lcg_muni import LcgMunicipalServiceExtractor
from src.service_extractor.vgo_muni import VgoMunicipalServiceExtractor

logger = get_logger("service_report")


def render_and_write_html(template_name: str, data: Dict[str, Any], output_path: str):
    from src.report_render import render_html_report
    html = render_html_report(template_name, data)
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(html)


def parse_args():
    parser = argparse.ArgumentParser(
        description="Generate GTFS service reports for a date or date range.")
    parser.add_argument('--start-date', type=str,
                        help='Start date (YYYY-MM-DD)')
    parser.add_argument('--end-date', type=str,
                        help='End date (YYYY-MM-DD, inclusive)')
    parser.add_argument('--all-dates', action='store_true', help='Process all dates in the feed')
    parser.add_argument('--output-dir', type=str, default="./output/", help='Directory to write reports to (default: ./output/)')
    parser.add_argument('--feed-dir', type=str, help="Path to the feed directory")
    parser.add_argument('--feed-url', type=str, help="URL to download the GTFS feed from (if not using local feed directory)")
    parser.add_argument('--force-download', action='store_true', help="Force download even if the feed hasn't been modified (only applies when using --feed-url)")
    parser.add_argument('--service-extractor', type=str, default="default", help="Service extractor to use (default|lcg_muni|vgo_muni)")
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


def main():
    from datetime import datetime as dt
    args = parse_args()
    output_dir = args.output_dir
    feed_url = args.feed_url
    generated_at = dt.now()  # Define at the start of main() for use throughout
    if not feed_url:
        feed_dir = args.feed_dir
    else:
        logger.info(f"Downloading GTFS feed from {feed_url}...")
        feed_dir = download_feed_from_url(feed_url, output_dir, args.force_download)
        if feed_dir is None:
            logger.info("Download was skipped (feed not modified). Exiting.")
            return


    # Select service extractor based on argument
    extractor_arg = getattr(args, 'service_extractor', 'default')
    if extractor_arg == 'lcg_muni':
        service_extractor = LcgMunicipalServiceExtractor
    elif extractor_arg == 'vgo_muni':
        service_extractor = VgoMunicipalServiceExtractor
    else:
        service_extractor = DefaultServiceExtractor

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

    all_generated_dates: list[str] = []
    services_by_date: dict[str, list[dict[str, str]]] = {}

    # Ensure date_list is not empty before processing
    if not date_list:
        logger.error("No valid dates to process.")
        return

    # Load static data once outside the date loop for better performance
    logger.info("Loading static feed data...")
    stops = get_all_stops(feed_dir)
    logger.info(f"Found {len(stops)} stops in the feed.")
    if not stops:
        logger.error("No stops found in the feed.")
        return
    
    routes = load_routes(feed_dir)
    logger.info(f"Loaded {len(routes)} routes from feed.")
    
    # Load all trips once and reuse (significant performance improvement)
    logger.info("Loading all trips data...")
    all_services = []
    for date in date_list:
        date_services = get_active_services(feed_dir, date)
        all_services.extend(date_services)
    
    # Remove duplicates while preserving order
    unique_services = list(dict.fromkeys(all_services))
    all_trips = get_trips_for_services(feed_dir, unique_services)
    logger.info(f"Loaded {sum(len(trips) for trips in all_trips.values())} trips for all services.")
    
    # Load all stop times once (biggest performance improvement)
    all_trip_ids = [trip.trip_id for trip_list in all_trips.values() for trip in trip_list]
    all_stops_for_trips = get_stops_for_trips(feed_dir, all_trip_ids)
    logger.info(f"Loaded stop times for {len(all_stops_for_trips)} trips.")

    # Process each date in date_list
    for current_date in date_list:
        generated_services: list[dict[str, str]] = []
        logger.info(f"Starting service report generation for date {current_date}")
        active_services = get_active_services(feed_dir, current_date)
        if active_services:
            logger.info(
                f"Found {len(active_services)} active services for date {current_date}.")
        else:
            logger.info("No active services found for the given date.")
            continue
        
        # Filter pre-loaded trips by active services for this date
        trips = {service_id: trip_list for service_id, trip_list in all_trips.items() 
                if service_id in active_services}
        total_trip_count = sum(len(trip_list) for trip_list in trips.values())
        logger.info(f"Found {total_trip_count} trips for active services.")
        
        # Filter pre-loaded stop times by trips for this date
        date_trip_ids = [trip.trip_id for trip_list in trips.values() for trip in trip_list]
        stops_for_all_trips = {trip_id: stops for trip_id, stops in all_stops_for_trips.items()
                              if trip_id in date_trip_ids}
        logger.info(f"Using stop times for {len(stops_for_all_trips)} trips.")
        # Prepare output directory for this date
        date_dir = os.path.join(output_dir, current_date)
        os.makedirs(date_dir, exist_ok=True)
        # Group trips by actual_service_id
        grouped_trips = {}
        service_id_to_name = {}
        canonical_to_original_ids = {}
        for service_id, trip_list in trips.items():
            try:
                actual_service_id = service_extractor.extract_actual_service_id_from_identifier(service_id)
            except Exception as e:
                logger.warning(f"Failed to extract actual service id for {service_id}: {e}")
                actual_service_id = service_id
            try:
                service_name = service_extractor.extract_service_name_from_identifier(service_id)
            except Exception as e:
                logger.warning(f"Failed to extract service name for {service_id}: {e}")
                service_name = service_id
            if actual_service_id not in grouped_trips:
                grouped_trips[actual_service_id] = []
                service_id_to_name[actual_service_id] = service_name
                canonical_to_original_ids[actual_service_id] = set()
            grouped_trips[actual_service_id].extend(trip_list)
            canonical_to_original_ids[actual_service_id].add(service_id)

        for actual_service_id, trip_list in grouped_trips.items():
            service_name = service_id_to_name.get(actual_service_id, actual_service_id)
            original_service_ids = sorted(canonical_to_original_ids.get(actual_service_id, []))
            try:
                # Restore the assignment of route_short_name and route_color to each trip
                for trip in trip_list:
                    route_info = routes.get(trip.route_id)
                    if route_info:
                        logger.debug(
                            f"Setting route_short_name and route_color for trip {trip.trip_id}: {route_info}")
                        trip.route_short_name = route_info['route_short_name']
                        trip.route_color = route_info['route_color']
                    else:
                        logger.warning(
                            f"Route ID {trip.route_id} not found in routes data.")

                # --- New: Generate trip detail pages ---
                trips_dir = os.path.join(output_dir, "trips")
                os.makedirs(trips_dir, exist_ok=True)
                # Prepare service data with timestamp for service template
                service_data_with_timestamp = {
                    "generated_at": generated_at
                }
                for trip in trip_list:
                    try:
                        trip_id = trip.trip_id
                        trip_detail_filename = f"trips/{trip_id}.html"
                        trip_detail_path = os.path.join(output_dir, trip_detail_filename)
                        # Gather stop sequence and times for this trip
                        stops_for_trip = stops_for_all_trips.get(trip_id, [])
                        # Each stop: stop_id, stop_name, arrival_time, departure_time, stop_lat, stop_lon
                        stop_sequence = []
                        # Build a lookup for stop_id -> stop_name from all stops
                        stop_id_to_obj = {}
                        for stop_id, stop_obj in stops.items():
                            # stops is a dict with stop_id as key and Stop object as value
                            stop_id_to_obj[stop_id] = stop_obj
                        for stop in stops_for_trip:
                            # Defensive: stop may be a dict or object or even a str (stop_id)
                            if hasattr(stop, "stop_id"):
                                stop_id = stop.stop_id
                                arrival_time = getattr(stop, "arrival_time", None)
                                departure_time = getattr(stop, "departure_time", None)
                            elif isinstance(stop, dict):
                                stop_id = stop.get("stop_id")
                                arrival_time = stop.get("arrival_time")
                                departure_time = stop.get("departure_time")
                            else:
                                stop_id = stop
                                arrival_time = None
                                departure_time = None
                            stop_info = {
                                "stop_id": stop_id,
                                "stop_name": stop_id_to_obj.get(stop_id, stop_id).stop_name,
                                "arrival_time": arrival_time,
                                "departure_time": departure_time,
                                "stop_lat": stop_id_to_obj.get(stop_id, stop_id).stop_lat if stop_id in stop_id_to_obj else None,
                                "stop_lon": stop_id_to_obj.get(stop_id, stop_id).stop_lon if stop_id in stop_id_to_obj else None
                            }
                            stop_sequence.append(stop_info)

                        trip_detail_data = {
                            "trip_id": trip_id,
                            "service_id": trip.service_id,  # always original GTFS service_id
                            "date": current_date,
                            "route_short_name": getattr(trip, "route_short_name", None),
                            "route_color": getattr(trip, "route_color", None),
                            "shape_id": getattr(trip, "shape_id", None),  # always original GTFS shape_id
                            "stop_sequence": stop_sequence,
                            "generated_at": generated_at
                        }
                        # Render trip detail page
                        render_and_write_html(
                            "trip_detail.html.j2",
                            trip_detail_data,
                            trip_detail_path
                        )
                        # Attach the filename to the trip for linking from service report
                        trip.trip_detail_filename = trip_detail_filename
                    except Exception as e:
                        logger.error(f"Error generating trip detail page for trip {trip_id} on {current_date}: {e}")

                # --- End new trip detail page generation ---

                filename = f"{actual_service_id}_{current_date}.html"
                file_path = os.path.join(date_dir, filename)
                # Add service_name to extra data for the report
                extra_data = dict(service_data_with_timestamp)
                extra_data["service_name"] = service_name
                extra_data["original_service_ids"] = original_service_ids
                write_service_html(file_path, feed_dir, actual_service_id, trip_list, current_date, stops_for_all_trips, extra_data, stops)
                # Compute summary details for index
                summary = get_service_report_data(feed_dir, actual_service_id, trip_list, current_date, stops_for_all_trips, stops)
                # First departure and last arrival
                first_departure = summary["trip_rows"][0]["first_arrival"] if summary.get("trip_rows") else None
                last_arrival = max(r.get("last_arrival") for r in summary.get("trip_rows", [])) if summary.get("trip_rows") else None
                # Unique lines with colors and counts
                # Compute trip counts per line
                counts: dict[str, int] = {}
                for r in summary.get("trip_rows", []):
                    name = r.get("route_short_name")
                    if name:
                        counts[name] = counts.get(name, 0) + 1
                seen = set()
                lines = []
                # Preserve order of first appearance
                for r in summary.get("trip_rows", []):
                    name = r.get("route_short_name")
                    color = r.get("route_colour")
                    if name and name not in seen:
                        seen.add(name)
                        lines.append({
                            "short_name": name,
                            "color": color,
                            "count": counts.get(name, 0)
                        })
                # Append enriched service info
                generated_services.append({
                    "service_id": actual_service_id,
                    "service_name": service_name,
                    "original_service_ids": original_service_ids,
                    "filename": filename,
                    "first_departure": first_departure,
                    "last_arrival": last_arrival,
                    "total_distance": summary.get("total_distance"),
                    "lines": lines
                })
            except Exception as e:
                logger.error(
                    f"Error generating report for service {service_id} on {current_date}: {e}")
        # Ensure all valid dates are added to all_generated_dates
        if current_date not in all_generated_dates:
            all_generated_dates.append(current_date)
        if generated_services:
            services_by_date[current_date] = generated_services
            # Compute unique lines for filter buttons
            unique_day_lines: list[dict[str,str]] = []
            seen_lines = set()
            for svc in generated_services:
                for ln in svc.get("lines", []):
                    name = ln.get("short_name")
                    color = ln.get("color")
                    if name and name not in seen_lines:
                        seen_lines.add(name)
                        unique_day_lines.append({"name": name, "color": color})
            # Sort lines by order in routes.txt
            import csv
            try:
                routes_path = os.path.join(feed_dir, 'routes.txt')
                with open(routes_path, encoding='utf-8') as rf:
                    reader = csv.DictReader(rf)
                    order = [row.get('route_short_name', '') for row in reader]
            except Exception:
                order = [ln['name'] for ln in unique_day_lines]
            unique_day_lines.sort(key=lambda ln: order.index(ln['name']) if ln['name'] in order else len(order))
            # Write per-date index
            render_and_write_html(
                "day_index.html.j2",
                {"date": current_date, "services": generated_services, "day_lines": unique_day_lines, "generated_at": generated_at},
                os.path.join(date_dir, "index.html")
            )

        logger.info(f"Service report generation completed for {current_date}.")
    # Write top-level index
    if all_generated_dates:
        render_and_write_html(
            "feed_index.html.j2",
            {"dates": all_generated_dates, "generated_at": generated_at.strftime('%Y-%m-%d %H:%M:%S %Z')},
            os.path.join(output_dir, "index.html")
        )
    logger.info("Service report generation completed.")

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
