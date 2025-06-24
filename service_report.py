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
from src.stops import get_all_stops
from src.services import get_active_services
from src.trips import get_trips_for_services
from src.report_writer import write_service_html
from src.stop_times import get_stops_for_trips
from src.routes import load_routes
from src.report_data import get_service_report_data

logger = get_logger("service_report")


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
                from datetime import datetime, timedelta
                # datetime and timedelta are already imported at the top of the file
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
                    dates.add(f"{d[:4]}-{d[4:6]}-{d[6:]}" )
            return sorted(dates)
    return []


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
    args = parse_args()
    output_dir = args.output_dir
    feed_url = args.feed_url
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

    all_generated_dates: list[str] = []
    services_by_date: dict[str, list[dict[str, str]]] = {}

    # Ensure date_list is not empty before processing
    if not date_list:
        logger.error("No valid dates to process.")
        return

    # Process each date in date_list
    for current_date in date_list:
        generated_services: list[dict[str, str]] = []
        logger.info(f"Starting service report generation for date {current_date}")
        stops = get_all_stops(feed_dir)
        logger.info(f"Found {len(stops)} stops in the feed.")
        if not stops:
            logger.info("No stops found in the feed.")
            continue
        active_services = get_active_services(feed_dir, current_date)
        if active_services:
            logger.info(
                f"Found {len(active_services)} active services for date {current_date}.")
        else:
            logger.info("No active services found for the given date.")
            continue
        trips = get_trips_for_services(feed_dir, active_services)
        total_trip_count = sum(len(trip_list) for trip_list in trips.values())
        logger.info(f"Found {total_trip_count} trips for active services.")
        all_trip_ids = [trip.trip_id for trip_list in trips.values() for trip in trip_list]
        stops_for_all_trips = get_stops_for_trips(feed_dir, all_trip_ids)
        logger.info(f"Precomputed stops for {len(stops_for_all_trips)} trips.")
        routes = load_routes(feed_dir)
        logger.info(f"Loaded {len(routes)} routes from feed.")
        # Prepare output directory for this date
        date_dir = os.path.join(output_dir, current_date)
        os.makedirs(date_dir, exist_ok=True)
        for service_id, trip_list in trips.items():
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
                filename = f"{service_id}_{current_date}.html"
                file_path = os.path.join(date_dir, filename)
                write_service_html(file_path, feed_dir, service_id, trip_list, current_date, stops_for_all_trips)
                # Compute summary details for index
                summary = get_service_report_data(feed_dir, service_id, trip_list, current_date, stops_for_all_trips)
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
                    "service_id": service_id,
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
                {"date": current_date, "services": generated_services, "day_lines": unique_day_lines},
                os.path.join(date_dir, "index.html")
            )

        logger.info(f"Service report generation completed for {current_date}.")
    # Write top-level index
    if all_generated_dates:
        render_and_write_html(
            "feed_index.html.j2",
            {"dates": all_generated_dates},
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
