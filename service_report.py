"""
Main script for generating service reports from GTFS data.
"""
import os
import shutil
import sys
import traceback

# Add the parent directory to sys.path if needed
if __name__ == "__main__":
    sys.path.insert(0, os.path.dirname(__file__))

from src.logger import get_logger
from src.config import DATE, OUTPUT_DIR
from src.stops import get_all_stops
from src.services import get_active_services
from src.trips import get_trips_for_services
from src.report import write_service_html
from src.stop_times import get_stops_for_trips
from src.routes import load_routes

logger = get_logger("service_report")


def main():
    """
    Main function to demonstrate the service report generation.
    """
    logger.info(f"Starting service report generation for date {DATE}")

    stops = get_all_stops()
    logger.info(f"Found {len(stops)} stops in the feed.")

    if not stops:
        logger.info("No stops found in the feed.")
        return

    # Get active services for the specified date
    active_services = get_active_services(DATE)

    if active_services:
        logger.info(
            f"Found {len(active_services)} active services for date {DATE}.")
    else:
        logger.info("No active services found for the given date.")
        return

    # Get trips for the active services
    trips = get_trips_for_services(active_services)
    total_trip_count = sum(len(trip_list) for trip_list in trips.values())
    logger.info(f"Found {total_trip_count} trips for active services.")

    # Precompute stops for all trips
    all_trip_ids = [trip.trip_id for trip_list in trips.values() for trip in trip_list]
    stops_for_all_trips = get_stops_for_trips(all_trip_ids)
    logger.info(f"Precomputed stops for {len(stops_for_all_trips)} trips.")

    # Load routes data
    routes = load_routes()
    logger.info(f"Loaded {len(routes)} routes from feed.")

    # Modify trips to include route_short_name and route_color
    for service_id, trip_list in trips.items():
        for trip in trip_list:
            route_info = routes.get(trip.route_id)
            if route_info:
                logger.debug(f"Setting route_short_name and route_color for trip {trip.trip_id}: {route_info}")
                trip.route_short_name = route_info['route_short_name']
                trip.route_color = route_info['route_color']
            else:
                logger.warning(f"Route ID {trip.route_id} not found in routes data.")

    # Prepare output directory
    if os.path.exists(OUTPUT_DIR):
        logger.debug(
            f"Output directory {OUTPUT_DIR} already exists, cleaning it.")
        try:
            shutil.rmtree(OUTPUT_DIR)
        except Exception as e:
            logger.error(f"Error removing output directory: {e}")

    try:
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        logger.debug(f"Output directory {OUTPUT_DIR} created/prepared.")
    except Exception as e:
        logger.error(f"Error creating output directory: {e}")
        return

    # Generate reports for each service
    for service_id, trip_list in trips.items():
        try:
            write_service_html(os.path.join(
                OUTPUT_DIR, f"{service_id}.html"), service_id, trip_list, DATE, stops_for_all_trips)
        except Exception as e:
            logger.error(
                f"Error generating report for service {service_id}: {e}")

    logger.info("Service report generation completed successfully.")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.critical(f"An unexpected error occurred: {e}", exc_info=True)
        traceback.print_exc()
        sys.exit(1)
