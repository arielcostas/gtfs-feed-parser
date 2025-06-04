"""
Functions for generating HTML reports from GTFS data.
"""
from src.logger import get_logger
from src.trips import TripLine
from src.stop_times import StopTime
from src.stops import get_all_stops

logger = get_logger("report")

from jinja2 import Environment, FileSystemLoader
import os

def write_service_html(filename: str, service_id: str, trips: list[TripLine], date: str, stops_for_trips: dict[str, list[StopTime]]):
    """
    Generate an HTML report for a service.
    Args:
        filename (str): The path to write the HTML file to.
        service_id (str): The service ID.
        trips (list[TripLine): List of trips for the service.
        date (str): The date of the service in YYYY-MM-DD format.
        stops_for_trips (dict[str, list[StopTime]]): Precomputed stops for all trips.
    """
    stops = get_all_stops()

    from typing import Any

    total_distance_km = 0
    total_trips = len(trips)

    trip_rows: list[dict[str, Any]] = []
    for trip in trips:
        trip_stops = stops_for_trips.get(trip.trip_id, [])
        if not trip_stops:
            continue

        first_stop = trip_stops[0]
        last_stop = trip_stops[-1]

        def stop_info(stop: StopTime) -> dict[str, str|None]:
            s = stops.get(stop.stop_id)
            return {
                "name": s.stop_name if s else "Unknown",
                "code": s.stop_code if s else "Unknown",
                "arrival": stop.arrival_time if s else None
            }

        fs = stop_info(first_stop)
        ls = stop_info(last_stop)

        distance_km = (last_stop.shape_dist_traveled - first_stop.shape_dist_traveled) / 1000
        total_distance_km += distance_km

        trip_rows.append({
            "route_short_name": trip.route_short_name,
            "route_colour": trip.route_color,
            "headsign": trip.headsign,
            "first_stop_name": fs["name"],
            "first_stop_code": fs["code"],
            "first_arrival": fs["arrival"],
            "last_stop_name": ls["name"],
            "last_stop_code": ls["code"],
            "last_arrival": ls["arrival"],
            "distance": f"{distance_km:,.2f}"
        })

    env = Environment(loader=FileSystemLoader(os.path.dirname(__file__)))
    template = env.get_template("service_report.html.j2")

    # Extract unique route_short_name and route_colour pairs
    unique_routes: dict[str, str] = {}
    for trip in trips:
        if hasattr(trip, 'route_short_name') and hasattr(trip, 'route_color') and trip.route_short_name and trip.route_color:
            unique_routes[str(trip.route_short_name)] = str(trip.route_color)

    # Generate CSS classes for unique routes with better opacity handling
    css_classes: list[str] = []
    for route_short_name, route_color in unique_routes.items():
        # Convert hex color to RGB components
        try:
            if len(route_color) == 6:  # Standard hex color without alpha
                r = int(route_color[0:2], 16)
                g = int(route_color[2:4], 16)
                b = int(route_color[4:6], 16)
                css_class = f".trip-line-{route_short_name.replace(' ', '-').replace('.', '-')} {{ background-color: rgba({r}, {g}, {b}, 0.30); }}"
                css_classes.append(css_class)
        except (ValueError, IndexError) as e:
            logger.warning(f"Error processing color '{route_color}' for route '{route_short_name}': {e}")

    # Join all CSS classes
    css_classes_str = "\n        ".join(css_classes)

    # Pass CSS classes to the template
    html_output = template.render(
        service_id=service_id,
        date=date,
        trip_rows=trip_rows,
        total_distance=f"{total_distance_km:,.2f}",
        total_trips=f"{total_trips:,}",
        css_classes=css_classes_str
    )

    try:
        with open(filename, "w", encoding="utf-8") as f:
            f.write(html_output)
        logger.debug(f"Successfully wrote HTML report to {filename}")
    except Exception as e:
        logger.error(f"Error writing HTML report to {filename}: {e}")
        raise
