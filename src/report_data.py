"""
Prepares structured data for service reports from GTFS data.
"""
from src.stops import get_all_stops
from src.logger import get_logger
from typing import List, Dict, Any
from src.trips import TripLine
from src.stop_times import StopTime

def get_service_report_data(service_id: str, trips: List[TripLine], date: str, stops_for_trips: Dict[str, List[StopTime]]) -> Dict[str, Any]:
    logger = get_logger("report_data")
    stops = get_all_stops()
    total_distance_km = 0
    total_trips = len(trips)
    trip_rows: list[dict[str, Any]] = []
    
    # Helper function to convert time to minutes for sorting
    def time_to_minutes(time_str: str) -> int:
        if not time_str:
            return 0
        parts = time_str.split(':')
        if len(parts) != 3:
            return 0
        try:
            hours = int(parts[0])
            minutes = int(parts[1])
            return hours * 60 + minutes
        except ValueError:
            return 0
    
    # Process each trip
    for trip in trips:
        trip_stops = stops_for_trips.get(trip.trip_id, [])
        if not trip_stops:
            continue
        first_stop = trip_stops[0]
        last_stop = trip_stops[-1]
        
        def stop_info(stop: StopTime) -> Dict[str, Any]:
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
            "route_short_name": trip.route_short_name or trip.route_id,
            "route_colour": trip.route_color or "cccccc",
            "headsign": trip.headsign,
            "first_stop_name": fs["name"],
            "first_stop_code": fs["code"],
            "first_arrival": fs["arrival"],
            "last_stop_name": ls["name"],
            "last_stop_code": ls["code"],
            "last_arrival": ls["arrival"],
            "distance": f"{distance_km:,.2f}"
        })
    
    # Sort trip rows by departure time
    trip_rows.sort(key=lambda x: time_to_minutes(x["first_arrival"]))
    
    # Unique routes for CSS
    unique_routes: dict[str, str] = {}
    for trip in trips:
        # Always use route_id as fallback for route_short_name, and default color if missing
        short_name = trip.route_short_name or trip.route_id
        color = trip.route_color or "cccccc"
        unique_routes[str(short_name)] = str(color)
    css_classes: list[str] = []
    for route_short_name, route_color in unique_routes.items():
        try:
            if len(route_color) == 6:
                r = int(route_color[0:2], 16)
                g = int(route_color[2:4], 16)
                b = int(route_color[4:6], 16)
                css_class = f".trip-line-{route_short_name.replace(' ', '-').replace('.', '-')} {{ background-color: rgba({r}, {g}, {b}, 0.30); }}"
                css_classes.append(css_class)
        except (ValueError, IndexError) as e:
            logger.warning(f"Error processing color '{route_color}' for route '{route_short_name}': {e}")
    css_classes_str = "\n        ".join(css_classes)
    return {
        "service_id": service_id,
        "date": date,
        "trip_rows": trip_rows,
        "total_distance": f"{total_distance_km:,.2f}",
        "total_trips": f"{total_trips:,}",
        "css_classes": css_classes_str
    }
