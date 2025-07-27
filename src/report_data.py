"""
Prepares structured data for service reports from GTFS data.
"""
from src.stops import get_all_stops
from src.logger import get_logger
from src.utils import time_to_seconds, safe_color_hex, format_distance, format_count
from typing import List, Dict, Any
from src.trips import TripLine
from src.stop_times import StopTime

logger = get_logger("report_data")


def time_to_minutes(time_str: str) -> int:
    """Convert time string (HH:MM:SS) to minutes for sorting."""
    return time_to_seconds(time_str) // 60


def get_stop_info(stop: StopTime, stops: Dict[str, Any]) -> Dict[str, Any]:
    """Extract stop information for a given stop time."""
    s = stops.get(stop.stop_id)
    return {
        "name": s.stop_name if s else "Unknown",
        "code": s.stop_code if s else "Unknown",
        "arrival": stop.arrival_time if s else None
    }


def calculate_trip_distance(first_stop: StopTime, last_stop: StopTime) -> float:
    """Calculate trip distance in kilometers from shape data."""
    if (first_stop.shape_dist_traveled is not None and 
        last_stop.shape_dist_traveled is not None):
        return (last_stop.shape_dist_traveled - first_stop.shape_dist_traveled) / 1000
    return 0.0


def create_trip_row(trip: TripLine, trip_stops: List[StopTime], stops: Dict[str, Any]) -> Dict[str, Any]:
    """Create a data row for a single trip."""
    if not trip_stops:
        return None
        
    first_stop = trip_stops[0]
    last_stop = trip_stops[-1]
    
    fs = get_stop_info(first_stop, stops)
    ls = get_stop_info(last_stop, stops)
    
    distance_km = calculate_trip_distance(first_stop, last_stop)
    
    row = {
        "trip_id": trip.trip_id,
        "route_short_name": trip.route_short_name or trip.route_id,
        "route_colour": safe_color_hex(trip.route_color),
        "headsign": trip.headsign,
        "first_stop_name": fs["name"],
        "first_stop_code": fs["code"],
        "first_arrival": fs["arrival"],
        "last_stop_name": ls["name"],
        "last_stop_code": ls["code"],
        "last_arrival": ls["arrival"]
    }
    
    formatted_distance = format_distance(distance_km)
    if formatted_distance:
        row["distance"] = formatted_distance
    
    return row, distance_km


def generate_css_classes(trips: List[TripLine]) -> str:
    """Generate CSS classes for route colors."""
    # Collect unique routes
    unique_routes: dict[str, str] = {}
    for trip in trips:
        short_name = trip.route_short_name or trip.route_id
        color = safe_color_hex(trip.route_color)
        unique_routes[str(short_name)] = color
    
    # Generate CSS classes
    css_classes: list[str] = []
    for route_short_name, route_color in unique_routes.items():
        try:
            if len(route_color) == 6:
                r = int(route_color[0:2], 16)
                g = int(route_color[2:4], 16)
                b = int(route_color[4:6], 16)
                safe_name = route_short_name.replace(' ', '-').replace('.', '-')
                css_class = f".trip-line-{safe_name} {{ background-color: rgba({r}, {g}, {b}, 0.30); }}"
                css_classes.append(css_class)
        except (ValueError, IndexError) as e:
            logger.warning(f"Error processing color '{route_color}' for route '{route_short_name}': {e}")
    
    return "\n        ".join(css_classes)


def get_service_report_data(trips: List[TripLine], stops_for_trips: Dict[str, List[StopTime]], 
                          stops: Dict[str, Any], routes: Dict[str, Any], 
                          service_extractor_class) -> List[Dict[str, Any]]:
    """
    Generate service report data from trips.
    This is the updated version used by the orchestrator.
    """
    service_data = []
    
    # Group trips by service_id and extract names
    grouped_trips = {}
    service_id_to_name = {}
    canonical_to_original_ids = {}
    
    for trip in trips:
        service_id = trip.service_id
        try:
            actual_service_id = service_extractor_class.extract_actual_service_id_from_identifier(service_id)
        except Exception as e:
            logger.warning(f"Failed to extract actual service id for {service_id}: {e}")
            actual_service_id = service_id
            
        try:
            service_name = service_extractor_class.extract_service_name_from_identifier(service_id)
        except Exception as e:
            logger.warning(f"Failed to extract service name for {service_id}: {e}")
            service_name = service_id
        
        if actual_service_id not in grouped_trips:
            grouped_trips[actual_service_id] = []
            service_id_to_name[actual_service_id] = service_name
            canonical_to_original_ids[actual_service_id] = set()
        
        grouped_trips[actual_service_id].append(trip)
        canonical_to_original_ids[actual_service_id].add(service_id)
    
    # Process each service group
    for actual_service_id, trip_list in grouped_trips.items():
        service_name = service_id_to_name.get(actual_service_id, actual_service_id)
        original_service_ids = sorted(canonical_to_original_ids.get(actual_service_id, []))
        
        # Add route information to trips
        for trip in trip_list:
            route_info = routes.get(trip.route_id)
            if route_info:
                trip.route_short_name = route_info['route_short_name']
                trip.route_color = route_info['route_color']
            else:
                logger.warning(f"Route ID {trip.route_id} not found in routes data.")
        
        service_data.append({
            "service_id": actual_service_id,
            "service_name": service_name,
            "original_service_ids": original_service_ids,
            "trips": trip_list,
            "number_of_trips": len(trip_list),
        })
    
    return service_data


def get_service_report_data_legacy(feed_dir: str, service_id: str, trips: List[TripLine], 
                                 date: str, stops_for_trips: Dict[str, List[StopTime]], 
                                 stops: Dict[str, Any] = None) -> Dict[str, Any]:
    """
    Legacy function for backward compatibility.
    Prepares structured data for service reports from GTFS data.
    """
    # Allow passing stops to avoid reloading them repeatedly
    if stops is None:
        stops = get_all_stops(feed_dir)
    
    total_distance_km = 0
    total_trips = len(trips)
    trip_rows: list[dict[str, Any]] = []

    # Process each trip
    for trip in trips:
        trip_stops = stops_for_trips.get(trip.trip_id, [])
        if not trip_stops:
            continue
            
        row, distance_km = create_trip_row(trip, trip_stops, stops)
        if row:
            trip_rows.append(row)
            total_distance_km += distance_km

    # Sort trip rows by departure time
    trip_rows.sort(key=lambda x: time_to_minutes(x["first_arrival"]))

    # Generate CSS classes
    css_classes_str = generate_css_classes(trips)

    # Build result
    result = {
        "service_id": service_id,
        "date": date,
        "trip_rows": trip_rows,
        "total_distance": format_distance(total_distance_km),
        "total_trips": format_count(total_trips),
        "css_classes": css_classes_str
    }
    return result


# Maintain backward compatibility
get_service_report_data_original = get_service_report_data_legacy
