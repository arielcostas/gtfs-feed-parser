"""
Functions for handling GTFS stop_times data.
"""
import os
from src.logger import get_logger

logger = get_logger("stop_times")

class StopTime:
    """
    Class representing a stop time entry in the GTFS data.
    """
    def __init__(self, trip_id: str, arrival_time: str, departure_time: str, stop_id: str, stop_sequence: int, shape_dist_traveled: float | None):
        self.trip_id = trip_id
        self.arrival_time = arrival_time
        self.departure_time = departure_time
        self.stop_id = stop_id
        self.stop_sequence = stop_sequence
        self.shape_dist_traveled = shape_dist_traveled
        self.day_change = False  # New attribute to indicate day change

    def __str__(self):
        return f"StopTime({self.trip_id=}, {self.arrival_time=}, {self.departure_time=}, {self.stop_id=}, {self.stop_sequence=})"


def get_stops_for_trips(feed_dir: str, trip_ids: list[str]) -> dict[str, list[StopTime]]:
    """
    Get stops for a list of trip IDs based on the 'stop_times.txt' file.
    Args:
        trip_ids (list[str]): List of trip IDs to find stops for.
    Returns:
        dict[str, list[StopTime]]: Dictionary mapping trip IDs to lists of StopTime objects (ordered by stop_sequence).
    """
    import csv
    
    stops: dict[str, list[StopTime]] = {}
    # Convert trip_ids to a set for O(1) lookup instead of O(n)
    trip_ids_set = set(trip_ids)
    
    try:
        with open(os.path.join(feed_dir, 'stop_times.txt'), 'r', encoding="utf-8", newline='') as stop_times_file:
            # Use csv.DictReader for better performance and cleaner code
            reader = csv.DictReader(stop_times_file)
            
            # Check for required columns
            required_columns = ['trip_id', 'arrival_time', 'departure_time', 'stop_id', 'stop_sequence']
            missing_columns = [col for col in required_columns if col not in reader.fieldnames]
            if missing_columns:
                logger.error(f"Required columns not found in header: {missing_columns}")
                return stops
            
            has_shape_dist = 'shape_dist_traveled' in reader.fieldnames
            if not has_shape_dist:
                logger.warning("Column 'shape_dist_traveled' not found in stop_times.txt. Distances will be set to None.")
            
            for row in reader:
                trip_id = row['trip_id']
                if trip_id in trip_ids_set:
                    if trip_id not in stops:
                        stops[trip_id] = []
                    
                    # Parse shape distance if available
                    dist = None
                    if has_shape_dist and row['shape_dist_traveled']:
                        try:
                            dist = float(row['shape_dist_traveled'])
                        except ValueError:
                            pass  # Keep dist as None if parsing fails
                    
                    try:
                        stops[trip_id].append(StopTime(
                            trip_id=trip_id,
                            arrival_time=row['arrival_time'],
                            departure_time=row['departure_time'],
                            stop_id=row['stop_id'],
                            stop_sequence=int(row['stop_sequence']),
                            shape_dist_traveled=dist
                        ))
                    except ValueError as e:
                        logger.warning(f"Error parsing stop_sequence for trip {trip_id}: {e}")
                        continue
        
        # Sort each trip's stops by stop_sequence
        for trip_id in stops:
            stops[trip_id].sort(key=lambda st: st.stop_sequence)
    except FileNotFoundError:
        logger.warning("stop_times.txt file not found.")
    return stops
