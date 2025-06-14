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
    def __init__(self, trip_id: str, arrival_time: str, departure_time: str, stop_id: str, stop_sequence: int, shape_dist_traveled: float):
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
    stops: dict[str, list[StopTime]] = {}
    try:
        with open(os.path.join(feed_dir, 'stop_times.txt'), 'r', encoding="utf-8") as stop_times_file:
            lines = stop_times_file.readlines()
            if len(lines) <= 1:
                logger.warning("stop_times.txt file is empty or has only header line, not processing.")
                return stops
            header = lines[0].strip().split(',')
            try:
                trip_id_index = header.index('trip_id')
                arrival_time_index = header.index('arrival_time')
                departure_time_index = header.index('departure_time')
                stop_id_index = header.index('stop_id')
                stop_sequence_index = header.index('stop_sequence')
            except ValueError as e:
                logger.error(f"Required column not found in header: {e}")
                return stops
            for line in lines[1:]:
                parts = line.strip().split(',')
                if len(parts) < len(header):
                    logger.warning(f"Skipping malformed line in stop_times.txt: {line.strip()}")
                    continue
                trip_id = parts[trip_id_index]
                if trip_id in trip_ids:
                    if trip_id not in stops:
                        stops[trip_id] = []
                    stops[trip_id].append(StopTime(
                        trip_id=trip_id,
                        arrival_time=parts[arrival_time_index],
                        departure_time=parts[departure_time_index],
                        stop_id=parts[stop_id_index],
                        stop_sequence=int(parts[stop_sequence_index]),
                        shape_dist_traveled=float(parts[header.index('shape_dist_traveled')])
                    ))
        # Sort each trip's stops by stop_sequence
        for trip_id in stops:
            stops[trip_id].sort(key=lambda st: st.stop_sequence)
    except FileNotFoundError:
        logger.warning("stop_times.txt file not found.")
    return stops
