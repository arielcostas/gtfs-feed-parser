"""
Functions for handling GTFS shapes data.
"""
import os
import csv
from typing import List, Dict, Tuple, Optional
from src.logger import get_logger

logger = get_logger("shapes")


class ShapePoint:
    """
    Class representing a shape point in the GTFS data.
    """
    def __init__(self, shape_id: str, shape_pt_lat: float, shape_pt_lon: float, shape_pt_sequence: int):
        self.shape_id = shape_id
        self.shape_pt_lat = shape_pt_lat
        self.shape_pt_lon = shape_pt_lon
        self.shape_pt_sequence = shape_pt_sequence

    def __str__(self):
        return f"ShapePoint({self.shape_id=}, {self.shape_pt_lat=}, {self.shape_pt_lon=}, {self.shape_pt_sequence=})"


def load_shapes(feed_dir: str) -> Dict[str, List[ShapePoint]]:
    """
    Load shapes data from the GTFS feed.

    Args:
        feed_dir: Path to the GTFS feed directory

    Returns:
        Dictionary mapping shape_id to lists of ShapePoint objects, sorted by sequence.
    """
    shapes: Dict[str, List[ShapePoint]] = {}
    shapes_file_path = os.path.join(feed_dir, 'shapes.txt')

    if not os.path.exists(shapes_file_path):
        logger.warning("shapes.txt file not found.")
        return shapes

    try:
        with open(shapes_file_path, 'r', encoding='utf-8') as shapes_file:
            reader = csv.DictReader(shapes_file)
            
            required_columns = ['shape_id', 'shape_pt_lat', 'shape_pt_lon', 'shape_pt_sequence']
            missing_columns = [col for col in required_columns if col not in reader.fieldnames]
            if missing_columns:
                logger.error(f"Required columns not found in shapes.txt: {missing_columns}")
                return shapes

            for row in reader:
                shape_id = row['shape_id']
                
                try:
                    shape_pt_lat = float(row['shape_pt_lat'])
                    shape_pt_lon = float(row['shape_pt_lon'])
                    shape_pt_sequence = int(row['shape_pt_sequence'])
                except ValueError as e:
                    logger.warning(f"Error parsing shape data for shape {shape_id}: {e}")
                    continue

                if shape_id not in shapes:
                    shapes[shape_id] = []

                shapes[shape_id].append(ShapePoint(
                    shape_id=shape_id,
                    shape_pt_lat=shape_pt_lat,
                    shape_pt_lon=shape_pt_lon,
                    shape_pt_sequence=shape_pt_sequence
                ))

        # Sort each shape's points by sequence
        for shape_id in shapes:
            shapes[shape_id].sort(key=lambda sp: sp.shape_pt_sequence)

        logger.info(f"Loaded {len(shapes)} shapes from feed.")
        
    except FileNotFoundError:
        logger.warning("shapes.txt file not found.")
    except Exception as e:
        logger.error(f"Error loading shapes: {e}")

    return shapes


def get_shape_for_trip(feed_dir: str, trip_id: str) -> Optional[str]:
    """
    Get the shape_id for a specific trip.

    Args:
        feed_dir: Path to the GTFS feed directory
        trip_id: The trip ID to look up

    Returns:
        The shape_id for the trip, or None if not found.
    """
    trips_file_path = os.path.join(feed_dir, 'trips.txt')
    
    if not os.path.exists(trips_file_path):
        logger.warning("trips.txt file not found.")
        return None

    try:
        with open(trips_file_path, 'r', encoding='utf-8') as trips_file:
            reader = csv.DictReader(trips_file)
            
            if 'trip_id' not in reader.fieldnames or 'shape_id' not in reader.fieldnames:
                logger.warning("Required columns (trip_id, shape_id) not found in trips.txt")
                return None

            for row in reader:
                if row['trip_id'] == trip_id:
                    return row.get('shape_id') or None
                    
    except Exception as e:
        logger.error(f"Error getting shape for trip {trip_id}: {e}")

    return None


def shapes_to_geojson(shapes: Dict[str, List[ShapePoint]]) -> Dict:
    """
    Convert shapes data to GeoJSON format.

    Args:
        shapes: Dictionary of shape_id to list of ShapePoint objects

    Returns:
        GeoJSON FeatureCollection containing all shapes as LineString features.
    """
    features = []
    
    for shape_id, shape_points in shapes.items():
        if not shape_points:
            continue
            
        coordinates = [[point.shape_pt_lon, point.shape_pt_lat] for point in shape_points]
        
        feature = {
            "type": "Feature",
            "properties": {
                "shape_id": shape_id
            },
            "geometry": {
                "type": "LineString",
                "coordinates": coordinates
            }
        }
        features.append(feature)
    
    return {
        "type": "FeatureCollection",
        "features": features
    }
