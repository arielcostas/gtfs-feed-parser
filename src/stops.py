import csv
import os
from dataclasses import dataclass
from typing import Dict, Optional
from src.logger import get_logger

logger = get_logger("stops")


@dataclass
class Stop:
    stop_id: str
    stop_code: Optional[str]
    stop_name: Optional[str]
    stop_lat: Optional[float]
    stop_lon: Optional[float]


def get_all_stops(feed_dir: str) -> Dict[str, Stop]:
    stops: Dict[str, Stop] = {}
    file_path = os.path.join(feed_dir, 'stops.txt')

    try:
        with open(file_path, 'r', encoding="utf-8", newline='') as f:
            reader = csv.DictReader(f, quotechar='"', delimiter=',')
            for row_num, row in enumerate(reader, start=2):
                try:
                    stop = Stop(
                        stop_id=row['stop_id'],
                        stop_code=row.get('stop_code'),
                        stop_name=row['stop_desc'].strip() if row.get('stop_desc', '').strip() else row.get('stop_name'),
                        stop_lat=float(row['stop_lat']) if row.get('stop_lat') else None,
                        stop_lon=float(row['stop_lon']) if row.get('stop_lon') else None,
                    )
                    stops[stop.stop_id] = stop
                except Exception as e:
                    logger.warning(f"Error parsing stops.txt line {row_num}: {e} - line data: {row}")

    except FileNotFoundError:
        logger.error(f"File not found: {file_path}")
    except Exception as e:
        logger.error(f"Error reading stops.txt: {e}")

    return stops
