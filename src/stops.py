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
        with open(file_path, 'r', encoding="utf-8") as f:
            lines = f.readlines()
            if len(lines) <= 1:
                logger.warning("stops.txt is empty or only contains header.")
                return stops

            header = lines[0].strip().split(',')
            col = {name: i for i, name in enumerate(header)}

            for line in lines[1:]:
                fields = line.strip().split(',')
                if len(fields) < len(header):
                    logger.warning(f"Skipping malformed line: {line.strip()}")
                    continue

                try:
                    stop = Stop(
                        stop_id=fields[col['stop_id']],
                        stop_code=fields[col['stop_code']
                                         ] if 'stop_code' in col else None,
                        stop_name=fields[col['stop_name']
                                         ] if 'stop_name' in col else None,
                        stop_lat=float(
                            fields[col['stop_lat']]) if 'stop_lat' in col and fields[col['stop_lat']] else None,
                        stop_lon=float(
                            fields[col['stop_lon']]) if 'stop_lon' in col and fields[col['stop_lon']] else None,
                    )
                    stops[stop.stop_id] = stop
                except Exception as e:
                    logger.warning(f"Error parsing line: {line.strip()} - {e}")

    except FileNotFoundError:
        logger.error(f"File not found: {file_path}")
    except Exception as e:
        logger.error(f"Error reading stops.txt: {e}")

    return stops
