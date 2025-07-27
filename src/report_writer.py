"""
Coordinates report data preparation and rendering, and writes the HTML file.
"""
from typing import List, Dict, Any
from src.report_data import get_service_report_data
from src.report_render import render_html_report
from src.logger import get_logger
from src.trips import TripLine
from src.stop_times import StopTime
import os


def write_service_html(filename: str, feed_dir: str, service_id: str, trips: List[TripLine], date: str, stops_for_trips: Dict[str, List[StopTime]], extra_data: Dict[str, Any] = None, stops: Dict[str, Any] = None) -> None:
    logger = get_logger("report_writer")
    
    try:
        # Prepare data, passing pre-loaded stops for performance
        data: dict[str, Any] = get_service_report_data(feed_dir, service_id, trips, date, stops_for_trips, stops)
        # Merge extra data if provided (e.g., service_name)
        if extra_data:
            data.update(extra_data)
        
        # Render HTML
        html_output: str = render_html_report("service.html.j2", data)
        
        # Ensure directory exists
        os.makedirs(os.path.dirname(filename), exist_ok=True)
        
        # Write with buffered I/O for better performance
        with open(filename, "w", encoding="utf-8", buffering=8192) as f:
            f.write(html_output)
    except Exception as e:
        logger.error(f"Error writing HTML report to {filename}: {e}")
        raise
