"""
Coordinates report data preparation and rendering, and writes the HTML file.
"""
from typing import List, Dict, Any
from src.report_data import get_service_report_data
from src.report_render import render_html_report
from src.logger import get_logger
from src.trips import TripLine
from src.stop_times import StopTime

def write_service_html(filename: str, feed_dir: str, service_id: str, trips: List[TripLine], date: str, stops_for_trips: Dict[str, List[StopTime]]) -> None:
    logger = get_logger("report_writer")
    
    data: dict[str, Any] = get_service_report_data(feed_dir, service_id, trips, date, stops_for_trips)
    
    html_output: str = render_html_report("service_report.html.j2", data)
    try:
        with open(filename, "w", encoding="utf-8") as f:
            f.write(html_output)
        logger.debug(f"Successfully wrote HTML report to {filename}")
    except Exception as e:
        logger.error(f"Error writing HTML report to {filename}: {e}")
        raise
