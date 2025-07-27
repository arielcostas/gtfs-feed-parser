"""
Report writers for various output formats (HTML, JSON).
Centralizes all write operations for different report types.
"""
from typing import List, Dict, Any
from src.report_data import get_service_report_data_legacy
from src.report_render import render_html_report
from src.logger import get_logger
from src.trips import TripLine
from src.stop_times import StopTime
import os
import json


def write_service_html(filename: str, feed_dir: str, service_id: str, trips: List[TripLine], date: str, stops_for_trips: Dict[str, List[StopTime]], extra_data: Dict[str, Any] = None, stops: Dict[str, Any] = None) -> None:
    logger = get_logger("report_writer")
    
    try:
        # Prepare data, passing pre-loaded stops for performance
        data: dict[str, Any] = get_service_report_data_legacy(feed_dir, service_id, trips, date, stops_for_trips, stops)
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


def render_and_write_html(template_name: str, data: Dict[str, Any], output_path: str) -> None:
    """
    Render a template with data and write to an HTML file.
    
    Args:
        template_name: Name of the Jinja2 template file
        data: Dictionary containing data to render in the template
        output_path: Path where the HTML file should be written
    """
    logger = get_logger("report_writer")
    
    try:
        # Render HTML using the template
        html_output = render_html_report(template_name, data)
        
        # Ensure directory exists
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        
        # Write to file
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(html_output)
            
        logger.debug(f"HTML report written to: {output_path}")
    except Exception as e:
        logger.error(f"Error writing HTML report to {output_path}: {e}")
        raise


def write_stop_json(output_dir: str, date: str, stop_code: str, arrivals: List[Dict[str, Any]], pretty: bool = False) -> None:
    """
    Write stop arrivals data to a JSON file.
    
    Args:
        output_dir: Base output directory
        date: Date string for the data
        stop_code: Stop code identifier
        arrivals: List of arrival dictionaries
        pretty: Whether to format JSON with indentation
    """
    logger = get_logger("report_writer")
    
    try:
        # Create the stops directory for this date
        date_dir = os.path.join(output_dir, "stops", date)
        os.makedirs(date_dir, exist_ok=True)

        # Create the JSON file
        file_path = os.path.join(date_dir, f"{stop_code}.json")

        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(arrivals, f, indent=2 if pretty else None, separators=(
                ",", ":") if not pretty else None, ensure_ascii=False)
                
        logger.debug(f"Stop JSON written to: {file_path}")
    except Exception as e:
        logger.error(f"Error writing stop JSON to {output_dir}/stops/{date}/{stop_code}.json: {e}")
        raise


def write_index_json(output_dir: str, data: Dict[str, Any], filename: str = "index.json", pretty: bool = False) -> None:
    """
    Write index data to a JSON file.
    
    Args:
        output_dir: Directory where the JSON file should be written
        data: Dictionary containing the index data
        filename: Name of the JSON file (default: "index.json")
        pretty: Whether to format JSON with indentation
    """
    logger = get_logger("report_writer")
    
    try:
        # Create the output directory if it doesn't exist
        os.makedirs(output_dir, exist_ok=True)
        
        # Write the index.json file
        index_filepath = os.path.join(output_dir, filename)
        with open(index_filepath, 'w', encoding='utf-8') as f:
            if pretty:
                json.dump(data, f, ensure_ascii=False, indent=2)
            else:
                json.dump(data, f, ensure_ascii=False, separators=(',', ':'))
        
        logger.info(f"Index JSON written to: {index_filepath}")
    except Exception as e:
        logger.error(f"Error writing index JSON to {output_dir}/{filename}: {e}")
        raise
