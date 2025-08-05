"""
Report generation orchestrators.
This module contains the main business logic for generating different types of reports,
separated from CLI parsing and main script concerns.
"""
import os
import json
import multiprocessing
from datetime import datetime as dt
from typing import List, Dict, Any, Optional
from multiprocessing import Pool, cpu_count

from .download import download_feed_from_url
from .logger import get_logger
from .common import get_all_feed_dates, date_range
from .stops import get_all_stops
from .services import get_active_services
from .trips import get_trips_for_services
from .stop_times import get_stops_for_trips
from .routes import load_routes
from .report_data import get_service_report_data_legacy
from .report_render import render_html_report
from .report_writer import write_service_html, write_index_json, render_and_write_html
from .shapes import load_shapes, shapes_to_geojson
from .street_name import get_street_name
from .utils import create_stop_id_to_code_mapping, time_to_seconds

# Service extractor imports
from .service_extractor.default import DefaultServiceExtractor
from .service_extractor.lcg_muni import LcgMunicipalServiceExtractor
from .service_extractor.vgo_muni import VgoMunicipalServiceExtractor

logger = get_logger("orchestrators")


def get_service_extractor_class(extractor_name: str):
    """Get the appropriate service extractor class."""
    if extractor_name == 'lcg_muni':
        return LcgMunicipalServiceExtractor
    elif extractor_name == 'vgo_muni':
        return VgoMunicipalServiceExtractor
    else:
        return DefaultServiceExtractor


def prepare_feed_directory(feed_dir: Optional[str], feed_url: Optional[str], 
                          output_dir: str, force_download: bool = False) -> Optional[str]:
    """
    Prepare the feed directory by downloading if necessary.
    
    Returns:
        Path to the feed directory, or None if download was skipped
    """
    if feed_url:
        logger.info(f"Downloading GTFS feed from {feed_url}...")
        feed_dir = download_feed_from_url(feed_url, output_dir, force_download)
        if feed_dir is None:
            logger.info("Download was skipped (feed not modified).")
            return None
    
    return feed_dir


def get_date_list(all_dates: bool, start_date: Optional[str], 
                 end_date: Optional[str], feed_dir: str) -> List[str]:
    """Get the list of dates to process."""
    if all_dates:
        all_feed_dates = get_all_feed_dates(feed_dir)
        if not all_feed_dates:
            raise ValueError('No valid dates found in feed.')
        return all_feed_dates
    else:
        end_date = end_date or start_date
        return list(date_range(start_date, end_date))


def generate_service_reports_orchestrator(feed_dir: str, output_dir: str,
                                        all_dates_flag: bool, start_date: Optional[str],
                                        end_date: Optional[str], service_extractor: str) -> Dict[str, Any]:
    """
    Orchestrate the generation of service reports.
    
    This function handles the complete workflow for generating service reports:
    1. Loads static data (stops, routes) once for performance
    2. Pre-loads all trips and stop times to avoid repeated queries  
    3. Processes each date, filtering data and generating HTML reports
    4. Returns comprehensive statistics about the generation process
    
    Returns:
        Dictionary with generation statistics and data
    """
    logger.info("Starting service report generation...")
    
    # Get service extractor class
    service_extractor_class = get_service_extractor_class(service_extractor)
    
    # Get date list
    date_list = get_date_list(all_dates_flag, start_date, end_date, feed_dir)
    
    if not date_list:
        raise ValueError("No valid dates to process.")
    
    # Load static data once for performance
    logger.info("Loading static feed data...")
    stops = get_all_stops(feed_dir)
    logger.info(f"Found {len(stops)} stops in the feed.")
    if not stops:
        raise ValueError("No stops found in the feed.")
    
    routes = load_routes(feed_dir)
    logger.info(f"Loaded {len(routes)} routes from feed.")
    
    # Pre-load all trips for performance
    logger.info("Loading all trips data...")
    all_services = []
    for date in date_list:
        date_services = get_active_services(feed_dir, date)
        all_services.extend(date_services)
    
    unique_services = list(dict.fromkeys(all_services))
    all_trips = get_trips_for_services(feed_dir, unique_services)
    logger.info(f"Loaded {sum(len(trips) for trips in all_trips.values())} trips for all services.")
    
    # Load all stop times once
    all_trip_ids = [trip.trip_id for trip_list in all_trips.values() for trip in trip_list]
    all_stops_for_trips = get_stops_for_trips(feed_dir, all_trip_ids)
    logger.info(f"Loaded stop times for {len(all_stops_for_trips)} trips.")
    
    # Generate trip HTML files once for all unique trips (not per date)
    logger.info("Generating individual trip HTML files...")
    trips_dir = os.path.join(output_dir, "trips")
    os.makedirs(trips_dir, exist_ok=True)
    
    generated_trip_count = 0
    for service_id, trip_list in all_trips.items():
        for trip in trip_list:
            try:
                trip_id = trip.trip_id
                trip_detail_filename = f"trips/{trip_id}.html"
                trip_detail_path = os.path.join(output_dir, trip_detail_filename)
                
                # Skip if trip file already exists (avoid duplicates)
                if os.path.exists(trip_detail_path):
                    continue
                
                # Gather stop sequence and times for this trip
                stops_for_trip = all_stops_for_trips.get(trip_id, [])
                stop_sequence = []
                
                for stop in stops_for_trip:
                    stop_id = stop.stop_id
                    arrival_time = stop.arrival_time
                    departure_time = stop.departure_time
                    
                    stop_obj = stops.get(stop_id)
                    if stop_obj:
                        stop_info = {
                            "stop_id": stop_id,
                            "stop_name": stop_obj.stop_name,
                            "arrival_time": arrival_time,
                            "departure_time": departure_time,
                            "stop_lat": stop_obj.stop_lat,
                            "stop_lon": stop_obj.stop_lon
                        }
                        stop_sequence.append(stop_info)
                
                # Get route info for this trip
                route_info = routes.get(trip.route_id, {})
                route_short_name = route_info.get('route_short_name', None)
                route_color = route_info.get('route_color', None)
                
                # Get trip name using the service extractor
                trip_name = service_extractor_class.get_trip_name_from_trip_id(trip_id)
                
                trip_detail_data = {
                    "trip_id": trip_id,
                    "trip_name": trip_name,
                    "service_id": trip.service_id,
                    "date": "various",  # Since trip spans multiple dates
                    "route_short_name": route_short_name,
                    "route_color": route_color,
                    "shape_id": getattr(trip, "shape_id", None),
                    "stop_sequence": stop_sequence,
                    "generated_at": generated_at
                }
                
                # Render trip detail page
                render_and_write_html(
                    "trip_detail.html.j2",
                    trip_detail_data,
                    trip_detail_path
                )
                
                generated_trip_count += 1
                
            except Exception as e:
                logger.error(f"Error generating trip detail page for trip {trip_id}: {e}")
    
    logger.info(f"Generated {generated_trip_count} unique trip HTML files.")
    
    # Process each date
    generated_at = dt.now()
    all_generated_dates = []
    services_by_date = {}
    
    for current_date in date_list:
        logger.info(f"Processing service report for date {current_date}")
        
        active_services = get_active_services(feed_dir, current_date)
        if not active_services:
            logger.info("No active services found for the given date.")
            continue
        
        logger.info(f"Found {len(active_services)} active services for date {current_date}.")
        
        # Filter pre-loaded trips by active services for this date
        trips = {service_id: trip_list for service_id, trip_list in all_trips.items()
                if service_id in active_services}
        
        # For now, use a simplified approach that generates HTML per service
        # (This maintains compatibility with existing write_service_html function)
        generated_services = []
        
        for service_id, trip_list in trips.items():
            try:
                # Extract service information
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
                
                # Add route information to trips
                for trip in trip_list:
                    route_info = routes.get(trip.route_id)
                    if route_info:
                        trip.route_short_name = route_info['route_short_name']
                        trip.route_color = route_info['route_color']
                    else:
                        logger.warning(f"Route ID {trip.route_id} not found in routes data.")
                
                # Generate and write service HTML
                # Create proper filename and path
                filename = f"{actual_service_id}.html"
                date_dir = os.path.join(output_dir, current_date)
                os.makedirs(date_dir, exist_ok=True)
                file_path = os.path.join(date_dir, filename)
                
                # Prepare extra data
                extra_data = {
                    "service_name": service_name,
                    "generated_at": generated_at
                }
                
                # Filter stops for trips for this service
                stops_for_service_trips = {trip_id: stops for trip_id, stops in all_stops_for_trips.items() 
                                         if any(trip.trip_id == trip_id for trip in trip_list)}
                
                write_service_html(file_path, feed_dir, actual_service_id, trip_list, current_date, 
                                 stops_for_service_trips, extra_data, stops)
                
                # Collect route information with consecutive trip counts in sequence
                service_routes = []
                if trip_list:
                    # Group consecutive trips with the same route
                    current_route = None
                    current_count = 0
                    
                    for trip in trip_list:
                        route_info = routes.get(trip.route_id, {})
                        route_short_name = route_info.get('route_short_name', trip.route_id)
                        route_color = route_info.get('route_color', '0074d9')
                        
                        if current_route is None or current_route['short_name'] != route_short_name:
                            # New route or different route, save previous if exists
                            if current_route is not None:
                                service_routes.append(current_route)
                            
                            # Start new route group
                            current_route = {
                                "short_name": route_short_name,
                                "color": route_color,
                                "count": 1
                            }
                        else:
                            # Same route as previous, increment count
                            current_count += 1
                            current_route["count"] += 1
                    
                    # Don't forget to add the last route
                    if current_route is not None:
                        service_routes.append(current_route)
                
                # Calculate first departure and last arrival times
                first_departure = None
                last_arrival = None
                if trip_list:
                    # Get all stop times for this service's trips
                    all_times = []
                    for trip in trip_list:
                        trip_stops = stops_for_service_trips.get(trip.trip_id, [])
                        if trip_stops:
                            all_times.extend([stop.departure_time for stop in trip_stops if stop.departure_time])
                            all_times.extend([stop.arrival_time for stop in trip_stops if stop.arrival_time])
                    
                    if all_times:
                        all_times.sort()
                        first_departure = all_times[0]
                        last_arrival = all_times[-1]
                
                generated_services.append({
                    "service_id": actual_service_id,
                    "service_name": service_name,
                    "number_of_trips": len(trip_list),
                    "filename": filename,
                    "lines": service_routes,
                    "first_departure": first_departure,
                    "last_arrival": last_arrival
                })
                
            except Exception as e:
                logger.error(f"Error processing service {service_id}: {e}")
        
        # Generate day index for this date
        try:
            # Compute unique lines for filter buttons
            unique_day_lines = []
            seen_lines = set()
            for service_data in generated_services:
                for line in service_data.get("lines", []):
                    name = line.get("short_name")
                    color = line.get("color")
                    if name and name not in seen_lines:
                        seen_lines.add(name)
                        unique_day_lines.append({"name": name, "color": color})
            
            # Sort lines by order in routes.txt
            import csv
            try:
                routes_path = os.path.join(feed_dir, 'routes.txt')
                with open(routes_path, encoding='utf-8') as rf:
                    reader = csv.DictReader(rf)
                    order = [row.get('route_short_name', '') for row in reader]
            except Exception:
                order = [ln['name'] for ln in unique_day_lines]
            unique_day_lines.sort(key=lambda ln: order.index(ln['name']) if ln['name'] in order else len(order))
            
            # Write per-date index
            render_and_write_html(
                "day_index.html.j2",
                {
                    "date": current_date, 
                    "services": generated_services, 
                    "day_lines": unique_day_lines, 
                    "generated_at": generated_at
                },
                os.path.join(date_dir, "index.html")
            )
            logger.info(f"Generated day index for {current_date}")
        except Exception as e:
            logger.error(f"Error generating day index for {current_date}: {e}")
        
        all_generated_dates.append(current_date)
        services_by_date[current_date] = generated_services
    
    logger.info(f"Service report generation completed for {len(all_generated_dates)} dates")
    
    # Generate feed-level index if we have generated dates
    if all_generated_dates:
        try:
            render_and_write_html(
                "feed_index.html.j2",
                {
                    "dates": all_generated_dates, 
                    "generated_at": generated_at.strftime('%Y-%m-%d %H:%M:%S %Z')
                },
                os.path.join(output_dir, "index.html")
            )
            logger.info("Generated feed-level index")
        except Exception as e:
            logger.error(f"Error generating feed-level index: {e}")
    
    return {
        'generated_dates': all_generated_dates,
        'services_by_date': services_by_date,
        'total_services': len(unique_services),
        'total_trips': len(all_trip_ids)
    }


def process_stop_date(args):
    """Process a single date for stop reports (used in multiprocessing)."""
    feed_dir, date, numeric_stop_code = args
    
    logger.info(f"Processing stop data for date {date}")
    
    stops = get_all_stops(feed_dir)
    active_services = get_active_services(feed_dir, date)
    
    if not active_services:
        logger.info(f"No active services found for date {date}")
        return date, {}
    
    trips = get_trips_for_services(feed_dir, active_services)
    all_trip_ids = [trip.trip_id for trip_list in trips.values() for trip in trip_list]
    stops_for_all_trips = get_stops_for_trips(feed_dir, all_trip_ids)
    routes = load_routes(feed_dir)
    
    # Create stop_id to stop_code mapping using utility function
    stop_id_to_code = create_stop_id_to_code_mapping(stops, numeric_stop_code)
    
    # Organize data by stop_code
    stop_arrivals = {}
    
    for service_id, trip_list in trips.items():
        for trip in trip_list:
            route_info = routes.get(trip.route_id, {})
            route_short_name = route_info.get('route_short_name', '')
            route_color = route_info.get('route_color', '')
            
            trip_stops = stops_for_all_trips.get(trip.trip_id, [])
            
            for i, stop_time in enumerate(trip_stops):
                stop_id = stop_time.stop_id
                stop_code = stop_id_to_code.get(stop_id)
                
                if not stop_code:
                    continue
                
                if stop_code not in stop_arrivals:
                    stop_arrivals[stop_code] = []
                
                # Get stop information
                stop_info = stops.get(stop_id)
                if not stop_info:
                    continue
                
                stop_name = stop_info.stop_name
                stop_lat = stop_info.stop_lat
                stop_lon = stop_info.stop_lon
                
                # Extract street name
                street_name = get_street_name(stop_name)
                
                arrival_data = {
                    'arrival_time': stop_time.arrival_time,
                    'arrival_time_seconds': time_to_seconds(stop_time.arrival_time),
                    'departure_time': stop_time.departure_time,
                    'departure_time_seconds': time_to_seconds(stop_time.departure_time),
                    'stop_sequence': stop_time.stop_sequence,
                    'trip_id': trip.trip_id,
                    'route_id': trip.route_id,
                    'route_short_name': route_short_name,
                    'route_color': route_color,
                    'service_id': service_id,
                    'trip_headsign': getattr(trip, 'trip_headsign', '') or '',
                    'stop_name': stop_name,
                    'stop_lat': stop_lat,
                    'stop_lon': stop_lon,
                    'street_name': street_name,
                    'is_first_stop': i == 0,
                    'is_last_stop': i == len(trip_stops) - 1
                }
                
                stop_arrivals[stop_code].append(arrival_data)
    
    # Sort arrivals by time for each stop
    for stop_code in stop_arrivals:
        stop_arrivals[stop_code].sort(key=lambda x: x['arrival_time_seconds'])
    
    return date, stop_arrivals


def generate_stop_reports_orchestrator(feed_dir: str, output_dir: str,
                                     all_dates_flag: bool, start_date: Optional[str],
                                     end_date: Optional[str], numeric_stop_code: bool = False,
                                     jobs: int = 0, pretty: bool = False) -> Dict[str, Any]:
    """
    Orchestrate the generation of stop reports.
    
    Returns:
        Dictionary with generation statistics
    """
    logger.info("Starting stop report generation...")
    
    # Get date list
    date_list = get_date_list(all_dates_flag, start_date, end_date, feed_dir)
    
    if not date_list:
        raise ValueError("No valid dates to process.")
    
    # Determine number of jobs
    if jobs <= 0:
        jobs = cpu_count()
    
    logger.info(f"Processing {len(date_list)} dates with {jobs} jobs")
    
    # Prepare arguments for multiprocessing
    process_args = [(feed_dir, date, numeric_stop_code) for date in date_list]
    
    # Process dates in parallel
    all_stops_summary = {}
    
    if jobs == 1:
        # Sequential processing for debugging
        results = [process_stop_date(args) for args in process_args]
    else:
        # Parallel processing
        with Pool(processes=jobs) as pool:
            results = pool.map(process_stop_date, process_args)
    
    # Write results and collect summary
    for date, stop_arrivals in results:
        if stop_arrivals:
            # Write JSON file for this date
            filename = f"stops_{date}.json"
            filepath = os.path.join(output_dir, filename)
            
            with open(filepath, 'w', encoding='utf-8') as f:
                if pretty:
                    json.dump(stop_arrivals, f, ensure_ascii=False, indent=2)
                else:
                    json.dump(stop_arrivals, f, ensure_ascii=False, separators=(',', ':'))
            
            logger.info(f"Written stop report for {date}: {len(stop_arrivals)} stops")
            
            # Add to summary
            all_stops_summary[date] = {
                'total_stops': len(stop_arrivals),
                'filename': filename
            }
    
    # Write index files
    write_index_json(output_dir, all_stops_summary, pretty)
    
    logger.info(f"Stop report generation completed for {len(results)} dates")
    
    return {
        'generated_dates': list(all_stops_summary.keys()),
        'total_dates': len(date_list),
        'total_stops': sum(summary['total_stops'] for summary in all_stops_summary.values())
    }


def generate_geojson_reports_orchestrator(feed_dir: str, output_dir: str, 
                                        pretty: bool = False) -> Dict[str, Any]:
    """
    Orchestrate the generation of GeoJSON shape files.
    
    Returns:
        Dictionary with generation statistics
    """
    logger.info("Starting GeoJSON shape generation...")
    
    # Load shapes from the feed
    shapes = load_shapes(feed_dir)
    logger.info(f"Loaded {len(shapes)} shapes from feed")
    
    if not shapes:
        logger.warning("No shapes found in the feed")
        return {'shapes_count': 0, 'files_written': 0}
    
    # Create the shapes directory
    shapes_dir = os.path.join(output_dir, "shapes")
    os.makedirs(shapes_dir, exist_ok=True)
    
    # Convert to GeoJSON and write combined file
    geojson_data = shapes_to_geojson(shapes)
    
    # Write the combined GeoJSON file
    combined_filepath = os.path.join(shapes_dir, "all_shapes.geojson")
    with open(combined_filepath, 'w', encoding='utf-8') as f:
        if pretty:
            json.dump(geojson_data, f, ensure_ascii=False, indent=2)
        else:
            json.dump(geojson_data, f, ensure_ascii=False, separators=(',', ':'))
    
    logger.info(f"Written combined GeoJSON file: {combined_filepath}")
    files_written = 1
    
    # Write individual shape files
    shape_ids = []
    for shape_id, shape_points in shapes.items():
        if not shape_points:
            continue
            
        coordinates = [[point.shape_pt_lon, point.shape_pt_lat] for point in shape_points]
        
        individual_geojson = {
            "type": "Feature",
            "properties": {
                "shape_id": shape_id
            },
            "geometry": {
                "type": "LineString",
                "coordinates": coordinates
            }
        }
        
        # Write individual file
        individual_filepath = os.path.join(shapes_dir, f"{shape_id}.geojson")
        with open(individual_filepath, 'w', encoding='utf-8') as f:
            if pretty:
                json.dump(individual_geojson, f, ensure_ascii=False, indent=2)
            else:
                json.dump(individual_geojson, f, ensure_ascii=False, separators=(',', ':'))
        
        shape_ids.append(shape_id)
        files_written += 1
    
    # Write shapes index
    index_filepath = os.path.join(shapes_dir, "index.json")
    index_data = {
        "shapes": sorted(shape_ids),
        "count": len(shape_ids)
    }
    
    with open(index_filepath, 'w', encoding='utf-8') as f:
        if pretty:
            json.dump(index_data, f, ensure_ascii=False, indent=2)
        else:
            json.dump(index_data, f, ensure_ascii=False, separators=(',', ':'))
    
    files_written += 1
    logger.info(f"Written shape index with {len(shape_ids)} shapes to {index_filepath}")
    
    return {
        'shapes_count': len(shapes),
        'files_written': files_written,
        'individual_shapes': len(shape_ids),
        'output_dir': shapes_dir
    }