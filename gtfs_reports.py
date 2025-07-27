#!/usr/bin/env python3
"""
Unified GTFS Report Generator
Generates service reports, stop reports, and GeoJSON files from GTFS data.
"""
import os
import sys
import argparse
import shutil
import traceback
from datetime import datetime as dt
from typing import List, Optional

from src.download import download_feed_from_url
from src.logger import get_logger
from src.common import get_all_feed_dates, date_range

logger = get_logger("gtfs_reports")


def parse_args():
    parser = argparse.ArgumentParser(
        description="Generate various reports from GTFS data.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Report Types:
  --generate-services    Generate HTML service reports
  --generate-stops       Generate JSON stop-based reports
  --generate-geojson     Generate GeoJSON shape files

Examples:
  # Generate all reports for all dates
  python gtfs_reports.py --feed-dir ./feed --all-dates --generate-services --generate-stops --generate-geojson

  # Generate only service reports for a specific date range
  python gtfs_reports.py --feed-url http://example.com/gtfs.zip --start-date 2025-07-24 --end-date 2025-07-30 --generate-services

  # Generate shapes only from local feed
  python gtfs_reports.py --feed-dir ./feed --generate-geojson
        """)
    
    # Feed source arguments
    source_group = parser.add_mutually_exclusive_group(required=True)
    source_group.add_argument('--feed-dir', type=str,
                             help="Path to the existing feed directory (unzipped)")
    source_group.add_argument('--feed-url', type=str,
                             help="URL to download the GTFS feed from")
    
    # Date selection arguments
    date_group = parser.add_mutually_exclusive_group(required=True)
    date_group.add_argument('--all-dates', action='store_true',
                           help='Process all dates in the feed')
    date_group.add_argument('--start-date', type=str,
                           help='Start date (YYYY-MM-DD)')
    
    parser.add_argument('--end-date', type=str,
                       help='End date (YYYY-MM-DD, inclusive). Required if --start-date is used.')
    
    # Report type arguments
    parser.add_argument('--generate-services', action='store_true',
                       help='Generate HTML service reports')
    parser.add_argument('--generate-stops', action='store_true',
                       help='Generate JSON stop-based reports')
    parser.add_argument('--generate-geojson', action='store_true',
                       help='Generate GeoJSON shape files')
    
    # Common output arguments
    parser.add_argument('--output-dir', type=str, default="./output/",
                       help='Directory to write reports to (default: ./output/)')
    parser.add_argument('--pretty', action='store_true',
                       help="Pretty-print JSON output")
    parser.add_argument('--force-download', action='store_true',
                       help="Force download even if the feed hasn't been modified")
    
    # Service-specific arguments
    parser.add_argument('--service-extractor', type=str, default="default",
                       help="Service extractor to use (default|lcg_muni|vgo_muni)")
    
    # Stop-specific arguments
    parser.add_argument('--numeric-stop-code', action='store_true',
                       help="Strip non-numeric characters from stop codes")
    parser.add_argument('--jobs', type=int, default=0,
                       help="Number of parallel processes to use (0 for auto-detection)")
    
    args = parser.parse_args()
    
    # Validation
    if args.start_date and not args.end_date:
        parser.error('--end-date is required when --start-date is specified')
    
    if args.feed_dir and not os.path.exists(args.feed_dir):
        parser.error(f"Feed directory does not exist: {args.feed_dir}")
    
    if not any([args.generate_services, args.generate_stops, args.generate_geojson]):
        parser.error("At least one report type must be specified (--generate-services, --generate-stops, or --generate-geojson)")
    
    return args


def generate_service_reports(feed_dir: str, date_list: List[str], args):
    """Generate HTML service reports using the existing service_report logic."""
    logger.info("Generating service reports...")
    
    # Import here to avoid circular imports
    from service_report import main as service_main
    import service_report
    
    # Temporarily modify sys.argv to pass arguments to service_report
    original_argv = sys.argv.copy()
    
    try:
        sys.argv = ['service_report.py']
        sys.argv.extend(['--feed-dir', feed_dir])
        sys.argv.extend(['--output-dir', args.output_dir])
        
        if args.all_dates:
            sys.argv.append('--all-dates')
        else:
            sys.argv.extend(['--start-date', date_list[0]])
            sys.argv.extend(['--end-date', date_list[-1]])
        
        if args.service_extractor != 'default':
            sys.argv.extend(['--service-extractor', args.service_extractor])
        
        # Call the service report main function
        service_main()
        logger.info("Service reports generated successfully")
        
    finally:
        sys.argv = original_argv


def generate_stop_reports(feed_dir: str, date_list: List[str], args):
    """Generate JSON stop reports using the existing stop_report logic."""
    logger.info("Generating stop reports...")
    
    # Import the functions we need from stop_report
    from stop_report import process_date, write_index_json
    from multiprocessing import Pool, cpu_count
    
    # Determine number of jobs for parallel processing
    jobs = args.jobs
    if jobs <= 0:
        jobs = cpu_count()
    
    logger.info(f"Processing {len(date_list)} dates with {jobs} jobs")
    
    # Dictionary to store summary data for index files
    all_stops_summary = {}
    
    if jobs > 1 and len(date_list) > 1:
        # Parallel processing
        try:
            with Pool(processes=jobs) as pool:
                tasks = [
                    (feed_dir, date, args.output_dir,
                     args.numeric_stop_code, args.pretty)
                    for date in date_list
                ]
                results = pool.starmap(process_date, tasks)
                
                for date, stop_summary in results:
                    all_stops_summary[date] = stop_summary
        except Exception as e:
            logger.error(f"Error in parallel processing: {e}")
            # Fallback to sequential processing
            for date in date_list:
                _, stop_summary = process_date(
                    feed_dir, date, args.output_dir, args.numeric_stop_code, args.pretty)
                all_stops_summary[date] = stop_summary
    else:
        # Sequential processing
        for date in date_list:
            _, stop_summary = process_date(
                feed_dir, date, args.output_dir, args.numeric_stop_code, args.pretty)
            all_stops_summary[date] = stop_summary
    
    # Write index files
    write_index_json(args.output_dir, all_stops_summary, args.pretty)
    logger.info("Stop reports generated successfully")


def generate_geojson_files(feed_dir: str, args):
    """Generate GeoJSON shape files using the existing shape_geojson logic."""
    logger.info("Generating GeoJSON shape files...")
    
    from src.shapes import load_shapes, shapes_to_geojson
    from shape_geojson import (
        write_shapes_geojson, 
        write_individual_shape_geojson, 
        write_shape_index
    )
    
    shapes_data = load_shapes(feed_dir)
    
    if not shapes_data:
        logger.warning("No shapes data found in feed.")
        return
    
    logger.info(f"Converting {len(shapes_data)} shapes to GeoJSON...")
    
    # Convert all shapes to GeoJSON
    all_shapes_geojson = shapes_to_geojson(shapes_data)
    
    # Write the combined GeoJSON file
    write_shapes_geojson(args.output_dir, all_shapes_geojson, args.pretty)
    
    # Write individual shape files
    shape_ids = []
    for shape_id, shape_points in shapes_data.items():
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
        
        write_individual_shape_geojson(args.output_dir, shape_id, individual_geojson, args.pretty)
        shape_ids.append(shape_id)
    
    # Write the shapes index
    write_shape_index(args.output_dir, shape_ids, args.pretty)
    
    logger.info("GeoJSON shape files generated successfully")


def main():
    args = parse_args()
    
    # Handle feed source (download or local directory)
    if args.feed_url:
        logger.info(f"Downloading GTFS feed from {args.feed_url}...")
        feed_dir = download_feed_from_url(args.feed_url, args.output_dir, args.force_download)
        if feed_dir is None:
            logger.info("Download was skipped (feed not modified). Exiting.")
            return
        cleanup_feed_dir = True
    else:
        feed_dir = args.feed_dir
        cleanup_feed_dir = False
    
    try:
        # Determine date list
        if args.all_dates:
            all_dates = get_all_feed_dates(feed_dir)
            if not all_dates:
                logger.error('No valid dates found in feed.')
                return
            date_list = all_dates
        else:
            start_date = args.start_date
            end_date = args.end_date or args.start_date
            date_list = list(date_range(start_date, end_date))
        
        if not date_list:
            logger.error("No valid dates to process.")
            return
        
        logger.info(f"Processing {len(date_list)} dates: {date_list[0]} to {date_list[-1]}")
        
        # Generate requested reports
        reports_generated = []
        
        if args.generate_services:
            generate_service_reports(feed_dir, date_list, args)
            reports_generated.append("service reports")
        
        if args.generate_stops:
            generate_stop_reports(feed_dir, date_list, args)
            reports_generated.append("stop reports")
        
        if args.generate_geojson:
            generate_geojson_files(feed_dir, args)
            reports_generated.append("GeoJSON files")
        
        logger.info(f"Successfully generated: {', '.join(reports_generated)}")
        
    finally:
        # Cleanup temporary feed directory if downloaded
        if cleanup_feed_dir and os.path.exists(feed_dir):
            logger.debug("Cleaning up temporary feed directory...")
            shutil.rmtree(feed_dir)
            logger.info(f"Removed temporary feed directory: {feed_dir}")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.critical(f"An unexpected error occurred: {e}", exc_info=True)
        traceback.print_exc()
        sys.exit(1)
