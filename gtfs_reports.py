#!/usr/bin/env python3
"""
Unified GTFS Report Generator
Generates service reports, stop reports, and GeoJSON files from GTFS data.
"""
import sys
import traceback
from typing import List, Optional

from src.cli_parser import create_unified_parser, validate_unified_args
from src.orchestrators import (
    generate_service_reports_orchestrator,
    generate_stop_reports_orchestrator, 
    generate_geojson_reports_orchestrator,
    prepare_feed_directory,
    get_date_list
)
from src.logger import get_logger

logger = get_logger("gtfs_reports")


def main():
    """Main function for unified report generation."""
    try:
        # Parse arguments
        parser = create_unified_parser()
        args = parser.parse_args()
        
        # Validate arguments
        validate_unified_args(args)
        
        # Prepare feed directory
        feed_dir = prepare_feed_directory(
            args.feed_dir, args.feed_url, args.output_dir, args.force_download
        )
        
        if feed_dir is None:
            logger.info("Download was skipped (feed not modified). Exiting.")
            return
        
        # Get date list for date-dependent reports
        date_list = []
        if args.generate_services or args.generate_stops:
            date_list = get_date_list(
                all_dates=args.all_dates,
                start_date=getattr(args, 'start_date', None),
                end_date=getattr(args, 'end_date', None),
                feed_dir=feed_dir
            )
        
        # Generate reports based on requested types
        results = {}
        
        if args.generate_services:
            logger.info("=== Generating Service Reports ===")
            try:
                results['services'] = generate_service_reports_orchestrator(
                    feed_dir=feed_dir,
                    output_dir=args.output_dir,
                    all_dates_flag=args.all_dates,
                    start_date=getattr(args, 'start_date', None),
                    end_date=getattr(args, 'end_date', None),
                    service_extractor=args.service_extractor
                )
                logger.info(f"Service reports completed: {len(results['services']['generated_dates'])} dates processed")
            except Exception as e:
                logger.error(f"Service report generation failed: {e}")
                results['services'] = {'error': str(e)}
        
        if args.generate_stops:
            logger.info("=== Generating Stop Reports ===")
            try:
                results['stops'] = generate_stop_reports_orchestrator(
                    feed_dir=feed_dir,
                    output_dir=args.output_dir,
                    all_dates_flag=args.all_dates,
                    start_date=getattr(args, 'start_date', None),
                    end_date=getattr(args, 'end_date', None),
                    numeric_stop_code=args.numeric_stop_code,
                    jobs=args.jobs,
                    pretty=args.pretty
                )
                logger.info(f"Stop reports completed: {len(results['stops']['generated_dates'])} dates processed")
            except Exception as e:
                logger.error(f"Stop report generation failed: {e}")
                results['stops'] = {'error': str(e)}
        
        if args.generate_geojson:
            logger.info("=== Generating GeoJSON Reports ===")
            try:
                results['geojson'] = generate_geojson_reports_orchestrator(
                    feed_dir=feed_dir,
                    output_dir=args.output_dir,
                    pretty=args.pretty
                )
                logger.info(f"GeoJSON reports completed: {results['geojson']['shapes_count']} shapes processed")
            except Exception as e:
                logger.error(f"GeoJSON generation failed: {e}")
                results['geojson'] = {'error': str(e)}
        
        # Summary
        logger.info("=== Generation Summary ===")
        for report_type, result in results.items():
            if 'error' in result:
                logger.error(f"{report_type.capitalize()}: FAILED - {result['error']}")
            else:
                logger.info(f"{report_type.capitalize()}: SUCCESS")
        
        # Exit with error if any generation failed
        if any('error' in result for result in results.values()):
            sys.exit(1)
        
        logger.info("All requested reports generated successfully!")
        
    except Exception as e:
        logger.error(f"Report generation failed: {e}")
        logger.debug(traceback.format_exc())
        sys.exit(1)


if __name__ == "__main__":
    main()
    
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
            shutil.rmtree(feed_dir)
            logger.info(f"Removed temporary feed directory: {feed_dir}")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.critical(f"An unexpected error occurred: {e}", exc_info=True)
        traceback.print_exc()
        sys.exit(1)
