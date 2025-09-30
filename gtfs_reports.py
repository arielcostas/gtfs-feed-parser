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
                    service_extractor=args.service_extractor,
                    rolling_dates_config_path=getattr(args, 'rolling_dates_config', None)
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
                    pretty=args.pretty,
                    rolling_dates_config_path=getattr(args, 'rolling_dates_config', None)
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