#!/usr/bin/env python3
"""
Main script for generating service reports from GTFS data.
"""
import sys
import traceback

from src.cli_parser import create_service_report_parser, CommonArgumentParser
from src.orchestrators import generate_service_reports_orchestrator, prepare_feed_directory
from src.logger import get_logger

logger = get_logger("service_report")


def main():
    """Main function for service report generation."""
    try:
        # Parse arguments
        parser = create_service_report_parser()
        args = parser.parse_args()
        
        # Validate arguments
        CommonArgumentParser.validate_common_args(args)
        
        # Prepare feed directory
        feed_dir = prepare_feed_directory(
            args.feed_dir, args.feed_url, args.output_dir, args.force_download
        )
        
        if feed_dir is None:
            logger.info("Download was skipped (feed not modified). Exiting.")
            return
        
        # Generate service reports
        result = generate_service_reports_orchestrator(
            feed_dir=feed_dir,
            output_dir=args.output_dir,
            all_dates_flag=args.all_dates,
            start_date=getattr(args, 'start_date', None),
            end_date=getattr(args, 'end_date', None),
            service_extractor=args.service_extractor
        )
        
        logger.info(f"Service report generation completed successfully:")
        logger.info(f"  - Generated reports for {len(result['generated_dates'])} dates")
        logger.info(f"  - Processed {result['total_services']} services")
        logger.info(f"  - Processed {result['total_trips']} trips")
        
    except Exception as e:
        logger.error(f"Service report generation failed: {e}")
        logger.debug(traceback.format_exc())
        sys.exit(1)


if __name__ == "__main__":
    main()