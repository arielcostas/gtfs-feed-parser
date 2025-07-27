#!/usr/bin/env python3
"""
Generate stop-based JSON reports from GTFS data.
"""
import sys
import traceback

from src.cli_parser import create_stop_report_parser, CommonArgumentParser
from src.orchestrators import generate_stop_reports_orchestrator, prepare_feed_directory
from src.logger import get_logger

logger = get_logger("stop_report")


def main():
    """Main function for stop report generation."""
    try:
        # Parse arguments
        parser = create_stop_report_parser()
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
        
        # Generate stop reports
        result = generate_stop_reports_orchestrator(
            feed_dir=feed_dir,
            output_dir=args.output_dir,
            all_dates_flag=args.all_dates,
            start_date=getattr(args, 'start_date', None),
            end_date=getattr(args, 'end_date', None),
            numeric_stop_code=args.numeric_stop_code,
            jobs=args.jobs,
            pretty=args.pretty
        )
        
        logger.info(f"Stop report generation completed successfully:")
        logger.info(f"  - Generated reports for {len(result['generated_dates'])} dates")
        logger.info(f"  - Processed {result['total_stops']} stop records")
        
    except Exception as e:
        logger.error(f"Stop report generation failed: {e}")
        logger.debug(traceback.format_exc())
        sys.exit(1)


if __name__ == "__main__":
    main()