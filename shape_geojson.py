#!/usr/bin/env python3
"""
Generate GeoJSON files from GTFS shapes data.
This script processes the shapes.txt file and creates GeoJSON files for use in web maps.
"""
import sys
import traceback

from src.cli_parser import create_shape_geojson_parser, CommonArgumentParser
from src.orchestrators import generate_geojson_reports_orchestrator, prepare_feed_directory
from src.logger import get_logger

logger = get_logger("shape_geojson")


def main():
    """Main function for GeoJSON shape generation."""
    try:
        # Parse arguments
        parser = create_shape_geojson_parser()
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
        
        # Generate GeoJSON reports
        result = generate_geojson_reports_orchestrator(
            feed_dir=feed_dir,
            output_dir=args.output_dir,
            pretty=args.pretty
        )
        
        logger.info(f"GeoJSON generation completed successfully:")
        logger.info(f"  - Processed {result['shapes_count']} shapes")
        logger.info(f"  - Generated {result['files_written']} files")
        logger.info(f"  - Created {result['individual_shapes']} individual shape files")
        
    except Exception as e:
        logger.error(f"GeoJSON generation failed: {e}")
        logger.debug(traceback.format_exc())
        sys.exit(1)


if __name__ == "__main__":
    main()
