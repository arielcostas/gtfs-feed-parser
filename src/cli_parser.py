"""
Shared CLI argument parsing utilities for GTFS report generators.
This module provides common argument parsing patterns to reduce duplication.
"""
import argparse
import os
from typing import Optional


class CommonArgumentParser:
    """Base class for common CLI argument patterns."""
    
    @staticmethod
    def add_feed_source_args(parser: argparse.ArgumentParser, required: bool = True):
        """Add mutually exclusive feed source arguments (--feed-dir or --feed-url)."""
        source_group = parser.add_mutually_exclusive_group(required=required)
        source_group.add_argument('--feed-dir', type=str,
                                 help="Path to the existing feed directory (unzipped)")
        source_group.add_argument('--feed-url', type=str,
                                 help="URL to download the GTFS feed from")
        return source_group

    @staticmethod
    def add_date_args(parser: argparse.ArgumentParser, required: bool = True):
        """Add date selection arguments."""
        date_group = parser.add_mutually_exclusive_group(required=required)
        date_group.add_argument('--all-dates', action='store_true',
                               help='Process all dates in the feed')
        date_group.add_argument('--start-date', type=str,
                               help='Start date (YYYY-MM-DD)')
        
        parser.add_argument('--end-date', type=str,
                           help='End date (YYYY-MM-DD, inclusive)')
        return date_group

    @staticmethod
    def add_output_args(parser: argparse.ArgumentParser):
        """Add common output-related arguments."""
        parser.add_argument('--output-dir', type=str, default="./output/",
                           help='Directory to write reports to (default: ./output/)')
        parser.add_argument('--force-download', action='store_true',
                           help="Force download even if the feed hasn't been modified")
        parser.add_argument('--pretty', action='store_true',
                           help="Pretty-print JSON output")

    @staticmethod
    def validate_common_args(args):
        """Validate common argument combinations."""
        # Date validation
        if hasattr(args, 'start_date') and hasattr(args, 'end_date'):
            if not getattr(args, 'all_dates', False):
                if not getattr(args, 'start_date', None):
                    raise argparse.ArgumentError(None, '--start-date is required unless --all-dates is specified')
                if getattr(args, 'start_date', None) and not getattr(args, 'end_date', None):
                    raise argparse.ArgumentError(None, '--end-date is required when --start-date is specified')

        # Feed source validation
        if hasattr(args, 'feed_dir') and hasattr(args, 'feed_url'):
            if getattr(args, 'feed_dir', None) and getattr(args, 'feed_url', None):
                raise argparse.ArgumentError(None, "Specify either --feed-dir or --feed-url, not both.")
            if not getattr(args, 'feed_dir', None) and not getattr(args, 'feed_url', None):
                raise argparse.ArgumentError(None, "You must specify either --feed-dir or --feed-url.")

        # Feed directory existence validation
        if hasattr(args, 'feed_dir') and getattr(args, 'feed_dir', None):
            if not os.path.exists(args.feed_dir):
                raise argparse.ArgumentError(None, f"Feed directory does not exist: {args.feed_dir}")


def create_service_report_parser() -> argparse.ArgumentParser:
    """Create argument parser for service reports."""
    parser = argparse.ArgumentParser(
        description="Generate GTFS service reports for a date or date range.")
    
    CommonArgumentParser.add_feed_source_args(parser)
    CommonArgumentParser.add_date_args(parser)
    CommonArgumentParser.add_output_args(parser)
    
    parser.add_argument('--service-extractor', type=str, default="default",
                       help="Service extractor to use (default|lcg_muni|vgo_muni)")
    
    return parser


def create_stop_report_parser() -> argparse.ArgumentParser:
    """Create argument parser for stop reports."""
    parser = argparse.ArgumentParser(
        description="Generate stop-based JSON reports for a date or date range.")
    
    CommonArgumentParser.add_feed_source_args(parser)
    CommonArgumentParser.add_date_args(parser)
    CommonArgumentParser.add_output_args(parser)
    
    parser.add_argument('--numeric-stop-code', action='store_true',
                       help="Strip non-numeric characters from stop codes")
    parser.add_argument('--jobs', type=int, default=0,
                       help="Number of parallel processes to use (0 for auto-detection)")
    
    return parser


def create_shape_geojson_parser() -> argparse.ArgumentParser:
    """Create argument parser for GeoJSON shape generation."""
    parser = argparse.ArgumentParser(
        description="Generate GeoJSON files from GTFS shapes data.")
    
    CommonArgumentParser.add_feed_source_args(parser)
    CommonArgumentParser.add_output_args(parser)
    
    return parser


def create_unified_parser() -> argparse.ArgumentParser:
    """Create argument parser for the unified report generator."""
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
    
    CommonArgumentParser.add_feed_source_args(parser)
    CommonArgumentParser.add_date_args(parser)
    CommonArgumentParser.add_output_args(parser)
    
    # Report type arguments
    parser.add_argument('--generate-services', action='store_true',
                       help='Generate HTML service reports')
    parser.add_argument('--generate-stops', action='store_true',
                       help='Generate JSON stop-based reports')
    parser.add_argument('--generate-geojson', action='store_true',
                       help='Generate GeoJSON shape files')
    
    # Service-specific arguments
    parser.add_argument('--service-extractor', type=str, default="default",
                       help="Service extractor to use (default|lcg_muni|vgo_muni)")
    
    # Stop-specific arguments
    parser.add_argument('--numeric-stop-code', action='store_true',
                       help="Strip non-numeric characters from stop codes")
    parser.add_argument('--jobs', type=int, default=0,
                       help="Number of parallel processes to use (0 for auto-detection)")
    
    return parser


def validate_unified_args(args):
    """Validate arguments for the unified report generator."""
    CommonArgumentParser.validate_common_args(args)
    
    if not any([args.generate_services, args.generate_stops, args.generate_geojson]):
        raise argparse.ArgumentError(None, "At least one report type must be specified (--generate-services, --generate-stops, or --generate-geojson)")