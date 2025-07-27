#!/usr/bin/env python3
"""
Generate GeoJSON files from GTFS shapes data.
This script processes the shapes.txt file and creates GeoJSON files for use in web maps.
"""
import os
import json
import argparse
import shutil
from typing import Dict, List
from src.download import download_feed_from_url
from src.logger import get_logger
from src.shapes import load_shapes, shapes_to_geojson

logger = get_logger("shape_geojson")


def parse_args():
    parser = argparse.ArgumentParser(
        description="Generate GeoJSON files from GTFS shapes data.")
    parser.add_argument('--output-dir', type=str, default="./output/",
                        help='Directory to write GeoJSON files to (default: ./output/)')
    parser.add_argument('--feed-dir', type=str,
                        help="Path to the feed directory")
    parser.add_argument('--feed-url', type=str,
                        help="URL to download the GTFS feed from (if not using local feed directory)")
    parser.add_argument('--pretty', action='store_true',
                        help="Pretty-print JSON output (default is compact JSON without spaces)")
    parser.add_argument('--force-download', action='store_true', 
                        help="Force download even if the feed hasn't been modified (only applies when using --feed-url)")
    args = parser.parse_args()

    if args.feed_dir and args.feed_url:
        parser.error("Specify either --feed-dir or --feed-url, not both.")
    if not args.feed_dir and not args.feed_url:
        parser.error(
            "You must specify either a path to the existing feed (unzipped) or a URL to download the GTFS feed from.")
    if args.feed_dir and not os.path.exists(args.feed_dir):
        parser.error(f"Feed directory does not exist: {args.feed_dir}")

    return args


def write_shapes_geojson(output_dir: str, shapes: Dict, pretty: bool):
    """Write the shapes GeoJSON to a file."""
    # Create the shapes directory
    shapes_dir = os.path.join(output_dir, "shapes")
    os.makedirs(shapes_dir, exist_ok=True)

    # Create the GeoJSON file
    file_path = os.path.join(shapes_dir, "all_shapes.geojson")

    with open(file_path, 'w', encoding='utf-8') as f:
        json.dump(shapes, f, indent=2 if pretty else None, separators=(
            ",", ":") if not pretty else None, ensure_ascii=False)

    logger.info(f"Written {len(shapes.get('features', []))} shapes to {file_path}")


def write_individual_shape_geojson(output_dir: str, shape_id: str, shape_geojson: Dict, pretty: bool):
    """Write individual shape GeoJSON to a file."""
    # Create the shapes directory
    shapes_dir = os.path.join(output_dir, "shapes")
    os.makedirs(shapes_dir, exist_ok=True)

    # Create the individual GeoJSON file
    file_path = os.path.join(shapes_dir, f"{shape_id}.geojson")

    with open(file_path, 'w', encoding='utf-8') as f:
        json.dump(shape_geojson, f, indent=2 if pretty else None, separators=(
            ",", ":") if not pretty else None, ensure_ascii=False)


def write_shape_index(output_dir: str, shape_ids: List[str], pretty: bool):
    """Write an index of available shapes."""
    # Create the shapes directory
    shapes_dir = os.path.join(output_dir, "shapes")
    os.makedirs(shapes_dir, exist_ok=True)

    # Create the index file
    file_path = os.path.join(shapes_dir, "index.json")
    
    index_data = {
        "shapes": sorted(shape_ids),
        "count": len(shape_ids)
    }

    with open(file_path, 'w', encoding='utf-8') as f:
        json.dump(index_data, f, indent=2 if pretty else None, separators=(
            ",", ":") if not pretty else None, ensure_ascii=False)

    logger.info(f"Written shape index with {len(shape_ids)} shapes to {file_path}")


def main():
    args = parse_args()
    output_dir = args.output_dir
    feed_url = args.feed_url
    pretty = args.pretty

    if not feed_url:
        feed_dir = args.feed_dir
    else:
        logger.info(f"Downloading GTFS feed from {feed_url}...")
        feed_dir = download_feed_from_url(feed_url, output_dir, args.force_download)
        if feed_dir is None:
            logger.info("Download was skipped (feed not modified). Exiting.")
            return

    logger.info("Loading shapes from GTFS feed...")
    shapes_data = load_shapes(feed_dir)
    
    if not shapes_data:
        logger.warning("No shapes data found in feed.")
        return

    logger.info(f"Converting {len(shapes_data)} shapes to GeoJSON...")
    
    # Convert all shapes to GeoJSON
    all_shapes_geojson = shapes_to_geojson(shapes_data)
    
    # Write the combined GeoJSON file
    write_shapes_geojson(output_dir, all_shapes_geojson, pretty)
    
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
        
        write_individual_shape_geojson(output_dir, shape_id, individual_geojson, pretty)
        shape_ids.append(shape_id)
    
    # Write the shapes index
    write_shape_index(output_dir, shape_ids, pretty)
    
    logger.info("Shape GeoJSON generation completed.")

    if feed_url:
        if os.path.exists(feed_dir):
            shutil.rmtree(feed_dir)
            logger.info(f"Removed temporary feed directory: {feed_dir}")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger = get_logger("shape_geojson")
        logger.critical(f"An unexpected error occurred: {e}", exc_info=True)
        import traceback
        traceback.print_exc()
        import sys
        sys.exit(1)
