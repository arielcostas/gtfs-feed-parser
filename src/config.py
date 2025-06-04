"""
Configuration settings for the GTFS application.
"""
import os

# Base paths
BASE_DIR = os.path.dirname(os.path.dirname(__file__))

# Constants (in the future they will be CLI options or a configuration file or whatever).
# For now, they are hardcoded here.
FEED_DIR = os.path.join(BASE_DIR, 'feed')
OUTPUT_DIR = os.path.join(BASE_DIR, 'output')
# DATE = '2025-06-03'  # No longer used, now set via CLI
