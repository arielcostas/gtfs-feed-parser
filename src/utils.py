"""
Common utility functions used across the GTFS report generators.
"""
from typing import Dict, Any


def normalize_stop_code(stop_code: str, numeric_only: bool = False) -> str:
    """Normalize stop code based on formatting requirements."""
    if not stop_code:
        return ""
    
    if numeric_only:
        # First strip non-numeric characters
        numeric_code = ''.join(c for c in stop_code if c.isdigit())
        # Then convert to integer and back to string to remove leading zeros
        return str(int(numeric_code)) if numeric_code else ""
    
    return stop_code


def create_stop_id_to_code_mapping(stops: Dict[str, Any], numeric_stop_code: bool = False) -> Dict[str, str]:
    """Create a reverse lookup from stop_id to stop_code."""
    stop_id_to_code = {}
    for stop_id, stop in stops.items():
        if stop.stop_code:
            stop_code = normalize_stop_code(stop.stop_code, numeric_stop_code)
            if stop_code:  # Only add if not empty after normalization
                stop_id_to_code[stop_id] = stop_code
    return stop_id_to_code


def time_to_seconds(time_str: str) -> int:
    """Convert HH:MM:SS to seconds since midnight."""
    if not time_str:
        return 0
    
    parts = time_str.split(':')
    if len(parts) != 3:
        return 0
    
    try:
        hours, minutes, seconds = map(int, parts)
        return hours * 3600 + minutes * 60 + seconds
    except ValueError:
        return 0


def normalize_gtfs_time(time_str: str) -> tuple[str, bool]:
    """
    Normalize GTFS time and determine if it's a next-day trip.
    
    Args:
        time_str: Time string in HH:MM:SS format (may be >= 24:00:00)
        
    Returns:
        Tuple of (normalized_time_str, is_next_day)
        - normalized_time_str: Time adjusted to 00:00:00-23:59:59 range
        - is_next_day: True if the original time was >= 24:00:00
    """
    if not time_str:
        return time_str, False
    
    parts = time_str.split(':')
    if len(parts) != 3:
        return time_str, False
    
    try:
        hours, minutes, seconds = map(int, parts)
        if hours >= 24:
            # Next day trip - subtract 24 hours
            adjusted_hours = hours - 24
            normalized_time = f"{adjusted_hours:02d}:{minutes:02d}:{seconds:02d}"
            return normalized_time, True
        else:
            return time_str, False
    except ValueError:
        return time_str, False


def seconds_to_time(seconds: int) -> str:
    """Convert seconds since midnight to HH:MM:SS format."""
    hours = seconds // 3600
    minutes = (seconds % 3600) // 60
    secs = seconds % 60
    return f"{hours:02d}:{minutes:02d}:{secs:02d}"


def safe_color_hex(color: str) -> str:
    """Ensure color is a valid 6-character hex code."""
    if not color:
        return "cccccc"
    
    # Remove # if present
    color = color.lstrip('#')
    
    # Ensure it's 6 characters and hex
    if len(color) == 6 and all(c in '0123456789abcdefABCDEF' for c in color):
        return color.lower()
    
    return "cccccc"


def format_distance(distance_km: float) -> str:
    """Format distance in kilometers with consistent formatting."""
    if distance_km <= 0:
        return None
    return f"{distance_km:,.2f}"


def format_count(count: int) -> str:
    """Format count with consistent number formatting."""
    return f"{count:,}"