"""
Performance utilities for optimizing GTFS processing.
"""
import time
import functools
from typing import Dict, Any, List
from src.logger import get_logger

logger = get_logger("performance")

def timer_decorator(func):
    """Decorator to time function execution."""
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.perf_counter()
        result = func(*args, **kwargs)
        end_time = time.perf_counter()
        logger.info(f"{func.__name__} took {end_time - start_time:.3f} seconds")
        return result
    return wrapper

def batch_process_items(items: List[Any], batch_size: int = 1000):
    """Generator to process items in batches for memory efficiency."""
    for i in range(0, len(items), batch_size):
        yield items[i:i + batch_size]

def optimize_stop_lookups(stops: Dict[str, Any]) -> Dict[str, Any]:
    """Pre-process stops data for faster lookups."""
    # Create additional lookup indices if needed
    stop_code_to_id = {}
    for stop_id, stop in stops.items():
        if hasattr(stop, 'stop_code') and stop.stop_code:
            stop_code_to_id[stop.stop_code] = stop_id
    
    return {
        'stops': stops,
        'stop_code_to_id': stop_code_to_id
    }
