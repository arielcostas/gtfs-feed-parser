"""
Functions for handling GTFS service data.
"""
import os
import datetime
from src.logger import get_logger

logger = get_logger("services")

def get_active_services(feed_dir: str, date: str) -> list[str]:
    """
    Get active services for a given date based on the 'calendar.txt' and 'calendar_dates.txt' files.
    
    Args:
        date (str): Date in 'YYYY-MM-DD' format.
        
    Returns:
        list[str]: List of active service IDs for the given date.
        
    Raises:
        ValueError: If the date format is incorrect.
    """
    search_date = date.replace("-", "").replace(":", "").replace("/", "")
    weekday = datetime.datetime.strptime(date, '%Y-%m-%d').weekday()
    active_services: list[str] = []

    logger.debug(f"Looking for active services {search_date=} {weekday=}")

    # Check calendar.txt for services active on the given date
    try:
        with open(os.path.join(feed_dir, 'calendar.txt'), 'r', encoding="utf-8") as calendar_file:
            lines = calendar_file.readlines()
            # If there is only one line (header), log a warning and skip it
            if len(lines) <= 1:
                logger.warning(
                    "calendar.txt file is empty or has only header line, not processing.")
            else:
                logger.debug(
                    "Processing calendar.txt file for active services. NOT IMPLEMENTED.")
                # For each line in the calendar.txt, check if the service has a '1' for the day of the week
                # Implementation would go here
    except FileNotFoundError:
        logger.warning("calendar.txt file not found.")

    # Check calendar_dates.txt for exceptions
    try:
        with open(os.path.join(feed_dir, 'calendar_dates.txt'), 'r', encoding="utf-8") as calendar_dates_file:
            lines = calendar_dates_file.readlines()
            # If there is only one line (header), log a warning and skip it
            if len(lines) <= 1:
                logger.warning(
                    "calendar_dates.txt file is empty or has only header line, not processing.")
                logger.debug("Early-returning empty active services list.")
                return active_services

            # First parse the header, get each column's index
            header = lines[0].strip().split(',')
            try:
                service_id_index = header.index('service_id')
                date_index = header.index('date')
                exception_type_index = header.index('exception_type')
            except ValueError as e:
                logger.error(f"Required column not found in header: {e}")
                return active_services

            logger.debug(f"Header indices: {header}")

            # Now read the rest of the file, find all services where 'date' matches the search_date
            # Start from 1 to skip header
            for idx, line in enumerate(lines[1:], 1):
                parts = line.strip().split(',')
                if len(parts) < len(header):
                    logger.warning(
                        f"Skipping malformed line in calendar_dates.txt line {idx+1}: {line.strip()}")
                    continue

                service_id = parts[service_id_index]
                date_value = parts[date_index]
                exception_type = parts[exception_type_index]

                if date_value == search_date and exception_type == '1':
                    active_services.append(service_id)
                    logger.debug(
                        f"Found active service: {service_id} on date {date_value}")

                if date_value == search_date and exception_type == '2':
                    if service_id in active_services:
                        active_services.remove(service_id)
                        logger.debug(
                            f"Removed service: {service_id} on date {date_value} due to exception type 2")
    except FileNotFoundError:
        logger.warning("calendar_dates.txt file not found.")

    return active_services
