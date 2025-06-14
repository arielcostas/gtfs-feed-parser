import os
import tempfile
import zipfile
import requests

from src.logger import get_logger

logger = get_logger("download")

def download_feed_from_url(feed_url: str) -> str:

    # Create a directory in the system temporary directory
    temp_dir = tempfile.mkdtemp(prefix='gtfs_vigo_')
    logger.debug(f"Temporary directory created: {temp_dir}")

    # Create a temporary zip file in the temporary directory
    zip_filename = os.path.join(temp_dir, 'gtfs_vigo.zip')

    headers = {}
    response = requests.get(feed_url, headers=headers)

    if response.status_code != 200:
        raise Exception(f"Failed to download GTFS data: {response.status_code}")

    with open(zip_filename, 'wb') as file:
        file.write(response.content)

    logger.debug(f"Downloaded GTFS data to {zip_filename}")

    # Extract the zip file
    with zipfile.ZipFile(zip_filename, 'r') as zip_ref:
        zip_ref.extractall(temp_dir)
    
    logger.debug(f"Extracted files to {temp_dir}")

    # Clean up the downloaded zip file
    os.remove(zip_filename)
    logger.debug(f"Removed temporary file {zip_filename}")

    logger.info(f"GTFS feed downloaded from {feed_url} and extracted to {temp_dir}")

    return temp_dir