import os
import tempfile
import zipfile
import requests

FEED_URL = "https://datos.vigo.org/data/transporte/gtfs_vigo.zip"
LAST_ETAG = None


def download_gtfs_to_file(url: str, filename: str, etag: str | None = None):
    headers: dict[str, str] = {}
    if etag:
        headers['If-None-Match'] = etag

    response = requests.get(url, headers=headers)

    if response.status_code == 304:
        print("No new data available.")
        return False

    if response.status_code != 200:
        raise Exception(
            f"Failed to download GTFS data: {response.status_code}")

    with open(filename, 'wb') as file:
        file.write(response.content)

    print(f"Downloaded GTFS data to {filename}")
    return True


def extract_zip_file(zip_filename: str, extract_to: str = '.'):
    with zipfile.ZipFile(zip_filename, 'r') as zip_ref:
        zip_ref.extractall(extract_to)
    print(f"Extracted files to {extract_to}")


def main():
    # Create a directory in the system temporary directory
    temp_dir = tempfile.mkdtemp(prefix='gtfs_vigo_')
    print(f"Temporary directory created: {temp_dir}")

    # Create a temporary zip file in the temporary directory
    zip_filename = os.path.join(temp_dir, 'gtfs_vigo.zip')

    if download_gtfs_to_file(FEED_URL, zip_filename, LAST_ETAG):
        extract_zip_file(zip_filename, temp_dir)

        # Clean up the downloaded zip file
        os.remove(zip_filename)
        print(f"Removed temporary file {zip_filename}")

    os.startfile(temp_dir)


if __name__ == "__main__":
    main()
