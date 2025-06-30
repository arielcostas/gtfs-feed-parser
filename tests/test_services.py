import os
import tempfile
from src.services import get_active_services

def write_file(path, content):
    with open(path, 'w', encoding='utf-8') as f:
        f.write(content)


def test_get_active_services_basic(tmp_path):
    # Setup temporary feed directory
    feed_dir = tmp_path / "feed"
    feed_dir.mkdir()

    # Create calendar.txt with header and two services active on Monday (2025-06-23 is Monday)
    calendar_content = (
        "service_id,monday,tuesday,wednesday,thursday,friday,saturday,sunday,start_date,end_date\n"
        "S1,1,0,0,0,0,0,0,20250601,20250630\n"
        "S2,1,1,1,1,1,1,1,20250601,20250630\n"
    )
    write_file(feed_dir / 'calendar.txt', calendar_content)

    # Create calendar_dates.txt overriding S2 off on 2025-06-23 and S3 added
    calendar_dates_content = (
        "service_id,date,exception_type\n"
        "S2,20250623,2\n"
        "S3,20250623,1\n"
    )
    write_file(feed_dir / 'calendar_dates.txt', calendar_dates_content)

    # Invoke function for 2025-06-23
    result = get_active_services(str(feed_dir), "2025-06-23")
    # Expect S1 (from calendar), remove S2 (exception), add S3 (exception)
    assert set(result) == {"S1", "S3"}
