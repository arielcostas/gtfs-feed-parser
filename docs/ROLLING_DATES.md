# Rolling Dates Feature

## Overview

The **Rolling Dates** feature allows you to extend GTFS feed coverage by mapping future dates (not included in the feed) to equivalent dates that exist in the feed. This is useful when you need to generate reports for dates beyond the feed's validity period but want to use data from similar past dates as a reference.

## How It Works

When a rolling date mapping is active:
1. The program uses data from the **source date** (a date that exists in the feed)
2. It generates reports for the **target date** (the requested future date)
3. Visual notices are displayed in HTML reports and metadata is included in JSON outputs to inform users that the data is from a different date

## Configuration

### Creating a Rolling Dates Configuration File

Create a JSON file with date mappings in the format:

```json
{
  "target_date": "source_date",
  "2025-09-30": "2025-09-24",
  "2025-10-01": "2025-09-25"
}
```

- **Keys** (target dates): Dates you want to generate reports for (YYYY-MM-DD format)
- **Values** (source dates): Dates in the feed that should be used as data sources (YYYY-MM-DD format)

**Example**: `rolling_dates.example.json` in the project root

### Using the Configuration

Add the `--rolling-dates-config` argument when running report generation:

```bash
# Generate service reports with rolling dates
python gtfs_reports.py \
  --feed-dir ./feed \
  --start-date 2025-09-30 \
  --end-date 2025-10-06 \
  --rolling-dates-config rolling_dates.json \
  --generate-services

# Generate stop reports with rolling dates
python gtfs_reports.py \
  --feed-dir ./feed \
  --start-date 2025-09-30 \
  --end-date 2025-10-06 \
  --rolling-dates-config rolling_dates.json \
  --generate-stops
```

## Output Behavior

### HTML Service Reports

When viewing a service or day index page for a rolling date, you'll see a notice like:

```
ℹ️ Aviso: Los datos mostrados para 2025-09-30 provienen de 2025-09-24, 
ya que la fecha solicitada no está disponible en el feed GTFS.
```

### JSON Stop Reports

Each arrival in the JSON output includes a `_rolling_date` field when applicable:

```json
{
  "line": {...},
  "trip": {...},
  "arrival_time": "08:30:00",
  "_rolling_date": {
    "source_date": "2025-09-24",
    "target_date": "2025-09-30"
  }
}
```

## Best Practices

1. **Choose Equivalent Days**: Map dates to the same day of the week for more accurate service patterns
   - Example: Map Monday 2025-09-30 to Monday 2025-09-24

2. **Document Your Mappings**: Keep track of why certain mappings were chosen (e.g., similar service patterns, holidays)

3. **Validate Feed Coverage**: Only use rolling dates for dates truly not in the feed; the program will use actual feed data if available

4. **Test Thoroughly**: Verify that the source dates provide representative service patterns for the target dates

## Technical Details

- Rolling date mappings are loaded once at the start of report generation
- Date validation ensures all dates are in YYYY-MM-DD format
- If a target date exists in the feed, the actual feed data takes precedence over rolling mappings
- The feature works with both service HTML reports and stop JSON reports
- Multiprocessing support is maintained for stop report generation

## Error Handling

The program will raise errors for:
- Missing configuration file (if path is specified)
- Invalid JSON format
- Invalid date formats (must be YYYY-MM-DD)
- File read/write permissions issues

## Limitations

- Rolling dates do not modify the underlying GTFS feed
- The feature is meant for reporting purposes only
- Notices are displayed to prevent confusion about data sources
