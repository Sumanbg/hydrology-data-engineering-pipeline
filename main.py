import requests

from database import create_tables, insert_station, insert_measurements
from transform import transform_readings

BASE_URL = "https://environment.data.gov.uk/hydrology/id/measures"

# Chesterfield (River Hipper) station GUID
STATION_ID = "2320611b-8413-4964-a538-04fe9d43d09e"
STATION_LABEL = "Chesterfield"
RIVER_NAME = "River Hipper"
LATITUDE = 53.231113
LONGITUDE = -1.454091

INSTANT_MEASURE = "2320611b-8413-4964-a538-04fe9d43d09e-level-i-900-m-qualified"
DAILY_MAX_MEASURE = "2320611b-8413-4964-a538-04fe9d43d09e-level-max-86400-m-qualified"


def get_latest_readings(measure_id):
    url = f"{BASE_URL}/{measure_id}/readings"

    # Fetch more data first
    params = {"_limit": 100}

    response = requests.get(url, params=params)
    response.raise_for_status()

    data = response.json()
    items = data.get("items", [])

    # Sort by dateTime descending (newest first)
    sorted_items = sorted(
        items,
        key=lambda x: x.get("dateTime", ""),
        reverse=True
    )

    # Keep only latest 10
    data["items"] = sorted_items[:10]

    return data


def main():
    print("Starting ETL pipeline...")

    # Step 1: Create tables
    create_tables()

    # Step 2: Insert station dimension
    station_tuple = (
        STATION_ID,
        STATION_LABEL,
        RIVER_NAME,
        LATITUDE,
        LONGITUDE
    )
    insert_station(station_tuple)

    # Step 3: Extract
    instant_data = get_latest_readings(INSTANT_MEASURE)
    daily_data = get_latest_readings(DAILY_MAX_MEASURE)

    # Step 4: Transform
    instant_rows = transform_readings(STATION_ID, "instantaneous_15min", instant_data)
    daily_rows = transform_readings(STATION_ID, "daily_max", daily_data)

    # Step 5: Load
    insert_measurements(instant_rows)
    insert_measurements(daily_rows)

    print("ETL pipeline completed successfully.")


if __name__ == "__main__":
    main()