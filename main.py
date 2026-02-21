import requests

from database import create_tables, insert_station, insert_measurements
from transform import transform_readings

BASE_URL = "https://environment.data.gov.uk/hydrology/id/measures"

# Chesterfield Park Road Bridge (River Hipper)
STATION_ID = "2e1a5cc0-7eaf-4daf-ab1c-e36e7ea2c61d"
STATION_NAME = "Chesterfield Park Road Bridge"
RIVER_NAME = "River Hipper"
LATITUDE = 53.232983
LONGITUDE = -1.430981

INSTANT_MEASURE = "2e1a5cc0-7eaf-4daf-ab1c-e36e7ea2c61d-level-i-900-m-qualified"
DAILY_MAX_MEASURE = "2e1a5cc0-7eaf-4daf-ab1c-e36e7ea2c61d-level-max-86400-m-qualified"


def get_latest_readings(measure_id):
    url = f"{BASE_URL}/{measure_id}/readings"

    # Fetch more records first
    params = {"_limit": 100}

    response = requests.get(url, params=params)
    response.raise_for_status()

    data = response.json()
    items = data.get("items", [])

    # Sort newest first
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
        STATION_NAME,
        RIVER_NAME,
        LATITUDE,
        LONGITUDE
    )
    insert_station(station_tuple)

    # Step 3: Extract
    instant_data = get_latest_readings(INSTANT_MEASURE)
    daily_data = get_latest_readings(DAILY_MAX_MEASURE)

    # Step 4: Transform
    instant_rows = transform_readings(
        STATION_ID,
        "instantaneous_15min",
        instant_data
    )

    daily_rows = transform_readings(
        STATION_ID,
        "daily_max",
        daily_data
    )

    # Step 5: Load
    insert_measurements(instant_rows)
    insert_measurements(daily_rows)

    print("ETL pipeline completed successfully.")


if __name__ == "__main__":
    main()