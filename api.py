import requests

BASE_URL = "https://environment.data.gov.uk/hydrology/id/measures"

INSTANT_MEASURE = "2320611b-8413-4964-a538-04fe9d43d09e-level-i-900-m-qualified"
DAILY_MAX_MEASURE = "2320611b-8413-4964-a538-04fe9d43d09e-level-max-86400-m-qualified"


def get_latest_readings(measure_id):
    """
    Fetch the 10 most recent readings for a given measure ID.
    """
    url = f"{BASE_URL}/{measure_id}/readings"
    params = {
        "_limit": 10,
        "_sorted": "-dateTime"
    }

    response = requests.get(url, params=params)
    response.raise_for_status()
    return response.json()


def print_readings(title, data):
    """
    Safely print readings while handling missing values.
    """
    print(f"\n{title}")

    for item in data.get("items", []):
        date_time = item.get("dateTime")
        value = item.get("value")

        if value is not None:
            print(date_time, value)
        else:
            print(date_time, "Missing value")


if __name__ == "__main__":
    print("Fetching Chesterfield (River Hipper) data...")

    instant_data = get_latest_readings(INSTANT_MEASURE)
    print_readings("Instantaneous (15min) Readings:", instant_data)

    daily_data = get_latest_readings(DAILY_MAX_MEASURE)
    print_readings("Daily Max Readings:", daily_data)