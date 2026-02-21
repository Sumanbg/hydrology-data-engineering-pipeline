def transform_readings(station_id, measure_type, api_data):
    """
    Transform raw API readings into structured tuples
    ready for insertion into the measurements fact table.
    """

    transformed_rows = []

    for item in api_data.get("items", []):
        date_time = item.get("dateTime")
        value = item.get("value")

        # Keep all 10 readings, even if value is None
        transformed_rows.append(
            (
                station_id,
                measure_type,
                date_time,
                float(value) if value is not None else None
            )
        )

    return transformed_rows