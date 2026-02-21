def transform_readings(station_id, measure_type, api_data):
    """
    Transform raw API readings into structured tuples
    ready for insertion into the measurements fact table.
    """

    transformed_rows = []

    for item in api_data.get("items", []):
        date_time = item.get("dateTime")
        value = item.get("value")

        # Only keep valid numeric values
        if value is not None:
            transformed_rows.append(
                (
                    station_id,
                    measure_type,
                    date_time,
                    float(value)
                )
            )

    return transformed_rows