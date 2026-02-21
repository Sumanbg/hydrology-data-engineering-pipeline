import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from transform import transform_readings


def test_transform_readings():
    sample_data = {
        "items": [
            {"dateTime": "2024-01-01T00:00:00", "value": 1.23},
            {"dateTime": "2024-01-01T00:15:00", "value": None},
            {"dateTime": "2024-01-01T00:30:00", "value": 2.34},
        ]
    }

    result = transform_readings(
        "station123",
        "instantaneous",
        sample_data
    )

    assert len(result) == 2
    assert result[0][3] == 1.23
    assert result[1][3] == 2.34