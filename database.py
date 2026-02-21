import sqlite3

DB_NAME = "hydrology.db"


def get_connection():
    return sqlite3.connect(DB_NAME)


def create_tables():
    conn = get_connection()
    cursor = conn.cursor()

    # Dimension table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS stations (
            station_id TEXT PRIMARY KEY,
            station_label TEXT,
            river_name TEXT,
            latitude REAL,
            longitude REAL
        )
    """)

    # Fact table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS measurements (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            station_id TEXT,
            measure_type TEXT,
            date_time TEXT,
            value REAL,
            FOREIGN KEY (station_id) REFERENCES stations (station_id)
        )
    """)

    conn.commit()
    conn.close()


def insert_station(station):
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
        INSERT OR IGNORE INTO stations
        VALUES (?, ?, ?, ?, ?)
    """, station)

    conn.commit()
    conn.close()


def insert_measurements(rows):
    conn = get_connection()
    cursor = conn.cursor()

    cursor.executemany("""
        INSERT INTO measurements (station_id, measure_type, date_time, value)
        VALUES (?, ?, ?, ?)
    """, rows)

    conn.commit()
    conn.close()