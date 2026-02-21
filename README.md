# Hydrology Data Engineering Pipeline

## Overview
This project implements a modular ETL pipeline using the Environment Agency Hydrology Data Explorer API.

The pipeline:
- Extracts the 10 most recent readings for two measures
- Transforms and validates the data
- Loads the results into a SQLite database using a star schema design

---

## Architecture

The project follows separation of concerns:

- `main.py` → Pipeline orchestration (ETL execution)
- `transform.py` → Data transformation logic
- `database.py` → Database schema creation and loading
- `tests/` → Unit tests
- `hydrology.db` → Generated SQLite database

---

## Database Schema (Star Schema)

### Dimension Table
- `stations`

### Fact Table
- `measurements`

The measurements table references the stations table via `station_id`.

---

## Setup Instructions

### 1. Create virtual environment
```
python -m venv venv
```

Activate:
- Mac/Linux:
```
source venv/bin/activate
```
- Windows:
```
venv\Scripts\activate
```

### 2. Install dependencies
```
pip install -r requirements.txt
```

### 3. Run the pipeline
```
python main.py
```

### 4. Run tests
```
pytest
```

---

## Output

The pipeline generates:

```
hydrology.db
```

This SQLite database contains:
- 1 station record
- 20 measurement records (10 per measure)

---

## Design Decisions

- Client-side sorting ensures retrieval of the 10 most recent readings even if API sorting is unsupported.
- Separation of extraction, transformation, and loading ensures maintainability and scalability.
- SQLite was chosen as a lightweight file-based database as required.