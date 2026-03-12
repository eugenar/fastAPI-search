# Python FastAPI app for text search in elasticsearch based on data in PostgreSQL

## Requirements

- Python 3.13+
- pip or pipenv

## Setup

### 1. Create virtual environment

```bash
python -m venv venv
```

### 2. Activate virtual environment

**On macOS/Linux:**
```bash
source venv/bin/activate
```

**On Windows:**
```bash
venv\Scripts\activate
```

### 3. Install dependencies

```bash
pip install -r requirements.txt
```
(includes pgsync)

## Running the Application

Run locally executing from app-py directory:
python3 -m src.main

### For development and testing convenience database tables are created and loaded at application startup. This should normally be realized on a separate ETL cron job.

### Create required tables in database 'bevel': 3 staging tables where 3 csv files from USDA Foundation Foods dataset https://fdc.nal.usda.gov/download-datasets . The data is then processed into 2 tables based on:

### Mapping nutrient -> nutrient category

Based on https://fdc.nal.usda.gov/Foundation_Foods_Documentation

Energy (Atwater Specific Factors) -> calories (more accurate than Energy (Atwater General Factors))
Protein -> protein
Total lipid (fat) -> fat
Carbohydrate, by difference -> carbs

### 'food' table is synced real time to Elasticsearch index 'food' via pgsync.
create_db_tables.py

### The csv data files are assumed to have been downloaded from https://fdc.nal.usda.gov/download-datasets in /data directory (a cron job that will connect to an ftp server would be suited for the task). Existing data in the tables is replaced.
data_load.py

### Use pgsync to create the index in Elasticsearch based on schema.json and the db table triggers (executing from app-py directory):
bootstrap --config schema.json

### Start pgsync as daemon to sync changes to 'food' table into Elasticsearch index 'food' (executing from app-py directory)
pgsync --config schema.json --daemon

# Run health checks

After starting the server check `localhost:3000/health` to see if the connection to database and Elasticsearch is working, and the required tables, triggers, and indices exist.

# Test
Access Swagger UI to test `localhost:3000/docs`

Testing /search endpoint with query 'hummus' will return:

"foods": [
    {
      "id": "321358",
      "name": "Hummus, commercial",
      "nutrients": "[{\"nutrient\" : \"fat\", \"amount\" : 16.100}, {\"nutrient\" : \"carbs\", \"amount\" : 13.900}, {\"nutrient\" : \"calories\", \"amount\" : 229.000}]"
    }
  ]

The Elasticsearch index 'food' is based on full word case insensitive; searching for 'hum' will not return any results.

The index will be visible in kibana http://localhost:5601 (needs to create a data view first).

### Development mode

```bash
uvicorn src.main:app --reload --port 3000
```

### Production mode

```bash
uvicorn src.main:app --host 0.0.0.0 --port 3000
```

## API Endpoints

- `GET /health` - Health check endpoint that verifies database and Elasticsearch connections
- `GET /search` - Example Elasticsearch endpoint (currently returns indices list)

## Environment Variables

The application uses the following environment variables (with defaults):

- `POSTGRES_HOST` (default: localhost)
- `POSTGRES_PORT` (default: 54328)
- `POSTGRES_USER` (default: bevel)
- `POSTGRES_PASSWORD` (default: password)
- `POSTGRES_DB` (default: bevel)
- `ELASTICSEARCH_URL` (default: http://localhost:9200)
- `PORT` (default: 3000)

You can create a `.env` file in the root directory to override these defaults.

## Testing

Run tests with pytest:

```bash
pytest
```

## Project Structure

```
data # directory with csv data files downloaded from https://fdc.nal.usda.gov/download-datasets to load
app-py/
├── src/
│   ├── __init__.py
│   ├── main.py           # FastAPI application entry point
│   ├── db.py            # PostgreSQL connection
│   └── es_client.py     # Elasticsearch client
├── requirements.txt      # Python dependencies
|__ schema.json         # used by pgsync to sync pg 'food' table with 'food' es index
├── .gitignore
└── README.md
```
