# Python Backend Boilerplate

This is a Python/FastAPI boilerplate for the backend take-home assessment, providing the same functionality as the TypeScript version.

## Requirements

- Python 3.9+
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

## Running the Application

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
app-py/
├── src/
│   ├── __init__.py
│   ├── main.py           # FastAPI application entry point
│   ├── db.py            # PostgreSQL connection
│   └── es_client.py     # Elasticsearch client
├── requirements.txt      # Python dependencies
├── .gitignore
└── README.md
```