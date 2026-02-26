import os
from typing import Optional
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse
from contextlib import asynccontextmanager
from dotenv import load_dotenv

from .db import get_pool, close_pool
from .es_client import es_client, close_es_client

# Load environment variables from .env file
load_dotenv()


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage application lifecycle - startup and shutdown."""
    # Startup
    print(f"Server starting on port {os.getenv('PORT', 3000)}")
    yield
    # Shutdown
    await close_pool()
    await close_es_client()


# Create FastAPI application
app = FastAPI(
    title="Python Backend Boilerplate",
    description="Python/FastAPI boilerplate for backend take-home assessment",
    version="1.0.0",
    lifespan=lifespan
)


@app.get("/health")
async def health_check():
    """
    Health check endpoint that verifies database and Elasticsearch connections.
    """
    try:
        # Check database connection
        pool = await get_pool()
        async with pool.acquire() as connection:
            db_time = await connection.fetchval("SELECT NOW()")

        # Check Elasticsearch connection
        es_info = await es_client.info()

        return {
            "status": "ok",
            "dbTime": str(db_time),
            "esVersion": es_info["version"]
        }
    except Exception as e:
        print(f"Error in /health: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/search")
async def search(q: Optional[str] = Query(default="", description="Search query")):
    """
    Example endpoint: search documents in Elasticsearch.
    TODO: Replace with actual search endpoint. For now, this just serves as a health check.
    """
    try:
        # Get list of indices as a health check
        indices = await es_client.cat.indices(format="json")

        return {
            "status": "ok",
            "indices": indices
        }
    except Exception as e:
        print(f"Error in /search: {e}")
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", 3000))
    uvicorn.run("src.main:app", host="0.0.0.0", port=port, reload=True)