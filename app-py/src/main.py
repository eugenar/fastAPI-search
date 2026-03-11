import os
import time
import traceback
from typing import Final, Optional
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from contextlib import asynccontextmanager

from .db import get_pool, close_pool
from .es_client import es_client, close_es_client
from .create_db_tables import create_tables
from .data_load import load_csv_files

# TODO: Add to config file
ES_FOOD_INDEX: Final[str] = "food"  # Elasticsearch index name for food data
ES_RESULT_SIZE: Final[int] = 20  # Number of search results to return
DB_QUERY: Final[
    str
] = """
            SELECT fn.food_id, json_agg(json_build_object('nutrient', fn.nutrient_type, 'amount', fn.amount)) AS nutrients
            FROM  food_nutrient fn 
            WHERE fn.food_id = ANY($1::int[])
            GROUP BY fn.food_id;
            """

DB_QUERY_TRIGGER: Final[str] = (
    "SELECT trigger_name, event_manipulation FROM information_schema.triggers WHERE event_object_table = 'food'"
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage application lifecycle - startup and shutdown."""
    # Startup
    print(f"Server starting on port {os.getenv('PORT', 3000)}")

    # Create database tables and load data on startup
    # TODO: This step should be handled separately (e.g., using migrations or cron jobs);
    # doing it on startup is not ideal for production but simplifies local development and testing.
    print("Create tables in database 'bevel'...")
    await create_tables()

    print("Load tables from csv files in /data directory...")
    await load_csv_files()

    yield
    # Shutdown
    await close_pool()
    await close_es_client()


# Create FastAPI application
app = FastAPI(
    title="FastAPI Search API",
    description="FastAPI application for searching food items with Elasticsearch and PostgreSQL",
    version="1.0.0",
    lifespan=lifespan,
)


app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],  # Allows all standard methods
    allow_headers=["*"],  # Allows all standard headers
)


@app.middleware("http")
async def logging_middleware(request: Request, call_next):
    """
    Logs the request path and processing time.
    """
    start_time = time.perf_counter()

    # Forward request to route handler
    response = await call_next(request)

    process_time = time.perf_counter() - start_time
    print(
        f"Request: {request.url.path} completed in {process_time:.4f} seconds with status {response.status_code}"
    )

    return response


@app.get("/health")
async def health_check():
    """Health check endpoint to verify database and Elasticsearch connectivity. Returns status, database triggers, Elasticsearch version, and indices information."""
    try:
        # Check database connection
        pool = await get_pool()
        async with pool.acquire() as connection:
            triggers = await connection.fetch(DB_QUERY_TRIGGER)

        if triggers is None:
            raise HTTPException(
                status_code=500, detail="'food' table does not exist or no triggers found"
            )

        if len(triggers) < 3:  # Expecting at least 3 triggers for the food table
            raise HTTPException(
                status_code=500, detail="'food' table does not have the expected triggers"
            )

        # Check Elasticsearch connection
        es_info = await es_client.info()

        # Check Elasticsearch indices and ensure the expected index 'food' exists
        indices = await es_client.cat.indices(format="json")
        if not indices:
            raise HTTPException(
                status_code=500, detail="Elasticsearch no indices found"
            )

        if not any(idx["index"] == ES_FOOD_INDEX for idx in indices):
            raise HTTPException(
                status_code=500,
                detail=f"Elasticsearch index '{ES_FOOD_INDEX}' not found",
            )

        return {
            "status": "ok",
            "'food' table triggers": triggers,
            "esVersion": es_info["version"],
            "esIndices": [idx for idx in indices if idx["index"] == ES_FOOD_INDEX],
        }

    except Exception as e:
        print(f"Error in /health: {e}")
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/search")
async def search(q: Optional[str] = Query(default="", description="Search query")):
    """Search endpoint that queries Elasticsearch for food items matching the search query and retrieves nutrient information from the database. Returns a list of matched food items with their nutrients."""
    try:
        # Search in Elasticsearch
        search_query = {"query": {"match": {"name": q}}}
        response = await es_client.search(
            index=ES_FOOD_INDEX, body=search_query, size=ES_RESULT_SIZE
        )

        results = []
        for hit in response["hits"]["hits"]:
            source = hit["_source"]
            results.append(
                {
                    "id": hit["_id"],
                    "name": source.get("name"),
                }
            )

        # Query database for nutrient information for the matched food items
        pool = await get_pool()
        food_ids = [int(res["id"]) for res in results]
        if food_ids:
            async with pool.acquire() as connection:

                db_results = await connection.fetch(DB_QUERY, food_ids)

                # Add database results to search results
                db_results_dic = {
                    str(record["food_id"]): record for record in db_results
                }
                for res in results:
                    id = res["id"]
                    if id in db_results_dic:
                        res["nutrients"] = db_results_dic[id]["nutrients"]
                    else:
                        res["nutrients"] = []

        return JSONResponse(content={"foods": results})
    except Exception as e:
        print(f"Error in /search: {e}")
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    import uvicorn

    port = int(os.getenv("PORT", 3000))
    uvicorn.run("src.main:app", host="0.0.0.0", port=port, reload=True)
