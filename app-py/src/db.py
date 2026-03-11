import os
from dotenv import load_dotenv
import asyncpg
from typing import Optional

# Load environment variables from .env file
load_dotenv()

# Database configuration with environment variables and defaults
DB_CONFIG = {
    'host': os.getenv('PG_HOST', 'localhost'),
    'port': int(os.getenv('PG_PORT', '54328')),
    'user': os.getenv('PG_USER', 'bevel'),
    'password': os.getenv('PG_PASSWORD', 'password'),
    'database': os.getenv('PG_DB', 'bevel'),
}

# Global connection pool
_pool: Optional[asyncpg.Pool] = None


async def get_pool() -> asyncpg.Pool:
    """Get or create the database connection pool."""
    global _pool
    if _pool is None:
        _pool = await asyncpg.create_pool(**DB_CONFIG)
    return _pool


async def close_pool():
    """Close the database connection pool."""
    global _pool
    if _pool:
        await _pool.close()
        _pool = None