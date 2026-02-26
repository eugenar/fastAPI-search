import pytest
from httpx import AsyncClient
from unittest.mock import AsyncMock, patch
from src.main import app


@pytest.mark.asyncio
async def test_health_endpoint():
    """Test the health check endpoint."""
    with patch('src.main.get_pool') as mock_pool, \
         patch('src.main.es_client') as mock_es:

        # Mock database response
        mock_connection = AsyncMock()
        mock_connection.fetchval = AsyncMock(return_value='2024-01-01 00:00:00')
        mock_pool_instance = AsyncMock()
        mock_pool_instance.acquire = AsyncMock()
        mock_pool_instance.acquire().__aenter__ = AsyncMock(return_value=mock_connection)
        mock_pool_instance.acquire().__aexit__ = AsyncMock()
        mock_pool.return_value = mock_pool_instance

        # Mock Elasticsearch response
        mock_es.info = AsyncMock(return_value={
            'version': {'number': '8.6.2', 'build_flavor': 'default'}
        })

        async with AsyncClient(app=app, base_url="http://test") as client:
            response = await client.get("/health")

        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "ok"
        assert "dbTime" in data
        assert "esVersion" in data


@pytest.mark.asyncio
async def test_search_endpoint():
    """Test the search endpoint."""
    with patch('src.main.es_client') as mock_es:
        # Mock Elasticsearch indices response
        mock_es.cat.indices = AsyncMock(return_value=[
            {'index': 'test-index-1', 'health': 'green'},
            {'index': 'test-index-2', 'health': 'yellow'}
        ])

        async with AsyncClient(app=app, base_url="http://test") as client:
            response = await client.get("/search?q=test")

        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "ok"
        assert "indices" in data