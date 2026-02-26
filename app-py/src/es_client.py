import os
from elasticsearch import AsyncElasticsearch

# Elasticsearch configuration
ES_URL = os.getenv('ELASTICSEARCH_URL', 'http://localhost:9200')

# Create async Elasticsearch client
es_client = AsyncElasticsearch(
    hosts=[ES_URL],
    verify_certs=False,  # For local development
    ssl_show_warn=False
)


async def close_es_client():
    """Close the Elasticsearch client connection."""
    await es_client.close()