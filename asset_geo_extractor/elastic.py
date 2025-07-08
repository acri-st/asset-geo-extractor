"""Elasticsearch client for updating geospatial metadata in the database."""

from despsharedlibrary.schemas.collaborative_schema import AssetType
from elasticsearch import AsyncElasticsearch
from msfwk.utils.config import read_config
from msfwk.utils.logging import get_logger
from pydantic import BaseModel

from asset_geo_extractor.extractors.base import GeoMetadata

logger = get_logger("elastic")


class ElasticConfig(BaseModel):
    """Configuration for Elasticsearch service"""

    url: str
    asset_index_prefix: str = ""
    username: str | None = None
    password: str | None = None
    port: int = 9200
    ssl_check: bool = True
    ssl: bool = True


class ElasticClient:
    """Client for Elasticsearch service"""

    def __init__(self, config: ElasticConfig):
        self.config = config
        connection_string = (
            f"{'https' if config.ssl else 'http'}://{config.username}:{config.password}@{config.url}:{config.port}"
        )

        self.client = AsyncElasticsearch(connection_string, verify_certs=config.ssl_check, request_timeout=10)

        self.asset_index_prefix = config.asset_index_prefix

    def _get_asset_index(self, asset_type: AssetType) -> str:
        """Get the index name for an asset type"""
        return f"{asset_type.name}_index"

    async def update_document(self, asset_type: AssetType, doc_id: str, file_path: str, geo_metadata: GeoMetadata) -> bool:
        """Update an existing document, adding or updating the geo_shape for a specific file path.

        Args:
            asset_type: The type of asset.
            doc_id: Document ID.
            file_path: Path to the file associated with this geo shape.
            geo_metadata: Extracted geospatial metadata (bbox and crs).

        Returns:
            Success status.
        """
        index = self._get_asset_index(asset_type)
        geo_shape_entry = None

        if geo_metadata.bbox:
            geo_shape_entry = {
                "file_path": file_path,
                "shape": {
                    "type": "envelope",
                    "coordinates": geo_metadata.bbox,
                }
            }
            if geo_metadata.crs:
                geo_shape_entry["crs"] = geo_metadata.crs

        if not geo_shape_entry:
            logger.info("No geospatial data found in GeoMetadata for file %s in document %s", file_path, doc_id)
            return True # No update needed if no geo data

        script = """
            if (ctx._source.geo_shapes == null) {
                ctx._source.geo_shapes = [params.geo_data];
            } else {
                boolean found = false;
                for (int i = 0; i < ctx._source.geo_shapes.size(); i++) {
                    if (ctx._source.geo_shapes[i].file_path == params.geo_data.file_path) {
                        ctx._source.geo_shapes[i] = params.geo_data;
                        found = true;
                        break;
                    }
                }
                if (!found) {
                    ctx._source.geo_shapes.add(params.geo_data);
                }
            }
        """

        try:
            logger.debug("Upserting geo shape for file %s in document %s in index %s", file_path, doc_id, index)
            result = await self.client.update(
                index=index,
                id=doc_id,
                script={
                    "source": script,
                    "lang": "painless",
                    "params": {"geo_data": geo_shape_entry}
                },
                # If the document doesn't exist, create it with the geo_shapes field initialized
                upsert={"geo_shapes": [geo_shape_entry]}
            )

            logger.info("Successfully upserted geo shape for file %s in document %s: %s", file_path, doc_id, result.get("result"))
            return True

        except Exception:
            logger.exception("Error upserting geo shape for file %s in document %s in index %s", file_path, doc_id, index)
            return False

    async def remove_geo_shape_for_file(self, asset_type: AssetType, doc_id: str, file_path: str) -> bool:
        """Remove a geo_shape entry for a specific file path from a document.

        Args:
            asset_type: The type of asset.
            doc_id: Document ID.
            file_path: Path to the file whose geo shape should be removed.

        Returns:
            Success status.
        """
        index = self._get_asset_index(asset_type)

        script = """
            if (ctx._source.geo_shapes != null) {
                ctx._source.geo_shapes.removeIf(shape -> shape.file_path == params.file_path);
            }
        """

        try:
            logger.debug("Removing geo shape for file %s from document %s in index %s", file_path, doc_id, index)
            result = await self.client.update(
                index=index,
                id=doc_id,
                script={
                    "source": script,
                    "lang": "painless",
                    "params": {"file_path": file_path}
                }
            )

            # Check if the document was found and updated
            if result.get("result") == "not_found":
                 logger.warning("Document %s not found in index %s, could not remove geo shape for file %s", doc_id, index, file_path)
                 return False # Or True depending on desired behavior for non-existent docs

            logger.info("Successfully processed removal request for geo shape file %s in document %s: %s", file_path, doc_id, result.get("result"))
            return True

        except Exception:
            logger.exception("Error removing geo shape for file %s from document %s in index %s", file_path, doc_id, index)
            return False

    async def close(self) -> None:
        """Close the Elasticsearch client connection"""
        await self.client.close()


async def get_elastic_config() -> ElasticConfig:
    """Get the Elasticsearch config"""
    config = read_config().get("elastic", {})
    return ElasticConfig(
        url=config.get("url"),
        username=config.get("username"),
        password=config.get("password"),
        ssl=config.get("ssl", True),
        ssl_check=config.get("ssl_check", False),
    )


async def get_elastic_client() -> ElasticClient:
    """Get an Elasticsearch client instance"""
    config = await get_elastic_config()
    return ElasticClient(config)
