import asyncio
import json
import os
import re
import tempfile

from aio_pika import IncomingMessage
from aio_pika.abc import AbstractRobustConnection
from despsharedlibrary.schemas.collaborative_schema import AssetType
from msfwk.mqclient import MQClient, consume_mq_queue_async
from msfwk.utils.logging import get_logger
from pydantic import ValidationError

from ..elastic import ElasticClient, get_elastic_client
from ..extractors import get_extractor, get_supported_extensions
from ..gitlab import GitlabClient, get_gitlab_client
from ..mq import GitlabPushEvent, mq_config
from ..utils import _list_storage

# Zero commit SHA pattern (indicates first push or branch deletion)
ZERO_COMMIT_PATTERN = r"^0+$"

logger = get_logger("geo_worker")


class GeoExtractionWorker:
    """Worker for processing geo extraction tasks from RabbitMQ queue"""

    def __init__(self):
        self.client: MQClient | None = None
        self.connection: AbstractRobustConnection | None = None
        self.consumer_task: asyncio.Task | None = None
        self.is_running = False
        self.gitlab_client: GitlabClient | None = None
        self.elastic_client: ElasticClient | None = None

    async def setup(self) -> bool:
        """Set up the worker and connect to RabbitMQ"""
        try:
            logger.info("Setting up GeoExtractionWorker...")
            self.client = MQClient()
            await self.client.setup()
            self.gitlab_client = await get_gitlab_client()
            self.elastic_client = await get_elastic_client()

            supported_extensions = get_supported_extensions()
            logger.info(
                "Initialized with support for file types: %s",
                ", ".join(supported_extensions),
            )

            self.is_running = True
            return True
        except Exception:
            logger.exception("Failed to setup GeoExtractionWorker")
            return False

    async def start_consuming(self) -> bool:
        """Start consuming messages from the queue"""
        if not self.is_running or not self.client:
            logger.error("Worker is not running or not properly initialized")
            return False

        try:
            logger.info(
                "Starting to consume messages from %s", mq_config.geo_extraction_queue
            )
            self.consumer_task = await consume_mq_queue_async(
                mq_config.geo_extraction_queue, self.process_message
            )
            return True
        except Exception as e:
            logger.error("Failed to start consuming: %s", e)
            return False

    async def stop(self) -> None:
        """Stop the worker and cleanup resources"""
        logger.info("Stopping GeoExtractionWorker...")
        if self.consumer_task:
            self.consumer_task.cancel()
            try:
                await self.consumer_task
            except asyncio.CancelledError:
                pass

        if self.client:
            await self.client.close()

        self.is_running = False
        logger.info("GeoExtractionWorker stopped")

    async def process_message(self, message: IncomingMessage) -> None:
        """Process incoming messages from the queue

        Args:
            message: The incoming message from RabbitMQ
        """
        async with message.process():
            try:
                # Decode message body
                body = message.body.decode()
                payload = json.loads(body)

                logger.info("Processing message: %s", payload)

                # Parse and validate the payload
                event = GitlabPushEvent.model_validate(payload)

                # Process the geo extraction task using validated data
                await self._extract_geo_data(
                    event.project_id,
                    event.project_name,
                    event.before,
                    event.after,
                    event.ref,
                )

                logger.info(
                    "Successfully processed message for project %s", event.project_id
                )

            except json.JSONDecodeError as e:
                logger.error("Failed to decode message: %s", e)
                # Message is acknowledged but we log the error
            except ValidationError as e:
                logger.error("Invalid message format: %s. Payload: %s", e, payload)
                # Acknowledge invalid messages
            except Exception:
                logger.exception("Error processing message:")
                # In case of other errors, we'll ack the message but log the error
                # If we want to reject/nack with requeue for retyr:
                # message.nack(requeue=True)

    async def _extract_geo_data(
        self, project_id, project_name, before_sha, after_sha, ref
    ):
        """Extract geo data from project changes

        Args:
            project_id: The GitLab project ID
            before_sha: The SHA before the change
            after_sha: The SHA after the change
            ref: The git reference (branch/tag)
        """
        if not self.gitlab_client or not self.elastic_client:
            logger.error("GitLab or Elasticsearch client not initialized")
            return

        # Get supported extensions from the extractors
        supported_extensions = get_supported_extensions()
        if not supported_extensions:
            logger.warning(
                "No registered extractors found. Cannot process geospatial files."
            )
            return

        try:
            # Determine the type of event (first push, branch deletion, update)
            is_first_push = re.match(ZERO_COMMIT_PATTERN, before_sha) is not None
            is_branch_deleted = re.match(ZERO_COMMIT_PATTERN, after_sha) is not None

            geo_files = []
            deleted_files = []

            if is_branch_deleted:
                logger.info(
                    "Branch deletion detected for project %s, ref %s. No files to process.",
                    project_id,
                    ref,
                )
                return

            if is_first_push:
                # First push to the branch - get all files in the repository
                logger.info(
                    "First push detected for project %s, ref %s. Getting repository tree.",
                    project_id,
                    ref,
                )

                tree = await self.gitlab_client.get_repository_tree(
                    project_id, after_sha
                )

                # Filter for supported file types (only files, not directories)
                for item in tree:
                    if item.get("type") == "blob":  # It's a file, not a directory
                        file_path = item.get("path", "")
                        if any(
                            file_path.lower().endswith(ext)
                            for ext in supported_extensions
                        ):
                            geo_files.append(file_path)

                logger.info(
                    "Found %d geo-related files in initial push for project %s",
                    len(geo_files),
                    project_id,
                )

            else:
                # Normal update - compare commits
                logger.info(
                    "Update detected for project %s. Comparing commits %s -> %s",
                    project_id,
                    before_sha,
                    after_sha,
                )

                # comparison = await self.gitlab_client.compare_commits(project_id, before_sha, after_sha)
                #
                ## Process diffs to find added, modified or renamed files
                # for diff in comparison.get("diffs", []):
                #    # Check if file was deleted
                #    if diff.get("deleted_file", False):
                #        old_path = diff.get("old_path", "")
                #        if any(old_path.lower().endswith(ext) for ext in supported_extensions):
                #            deleted_files.append(old_path)
                #        continue
                #
                #    # Get the new path (for renamed files this will be the new name)
                #    file_path = diff.get("new_path")
                #
                #    if not file_path:
                #        continue
                #
                #    # Check if the file has a supported extension
                #    if any(file_path.lower().endswith(ext) for ext in supported_extensions):
                #        geo_files.append(file_path)

                # Get file from search repo

                list_of_files = await _list_storage(project_id)

                for file in list_of_files:
                    if any(
                        file["filePath"].lower().endswith(ext)
                        for ext in supported_extensions
                    ):
                        geo_files.append(file["filePath"])

                logger.info(
                    "Found %d geo-related files in update for project %s",
                    len(geo_files),
                    project_id,
                )
                logger.info(
                    "Found %d deleted geo-related files in update for project %s",
                    len(deleted_files),
                    project_id,
                )

            # Handle deleted files

            # Process each geo file
            processed_count = 0
            for file_path in geo_files:
                try:
                    # Get raw file content
                    content = await self.gitlab_client.get_raw_file_content(
                        project_id, file_path, after_sha
                    )

                    # Process the file based on its extension
                    await self._process_geo_file(
                        project_id, project_name, file_path, content, ref
                    )

                    # Log file information
                    logger.info(
                        "Processed geo file: %s (size: %d bytes)",
                        file_path,
                        len(content),
                    )
                    processed_count += 1

                except Exception as e:
                    logger.error("Error processing file %s: %s", file_path, e)

            logger.info(
                "Processed %d/%d geo files for project %s",
                processed_count,
                len(geo_files),
                project_id,
            )

        except Exception as e:
            logger.error("Error extracting geo data: %s", e)
            raise

    async def _process_geo_file(
        self,
        project_id: str,
        project_name: str,
        file_path: str,
        content: bytes,
        ref: str,
    ) -> None:
        """Process a geospatial file and extract its metadata

        Args:
            project_id: The GitLab project ID
            project_name: The GitLab project name
            file_path: Path to the file in the repository
            content: Raw file content as bytes
            ref: The git reference (branch/tag)
        """
        try:
            # Create a temporary file to write the content to so the extractor can read it
            with tempfile.NamedTemporaryFile(
                suffix=os.path.splitext(file_path)[1], delete=False
            ) as temp_file:
                temp_path = temp_file.name
                temp_file.write(content)

            try:
                # Get the appropriate extractor for this file type
                extractor = get_extractor(file_path)
                if not extractor:
                    logger.error("No extractor found for file: %s", file_path)
                    return

                # Extract geospatial metadata
                geo_metadata_obj = extractor.extract(temp_path)
                if not geo_metadata_obj:
                    logger.error(
                        "Failed to extract geospatial metadata from file: %s", file_path
                    )
                    return

                logger.info("Extracted geospatial metadata: %s", geo_metadata_obj)

                # Extract additional metadata from the file content for richer ES documents
                additional_metadata = {}

                # For GeoJSON files, extract feature count, geometry type, etc.
                if file_path.lower().endswith(".geojson"):
                    geojson_data = json.loads(content.decode("utf-8"))

                    # Get feature count
                    if geojson_data.get("type") == "FeatureCollection":
                        additional_metadata["geo_feature_count"] = len(
                            geojson_data.get("features", [])
                        )

                        # Get geometry type from the first feature
                        features = geojson_data.get("features", [])
                        if features:
                            geometry = features[0].get("geometry", {})
                            if geometry and "type" in geometry:
                                additional_metadata["geo_geometry_type"] = geometry.get(
                                    "type"
                                )

                            # Extract common properties
                            if features[0].get("properties"):
                                prop_names = ", ".join(
                                    features[0].get("properties", {}).keys()
                                )
                                additional_metadata["geo_properties"] = prop_names

                    elif geojson_data.get("type") == "Feature":
                        additional_metadata["geo_feature_count"] = 1
                        geometry = geojson_data.get("geometry", {})
                        if geometry and "type" in geometry:
                            additional_metadata["geo_geometry_type"] = geometry.get(
                                "type"
                            )

                # Prepare geo metadata for Elasticsearch with flattened structure
                geo_metadata_dict = {
                    "file_path": file_path,
                    "ref": ref,
                    **additional_metadata,
                }

                # Add CRS if available
                if geo_metadata_obj.crs:
                    geo_metadata_dict["geo_crs"] = geo_metadata_obj.crs

                # Add bounding box if available
                if geo_metadata_obj.bbox:
                    west, north = geo_metadata_obj.bbox[0]
                    east, south = geo_metadata_obj.bbox[1]
                    geo_metadata_dict["geo_bbox_west"] = west
                    geo_metadata_dict["geo_bbox_south"] = south
                    geo_metadata_dict["geo_bbox_east"] = east
                    geo_metadata_dict["geo_bbox_north"] = north

                # Generate asset ID from project name
                asset_id = await self._get_asset_id(project_name)

                # Update the document in Elasticsearch
                asset_type = AssetType.dataset
                logger.debug(
                    "Enriching asset %s with geospatial data from %s",
                    asset_id,
                    file_path,
                )
                logger.debug("Geo metadata: %s", geo_metadata_dict)

                success = await self.elastic_client.update_document(
                    asset_type=asset_type,
                    doc_id=asset_id,
                    file_path=file_path,
                    geo_metadata=geo_metadata_obj,
                )

                if success:
                    logger.info(
                        "Successfully enriched asset %s with geospatial data from %s",
                        asset_id,
                        file_path,
                    )
                else:
                    logger.error(
                        "Failed to enrich asset with geospatial data from %s", file_path
                    )

            finally:
                # Clean up the temporary file
                try:
                    os.unlink(temp_path)
                except Exception as e:
                    logger.warning(
                        "Failed to delete temporary file %s: %s", temp_path, e
                    )

        except json.JSONDecodeError as e:
            logger.error("Invalid JSON format in %s: %s", file_path, e)
        except Exception:
            logger.exception("Error processing geospatial file %s", file_path)

    async def _get_asset_id(self, project_name: str) -> str:
        """Get asset ID for Elasticsearch

        Args:
            project_name: The GitLab project name

        Returns:
            Asset ID string
        """
        # Get project details to extract the UUID from project name
        try:
            # Extract UUID from project name if it follows the pattern name_uuid
            # The UUID format we're looking for is 8-4-4-4-12 hex digits

            # Look for a UUID pattern in the project name
            uuid_pattern = (
                r"([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})"
            )
            uuid_match = re.search(uuid_pattern, project_name)

            if uuid_match:
                return uuid_match.group(1)
        except Exception:
            logger.exception("Could not extract UUID from project name:")
            # Return project name as fallback if UUID extraction fails
            return project_name


# Singleton instance
worker = GeoExtractionWorker()


async def setup_worker() -> bool:
    """Set up the geo extraction worker"""
    success = await worker.setup()
    if success:
        await worker.start_consuming()
    return success


async def shutdown_worker() -> None:
    """Shutdown the geo extraction worker"""
    await worker.stop()
