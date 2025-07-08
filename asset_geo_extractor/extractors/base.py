"""Base classes and utilities for geospatial metadata extraction."""

import os
from typing import Protocol, Set

from msfwk.utils.logging import get_logger
from pydantic import BaseModel

logger = get_logger("extractor")


class GeoMetadata(BaseModel):
    """Standardized geospatial metadata extracted from a file."""

    bbox: list[list[float]] | None = None  # [[west, north], [east, south]] in WGS84
    crs: str | None = None


class GeoExtractor(Protocol):
    """Protocol for classes that can extract GeoMetadata from a file."""

    def extract(self, file_path: str) -> GeoMetadata | None:
        """Extract GeoMetadata from a file."""
        ...


# Registry of available extractors
EXTRACTORS = {}


def register_extractor(extension: str, extractor_instance: GeoExtractor) -> None:
    """Register an extractor for a given file extension."""
    EXTRACTORS[extension.lower()] = extractor_instance


def get_extractor(file_path: str) -> GeoExtractor | None:
    """Gets the appropriate GeoExtractor based on file extension."""
    _, ext = os.path.splitext(file_path)
    extractor = EXTRACTORS.get(ext.lower())
    if not extractor:
        logger.warning("No extractor found for file type: %s", ext)
    return extractor


def get_supported_extensions() -> Set[str]:
    """Returns a set of all registered file extensions.
    
    Returns:
        Set of file extensions with registered extractors (e.g., {'.geojson', '.shp'})
    """
    return set(EXTRACTORS.keys())
