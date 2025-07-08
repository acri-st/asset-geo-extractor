"""Extractors for geospatial metadata from GeoJSON files."""

import json

import geojson
from msfwk.utils.logging import get_logger

from asset_geo_extractor.extractors.base import GeoMetadata, register_extractor

logger = get_logger("extractor.geojson")


class GeoJsonExtractor:
    """Extracts geospatial metadata from GeoJSON files."""

    def __init__(self) -> None:
        # Register this extractor for geojson files
        register_extractor(".geojson", self)

    def extract(self, file_path: str) -> GeoMetadata | None:
        """Extract bbox and CRS information from a GeoJSON file."""
        try:
            with open(file_path) as f:
                gj = geojson.load(f)

            if not gj.is_valid:
                logger.warning("Invalid GeoJSON file: %s", file_path)
           

            # --- Bounding Box ---
            bbox = None
            min_lon, min_lat, max_lon, max_lat = float('inf'), float('inf'), float('-inf'), float('-inf')
            coord_generator = geojson.utils.coords(gj)
            first_coord = next(coord_generator, None) # Get the first coordinate to initialize bounds

            if first_coord:
                min_lon, min_lat = first_coord
                max_lon, max_lat = first_coord

                # Iterate over the rest of the coordinates
                for lon, lat in coord_generator:
                    min_lon = min(min_lon, lon)
                    min_lat = min(min_lat, lat)
                    max_lon = max(max_lon, lon)
                    max_lat = max(max_lat, lat)

                # Convert to [[west, north], [east, south]] format if bounds were found
                bbox = [[min_lon, max_lat], [max_lon, min_lat]]

            # --- CRS ---
            # GeoJSON spec implies WGS84 (EPSG:4326) unless a 'crs' member is present
            crs = gj.get("crs", {}).get("properties", {}).get("name", "EPSG:4326")
            # Basic check if it needs reprojection (this logic would need enhancement)
            if "urn:ogc:def:crs:EPSG::" in crs:
                crs_code = crs.split(":")[-1]
                crs = f"EPSG:{crs_code}"
                # TODO: Add reprojection logic here if crs is not EPSG:4326
                # using pyproj or rasterio/fiona
                if crs != "EPSG:4326":
                    logger.warning("CRS is %s, not EPSG:4326. Reprojection needed for file %s", crs, file_path)
                    # For now, we still return the data, but a real implementation
                    # should reproject bbox to EPSG:4326 here.

            return GeoMetadata(bbox=bbox, crs=crs)

        except FileNotFoundError:
            logger.error("File not found for extraction: %s", file_path)
            return None
        except json.JSONDecodeError:
            logger.error("Invalid JSON in file: %s", file_path)
            return None
        except Exception:
            logger.exception("Error extracting GeoJSON metadata from %s", file_path)
            return None


# Instantiate the extractor to register it
GeoJsonExtractor()
