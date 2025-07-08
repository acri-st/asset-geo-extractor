import urllib.parse
from typing import Any

import aiohttp
from msfwk.utils.config import read_config
from msfwk.utils.logging import get_logger
from pydantic import BaseModel

logger = get_logger("application")


class GitlabConfig(BaseModel):
    """Configuration for Gitlab service"""

    url: str
    token: str
    project_group: str


class GitlabClient:
    """Client for Gitlab service"""

    def __init__(self, config: GitlabConfig):
        self.config = config
        self.api_url = f"{config.url.strip('/')}/api/v4"
        self.headers = {
            "PRIVATE-TOKEN": config.token,
            "Content-Type": "application/json",
        }

    async def get_repository_tree(self, project_id: int | str, ref: str) -> list[dict[str, Any]]:
        """Get repository tree recursively

        Args:
            project_id: The ID or URL-encoded path of the project
            ref: The branch, tag or commit SHA

        Returns:
            List of repository entries
        """
        url = f"{self.api_url}/projects/{urllib.parse.quote(str(project_id), safe='')}/repository/tree"
        params = {"recursive": "true", "ref": ref, "per_page": 100}

        tree = []
        page = 1

        try:
            async with aiohttp.ClientSession() as session:
                while True:
                    params["page"] = page
                    logger.info(f"Fetching repository tree page {page} for project {project_id}, ref {ref}")

                    async with session.get(url, headers=self.headers, params=params) as response:
                        if response.status != 200:
                            error_body = await response.text()
                            logger.error(f"Failed to get repository tree: {response.status} - {error_body}")
                            response.raise_for_status()

                        result = await response.json()
                        if not result:
                            break

                        tree.extend(result)

                        # Check if we need to paginate
                        total_pages = response.headers.get("X-Total-Pages")
                        if total_pages and page >= int(total_pages):
                            break

                        page += 1

            logger.info(f"Retrieved {len(tree)} tree entries for project {project_id}")
            return tree

        except aiohttp.ClientError as e:
            logger.error(f"HTTP error while fetching repository tree: {e}")
            raise
        except Exception as e:
            logger.error(f"Error fetching repository tree: {e}")
            raise

    async def compare_commits(self, project_id: int | str, from_sha: str, to_sha: str) -> dict[str, Any]:
        """Compare two commits

        Args:
            project_id: The ID or URL-encoded path of the project
            from_sha: The base commit SHA
            to_sha: The head commit SHA

        Returns:
            Dictionary with comparison information
        """
        url = f"{self.api_url}/projects/{urllib.parse.quote(str(project_id), safe='')}/repository/compare"
        params = {"from": from_sha, "to": to_sha}

        try:
            async with aiohttp.ClientSession() as session:
                logger.info(f"Comparing commits from {from_sha} to {to_sha} for project {project_id}")

                async with session.get(url, headers=self.headers, params=params) as response:
                    if response.status != 200:
                        error_body = await response.text()
                        logger.error(f"Failed to compare commits: {response.status} - {error_body}")
                        response.raise_for_status()

                    result = await response.json()

                    logger.info(f"Retrieved comparison with {len(result.get('diffs', []))} diffs")
                    return result

        except aiohttp.ClientError as e:
            logger.error(f"HTTP error while comparing commits: {e}")
            raise
        except Exception as e:
            logger.error(f"Error comparing commits: {e}")
            raise

    async def get_raw_file_content(self, project_id: int | str, file_path: str, ref: str) -> bytes:
        """Get raw content of a file

        Args:
            project_id: The ID or URL-encoded path of the project
            file_path: The file path within the repository
            ref: The branch, tag or commit SHA

        Returns:
            Raw file content as bytes
        """
        # URL encode the file path
        encoded_file_path = urllib.parse.quote(file_path, safe="")
        url = f"{self.api_url}/projects/{urllib.parse.quote(str(project_id), safe='')}/repository/files/{encoded_file_path}/raw"
        params = {"ref": ref}

        try:
            async with aiohttp.ClientSession() as session:
                logger.info(f"Fetching raw content of {file_path} at {ref} for project {project_id}")

                async with session.get(url, headers=self.headers, params=params) as response:
                    if response.status != 200:
                        error_body = await response.text()
                        logger.error(f"Failed to get file content: {response.status} - {error_body}")
                        response.raise_for_status()

                    # Return the raw content as bytes
                    content = await response.read()
                    logger.info(f"Retrieved {len(content)} bytes of content from {file_path}")
                    return content

        except aiohttp.ClientError as e:
            logger.error(f"HTTP error while fetching file content: {e}")
            raise
        except Exception as e:
            logger.error(f"Error fetching file content: {e}")
            raise

    async def get_project(self, project_id: int | str) -> dict[str, Any]:
        """Get project details

        Args:
            project_id: The ID or URL-encoded path of the project

        Returns:
            Dictionary with project details
        """
        url = f"{self.api_url}/projects/{urllib.parse.quote(str(project_id), safe='')}"

        try:
            async with aiohttp.ClientSession() as session:
                logger.info(f"Fetching project details for project {project_id}")

                async with session.get(url, headers=self.headers) as response:
                    if response.status != 200:
                        error_body = await response.text()
                        logger.error(f"Failed to get project details: {response.status} - {error_body}")
                        response.raise_for_status()

                    result = await response.json()
                    logger.info(f"Retrieved project details for {result.get('name', project_id)}")
                    return result

        except aiohttp.ClientError as e:
            logger.error(f"HTTP error while fetching project details: {e}")
            raise
        except Exception as e:
            logger.error(f"Error fetching project details: {e}")
            raise


async def get_gitlab_config() -> GitlabConfig:
    """Get the Gitlab config"""
    config = read_config().get("storage").get("git")
    logger.info(f"Gitlab config: {config}")
    return GitlabConfig(
        url=config.get("url"),
        token=config.get("token"),
        project_group=config.get("project_group"),
    )


async def get_gitlab_client() -> GitlabClient:
    """Get a GitLab client instance"""
    config = await get_gitlab_config()
    return GitlabClient(config)
