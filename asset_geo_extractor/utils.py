from typing import TYPE_CHECKING

from msfwk.request import HttpClient
from msfwk.utils.logging import get_logger

if TYPE_CHECKING:
    pass

logger = get_logger("assets")


class RepositoryRetrievalError(Exception):
    """Raised when we can't retrieve the repository"""


async def _list_storage(repository_id: int) -> list[dict]:
    http_client = HttpClient()
    # Call the search service to get the asset files
    async with (
        http_client.get_service_session("storage") as session,
        session.get(
            f"/repositories/{repository_id}",
        ) as response,
    ):
        if response.status not in (200, 201):
            message = f"Storage service answered {response.status}"
            logger.error(
                "Failed to list repository files %s, %s",
                response.status,
                await response.text(),
            )
            raise RepositoryRetrievalError(message)
        return (await response.json())["data"]["files"]