import logging
from typing import Annotated

from fastapi import Depends, HTTPException, Request, status
from fastapi.responses import JSONResponse
from msfwk.application import app, openapi_extra
from msfwk.context import current_config, register_destroy, register_init
from msfwk.mqclient import load_default_rabbitmq_config
from msfwk.utils.logging import get_logger

from asset_geo_extractor.gitlab import GitlabConfig, get_gitlab_config
from asset_geo_extractor.mq import (
    GitlabPushEvent,
    publish_gitlab_push_event,
    setup_rabbitmq,
)
from asset_geo_extractor.workers.geo_worker import setup_worker, shutdown_worker



logger = get_logger("application")

logging.getLogger("aiormq.connection").setLevel(logging.INFO)


async def init(config: dict) -> bool:
    """Init"""
    logger.info("Initialising Asset Geo Extractor...")
    load_succeded = load_default_rabbitmq_config()
    current_config.set(config)

    # Setup RabbitMQ exchange, queue and binding
    rabbitmq_setup = await setup_rabbitmq()
    if not rabbitmq_setup:
        logger.warning("Failed to setup RabbitMQ exchange and queue")

    # Start the geo extraction worker
    worker_setup = await setup_worker()
    if not worker_setup:
        logger.warning("Failed to setup geo extraction worker")

    return load_succeded


async def destroy():
    """Destroy"""
    logger.info("Destroying Asset Geo Extractor...")

    # Shutdown the geo extraction worker
    await shutdown_worker()


register_init(init)
register_destroy(destroy)


@app.route("/health")
async def health():
    return {"status": "ok"}


@app.post("/webhook/gitlab/push",
           openapi_extra=openapi_extra(secured=True, internal=True),)
async def gitlab_webhook(request: Request, gitlab_config: Annotated[GitlabConfig, Depends(get_gitlab_config)]):
    # Verify GitLab webhook token
    # if not x_gitlab_token or x_gitlab_token != gitlab_config.token:
    #     raise HTTPException(status_code=401, detail="Invalid GitLab token")

    # Parse request body
    payload = await request.json()

    # Validate required fields
    if not payload.get("project") or not payload.get("before") or not payload.get("after") or not payload.get("ref"):
        raise HTTPException(status_code=400, detail="Missing required fields in webhook payload")

    # Extract relevant data
    project_id = payload["project"].get("id")
    project_name = payload["project"].get("name")
    project_path = payload["project"].get("path_with_namespace")
    before_sha = payload["before"]
    after_sha = payload["after"]
    ref = payload["ref"]

    logger.info(
        f"Received GitLab push event: project={project_path}, ref={ref}, before={before_sha}, after={after_sha}"
    )

    try:
        # Create GitlabPushEvent and publish
        event = GitlabPushEvent(
            project_id=project_id,
            project_name=project_name,
            project_path=project_path,
            before=before_sha,
            after=after_sha,
            ref=ref
        )

        success = await publish_gitlab_push_event(event)

        if not success:
            raise Exception("Failed to publish event")

        logger.info("Successfully queued job for project %s", project_id)

        # Return 202 Accepted
        return JSONResponse(
            status_code=status.HTTP_202_ACCEPTED, content={"status": "accepted", "message": "Job queued for processing"}
        )

    except Exception:
        # Use logging.exception for unnamed exceptions
        logger.exception("Failed to queue job:")
        raise HTTPException(status_code=500, detail="Failed to queue job for processing")
