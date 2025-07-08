import json

import aio_pika
from msfwk.mqclient import MQClient
from msfwk.utils.logging import get_logger
from pydantic import BaseModel

logger = get_logger("mq")


class RabbitMQConfig(BaseModel):
    """RabbitMQ Configuration Constants"""

    geo_extraction_exchange: str = "GeoExtractionExchange"
    geo_extraction_queue: str = "GeoExtractionTasks"
    push_event_routing_key: str = "push.event"


mq_config = RabbitMQConfig()


class GitlabPushEvent(BaseModel):
    """Data model for GitLab push event information"""

    project_id: int
    project_name: str
    project_path: str
    before: str
    after: str
    ref: str


async def setup_rabbitmq():
    """Setup RabbitMQ exchange, queue and binding"""
    try:
        mqclient = MQClient()
        await mqclient.setup()

        if not mqclient.connection or mqclient.connection.is_closed:
            logger.error("Failed to establish RabbitMQ connection")
            return False

        # Declare exchange
        _ = await mqclient.channel.declare_exchange(
            name=mq_config.geo_extraction_exchange, type="direct", durable=True
        )

        # Declare queue
        queue = await mqclient.channel.declare_queue(
            name=mq_config.geo_extraction_queue, durable=True
        )

        # Bind queue to exchange with routing key
        await queue.bind(
            exchange=mq_config.geo_extraction_exchange,
            routing_key=mq_config.push_event_routing_key,
        )

        logger.info(
            "Successfully setup exchange '%s' and queue '%s'",
            mq_config.geo_extraction_exchange,
            mq_config.geo_extraction_queue,
        )
        await mqclient.close()
        return True

    except Exception as e:
        logger.error("Error setting up RabbitMQ: %s", e)
        return False


async def get_rabbitmq_connection():
    """Get a RabbitMQ connection"""
    try:
        mqclient = MQClient()
        await mqclient.setup()

        if not mqclient.connection or mqclient.connection.is_closed:
            logger.error("Failed to establish RabbitMQ connection")
            return None, None

        return mqclient, mqclient.channel
    except Exception as e:
        logger.error("Error establishing RabbitMQ connection: %s", e)
        return None, None


async def publish_gitlab_push_event(event: GitlabPushEvent):
    """Publish GitLab push event to RabbitMQ"""
    try:
        # Get connection and channel
        mqclient, channel = await get_rabbitmq_connection()
        if not mqclient or not channel:
            return False

        message_body = json.dumps(event.model_dump()).encode("utf-8")

        # Get exchange
        exchange = await channel.get_exchange(mq_config.geo_extraction_exchange)

        # Publish message directly
        await exchange.publish(
            aio_pika.Message(
                body=message_body, delivery_mode=aio_pika.DeliveryMode.PERSISTENT
            ),
            routing_key=mq_config.push_event_routing_key,
        )

        logger.info(
            "Successfully published GitLab push event for %s to RabbitMQ",
            event.project_path,
        )

        # Close connection
        await mqclient.close()
        return True

    except Exception as e:
        logger.error("Failed to publish GitLab push event: %s", e)
        return False
