import asyncio
import json
import logging
import aio_pika
import random
import math
from aio_pika import Message, DeliveryMode
import pydantic_core
from sqlalchemy.exc import SQLAlchemyError
from rabbit_consumer.rabbitmq import rabbit_connector
from rabbit_consumer.db import Async_Session_pg, ConsumedPayload, change_status, mark_outbox_message
from rabbit_consumer.config import settings

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=settings.log_level.upper(),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)


async def fake_process_payment(retry_count):
    try:
        delay = random.uniform(2,5)
        await asyncio.sleep(delay)
        success_chance = random.uniform(0,1)
        if success_chance > 0.1:
            return True, 0
        else:
            ttl = math.exp(retry_count + 1)
            return False, ttl
    except BaseException:
        raise


async def main():
    async def publish_failed(body: bytes, retry_count: int, error: str):
        await channel.default_exchange.publish(
            Message(
                body=body,
                headers={
                    "x-retry-count": retry_count,
                    "x-error": error,
                },
                delivery_mode=DeliveryMode.PERSISTENT,
            ),
            routing_key="failed",
        )

    async def publish_retry(body: bytes, retry_count: int, delay: int):
        await channel.default_exchange.publish(
            Message(
                body=body,
                headers={
                    "x-retry-count": retry_count,
                },
                expiration=delay,
                delivery_mode=DeliveryMode.PERSISTENT,
            ),
            routing_key="retry",
        )

    async def callback_with_retry(message: aio_pika.IncomingMessage):
        async with message.process(ignore_processed=True):
            logger.info("message received")

            headers = message.headers or {}
            retry_count = headers.get("x-retry-count", 0)

            max_retries = settings.retry_count
            body = message.body
            logger.info(f'retry_count {retry_count}')

            try:
                data = ConsumedPayload.model_validate_json(body)
                success, ttl = await fake_process_payment(retry_count)
                if success:
                    logger.info("Success process payment")
                    async with Async_Session_pg() as session:
                        await change_status(session, data.payment_id, "SUCCEDED")
                        await mark_outbox_message(session, data.payment_id, True)
                    await message.ack()
                else:
                    if retry_count < max_retries:
                        logger.error("Failed to process payment -> to retry queue")
                        await publish_retry(body, retry_count + 1, ttl)
                    else:
                        logger.error("Failed to process payment max_retries exceeded -> to dlx queue")
                        async with Async_Session_pg() as session:
                            await change_status(session, data.payment_id, "FAILED")
                        await publish_failed(body, retry_count, "Failed to process payment max_retries exceeded")
                        await message.ack()

            except pydantic_core._pydantic_core.ValidationError as e:
                logger.error(f"pydantic ValidationError with message {body}, {e}")
                logger.error("sent to the dead queue")
                await publish_failed(body, retry_count, "ValidationError")
                await message.ack()

            except SQLAlchemyError as e:
                logger.info(f"SQLAlchemyError {e}")
                if retry_count < max_retries:
                    await publish_retry(body, retry_count + 1, ttl)
                else:
                    logger.error("SQLAlchemyError, exceeded number of retry -> to dead queue")
                    await publish_failed(body, retry_count, "SQLAlchemyError max_retries exceeded")
                    await message.ack()

            except Exception as e:
                logging.critical(f"Unexpected error {e}")
                await publish_failed(body, retry_count, f"Unexpected error {e}")
                await message.ack()

            except BaseException:
                logging.critical(f"Fake payment processing error")

    async with rabbit_connector as connection:
        channel = await connection.channel()
# dead letters
        failed_queue = await channel.declare_queue("failed", durable=True)
# retry
        retry_queue = await channel.declare_queue(
            "retry",
            durable=True,
            arguments={
                "x-dead-letter-exchange": "",
                "x-dead-letter-routing-key": settings.rabbit_queue,
            },
        )
# main
        main_queue = await channel.declare_queue(
            settings.rabbit_queue,
            durable=True,
        )
        await main_queue.consume(callback_with_retry)
        logger.info("Consumer started")
        await asyncio.Future()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass