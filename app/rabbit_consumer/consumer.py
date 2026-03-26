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
            ttl = math.exp(retry_count) * 1000
            return False, ttl
    except BaseException:
        raise


async def main():
    async def callback_with_retry(message: aio_pika.IncomingMessage):
        async with message.process(ignore_processed=True):
            logger.info("message received")

            headers = message.headers or {}
            death_headers = headers.get("x-death")
            retry_count = 0

            if death_headers:
                retry_count = death_headers[0].get("count", 0)

            max_retries = settings.retry_count
            body = message.body

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
                    logger.error("Failed to process payment -> to retry queue")
                    async with Async_Session_pg() as session:
                        await change_status(session, data.payment_id, "FAILED")
                    await channel.default_exchange.publish(
                        Message(
                            body,
                            headers={
                                "x-retry-count": retry_count,
                                "x-error": "Failed to process payment",
                            },
                            delivery_mode=DeliveryMode.PERSISTENT,
                            expiration=ttl,
                        ),
                        routing_key="failed",
                    )
                    await message.ack()

            except pydantic_core._pydantic_core.ValidationError as e:
                logger.error(f"pydantic ValidationError with message {body}, {e}")
                logger.error("sent to the dead queue")
                await channel.default_exchange.publish(
                    Message(
                        body,
                        headers={
                            "x-retry-count": retry_count,
                            "x-error": "ValidationError",
                        },
                        delivery_mode=DeliveryMode.PERSISTENT,
                    ),
                    routing_key="failed",
                )
                await message.ack()

            except SQLAlchemyError as e:
                logger.info(f"SQLAlchemyError {e}")
                if retry_count < max_retries:
                    await message.reject(requeue=False)
                else:
                    logger.error("SQLAlchemyError, exceeded number of retry -> to dead queue")
                    await channel.default_exchange.publish(
                        Message(
                            body,
                            headers={
                                "x-retry-count": retry_count,
                                "x-error": "Exceeded number of retry",
                            },
                            delivery_mode=DeliveryMode.PERSISTENT,
                        ),
                        routing_key="failed",
                    )
                    await message.ack()

            except Exception as e:
                logging.critical(f"Unexpected error {e}")
                await channel.default_exchange.publish(
                    Message(
                        body,
                        headers={
                            "x-retry-count": retry_count,
                            "x-error": "Unexpected error",
                        },
                        delivery_mode=DeliveryMode.PERSISTENT,
                    ),
                    routing_key="failed",
                )
                await message.ack()

            except BaseException:
                logging.critical(f"Fake payment processing error")

    async with rabbit_connector as connection:
        channel = await connection.channel()
# dead letters
        dlx = await channel.declare_exchange("dlx", type='direct', durable=True)
        dead_queue = await channel.declare_queue("dead_letters", durable=True)
        await dead_queue.bind(dlx, "failed")
# retry
        retry_exchange = await channel.declare_exchange("retry", type='direct', durable=True)
        retry_queue = await channel.declare_queue(
            "retry_exp",
            durable=True,
            arguments={
                "x-dead-letter-exchange": "main",
                "x-dead-letter-routing-key": settings.rabbit_queue,
            },
        )
        await retry_queue.bind(retry_exchange, "retry")
# main
        main_exchange = await channel.declare_exchange("main", type='direct', durable=True)
        queue = await channel.declare_queue(
            settings.rabbit_queue,
            durable=True,
            arguments={
                "x-dead-letter-exchange": "retry",
                "x-dead-letter-routing-key": "retry",
            },
        )
        await queue.bind(main_exchange, settings.rabbit_queue)
        await queue.consume(callback_with_retry)
        logger.info("Start consuming...")
        await asyncio.Future()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass