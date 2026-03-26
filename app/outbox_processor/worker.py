import json
import aio_pika
from sqlalchemy import select
from db import OutboxMessage, get_async_session
from rabbitmq import rabbit_connector
import asyncio
import logging
from config import settings

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=settings.log_level.upper(),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")


async def outbox_worker():
    async with rabbit_connector as connection:
        channel = await connection.channel()
        async for session in get_async_session():
            while True:
                async with session.begin():
                    stmt = select(OutboxMessage).where(OutboxMessage.processed == False).limit(50).with_for_update(skip_locked=True)
                    result = await session.execute(stmt)
                    rows = result.scalars().all()

                    for row in rows:
                        try:
                            await channel.default_exchange.publish(
                                aio_pika.Message(body=json.dumps(row.payload).encode()),
                                routing_key=settings.rabbit_queue,
                            )
                            row.processed = True
                        except Exception as e:
                            logger.error(f"Failed to publish message {row.id}: {e}")

                await asyncio.sleep(0.5)

if __name__ == "__main__":
    asyncio.run(outbox_worker())
