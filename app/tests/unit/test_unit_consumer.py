import asyncio
import json
import time

import aio_pika
import pytest
from faker import Faker
import aiohttp
from contextlib import nullcontext as does_not_raise
from datetime import datetime
from sqlalchemy import text
from rabbit_consumer.config import settings
from rabbit_consumer.db import Async_Session_pg, ConsumedPayload
from rabbit_consumer.rabbitmq import rabbit_connector

fake = Faker()


@pytest.mark.asyncio
@pytest.mark.unit
@pytest.mark.parametrize('queue_name', ['failed', 'retry', settings.rabbit_queue])
async def test_check_queues_exists(queue_name):
    async with rabbit_connector as connection:
        async with connection.channel() as channel:
            try:
                result = await channel.declare_queue(queue_name, passive=True)
                assert True
            except aio_pika.exceptions as e:
                pytest.fail(f"Queue '{queue_name}' was not found: {e}")


@pytest.mark.asyncio
@pytest.mark.parametrize('body',
    [ConsumedPayload(payment_id=fake.pyint(), amount=fake.pyint(), currency="RUB", description=fake.pystr(),
                     metadata_info=fake.pydict(), idempotency_key='1234', webhook_url=fake.pystr()),
    ConsumedPayload(payment_id=fake.pyint(), amount=fake.pyint(), currency="USD", description=fake.pystr(),
                     metadata_info=fake.pydict(), idempotency_key='1234', webhook_url=fake.pystr()),
    ConsumedPayload(payment_id=fake.pyint(), amount=fake.pyint(), currency="EUR", description=fake.pystr(),
                     metadata_info=fake.pydict(), idempotency_key='1234', webhook_url=fake.pystr())
 ])
async def test_correct_message_consuming(body):
    async with rabbit_connector as connection:
        channel = await connection.channel()
        try:
            await channel.default_exchange.publish(
                aio_pika.Message(body=body.model_dump_json().encode()),
                routing_key=settings.rabbit_queue,
            )
            assert True
        except aio_pika.exceptions as e:
            pytest.fail(f"message '{body}' was not published: {e}")


@pytest.mark.asyncio
@pytest.mark.parametrize('body',
    [str({'payment_id':fake.pyint(), 'amount':fake.pyint(), 'currency':"RUB77777", 'description':fake.pystr(),
                     'metadata_info':fake.pydict(), 'idempotency_key':fake.pyint(), 'webhook_url':fake.pystr()}),
    str({'payment_id':fake.pyint(), 'amount':fake.pystr(), 'currency':"USD", 'description':fake.pystr(),
                     'metadata_info':fake.pydict(), 'idempotency_key':fake.pyint(), 'webhook_url':fake.pystr()}),
    str({'payment_id':fake.pydict(), 'amount':fake.pyint(), 'currency':"EUR", 'description':fake.pystr(),
                     'metadata_info':fake.pydict(), 'idempotency_key':fake.pydict(), 'webhook_url':fake.pystr()})
 ])
async def test_incorrect_message_consuming(body):
    async with rabbit_connector as connection:
        channel = await connection.channel()
        try:
            queue_info_before = await channel.declare_queue("failed", passive=True)
            count_before = queue_info_before.declaration_result.message_count
            message = await channel.default_exchange.publish(
                aio_pika.Message(body=json.dumps(body).encode()),
                routing_key=settings.rabbit_queue,
            )
            start = time.monotonic()
            while time.monotonic() - start < 5:
                queue = await channel.declare_queue("failed", passive=True)
                count_after = queue.declaration_result.message_count
                if count_after >= count_before:
                    assert True
                await asyncio.sleep(0.2)
        except Exception as e:
            pytest.fail(f"message publish exception with body: '{body}' \n {e}")

