import pytest
import aiohttp
from contextlib import nullcontext as does_not_raise
from sqlalchemy import text
from endpoints.config import settings
from endpoints.db import Async_Session_pg

correct_body = {
        "amount": 10,
        "currency": "RUB",
        "description": "string",
        "metadata": {
            "additionalProp1": {}
        },
        "webhook_url": "string"
    }
correct_header = {"X-API-Key": "super-secret-key-123",
                  'idempotency-key': '956456855'}


@pytest.mark.asyncio
@pytest.mark.unit
@pytest.mark.parametrize(
    'api_key, body, expectation, expected_result',
    [
        ("super-secret-key-123", correct_body, does_not_raise(), 202),
        ("wrong-api-key", correct_body, does_not_raise(), 401)
    ]
)
async def test_check_api_key(api_key, body, expectation, expected_result):
    custom_headers = {"X-API-Key": api_key,
                      'idempotency-key': '956456855'}
    with expectation:
        async with aiohttp.ClientSession() as session:
            async with session.post(f"http://{settings.endpoint_host}:{settings.endpoint_port}/api/v1/payments",
                                    headers=custom_headers, json=body) as response:
                assert response.status == expected_result


@pytest.mark.asyncio
@pytest.mark.unit
@pytest.mark.parametrize(
    'header, body, expectation, expected_result',
    [
        (correct_header, correct_body, does_not_raise(), 202),
        (correct_header, {
            "amount": '10',
            "currency": "RUB",
            "description": "string",
            "metadata": {
                "additionalProp1": {}
            },
            "webhook_url": "string"
        }, does_not_raise(), 202),
        (correct_header, {
            "amount": '10zzz',
            "currency": "RUB",
            "description": "string",
            "metadata": {
                "additionalProp1": {}
            },
            "webhook_url": "string"
        }, does_not_raise(), 422),
        (correct_header, {
            "amount": 10,
            "currency": "RUBqaz",
            "description": "string",
            "metadata": {
                "additionalProp1": {}
            },
            "webhook_url": "string"
        }, does_not_raise(), 422),
        (correct_header, {
            "amount": 10,
            "currency": "RUB",
            "description": 555,
            "metadata": {
                "additionalProp1": {}
            },
            "webhook_url": "string"
        }, does_not_raise(), 422),
        (correct_header, {
            "amount": 10,
            "currency": "RUB",
            "description": "string",
            "metadata": [
                "additionalProp1"
            ],
            "webhook_url": "string"
        }, does_not_raise(), 422),
        (correct_header, {
            "amount": 10,
            "currency": "RUB",
            "description": "string",
            "metadata": {
                "additionalProp1": {}
            },
            "webhook_url": 555
        }, does_not_raise(), 422),
    ]
)
async def test_check_validation(header, body, expectation, expected_result):
    with expectation:
        async with aiohttp.ClientSession() as session:
            async with session.post(f"http://{settings.endpoint_host}:{settings.endpoint_port}/api/v1/payments",
                                    headers=header, json=body) as response:
                assert response.status == expected_result


@pytest.mark.asyncio
@pytest.mark.unit
@pytest.mark.parametrize(
    'header, body, expectation, expected_result',
    [(correct_header, correct_body, does_not_raise(), 200)]
)
async def test_check_create_payment_and_outboxmessage(header, body, expectation, expected_result):
    with expectation:
        async with aiohttp.ClientSession() as session:
            async with session.post(f"http://{settings.endpoint_host}:{settings.endpoint_port}/api/v1/payments",
                                    headers=header, json=body) as response_post:
                result_post = await response_post.json()
            async with session.get(f"http://{settings.endpoint_host}:{settings.endpoint_port}/api/v1/payments/{result_post['payment_id']}",
                                   headers=header) as response_get:
                result_get = await response_get.json()
                assert response_get.status == expected_result, 'Wrong response_get.status'
                assert result_post['payment_id'] == result_get['Payments']['id'], 'Wrong ID'
                assert result_post['created_at'] == result_get['Payments']['created_at'], 'Wrong created_at'
            async with Async_Session_pg() as session:
                stmt = text(f"""select payload from outbox_messages where payload->>'payment_id' = '{result_post['payment_id']}'""")
                row_select = await session.execute(stmt)
                result_select = row_select.fetchone()[0]
                assert result_select['payment_id'] == result_post['payment_id']


