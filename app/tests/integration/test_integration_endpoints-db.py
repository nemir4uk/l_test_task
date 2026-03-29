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
    'header, body, expectation, expected_result',
    [
        (correct_header, correct_body, does_not_raise(), 202),
    ]
)
async def test_check_idempotency_key(header, body, expectation, expected_result):
    with expectation:
        async with aiohttp.ClientSession() as session:
            async with session.post(f"http://{settings.endpoint_host}:{settings.endpoint_port}/api/v1/payments",
                                    headers=header, json=body) as response:
                assert response.status == expected_result
                result = await response.json()
            async with session.post(f"http://{settings.endpoint_host}:{settings.endpoint_port}/api/v1/payments",
                                    headers=header, json=body) as response2:
                assert response.status == expected_result
                result2 = await response2.json()
                assert result['payment_id'] == result2['payment_id']
            async with Async_Session_pg() as session:
                stmt = text(
                    f"""select payload from outbox_messages where payload->>'payment_id' = '{result['payment_id']}'""")
                row_select = await session.execute(stmt)
                result_outbox_messages = row_select.fetchall()
                assert len(result_outbox_messages) == 1
                stmt2 = text(
                    f"""select * from payments where id = '{result['payment_id']}'""")
                row_select = await session.execute(stmt2)
                result_payments = row_select.fetchall()
                assert len(result_payments) == 1

