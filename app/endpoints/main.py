from fastapi.encoders import jsonable_encoder
import uvicorn
from typing import Annotated
import logging
from fastapi import FastAPI, Depends, HTTPException, Header, Body, APIRouter
from sqlalchemy import select, func, literal, cast
from sqlalchemy.dialects.postgresql import insert, JSON
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.exc import DBAPIError
from starlette.requests import Request
from starlette.responses import JSONResponse
from db import get_async_session, Payload, Payments, OutboxMessage
from config import settings

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=settings.log_level.upper(),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")

API_KEY = "super-secret-key-123"


async def verify_api_key(request: Request):
    if request.headers.get("X-API-Key") != API_KEY:
        raise HTTPException(status_code=401, detail="Unauthorized")


app = FastAPI(dependencies=[Depends(verify_api_key)])
# app = FastAPI()
router = APIRouter(prefix='/api/v1')


def get_full_data(
        amount: Annotated[int, Body()],
        currency: Annotated[str, Body()],
        description: Annotated[str, Body()],
        metadata: Annotated[dict, Body()],
        idempotency_key: Annotated[str, Header()],
        webhook_url: Annotated[str, Body()]
):
    return Payload(
        amount=amount,
        currency=currency,
        description=description,
        metadata=metadata,
        idempotency_key=idempotency_key,
        webhook_url=webhook_url
    )


@app.exception_handler(DBAPIError)
async def db_api_error_handler(request: Request, exc: DBAPIError):
    logger.error(f"Database error occurred: {exc}")
    return JSONResponse(
        status_code=503,
        content={"detail": "База данных временно недоступна"}
    )


@router.post("/payments")
async def send_to_db(
        payload: Annotated[Payload, Depends(get_full_data)],
        session: Annotated[AsyncSession, Depends(get_async_session)]
):
    async with ((session.begin())):
        payment_insert_stmt = insert(Payments).values(amount=payload.amount,
                                       currency=payload.currency,
                                       description=payload.description,
                                       metadata_info=payload.metadata,
                                       status="PENDING",
                                       idempotency_key=payload.idempotency_key,
                                       webhook_url=payload.webhook_url
                                       ).on_conflict_do_nothing(index_elements=['idempotency_key']
                                                                ).returning(Payments.id,
                                                                            Payments.created_at,
                                                                            Payments.status).cte("new_payment")

        outbox_stmt = insert(OutboxMessage).from_select(
            ["payload", "queue", "processed"], select(
                func.json_build_object(
                    'payment_id', payment_insert_stmt.c.id,
                    "amount", payload.amount,
                    "currency", payload.currency.value,
                    "description", payload.description,
                    "metadata_info", cast(payload.metadata, JSON),
                    "idempotency_key", payload.idempotency_key,
                    "webhook_url", payload.webhook_url
                ),
                literal(settings.rabbit_queue),
                literal(False)
            ).select_from(payment_insert_stmt)
        )

        final_query = (
            select(payment_insert_stmt.c.id, payment_insert_stmt.c.created_at, payment_insert_stmt.c.status)
            .union_all(
                select(Payments.id, Payments.created_at, Payments.status)
                .where(Payments.idempotency_key == payload.idempotency_key)
            )
            .limit(1)
        )
        await session.execute(outbox_stmt)
        result = await session.execute(final_query)
        returned_data = result.fetchone()
        content = jsonable_encoder({
            "payment_id": returned_data.id,
            "status": returned_data.status,
            "created_at": returned_data.created_at
        })
    return JSONResponse(content=content, status_code=202)



@router.get("/paynents/{payment_id}")
async def get_payment_info(
        payment_id: int,
        session: Annotated[AsyncSession, Depends(get_async_session)]
):
    stmt = select(Payments).where(Payments.id == payment_id)
    res = await session.execute(stmt)
    payment_info = res.fetchone()
    return JSONResponse(content=jsonable_encoder(payment_info._asdict()), status_code=200)


app.include_router(router)

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
