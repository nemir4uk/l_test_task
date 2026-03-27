from fastapi.encoders import jsonable_encoder
import uvicorn
from typing import Annotated
import logging
from fastapi import FastAPI, Depends, HTTPException, Header, Body, APIRouter
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.exc import IntegrityError
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


# app = FastAPI(dependencies=[Depends(verify_api_key)])
app = FastAPI()
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


@router.post("/payments")
async def send_to_db(
        payload: Annotated[Payload, Depends(get_full_data)],
        session: Annotated[AsyncSession, Depends(get_async_session)]
):
    try:
        async with session.begin():
            stmt = insert(Payments).values(amount=payload.amount,
                                           currency=payload.currency,
                                           description=payload.description,
                                           metadata_info=payload.metadata,
                                           status="PENDING",
                                           idempotency_key=payload.idempotency_key,
                                           webhook_url=payload.webhook_url).returning(Payments.id, Payments.created_at)
            result = await session.execute(stmt)
            returned_data = result.first()
            outbox_payload = {
                "payment_id": returned_data.id,
                "amount": payload.amount,
                "currency": payload.currency,
                "description": payload.description,
                "metadata_info": payload.metadata,
                "idempotency_key": payload.idempotency_key,
                "webhook_url": payload.webhook_url
            }

            outbox_stmt = insert(OutboxMessage).values(
                payload=outbox_payload,
                queue=settings.rabbit_queue
            )
            await session.execute(outbox_stmt)
            content = jsonable_encoder({
                "payment_id": returned_data.id,
                "status": "PENDING",
                "created_at": returned_data.created_at
            })
        return JSONResponse(content=content, status_code=202)
    except IntegrityError:
        return JSONResponse(content={"message": "Duplicate Order"}, status_code=409)


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
