import uvicorn
from typing import Annotated
import logging
from fastapi import FastAPI, Depends, HTTPException, Header, Body, APIRouter
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.exc import DBAPIError
from starlette.requests import Request
from starlette.responses import JSONResponse
from endpoints.db import get_async_session, Payload, get_payment_info_statement, send_to_db
from endpoints.config import settings
from pydantic_core import ValidationError

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


@app.exception_handler(ValidationError)
async def validation_exception_handler(request: Request, exc: ValidationError):
    return JSONResponse(
        status_code=422,
        content={
            "message": "Ошибка валидации данных",
            "errors": exc.errors(),
        },
    )


@router.post("/payments")
async def payment_creation(
        payload: Annotated[Payload, Depends(get_full_data)],
        session: Annotated[AsyncSession, Depends(get_async_session)]
):
    return await send_to_db(payload, session)



@router.get("/payments/{payment_id}")
async def get_payment_info(
        payment_id: int,
        session: Annotated[AsyncSession, Depends(get_async_session)]
):
    return await get_payment_info_statement(payment_id, session)


app.include_router(router)

if __name__ == "__main__":
    uvicorn.run("endpoints.main:app", host="0.0.0.0", port=8000, reload=True)
