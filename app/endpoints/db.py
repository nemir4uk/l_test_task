import enum
from datetime import datetime
from typing import Any, Dict
from pydantic import BaseModel, Field
from sqlalchemy import BigInteger, Identity, DateTime, func, Enum, select, literal, cast
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy.dialects.postgresql import insert, JSON
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from starlette.responses import JSONResponse
from fastapi.encoders import jsonable_encoder
from endpoints.config import settings


async_engine_pg = create_async_engine(f'postgresql+asyncpg://{settings.pg_user}:{settings.pg_pass}@{settings.pg_host}:{settings.pg_port}/{settings.pg_db}',
                                      echo=False,
                                      pool_size=50,
                                      max_overflow=50)

Async_Session_pg = async_sessionmaker(async_engine_pg, class_=AsyncSession, expire_on_commit=False)


async def get_async_session():
    async with Async_Session_pg() as async_session:
        yield async_session


class Currency(str, enum.Enum):
    rub = "RUB"
    usd = "USD"
    eur = "EUR"


class Status(str, enum.Enum):
    pending = "PENDING"
    succeeded = "SUCCEDED"
    failed = "FAILED"


class Payload(BaseModel):
    amount: int = Field(gt=0)
    currency: Currency
    description: str
    metadata: Dict[str, Any] = {}
    idempotency_key: str
    webhook_url: str


class PgBase(DeclarativeBase):
    __abstract__ = True

    id: Mapped[int] = mapped_column(BigInteger, Identity(), primary_key=True)


class Payments(PgBase):
    __tablename__ = 'payments'

    amount: Mapped[int]
    currency: Mapped[Currency] = mapped_column(Enum(Currency))
    description: Mapped[str | None]
    metadata_info: Mapped[Dict[str, Any]] = mapped_column(JSON, nullable=False)
    status: Mapped[Status] = mapped_column(Enum(Status))
    idempotency_key: Mapped[str] = mapped_column(unique=True)
    webhook_url: Mapped[str]
    created_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), server_default=func.now())
    processed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class OutboxMessage(PgBase):
    __tablename__ = "outbox_messages"

    payload: Mapped[Dict[str, Any]] = mapped_column(JSON, nullable=False)
    queue: Mapped[str]
    processed: Mapped[bool] = mapped_column(default=False)
    created_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), server_default=func.now())


async def get_payment_info_statement(payment_id, session: Async_Session_pg):
    stmt = select(Payments).where(Payments.id == payment_id)
    res = await session.execute(stmt)
    payment_info = res.fetchone()
    if payment_info:
        return JSONResponse(content=jsonable_encoder(payment_info._asdict()), status_code=200)
    else:
        return JSONResponse(content={'message': 'wrong payment_id'}, status_code=404)


async def send_to_db(payload: Payload, session: Async_Session_pg):
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
