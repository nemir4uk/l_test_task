import enum
from datetime import datetime
from typing import Any, Dict
from pydantic import BaseModel, AwareDatetime
from sqlalchemy import BigInteger, Identity, DateTime, func, Enum, update
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy.dialects.postgresql import JSON
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from rabbit_consumer.config import settings


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


class ConsumedPayload(BaseModel):
    payment_id: int
    amount: int
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
    processed: Mapped[bool]
    created_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), server_default=func.now())


async def change_status(session, payment_id, status) -> None:
    stmt = update(Payments).where(Payments.id == payment_id).values(status=status)
    await session.execute(stmt)
    await session.commit()


async def mark_outbox_message(session, message_id, status) -> None:
    stmt = update(OutboxMessage).where(OutboxMessage.id == message_id).values(processed=status)
    await session.execute(stmt)
    await session.commit()
