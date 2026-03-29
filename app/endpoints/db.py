import enum
from datetime import datetime
from typing import Any, Dict
from pydantic import BaseModel, AwareDatetime, Field
from sqlalchemy import BigInteger, Identity, DateTime, func, Enum
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy.dialects.postgresql import JSON
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
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
