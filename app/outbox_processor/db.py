import enum
from datetime import datetime
from typing import Any, Dict
from pydantic import BaseModel
from sqlalchemy import BigInteger, Identity, DateTime, func
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy.dialects.postgresql import JSON
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from config import settings


async_engine_pg = create_async_engine(f'postgresql+asyncpg://{settings.pg_user}:{settings.pg_pass}@{settings.pg_host}:{settings.pg_port}/{settings.pg_db}',
                                      echo=False)

Async_Session_pg = async_sessionmaker(async_engine_pg, class_=AsyncSession, expire_on_commit=False)


async def get_async_session():
    async with Async_Session_pg() as async_session:
        yield async_session


class Currency(str, enum.Enum):
    rub = "RUB"
    usd = "USD"
    eur = "EUR"


class PaymentMessage(BaseModel):
    payload: Dict[str, Any] = {}
    queue: str
    processed: bool
    created_at: datetime


class PgBase(DeclarativeBase):
    __abstract__ = True

    id: Mapped[int] = mapped_column(BigInteger, Identity(), primary_key=True)


class OutboxMessage(PgBase):
    __tablename__ = "outbox_messages"

    payload: Mapped[Dict[str, Any]] = mapped_column(JSON, nullable=False)
    queue: Mapped[str]
    processed: Mapped[bool] = mapped_column(default=False)
    created_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), server_default=func.now())
