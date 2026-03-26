FROM python:3.12-alpine

COPY .env /
COPY requirements.txt /
COPY rabbit_consumer/ /rabbit_consumer/
COPY alembic_folder/ /alembic_folder/
COPY *.ini /

RUN pip install --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt



