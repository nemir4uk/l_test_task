FROM python:3.12-alpine

COPY .env /
COPY requirements.txt /
COPY outbox_processor/ /

RUN pip install --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt

CMD ["python", "worker.py"]

