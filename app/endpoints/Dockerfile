FROM python:3.12-alpine

COPY .env /
COPY requirements.txt /
COPY endpoints/ /

RUN pip install --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]

