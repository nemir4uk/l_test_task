FROM python:3.12-alpine

COPY .env /
COPY requirements.txt /
COPY endpoints/ /endpoints/

RUN pip install --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt

CMD ["python", "-m", "endpoints.main"]

