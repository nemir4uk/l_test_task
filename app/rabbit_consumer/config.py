from os import getenv
from pydantic_settings import BaseSettings
from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv())


class Setting(BaseSettings):
    rabbit_host: str = getenv('RABBITMQ_HOST')
    rabbit_port: str = getenv('RABBITMQ_PORT')
    rabbit_user: str = getenv('RABBITMQ_USER')
    rabbit_pass: str = getenv('RABBITMQ_PASS')
    rabbit_queue: str = getenv('RABBITMQ_QUEUE')

    pg_host: str = getenv('POSTGRES_HOST')
    pg_port: str = getenv('POSTGRES_PORT')
    pg_user: str = getenv('POSTGRES_USER')
    pg_pass: str = getenv('POSTGRES_PASS')
    pg_db: str = getenv('POSTGRES_DB')
    pg_table: str = getenv('POSTGRES_TABLE')

    log_level: str = getenv('LOG_LEVEL')
    retry_count: int = getenv('RETRY_COUNT')


settings = Setting()
