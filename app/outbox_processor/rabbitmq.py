import aio_pika
from config import settings


class AsyncRabbitConnectorClass:
    def __init__(self, host, port, username, password):
        self.host = host
        self.port = port
        self.username = username
        self.password = password

    async def __aenter__(self) -> aio_pika.robust_connection.AbstractRobustConnection:
        self.connection = await aio_pika.connect_robust(host=self.host, port=self.port,
                              login=self.username, password=self.password)
        return self.connection

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.connection.close()


rabbit_connector = AsyncRabbitConnectorClass(settings.rabbit_host, int(settings.rabbit_port),
                                        settings.rabbit_user, settings.rabbit_pass)
