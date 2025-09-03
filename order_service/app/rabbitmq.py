import aio_pika
import json
import os
import asyncio
from contextlib import asynccontextmanager
from typing import Optional
import logging


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

RABBITMQ_URL = os.getenv("RABBITMQ_URL")


class RabbitMQ:
    def __init__(self):
        self.connection: Optional[aio_pika.Connection] = None
        self.channel: Optional[aio_pika.Channel] = None
        self.exchange: Optional[aio_pika.Exchange] = None

    async def connect(self, max_retries: int = 5, initial_delay: float = 1.0):
        delay = initial_delay
        for attempt in range(1, max_retries + 1):
            try:
                logger.info(f"Попытка подключения к RabbitMQ ({attempt}/{max_retries})...")
                self.connection = await aio_pika.connect_robust(RABBITMQ_URL)
                self.channel = await self.connection.channel()
                # Declare a direct exchange
                self.exchange = await self.channel.declare_exchange(
                    "orders", aio_pika.ExchangeType.DIRECT, durable=True
                )
                logger.info("Успешное подключение к RabbitMQ!")
                return  # Успех, выходим из функции
            except Exception as e:
                logger.error(f"Ошибка подключения к RabbitMQ (попытка {attempt}/{max_retries}): {e}")
                if attempt == max_retries:
                    logger.error("Все попытки подключения исчерпаны. Завершение работы.")
                    raise  # Если попытки исчерпаны, пробрасываем исключение дальше
                
                logger.info(f"Повторная попытка через {delay} сек...")
                await asyncio.sleep(delay)
                delay *= 2  # Экспоненциальная задержка (backoff)
        
    async def close(self):
        if self.connection:
            await self.connection.close()
            logger.info("Соединение с RabbitMQ закрыто.")

    async def publish_order_event(self, order_data: dict, routing_key: str):
        if not self.connection:
            await self.connect()
            
        message_body = json.dumps(order_data).encode()
        message = aio_pika.Message(
            message_body,
            delivery_mode=aio_pika.DeliveryMode.PERSISTENT
        )
        
        await self.exchange.publish(message, routing_key=routing_key)
        logger.info(f"Сообщение отправлено в RabbitMQ с ключом маршрутизации: '{routing_key}'")


# Глобальный экземпляр RabbitMQ
rabbitmq = RabbitMQ()


@asynccontextmanager
async def get_rabbitmq():
    try:
        await rabbitmq.connect()
        yield rabbitmq
    finally:
        await rabbitmq.close()