import aio_pika
import json
import os
import asyncio
import logging
from typing import Optional, Dict, Any
from api_client import OrderAPIClient

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

RABBITMQ_URL = os.getenv("RABBITMQ_URL", "amqp://guest:guest@rabbitmq:5672/")


class ShippingService:
    def __init__(self):
        self.connection: Optional[aio_pika.Connection] = None
        self.channel: Optional[aio_pika.Channel] = None
        self.exchange: Optional[aio_pika.Exchange] = None
        self.api_client = OrderAPIClient()

    async def connect(self):
        max_retries = 5
        delay = 1.0
        
        for attempt in range(1, max_retries + 1):
            try:
                logger.info(f"Попытка подключения к RabbitMQ ({attempt}/{max_retries})...")
                
                self.connection = await aio_pika.connect_robust(RABBITMQ_URL)
                self.channel = await self.connection.channel()
                
                # Объявляем exchange
                self.exchange = await self.channel.declare_exchange(
                    "orders", aio_pika.ExchangeType.DIRECT, durable=True
                )
                
                # Создаем очередь для обработки оплаченных заказов
                queue = await self.channel.declare_queue("shipping_orders", durable=True)
                
                # Привязываем очередь к exchange с routing_key "payment.processed"
                await queue.bind(self.exchange, routing_key="payment.processed")
                
                # Начинаем потребление сообщений
                await queue.consume(self.process_payment)
                
                # Подключаем API клиент
                await self.api_client.connect()
                
                logger.info("Shipping service подключен к RabbitMQ и готов к работе!")
                return
                
            except Exception as e:
                logger.error(f"Ошибка подключения: {e}")
                if attempt == max_retries:
                    raise
                await asyncio.sleep(delay)
                delay *= 2

    async def process_payment(self, message: aio_pika.IncomingMessage):
        """Обработка сообщения об успешной оплате"""
        async with message.process():
            try:
                payment_data = json.loads(message.body.decode())
                logger.info(f"Получено уведомление об оплате заказа: {payment_data}")
                
                # Имитация процесса доставки
                order_id = payment_data["order_id"]
                shipping_time = 3 + (order_id % 4)  # Разное время для наглядности
                logger.info(f"Начинаю обработку доставки заказа {order_id}, время: {shipping_time}с")
                
                await asyncio.sleep(shipping_time)
                
                # Обновляем статус заказа через API order_service
                result = await self.api_client.update_order_status(order_id, "shipped")
                
                if result:
                    # Публикуем событие о доставке заказа
                    shipping_data = {
                        "order_id": order_id,
                        "status": "shipped",
                        "shipping_id": f"ship_{order_id}_{os.urandom(4).hex()}",
                        "estimated_delivery": "2023-12-25",  # Примерная дата доставки
                        "timestamp": asyncio.get_event_loop().time()
                    }
                    
                    await self.publish_shipping_event(shipping_data, "order.shipped")
                    logger.info(f"Доставка заказа {order_id} успешно обработана")
                else:
                    logger.error(f"Не удалось обновить статус заказа {order_id}")
                    
            except Exception as e:
                logger.error(f"Ошибка при обработке доставки: {e}")

    async def publish_shipping_event(self, shipping_data: Dict[str, Any], routing_key: str):
        """Публикация события о доставке"""
        message_body = json.dumps(shipping_data).encode()
        message = aio_pika.Message(
            message_body,
            delivery_mode=aio_pika.DeliveryMode.PERSISTENT
        )
        
        await self.exchange.publish(message, routing_key=routing_key)
        logger.info(f"Событие '{routing_key}' опубликовано для заказа {shipping_data['order_id']}")

    async def close(self):
        """Закрытие соединений"""
        if self.connection:
            await self.connection.close()
        await self.api_client.close()