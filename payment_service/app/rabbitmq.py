import aio_pika
import json
import os
import asyncio
import logging
from typing import Optional, Callable

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

RABBITMQ_URL = os.getenv("RABBITMQ_URL", "amqp://guest:guest@rabbitmq:5672/")


class PaymentService:
    def __init__(self):
        self.connection: Optional[aio_pika.Connection] = None
        self.channel: Optional[aio_pika.Channel] = None
        self.exchange: Optional[aio_pika.Exchange] = None

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
                
                # Создаем очередь для обработки заказов
                queue = await self.channel.declare_queue("payment_orders", durable=True)
                
                # Привязываем очередь к exchange с routing_key "order.created"
                await queue.bind(self.exchange, routing_key="order.created")
                
                # Начинаем потребление сообщений
                await queue.consume(self.process_order)
                
                logger.info("Payment service подключен к RabbitMQ и готов к работе!")
                return
                
            except Exception as e:
                logger.error(f"Ошибка подключения: {e}")
                if attempt == max_retries:
                    raise
                await asyncio.sleep(delay)
                delay *= 2

    async def process_order(self, message: aio_pika.IncomingMessage):
        """Обработка сообщения о создании заказа"""
        async with message.process():
            try:
                order_data = json.loads(message.body.decode())
                logger.info(f"Получен заказ для обработки платежа: {order_data}")
                
                # Имитация обработки платежа (задержка 2-5 секунд)
                processing_time = 2 + (int(order_data["id"]) % 4)  # Разное время для наглядности
                logger.info(f"Обрабатываю платеж для заказа {order_data['id']}, время: {processing_time}с")
                
                await asyncio.sleep(processing_time)
                
                # В 90% случаев платеж успешен, в 10% - нет
                payment_success = (int(order_data["id"]) % 10) != 0
                
                if payment_success:
                    # Публикуем событие об успешной оплате
                    payment_result = {
                        "order_id": order_data["id"],
                        "status": "paid",
                        "payment_id": f"pay_{order_data['id']}_{os.urandom(4).hex()}",
                        "amount": order_data["total_price"],
                        "timestamp": asyncio.get_event_loop().time()
                    }
                    
                    await self.publish_payment_event(payment_result, "payment.processed")
                    logger.info(f"Платеж для заказа {order_data['id']} успешно обработан")
                else:
                    # Публикуем событие о неудачной оплате
                    payment_result = {
                        "order_id": order_data["id"],
                        "status": "failed",
                        "reason": "Недостаточно средств",
                        "timestamp": asyncio.get_event_loop().time()
                    }
                    
                    await self.publish_payment_event(payment_result, "payment.failed")
                    logger.warning(f"Платеж для заказа {order_data['id']} не прошел")
                    
            except Exception as e:
                logger.error(f"Ошибка при обработке платежа: {e}")

    async def publish_payment_event(self, payment_data: dict, routing_key: str):
        """Публикация события о результате платежа"""
        message_body = json.dumps(payment_data).encode()
        message = aio_pika.Message(
            message_body,
            delivery_mode=aio_pika.DeliveryMode.PERSISTENT
        )
        
        await self.exchange.publish(message, routing_key=routing_key)
        logger.info(f"Событие '{routing_key}' опубликовано для заказа {payment_data['order_id']}")

    async def close(self):
        """Закрытие соединения"""
        if self.connection:
            await self.connection.close()