import aio_pika
import json
import os
import asyncio
import logging
from typing import Optional, Dict, Any

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

RABBITMQ_URL = os.getenv("RABBITMQ_URL", "amqp://guest:guest@rabbitmq:5672/")

class NotificationService:
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
                
                # Создаем и привязываем очереди для разных типов уведомлений
                queues_and_bindings = [
                    ("notifications_orders", "order.created"),
                    ("notifications_payments", "payment.processed"),
                    ("notifications_payments", "payment.failed")
                ]
                
                for queue_name, routing_key in queues_and_bindings:
                    queue = await self.channel.declare_queue(queue_name, durable=True)
                    await queue.bind(self.exchange, routing_key=routing_key)
                    await queue.consume(self.process_notification)
                    logger.info(f"Подписались на уведомления: {routing_key}")
                
                logger.info("Notification service подключен к RabbitMQ и готов к работе!")
                return
                
            except Exception as e:
                logger.error(f"Ошибка подключения: {e}")
                if attempt == max_retries:
                    raise
                await asyncio.sleep(delay)
                delay *= 2
    
    async def process_notification(self, message: aio_pika.IncomingMessage):
        """Обработка уведомлений"""
        async with message.process():
            try:
                event_data = json.loads(message.body.decode())
                routing_key = message.routing_key
                
                # Определяем тип уведомления по routing key
                if routing_key == "order.created":
                    await self.send_order_created_notification(event_data)
                elif routing_key == "payment.processed":
                    await self.send_payment_processed_notification(event_data)
                elif routing_key == "payment.failed":
                    await self.send_payment_failed_notification(event_data)
                    
                logger.info(f"Обработано уведомление: {routing_key}")
                
            except Exception as e:
                logger.error(f"Ошибка при обработке уведомления: {e}")

    async def send_order_created_notification(self, order_data: Dict[str, Any]):
        """Уведомление о создании заказа"""
        # В реальном приложении здесь была бы отправка email/SMS/push
        logger.info(f"📧 УВЕДОМЛЕНИЕ: Создан новый заказ #{order_data['id']}")
        logger.info(f"   Пользователь: {order_data['user_id']}")
        logger.info(f"   Товар: {order_data['product_id']}")
        logger.info(f"   Количество: {order_data['quantity']}")
        logger.info(f"   Сумма: {order_data['total_price']}")
        logger.info(f"   Статус: {order_data['status']}")

    async def send_payment_processed_notification(self, payment_data: Dict[str, Any]):
        """Уведомление об успешной оплате"""
        logger.info(f"✅ УВЕДОМЛЕНИЕ: Платеж успешно обработан")
        logger.info(f"   Заказ: #{payment_data['order_id']}")
        logger.info(f"   ID платежа: {payment_data.get('payment_id', 'N/A')}")
        logger.info(f"   Сумма: {payment_data.get('amount', 'N/A')}")

    async def send_payment_failed_notification(self, payment_data: Dict[str, Any]):
        """Уведомление о неудачной оплате"""
        logger.info(f"❌ УВЕДОМЛЕНИЕ: Платеж не прошел")
        logger.info(f"   Заказ: #{payment_data['order_id']}")
        logger.info(f"   Причина: {payment_data.get('reason', 'Неизвестно')}")

    async def close(self):
        """Закрытие соединения"""
        if self.connection:
            await self.connection.close()