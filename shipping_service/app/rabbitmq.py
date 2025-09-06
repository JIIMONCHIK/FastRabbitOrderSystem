import aio_pika
import json
import os
import asyncio
import logging
from typing import Optional, Dict, Any
from api_client import OrderAPIClient
from database import AsyncSessionLocal
from models import Shipping, ShippingStatus
from sqlalchemy import text, select

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

                # Создаем очередь для обработки удаленных заказов
                delete_queue = await self.channel.declare_queue("shipping_deletes", durable=True)

                await delete_queue.bind(self.exchange, routing_key="order.deleted")
                
                await delete_queue.consume(self.process_order_delete)
                
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

    async def create_shipping_record(self, order_id: int, user_id: int, total_price: float):
        """Создание записи о доставке в базе данных"""
        async with AsyncSessionLocal() as session:
            try:
                # Проверяем, нет ли уже записи для этого заказа
                existing_shipping = await session.execute(
                    text("SELECT * FROM shippings WHERE order_id = :order_id"),
                    {"order_id": order_id}
                )
                if existing_shipping.fetchone():
                    logger.info(f"Запись о доставке для заказа {order_id} уже существует")
                    return
                
                # Создаем новую запись о доставке
                shipping_cost = total_price * 0.1  # 10% от стоимости заказа
                shipping = Shipping(
                    order_id=order_id,
                    user_id=user_id,
                    address="123 Main St, City, Country",  # В реальном приложении это бы приходило из заказа
                    shipping_cost=shipping_cost,
                    status=ShippingStatus.PROCESSING
                )
                
                session.add(shipping)
                await session.commit()
                logger.info(f"Создана запись о доставке для заказа {order_id}")
                
            except Exception as e:
                logger.error(f"Ошибка при создании записи о доставке: {e}")
                await session.rollback()

    async def update_shipping_status(self, order_id: int, status: ShippingStatus, tracking_number: str = None):
        """Обновление статуса доставки в базе данных"""
        async with AsyncSessionLocal() as session:
            try:
                # Находим запись о доставке
                result = await session.execute(
                    select(Shipping).where(Shipping.order_id == order_id)
                )
                shipping = result.scalar_one_or_none()
                
                if shipping:
                    # Обновляем статус
                    shipping.status = status
                    shipping.tracking_number = tracking_number
                    await session.commit()
                    logger.info(f"Статус доставки для заказа {order_id} обновлен на '{status.value}'")
                else:
                    logger.warning(f"Запись о доставке для заказа {order_id} не найдена")
                    
            except Exception as e:
                logger.error(f"Ошибка при обновлении статуса доставки: {e}")
                await session.rollback()

    async def process_payment(self, message: aio_pika.IncomingMessage):
        """Обработка сообщения об успешной оплате"""
        async with message.process():
            try:
                payment_data = json.loads(message.body.decode())
                logger.info(f"Получено уведомление об оплате заказа: {payment_data}")
                
                # Создаем запись о доставке в базе данных
                order_id = payment_data["order_id"]
                user_id = payment_data.get("user_id", 1)
                total_price = payment_data.get("amount", 0)
                
                await self.create_shipping_record(order_id, user_id, total_price)
                
                # Имитация процесса доставки
                shipping_time = 3 + (order_id % 4)  # Разное время для наглядности
                logger.info(f"Начинаю обработку доставки заказа {order_id}, время: {shipping_time}с")
                
                await asyncio.sleep(shipping_time)
                
                # Обновляем статус заказа через API order_service
                result = await self.api_client.update_order_status(order_id, "shipped")
                
                if result:
                    # Обновляем статус доставки в нашей БД
                    tracking_number = f"TRACK_{order_id}_{os.urandom(4).hex().upper()}"
                    await self.update_shipping_status(order_id, ShippingStatus.SHIPPED, tracking_number)
                    
                    # Публикуем событие о доставке заказа
                    shipping_data = {
                        "order_id": order_id,
                        "status": "shipped",
                        "tracking_number": tracking_number,
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
    
    async def process_order_delete(self, message: aio_pika.IncomingMessage):
        """Обработка сообщения об удалении заказа"""
        async with message.process():
            try:
                order_data = json.loads(message.body.decode())
                order_id = order_data["id"]
                
                # Удаляем запись о доставке из БД
                await self.delete_shipping_record(order_id)
                
                logger.info(f"Запись о доставке для заказа {order_id} удалена")
                
            except Exception as e:
                logger.error(f"Ошибка при удалении записи о доставке: {e}")

    async def delete_shipping_record(self, order_id: int):
        """Удаление записи о доставке из базы данных"""
        async with AsyncSessionLocal() as session:
            try:
                # Находим запись о доставке
                result = await session.execute(
                    select(Shipping).where(Shipping.order_id == order_id)
                )
                shipping = result.scalar_one_or_none()
                
                if shipping:
                    # Удаляем запись
                    await session.delete(shipping)
                    await session.commit()
                    logger.info(f"Запись о доставке для заказа {order_id} удалена из БД")
                else:
                    logger.warning(f"Запись о доставке для заказа {order_id} не найдена")
                    
            except Exception as e:
                logger.error(f"Ошибка при удалении записи о доставке: {e}")
                await session.rollback()

    async def close(self):
        """Закрытие соединений"""
        if self.connection:
            await self.connection.close()
        await self.api_client.close()