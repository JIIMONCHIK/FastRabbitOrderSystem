import asyncio
import logging
from rabbitmq import ShippingService
from database import engine, Base

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def init_db():
    try:
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
        logger.info("База данных shipping_db успешно инициализирована")
    except Exception as e:
        logger.error(f"Ошибка при инициализации базы данных: {e}")


async def main():
    await init_db()
    
    shipping_service = ShippingService()
    
    try:
        await shipping_service.connect()
        logger.info("Shipping service запущен. Для остановки нажмите Ctrl+C")
        
        # Бесконечный цикл для поддержания работы сервиса
        await asyncio.Future()
        
    except asyncio.CancelledError:
        logger.info("Получен сигнал остановки")
    except Exception as e:
        logger.error(f"Ошибка в работе shipping service: {e}")
    finally:
        await shipping_service.close()
        logger.info("Shipping service остановлен")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Приложение остановлено пользователем")