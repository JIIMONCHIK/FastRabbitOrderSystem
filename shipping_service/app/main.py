import asyncio
import logging
from rabbitmq import ShippingService

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def main():
    shipping_service = ShippingService()
    
    try:
        await shipping_service.connect()
        logger.info("Shipping service запущен. Для остановки нажмите Ctrl+C")
        
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