import asyncio
import logging
from rabbitmq import NotificationService

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def main():
    notification_service = NotificationService()
    
    try:
        await notification_service.connect()
        logger.info("Notification service запущен. Для остановки нажмите Ctrl+C")
        
        # Бесконечный цикл для поддержания работы сервиса
        await asyncio.Future()
        
    except asyncio.CancelledError:
        logger.info("Получен сигнал остановки")
    except Exception as e:
        logger.error(f"Ошибка в работе notification service: {e}")
    finally:
        await notification_service.close()
        logger.info("Notification service остановлен")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Приложение остановлено пользователем")