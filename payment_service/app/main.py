import asyncio
import logging
from rabbitmq import PaymentService

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def main():
    payment_service = PaymentService()
    
    try:
        await payment_service.connect()
        logger.info("Payment service запущен. Для остановки нажмите Ctrl+C")
        
        # Бесконечный цикл для поддержания работы сервиса
        await asyncio.Future()
        
    except asyncio.CancelledError:
        logger.info("Получен сигнал остановки")
    except Exception as e:
        logger.error(f"Ошибка в работе payment service: {e}")
    finally:
        await payment_service.close()
        logger.info("Payment service остановлен")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Приложение остановлено пользователем")