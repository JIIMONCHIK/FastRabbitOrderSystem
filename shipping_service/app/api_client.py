import aiohttp
import logging
from typing import Dict, Any

logger = logging.getLogger(__name__)


class OrderAPIClient:
    def __init__(self, base_url: str = "http://order_service:8001"):
        self.base_url = base_url
        self.session = None

    async def connect(self):
        """Создание сессии aiohttp"""
        self.session = aiohttp.ClientSession()

    async def close(self):
        """Закрытие сессии aiohttp"""
        if self.session:
            await self.session.close()

    async def update_order_status(self, order_id: int, status: str) -> Dict[str, Any]:
        """Обновление статуса заказа через API order_service"""
        if not self.session:
            await self.connect()
            
        url = f"{self.base_url}/orders/{order_id}/status"
        params = {"status": status}
        
        try:
            async with self.session.patch(url, params=params) as response:
                if response.status == 200:
                    data = await response.json()
                    logger.info(f"Статус заказа {order_id} успешно обновлен на '{status}'")
                    return data
                else:
                    error_text = await response.text()
                    logger.error(f"Ошибка при обновлении статуса заказа {order_id}: {error_text}")
                    return None
        except Exception as e:
            logger.error(f"Исключение при обновлении статуса заказа {order_id}: {e}")
            return None