from app.database import get_db
from app.rabbitmq import get_rabbitmq
from fastapi import Depends
from sqlalchemy.ext.asyncio import AsyncSession


async def get_rabbitmq_dependency():
    async with get_rabbitmq() as rabbit:
        yield rabbit