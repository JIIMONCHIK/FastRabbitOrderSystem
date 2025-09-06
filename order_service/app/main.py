from contextlib import asynccontextmanager
from fastapi import FastAPI, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from typing import List

from app import models
from app import schemas
from app.database import engine, get_db
from app.rabbitmq import RabbitMQ

rabbitmq = RabbitMQ()


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup: создаем таблицы и подключаемся к RabbitMQ
    async with engine.begin() as conn:
        await conn.run_sync(models.Base.metadata.create_all)
    await rabbitmq.connect()
    yield
    # Shutdown: закрываем соединение с RabbitMQ
    await rabbitmq.close()


app = FastAPI(title="Order Service", version="1.0.0", lifespan=lifespan)


@app.post("/orders/", response_model=schemas.OrderResponse, status_code=status.HTTP_201_CREATED)
async def create_order(
    order: schemas.OrderCreate,
    db: AsyncSession = Depends(get_db),
):
    # Создаем заказ в БД
    db_order = models.Order(**order.model_dump())
    db.add(db_order)
    await db.commit()
    await db.refresh(db_order)
    
    # Публикуем событие о создании заказа
    order_data = db_order.to_dict()
    await rabbitmq.publish_order_event(order_data, "order.created")
    
    return db_order


@app.get("/orders/", response_model=List[schemas.OrderResponse])
async def get_orders(db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(models.Order))
    orders = result.scalars().all()
    return orders


@app.get("/orders/{order_id}", response_model=schemas.OrderResponse)
async def get_order(order_id: int, db: AsyncSession = Depends(get_db)):
    result = await db.execute(
        select(models.Order).where(models.Order.id == order_id)
    )
    order = result.scalar_one_or_none()
    
    if order is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Order not found"
        )
    
    return order


@app.patch("/orders/{order_id}/status", response_model=schemas.OrderResponse)
async def update_order_status(
    order_id: int, 
    status: schemas.OrderStatus,
    db: AsyncSession = Depends(get_db)
):
    result = await db.execute(
        select(models.Order).where(models.Order.id == order_id)
    )
    order = result.scalar_one_or_none()
    
    if order is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Order not found"
        )
    
    # Преобразуем статус из схемы в модельный enum
    order.status = models.OrderStatus(status.value)
    await db.commit()
    await db.refresh(order)
    
    # Публикуем событие об изменении статуса заказа
    order_data = order.to_dict()
    await rabbitmq.publish_order_event(order_data, f"order.{status.value}")
    
    return order


@app.get("/health")
async def health_check():
    return {"status": "healthy"}


@app.get("/health/db")
async def db_health_check(db: AsyncSession = Depends(get_db)):
    try:
        await db.execute(select(1))
        return {"database": "connected"}
    except Exception:
        return {"database": "disconnected"}, 503


@app.delete("/orders/{order_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_order(
    order_id: int, 
    db: AsyncSession = Depends(get_db)
):
    result = await db.execute(
        select(models.Order).where(models.Order.id == order_id)
    )
    order = result.scalar_one_or_none()
    
    if order is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Order not found"
        )
    
    # Публикуем событие об удалении заказа перед его удалением из БД
    order_data = order.to_dict()
    await rabbitmq.publish_order_event(order_data, "order.deleted")
    
    await db.delete(order)
    await db.commit()
    
    return None
