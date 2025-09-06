from pydantic import BaseModel, field_validator
from datetime import datetime
from typing import Optional
from enum import Enum


class ShippingStatus(str, Enum):
    PENDING = "pending"
    PROCESSING = "processing"
    SHIPPED = "shipped"
    DELIVERED = "delivered"
    CANCELLED = "cancelled"


class ShippingCreate(BaseModel):
    order_id: int
    user_id: int
    address: str
    shipping_cost: float

    @field_validator('shipping_cost')
    def shipping_cost_must_be_positive(cls, v):
        if v < 0:
            raise ValueError('Shipping cost must be positive')
        return v


class ShippingResponse(BaseModel):
    id: int
    order_id: int
    user_id: int
    address: str
    status: ShippingStatus
    tracking_number: Optional[str] = None
    shipping_cost: float
    estimated_delivery: Optional[datetime] = None
    created_at: datetime
    updated_at: Optional[datetime] = None

    class Config:
        from_attributes = True
        use_enum_values = True