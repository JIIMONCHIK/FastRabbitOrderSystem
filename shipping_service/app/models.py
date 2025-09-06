from sqlalchemy import Column, Integer, String, DateTime, Enum, Float
from sqlalchemy.sql import func
from database import Base
import enum


class ShippingStatus(enum.Enum):
    PENDING = "pending"
    PROCESSING = "processing"
    SHIPPED = "shipped"
    DELIVERED = "delivered"
    CANCELLED = "cancelled"


class Shipping(Base):
    __tablename__ = "shippings"

    id = Column(Integer, primary_key=True, index=True)
    order_id = Column(Integer, nullable=False, unique=True)
    user_id = Column(Integer, nullable=False)
    address = Column(String, nullable=False)
    status = Column(Enum(ShippingStatus), default=ShippingStatus.PENDING)
    tracking_number = Column(String, nullable=True)
    shipping_cost = Column(Float, default=0.0)
    estimated_delivery = Column(DateTime, nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())

    def to_dict(self):
        return {
            "id": self.id,
            "order_id": self.order_id,
            "user_id": self.user_id,
            "address": self.address,
            "status": self.status.value,
            "tracking_number": self.tracking_number,
            "shipping_cost": self.shipping_cost,
            "estimated_delivery": self.estimated_delivery.isoformat() if self.estimated_delivery else None,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None
        }