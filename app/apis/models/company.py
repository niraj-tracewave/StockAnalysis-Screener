from sqlalchemy import Column, String, func, DateTime, Boolean

from app.db.postgres.base import Base


class Company(Base):
    __tablename__ = "company"

    company_name = Column(String(200), primary_key=True, unique=True)
    symbol = Column(String(150), nullable=False, unique=True)
    platform = Column(String(10), nullable=False)
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())