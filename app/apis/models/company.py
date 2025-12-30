from sqlalchemy import Column, String, func, DateTime, Boolean, UniqueConstraint, Text, Integer

from app.db.postgres.base import Base


class Company(Base):
    __tablename__ = "company"

    id = Column(Integer, primary_key=True, autoincrement=True)
    company_name = Column(String(200), nullable=False)
    symbol = Column(String(150), nullable=False)
    platform = Column(String(10), nullable=False)
    is_active = Column(Boolean, default=True)
    url = Column(Text, nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    __table_args__ = (
        UniqueConstraint("symbol", "platform", name="uq_symbol_platform"),
    )