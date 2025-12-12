import enum

from sqlalchemy import Column, String, Boolean, DateTime, Enum, func

from app.db.postgres.base import Base

class RoleStatus(enum.Enum):
    admin = "admin"
    sub_admin = "sub-admin"
    user = "user"

class User(Base):
    __tablename__ = "users"

    mobile_number = Column(String(20), primary_key=True, unique=True)
    first_name = Column(String(150), nullable=True)
    last_name = Column(String(150), nullable=True)
    email = Column(String(255), unique=True, nullable=True)
    is_active = Column(Boolean, default=True)
    profile_image = Column(String(255), nullable=True)
    account_role =Column(Enum(RoleStatus), nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())