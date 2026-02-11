from sqlalchemy import Column, Integer

from app.db.postgres.base import ExternalBase


class UserExternal(ExternalBase):
    __tablename__ = "users_user"

    id = Column(Integer, primary_key=True)