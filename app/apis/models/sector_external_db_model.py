from sqlalchemy import Column, Integer, String
from app.db.postgres.base import ExternalBase


class SectorExternal(ExternalBase):
    __tablename__ = "follow_unfollow_sector"

    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, index=True)
    sector_name = Column(String(255))
