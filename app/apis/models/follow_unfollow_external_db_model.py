from sqlalchemy import Column, Integer, String, ForeignKey

from app.db.postgres.base import ExternalBase


class FollowUnfollowExternal(ExternalBase):
    __tablename__ = "follow_unfollow_followunfollow"

    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("auth_user.id"))
    symbol = Column(String(120))
