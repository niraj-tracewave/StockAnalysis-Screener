import redis

from app.core.config import get_settings


settings = get_settings()

redis_client = redis.Redis.from_url(settings.redis_url, decode_responses=True)

redis_client_1 = redis.Redis.from_url(settings.redis_secondary_url, decode_responses=True)
