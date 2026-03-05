"""Service-layer initialisation and teardown."""

from .database import connect_db, close_db
from .redis_cache import connect_redis, close_redis
from .ai import connect_ai, close_ai

__all__ = [
    "connect_db", "close_db",
    "connect_redis", "close_redis",
    "connect_ai", "close_ai",
]
