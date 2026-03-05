"""Redis async client with smart caching for Marathon Intel."""

import json
import logging
from typing import Optional

import redis.asyncio as aioredis

import config

log = logging.getLogger("marathon.redis")

# TTLs in seconds
TTL_COMMUNITY_STATS = 300     # 5 min
TTL_LEADERBOARD = 600         # 10 min
TTL_AI_INSIGHT = 3600         # 1 hour
TTL_META = 900                # 15 min
TTL_RUNNER = 600              # 10 min
TTL_HISTORY = 3600            # 1 hour
TTL_SERVER_STATUS = 120       # 2 min
TTL_PEAK_HOURS = 1800         # 30 min
TTL_META_SNAPSHOT = 86400     # 24 hours
TTL_LIVE_STATUS = 15          # 15 sec — netcapture pushes every few seconds


async def connect_redis() -> Optional[aioredis.Redis]:
    try:
        client = aioredis.Redis(
            host=config.REDIS_HOST,
            port=config.REDIS_PORT,
            password=config.REDIS_PASSWORD or None,
            decode_responses=True,
            socket_connect_timeout=5,
        )
        await client.ping()
        log.info("Redis connected (%s:%s)", config.REDIS_HOST, config.REDIS_PORT)
        return client
    except (OSError, aioredis.RedisError) as exc:
        log.warning("Could not connect to Redis — running without cache: %s", exc)
        return None


async def close_redis(client: Optional[aioredis.Redis]) -> None:
    if client is not None:
        await client.aclose()
        log.info("Redis connection closed")


async def cache_get(client: Optional[aioredis.Redis], key: str) -> Optional[dict]:
    if client is None:
        return None
    try:
        raw = await client.get(key)
        if raw:
            return json.loads(raw)
    except Exception:
        pass
    return None


async def cache_set(client: Optional[aioredis.Redis], key: str, data: dict, ttl: int = 300) -> None:
    if client is None:
        return
    try:
        await client.set(key, json.dumps(data, default=str), ex=ttl)
    except Exception:
        pass


async def invalidate_match_caches(client: Optional[aioredis.Redis]) -> None:
    """Invalidate caches that depend on match data."""
    if client is None:
        return
    try:
        keys = []
        async for key in client.scan_iter("marathon:stats:*"):
            keys.append(key)
        async for key in client.scan_iter("marathon:leaderboard:*"):
            keys.append(key)
        async for key in client.scan_iter("marathon:meta:*"):
            keys.append(key)
        if keys:
            await client.delete(*keys)
    except Exception:
        pass
