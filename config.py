"""Centralized configuration loaded from .env"""

import os
import sys
from pathlib import Path
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent / ".env", override=True)


def _require(key: str) -> str:
    val = os.getenv(key)
    if not val:
        sys.exit(f"FATAL: missing required env var {key}")
    return val


# -- Discord --
DISCORD_TOKEN: str = _require("DISCORD_TOKEN")
DISCORD_GUILD_ID: int = int(_require("DISCORD_GUILD_ID"))

# -- AI --
ANTHROPIC_API_KEY: str = _require("ANTHROPIC_API_KEY")

# -- PostgreSQL --
DB_DSN: str = os.getenv("DATABASE_URL", "")
if not DB_DSN:
    DB_HOST = os.getenv("DB_HOST", "localhost")
    DB_PORT = int(os.getenv("DB_PORT", "5432"))
    DB_NAME = os.getenv("DB_NAME", "marathon_intel")
    DB_USER = os.getenv("DB_USER", "marathon")
    DB_PASSWORD = _require("DB_PASSWORD")
    DB_DSN = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
else:
    from urllib.parse import urlparse
    _parsed = urlparse(DB_DSN)
    DB_HOST = _parsed.hostname or "unknown"
    DB_PORT = _parsed.port or 5432
    DB_NAME = (_parsed.path or "/unknown").lstrip("/")
    DB_USER = _parsed.username or "unknown"
    DB_PASSWORD = _parsed.password or ""

# -- Redis --
REDIS_URL: str = os.getenv("REDIS_URL", "")
if not REDIS_URL:
    REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
    REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
    REDIS_PASSWORD = os.getenv("REDIS_PASSWORD", "")
else:
    from urllib.parse import urlparse as _urlparse_redis
    _rparsed = _urlparse_redis(REDIS_URL)
    REDIS_HOST = _rparsed.hostname or "localhost"
    REDIS_PORT = _rparsed.port or 6379
    REDIS_PASSWORD = _rparsed.password or ""

# -- Web --
WEB_PORT: int = int(os.getenv("PORT", "3000"))
WEB_BASE_URL: str = os.getenv("WEB_BASE_URL", "https://marathon.straightfirefood.blog")

# -- Bungie (optional) --
BUNGIE_API_KEY: str = os.getenv("BUNGIE_API_KEY", "")

# -- Misc --
LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")
