"""PostgreSQL connection pool and schema bootstrap."""

import logging
from typing import Optional

import asyncpg

import config

log = logging.getLogger("marathon.database")

SCHEMA_SQL = """\
-- runners
CREATE TABLE IF NOT EXISTS runners (
    id          SERIAL      PRIMARY KEY,
    name        TEXT        NOT NULL UNIQUE,
    role        TEXT        NOT NULL DEFAULT 'assault',
    abilities   JSONB       NOT NULL DEFAULT '[]'::jsonb,
    base_hp     INT         NOT NULL DEFAULT 100,
    base_speed  FLOAT       NOT NULL DEFAULT 1.0,
    tier        TEXT        NOT NULL DEFAULT 'B',
    pick_rate   FLOAT       NOT NULL DEFAULT 0.0,
    win_rate    FLOAT       NOT NULL DEFAULT 0.0,
    ban_rate    FLOAT       NOT NULL DEFAULT 0.0,
    patch       TEXT        NOT NULL DEFAULT '1.0',
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_runners_name ON runners (name);
CREATE INDEX IF NOT EXISTS idx_runners_tier ON runners (tier);

-- matches
CREATE TABLE IF NOT EXISTS matches (
    id          SERIAL      PRIMARY KEY,
    user_hash   TEXT        NOT NULL,
    runner_name TEXT        NOT NULL,
    map_name    TEXT        NOT NULL DEFAULT 'unknown',
    mode        TEXT        NOT NULL DEFAULT 'extraction',
    result      TEXT        NOT NULL CHECK (result IN ('win', 'loss', 'draw')),
    kills       INT         NOT NULL DEFAULT 0,
    deaths      INT         NOT NULL DEFAULT 0,
    assists     INT         NOT NULL DEFAULT 0,
    damage      INT         NOT NULL DEFAULT 0,
    duration_s  INT         NOT NULL DEFAULT 0,
    loadout     JSONB       NOT NULL DEFAULT '{}'::jsonb,
    patch       TEXT        NOT NULL DEFAULT '1.0',
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_matches_user ON matches (user_hash, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_matches_runner ON matches (runner_name, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_matches_map ON matches (map_name);
CREATE INDEX IF NOT EXISTS idx_matches_patch ON matches (patch);

-- network_performance
CREATE TABLE IF NOT EXISTS network_performance (
    id          SERIAL      PRIMARY KEY,
    user_hash   TEXT        NOT NULL,
    server_ip   TEXT        NOT NULL DEFAULT '',
    region      TEXT        NOT NULL DEFAULT 'unknown',
    map_name    TEXT        NOT NULL DEFAULT 'unknown',
    avg_ping_ms FLOAT       NOT NULL DEFAULT 0,
    jitter_ms   FLOAT       NOT NULL DEFAULT 0,
    packet_loss FLOAT       NOT NULL DEFAULT 0,
    tick_rate   INT         NOT NULL DEFAULT 0,
    patch       TEXT        NOT NULL DEFAULT '1.0',
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_network_region ON network_performance (region);
CREATE INDEX IF NOT EXISTS idx_network_map ON network_performance (map_name);

-- patch_notes
CREATE TABLE IF NOT EXISTS patch_notes (
    id          SERIAL      PRIMARY KEY,
    version     TEXT        NOT NULL UNIQUE,
    title       TEXT        NOT NULL DEFAULT '',
    summary     TEXT        NOT NULL DEFAULT '',
    changes     JSONB       NOT NULL DEFAULT '[]'::jsonb,
    ai_analysis TEXT,
    released_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_patches_version ON patch_notes (version);

-- loadouts
CREATE TABLE IF NOT EXISTS loadouts (
    id          SERIAL      PRIMARY KEY,
    runner_name TEXT        NOT NULL,
    map_name    TEXT        NOT NULL DEFAULT 'any',
    weapon_primary   TEXT   NOT NULL DEFAULT '',
    weapon_secondary TEXT   NOT NULL DEFAULT '',
    ability_setup    JSONB  NOT NULL DEFAULT '{}'::jsonb,
    source      TEXT        NOT NULL DEFAULT 'community',
    win_rate    FLOAT       NOT NULL DEFAULT 0.0,
    sample_size INT         NOT NULL DEFAULT 0,
    patch       TEXT        NOT NULL DEFAULT '1.0',
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_loadouts_runner ON loadouts (runner_name, map_name);

-- leaderboard_cache
CREATE TABLE IF NOT EXISTS leaderboard_cache (
    id          SERIAL      PRIMARY KEY,
    user_hash   TEXT        NOT NULL UNIQUE,
    display_name TEXT       NOT NULL DEFAULT 'Anonymous',
    total_matches INT       NOT NULL DEFAULT 0,
    wins        INT         NOT NULL DEFAULT 0,
    losses      INT         NOT NULL DEFAULT 0,
    win_rate    FLOAT       NOT NULL DEFAULT 0.0,
    avg_kd      FLOAT       NOT NULL DEFAULT 0.0,
    main_runner TEXT        NOT NULL DEFAULT '',
    score       FLOAT       NOT NULL DEFAULT 0.0,
    rank        INT         NOT NULL DEFAULT 0,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_leaderboard_rank ON leaderboard_cache (rank);
CREATE INDEX IF NOT EXISTS idx_leaderboard_score ON leaderboard_cache (score DESC);

-- ai_insights
CREATE TABLE IF NOT EXISTS ai_insights (
    id          SERIAL      PRIMARY KEY,
    insight_type TEXT       NOT NULL,
    subject     TEXT        NOT NULL DEFAULT '',
    content     TEXT        NOT NULL,
    data        JSONB       NOT NULL DEFAULT '{}'::jsonb,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    expires_at  TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS idx_insights_type ON ai_insights (insight_type, created_at DESC);

-- materialized view: community stats
CREATE OR REPLACE VIEW community_stats_view AS
SELECT
    COUNT(*) AS total_matches,
    COUNT(*) FILTER (WHERE result = 'win') AS total_wins,
    COUNT(DISTINCT user_hash) AS unique_players,
    COUNT(DISTINCT runner_name) AS runners_used,
    COUNT(DISTINCT map_name) AS maps_played,
    ROUND(AVG(kills)::numeric, 1) AS avg_kills,
    ROUND(AVG(deaths)::numeric, 1) AS avg_deaths,
    ROUND(AVG(damage)::numeric, 0) AS avg_damage,
    ROUND(AVG(duration_s)::numeric, 0) AS avg_duration_s
FROM matches;

-- seed runners
INSERT INTO runners (name, role, base_hp, base_speed, tier, abilities) VALUES
    ('LOCUS', 'recon', 100, 1.2, 'S', '["Pulse Scan", "Tracker Dart", "Recon Surge"]'::jsonb),
    ('GLITCH', 'assault', 110, 1.0, 'A', '["System Hack", "EMP Burst", "Digital Ghost"]'::jsonb),
    ('VIPER', 'assault', 100, 1.1, 'A', '["Toxic Cloud", "Acid Spray", "Venomstrike"]'::jsonb),
    ('IRON', 'tank', 150, 0.8, 'B', '["Shield Wall", "Fortify", "Ground Pound"]'::jsonb),
    ('SPECTER', 'stealth', 90, 1.3, 'S', '["Cloak", "Shadow Step", "Phantom Strike"]'::jsonb),
    ('NOVA', 'support', 100, 1.0, 'A', '["Heal Pulse", "Shield Boost", "Revive Field"]'::jsonb),
    ('BLAZE', 'assault', 105, 1.1, 'B', '["Incendiary", "Fire Wall", "Inferno"]'::jsonb),
    ('DRIFT', 'recon', 95, 1.4, 'A', '["Grapple", "Jet Dash", "Aerial Scan"]'::jsonb),
    ('ECHO', 'support', 100, 1.0, 'C', '["Sound Wave", "Sonic Shield", "Resonance"]'::jsonb),
    ('TITAN', 'tank', 160, 0.7, 'B', '["Barrier", "Charge", "Seismic Slam"]'::jsonb),
    ('WRAITH', 'stealth', 85, 1.3, 'S', '["Phase Shift", "Void Walk", "Rift Strike"]'::jsonb),
    ('SAGE', 'support', 100, 1.0, 'A', '["Mend", "Barrier Orb", "Resurrection"]'::jsonb)
ON CONFLICT (name) DO NOTHING;
"""


async def connect_db() -> Optional[asyncpg.Pool]:
    try:
        bootstrap_conn = await asyncpg.connect(dsn=config.DB_DSN, timeout=10)
        try:
            await bootstrap_conn.execute(SCHEMA_SQL)
            log.info("Database schema verified / created")
        except asyncpg.PostgresError as exc:
            log.warning("Schema bootstrap issue: %s", exc)
        finally:
            await bootstrap_conn.close()
    except (OSError, asyncpg.PostgresError) as exc:
        log.warning("Could not connect to PostgreSQL — running without DB: %s", exc)
        return None

    try:
        pool = await asyncpg.create_pool(
            dsn=config.DB_DSN,
            min_size=2,
            max_size=10,
            command_timeout=30,
        )
        log.info("PostgreSQL pool created (%s:%s/%s)", config.DB_HOST, config.DB_PORT, config.DB_NAME)
    except (OSError, asyncpg.PostgresError) as exc:
        log.warning("Could not create PostgreSQL pool: %s", exc)
        return None

    return pool


async def close_db(pool: Optional[asyncpg.Pool]) -> None:
    if pool is not None:
        await pool.close()
        log.info("PostgreSQL pool closed")
