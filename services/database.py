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
    activity_id TEXT,
    source      TEXT        NOT NULL DEFAULT 'manual',
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_matches_user ON matches (user_hash, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_matches_runner ON matches (runner_name, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_matches_map ON matches (map_name);
CREATE INDEX IF NOT EXISTS idx_matches_patch ON matches (patch);
CREATE UNIQUE INDEX IF NOT EXISTS idx_matches_activity ON matches (activity_id) WHERE activity_id IS NOT NULL;

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

-- weapons
CREATE TABLE IF NOT EXISTS weapons (
    id          SERIAL      PRIMARY KEY,
    name        TEXT        NOT NULL UNIQUE,
    category    TEXT        NOT NULL DEFAULT 'unknown',
    damage      FLOAT       NOT NULL DEFAULT 0,
    fire_rate   FLOAT       NOT NULL DEFAULT 0,
    mag_size    INT         NOT NULL DEFAULT 0,
    reload_s    FLOAT       NOT NULL DEFAULT 0,
    range_m     FLOAT       NOT NULL DEFAULT 0,
    pick_rate   FLOAT       NOT NULL DEFAULT 0.0,
    win_rate    FLOAT       NOT NULL DEFAULT 0.0,
    patch       TEXT        NOT NULL DEFAULT '1.0',
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_weapons_name ON weapons (name);
CREATE INDEX IF NOT EXISTS idx_weapons_category ON weapons (category);

-- seed weapons (real Marathon arsenal)
INSERT INTO weapons (name, category) VALUES
    ('Impact HAR', 'assault_rifle'),
    ('M77 Assault Rifle', 'assault_rifle'),
    ('Overrun AR', 'assault_rifle'),
    ('V75 Scar', 'assault_rifle'),
    ('Conquest LMG', 'machine_gun'),
    ('Demolition HMG', 'machine_gun'),
    ('Retaliator LMG', 'machine_gun'),
    ('V11 Punch', 'melee'),
    ('CE Tactical Sidearm', 'pistol'),
    ('Magnum MC', 'pistol'),
    ('BR33 Volley Rifle', 'precision_rifle'),
    ('Hardline PR', 'precision_rifle'),
    ('Repeater HPR', 'precision_rifle'),
    ('Stryder M1T', 'precision_rifle'),
    ('Twin Tap HBR', 'precision_rifle'),
    ('V66 Lookout', 'precision_rifle'),
    ('V95 Lookout', 'precision_rifle'),
    ('Ares RG', 'railgun'),
    ('V00 Zeus RG', 'railgun'),
    ('Misriah 2442', 'shotgun'),
    ('V85 Circuit Breaker', 'shotgun'),
    ('WSTR Combat Shotgun', 'shotgun'),
    ('Longshot', 'sniper_rifle'),
    ('Outland', 'sniper_rifle'),
    ('V99 Channel Rifle', 'sniper_rifle'),
    ('BRRT SMG', 'smg'),
    ('Bully SMG', 'smg'),
    ('Copperhead RF', 'smg'),
    ('V22 Volt Thrower', 'smg')
ON CONFLICT (name) DO NOTHING;

-- tracked_players (for Bungie API auto-sync)
CREATE TABLE IF NOT EXISTS tracked_players (
    id              SERIAL      PRIMARY KEY,
    membership_id   TEXT        NOT NULL,
    membership_type INT         NOT NULL,
    display_name    TEXT        NOT NULL DEFAULT '',
    bungie_name     TEXT        NOT NULL DEFAULT '',
    last_synced_at  TIMESTAMPTZ,
    last_activity_id TEXT,
    auto_sync       BOOLEAN     NOT NULL DEFAULT true,
    added_by        TEXT        NOT NULL DEFAULT '',
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (membership_id, membership_type)
);
CREATE INDEX IF NOT EXISTS idx_tracked_sync ON tracked_players (auto_sync, last_synced_at);

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

-- server_status_checks (uptime monitoring)
CREATE TABLE IF NOT EXISTS server_status_checks (
    id          SERIAL      PRIMARY KEY,
    endpoint    TEXT        NOT NULL,
    status_code INT,
    response_ms FLOAT       NOT NULL DEFAULT 0,
    is_up       BOOLEAN     NOT NULL DEFAULT false,
    error       TEXT        NOT NULL DEFAULT '',
    checked_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_status_endpoint ON server_status_checks (endpoint, checked_at DESC);

-- blog_posts (RSS / announcement tracking)
CREATE TABLE IF NOT EXISTS blog_posts (
    id          SERIAL      PRIMARY KEY,
    url         TEXT        NOT NULL UNIQUE,
    title       TEXT        NOT NULL DEFAULT '',
    summary     TEXT        NOT NULL DEFAULT '',
    published_at TIMESTAMPTZ,
    is_patch    BOOLEAN     NOT NULL DEFAULT false,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_blog_published ON blog_posts (published_at DESC);

-- match_sessions (auto-detected from network traffic)
CREATE TABLE IF NOT EXISTS match_sessions (
    id          SERIAL      PRIMARY KEY,
    user_hash   TEXT        NOT NULL,
    server_ip   TEXT        NOT NULL DEFAULT '',
    region      TEXT        NOT NULL DEFAULT 'unknown',
    started_at  TIMESTAMPTZ NOT NULL,
    ended_at    TIMESTAMPTZ,
    duration_s  INT         NOT NULL DEFAULT 0,
    peak_ping_ms FLOAT      NOT NULL DEFAULT 0,
    avg_ping_ms FLOAT       NOT NULL DEFAULT 0,
    total_packets INT       NOT NULL DEFAULT 0,
    queue_time_s INT        NOT NULL DEFAULT 0,
    patch       TEXT        NOT NULL DEFAULT '1.0',
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_sessions_user ON match_sessions (user_hash, started_at DESC);
CREATE INDEX IF NOT EXISTS idx_sessions_region ON match_sessions (region);

-- crew_finder (LFG system)
CREATE TABLE IF NOT EXISTS crew_finder (
    id SERIAL PRIMARY KEY,
    discord_user_id TEXT NOT NULL,
    display_name TEXT NOT NULL,
    region TEXT NOT NULL DEFAULT 'any',
    playstyle TEXT NOT NULL DEFAULT 'any',
    main_runner TEXT NOT NULL DEFAULT 'any',
    play_times TEXT NOT NULL DEFAULT 'any',
    message TEXT NOT NULL DEFAULT '',
    active BOOLEAN NOT NULL DEFAULT true,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_crew_finder_user ON crew_finder (discord_user_id);
CREATE INDEX IF NOT EXISTS idx_crew_finder_active ON crew_finder (active, region);

-- seed runners (real Marathon roster)
INSERT INTO runners (name, role, base_hp, base_speed, tier, abilities) VALUES
    ('ASSASSIN', 'stealth', 90, 1.3, 'A', '["Shadow Strike"]'::jsonb),
    ('DESTROYER', 'tank', 140, 0.85, 'A', '["Heavy Ordnance"]'::jsonb),
    ('RECON', 'recon', 100, 1.1, 'A', '["Tactical Scan"]'::jsonb),
    ('ROOK', 'opportunist', 100, 1.0, 'B', '["Wildcard"]'::jsonb),
    ('THIEF', 'stealth', 85, 1.3, 'B', '["Covert Acquisitions"]'::jsonb),
    ('TRIAGE', 'support', 100, 1.0, 'A', '["Field Medic"]'::jsonb),
    ('VANDAL', 'assault', 110, 1.05, 'A', '["Combat Anarchy"]'::jsonb)
ON CONFLICT (name) DO NOTHING;

-- seasons (seasonal ladder)
CREATE TABLE IF NOT EXISTS seasons (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    ended_at TIMESTAMPTZ,
    active BOOLEAN NOT NULL DEFAULT true
);

-- seasonal_ratings (SR ladder per season)
CREATE TABLE IF NOT EXISTS seasonal_ratings (
    id SERIAL PRIMARY KEY,
    season_id INT NOT NULL,
    user_hash TEXT NOT NULL,
    display_name TEXT NOT NULL DEFAULT '',
    sr INT NOT NULL DEFAULT 1000,
    tier TEXT NOT NULL DEFAULT 'Bronze',
    matches INT NOT NULL DEFAULT 0,
    wins INT NOT NULL DEFAULT 0,
    losses INT NOT NULL DEFAULT 0,
    peak_sr INT NOT NULL DEFAULT 1000,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE(season_id, user_hash)
);
CREATE INDEX IF NOT EXISTS idx_seasonal_ratings_season ON seasonal_ratings (season_id, sr DESC);
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


# -- SR tier helpers --

SR_TIERS = [
    (2500, "Champion"),
    (2000, "Platinum"),
    (1400, "Gold"),
    (800, "Silver"),
    (0, "Bronze"),
]


def sr_to_tier(sr: int) -> str:
    """Return the tier name for a given SR value."""
    for threshold, name in SR_TIERS:
        if sr >= threshold:
            return name
    return "Bronze"


async def update_sr(
    pool: asyncpg.Pool,
    user_hash: str,
    display_name: str,
    result: str,
    kills: int,
    deaths: int,
) -> Optional[dict]:
    """Calculate and apply SR change for a match result.

    Returns dict with new_sr, tier, sr_change or None if no active season.
    """
    # Get or create active season
    season = await pool.fetchrow(
        "SELECT id FROM seasons WHERE active = true ORDER BY started_at DESC LIMIT 1"
    )
    if not season:
        # Auto-create first season
        season = await pool.fetchrow(
            "INSERT INTO seasons (name) VALUES ('Season 1') "
            "ON CONFLICT (name) DO UPDATE SET name = seasons.name "
            "RETURNING id"
        )
    if not season:
        return None
    season_id = season["id"]

    # Get or create rating row
    row = await pool.fetchrow(
        "SELECT sr, matches, wins, losses, peak_sr FROM seasonal_ratings "
        "WHERE season_id = $1 AND user_hash = $2",
        season_id, user_hash,
    )

    current_sr = row["sr"] if row else 1000
    matches = row["matches"] if row else 0
    wins = row["wins"] if row else 0
    losses = row["losses"] if row else 0
    peak_sr = row["peak_sr"] if row else 1000

    # K/D ratio
    kd = kills / deaths if deaths > 0 else float(kills) if kills > 0 else 1.0

    # SR calculation
    if result == "win":
        sr_change = 25
        if kd > 2.0:
            sr_change += 5
        elif kd < 0.5:
            sr_change -= 5
        wins += 1
    elif result == "loss":
        if kd > 2.0:
            sr_change = -10  # good performance in a loss
        else:
            sr_change = -20
        losses += 1
    else:  # draw
        sr_change = 5

    new_sr = max(0, current_sr + sr_change)
    matches += 1
    peak_sr = max(peak_sr, new_sr)
    tier = sr_to_tier(new_sr)

    # Upsert
    await pool.execute(
        "INSERT INTO seasonal_ratings (season_id, user_hash, display_name, sr, tier, matches, wins, losses, peak_sr, updated_at) "
        "VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, now()) "
        "ON CONFLICT (season_id, user_hash) DO UPDATE SET "
        "display_name = $3, sr = $4, tier = $5, matches = $6, wins = $7, losses = $8, peak_sr = $9, updated_at = now()",
        season_id, user_hash, display_name, new_sr, tier, matches, wins, losses, peak_sr,
    )

    return {"new_sr": new_sr, "tier": tier, "sr_change": sr_change, "peak_sr": peak_sr}
