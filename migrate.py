"""One-time migration script — run before deploying v2 over an existing v1 database.

Safe to run multiple times (all statements are idempotent).
Usage: python migrate.py
"""

import asyncio
import sys
from pathlib import Path
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent / ".env", override=True)

import config  # noqa: E402

MIGRATE_SQL = """\
-- Add new columns to matches (safe if they already exist)
DO $$ BEGIN
    ALTER TABLE matches ADD COLUMN activity_id TEXT;
EXCEPTION WHEN duplicate_column THEN NULL;
END $$;

DO $$ BEGIN
    ALTER TABLE matches ADD COLUMN source TEXT NOT NULL DEFAULT 'manual';
EXCEPTION WHEN duplicate_column THEN NULL;
END $$;

CREATE UNIQUE INDEX IF NOT EXISTS idx_matches_activity
    ON matches (activity_id) WHERE activity_id IS NOT NULL;

-- Create weapons table
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

-- Seed weapons
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

-- Create tracked_players table
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

-- Replace old fake runners with real Marathon roster
DELETE FROM runners WHERE name IN (
    'LOCUS', 'GLITCH', 'VIPER', 'IRON', 'SPECTER', 'NOVA',
    'BLAZE', 'DRIFT', 'ECHO', 'TITAN', 'WRAITH', 'SAGE'
);

INSERT INTO runners (name, role, base_hp, base_speed, tier, abilities) VALUES
    ('ASSASSIN', 'stealth', 90, 1.3, 'A', '["Shadow Strike"]'::jsonb),
    ('DESTROYER', 'tank', 140, 0.85, 'A', '["Heavy Ordnance"]'::jsonb),
    ('RECON', 'recon', 100, 1.1, 'A', '["Tactical Scan"]'::jsonb),
    ('ROOK', 'opportunist', 100, 1.0, 'B', '["Wildcard"]'::jsonb),
    ('THIEF', 'stealth', 85, 1.3, 'B', '["Covert Acquisitions"]'::jsonb),
    ('TRIAGE', 'support', 100, 1.0, 'A', '["Field Medic"]'::jsonb),
    ('VANDAL', 'assault', 110, 1.05, 'A', '["Combat Anarchy"]'::jsonb)
ON CONFLICT (name) DO UPDATE SET
    role = EXCLUDED.role,
    base_hp = EXCLUDED.base_hp,
    base_speed = EXCLUDED.base_speed,
    tier = EXCLUDED.tier,
    abilities = EXCLUDED.abilities,
    updated_at = now();
"""


async def main():
    import asyncpg

    print(f"Connecting to {config.DB_HOST}:{config.DB_PORT}/{config.DB_NAME}...")
    try:
        conn = await asyncpg.connect(dsn=config.DB_DSN, timeout=10)
    except Exception as exc:
        print(f"FATAL: Could not connect: {exc}")
        sys.exit(1)

    try:
        await conn.execute(MIGRATE_SQL)
        print("Migration complete:")
        print("  - matches table: added activity_id, source columns")
        print("  - weapons table: created + seeded 29 weapons")
        print("  - tracked_players table: created")
        print("  - runners: replaced fake roster with real 7 Marathon runners")
    except Exception as exc:
        print(f"Migration failed: {exc}")
        sys.exit(1)
    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(main())
