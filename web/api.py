"""FastAPI routes — all API endpoints for Marathon Intel."""

import json
import logging
import time
from collections import defaultdict
from typing import Optional

from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field, field_validator
from pathlib import Path

from services.redis_cache import (
    cache_get, cache_set, invalidate_match_caches,
    TTL_COMMUNITY_STATS, TTL_LEADERBOARD, TTL_META, TTL_AI_INSIGHT,
)

log = logging.getLogger("marathon.web")

PUBLIC_DIR = Path(__file__).parent.parent / "public"

# Simple in-memory rate limiter (per IP, per endpoint group)
_rate_limits: dict[str, list[float]] = defaultdict(list)
RATE_LIMIT_WINDOW = 60  # seconds
RATE_LIMIT_MAX_WRITES = 30  # max write requests per window per IP
RATE_LIMIT_MAX_READS = 120  # max read requests per window per IP


def _check_rate_limit(client_ip: str, is_write: bool = False) -> bool:
    """Returns True if request is allowed, False if rate limited."""
    key = f"{client_ip}:{'w' if is_write else 'r'}"
    now = time.time()
    cutoff = now - RATE_LIMIT_WINDOW
    # Clean old entries
    _rate_limits[key] = [t for t in _rate_limits[key] if t > cutoff]
    limit = RATE_LIMIT_MAX_WRITES if is_write else RATE_LIMIT_MAX_READS
    if len(_rate_limits[key]) >= limit:
        return False
    _rate_limits[key].append(now)
    return True


# -- Pydantic models --

class MatchSubmission(BaseModel):
    user_hash: str = Field(..., min_length=1, max_length=100)
    runner_name: str = Field(..., min_length=1, max_length=50)
    map_name: str = Field(default="unknown", max_length=100)
    mode: str = Field(default="extraction", max_length=50)
    result: str  # win, loss, draw
    kills: int = Field(default=0, ge=0, le=999)
    deaths: int = Field(default=0, ge=0, le=999)
    assists: int = Field(default=0, ge=0, le=999)
    damage: int = Field(default=0, ge=0, le=999999)
    duration_s: int = Field(default=0, ge=0, le=7200)
    loadout: dict = Field(default_factory=dict)
    patch: str = Field(default="1.0", max_length=20)

    @field_validator("user_hash", "runner_name", "map_name")
    @classmethod
    def strip_whitespace(cls, v: str) -> str:
        return v.strip()


class LiveStatusUpdate(BaseModel):
    user_hash: str = Field(..., min_length=1, max_length=100)
    state: str = "idle"  # idle, queuing, in_match
    server_ip: str = ""
    region: str = "unknown"
    ping_ms: float = 0
    jitter_ms: float = 0
    packet_loss: float = 0
    tick_rate: int = 0
    match_duration_s: int = 0
    queue_time_s: int = 0
    packets_per_sec: float = 0
    session_matches: int = 0
    session_wins: int = 0
    session_losses: int = 0


class NetworkSubmission(BaseModel):
    user_hash: str
    server_ip: str = ""
    region: str = "unknown"
    map_name: str = "unknown"
    avg_ping_ms: float = 0
    jitter_ms: float = 0
    packet_loss: float = 0
    tick_rate: int = 0
    patch: str = "1.0"


class PatchSubmission(BaseModel):
    version: str
    title: str = ""
    summary: str = ""
    changes: list[str] = Field(default_factory=list)


class MatchSessionSubmission(BaseModel):
    user_hash: str
    server_ip: str = ""
    region: str = "unknown"
    started_at: str = ""
    ended_at: str = ""
    duration_s: int = 0
    peak_ping_ms: float = 0
    avg_ping_ms: float = 0
    total_packets: int = 0
    queue_time_s: int = 0
    patch: str = "1.0"


def create_app(bot) -> FastAPI:
    app = FastAPI(title="Marathon Data Intel", version="1.0.0")
    app.state.bot = bot

    # CORS — allow browser requests from the dashboard
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["https://marathon.straightfirefood.blog"],
        allow_methods=["GET", "POST"],
        allow_headers=["Content-Type"],
    )

    # Serve static public files
    if PUBLIC_DIR.exists():
        app.mount("/static", StaticFiles(directory=str(PUBLIC_DIR)), name="static")

    def _pool():
        return getattr(bot, "pool", None)

    def _redis():
        return getattr(bot, "redis", None)

    def _ai():
        return getattr(bot, "ai_client", None)

    # -- Health --
    @app.get("/api/health")
    async def health():
        pool = _pool()
        redis = _redis()
        db_ok = False
        if pool:
            try:
                await pool.fetchval("SELECT 1")
                db_ok = True
            except Exception:
                pass
        redis_ok = False
        if redis:
            try:
                await redis.ping()
                redis_ok = True
            except Exception:
                pass
        bot_ok = bot.is_ready() if hasattr(bot, "is_ready") else False
        status = 200 if db_ok else 503
        return JSONResponse(
            {"status": "ok" if db_ok else "degraded", "db": db_ok, "redis": redis_ok, "bot": bot_ok},
            status_code=status,
        )

    # -- Community Stats --
    @app.get("/api/community-stats")
    async def community_stats():
        pool = _pool()
        if not pool:
            raise HTTPException(503, "Database offline")
        cached = await cache_get(_redis(), "marathon:stats:community")
        if cached:
            return cached
        row = await pool.fetchrow("SELECT * FROM community_stats_view")
        if not row:
            return {"total_matches": 0}
        data = dict(row)
        await cache_set(_redis(), "marathon:stats:community", data, TTL_COMMUNITY_STATS)
        return data

    # -- Runners --
    @app.get("/api/runners")
    async def list_runners():
        pool = _pool()
        if not pool:
            raise HTTPException(503, "Database offline")
        rows = await pool.fetch("SELECT * FROM runners ORDER BY tier, name")
        return {"runners": [dict(r) for r in rows]}

    @app.get("/api/runners/{name}")
    async def get_runner(name: str):
        pool = _pool()
        if not pool:
            raise HTTPException(503, "Database offline")
        row = await pool.fetchrow("SELECT * FROM runners WHERE UPPER(name) = UPPER($1)", name)
        if not row:
            raise HTTPException(404, "Runner not found")
        return dict(row)

    @app.get("/api/runners/{name}/matches")
    async def get_runner_matches(name: str, limit: int = 50):
        pool = _pool()
        if not pool:
            raise HTTPException(503, "Database offline")
        limit = min(limit, 200)
        rows = await pool.fetch(
            "SELECT * FROM matches WHERE UPPER(runner_name) = UPPER($1) ORDER BY created_at DESC LIMIT $2",
            name, limit,
        )
        return {"matches": [dict(r) for r in rows]}

    # -- Matches --
    @app.post("/api/matches")
    async def submit_match(match: MatchSubmission, request: Request):
        client_ip = request.client.host if request.client else "unknown"
        if not _check_rate_limit(client_ip, is_write=True):
            raise HTTPException(429, "Rate limited. Max 30 submissions per minute.")
        pool = _pool()
        if not pool:
            raise HTTPException(503, "Database offline")
        if match.result not in ("win", "loss", "draw"):
            raise HTTPException(400, "result must be win, loss, or draw")
        await pool.execute(
            "INSERT INTO matches (user_hash, runner_name, map_name, mode, result, kills, deaths, assists, damage, duration_s, loadout, patch) "
            "VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11::jsonb, $12)",
            match.user_hash, match.runner_name, match.map_name, match.mode, match.result,
            match.kills, match.deaths, match.assists, match.damage, match.duration_s,
            json.dumps(match.loadout) if match.loadout else "{}",
            match.patch,
        )
        await invalidate_match_caches(_redis())
        return {"status": "recorded"}

    # -- Network --
    @app.post("/api/network")
    async def submit_network(data: NetworkSubmission, request: Request):
        client_ip = request.client.host if request.client else "unknown"
        if not _check_rate_limit(client_ip, is_write=True):
            raise HTTPException(429, "Rate limited. Max 30 submissions per minute.")
        pool = _pool()
        if not pool:
            raise HTTPException(503, "Database offline")
        await pool.execute(
            "INSERT INTO network_performance (user_hash, server_ip, region, map_name, avg_ping_ms, jitter_ms, packet_loss, tick_rate, patch) "
            "VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)",
            data.user_hash, data.server_ip, data.region, data.map_name,
            data.avg_ping_ms, data.jitter_ms, data.packet_loss, data.tick_rate, data.patch,
        )
        return {"status": "recorded"}

    @app.get("/api/network")
    async def network_stats():
        pool = _pool()
        if not pool:
            raise HTTPException(503, "Database offline")
        rows = await pool.fetch(
            "SELECT region, map_name, "
            "ROUND(AVG(avg_ping_ms)::numeric, 1) AS avg_ping, "
            "ROUND(AVG(jitter_ms)::numeric, 1) AS avg_jitter, "
            "ROUND(AVG(packet_loss)::numeric, 2) AS avg_loss, "
            "ROUND(AVG(tick_rate)::numeric, 0) AS avg_tick_rate, "
            "COUNT(*) AS samples "
            "FROM network_performance GROUP BY region, map_name ORDER BY region, map_name"
        )
        return {"network": [dict(r) for r in rows]}

    @app.get("/api/network/regions")
    async def network_regions():
        pool = _pool()
        if not pool:
            raise HTTPException(503, "Database offline")
        rows = await pool.fetch(
            "SELECT region, "
            "ROUND(AVG(avg_ping_ms)::numeric, 1) AS avg_ping, "
            "ROUND(AVG(jitter_ms)::numeric, 1) AS avg_jitter, "
            "ROUND(AVG(packet_loss)::numeric, 2) AS avg_loss, "
            "COUNT(*) AS samples "
            "FROM network_performance GROUP BY region ORDER BY avg_ping"
        )
        return {"regions": [dict(r) for r in rows]}

    # -- Leaderboard --
    @app.get("/api/leaderboard")
    async def leaderboard(limit: int = 10):
        pool = _pool()
        if not pool:
            raise HTTPException(503, "Database offline")
        limit = min(limit, 100)
        cached = await cache_get(_redis(), f"marathon:leaderboard:{limit}")
        if cached:
            return cached
        rows = await pool.fetch("SELECT * FROM leaderboard_cache ORDER BY rank LIMIT $1", limit)
        data = {"leaderboard": [dict(r) for r in rows]}
        await cache_set(_redis(), f"marathon:leaderboard:{limit}", data, TTL_LEADERBOARD)
        return data

    # -- Patches --
    @app.get("/api/patches")
    async def list_patches():
        pool = _pool()
        if not pool:
            raise HTTPException(503, "Database offline")
        rows = await pool.fetch("SELECT * FROM patch_notes ORDER BY released_at DESC")
        return {"patches": [dict(r) for r in rows]}

    @app.post("/api/patches")
    async def submit_patch(patch: PatchSubmission):
        pool = _pool()
        if not pool:
            raise HTTPException(503, "Database offline")
        await pool.execute(
            "INSERT INTO patch_notes (version, title, summary, changes) VALUES ($1, $2, $3, $4::jsonb) "
            "ON CONFLICT (version) DO UPDATE SET title = $2, summary = $3, changes = $4::jsonb",
            patch.version, patch.title, patch.summary, json.dumps(patch.changes),
        )
        # Auto-generate AI analysis
        ai = _ai()
        if ai:
            from services.ai import generate_patch_analysis
            analysis = await generate_patch_analysis(ai, f"Patch {patch.version}: {patch.title}\n{patch.summary}\nChanges: {', '.join(patch.changes)}")
            if analysis:
                await pool.execute("UPDATE patch_notes SET ai_analysis = $1 WHERE version = $2", analysis, patch.version)
        return {"status": "recorded"}

    # -- Meta --
    @app.get("/api/meta/current")
    async def current_meta():
        pool = _pool()
        if not pool:
            raise HTTPException(503, "Database offline")
        cached = await cache_get(_redis(), "marathon:meta:current")
        if cached:
            return cached
        rows = await pool.fetch("SELECT name, role, tier, win_rate, pick_rate, ban_rate, patch FROM runners ORDER BY tier, name")
        tiers: dict[str, list] = {}
        for r in rows:
            tiers.setdefault(r["tier"], []).append(dict(r))
        data = {"meta": tiers, "runner_count": len(rows)}
        await cache_set(_redis(), "marathon:meta:current", data, TTL_META)
        return data

    @app.get("/api/meta/history")
    async def meta_history():
        pool = _pool()
        if not pool:
            raise HTTPException(503, "Database offline")
        rows = await pool.fetch(
            "SELECT * FROM ai_insights WHERE insight_type = 'meta_report' ORDER BY created_at DESC LIMIT 10"
        )
        return {"reports": [dict(r) for r in rows]}

    # -- Recap --
    @app.get("/api/recap/{user_hash}")
    async def recap(user_hash: str):
        pool = _pool()
        if not pool:
            raise HTTPException(503, "Database offline")
        rows = await pool.fetch(
            "SELECT * FROM matches WHERE user_hash = $1 ORDER BY created_at DESC LIMIT 20",
            user_hash,
        )
        if not rows:
            raise HTTPException(404, "No matches found")
        total = len(rows)
        wins = sum(1 for r in rows if r["result"] == "win")
        kills = sum(r["kills"] for r in rows)
        deaths = sum(r["deaths"] for r in rows)
        return {
            "user_hash": user_hash,
            "matches": total,
            "wins": wins,
            "losses": total - wins,
            "win_rate": round(wins / total * 100, 1) if total > 0 else 0,
            "kd": round(kills / max(deaths, 1), 2),
            "total_kills": kills,
            "total_deaths": deaths,
            "recent_matches": [dict(r) for r in rows],
        }

    # -- Weapons --
    @app.get("/api/weapons")
    async def list_weapons(category: str = ""):
        pool = _pool()
        if not pool:
            raise HTTPException(503, "Database offline")
        if category:
            rows = await pool.fetch("SELECT * FROM weapons WHERE category = $1 ORDER BY name", category)
        else:
            rows = await pool.fetch("SELECT * FROM weapons ORDER BY category, name")
        return {"weapons": [dict(r) for r in rows]}

    @app.get("/api/weapons/{name}")
    async def get_weapon(name: str):
        pool = _pool()
        if not pool:
            raise HTTPException(503, "Database offline")
        row = await pool.fetchrow("SELECT * FROM weapons WHERE UPPER(name) = UPPER($1)", name)
        if not row:
            raise HTTPException(404, "Weapon not found")
        return dict(row)

    # -- Tracked Players --
    @app.get("/api/tracked")
    async def list_tracked():
        pool = _pool()
        if not pool:
            raise HTTPException(503, "Database offline")
        rows = await pool.fetch(
            "SELECT display_name, bungie_name, auto_sync, last_synced_at, membership_type "
            "FROM tracked_players ORDER BY display_name"
        )
        return {"tracked": [dict(r) for r in rows]}

    # -- Bungie API proxy --
    @app.get("/api/bungie/search/{name}")
    async def bungie_search(name: str):
        bungie = getattr(bot, "bungie", None)
        if not bungie:
            raise HTTPException(503, "Bungie API not connected")
        from services.bungie import BungieAPIError
        try:
            data = await bungie.search_players(name)
        except BungieAPIError as exc:
            raise HTTPException(502, f"Bungie API error: {exc.message}")
        return data

    @app.get("/api/bungie/player/{bungie_name}")
    async def bungie_player(bungie_name: str):
        bungie = getattr(bot, "bungie", None)
        if not bungie:
            raise HTTPException(503, "Bungie API not connected")
        from services.bungie import BungieAPIError, parse_bungie_name
        try:
            display_name, code = parse_bungie_name(bungie_name)
        except ValueError as exc:
            raise HTTPException(400, str(exc))
        try:
            results = await bungie.search_player_exact(display_name, code)
        except BungieAPIError as exc:
            raise HTTPException(502, f"Bungie API error: {exc.message}")
        return {"memberships": results}

    # -- Server Status --
    @app.get("/api/server-status")
    async def server_status():
        from services.monitor import check_all_endpoints
        results = await check_all_endpoints()
        # Store in DB
        pool = _pool()
        if pool:
            for r in results:
                try:
                    await pool.execute(
                        "INSERT INTO server_status_checks (endpoint, status_code, response_ms, is_up, error) "
                        "VALUES ($1, $2, $3, $4, $5)",
                        r["endpoint"], r.get("status_code", 0), r["response_ms"], r["is_up"], r.get("error", ""),
                    )
                except Exception:
                    pass
        return {"status": results}

    @app.get("/api/server-status/history")
    async def server_status_history(hours: int = 24):
        pool = _pool()
        if not pool:
            raise HTTPException(503, "Database offline")
        hours = min(hours, 168)
        rows = await pool.fetch(
            "SELECT endpoint, "
            "COUNT(*) AS total_checks, "
            "COUNT(*) FILTER (WHERE is_up) AS up_checks, "
            "ROUND(AVG(response_ms)::numeric, 1) AS avg_response, "
            "ROUND(MAX(response_ms)::numeric, 1) AS max_response "
            "FROM server_status_checks "
            "WHERE checked_at > now() - ($1 || ' hours')::interval "
            "GROUP BY endpoint ORDER BY endpoint",
            str(hours),
        )
        return {"uptime": [dict(r) for r in rows]}

    # -- Peak Hours --
    @app.get("/api/peak-hours")
    async def peak_hours(region: str = ""):
        pool = _pool()
        if not pool:
            raise HTTPException(503, "Database offline")
        if region:
            rows = await pool.fetch(
                "SELECT EXTRACT(hour FROM recorded_at)::int AS hour, "
                "COUNT(*) AS samples, "
                "ROUND(AVG(avg_ping_ms)::numeric, 1) AS avg_ping, "
                "ROUND(AVG(jitter_ms)::numeric, 1) AS avg_jitter, "
                "ROUND(AVG(packet_loss)::numeric, 2) AS avg_loss "
                "FROM network_performance WHERE UPPER(region) = UPPER($1) "
                "GROUP BY hour ORDER BY hour",
                region,
            )
        else:
            rows = await pool.fetch(
                "SELECT EXTRACT(hour FROM recorded_at)::int AS hour, "
                "COUNT(*) AS samples, "
                "ROUND(AVG(avg_ping_ms)::numeric, 1) AS avg_ping, "
                "ROUND(AVG(jitter_ms)::numeric, 1) AS avg_jitter, "
                "ROUND(AVG(packet_loss)::numeric, 2) AS avg_loss "
                "FROM network_performance "
                "GROUP BY hour ORDER BY hour"
            )
        return {"peak_hours": [dict(r) for r in rows]}

    # -- TTK Calculator --
    @app.get("/api/ttk/{weapon_name}")
    async def ttk_calc(weapon_name: str, hp: int = 100):
        pool = _pool()
        if not pool:
            raise HTTPException(503, "Database offline")
        row = await pool.fetchrow(
            "SELECT * FROM weapons WHERE UPPER(name) = UPPER($1)", weapon_name
        )
        if not row:
            row = await pool.fetchrow(
                "SELECT * FROM weapons WHERE UPPER(name) LIKE UPPER($1) LIMIT 1",
                f"%{weapon_name}%",
            )
        if not row:
            raise HTTPException(404, "Weapon not found")

        damage = row["damage"]
        fire_rate = row["fire_rate"]
        mag_size = row["mag_size"]
        reload_s = row["reload_s"]

        if damage <= 0 or fire_rate <= 0:
            return {"weapon": row["name"], "error": "Missing damage/fire rate data"}

        shots_to_kill = -(-hp // int(damage))
        time_between = 60.0 / fire_rate
        ttk_ms = (shots_to_kill - 1) * time_between * 1000
        dps = damage * fire_rate / 60

        runner_hps = [85, 90, 100, 110, 140]
        breakdown = {}
        for rhp in runner_hps:
            stk = -(-rhp // int(damage))
            t = (stk - 1) * time_between * 1000
            breakdown[str(rhp)] = {"shots": stk, "ttk_ms": round(t, 1)}

        return {
            "weapon": row["name"],
            "target_hp": hp,
            "shots_to_kill": shots_to_kill,
            "ttk_ms": round(ttk_ms, 1),
            "dps": round(dps, 1),
            "needs_reload": shots_to_kill > mag_size if mag_size > 0 else False,
            "runner_breakdown": breakdown,
        }

    # -- Streaks --
    @app.get("/api/streaks/{user_hash}")
    async def streaks(user_hash: str):
        pool = _pool()
        if not pool:
            raise HTTPException(503, "Database offline")
        rows = await pool.fetch(
            "SELECT result, runner_name, created_at FROM matches "
            "WHERE user_hash = $1 ORDER BY created_at DESC LIMIT 50",
            user_hash,
        )
        if not rows:
            raise HTTPException(404, "No matches found")

        # Current streak
        streak_type = rows[0]["result"]
        current_streak = 0
        for r in rows:
            if r["result"] == streak_type:
                current_streak += 1
            else:
                break

        # Longest streaks
        max_win = max_loss = current_run = 1
        for i in range(1, len(rows)):
            if rows[i]["result"] == rows[i - 1]["result"]:
                current_run += 1
            else:
                if rows[i - 1]["result"] == "win":
                    max_win = max(max_win, current_run)
                elif rows[i - 1]["result"] == "loss":
                    max_loss = max(max_loss, current_run)
                current_run = 1
        if rows[-1]["result"] == "win":
            max_win = max(max_win, current_run)
        elif rows[-1]["result"] == "loss":
            max_loss = max(max_loss, current_run)

        return {
            "user_hash": user_hash,
            "current_streak": {"type": streak_type, "count": current_streak},
            "best_win_streak": max_win,
            "worst_loss_streak": max_loss,
            "last_10": [r["result"] for r in rows[:10]],
        }

    # -- Match Sessions --
    @app.post("/api/sessions")
    async def submit_session(session: MatchSessionSubmission, request: Request):
        client_ip = request.client.host if request.client else "unknown"
        if not _check_rate_limit(client_ip, is_write=True):
            raise HTTPException(429, "Rate limited. Max 30 submissions per minute.")
        pool = _pool()
        if not pool:
            raise HTTPException(503, "Database offline")
        from datetime import datetime, timezone
        started = datetime.fromisoformat(session.started_at) if session.started_at else datetime.now(timezone.utc)
        ended = datetime.fromisoformat(session.ended_at) if session.ended_at else None
        await pool.execute(
            "INSERT INTO match_sessions (user_hash, server_ip, region, started_at, ended_at, "
            "duration_s, peak_ping_ms, avg_ping_ms, total_packets, queue_time_s, patch) "
            "VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)",
            session.user_hash, session.server_ip, session.region, started, ended,
            session.duration_s, session.peak_ping_ms, session.avg_ping_ms,
            session.total_packets, session.queue_time_s, session.patch,
        )
        return {"status": "recorded"}

    @app.get("/api/sessions")
    async def list_sessions(region: str = "", limit: int = 50):
        pool = _pool()
        if not pool:
            raise HTTPException(503, "Database offline")
        limit = min(limit, 200)
        if region:
            rows = await pool.fetch(
                "SELECT * FROM match_sessions WHERE UPPER(region) = UPPER($1) "
                "ORDER BY started_at DESC LIMIT $2",
                region, limit,
            )
        else:
            rows = await pool.fetch(
                "SELECT * FROM match_sessions ORDER BY started_at DESC LIMIT $1", limit
            )
        return {"sessions": [dict(r) for r in rows]}

    @app.get("/api/queue-times")
    async def queue_times():
        pool = _pool()
        if not pool:
            raise HTTPException(503, "Database offline")
        rows = await pool.fetch(
            "SELECT region, "
            "ROUND(AVG(queue_time_s)::numeric, 0) AS avg_queue, "
            "ROUND(MIN(queue_time_s)::numeric, 0) AS min_queue, "
            "ROUND(MAX(queue_time_s)::numeric, 0) AS max_queue, "
            "COUNT(*) AS samples "
            "FROM match_sessions WHERE queue_time_s > 0 "
            "GROUP BY region ORDER BY avg_queue"
        )
        return {"queue_times": [dict(r) for r in rows]}

    # -- Blog Posts --
    @app.get("/api/blog")
    async def list_blog_posts(limit: int = 20):
        pool = _pool()
        if not pool:
            raise HTTPException(503, "Database offline")
        limit = min(limit, 100)
        rows = await pool.fetch(
            "SELECT * FROM blog_posts ORDER BY created_at DESC LIMIT $1", limit
        )
        return {"posts": [dict(r) for r in rows]}

    # -- Live Status (netcapture <-> dashboard bridge) --
    @app.post("/api/live/{user_hash}")
    async def update_live_status(user_hash: str, status: LiveStatusUpdate, request: Request):
        client_ip = request.client.host if request.client else "unknown"
        if not _check_rate_limit(client_ip, is_write=True):
            raise HTTPException(429, "Rate limited.")
        redis = _redis()
        if redis:
            await cache_set(redis, f"marathon:live:{user_hash}", {
                "state": status.state,
                "server_ip": status.server_ip,
                "region": status.region,
                "ping_ms": status.ping_ms,
                "jitter_ms": status.jitter_ms,
                "packet_loss": status.packet_loss,
                "tick_rate": status.tick_rate,
                "match_duration_s": status.match_duration_s,
                "queue_time_s": status.queue_time_s,
                "packets_per_sec": status.packets_per_sec,
                "session_matches": status.session_matches,
                "session_wins": status.session_wins,
                "session_losses": status.session_losses,
            }, 15)
        return {"status": "ok"}

    @app.get("/api/live/{user_hash}")
    async def get_live_status(user_hash: str):
        redis = _redis()
        data = await cache_get(redis, f"marathon:live:{user_hash}")
        if not data:
            return {"state": "offline", "active": False}

        # Enrich with contextual tips based on region and state
        tips = []
        pool = _pool()
        region = data.get("region", "unknown")

        if data.get("state") == "in_match" and pool:
            # Get region-specific best runners
            try:
                top_runners = await pool.fetch(
                    "SELECT runner_name, "
                    "COUNT(*) FILTER (WHERE result = 'win') AS wins, "
                    "COUNT(*) AS total, "
                    "ROUND(COUNT(*) FILTER (WHERE result = 'win')::numeric / GREATEST(COUNT(*), 1) * 100, 1) AS wr "
                    "FROM matches GROUP BY runner_name HAVING COUNT(*) >= 3 "
                    "ORDER BY wr DESC LIMIT 3"
                )
                if top_runners:
                    names = ", ".join(f"{r['runner_name']} ({r['wr']}%)" for r in top_runners)
                    tips.append(f"Top runners right now: {names}")
            except Exception:
                pass

            # Ping quality tip
            ping = data.get("ping_ms", 0)
            if ping > 100:
                tips.append("High ping detected — avoid twitch-aim runners like ASSASSIN")
            elif ping < 40:
                tips.append("Low ping — great for precision runners like RECON")

            # Packet loss warning
            loss = data.get("packet_loss", 0)
            if loss > 2:
                tips.append(f"Packet loss at {loss}% — expect rubber banding")

            # Server reputation
            server_ip = data.get("server_ip", "")
            if server_ip and pool:
                try:
                    bad_server = await pool.fetchrow(
                        "SELECT ROUND(AVG(packet_loss)::numeric, 2) AS avg_loss, COUNT(*) AS reports "
                        "FROM network_performance WHERE server_ip = $1 "
                        "GROUP BY server_ip "
                        "HAVING COUNT(*) >= 3 AND AVG(packet_loss) > 1",
                        server_ip,
                    )
                    if bad_server:
                        tips.append(f"This server has {bad_server['reports']} community reports of issues")
                except Exception:
                    pass

        elif data.get("state") == "queuing":
            tips.append("In queue — good time to check your loadout")

        # Session tilt detection
        session_matches = data.get("session_matches", 0)
        session_losses = data.get("session_losses", 0)
        if session_matches >= 3:
            loss_rate = session_losses / session_matches * 100
            if loss_rate >= 60:
                tips.append("Rough session — consider switching runners or taking a break")
            elif loss_rate <= 30:
                tips.append("You're on fire — keep the momentum going")

        data["active"] = True
        data["tips"] = tips
        return data

    # -- Map Stats --
    @app.get("/api/maps")
    async def map_stats():
        pool = _pool()
        if not pool:
            raise HTTPException(503, "Database offline")
        rows = await pool.fetch(
            "SELECT map_name, "
            "COUNT(*) AS total, "
            "COUNT(*) FILTER (WHERE result = 'win') AS wins, "
            "ROUND(COUNT(*) FILTER (WHERE result = 'win')::numeric / GREATEST(COUNT(*), 1) * 100, 1) AS win_rate, "
            "ROUND(AVG(kills)::numeric, 1) AS avg_kills, "
            "ROUND(AVG(deaths)::numeric, 1) AS avg_deaths, "
            "ROUND(AVG(damage)::numeric, 0) AS avg_damage "
            "FROM matches GROUP BY map_name ORDER BY total DESC"
        )
        return {"maps": [dict(r) for r in rows]}

    # -- Server Blacklist --
    @app.get("/api/servers/problems")
    async def problem_servers():
        pool = _pool()
        if not pool:
            raise HTTPException(503, "Database offline")
        rows = await pool.fetch(
            "SELECT server_ip, region, "
            "ROUND(AVG(avg_ping_ms)::numeric, 1) AS avg_ping, "
            "ROUND(AVG(jitter_ms)::numeric, 1) AS avg_jitter, "
            "ROUND(AVG(packet_loss)::numeric, 2) AS avg_loss, "
            "COUNT(*) AS samples "
            "FROM network_performance WHERE server_ip != '' "
            "GROUP BY server_ip, region "
            "HAVING COUNT(*) >= 3 AND (AVG(packet_loss) > 1 OR AVG(avg_ping_ms) > 100 OR AVG(jitter_ms) > 20) "
            "ORDER BY AVG(packet_loss) DESC LIMIT 10"
        )
        return {"problem_servers": [dict(r) for r in rows]}

    # -- Meta Shift --
    @app.get("/api/meta/shifts")
    async def meta_shifts():
        pool = _pool()
        if not pool:
            raise HTTPException(503, "Database offline")
        rows = await pool.fetch(
            "SELECT * FROM ai_insights WHERE insight_type = 'meta_shift' "
            "ORDER BY created_at DESC LIMIT 10"
        )
        return {"shifts": [dict(r) for r in rows]}

    # -- Community Intel --

    @app.get("/api/community/live")
    async def community_live():
        """Shows what's happening across all active capture agents right now."""
        redis = _redis()
        if not redis:
            raise HTTPException(503, "Redis offline")
        try:
            agents = []
            async for key in redis.scan_iter(match="marathon:live:*"):
                data = await cache_get(redis, key)
                if data:
                    agents.append(data)
        except Exception:
            raise HTTPException(503, "Failed to scan live agents")

        total = len(agents)
        in_match = sum(1 for a in agents if a.get("state") == "in_match")
        queuing = sum(1 for a in agents if a.get("state") == "queuing")

        regions: dict[str, int] = defaultdict(int)
        ping_values = []
        for a in agents:
            region = a.get("region", "unknown")
            regions[region] += 1
            ping = a.get("ping_ms", 0)
            if ping > 0:
                ping_values.append(ping)

        avg_ping = round(sum(ping_values) / len(ping_values), 1) if ping_values else 0

        return {
            "total_active": total,
            "in_match": in_match,
            "queuing": queuing,
            "regions": [{"region": r, "players": c} for r, c in sorted(regions.items(), key=lambda x: -x[1])],
            "avg_ping": avg_ping,
        }

    @app.get("/api/community/trending")
    async def community_trending():
        """What runners are being picked this hour."""
        pool = _pool()
        if not pool:
            raise HTTPException(503, "Database offline")
        rows = await pool.fetch(
            "SELECT runner_name, "
            "COUNT(*) AS picks, "
            "COUNT(*) FILTER (WHERE result = 'win') AS wins, "
            "ROUND(COUNT(*) FILTER (WHERE result = 'win')::numeric / GREATEST(COUNT(*), 1) * 100, 1) AS win_rate "
            "FROM matches "
            "WHERE created_at > now() - INTERVAL '2 hours' "
            "GROUP BY runner_name "
            "ORDER BY picks DESC "
            "LIMIT 10"
        )
        return {
            "window_hours": 2,
            "trending": [dict(r) for r in rows],
        }

    @app.get("/api/community/scouting/{runner_name}")
    async def community_scouting(runner_name: str):
        """Pre-match scouting tips for a specific runner."""
        pool = _pool()
        if not pool:
            raise HTTPException(503, "Database offline")

        # Overall runner stats
        runner = await pool.fetchrow(
            "SELECT * FROM runners WHERE UPPER(name) = UPPER($1)", runner_name
        )
        if not runner:
            raise HTTPException(404, "Runner not found")

        # Map-specific win rates
        map_rows = await pool.fetch(
            "SELECT map_name, "
            "COUNT(*) AS total, "
            "COUNT(*) FILTER (WHERE result = 'win') AS wins, "
            "ROUND(COUNT(*) FILTER (WHERE result = 'win')::numeric / GREATEST(COUNT(*), 1) * 100, 1) AS win_rate "
            "FROM matches "
            "WHERE UPPER(runner_name) = UPPER($1) "
            "GROUP BY map_name "
            "HAVING COUNT(*) >= 2 "
            "ORDER BY win_rate DESC",
            runner_name,
        )

        best_maps = [dict(r) for r in map_rows[:5]]
        worst_maps = [dict(r) for r in sorted(map_rows, key=lambda r: r["win_rate"])[:5]] if map_rows else []

        # Best counter-picks: runners with highest win rate against this runner on the same maps
        counters = await pool.fetch(
            "SELECT m2.runner_name, "
            "COUNT(*) AS encounters, "
            "COUNT(*) FILTER (WHERE m2.result = 'win') AS wins, "
            "ROUND(COUNT(*) FILTER (WHERE m2.result = 'win')::numeric / GREATEST(COUNT(*), 1) * 100, 1) AS win_rate "
            "FROM matches m1 "
            "JOIN matches m2 ON m1.map_name = m2.map_name "
            "  AND m1.created_at::date = m2.created_at::date "
            "  AND UPPER(m1.runner_name) != UPPER(m2.runner_name) "
            "WHERE UPPER(m1.runner_name) = UPPER($1) "
            "  AND m1.result = 'loss' "
            "  AND m2.result = 'win' "
            "GROUP BY m2.runner_name "
            "HAVING COUNT(*) >= 2 "
            "ORDER BY win_rate DESC "
            "LIMIT 5",
            runner_name,
        )

        return {
            "runner": dict(runner),
            "best_maps": best_maps,
            "worst_maps": worst_maps,
            "counters": [dict(r) for r in counters],
        }

    @app.get("/api/community/servers/active")
    async def community_active_servers():
        """Active servers right now from all agents."""
        redis = _redis()
        if not redis:
            raise HTTPException(503, "Redis offline")
        try:
            agents = []
            async for key in redis.scan_iter(match="marathon:live:*"):
                data = await cache_get(redis, key)
                if data:
                    agents.append(data)
        except Exception:
            raise HTTPException(503, "Failed to scan live agents")

        # Group by server_ip and region
        servers: dict[tuple[str, str], list] = defaultdict(list)
        for a in agents:
            server_ip = a.get("server_ip", "")
            if not server_ip:
                continue
            region = a.get("region", "unknown")
            servers[(server_ip, region)].append(a)

        result = []
        for (server_ip, region), group in servers.items():
            pings = [a.get("ping_ms", 0) for a in group if a.get("ping_ms", 0) > 0]
            avg_ping = round(sum(pings) / len(pings), 1) if pings else 0
            result.append({
                "server_ip": server_ip,
                "region": region,
                "player_count": len(group),
                "avg_ping": avg_ping,
            })

        result.sort(key=lambda s: -s["player_count"])
        return {"active_servers": result}

    # -- Shareable Report --
    @app.get("/api/report/{user_hash}")
    async def user_report(user_hash: str):
        pool = _pool()
        if not pool:
            raise HTTPException(503, "Database offline")

        # Latest sessions
        sessions = await pool.fetch(
            "SELECT * FROM match_sessions WHERE user_hash = $1 ORDER BY started_at DESC LIMIT 10",
            user_hash,
        )

        # Match stats
        matches = await pool.fetch(
            "SELECT * FROM matches WHERE user_hash = $1 ORDER BY created_at DESC LIMIT 20",
            user_hash,
        )

        # Network data (user's submissions)
        network = await pool.fetch(
            "SELECT region, "
            "ROUND(AVG(avg_ping_ms)::numeric, 1) AS avg_ping, "
            "ROUND(AVG(jitter_ms)::numeric, 1) AS avg_jitter, "
            "ROUND(AVG(packet_loss)::numeric, 2) AS avg_loss, "
            "ROUND(AVG(tick_rate)::numeric, 0) AS avg_tick_rate, "
            "ROUND(MAX(tick_rate)::numeric, 0) AS max_tick_rate, "
            "COUNT(*) AS samples "
            "FROM network_performance WHERE user_hash = $1 "
            "GROUP BY region ORDER BY avg_ping",
            user_hash,
        )

        # Session summary
        total_duration = sum(s["duration_s"] for s in sessions)
        total_packets = sum(s["total_packets"] for s in sessions)
        peak_ping = max((s["peak_ping_ms"] for s in sessions), default=0)
        regions_played = list(set(s["region"] for s in sessions))

        # Best tick rate from network data
        tick_row = await pool.fetchrow(
            "SELECT MAX(tick_rate) AS max_tick FROM network_performance WHERE user_hash = $1 AND tick_rate > 0",
            user_hash,
        )
        server_tick_rate = int(tick_row["max_tick"]) if tick_row and tick_row["max_tick"] else 0

        # Match summary
        total_matches = len(matches)
        wins = sum(1 for m in matches if m["result"] == "win")
        kills = sum(m["kills"] for m in matches)
        deaths = sum(m["deaths"] for m in matches)
        damage = sum(m["damage"] for m in matches)
        runners_used = {}
        for m in matches:
            runners_used[m["runner_name"]] = runners_used.get(m["runner_name"], 0) + 1
        top_runner = max(runners_used, key=runners_used.get) if runners_used else None

        return {
            "user_hash": user_hash,
            "sessions": {
                "count": len(sessions),
                "total_duration_s": total_duration,
                "total_packets": total_packets,
                "peak_ping_ms": peak_ping,
                "server_tick_rate": server_tick_rate,
                "regions": regions_played,
                "recent": [dict(s) for s in sessions[:5]],
            },
            "matches": {
                "count": total_matches,
                "wins": wins,
                "losses": total_matches - wins,
                "win_rate": round(wins / total_matches * 100, 1) if total_matches > 0 else 0,
                "kd": round(kills / max(deaths, 1), 2),
                "total_kills": kills,
                "total_deaths": deaths,
                "total_damage": damage,
                "top_runner": top_runner,
                "runners_used": runners_used,
            },
            "network": [dict(r) for r in network],
        }

    # -- HTML pages served from public/ --
    @app.get("/")
    async def index():
        f = PUBLIC_DIR / "index.html"
        if f.exists():
            return FileResponse(f)
        return JSONResponse({"name": "Marathon Data Intel", "version": "1.0.0", "docs": "/docs"})

    @app.get("/leaderboard")
    async def leaderboard_page():
        f = PUBLIC_DIR / "leaderboard.html"
        if f.exists():
            return FileResponse(f)
        raise HTTPException(404)

    @app.get("/network")
    async def network_page():
        f = PUBLIC_DIR / "network.html"
        if f.exists():
            return FileResponse(f)
        raise HTTPException(404)

    @app.get("/submit")
    async def submit_page():
        f = PUBLIC_DIR / "submit.html"
        if f.exists():
            return FileResponse(f)
        raise HTTPException(404)

    @app.get("/me")
    async def dashboard_page():
        f = PUBLIC_DIR / "dashboard.html"
        if f.exists():
            return FileResponse(f)
        raise HTTPException(404)

    @app.get("/capture")
    async def capture_page():
        f = PUBLIC_DIR / "capture.html"
        if f.exists():
            return FileResponse(f)
        raise HTTPException(404)

    @app.get("/api-tool")
    async def api_tool_page():
        f = PUBLIC_DIR / "api-tool.html"
        if f.exists():
            return FileResponse(f)
        raise HTTPException(404)

    @app.get("/report/{user_hash}")
    async def report_page(user_hash: str):
        f = PUBLIC_DIR / "report.html"
        if f.exists():
            return FileResponse(f)
        raise HTTPException(404)

    return app
