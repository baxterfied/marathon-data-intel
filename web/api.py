"""FastAPI routes — all API endpoints for Marathon Intel."""

import json
import logging
from collections import defaultdict
from datetime import datetime, timezone
from typing import Optional

from fastapi import FastAPI, Header, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse, Response
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field, field_validator
from pathlib import Path

import config
from services.database import update_sr
from services.redis_cache import (
    cache_get, cache_set, invalidate_match_caches,
    TTL_COMMUNITY_STATS, TTL_LEADERBOARD, TTL_META, TTL_AI_INSIGHT,
)

log = logging.getLogger("marathon.web")

PUBLIC_DIR = Path(__file__).parent.parent / "public"

# Rate limit config
RATE_LIMIT_WINDOW = 60  # seconds
RATE_LIMIT_MAX_WRITES = 30  # max write requests per window per IP
RATE_LIMIT_MAX_READS = 120  # max read requests per window per IP


async def _check_rate_limit(redis_client, client_ip: str, is_write: bool = False) -> bool:
    """Redis-backed sliding window rate limiter. Falls back to allow if Redis is down."""
    if redis_client is None:
        return True
    kind = "w" if is_write else "r"
    key = f"marathon:rate:{client_ip}:{kind}"
    limit = RATE_LIMIT_MAX_WRITES if is_write else RATE_LIMIT_MAX_READS
    try:
        count = await redis_client.incr(key)
        if count == 1:
            await redis_client.expire(key, RATE_LIMIT_WINDOW)
        return count <= limit
    except Exception:
        return True


def _check_api_key(x_api_key: Optional[str]) -> None:
    """Validate API write key. Raises 401 if invalid."""
    if not config.API_WRITE_KEY:
        return  # no key configured = open (backward compat during rollout)
    if x_api_key != config.API_WRITE_KEY:
        raise HTTPException(401, "Invalid or missing API key")


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


class RelayHopInfo(BaseModel):
    ip: str = ""
    region: str = "unknown"
    duration_s: int = 0
    ping_ms: float = 0


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
    relay_hops: list[RelayHopInfo] = Field(default_factory=list)
    session_matches: int = 0
    session_wins: int = 0
    session_losses: int = 0


class NetworkSubmission(BaseModel):
    user_hash: str
    server_ip: str = ""
    region: str = "unknown"
    map_name: str = "unknown"
    avg_ping_ms: float = 0
    min_ping_ms: float = 0
    max_ping_ms: float = 0
    jitter_ms: float = 0
    packet_loss: float = 0
    tick_rate: int = 0
    rtt_samples: int = 0
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
    relay_hops: list[RelayHopInfo] = Field(default_factory=list)
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

    @app.get("/api/matches/{user_hash}")
    async def get_user_matches(user_hash: str, limit: int = 30):
        """Return the last N matches for a user, newest first."""
        pool = _pool()
        if not pool:
            raise HTTPException(503, "Database offline")
        limit = min(max(limit, 1), 200)
        rows = await pool.fetch(
            "SELECT * FROM matches WHERE user_hash = $1 ORDER BY created_at DESC LIMIT $2",
            user_hash, limit,
        )
        return {"matches": [dict(r) for r in rows]}

    # -- Matches --
    @app.post("/api/matches")
    async def submit_match(match: MatchSubmission, request: Request, x_api_key: Optional[str] = Header(None)):
        _check_api_key(x_api_key)
        client_ip = request.client.host if request.client else "unknown"
        if not await _check_rate_limit(_redis(), client_ip, is_write=True):
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

        # Update seasonal ladder SR
        sr_result = None
        try:
            sr_result = await update_sr(
                pool, match.user_hash, match.user_hash,
                match.result, match.kills, match.deaths,
            )
        except Exception as exc:
            log.debug("SR update failed: %s", exc)

        # Generate AI match commentary (non-blocking — failures are swallowed)
        commentary = None
        try:
            ai = _ai()
            if ai:
                from services.ai import generate_match_commentary
                commentary = await generate_match_commentary(ai, {
                    "runner_name": match.runner_name,
                    "map_name": match.map_name,
                    "result": match.result,
                    "kills": match.kills,
                    "deaths": match.deaths,
                    "assists": match.assists,
                    "damage": match.damage,
                    "duration_s": match.duration_s,
                })
        except Exception as exc:
            log.debug("Match commentary generation failed: %s", exc)

        response = {"status": "recorded"}
        if sr_result:
            response["sr"] = sr_result
        if commentary:
            response["commentary"] = commentary
        return response

    # -- Network --
    @app.post("/api/network")
    async def submit_network(data: NetworkSubmission, request: Request, x_api_key: Optional[str] = Header(None)):
        _check_api_key(x_api_key)
        client_ip = request.client.host if request.client else "unknown"
        if not await _check_rate_limit(_redis(), client_ip, is_write=True):
            raise HTTPException(429, "Rate limited. Max 30 submissions per minute.")
        pool = _pool()
        if not pool:
            raise HTTPException(503, "Database offline")
        await pool.execute(
            "INSERT INTO network_performance (user_hash, server_ip, region, map_name, avg_ping_ms, min_ping_ms, max_ping_ms, jitter_ms, packet_loss, tick_rate, rtt_samples, patch) "
            "VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)",
            data.user_hash, data.server_ip, data.region, data.map_name,
            data.avg_ping_ms, data.min_ping_ms, data.max_ping_ms,
            data.jitter_ms, data.packet_loss, data.tick_rate, data.rtt_samples, data.patch,
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
    async def submit_patch(patch: PatchSubmission, x_api_key: Optional[str] = Header(None)):
        _check_api_key(x_api_key)
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
    async def submit_session(session: MatchSessionSubmission, request: Request, x_api_key: Optional[str] = Header(None)):
        _check_api_key(x_api_key)
        client_ip = request.client.host if request.client else "unknown"
        if not await _check_rate_limit(_redis(), client_ip, is_write=True):
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
    async def update_live_status(user_hash: str, status: LiveStatusUpdate, request: Request, x_api_key: Optional[str] = Header(None)):
        _check_api_key(x_api_key)
        client_ip = request.client.host if request.client else "unknown"
        if not await _check_rate_limit(_redis(), client_ip, is_write=True):
            raise HTTPException(429, "Rate limited.")
        redis = _redis()
        if redis:
            # Use user_hash from JSON body (status.user_hash) — the URL path
            # param may be truncated if gamertag contains '#' (treated as fragment)
            live_key = status.user_hash or user_hash
            await cache_set(redis, f"marathon:live:{live_key}", {
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
                "relay_hops": [h.model_dump() for h in status.relay_hops],
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

    # -- Overlay (Twitch browser source) --
    @app.get("/api/overlay/{user_hash}")
    async def overlay_data(user_hash: str):
        """Combined live status + session aggregates for stream overlay."""
        redis = _redis()
        live = await cache_get(redis, f"marathon:live:{user_hash}")

        if not live:
            return {"state": "offline"}

        result = {
            "state": live.get("state", "idle"),
            "region": live.get("region", "unknown"),
            "ping_ms": live.get("ping_ms", 0),
            "match_duration_s": live.get("match_duration_s", 0),
            "session_wins": live.get("session_wins", 0),
            "session_losses": live.get("session_losses", 0),
        }

        # Pull session kill/death aggregates from DB (today's matches)
        pool = _pool()
        if pool:
            try:
                row = await pool.fetchrow(
                    "SELECT COALESCE(SUM(kills), 0) AS kills, "
                    "COALESCE(SUM(deaths), 0) AS deaths "
                    "FROM matches WHERE user_hash = $1 "
                    "AND created_at >= CURRENT_DATE",
                    user_hash,
                )
                if row:
                    result["session_kills"] = int(row["kills"])
                    result["session_deaths"] = int(row["deaths"])
            except Exception:
                pass

        return result

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

    # -- Competitive Intel --

    @app.get("/api/intel/best-times/{user_hash}")
    async def intel_best_times(user_hash: str):
        """Best time to play — network quality and match performance by hour."""
        pool = _pool()
        if not pool:
            raise HTTPException(503, "Database offline")
        try:
            net_rows = await pool.fetch(
                "SELECT EXTRACT(hour FROM recorded_at)::int AS hour, "
                "AVG(avg_ping_ms) AS avg_ping, AVG(jitter_ms) AS avg_jitter, "
                "AVG(packet_loss) AS avg_loss "
                "FROM network_performance WHERE user_hash = $1 "
                "GROUP BY EXTRACT(hour FROM recorded_at) ORDER BY hour",
                user_hash,
            )
            match_rows = await pool.fetch(
                "SELECT EXTRACT(hour FROM created_at)::int AS hour, "
                "COUNT(*) AS matches, "
                "SUM(CASE WHEN result = 'win' THEN 1 ELSE 0 END)::float / COUNT(*) * 100 AS win_rate, "
                "AVG(kills) AS avg_kills, AVG(deaths) AS avg_deaths "
                "FROM matches WHERE user_hash = $1 "
                "GROUP BY EXTRACT(hour FROM created_at) ORDER BY hour",
                user_hash,
            )
            net_by_hour = {r["hour"]: dict(r) for r in net_rows}
            match_by_hour = {r["hour"]: dict(r) for r in match_rows}
            hours = sorted(set(list(net_by_hour.keys()) + list(match_by_hour.keys())))
            combined = []
            for h in hours:
                entry = {"hour": h}
                n = net_by_hour.get(h)
                m = match_by_hour.get(h)
                if n:
                    entry["avg_ping"] = round(float(n["avg_ping"]), 1)
                    entry["avg_jitter"] = round(float(n["avg_jitter"]), 2)
                    entry["avg_loss"] = round(float(n["avg_loss"]), 3)
                else:
                    entry["avg_ping"] = None
                    entry["avg_jitter"] = None
                    entry["avg_loss"] = None
                if m:
                    entry["win_rate"] = round(float(m["win_rate"]), 1)
                    entry["avg_kills"] = round(float(m["avg_kills"]), 1)
                    entry["avg_deaths"] = round(float(m["avg_deaths"]), 1)
                    entry["matches"] = m["matches"]
                else:
                    entry["win_rate"] = None
                    entry["avg_kills"] = None
                    entry["avg_deaths"] = None
                    entry["matches"] = 0
                combined.append(entry)
            return {"user_hash": user_hash, "hours": combined}
        except HTTPException:
            raise
        except Exception as e:
            log.exception("intel_best_times failed")
            raise HTTPException(500, str(e))

    @app.get("/api/intel/server-quality")
    async def intel_server_quality():
        """Server quality rankings based on network performance data."""
        pool = _pool()
        if not pool:
            raise HTTPException(503, "Database offline")
        try:
            rows = await pool.fetch(
                "SELECT server_ip, AVG(avg_ping_ms) AS avg_ping, "
                "AVG(jitter_ms) AS avg_jitter, AVG(packet_loss) AS avg_loss, "
                "AVG(tick_rate) AS avg_tick_rate, COUNT(*) AS sample_count "
                "FROM network_performance GROUP BY server_ip "
                "ORDER BY AVG(packet_loss) ASC, AVG(avg_ping_ms) ASC LIMIT 20"
            )
            servers = []
            for r in rows:
                ping = float(r["avg_ping"])
                loss = float(r["avg_loss"])
                if loss < 1 and ping < 50:
                    quality = "good"
                elif loss < 3 and ping < 100:
                    quality = "ok"
                else:
                    quality = "bad"
                servers.append({
                    "server_ip": r["server_ip"],
                    "avg_ping": round(ping, 1),
                    "avg_jitter": round(float(r["avg_jitter"]), 2),
                    "avg_loss": round(loss, 3),
                    "avg_tick_rate": round(float(r["avg_tick_rate"]), 1) if r["avg_tick_rate"] else None,
                    "sample_count": r["sample_count"],
                    "quality": quality,
                })
            return {"servers": servers}
        except HTTPException:
            raise
        except Exception as e:
            log.exception("intel_server_quality failed")
            raise HTTPException(500, str(e))

    @app.get("/api/intel/session-decay/{user_hash}")
    async def intel_session_decay(user_hash: str):
        """Track performance decay within play sessions."""
        pool = _pool()
        if not pool:
            raise HTTPException(503, "Database offline")
        try:
            match_rows = await pool.fetch(
                "SELECT kills, deaths, result, created_at FROM matches "
                "WHERE user_hash = $1 ORDER BY created_at ASC",
                user_hash,
            )
            if not match_rows:
                return {"user_hash": user_hash, "sessions": [], "message": "No matches found"}

            # Group matches into sessions (gap > 30 min = new session)
            sessions = []
            current_session = [match_rows[0]]
            for i in range(1, len(match_rows)):
                gap = (match_rows[i]["created_at"] - match_rows[i - 1]["created_at"]).total_seconds()
                if gap > 1800:
                    sessions.append(current_session)
                    current_session = [match_rows[i]]
                else:
                    current_session.append(match_rows[i])
            sessions.append(current_session)

            def _calc_stats(matches):
                if not matches:
                    return {"kd": 0, "win_rate": 0}
                k = sum(m["kills"] for m in matches)
                d = sum(m["deaths"] for m in matches)
                w = sum(1 for m in matches if m["result"] == "win")
                return {
                    "kd": round(k / max(d, 1), 2),
                    "win_rate": round(w / len(matches) * 100, 1),
                }

            # Query network performance for ping decay tracking
            net_rows = await pool.fetch(
                "SELECT avg_ping_ms, recorded_at FROM network_performance "
                "WHERE user_hash = $1 ORDER BY recorded_at ASC",
                user_hash,
            )

            session_results = []
            for sess in sessions:
                if len(sess) < 2:
                    continue
                mid = len(sess) // 2
                first_half = sess[:mid]
                second_half = sess[mid:]
                first_stats = _calc_stats(first_half)
                second_stats = _calc_stats(second_half)

                # Check for ping increase during session window
                sess_start = sess[0]["created_at"]
                sess_end = sess[-1]["created_at"]
                session_pings = [
                    float(n["avg_ping_ms"]) for n in net_rows
                    if sess_start <= n["recorded_at"] <= sess_end
                ]
                ping_increased = False
                if len(session_pings) >= 2:
                    first_ping = sum(session_pings[:len(session_pings)//2]) / (len(session_pings)//2)
                    second_ping = sum(session_pings[len(session_pings)//2:]) / len(session_pings[len(session_pings)//2:])
                    ping_increased = second_ping > first_ping * 1.1

                session_results.append({
                    "started_at": sess[0]["created_at"].isoformat(),
                    "match_count": len(sess),
                    "first_half_kd": first_stats["kd"],
                    "second_half_kd": second_stats["kd"],
                    "first_half_win_rate": first_stats["win_rate"],
                    "second_half_win_rate": second_stats["win_rate"],
                    "kd_decay": round(second_stats["kd"] - first_stats["kd"], 2),
                    "win_rate_decay": round(second_stats["win_rate"] - first_stats["win_rate"], 1),
                    "ping_increased": ping_increased,
                })

            return {"user_hash": user_hash, "sessions": session_results}
        except HTTPException:
            raise
        except Exception as e:
            log.exception("intel_session_decay failed")
            raise HTTPException(500, str(e))

    @app.get("/api/intel/queue-predict/{region}")
    async def intel_queue_predict(region: str):
        """Predict queue times by hour for a given region."""
        pool = _pool()
        if not pool:
            raise HTTPException(503, "Database offline")
        try:
            from datetime import datetime, timezone
            rows = await pool.fetch(
                "SELECT EXTRACT(hour FROM started_at)::int AS hour, "
                "AVG(queue_time_s) AS avg_queue_time, COUNT(*) AS sample_count "
                "FROM match_sessions WHERE UPPER(region) = UPPER($1) AND queue_time_s > 0 "
                "GROUP BY EXTRACT(hour FROM started_at) ORDER BY hour",
                region,
            )
            if not rows:
                return {"region": region, "predictions": [], "message": "No queue data for this region"}

            by_hour = {r["hour"]: dict(r) for r in rows}
            current_hour = datetime.now(timezone.utc).hour
            predictions = []
            for offset in range(4):
                h = (current_hour + offset) % 24
                data = by_hour.get(h)
                predictions.append({
                    "hour": h,
                    "avg_queue_time_s": round(float(data["avg_queue_time"]), 1) if data else None,
                    "confidence": data["sample_count"] if data else 0,
                    "label": "current" if offset == 0 else f"+{offset}h",
                })

            return {"region": region, "predictions": predictions}
        except HTTPException:
            raise
        except Exception as e:
            log.exception("intel_queue_predict failed")
            raise HTTPException(500, str(e))

    @app.get("/api/intel/lobby-intensity/{user_hash}")
    async def intel_lobby_intensity(user_hash: str):
        """Match pacing/intensity based on packets per second."""
        pool = _pool()
        if not pool:
            raise HTTPException(503, "Database offline")
        try:
            rows = await pool.fetch(
                "SELECT server_ip, region, started_at, duration_s, total_packets, avg_ping_ms "
                "FROM match_sessions WHERE user_hash = $1 AND duration_s > 0 "
                "ORDER BY started_at DESC LIMIT 20",
                user_hash,
            )
            if not rows:
                return {"user_hash": user_hash, "sessions": [], "message": "No session data found"}

            sessions = []
            for r in rows:
                duration = float(r["duration_s"])
                total_packets = r["total_packets"] or 0
                pps = round(total_packets / max(duration, 1), 1)
                if pps < 80:
                    intensity = "calm"
                elif pps <= 120:
                    intensity = "normal"
                else:
                    intensity = "intense"
                sessions.append({
                    "server_ip": r["server_ip"],
                    "region": r["region"],
                    "started_at": r["started_at"].isoformat() if r["started_at"] else None,
                    "duration_s": duration,
                    "total_packets": total_packets,
                    "packets_per_second": pps,
                    "intensity": intensity,
                    "avg_ping_ms": round(float(r["avg_ping_ms"]), 1) if r["avg_ping_ms"] else None,
                })

            return {"user_hash": user_hash, "sessions": sessions}
        except HTTPException:
            raise
        except Exception as e:
            log.exception("intel_lobby_intensity failed")
            raise HTTPException(500, str(e))

    @app.get("/api/intel/performance/{user_hash}")
    async def intel_performance(user_hash: str):
        """Personal performance summary — best/worst runners, maps, trends."""
        pool = _pool()
        if not pool:
            raise HTTPException(503, "Database offline")
        try:
            rows = await pool.fetch(
                "SELECT runner_name, map_name, result, kills, deaths, duration_s, created_at "
                "FROM matches WHERE user_hash = $1 ORDER BY created_at DESC",
                user_hash,
            )
            if not rows:
                return {"user_hash": user_hash, "message": "No matches found"}

            # Runner stats (min 3 matches)
            runner_stats: dict[str, dict] = defaultdict(lambda: {"wins": 0, "total": 0})
            for r in rows:
                name = r["runner_name"]
                runner_stats[name]["total"] += 1
                if r["result"] == "win":
                    runner_stats[name]["wins"] += 1

            qualified_runners = {
                k: v for k, v in runner_stats.items() if v["total"] >= 3
            }
            best_runner = None
            worst_runner = None
            if qualified_runners:
                best_runner = max(
                    qualified_runners,
                    key=lambda k: qualified_runners[k]["wins"] / qualified_runners[k]["total"],
                )
                worst_runner = min(
                    qualified_runners,
                    key=lambda k: qualified_runners[k]["wins"] / qualified_runners[k]["total"],
                )

            # Map stats
            map_stats: dict[str, dict] = defaultdict(lambda: {"wins": 0, "total": 0})
            for r in rows:
                name = r["map_name"]
                map_stats[name]["total"] += 1
                if r["result"] == "win":
                    map_stats[name]["wins"] += 1

            qualified_maps = {k: v for k, v in map_stats.items() if v["total"] >= 3}
            best_map = None
            worst_map = None
            if qualified_maps:
                best_map = max(
                    qualified_maps,
                    key=lambda k: qualified_maps[k]["wins"] / qualified_maps[k]["total"],
                )
                worst_map = min(
                    qualified_maps,
                    key=lambda k: qualified_maps[k]["wins"] / qualified_maps[k]["total"],
                )

            # Average session length
            durations = [float(r["duration_s"]) for r in rows if r["duration_s"]]
            avg_session_length = round(sum(durations) / len(durations), 1) if durations else 0

            # Performance trend: last 10 vs previous 10
            recent_10 = list(rows[:10])
            prev_10 = list(rows[10:20])
            def _trend_stats(matches):
                if not matches:
                    return {"win_rate": 0, "kd": 0}
                w = sum(1 for m in matches if m["result"] == "win")
                k = sum(m["kills"] for m in matches)
                d = sum(m["deaths"] for m in matches)
                return {
                    "win_rate": round(w / len(matches) * 100, 1),
                    "kd": round(k / max(d, 1), 2),
                }

            recent_stats = _trend_stats(recent_10)
            prev_stats = _trend_stats(prev_10)
            trend = "stable"
            if prev_stats["win_rate"] > 0:
                if recent_stats["win_rate"] > prev_stats["win_rate"] + 5:
                    trend = "improving"
                elif recent_stats["win_rate"] < prev_stats["win_rate"] - 5:
                    trend = "declining"

            def _runner_info(name, stats_dict):
                if not name:
                    return None
                s = stats_dict[name]
                return {
                    "name": name,
                    "win_rate": round(s["wins"] / s["total"] * 100, 1),
                    "matches": s["total"],
                }

            return {
                "user_hash": user_hash,
                "total_matches": len(rows),
                "best_runner": _runner_info(best_runner, qualified_runners),
                "worst_runner": _runner_info(worst_runner, qualified_runners),
                "best_map": _runner_info(best_map, qualified_maps),
                "worst_map": _runner_info(worst_map, qualified_maps),
                "avg_session_length_s": avg_session_length,
                "trend": {
                    "direction": trend,
                    "recent": recent_stats,
                    "previous": prev_stats,
                },
            }
        except HTTPException:
            raise
        except Exception as e:
            log.exception("intel_performance failed")
            raise HTTPException(500, str(e))

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

    # -- SVG Stats Card (OG:image) --
    @app.get("/api/card/{user_hash}")
    async def stats_card(user_hash: str):
        redis = _redis()
        cache_key = f"marathon:card:{user_hash}"
        cached = await cache_get(redis, cache_key) if redis else None
        if cached:
            return Response(content=cached["svg"], media_type="image/svg+xml",
                            headers={"Cache-Control": "public, max-age=900"})

        pool = _pool()
        if not pool:
            raise HTTPException(503, "Database offline")

        # Aggregate match stats
        row = await pool.fetchrow(
            "SELECT COUNT(*) AS total, "
            "SUM(CASE WHEN result = 'win' THEN 1 ELSE 0 END) AS wins, "
            "SUM(kills) AS kills, SUM(deaths) AS deaths, "
            "SUM(damage) AS damage "
            "FROM matches WHERE user_hash = $1",
            user_hash,
        )

        total = int(row["total"]) if row["total"] else 0
        wins = int(row["wins"]) if row["wins"] else 0
        kills = int(row["kills"]) if row["kills"] else 0
        deaths = int(row["deaths"]) if row["deaths"] else 0
        damage = int(row["damage"]) if row["damage"] else 0
        win_rate = round(wins / total * 100, 1) if total > 0 else 0
        kd = round(kills / max(deaths, 1), 2)

        # Most-played runner
        runner_row = await pool.fetchrow(
            "SELECT runner_name, COUNT(*) AS cnt FROM matches "
            "WHERE user_hash = $1 GROUP BY runner_name ORDER BY cnt DESC LIMIT 1",
            user_hash,
        )
        main_runner = runner_row["runner_name"] if runner_row else "N/A"

        # Format damage for display
        if damage >= 1_000_000:
            damage_str = f"{damage / 1_000_000:.1f}M"
        elif damage >= 1_000:
            damage_str = f"{damage / 1_000:.1f}K"
        else:
            damage_str = str(damage)

        # Determine accent colors for stats
        wr_color = "#00ff88" if win_rate >= 55 else "#ffcc00" if win_rate >= 45 else "#ff4455"
        kd_color = "#00ff88" if kd >= 1.5 else "#ffcc00" if kd >= 1.0 else "#ff4455"

        # Escape gamertag for SVG/XML safety
        import html as html_mod
        safe_tag = html_mod.escape(user_hash)

        svg = f'''<svg xmlns="http://www.w3.org/2000/svg" width="1200" height="630" viewBox="0 0 1200 630">
  <defs>
    <linearGradient id="bg" x1="0" y1="0" x2="1" y2="1">
      <stop offset="0%" stop-color="#0a0a0f"/>
      <stop offset="100%" stop-color="#0f1118"/>
    </linearGradient>
    <linearGradient id="accent" x1="0" y1="0" x2="1" y2="0">
      <stop offset="0%" stop-color="#00ff88"/>
      <stop offset="100%" stop-color="#00cc6a"/>
    </linearGradient>
  </defs>

  <!-- Background -->
  <rect width="1200" height="630" fill="url(#bg)"/>

  <!-- Border accent -->
  <rect x="0" y="0" width="1200" height="4" fill="url(#accent)"/>
  <rect x="0" y="626" width="1200" height="4" fill="url(#accent)" opacity="0.3"/>

  <!-- Decorative grid lines -->
  <line x1="0" y1="140" x2="1200" y2="140" stroke="#00ff88" stroke-opacity="0.08" stroke-width="1"/>
  <line x1="0" y1="420" x2="1200" y2="420" stroke="#00ff88" stroke-opacity="0.08" stroke-width="1"/>
  <line x1="80" y1="0" x2="80" y2="630" stroke="#00ff88" stroke-opacity="0.04" stroke-width="1"/>
  <line x1="1120" y1="0" x2="1120" y2="630" stroke="#00ff88" stroke-opacity="0.04" stroke-width="1"/>

  <!-- Corner accents -->
  <polyline points="0,40 0,0 40,0" fill="none" stroke="#00ff88" stroke-width="2" opacity="0.4"/>
  <polyline points="1160,0 1200,0 1200,40" fill="none" stroke="#00ff88" stroke-width="2" opacity="0.4"/>
  <polyline points="0,590 0,630 40,630" fill="none" stroke="#00ff88" stroke-width="2" opacity="0.4"/>
  <polyline points="1160,630 1200,630 1200,590" fill="none" stroke="#00ff88" stroke-width="2" opacity="0.4"/>

  <!-- Header: MARATHON INTEL -->
  <text x="600" y="75" text-anchor="middle" font-family="Arial Black, Arial, sans-serif"
        font-size="42" font-weight="900" fill="#00ff88" letter-spacing="12">MARATHON INTEL</text>

  <!-- Gamertag -->
  <text x="600" y="125" text-anchor="middle" font-family="Consolas, monospace"
        font-size="28" fill="#ffffff" opacity="0.95">{safe_tag}</text>

  <!-- Divider line -->
  <line x1="300" y1="155" x2="900" y2="155" stroke="#00ff88" stroke-opacity="0.25" stroke-width="1"/>

  <!-- Stats boxes -->
  <!-- Matches -->
  <rect x="90" y="190" width="180" height="160" rx="8" fill="#161b22" stroke="#30363d" stroke-width="1"/>
  <text x="180" y="235" text-anchor="middle" font-family="Arial, sans-serif"
        font-size="13" fill="#8b949e" letter-spacing="3" font-weight="600">MATCHES</text>
  <text x="180" y="310" text-anchor="middle" font-family="Arial Black, Arial, sans-serif"
        font-size="56" font-weight="900" fill="#ffffff">{total}</text>

  <!-- Win Rate -->
  <rect x="300" y="190" width="180" height="160" rx="8" fill="#161b22" stroke="#30363d" stroke-width="1"/>
  <text x="390" y="235" text-anchor="middle" font-family="Arial, sans-serif"
        font-size="13" fill="#8b949e" letter-spacing="3" font-weight="600">WIN RATE</text>
  <text x="390" y="310" text-anchor="middle" font-family="Arial Black, Arial, sans-serif"
        font-size="56" font-weight="900" fill="{wr_color}">{win_rate}%</text>

  <!-- K/D -->
  <rect x="510" y="190" width="180" height="160" rx="8" fill="#161b22" stroke="#30363d" stroke-width="1"/>
  <text x="600" y="235" text-anchor="middle" font-family="Arial, sans-serif"
        font-size="13" fill="#8b949e" letter-spacing="3" font-weight="600">K/D RATIO</text>
  <text x="600" y="310" text-anchor="middle" font-family="Arial Black, Arial, sans-serif"
        font-size="56" font-weight="900" fill="{kd_color}">{kd}</text>

  <!-- Main Runner -->
  <rect x="720" y="190" width="180" height="160" rx="8" fill="#161b22" stroke="#30363d" stroke-width="1"/>
  <text x="810" y="235" text-anchor="middle" font-family="Arial, sans-serif"
        font-size="13" fill="#8b949e" letter-spacing="3" font-weight="600">MAIN RUNNER</text>
  <text x="810" y="305" text-anchor="middle" font-family="Arial Black, Arial, sans-serif"
        font-size="36" font-weight="900" fill="#00ff88">{html_mod.escape(main_runner)}</text>

  <!-- Total Damage -->
  <rect x="930" y="190" width="180" height="160" rx="8" fill="#161b22" stroke="#30363d" stroke-width="1"/>
  <text x="1020" y="235" text-anchor="middle" font-family="Arial, sans-serif"
        font-size="13" fill="#8b949e" letter-spacing="3" font-weight="600">TOTAL DMG</text>
  <text x="1020" y="310" text-anchor="middle" font-family="Arial Black, Arial, sans-serif"
        font-size="56" font-weight="900" fill="#ffffff">{damage_str}</text>

  <!-- Kill / Death breakdown -->
  <text x="180" y="430" text-anchor="middle" font-family="Consolas, monospace"
        font-size="16" fill="#8b949e">{kills} kills / {deaths} deaths</text>
  <text x="600" y="430" text-anchor="middle" font-family="Consolas, monospace"
        font-size="16" fill="#8b949e">{wins}W - {total - wins}L</text>

  <!-- Decorative hex pattern (subtle) -->
  <circle cx="150" cy="530" r="30" fill="none" stroke="#00ff88" stroke-opacity="0.06" stroke-width="1"/>
  <circle cx="1050" cy="530" r="30" fill="none" stroke="#00ff88" stroke-opacity="0.06" stroke-width="1"/>

  <!-- Branding footer -->
  <text x="600" y="560" text-anchor="middle" font-family="Consolas, monospace"
        font-size="14" fill="#00ff88" opacity="0.6">marathon.straightfirefood.blog</text>
  <text x="600" y="590" text-anchor="middle" font-family="Arial, sans-serif"
        font-size="12" fill="#6b7280">Community Network Capture Intelligence</text>
</svg>'''

        # Cache for 15 minutes
        await cache_set(redis, cache_key, {"svg": svg}, 900)

        return Response(content=svg, media_type="image/svg+xml",
                        headers={"Cache-Control": "public, max-age=900"})

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

    @app.get("/exports/{filename}")
    async def download_export(filename: str):
        """Serve export files for download."""
        import re
        if not re.match(r'^[\w\-]+\.json$', filename):
            raise HTTPException(400, "Invalid filename")
        f = PUBLIC_DIR / "exports" / filename
        if not f.exists():
            raise HTTPException(404)
        return FileResponse(f, media_type="application/json", filename=filename)

    @app.get("/api-tool")
    async def api_tool_page():
        f = PUBLIC_DIR / "api-tool.html"
        if f.exists():
            return FileResponse(f)
        raise HTTPException(404)

    @app.get("/overlay/{user_hash}")
    async def overlay_page(user_hash: str):
        f = PUBLIC_DIR / "overlay.html"
        if f.exists():
            return FileResponse(f)
        raise HTTPException(404)

    @app.get("/report/{user_hash}")
    async def report_page(user_hash: str):
        f = PUBLIC_DIR / "report.html"
        if not f.exists():
            raise HTTPException(404)
        import html as html_mod
        safe_tag = html_mod.escape(user_hash)
        content = f.read_text()
        # Replace the existing static OG tags with dynamic ones
        content = content.replace(
            '<meta property="og:title" content="Marathon Intel Report">',
            f'<meta property="og:title" content="{safe_tag} - Marathon Intel Report">',
        )
        content = content.replace(
            '<meta property="og:description" content="Live network capture data from Marathon">',
            f'<meta property="og:description" content="Player stats and network capture data for {safe_tag}">',
        )
        content = content.replace(
            '<meta property="og:type" content="website">',
            '<meta property="og:type" content="website">\n'
            f'    <meta property="og:image" content="/api/card/{safe_tag}">\n'
            f'    <meta property="og:image:width" content="1200">\n'
            f'    <meta property="og:image:height" content="630">\n'
            f'    <meta name="twitter:card" content="summary_large_image">\n'
            f'    <meta name="twitter:title" content="{safe_tag} - Marathon Intel Report">\n'
            f'    <meta name="twitter:image" content="/api/card/{safe_tag}">',
        )
        return HTMLResponse(content=content)

    # -- Community Intel Feed --
    @app.get("/api/feed")
    async def intel_feed():
        """Aggregated community intel feed with recent activity across all data sources."""
        redis = _redis()
        cached = await cache_get(redis, "marathon:feed")
        if cached:
            return cached

        pool = _pool()
        if not pool:
            raise HTTPException(503, "Database offline")

        feed_items = []
        now = datetime.now(timezone.utc)

        # 1. Recent patch notes with AI analysis
        try:
            patches = await pool.fetch(
                "SELECT version, title, summary, ai_analysis, released_at "
                "FROM patch_notes ORDER BY released_at DESC LIMIT 3"
            )
            for p in patches:
                body = p["summary"] or ""
                if p["ai_analysis"]:
                    body += f" | AI: {p['ai_analysis'][:200]}"
                feed_items.append({
                    "type": "patch",
                    "title": f"Patch {p['version']}: {p['title'] or 'Update'}",
                    "body": body[:300],
                    "timestamp": str(p["released_at"]) if p["released_at"] else str(now),
                })
        except Exception as exc:
            log.warning("Feed: patches query failed: %s", exc)

        # 2. Meta shifts from AI insights
        try:
            shifts = await pool.fetch(
                "SELECT title, content, created_at FROM ai_insights "
                "WHERE insight_type = 'meta_shift' "
                "ORDER BY created_at DESC LIMIT 3"
            )
            for s in shifts:
                feed_items.append({
                    "type": "meta",
                    "title": s["title"] or "Meta Shift Detected",
                    "body": (s["content"] or "")[:300],
                    "timestamp": str(s["created_at"]),
                })
        except Exception as exc:
            log.warning("Feed: meta shifts query failed: %s", exc)

        # 3. Trending runners (last 2 hours)
        try:
            trending = await pool.fetch(
                "SELECT runner_name, COUNT(*) AS picks, "
                "ROUND(COUNT(*) FILTER (WHERE result = 'win')::numeric / GREATEST(COUNT(*), 1) * 100, 1) AS win_rate "
                "FROM matches WHERE created_at > now() - INTERVAL '2 hours' "
                "GROUP BY runner_name ORDER BY picks DESC LIMIT 5"
            )
            if trending:
                top_names = ", ".join(f"{r['runner_name']} ({r['picks']} picks, {r['win_rate']}% WR)" for r in trending[:3])
                feed_items.append({
                    "type": "trending",
                    "title": "Trending Runners Right Now",
                    "body": f"Top picks in the last 2h: {top_names}",
                    "timestamp": str(now),
                })
        except Exception as exc:
            log.warning("Feed: trending query failed: %s", exc)

        # 4. Recent blog post summaries
        try:
            posts = await pool.fetch(
                "SELECT title, summary, created_at FROM blog_posts "
                "ORDER BY created_at DESC LIMIT 2"
            )
            for p in posts:
                feed_items.append({
                    "type": "intel",
                    "title": p["title"] or "Intel Report",
                    "body": (p["summary"] or "")[:300],
                    "timestamp": str(p["created_at"]),
                })
        except Exception as exc:
            log.warning("Feed: blog query failed: %s", exc)

        # 5. Active community stats
        try:
            agents_online = 0
            if redis:
                count = 0
                async for _key in redis.scan_iter(match="marathon:live:*"):
                    count += 1
                agents_online = count

            matches_today_row = await pool.fetchrow(
                "SELECT COUNT(*) AS cnt FROM matches WHERE created_at > now() - INTERVAL '24 hours'"
            )
            matches_today = matches_today_row["cnt"] if matches_today_row else 0

            if agents_online > 0 or matches_today > 0:
                feed_items.append({
                    "type": "intel",
                    "title": "Community Activity",
                    "body": f"{agents_online} agents online, {matches_today} matches in the last 24h.",
                    "timestamp": str(now),
                })
        except Exception as exc:
            log.warning("Feed: community stats query failed: %s", exc)

        # Sort by timestamp descending
        feed_items.sort(key=lambda x: x.get("timestamp", ""), reverse=True)

        result = {"feed": feed_items}
        await cache_set(redis, "marathon:feed", result, TTL_COMMUNITY_STATS)  # 5 min
        return result

    # -- Seasonal Ladder --

    @app.get("/api/ladder")
    async def ladder(limit: int = 100):
        pool = _pool()
        if not pool:
            raise HTTPException(503, "Database offline")
        season = await pool.fetchrow(
            "SELECT id, name FROM seasons WHERE active = true ORDER BY started_at DESC LIMIT 1"
        )
        if not season:
            return {"season": None, "ladder": []}
        limit = min(limit, 100)
        rows = await pool.fetch(
            "SELECT user_hash, display_name, sr, tier, matches, wins, losses, peak_sr, updated_at "
            "FROM seasonal_ratings WHERE season_id = $1 ORDER BY sr DESC LIMIT $2",
            season["id"], limit,
        )
        ladder_list = []
        for rank, r in enumerate(rows, 1):
            ladder_list.append({
                "rank": rank,
                "user_hash": r["user_hash"],
                "display_name": r["display_name"],
                "sr": r["sr"],
                "tier": r["tier"],
                "matches": r["matches"],
                "wins": r["wins"],
                "losses": r["losses"],
                "peak_sr": r["peak_sr"],
                "updated_at": r["updated_at"].isoformat() if r["updated_at"] else None,
            })
        return {"season": {"id": season["id"], "name": season["name"]}, "ladder": ladder_list}

    @app.get("/api/ladder/{user_hash}")
    async def ladder_player(user_hash: str):
        pool = _pool()
        if not pool:
            raise HTTPException(503, "Database offline")
        season = await pool.fetchrow(
            "SELECT id, name FROM seasons WHERE active = true ORDER BY started_at DESC LIMIT 1"
        )
        if not season:
            raise HTTPException(404, "No active season")
        row = await pool.fetchrow(
            "SELECT user_hash, display_name, sr, tier, matches, wins, losses, peak_sr, updated_at "
            "FROM seasonal_ratings WHERE season_id = $1 AND user_hash = $2",
            season["id"], user_hash,
        )
        if not row:
            raise HTTPException(404, "Player not found in current season")
        # Calculate rank
        rank_row = await pool.fetchrow(
            "SELECT COUNT(*) + 1 AS rank FROM seasonal_ratings "
            "WHERE season_id = $1 AND sr > $2",
            season["id"], row["sr"],
        )
        return {
            "season": {"id": season["id"], "name": season["name"]},
            "rank": int(rank_row["rank"]) if rank_row else 0,
            "user_hash": row["user_hash"],
            "display_name": row["display_name"],
            "sr": row["sr"],
            "tier": row["tier"],
            "matches": row["matches"],
            "wins": row["wins"],
            "losses": row["losses"],
            "peak_sr": row["peak_sr"],
            "updated_at": row["updated_at"].isoformat() if row["updated_at"] else None,
        }

    @app.get("/api/seasons")
    async def list_seasons():
        pool = _pool()
        if not pool:
            raise HTTPException(503, "Database offline")
        rows = await pool.fetch(
            "SELECT id, name, started_at, ended_at, active FROM seasons ORDER BY started_at DESC"
        )
        return {"seasons": [
            {
                "id": r["id"],
                "name": r["name"],
                "started_at": r["started_at"].isoformat() if r["started_at"] else None,
                "ended_at": r["ended_at"].isoformat() if r["ended_at"] else None,
                "active": r["active"],
            }
            for r in rows
        ]}

    # -- Player Profile --
    @app.get("/api/player/{user_hash}")
    async def player_profile(user_hash: str):
        """Comprehensive player profile data."""
        pool = _pool()
        if not pool:
            raise HTTPException(503, "Database offline")

        # All matches for this player
        matches = await pool.fetch(
            "SELECT runner_name, map_name, result, kills, deaths, assists, damage, duration_s, created_at "
            "FROM matches WHERE user_hash = $1 ORDER BY created_at DESC",
            user_hash,
        )
        if not matches:
            raise HTTPException(404, "No data found for this player")

        total = len(matches)
        wins = sum(1 for m in matches if m["result"] == "win")
        losses = sum(1 for m in matches if m["result"] == "loss")
        kills = sum(m["kills"] for m in matches)
        deaths = sum(m["deaths"] for m in matches)
        total_damage = sum(m["damage"] for m in matches)
        total_duration = sum(m["duration_s"] for m in matches)
        avg_damage = round(total_damage / total, 1) if total > 0 else 0
        kd = round(kills / max(deaths, 1), 2)
        win_rate = round(wins / total * 100, 1) if total > 0 else 0

        # Member since
        member_since = matches[-1]["created_at"].isoformat() if matches else None

        # Main runner
        runner_counts: dict[str, int] = defaultdict(int)
        for m in matches:
            runner_counts[m["runner_name"]] += 1
        main_runner = max(runner_counts, key=runner_counts.get) if runner_counts else None

        # Rank from leaderboard_cache
        rank_row = await pool.fetchrow(
            "SELECT rank FROM leaderboard_cache WHERE user_hash = $1", user_hash
        )
        rank = int(rank_row["rank"]) if rank_row and rank_row["rank"] else None

        # Per-runner breakdown with comfort score
        runner_data: dict[str, dict] = defaultdict(lambda: {
            "wins": 0, "losses": 0, "matches": 0,
            "kills": 0, "deaths": 0, "damage": 0,
        })
        for m in matches:
            rd = runner_data[m["runner_name"]]
            rd["matches"] += 1
            if m["result"] == "win":
                rd["wins"] += 1
            elif m["result"] == "loss":
                rd["losses"] += 1
            rd["kills"] += m["kills"]
            rd["deaths"] += m["deaths"]
            rd["damage"] += m["damage"]

        runners_list = []
        for rname, rd in runner_data.items():
            r_wr = round(rd["wins"] / rd["matches"] * 100, 1) if rd["matches"] > 0 else 0
            r_kd = round(rd["kills"] / max(rd["deaths"], 1), 2)
            comfort = min(100, round((rd["matches"] * 2) + (r_wr * 0.5)))
            runners_list.append({
                "runner_name": rname,
                "matches": rd["matches"],
                "wins": rd["wins"],
                "losses": rd["losses"],
                "win_rate": r_wr,
                "kd": r_kd,
                "avg_damage": round(rd["damage"] / rd["matches"], 1) if rd["matches"] > 0 else 0,
                "comfort_score": comfort,
            })
        runners_list.sort(key=lambda x: -x["matches"])

        # Per-map breakdown
        map_data: dict[str, dict] = defaultdict(lambda: {"wins": 0, "matches": 0})
        for m in matches:
            md = map_data[m["map_name"]]
            md["matches"] += 1
            if m["result"] == "win":
                md["wins"] += 1
        maps_list = []
        for mname, md in map_data.items():
            m_wr = round(md["wins"] / md["matches"] * 100, 1) if md["matches"] > 0 else 0
            maps_list.append({
                "map_name": mname,
                "matches": md["matches"],
                "wins": md["wins"],
                "win_rate": m_wr,
            })
        maps_list.sort(key=lambda x: -x["matches"])

        # Last 20 matches
        recent = []
        for m in matches[:20]:
            recent.append({
                "runner_name": m["runner_name"],
                "map_name": m["map_name"],
                "result": m["result"],
                "kills": m["kills"],
                "deaths": m["deaths"],
                "assists": m["assists"],
                "damage": m["damage"],
                "duration_s": m["duration_s"],
                "created_at": m["created_at"].isoformat() if m["created_at"] else None,
            })

        # Network stats summary from match_sessions
        net_rows = await pool.fetch(
            "SELECT region, "
            "ROUND(AVG(peak_ping_ms)::numeric, 1) AS peak_ping, "
            "ROUND(AVG(avg_ping_ms)::numeric, 1) AS avg_ping, "
            "COUNT(*) AS sessions "
            "FROM match_sessions WHERE user_hash = $1 "
            "GROUP BY region ORDER BY sessions DESC",
            user_hash,
        )
        network = [dict(r) for r in net_rows]

        return {
            "user_hash": user_hash,
            "member_since": member_since,
            "overall": {
                "total_matches": total,
                "wins": wins,
                "losses": losses,
                "win_rate": win_rate,
                "kd": kd,
                "avg_damage": avg_damage,
                "total_damage": total_damage,
                "total_duration_s": total_duration,
                "main_runner": main_runner,
                "rank": rank,
            },
            "runners": runners_list,
            "maps": maps_list,
            "recent_matches": recent,
            "network": network,
        }

    @app.get("/player/{user_hash}")
    async def player_page(user_hash: str):
        f = PUBLIC_DIR / "player.html"
        if not f.exists():
            raise HTTPException(404)
        import html as html_mod
        safe_tag = html_mod.escape(user_hash)
        content = f.read_text()
        content = content.replace(
            '<meta property="og:title" content="Player Profile - Marathon Intel">',
            f'<meta property="og:title" content="{safe_tag} - Marathon Intel">',
        )
        content = content.replace(
            '<meta property="og:description" content="Player stats and performance data from Marathon">',
            f'<meta property="og:description" content="Stats, runners, and match history for {safe_tag} on Marathon Intel">',
        )
        content = content.replace(
            '<meta property="og:type" content="website">',
            '<meta property="og:type" content="website">\n'
            f'    <meta property="og:image" content="/api/card/{safe_tag}">\n'
            f'    <meta property="og:image:width" content="1200">\n'
            f'    <meta property="og:image:height" content="630">\n'
            f'    <meta name="twitter:card" content="summary_large_image">\n'
            f'    <meta name="twitter:title" content="{safe_tag} - Marathon Intel">\n'
            f'    <meta name="twitter:image" content="/api/card/{safe_tag}">',
        )
        return HTMLResponse(content=content)

    return app
