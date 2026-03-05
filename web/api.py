"""FastAPI routes — all API endpoints for Marathon Intel."""

import json
import logging
from typing import Optional

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field
from pathlib import Path

from services.redis_cache import (
    cache_get, cache_set, invalidate_match_caches,
    TTL_COMMUNITY_STATS, TTL_LEADERBOARD, TTL_META, TTL_AI_INSIGHT,
)

log = logging.getLogger("marathon.web")

PUBLIC_DIR = Path(__file__).parent.parent / "public"


# -- Pydantic models --

class MatchSubmission(BaseModel):
    user_hash: str
    runner_name: str
    map_name: str = "unknown"
    mode: str = "extraction"
    result: str  # win, loss, draw
    kills: int = 0
    deaths: int = 0
    assists: int = 0
    damage: int = 0
    duration_s: int = 0
    loadout: dict = Field(default_factory=dict)
    patch: str = "1.0"


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


def create_app(bot) -> FastAPI:
    app = FastAPI(title="Marathon Data Intel", version="1.0.0")
    app.state.bot = bot

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
    async def submit_match(match: MatchSubmission):
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
    async def submit_network(data: NetworkSubmission):
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

    @app.get("/api-tool")
    async def api_tool_page():
        f = PUBLIC_DIR / "api-tool.html"
        if f.exists():
            return FileResponse(f)
        raise HTTPException(404)

    return app
