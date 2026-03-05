"""Background tasks — hourly stats refresh, daily meta report, weekly leaderboard, server monitoring, blog watching."""

import json
import logging
from datetime import datetime, timezone

import discord
from discord.ext import commands, tasks

from services import ai as ai_service
from services.redis_cache import cache_set, cache_get, TTL_COMMUNITY_STATS, TTL_AI_INSIGHT, TTL_META_SNAPSHOT
from services.monitor import check_all_endpoints, fetch_blog_posts

log = logging.getLogger("marathon.cogs.tasks")


class BackgroundTasks(commands.Cog):
    def __init__(self, bot: commands.Bot) -> None:
        self.bot = bot

    async def cog_load(self) -> None:
        self.refresh_stats.start()
        self.recalc_runner_stats.start()
        self.daily_meta_report.start()
        self.weekly_leaderboard.start()
        self.server_health_check.start()
        self.blog_watcher.start()
        self.meta_shift_detector.start()

    async def cog_unload(self) -> None:
        self.refresh_stats.cancel()
        self.recalc_runner_stats.cancel()
        self.daily_meta_report.cancel()
        self.weekly_leaderboard.cancel()
        self.server_health_check.cancel()
        self.blog_watcher.cancel()
        self.meta_shift_detector.cancel()

    def _pool(self):
        return getattr(self.bot, "pool", None)

    def _redis(self):
        return getattr(self.bot, "redis", None)

    def _ai(self):
        return getattr(self.bot, "ai_client", None)

    @tasks.loop(hours=1)
    async def refresh_stats(self) -> None:
        pool = self._pool()
        redis = self._redis()
        if not pool:
            return

        try:
            row = await pool.fetchrow("SELECT * FROM community_stats_view")
            if row:
                await cache_set(redis, "marathon:stats:community", dict(row), TTL_COMMUNITY_STATS)
                log.info("Community stats cache refreshed")
        except Exception as exc:
            log.error("Stats refresh failed: %s", exc)

    @refresh_stats.before_loop
    async def before_refresh(self) -> None:
        await self.bot.wait_until_ready()

    @tasks.loop(hours=3)
    async def recalc_runner_stats(self) -> None:
        """Recalculate runner win_rate and pick_rate from actual match data."""
        pool = self._pool()
        if not pool:
            return

        try:
            total_matches = await pool.fetchval("SELECT COUNT(*) FROM matches")
            if not total_matches or total_matches == 0:
                return

            # Update each runner's stats from match data
            await pool.execute("""
                UPDATE runners r SET
                    win_rate = COALESCE(s.wr, 0),
                    pick_rate = COALESCE(s.pr, 0),
                    updated_at = now()
                FROM (
                    SELECT
                        UPPER(runner_name) AS rname,
                        ROUND(
                            COUNT(*) FILTER (WHERE result = 'win')::numeric
                            / GREATEST(COUNT(*), 1) * 100, 1
                        ) AS wr,
                        ROUND(COUNT(*)::numeric / $1 * 100, 1) AS pr
                    FROM matches
                    GROUP BY UPPER(runner_name)
                ) s
                WHERE UPPER(r.name) = s.rname
            """, total_matches)

            # Also recalc weapon stats if we have loadout data
            await pool.execute("""
                UPDATE weapons w SET
                    win_rate = COALESCE(s.wr, 0),
                    pick_rate = COALESCE(s.pr, 0),
                    updated_at = now()
                FROM (
                    SELECT
                        loadout->>'primary' AS wname,
                        ROUND(
                            COUNT(*) FILTER (WHERE result = 'win')::numeric
                            / GREATEST(COUNT(*), 1) * 100, 1
                        ) AS wr,
                        ROUND(COUNT(*)::numeric / $1 * 100, 1) AS pr
                    FROM matches
                    WHERE loadout->>'primary' IS NOT NULL
                    GROUP BY loadout->>'primary'
                ) s
                WHERE UPPER(w.name) = UPPER(s.wname)
            """, total_matches)

            log.info("Runner and weapon stats recalculated from %d matches", total_matches)
        except Exception as exc:
            log.error("Runner stats recalc failed: %s", exc)

    @recalc_runner_stats.before_loop
    async def before_recalc(self) -> None:
        await self.bot.wait_until_ready()

    @tasks.loop(hours=24)
    async def daily_meta_report(self) -> None:
        pool = self._pool()
        ai_client = self._ai()
        if not pool or not ai_client:
            return

        try:
            runners = await pool.fetch("SELECT name, role, tier, win_rate, pick_rate, ban_rate FROM runners ORDER BY tier, name")
            if not runners:
                return

            data = "\n".join(
                f"{r['name']} ({r['role']}): Tier {r['tier']}, WR {r['win_rate']:.1f}%, PR {r['pick_rate']:.1f}%, BR {r['ban_rate']:.1f}%"
                for r in runners
            )
            report = await ai_service.generate_meta_report(ai_client, data)
            if report:
                await pool.execute(
                    "INSERT INTO ai_insights (insight_type, subject, content) VALUES ($1, $2, $3)",
                    "meta_report", "weekly", report,
                )
                log.info("Daily meta report generated")
        except Exception as exc:
            log.error("Meta report generation failed: %s", exc)

    @daily_meta_report.before_loop
    async def before_meta(self) -> None:
        await self.bot.wait_until_ready()

    @tasks.loop(hours=168)  # weekly
    async def weekly_leaderboard(self) -> None:
        pool = self._pool()
        if not pool:
            return

        try:
            await pool.execute("""
                INSERT INTO leaderboard_cache (user_hash, display_name, total_matches, wins, losses, win_rate, avg_kd, main_runner, score, rank)
                SELECT
                    user_hash,
                    user_hash AS display_name,
                    COUNT(*) AS total_matches,
                    COUNT(*) FILTER (WHERE result = 'win') AS wins,
                    COUNT(*) FILTER (WHERE result = 'loss') AS losses,
                    ROUND((COUNT(*) FILTER (WHERE result = 'win'))::numeric / GREATEST(COUNT(*), 1) * 100, 1) AS win_rate,
                    ROUND(SUM(kills)::numeric / GREATEST(SUM(deaths), 1), 2) AS avg_kd,
                    MODE() WITHIN GROUP (ORDER BY runner_name) AS main_runner,
                    ROUND(
                        (COUNT(*) FILTER (WHERE result = 'win'))::numeric / GREATEST(COUNT(*), 1) * 50
                        + SUM(kills)::numeric / GREATEST(SUM(deaths), 1) * 30
                        + COUNT(*)::numeric * 0.5
                    , 1) AS score,
                    ROW_NUMBER() OVER (ORDER BY
                        (COUNT(*) FILTER (WHERE result = 'win'))::numeric / GREATEST(COUNT(*), 1) * 50
                        + SUM(kills)::numeric / GREATEST(SUM(deaths), 1) * 30
                        + COUNT(*)::numeric * 0.5
                    DESC)::int AS rank
                FROM matches
                GROUP BY user_hash
                HAVING COUNT(*) >= 5
                ON CONFLICT (user_hash) DO UPDATE SET
                    total_matches = EXCLUDED.total_matches,
                    wins = EXCLUDED.wins,
                    losses = EXCLUDED.losses,
                    win_rate = EXCLUDED.win_rate,
                    avg_kd = EXCLUDED.avg_kd,
                    main_runner = EXCLUDED.main_runner,
                    score = EXCLUDED.score,
                    rank = EXCLUDED.rank,
                    updated_at = now()
            """)
            log.info("Leaderboard cache refreshed")
        except Exception as exc:
            log.error("Leaderboard refresh failed: %s", exc)

    @weekly_leaderboard.before_loop
    async def before_leaderboard(self) -> None:
        await self.bot.wait_until_ready()

    # ── Server health check (every 5 min) ──

    @tasks.loop(minutes=5)
    async def server_health_check(self) -> None:
        pool = self._pool()
        if not pool:
            return

        try:
            results = await check_all_endpoints()
            for r in results:
                await pool.execute(
                    "INSERT INTO server_status_checks (endpoint, status_code, response_ms, is_up, error) "
                    "VALUES ($1, $2, $3, $4, $5)",
                    r["endpoint"], r.get("status_code", 0), r["response_ms"], r["is_up"], r.get("error", ""),
                )

            down = [r for r in results if not r["is_up"]]
            if down:
                log.warning("Server health: %d endpoint(s) down: %s",
                            len(down), ", ".join(r["endpoint"] for r in down))
            else:
                log.debug("Server health: all endpoints up")
        except Exception as exc:
            log.error("Server health check failed: %s", exc)

    @server_health_check.before_loop
    async def before_health(self) -> None:
        await self.bot.wait_until_ready()

    # ── Blog / RSS watcher (every 30 min) ──

    @tasks.loop(minutes=30)
    async def blog_watcher(self) -> None:
        pool = self._pool()
        ai_client = self._ai()
        if not pool:
            return

        try:
            posts = await fetch_blog_posts()
            new_count = 0
            for post in posts:
                if not post.get("url"):
                    continue
                # Check if we already have this post
                existing = await pool.fetchval(
                    "SELECT id FROM blog_posts WHERE url = $1", post["url"]
                )
                if existing:
                    continue

                # New post found
                await pool.execute(
                    "INSERT INTO blog_posts (url, title, summary, is_patch) "
                    "VALUES ($1, $2, $3, $4) ON CONFLICT (url) DO NOTHING",
                    post["url"], post["title"], post["summary"], post.get("is_patch", False),
                )
                new_count += 1

                # If it's Marathon-related and looks like a patch, auto-create patch entry
                if post.get("is_patch") and post.get("is_marathon"):
                    if ai_client:
                        analysis = await ai_service.generate_blog_summary(
                            ai_client, post["title"], post["summary"]
                        )
                        if analysis:
                            await pool.execute(
                                "INSERT INTO ai_insights (insight_type, subject, content) VALUES ($1, $2, $3)",
                                "blog_summary", post["title"][:100], analysis,
                            )

                log.info("New blog post: %s", post["title"])

            if new_count > 0:
                log.info("Blog watcher: %d new post(s) found", new_count)
        except Exception as exc:
            log.error("Blog watcher failed: %s", exc)

    @blog_watcher.before_loop
    async def before_blog(self) -> None:
        await self.bot.wait_until_ready()

    # ── Meta shift detector (every 6 hours) ──

    @tasks.loop(hours=6)
    async def meta_shift_detector(self) -> None:
        pool = self._pool()
        redis = self._redis()
        ai_client = self._ai()
        if not pool:
            return

        try:
            # Get current runner stats
            runners = await pool.fetch(
                "SELECT name, tier, win_rate, pick_rate, ban_rate FROM runners ORDER BY name"
            )
            if not runners:
                return

            current_data = {r["name"]: {
                "tier": r["tier"],
                "win_rate": float(r["win_rate"]),
                "pick_rate": float(r["pick_rate"]),
                "ban_rate": float(r["ban_rate"]),
            } for r in runners}

            current_json = json.dumps(current_data, default=str)

            # Compare with last snapshot
            previous = await cache_get(redis, "marathon:meta_snapshot:previous")

            if previous:
                # Check for significant changes
                has_shift = False
                for name, curr in current_data.items():
                    prev = previous.get(name, {})
                    if prev:
                        wr_delta = abs(curr["win_rate"] - prev.get("win_rate", 0))
                        pr_delta = abs(curr["pick_rate"] - prev.get("pick_rate", 0))
                        if wr_delta >= 3 or pr_delta >= 5:
                            has_shift = True
                            break

                if has_shift and ai_client:
                    prev_str = "\n".join(
                        f"{n}: WR {d.get('win_rate', 0):.1f}%, PR {d.get('pick_rate', 0):.1f}%"
                        for n, d in previous.items()
                    )
                    curr_str = "\n".join(
                        f"{n}: WR {d['win_rate']:.1f}%, PR {d['pick_rate']:.1f}%"
                        for n, d in current_data.items()
                    )
                    alert = await ai_service.generate_meta_shift_alert(ai_client, prev_str, curr_str)
                    if alert:
                        await pool.execute(
                            "INSERT INTO ai_insights (insight_type, subject, content) VALUES ($1, $2, $3)",
                            "meta_shift", "auto_detected", alert,
                        )
                        log.info("Meta shift detected and recorded")

            # Save current as the new snapshot
            await cache_set(redis, "marathon:meta_snapshot:previous", current_data, TTL_META_SNAPSHOT)
        except Exception as exc:
            log.error("Meta shift detector failed: %s", exc)

    @meta_shift_detector.before_loop
    async def before_meta_shift(self) -> None:
        await self.bot.wait_until_ready()


async def setup(bot: commands.Bot) -> None:
    await bot.add_cog(BackgroundTasks(bot))
