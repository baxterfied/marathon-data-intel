"""Extended tracking commands — /ttk, /serverstatus, /peakhours, /streaks, /metashift, /queuetimes."""

import json
import logging
from datetime import datetime, timezone

import discord
from discord import app_commands
from discord.ext import commands

import config
from services.redis_cache import cache_get, cache_set, invalidate_match_caches, TTL_SERVER_STATUS, TTL_PEAK_HOURS
from services.monitor import check_all_endpoints

log = logging.getLogger("marathon.cogs.tracker")

GUILD = discord.Object(id=config.DISCORD_GUILD_ID)

HOUR_LABELS = [
    "12am", "1am", "2am", "3am", "4am", "5am", "6am", "7am",
    "8am", "9am", "10am", "11am", "12pm", "1pm", "2pm", "3pm",
    "4pm", "5pm", "6pm", "7pm", "8pm", "9pm", "10pm", "11pm",
]


def _heatmap_bar(value: float, max_val: float, length: int = 12) -> str:
    if max_val <= 0:
        return "`" + "-" * length + "`"
    filled = round(value / max_val * length)
    return "`" + "#" * filled + "-" * (length - filled) + "`"


class Tracker(commands.Cog):
    def __init__(self, bot: commands.Bot) -> None:
        self.bot = bot

    def _pool(self):
        return getattr(self.bot, "pool", None)

    def _redis(self):
        return getattr(self.bot, "redis", None)

    # -- /ttk --
    @app_commands.command(name="ttk", description="Time-to-kill calculator for any weapon at any HP")
    @app_commands.guilds(GUILD)
    @app_commands.describe(
        weapon="Weapon name (e.g. Ares RG)",
        hp="Target HP (default: 100)",
    )
    async def ttk(self, interaction: discord.Interaction, weapon: str, hp: int = 100) -> None:
        await interaction.response.defer()
        pool = self._pool()
        if not pool:
            return await interaction.followup.send("Database offline.")

        row = await pool.fetchrow(
            "SELECT * FROM weapons WHERE UPPER(name) = UPPER($1)", weapon
        )
        if not row:
            row = await pool.fetchrow(
                "SELECT * FROM weapons WHERE UPPER(name) LIKE UPPER($1) LIMIT 1",
                f"%{weapon}%",
            )
        if not row:
            return await interaction.followup.send(f"Weapon `{weapon}` not found.")

        damage = row["damage"]
        fire_rate = row["fire_rate"]
        mag_size = row["mag_size"]
        reload_s = row["reload_s"]

        if damage <= 0 or fire_rate <= 0:
            return await interaction.followup.send(
                f"**{row['name']}** doesn't have damage/fire rate data yet."
            )

        # Calculate shots to kill
        shots_to_kill = -(-hp // int(damage))  # Ceiling division
        time_between_shots = 60.0 / fire_rate  # seconds

        # TTK = time from first shot hitting to kill shot
        # First shot is instant, subsequent shots take time_between_shots each
        ttk_ms = (shots_to_kill - 1) * time_between_shots * 1000

        # Check if we need to reload
        needs_reload = shots_to_kill > mag_size if mag_size > 0 else False
        ttk_with_reload = ttk_ms
        if needs_reload and reload_s > 0:
            mags_needed = -(-shots_to_kill // mag_size) if mag_size > 0 else 1
            reloads = mags_needed - 1
            ttk_with_reload = ttk_ms + (reloads * reload_s * 1000)

        # Calculate for all runner HP values
        runner_hps = [85, 90, 100, 110, 140]
        ttk_table = []
        for rhp in runner_hps:
            stk = -(-rhp // int(damage))
            t = (stk - 1) * time_between_shots * 1000
            ttk_table.append((rhp, stk, t))

        embed = discord.Embed(
            title=f"TTK: {row['name']}",
            colour=0xFF4444,
        )
        embed.add_field(name="Damage", value=f"{damage:.0f}", inline=True)
        embed.add_field(name="Fire Rate", value=f"{fire_rate:.0f} RPM", inline=True)
        embed.add_field(name="Mag Size", value=f"{mag_size}" if mag_size > 0 else "—", inline=True)

        embed.add_field(
            name=f"vs {hp} HP",
            value=f"**{ttk_ms:.0f}ms** ({shots_to_kill} shots)",
            inline=False,
        )

        if needs_reload:
            embed.add_field(
                name="With Reload",
                value=f"**{ttk_with_reload:.0f}ms** (needs reload — {mag_size} mag)",
                inline=False,
            )

        # All runners table
        lines = []
        for rhp, stk, t in ttk_table:
            lines.append(f"`{rhp:>3} HP` — **{t:.0f}ms** ({stk} shots)")
        embed.add_field(name="vs All Runners", value="\n".join(lines), inline=False)

        # DPS
        dps = damage * fire_rate / 60
        embed.add_field(name="DPS", value=f"**{dps:.0f}**", inline=True)
        embed.set_footer(text=f"Patch {row['patch']} | Assumes all shots hit")

        await interaction.followup.send(embed=embed)

    # -- /serverstatus --
    @app_commands.command(name="serverstatus", description="Check Marathon/Bungie server status")
    @app_commands.guilds(GUILD)
    async def serverstatus(self, interaction: discord.Interaction) -> None:
        await interaction.response.defer()
        pool = self._pool()
        redis = self._redis()

        # Check cache first
        cached = await cache_get(redis, "marathon:serverstatus")
        if cached:
            results = cached["results"]
        else:
            results = await check_all_endpoints()
            await cache_set(redis, "marathon:serverstatus", {"results": results}, TTL_SERVER_STATUS)

        embed = discord.Embed(title="Server Status", colour=0x00FF88)

        all_up = True
        for r in results:
            name = r.get("name", r["endpoint"])
            is_up = r["is_up"]
            ms = r["response_ms"]
            code = r["status_code"]

            if is_up:
                status = ":green_circle: Online"
                detail = f"`{ms:.0f}ms` (HTTP {code})"
            else:
                status = ":red_circle: Down"
                detail = r.get("error", "Unknown error")[:100] if r.get("error") else f"HTTP {code}"
                all_up = False

            embed.add_field(
                name=f"{status} — {name}",
                value=detail,
                inline=False,
            )

        # Store check in DB for history
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

        # Uptime stats from DB
        if pool:
            try:
                uptime = await pool.fetchrow(
                    "SELECT "
                    "COUNT(*) AS total, "
                    "COUNT(*) FILTER (WHERE is_up) AS up_count "
                    "FROM server_status_checks "
                    "WHERE checked_at > now() - interval '24 hours'"
                )
                if uptime and uptime["total"] > 0:
                    pct = uptime["up_count"] / uptime["total"] * 100
                    embed.add_field(
                        name="24h Uptime",
                        value=f"**{pct:.1f}%** ({uptime['up_count']}/{uptime['total']} checks)",
                        inline=False,
                    )
            except Exception:
                pass

        embed.set_footer(
            text=f"{'All systems operational' if all_up else 'Issues detected'} | "
            f"{datetime.now(timezone.utc).strftime('%H:%M UTC')}"
        )
        await interaction.followup.send(embed=embed)

    # -- /peakhours --
    @app_commands.command(name="peakhours", description="When are servers busiest/best?")
    @app_commands.guilds(GUILD)
    @app_commands.describe(region="Filter by region (optional)")
    async def peakhours(self, interaction: discord.Interaction, region: str = "") -> None:
        await interaction.response.defer()
        pool = self._pool()
        if not pool:
            return await interaction.followup.send("Database offline.")

        cache_key = f"marathon:peakhours:{region or 'all'}"
        cached = await cache_get(self._redis(), cache_key)
        if cached:
            hours_data = cached["hours"]
        else:
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

            if not rows:
                return await interaction.followup.send("No network data yet. Run the capture agent!")

            hours_data = [dict(r) for r in rows]
            await cache_set(self._redis(), cache_key, {"hours": hours_data}, TTL_PEAK_HOURS)

        # Build heatmap
        max_samples = max(h["samples"] for h in hours_data) if hours_data else 1
        max_ping = max(h["avg_ping"] for h in hours_data) if hours_data else 1

        # Activity heatmap
        activity_lines = []
        for h in hours_data:
            hour = int(h["hour"])
            label = HOUR_LABELS[hour]
            bar = _heatmap_bar(h["samples"], max_samples)
            activity_lines.append(f"`{label:>5}` {bar} {h['samples']}")

        # Ping heatmap
        ping_lines = []
        for h in hours_data:
            hour = int(h["hour"])
            label = HOUR_LABELS[hour]
            ping = float(h["avg_ping"])
            status = ":green_circle:" if ping < 50 else ":yellow_circle:" if ping < 100 else ":red_circle:"
            ping_lines.append(f"`{label:>5}` {status} `{ping}ms` jitter:`{h['avg_jitter']}ms`")

        # Find best/worst hours
        best = min(hours_data, key=lambda h: float(h["avg_ping"]))
        worst = max(hours_data, key=lambda h: float(h["avg_ping"]))
        busiest = max(hours_data, key=lambda h: h["samples"])

        title = f"Peak Hours — {region.upper()}" if region else "Peak Hours — All Regions"
        embed = discord.Embed(title=title, colour=0x00BFFF)

        if activity_lines:
            embed.add_field(
                name="Activity (UTC)",
                value="\n".join(activity_lines[:12]),
                inline=True,
            )
            if len(activity_lines) > 12:
                embed.add_field(
                    name="\u200b",
                    value="\n".join(activity_lines[12:]),
                    inline=True,
                )

        embed.add_field(
            name="Summary",
            value=(
                f":trophy: Best ping: **{HOUR_LABELS[int(best['hour'])]}** (`{best['avg_ping']}ms`)\n"
                f":warning: Worst ping: **{HOUR_LABELS[int(worst['hour'])]}** (`{worst['avg_ping']}ms`)\n"
                f":fire: Busiest: **{HOUR_LABELS[int(busiest['hour'])]}** ({busiest['samples']} samples)"
            ),
            inline=False,
        )

        await interaction.followup.send(embed=embed)

    # -- /streaks --
    @app_commands.command(name="streaks", description="Win/loss streak analysis")
    @app_commands.guilds(GUILD)
    @app_commands.describe(user_hash="Your gamertag")
    async def streaks(self, interaction: discord.Interaction, user_hash: str) -> None:
        await interaction.response.defer(ephemeral=True)
        pool = self._pool()
        if not pool:
            return await interaction.followup.send("Database offline.")

        rows = await pool.fetch(
            "SELECT result, runner_name, map_name, kills, deaths, created_at "
            "FROM matches WHERE user_hash = $1 ORDER BY created_at DESC LIMIT 50",
            user_hash,
        )
        if not rows:
            return await interaction.followup.send("No matches found for that hash.")

        # Calculate current streak
        current_streak = 0
        streak_type = rows[0]["result"]
        for r in rows:
            if r["result"] == streak_type:
                current_streak += 1
            else:
                break

        # Find longest win and loss streaks
        max_win_streak = 0
        max_loss_streak = 0
        current_run = 1
        for i in range(1, len(rows)):
            if rows[i]["result"] == rows[i - 1]["result"]:
                current_run += 1
            else:
                if rows[i - 1]["result"] == "win":
                    max_win_streak = max(max_win_streak, current_run)
                elif rows[i - 1]["result"] == "loss":
                    max_loss_streak = max(max_loss_streak, current_run)
                current_run = 1
        # Final run
        if rows[-1]["result"] == "win":
            max_win_streak = max(max_win_streak, current_run)
        elif rows[-1]["result"] == "loss":
            max_loss_streak = max(max_loss_streak, current_run)

        # Per-runner streaks
        runner_stats = {}
        for r in rows:
            name = r["runner_name"]
            if name not in runner_stats:
                runner_stats[name] = {"wins": 0, "losses": 0, "total": 0}
            runner_stats[name]["total"] += 1
            if r["result"] == "win":
                runner_stats[name]["wins"] += 1
            else:
                runner_stats[name]["losses"] += 1

        # Hot/cold runners
        hot_runners = []
        cold_runners = []
        for name, s in runner_stats.items():
            if s["total"] >= 3:
                wr = s["wins"] / s["total"] * 100
                if wr >= 60:
                    hot_runners.append((name, wr, s["total"]))
                elif wr <= 40:
                    cold_runners.append((name, wr, s["total"]))

        embed = discord.Embed(title="Streak Analysis", colour=0xFFD700)

        # Current streak
        streak_emoji = ":fire:" if streak_type == "win" else ":cold_face:" if streak_type == "loss" else ":neutral_face:"
        embed.add_field(
            name="Current Streak",
            value=f"{streak_emoji} **{current_streak} {streak_type}{'s' if current_streak > 1 else ''}**",
            inline=True,
        )
        embed.add_field(
            name="Best Win Streak",
            value=f":trophy: **{max_win_streak}**",
            inline=True,
        )
        embed.add_field(
            name="Worst Loss Streak",
            value=f":skull: **{max_loss_streak}**",
            inline=True,
        )

        if hot_runners:
            hot_runners.sort(key=lambda x: x[1], reverse=True)
            lines = [f":fire: **{n}** — {wr:.0f}% WR ({t} matches)" for n, wr, t in hot_runners[:5]]
            embed.add_field(name="Hot Runners", value="\n".join(lines), inline=False)

        if cold_runners:
            cold_runners.sort(key=lambda x: x[1])
            lines = [f":cold_face: **{n}** — {wr:.0f}% WR ({t} matches)" for n, wr, t in cold_runners[:5]]
            embed.add_field(name="Cold Runners", value="\n".join(lines), inline=False)

        # Last 10 match timeline
        timeline = ""
        for r in rows[:10]:
            icon = ":green_square:" if r["result"] == "win" else ":red_square:" if r["result"] == "loss" else ":yellow_square:"
            timeline += icon
        embed.add_field(name="Last 10", value=timeline, inline=False)

        embed.set_footer(text=f"Based on last {len(rows)} matches")
        await interaction.followup.send(embed=embed)

    # -- /metashift --
    @app_commands.command(name="metashift", description="Detect recent meta changes")
    @app_commands.guilds(GUILD)
    async def metashift(self, interaction: discord.Interaction) -> None:
        await interaction.response.defer()
        pool = self._pool()
        if not pool:
            return await interaction.followup.send("Database offline.")

        # Compare last 7 days vs previous 7 days
        current = await pool.fetch(
            "SELECT runner_name, "
            "COUNT(*) AS total, "
            "COUNT(*) FILTER (WHERE result = 'win') AS wins, "
            "ROUND(COUNT(*) FILTER (WHERE result = 'win')::numeric / GREATEST(COUNT(*), 1) * 100, 1) AS win_rate, "
            "ROUND(COUNT(*)::numeric / GREATEST((SELECT COUNT(*) FROM matches WHERE created_at > now() - interval '7 days'), 1) * 100, 1) AS pick_rate "
            "FROM matches WHERE created_at > now() - interval '7 days' "
            "GROUP BY runner_name ORDER BY win_rate DESC"
        )
        previous = await pool.fetch(
            "SELECT runner_name, "
            "COUNT(*) AS total, "
            "COUNT(*) FILTER (WHERE result = 'win') AS wins, "
            "ROUND(COUNT(*) FILTER (WHERE result = 'win')::numeric / GREATEST(COUNT(*), 1) * 100, 1) AS win_rate, "
            "ROUND(COUNT(*)::numeric / GREATEST((SELECT COUNT(*) FROM matches WHERE created_at BETWEEN now() - interval '14 days' AND now() - interval '7 days'), 1) * 100, 1) AS pick_rate "
            "FROM matches WHERE created_at BETWEEN now() - interval '14 days' AND now() - interval '7 days' "
            "GROUP BY runner_name ORDER BY win_rate DESC"
        )

        if not current:
            return await interaction.followup.send("Not enough recent match data for meta analysis.")

        prev_map = {r["runner_name"].upper(): dict(r) for r in previous}
        curr_map = {r["runner_name"].upper(): dict(r) for r in current}

        risers = []
        fallers = []
        for name, c in curr_map.items():
            p = prev_map.get(name)
            if p:
                wr_delta = float(c["win_rate"]) - float(p["win_rate"])
                pr_delta = float(c["pick_rate"]) - float(p["pick_rate"])
                if wr_delta >= 3 or pr_delta >= 5:
                    risers.append((name, float(c["win_rate"]), wr_delta, float(c["pick_rate"]), pr_delta))
                elif wr_delta <= -3 or pr_delta <= -5:
                    fallers.append((name, float(c["win_rate"]), wr_delta, float(c["pick_rate"]), pr_delta))

        embed = discord.Embed(title="Meta Shift — Last 7 Days", colour=0xFF6600)

        if risers:
            risers.sort(key=lambda x: x[2], reverse=True)
            lines = []
            for name, wr, wrd, pr, prd in risers:
                wr_arrow = f"+{wrd:.1f}%" if wrd > 0 else f"{wrd:.1f}%"
                pr_arrow = f"+{prd:.1f}%" if prd > 0 else f"{prd:.1f}%"
                lines.append(f":chart_with_upwards_trend: **{name}** — WR: {wr:.1f}% ({wr_arrow}) | PR: ({pr_arrow})")
            embed.add_field(name="Rising", value="\n".join(lines), inline=False)

        if fallers:
            fallers.sort(key=lambda x: x[2])
            lines = []
            for name, wr, wrd, pr, prd in fallers:
                wr_arrow = f"{wrd:.1f}%"
                pr_arrow = f"{prd:.1f}%"
                lines.append(f":chart_with_downwards_trend: **{name}** — WR: {wr:.1f}% ({wr_arrow}) | PR: ({pr_arrow})")
            embed.add_field(name="Falling", value="\n".join(lines), inline=False)

        if not risers and not fallers:
            embed.description = "No significant meta shifts detected in the last 7 days. The meta is stable."

        # Weapon shifts
        curr_weapons = await pool.fetch(
            "SELECT loadout->>'primary' AS weapon, "
            "COUNT(*) AS total, "
            "ROUND(COUNT(*) FILTER (WHERE result = 'win')::numeric / GREATEST(COUNT(*), 1) * 100, 1) AS win_rate "
            "FROM matches WHERE created_at > now() - interval '7 days' "
            "AND loadout->>'primary' IS NOT NULL "
            "GROUP BY weapon HAVING COUNT(*) >= 5 ORDER BY win_rate DESC LIMIT 5"
        )
        if curr_weapons:
            lines = [f"**{r['weapon']}** — {r['win_rate']}% WR ({r['total']} matches)" for r in curr_weapons]
            embed.add_field(name="Top Weapons This Week", value="\n".join(lines), inline=False)

        embed.set_footer(text="Comparing last 7 days vs previous 7 days")
        await interaction.followup.send(embed=embed)

    # -- /queuetimes --
    @app_commands.command(name="queuetimes", description="Estimated matchmaking queue times by region")
    @app_commands.guilds(GUILD)
    async def queuetimes(self, interaction: discord.Interaction) -> None:
        await interaction.response.defer()
        pool = self._pool()
        if not pool:
            return await interaction.followup.send("Database offline.")

        rows = await pool.fetch(
            "SELECT region, "
            "ROUND(AVG(queue_time_s)::numeric, 0) AS avg_queue, "
            "ROUND(MIN(queue_time_s)::numeric, 0) AS min_queue, "
            "ROUND(MAX(queue_time_s)::numeric, 0) AS max_queue, "
            "COUNT(*) AS samples "
            "FROM match_sessions WHERE queue_time_s > 0 "
            "GROUP BY region ORDER BY avg_queue"
        )
        if not rows:
            return await interaction.followup.send("No queue time data yet. Run the capture agent with match detection!")

        embed = discord.Embed(title="Queue Times by Region", colour=0x00BFFF)
        for r in rows:
            avg = int(r["avg_queue"])
            status = ":green_circle:" if avg < 30 else ":yellow_circle:" if avg < 60 else ":red_circle:"
            embed.add_field(
                name=f"{status} {r['region']}",
                value=f"Avg: `{avg}s` | Min: `{int(r['min_queue'])}s` | Max: `{int(r['max_queue'])}s` ({r['samples']} sessions)",
                inline=False,
            )

        await interaction.followup.send(embed=embed)

    # -- /uptime --
    @app_commands.command(name="uptime", description="Server uptime history")
    @app_commands.guilds(GUILD)
    @app_commands.describe(hours="Hours of history to show (default: 24)")
    async def uptime(self, interaction: discord.Interaction, hours: int = 24) -> None:
        await interaction.response.defer()
        pool = self._pool()
        if not pool:
            return await interaction.followup.send("Database offline.")

        hours = min(hours, 168)  # Max 1 week

        rows = await pool.fetch(
            "SELECT endpoint, "
            "COUNT(*) AS total_checks, "
            "COUNT(*) FILTER (WHERE is_up) AS up_checks, "
            "ROUND(AVG(response_ms)::numeric, 1) AS avg_response, "
            "ROUND(MAX(response_ms)::numeric, 1) AS max_response, "
            "MIN(checked_at) FILTER (WHERE NOT is_up) AS last_down "
            "FROM server_status_checks "
            "WHERE checked_at > now() - ($1 || ' hours')::interval "
            "GROUP BY endpoint ORDER BY endpoint",
            str(hours),
        )
        if not rows:
            return await interaction.followup.send("No uptime data yet. Use `/serverstatus` first!")

        embed = discord.Embed(title=f"Uptime — Last {hours}h", colour=0x00FF88)
        for r in rows:
            total = r["total_checks"]
            up = r["up_checks"]
            pct = up / total * 100 if total > 0 else 0
            status = ":green_circle:" if pct >= 99 else ":yellow_circle:" if pct >= 95 else ":red_circle:"

            endpoint_name = r["endpoint"].replace("https://", "").split("/")[0]
            last_down = r["last_down"].strftime("%Y-%m-%d %H:%M UTC") if r["last_down"] else "None recorded"

            embed.add_field(
                name=f"{status} {endpoint_name}",
                value=(
                    f"Uptime: **{pct:.1f}%** ({up}/{total})\n"
                    f"Avg: `{r['avg_response']}ms` | Peak: `{r['max_response']}ms`\n"
                    f"Last outage: {last_down}"
                ),
                inline=False,
            )

        await interaction.followup.send(embed=embed)


    # -- /blog --
    @app_commands.command(name="blog", description="Latest Bungie blog posts and news")
    @app_commands.guilds(GUILD)
    async def blog(self, interaction: discord.Interaction) -> None:
        await interaction.response.defer()
        pool = self._pool()
        if not pool:
            return await interaction.followup.send("Database offline.")

        rows = await pool.fetch(
            "SELECT title, url, summary, is_patch, created_at "
            "FROM blog_posts ORDER BY created_at DESC LIMIT 5"
        )
        if not rows:
            return await interaction.followup.send("No blog posts tracked yet. The watcher checks every 30 minutes.")

        embed = discord.Embed(title="Latest Bungie News", colour=0x00BFFF)
        for r in rows:
            icon = ":wrench:" if r["is_patch"] else ":newspaper:"
            title = r["title"][:100] if r["title"] else "Untitled"
            summary = r["summary"][:200] if r["summary"] else "No summary"
            embed.add_field(
                name=f"{icon} {title}",
                value=f"{summary}\n[Read more]({r['url']})",
                inline=False,
            )

        embed.set_footer(text="Auto-updated every 30 minutes")
        await interaction.followup.send(embed=embed)

    # -- /mapstats --
    @app_commands.command(name="mapstats", description="Win rates and performance by map")
    @app_commands.guilds(GUILD)
    async def mapstats(self, interaction: discord.Interaction) -> None:
        await interaction.response.defer()
        pool = self._pool()
        if not pool:
            return await interaction.followup.send("Database offline.")

        rows = await pool.fetch(
            "SELECT map_name, "
            "COUNT(*) AS total, "
            "COUNT(*) FILTER (WHERE result = 'win') AS wins, "
            "ROUND(COUNT(*) FILTER (WHERE result = 'win')::numeric / GREATEST(COUNT(*), 1) * 100, 1) AS win_rate, "
            "ROUND(AVG(kills)::numeric, 1) AS avg_kills, "
            "ROUND(AVG(deaths)::numeric, 1) AS avg_deaths, "
            "ROUND(AVG(damage)::numeric, 0) AS avg_damage, "
            "ROUND(AVG(duration_s)::numeric, 0) AS avg_duration "
            "FROM matches GROUP BY map_name HAVING COUNT(*) >= 3 ORDER BY win_rate DESC"
        )
        if not rows:
            return await interaction.followup.send("Not enough match data per map yet.")

        embed = discord.Embed(title="Map Performance", colour=0x2ECC71)
        for r in rows:
            wr = float(r["win_rate"])
            status = ":green_circle:" if wr >= 55 else ":yellow_circle:" if wr >= 45 else ":red_circle:"
            embed.add_field(
                name=f"{status} {r['map_name']}",
                value=(
                    f"WR: **{wr:.1f}%** ({r['total']} matches)\n"
                    f"Avg K/D: {r['avg_kills']}/{r['avg_deaths']} | Dmg: {int(r['avg_damage'])} | "
                    f"Duration: {int(r['avg_duration'])}s"
                ),
                inline=False,
            )

        await interaction.followup.send(embed=embed)

    # -- /serverblacklist --
    @app_commands.command(name="serverblacklist", description="Worst performing servers from community data")
    @app_commands.guilds(GUILD)
    async def serverblacklist(self, interaction: discord.Interaction) -> None:
        await interaction.response.defer()
        pool = self._pool()
        if not pool:
            return await interaction.followup.send("Database offline.")

        rows = await pool.fetch(
            "SELECT server_ip, region, "
            "ROUND(AVG(avg_ping_ms)::numeric, 1) AS avg_ping, "
            "ROUND(AVG(jitter_ms)::numeric, 1) AS avg_jitter, "
            "ROUND(AVG(packet_loss)::numeric, 2) AS avg_loss, "
            "COUNT(*) AS samples "
            "FROM network_performance WHERE server_ip != '' "
            "GROUP BY server_ip, region "
            "HAVING COUNT(*) >= 3 AND (AVG(packet_loss) > 1 OR AVG(avg_ping_ms) > 100 OR AVG(jitter_ms) > 20) "
            "ORDER BY AVG(packet_loss) DESC, AVG(avg_ping_ms) DESC LIMIT 10"
        )
        if not rows:
            return await interaction.followup.send("No problematic servers detected yet. Good news!")

        embed = discord.Embed(title="Problem Servers", colour=0xFF0000)
        for r in rows:
            ip_masked = r["server_ip"].rsplit(".", 1)[0] + ".x"
            issues = []
            if float(r["avg_loss"]) > 1:
                issues.append(f"Loss: {r['avg_loss']}%")
            if float(r["avg_ping"]) > 100:
                issues.append(f"Ping: {r['avg_ping']}ms")
            if float(r["avg_jitter"]) > 20:
                issues.append(f"Jitter: {r['avg_jitter']}ms")

            embed.add_field(
                name=f":warning: {ip_masked} ({r['region']})",
                value=f"{' | '.join(issues)} ({r['samples']} reports)",
                inline=False,
            )

        embed.set_footer(text="Based on community network captures")
        await interaction.followup.send(embed=embed)


    # -- /submit --
    @app_commands.command(name="submit", description="Submit a match result directly from Discord")
    @app_commands.guilds(GUILD)
    @app_commands.describe(
        runner="Runner name",
        result="Match result",
        map_name="Map name (default: unknown)",
        kills="Kill count (default: 0)",
        deaths="Death count (default: 0)",
        damage="Damage dealt (default: 0)",
        primary_weapon="Primary weapon used",
        secondary_weapon="Secondary weapon used",
    )
    @app_commands.choices(result=[
        app_commands.Choice(name="Win", value="win"),
        app_commands.Choice(name="Loss", value="loss"),
        app_commands.Choice(name="Draw", value="draw"),
    ])
    async def submit(
        self,
        interaction: discord.Interaction,
        runner: str,
        result: app_commands.Choice[str],
        map_name: str = "unknown",
        kills: int = 0,
        deaths: int = 0,
        damage: int = 0,
        primary_weapon: str = "",
        secondary_weapon: str = "",
    ) -> None:
        await interaction.response.defer(ephemeral=True)
        pool = self._pool()
        if not pool:
            return await interaction.followup.send("Database offline.")

        user_hash = str(interaction.user.id)
        result_value = result.value

        # Build loadout JSON from weapon fields
        loadout = {}
        if primary_weapon:
            loadout["primary"] = primary_weapon
        if secondary_weapon:
            loadout["secondary"] = secondary_weapon

        # Insert into matches table
        await pool.execute(
            "INSERT INTO matches (user_hash, runner_name, result, map_name, kills, deaths, damage, loadout, source) "
            "VALUES ($1, $2, $3, $4, $5, $6, $7, $8::jsonb, $9)",
            user_hash, runner.upper(), result_value, map_name, kills, deaths, damage,
            json.dumps(loadout) if loadout else "{}",
            "discord",
        )

        # Invalidate match caches
        await invalidate_match_caches(self._redis())

        # Confirmation embed
        embed = discord.Embed(
            title="Match Submitted",
            colour=0x2ECC71 if result_value == "win" else 0xE74C3C if result_value == "loss" else 0xF1C40F,
        )
        embed.add_field(name="Runner", value=runner, inline=True)
        embed.add_field(name="Result", value=result_value.capitalize(), inline=True)
        embed.add_field(name="Map", value=map_name, inline=True)
        embed.add_field(name="K/D", value=f"{kills}/{deaths}", inline=True)
        embed.add_field(name="Damage", value=str(damage), inline=True)
        if primary_weapon:
            embed.add_field(name="Primary", value=primary_weapon, inline=True)
        if secondary_weapon:
            embed.add_field(name="Secondary", value=secondary_weapon, inline=True)
        embed.set_footer(text=f"Recorded for {user_hash}")

        # Generate AI match commentary if available
        ai_client = getattr(self.bot, "ai_client", None)
        if ai_client:
            try:
                from services.ai import generate_match_commentary
                commentary = await generate_match_commentary(ai_client, {
                    "runner_name": runner,
                    "map_name": map_name,
                    "result": result_value,
                    "kills": kills,
                    "deaths": deaths,
                    "damage": damage,
                })
                if commentary:
                    embed.add_field(name="AI Commentary", value=commentary, inline=False)
            except Exception:
                pass  # AI failure should never block match submission

        await interaction.followup.send(embed=embed)


async def setup(bot: commands.Bot) -> None:
    await bot.add_cog(Tracker(bot))
