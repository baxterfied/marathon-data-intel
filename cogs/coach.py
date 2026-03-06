"""AI coaching command — /coach delivers personalised performance analysis."""

import logging
from datetime import datetime, timezone

import discord
from discord import app_commands
from discord.ext import commands

import config
from services.ai import generate_insight

log = logging.getLogger("marathon.cogs.coach")

GUILD = discord.Object(id=config.DISCORD_GUILD_ID)

COACHING_SYSTEM = """\
You are Marathon Intel's personal coach — a sharp, data-driven performance \
analyst for Bungie's Marathon (2025). You receive a player's recent match \
history and network session data and produce actionable coaching advice.

Your analysis must cover (when data is available):
1. **Win Rate Trend** — Are they improving or declining? Compare first half \
vs second half of recent matches.
2. **Best/Worst Runners** — Which runners have the highest and lowest win \
rates? Recommend leaning into strengths or dropping underperformers.
3. **Best/Worst Maps** — Where do they dominate and where do they struggle? \
Suggest map-specific adjustments.
4. **K/D Patterns** — Overall K/D, kills-per-match trends, death patterns. \
Flag if deaths are too high relative to kills.
5. **Time-of-Day Performance** — If enough data, note whether they perform \
better at certain hours (UTC).
6. **Network Impact** — If session data shows high ping or packet loss, \
note how it may be affecting results.
7. **Actionable Advice** — End with 3-5 specific, concrete recommendations \
the player can act on immediately.

Formatting rules:
- Use Discord markdown (bold, bullet points).
- Keep the total response under 1900 characters for embed limits.
- Be direct and constructive — like a coach reviewing film, not a chatbot.
- Cite actual numbers from the data provided.
- If data is thin, say so and give what advice you can.
"""


class Coach(commands.Cog):
    def __init__(self, bot: commands.Bot) -> None:
        self.bot = bot

    def _pool(self):
        return getattr(self.bot, "pool", None)

    def _redis(self):
        return getattr(self.bot, "redis", None)

    @app_commands.command(name="coach", description="Get personalised AI coaching based on your recent matches")
    @app_commands.guilds(GUILD)
    async def coach(self, interaction: discord.Interaction) -> None:
        await interaction.response.defer()

        pool = self._pool()
        if not pool:
            return await interaction.followup.send("Database offline.")

        user_hash = str(interaction.user.id)

        # Fetch last 30 matches
        matches = await pool.fetch(
            "SELECT runner_name, map_name, mode, result, kills, deaths, assists, "
            "damage, duration_s, created_at "
            "FROM matches WHERE user_hash = $1 "
            "ORDER BY created_at DESC LIMIT 30",
            user_hash,
        )

        if not matches:
            return await interaction.followup.send(
                "No match data found. Submit some matches with `/submit` first!"
            )

        # Fetch network sessions
        sessions = await pool.fetch(
            "SELECT server_ip, region, duration_s, peak_ping_ms, total_packets "
            "FROM match_sessions WHERE user_hash = $1 "
            "ORDER BY duration_s DESC LIMIT 30",
            user_hash,
        )

        # Build data summary for AI
        summary = _build_data_summary(matches, sessions)

        # Call AI
        ai_client = getattr(self.bot, "ai_client", None)
        coaching = await generate_insight(
            ai_client,
            COACHING_SYSTEM,
            summary,
            max_tokens=2000,
        )

        if not coaching:
            return await interaction.followup.send(
                "AI coaching is temporarily unavailable. Try again later."
            )

        # Build embed
        embed = discord.Embed(
            title="Your Coaching Report",
            description=coaching[:4096],
            colour=0x9B59B6,
            timestamp=datetime.now(timezone.utc),
        )
        embed.set_footer(text=f"Based on your last {len(matches)} matches")

        await interaction.followup.send(embed=embed)


def _build_data_summary(matches: list, sessions: list) -> str:
    """Build a structured text summary of player data for the AI prompt."""
    total = len(matches)
    wins = sum(1 for m in matches if m["result"] == "win")
    losses = sum(1 for m in matches if m["result"] == "loss")
    draws = total - wins - losses
    overall_wr = (wins / total * 100) if total > 0 else 0

    # First half vs second half trend
    mid = total // 2
    if mid > 0:
        recent_half = matches[:mid]
        older_half = matches[mid:]
        recent_wr = sum(1 for m in recent_half if m["result"] == "win") / len(recent_half) * 100
        older_wr = sum(1 for m in older_half if m["result"] == "win") / len(older_half) * 100
    else:
        recent_wr = overall_wr
        older_wr = overall_wr

    # K/D stats
    total_kills = sum(m["kills"] or 0 for m in matches)
    total_deaths = sum(m["deaths"] or 0 for m in matches)
    total_assists = sum(m["assists"] or 0 for m in matches)
    total_damage = sum(m["damage"] or 0 for m in matches)
    avg_kills = total_kills / total if total > 0 else 0
    avg_deaths = total_deaths / total if total > 0 else 0
    kd_ratio = total_kills / total_deaths if total_deaths > 0 else total_kills

    # Per-runner breakdown
    runner_stats = {}
    for m in matches:
        name = m["runner_name"] or "Unknown"
        if name not in runner_stats:
            runner_stats[name] = {"wins": 0, "losses": 0, "total": 0, "kills": 0, "deaths": 0}
        runner_stats[name]["total"] += 1
        if m["result"] == "win":
            runner_stats[name]["wins"] += 1
        elif m["result"] == "loss":
            runner_stats[name]["losses"] += 1
        runner_stats[name]["kills"] += m["kills"] or 0
        runner_stats[name]["deaths"] += m["deaths"] or 0

    runner_lines = []
    for name, s in sorted(runner_stats.items(), key=lambda x: x[1]["total"], reverse=True):
        wr = s["wins"] / s["total"] * 100 if s["total"] > 0 else 0
        kd = s["kills"] / s["deaths"] if s["deaths"] > 0 else s["kills"]
        runner_lines.append(f"  {name}: {s['total']} matches, {wr:.0f}% WR, {kd:.2f} K/D")

    # Per-map breakdown
    map_stats = {}
    for m in matches:
        name = m["map_name"] or "Unknown"
        if name not in map_stats:
            map_stats[name] = {"wins": 0, "losses": 0, "total": 0}
        map_stats[name]["total"] += 1
        if m["result"] == "win":
            map_stats[name]["wins"] += 1
        elif m["result"] == "loss":
            map_stats[name]["losses"] += 1

    map_lines = []
    for name, s in sorted(map_stats.items(), key=lambda x: x[1]["total"], reverse=True):
        wr = s["wins"] / s["total"] * 100 if s["total"] > 0 else 0
        map_lines.append(f"  {name}: {s['total']} matches, {wr:.0f}% WR")

    # Per-mode breakdown
    mode_stats = {}
    for m in matches:
        mode = m["mode"] or "Unknown"
        if mode not in mode_stats:
            mode_stats[mode] = {"wins": 0, "total": 0}
        mode_stats[mode]["total"] += 1
        if m["result"] == "win":
            mode_stats[mode]["wins"] += 1

    mode_lines = []
    for mode, s in sorted(mode_stats.items(), key=lambda x: x[1]["total"], reverse=True):
        wr = s["wins"] / s["total"] * 100 if s["total"] > 0 else 0
        mode_lines.append(f"  {mode}: {s['total']} matches, {wr:.0f}% WR")

    # Time-of-day analysis
    hour_stats = {}
    for m in matches:
        if m["created_at"]:
            hour = m["created_at"].hour
            if hour not in hour_stats:
                hour_stats[hour] = {"wins": 0, "total": 0}
            hour_stats[hour]["total"] += 1
            if m["result"] == "win":
                hour_stats[hour]["wins"] += 1

    hour_lines = []
    for hour in sorted(hour_stats.keys()):
        s = hour_stats[hour]
        if s["total"] >= 2:
            wr = s["wins"] / s["total"] * 100
            hour_lines.append(f"  {hour:02d}:00 UTC: {s['total']} matches, {wr:.0f}% WR")

    # Duration stats
    durations = [m["duration_s"] for m in matches if m["duration_s"]]
    avg_duration = sum(durations) / len(durations) if durations else 0

    # Build summary
    parts = [
        f"=== PLAYER PERFORMANCE SUMMARY ({total} recent matches) ===",
        f"Overall: {wins}W / {losses}L / {draws}D — {overall_wr:.1f}% win rate",
        f"Trend: Recent half {recent_wr:.1f}% WR vs older half {older_wr:.1f}% WR",
        f"K/D: {kd_ratio:.2f} ({total_kills} kills / {total_deaths} deaths, {total_assists} assists)",
        f"Avg per match: {avg_kills:.1f} kills, {avg_deaths:.1f} deaths, {total_damage / total if total else 0:.0f} damage",
        f"Avg match duration: {avg_duration:.0f}s",
        "",
        "--- Runner Breakdown ---",
        "\n".join(runner_lines),
        "",
        "--- Map Breakdown ---",
        "\n".join(map_lines),
    ]

    if mode_lines:
        parts.extend(["", "--- Mode Breakdown ---", "\n".join(mode_lines)])

    if hour_lines:
        parts.extend(["", "--- Time-of-Day (UTC, 2+ matches only) ---", "\n".join(hour_lines)])

    # Network context
    if sessions:
        pings = [s["peak_ping_ms"] for s in sessions if s["peak_ping_ms"]]
        regions = {}
        for s in sessions:
            r = s["region"] or "Unknown"
            regions[r] = regions.get(r, 0) + 1

        parts.append("")
        parts.append("--- Network Context ---")
        parts.append(f"Sessions: {len(sessions)}")
        if pings:
            parts.append(f"Peak ping: avg {sum(pings) / len(pings):.0f}ms, max {max(pings)}ms")
        region_str = ", ".join(f"{r}: {c}" for r, c in sorted(regions.items(), key=lambda x: x[1], reverse=True))
        parts.append(f"Regions: {region_str}")

    return "\n".join(parts)


async def setup(bot: commands.Bot) -> None:
    await bot.add_cog(Coach(bot))
