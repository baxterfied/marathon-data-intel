"""Core Marathon Intel slash commands — /stats, /meta, /runner, /leaderboard, /patch, /recap, /loadout, /network."""

import discord
from discord import app_commands
from discord.ext import commands

import config
from services.redis_cache import cache_get, cache_set, TTL_COMMUNITY_STATS, TTL_LEADERBOARD, TTL_META, TTL_RUNNER

GUILD = discord.Object(id=config.DISCORD_GUILD_ID)

# tier emoji mapping
TIER_EMOJI = {"S": "<:tier_s:0> **S**", "A": "<:tier_a:0> **A**", "B": "<:tier_b:0> **B**", "C": "<:tier_c:0> **C**", "D": "<:tier_d:0> **D**"}
TIER_FALLBACK = {"S": "**S**", "A": "**A**", "B": "**B**", "C": "**C**", "D": "**D**"}
MEDAL = {1: ":first_place:", 2: ":second_place:", 3: ":third_place:"}


def _bar(pct: float, length: int = 10) -> str:
    filled = round(pct / 100 * length)
    return "`" + "=" * filled + "-" * (length - filled) + f"` {pct:.1f}%"


class Intel(commands.Cog):
    def __init__(self, bot: commands.Bot) -> None:
        self.bot = bot

    def _pool(self):
        return getattr(self.bot, "pool", None)

    def _redis(self):
        return getattr(self.bot, "redis", None)

    # -- /stats --
    @app_commands.command(name="stats", description="Community win rates and match stats")
    @app_commands.guilds(GUILD)
    async def stats(self, interaction: discord.Interaction) -> None:
        await interaction.response.defer()
        pool = self._pool()
        if not pool:
            return await interaction.followup.send("Database offline.")

        cached = await cache_get(self._redis(), "marathon:stats:community")
        if cached:
            data = cached
        else:
            row = await pool.fetchrow("SELECT * FROM community_stats_view")
            if not row:
                return await interaction.followup.send("No match data yet.")
            data = dict(row)
            await cache_set(self._redis(), "marathon:stats:community", data, TTL_COMMUNITY_STATS)

        total = data.get("total_matches", 0)
        wins = data.get("total_wins", 0)
        wr = (wins / total * 100) if total > 0 else 0

        # runner win rates
        rows = await pool.fetch(
            "SELECT runner_name, COUNT(*) AS total, "
            "COUNT(*) FILTER (WHERE result='win') AS wins "
            "FROM matches GROUP BY runner_name ORDER BY COUNT(*) DESC LIMIT 8"
        )

        embed = discord.Embed(title="Community Stats", colour=0x00FF88)
        embed.add_field(name="Total Matches", value=f"**{total:,}**", inline=True)
        embed.add_field(name="Unique Players", value=f"**{data.get('unique_players', 0):,}**", inline=True)
        embed.add_field(name="Overall Win Rate", value=_bar(wr), inline=False)
        embed.add_field(name="Avg K/D/A", value=f"{data.get('avg_kills', 0)}/{data.get('avg_deaths', 0)}/—", inline=True)
        embed.add_field(name="Avg Damage", value=f"{data.get('avg_damage', 0):,.0f}", inline=True)

        if rows:
            lines = []
            for r in rows:
                rwr = (r["wins"] / r["total"] * 100) if r["total"] > 0 else 0
                lines.append(f"**{r['runner_name']}** {_bar(rwr)}")
            embed.add_field(name="Runner Win Rates", value="\n".join(lines), inline=False)

        embed.set_footer(text="Data from community submissions")
        await interaction.followup.send(embed=embed)

    # -- /meta --
    @app_commands.command(name="meta", description="Current runner tier list")
    @app_commands.guilds(GUILD)
    async def meta(self, interaction: discord.Interaction) -> None:
        await interaction.response.defer()
        pool = self._pool()
        if not pool:
            return await interaction.followup.send("Database offline.")

        rows = await pool.fetch("SELECT name, role, tier, win_rate, pick_rate, ban_rate FROM runners ORDER BY tier, name")
        if not rows:
            return await interaction.followup.send("No runner data.")

        tiers: dict[str, list[str]] = {}
        for r in rows:
            t = r["tier"]
            entry = f"**{r['name']}** ({r['role']}) — WR: {r['win_rate']:.1f}% | PR: {r['pick_rate']:.1f}%"
            tiers.setdefault(t, []).append(entry)

        embed = discord.Embed(title="Meta Tier List", colour=0xFFD700)
        for tier in ["S", "A", "B", "C", "D"]:
            if tier in tiers:
                label = TIER_FALLBACK.get(tier, tier)
                embed.add_field(name=f"{label} Tier", value="\n".join(tiers[tier]), inline=False)

        embed.set_footer(text="Based on community match data")
        await interaction.followup.send(embed=embed)

    # -- /runner --
    @app_commands.command(name="runner", description="Deep breakdown of a runner")
    @app_commands.guilds(GUILD)
    @app_commands.describe(name="Runner name (e.g. LOCUS, GLITCH)")
    async def runner(self, interaction: discord.Interaction, name: str) -> None:
        await interaction.response.defer()
        pool = self._pool()
        if not pool:
            return await interaction.followup.send("Database offline.")

        runner = await pool.fetchrow("SELECT * FROM runners WHERE UPPER(name) = UPPER($1)", name)
        if not runner:
            return await interaction.followup.send(f"Runner `{name}` not found.")

        # match stats for this runner
        stats = await pool.fetchrow(
            "SELECT COUNT(*) AS total, "
            "COUNT(*) FILTER (WHERE result='win') AS wins, "
            "ROUND(AVG(kills)::numeric, 1) AS avg_kills, "
            "ROUND(AVG(deaths)::numeric, 1) AS avg_deaths, "
            "ROUND(AVG(damage)::numeric, 0) AS avg_damage "
            "FROM matches WHERE UPPER(runner_name) = UPPER($1)", name
        )

        embed = discord.Embed(title=f"{runner['name']} — {runner['role'].title()}", colour=0x00BFFF)
        embed.add_field(name="Tier", value=f"**{runner['tier']}**", inline=True)
        embed.add_field(name="HP / Speed", value=f"{runner['base_hp']} / {runner['base_speed']}x", inline=True)

        abilities = runner["abilities"] if isinstance(runner["abilities"], list) else []
        if abilities:
            embed.add_field(name="Abilities", value=", ".join(abilities), inline=False)

        embed.add_field(name="Win Rate", value=_bar(runner["win_rate"]), inline=True)
        embed.add_field(name="Pick Rate", value=f"{runner['pick_rate']:.1f}%", inline=True)
        embed.add_field(name="Ban Rate", value=f"{runner['ban_rate']:.1f}%", inline=True)

        if stats and stats["total"] > 0:
            embed.add_field(
                name="Community Data",
                value=f"**{stats['total']}** matches | Avg K/D: {stats['avg_kills']}/{stats['avg_deaths']} | Avg Dmg: {stats['avg_damage']}",
                inline=False,
            )

        await interaction.followup.send(embed=embed)

    # -- /leaderboard --
    @app_commands.command(name="leaderboard", description="Top 10 community players")
    @app_commands.guilds(GUILD)
    async def leaderboard(self, interaction: discord.Interaction) -> None:
        await interaction.response.defer()
        pool = self._pool()
        if not pool:
            return await interaction.followup.send("Database offline.")

        rows = await pool.fetch(
            "SELECT display_name, wins, losses, win_rate, avg_kd, main_runner, score "
            "FROM leaderboard_cache ORDER BY rank LIMIT 10"
        )
        if not rows:
            return await interaction.followup.send("No leaderboard data yet.")

        lines = []
        for i, r in enumerate(rows, 1):
            medal = MEDAL.get(i, f"**#{i}**")
            lines.append(
                f"{medal} **{r['display_name']}** — "
                f"WR: {r['win_rate']:.1f}% | K/D: {r['avg_kd']:.2f} | "
                f"Main: {r['main_runner']} | Score: {r['score']:.0f}"
            )

        embed = discord.Embed(title="Leaderboard — Top 10", colour=0xFFD700, description="\n".join(lines))
        await interaction.followup.send(embed=embed)

    # -- /network --
    @app_commands.command(name="network", description="Server performance data")
    @app_commands.guilds(GUILD)
    async def network(self, interaction: discord.Interaction) -> None:
        await interaction.response.defer()
        pool = self._pool()
        if not pool:
            return await interaction.followup.send("Database offline.")

        rows = await pool.fetch(
            "SELECT region, "
            "ROUND(AVG(avg_ping_ms)::numeric, 1) AS ping, "
            "ROUND(AVG(jitter_ms)::numeric, 1) AS jitter, "
            "ROUND(AVG(packet_loss)::numeric, 2) AS loss, "
            "COUNT(*) AS samples "
            "FROM network_performance GROUP BY region ORDER BY ping"
        )
        if not rows:
            return await interaction.followup.send("No network data yet.")

        embed = discord.Embed(title="Server Performance by Region", colour=0x00BFFF)
        for r in rows:
            status = ":green_circle:" if r["ping"] < 50 else ":yellow_circle:" if r["ping"] < 100 else ":red_circle:"
            embed.add_field(
                name=f"{status} {r['region']}",
                value=f"Ping: `{r['ping']}ms` | Jitter: `{r['jitter']}ms` | Loss: `{r['loss']}%` ({r['samples']} samples)",
                inline=False,
            )

        await interaction.followup.send(embed=embed)

    # -- /patch --
    @app_commands.command(name="patch", description="Latest patch impact analysis")
    @app_commands.guilds(GUILD)
    async def patch(self, interaction: discord.Interaction) -> None:
        await interaction.response.defer()
        pool = self._pool()
        if not pool:
            return await interaction.followup.send("Database offline.")

        row = await pool.fetchrow("SELECT * FROM patch_notes ORDER BY released_at DESC LIMIT 1")
        if not row:
            return await interaction.followup.send("No patch data yet.")

        embed = discord.Embed(title=f"Patch {row['version']} — {row['title']}", colour=0xFF6600)
        if row["summary"]:
            embed.description = row["summary"][:2000]
        if row["ai_analysis"]:
            embed.add_field(name="AI Impact Analysis", value=row["ai_analysis"][:1024], inline=False)

        changes = row["changes"] if isinstance(row["changes"], list) else []
        if changes:
            change_text = "\n".join(f"- {c}" for c in changes[:15])
            embed.add_field(name="Key Changes", value=change_text[:1024], inline=False)

        embed.set_footer(text=f"Released {row['released_at'].strftime('%Y-%m-%d')}")
        await interaction.followup.send(embed=embed)

    # -- /recap --
    @app_commands.command(name="recap", description="Your personal last session recap")
    @app_commands.guilds(GUILD)
    @app_commands.describe(user_hash="Your gamertag")
    async def recap(self, interaction: discord.Interaction, user_hash: str) -> None:
        await interaction.response.defer(ephemeral=True)
        pool = self._pool()
        if not pool:
            return await interaction.followup.send("Database offline.")

        rows = await pool.fetch(
            "SELECT * FROM matches WHERE user_hash = $1 ORDER BY created_at DESC LIMIT 10",
            user_hash,
        )
        if not rows:
            return await interaction.followup.send("No matches found for that hash.")

        total = len(rows)
        wins = sum(1 for r in rows if r["result"] == "win")
        kills = sum(r["kills"] for r in rows)
        deaths = sum(r["deaths"] for r in rows)
        damage = sum(r["damage"] for r in rows)
        kd = kills / max(deaths, 1)

        embed = discord.Embed(title="Session Recap", colour=0x9B59B6)
        embed.add_field(name="Matches", value=f"**{total}** ({wins}W / {total - wins}L)", inline=True)
        embed.add_field(name="K/D", value=f"**{kd:.2f}** ({kills}K / {deaths}D)", inline=True)
        embed.add_field(name="Total Damage", value=f"**{damage:,}**", inline=True)

        runners_used = {}
        for r in rows:
            runners_used[r["runner_name"]] = runners_used.get(r["runner_name"], 0) + 1
        top_runner = max(runners_used, key=runners_used.get) if runners_used else "—"
        embed.add_field(name="Most Played", value=f"**{top_runner}**", inline=True)

        maps_used = {}
        for r in rows:
            maps_used[r["map_name"]] = maps_used.get(r["map_name"], 0) + 1
        top_map = max(maps_used, key=maps_used.get) if maps_used else "—"
        embed.add_field(name="Most Played Map", value=f"**{top_map}**", inline=True)

        embed.set_footer(text="Last 10 matches")
        await interaction.followup.send(embed=embed)

    # -- /loadout --
    @app_commands.command(name="loadout", description="Optimal loadout recommendation")
    @app_commands.guilds(GUILD)
    @app_commands.describe(runner="Runner name", map_name="Map name (optional)")
    async def loadout(self, interaction: discord.Interaction, runner: str, map_name: str = "any") -> None:
        await interaction.response.defer()
        pool = self._pool()
        if not pool:
            return await interaction.followup.send("Database offline.")

        row = await pool.fetchrow(
            "SELECT * FROM loadouts WHERE UPPER(runner_name) = UPPER($1) "
            "AND (UPPER(map_name) = UPPER($2) OR map_name = 'any') "
            "ORDER BY win_rate DESC LIMIT 1",
            runner, map_name,
        )

        if not row:
            return await interaction.followup.send(f"No loadout data for **{runner}** on **{map_name}**.")

        embed = discord.Embed(
            title=f"Loadout: {row['runner_name']} — {row['map_name']}",
            colour=0x2ECC71,
        )
        embed.add_field(name="Primary", value=row["weapon_primary"] or "—", inline=True)
        embed.add_field(name="Secondary", value=row["weapon_secondary"] or "—", inline=True)
        embed.add_field(name="Win Rate", value=f"{row['win_rate']:.1f}% ({row['sample_size']} matches)", inline=True)

        setup = row["ability_setup"] if isinstance(row["ability_setup"], dict) else {}
        if setup:
            lines = [f"**{k}**: {v}" for k, v in setup.items()]
            embed.add_field(name="Ability Setup", value="\n".join(lines), inline=False)

        embed.set_footer(text=f"Source: {row['source']} | Patch {row['patch']}")
        await interaction.followup.send(embed=embed)


async def setup(bot: commands.Bot) -> None:
    await bot.add_cog(Intel(bot))
