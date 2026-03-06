"""Seasonal Ladder commands — /ladder, /myrank, /seasons."""

import logging

import discord
from discord import app_commands
from discord.ext import commands

import config

log = logging.getLogger("marathon.cogs.ladder")

GUILD = discord.Object(id=config.DISCORD_GUILD_ID)

TIER_BADGES = {
    "Champion": ":crown:",
    "Platinum": ":diamond_shape_with_a_dot_inside:",
    "Gold": ":first_place:",
    "Silver": ":second_place:",
    "Bronze": ":third_place:",
}


class Ladder(commands.Cog):
    def __init__(self, bot: commands.Bot) -> None:
        self.bot = bot

    def _pool(self):
        return getattr(self.bot, "pool", None)

    # -- /ladder --
    @app_commands.command(name="ladder", description="Show the top 10 on the seasonal SR ladder")
    @app_commands.guilds(GUILD)
    async def ladder(self, interaction: discord.Interaction) -> None:
        await interaction.response.defer()
        pool = self._pool()
        if not pool:
            return await interaction.followup.send("Database offline.")

        season = await pool.fetchrow(
            "SELECT id, name FROM seasons WHERE active = true ORDER BY started_at DESC LIMIT 1"
        )
        if not season:
            return await interaction.followup.send("No active season found.")

        rows = await pool.fetch(
            "SELECT user_hash, display_name, sr, tier, matches, wins, losses "
            "FROM seasonal_ratings WHERE season_id = $1 ORDER BY sr DESC LIMIT 10",
            season["id"],
        )
        if not rows:
            return await interaction.followup.send("No players on the ladder yet. Submit some matches!")

        embed = discord.Embed(
            title=f"Seasonal Ladder — {season['name']}",
            colour=0xFFD700,
        )

        lines = []
        for rank, r in enumerate(rows, 1):
            badge = TIER_BADGES.get(r["tier"], ":small_blue_diamond:")
            name = r["display_name"] or r["user_hash"]
            wl = f"{r['wins']}W/{r['losses']}L"
            lines.append(
                f"**#{rank}** {badge} **{name}** — `{r['sr']} SR` | {r['tier']} | {wl} ({r['matches']} played)"
            )

        embed.description = "\n".join(lines)
        embed.set_footer(text="SR updates on every match submission")
        await interaction.followup.send(embed=embed)

    # -- /myrank --
    @app_commands.command(name="myrank", description="Show your personal SR, tier, rank, and season stats")
    @app_commands.guilds(GUILD)
    async def myrank(self, interaction: discord.Interaction) -> None:
        await interaction.response.defer(ephemeral=True)
        pool = self._pool()
        if not pool:
            return await interaction.followup.send("Database offline.")

        season = await pool.fetchrow(
            "SELECT id, name FROM seasons WHERE active = true ORDER BY started_at DESC LIMIT 1"
        )
        if not season:
            return await interaction.followup.send("No active season found.")

        user_hash = str(interaction.user.id)
        row = await pool.fetchrow(
            "SELECT sr, tier, matches, wins, losses, peak_sr "
            "FROM seasonal_ratings WHERE season_id = $1 AND user_hash = $2",
            season["id"], user_hash,
        )
        if not row:
            return await interaction.followup.send("You haven't played any matches this season yet.")

        # Calculate rank
        rank_row = await pool.fetchrow(
            "SELECT COUNT(*) + 1 AS rank FROM seasonal_ratings "
            "WHERE season_id = $1 AND sr > $2",
            season["id"], row["sr"],
        )
        rank = int(rank_row["rank"]) if rank_row else 0

        badge = TIER_BADGES.get(row["tier"], ":small_blue_diamond:")
        win_rate = (row["wins"] / row["matches"] * 100) if row["matches"] > 0 else 0.0

        embed = discord.Embed(
            title=f"Your Rank — {season['name']}",
            colour=0x00BFFF,
        )
        embed.add_field(name="Rank", value=f"**#{rank}**", inline=True)
        embed.add_field(name="SR", value=f"**{row['sr']}**", inline=True)
        embed.add_field(name="Tier", value=f"{badge} {row['tier']}", inline=True)
        embed.add_field(name="Peak SR", value=f"**{row['peak_sr']}**", inline=True)
        embed.add_field(name="Matches", value=f"**{row['matches']}**", inline=True)
        embed.add_field(name="Win Rate", value=f"**{win_rate:.1f}%**", inline=True)
        embed.add_field(
            name="Record",
            value=f"{row['wins']}W / {row['losses']}L / {row['matches'] - row['wins'] - row['losses']}D",
            inline=False,
        )
        embed.set_footer(text=f"Player: {interaction.user.display_name}")
        await interaction.followup.send(embed=embed)

    # -- /seasons --
    @app_commands.command(name="seasons", description="List past and current seasons")
    @app_commands.guilds(GUILD)
    async def seasons(self, interaction: discord.Interaction) -> None:
        await interaction.response.defer()
        pool = self._pool()
        if not pool:
            return await interaction.followup.send("Database offline.")

        rows = await pool.fetch(
            "SELECT id, name, started_at, ended_at, active FROM seasons ORDER BY started_at DESC"
        )
        if not rows:
            return await interaction.followup.send("No seasons found.")

        embed = discord.Embed(title="Seasons", colour=0x9B59B6)

        for r in rows:
            status = ":green_circle: Active" if r["active"] else ":red_circle: Ended"
            started = r["started_at"].strftime("%Y-%m-%d") if r["started_at"] else "N/A"
            ended = r["ended_at"].strftime("%Y-%m-%d") if r["ended_at"] else "Ongoing"
            embed.add_field(
                name=f"{status} — {r['name']}",
                value=f"Started: {started} | Ended: {ended}",
                inline=False,
            )

        await interaction.followup.send(embed=embed)


async def setup(bot: commands.Bot) -> None:
    await bot.add_cog(Ladder(bot))
