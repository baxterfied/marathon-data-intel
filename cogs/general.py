"""General utility commands — /ping, /status."""

import time
from typing import Optional

import asyncpg
import discord
import redis.asyncio as aioredis
from discord import app_commands
from discord.ext import commands

import config

GUILD = discord.Object(id=config.DISCORD_GUILD_ID)


class General(commands.Cog):
    def __init__(self, bot: commands.Bot) -> None:
        self.bot = bot

    @app_commands.command(name="ping", description="Check Marathon Intel latency")
    @app_commands.guilds(GUILD)
    async def ping(self, interaction: discord.Interaction) -> None:
        ws_ms = round(self.bot.latency * 1000)
        start = time.perf_counter()
        await interaction.response.send_message("Pinging...", ephemeral=True)
        api_ms = round((time.perf_counter() - start) * 1000)
        await interaction.edit_original_response(
            content=f"**Pong!** WS: `{ws_ms}ms` | API: `{api_ms}ms`",
        )

    @app_commands.command(name="status", description="Marathon Intel system health")
    @app_commands.guilds(GUILD)
    async def status(self, interaction: discord.Interaction) -> None:
        embed = discord.Embed(title="Marathon Intel Status", colour=0x00FF88)

        ws_ms = round(self.bot.latency * 1000)
        embed.add_field(name="WebSocket", value=f"`{ws_ms}ms`", inline=True)

        pool: Optional[asyncpg.Pool] = getattr(self.bot, "pool", None)
        if pool is not None:
            try:
                start = time.perf_counter()
                async with pool.acquire() as conn:
                    await conn.fetchval("SELECT 1")
                db_ms = round((time.perf_counter() - start) * 1000)
                db_status = f"OK `{db_ms}ms` ({pool.get_size()}/{pool.get_max_size()})"
            except Exception as exc:
                db_status = f"ERROR: {exc}"
        else:
            db_status = "Not connected"
        embed.add_field(name="PostgreSQL", value=db_status, inline=True)

        redis_client: Optional[aioredis.Redis] = getattr(self.bot, "redis", None)
        if redis_client is not None:
            try:
                start = time.perf_counter()
                await redis_client.ping()
                r_ms = round((time.perf_counter() - start) * 1000)
                r_status = f"OK `{r_ms}ms`"
            except Exception as exc:
                r_status = f"ERROR: {exc}"
        else:
            r_status = "Not connected"
        embed.add_field(name="Redis", value=r_status, inline=True)

        ai = getattr(self.bot, "ai_client", None)
        embed.add_field(name="Claude AI", value="Online" if ai else "Offline", inline=True)

        bungie = getattr(self.bot, "bungie", None)
        embed.add_field(name="Bungie API", value="Online" if bungie else "Offline", inline=True)

        all_ok = "ERROR" not in db_status and "Not connected" not in db_status
        embed.colour = 0x00FF88 if all_ok else 0xFF4444

        await interaction.response.send_message(embed=embed, ephemeral=True)

    @app_commands.command(name="netcapture", description="How to set up network capture")
    @app_commands.guilds(GUILD)
    async def netcapture(self, interaction: discord.Interaction) -> None:
        embed = discord.Embed(
            title="Network Capture Agent",
            description=(
                "Capture live network telemetry from your Marathon sessions "
                "and contribute server performance data to the community."
            ),
            colour=0x00BFFF,
        )
        embed.add_field(
            name="1. Install tshark",
            value=(
                "**Windows:** [wireshark.org/download](https://www.wireshark.org/download.html)\n"
                "**macOS:** `brew install wireshark`\n"
                "**Linux:** `sudo apt install tshark`"
            ),
            inline=False,
        )
        embed.add_field(
            name="2. Download the agent",
            value=f"Get `netcapture.py` from the [GitHub repo](https://github.com/baxterfied/marathon-data-intel/blob/main/utils/netcapture.py)",
            inline=False,
        )
        embed.add_field(
            name="3. Run it",
            value=(
                "```\npython netcapture.py --user-hash YOUR_NAME\n```\n"
                "Then start Marathon and play. Network data is captured automatically "
                "and submitted every 60 seconds."
            ),
            inline=False,
        )
        embed.add_field(
            name="Options",
            value=(
                "`--interface eth0` — pick a specific network interface\n"
                "`--patch 1.0.1` — tag data with current patch\n"
                "`--dry-run` — print stats without submitting"
            ),
            inline=False,
        )
        embed.set_footer(text="Requires admin/root for packet capture")
        await interaction.response.send_message(embed=embed)


async def setup(bot: commands.Bot) -> None:
    await bot.add_cog(General(bot))
