"""Player lookup via Bungie API — /lookup command."""

import discord
from discord import app_commands
from discord.ext import commands

import config
from services.bungie import (
    BungieClient,
    BungieAPIError,
    parse_bungie_name,
    extract_marathon_memberships,
    format_platform,
    MEMBERSHIP_TYPE_ALL,
)

GUILD = discord.Object(id=config.DISCORD_GUILD_ID)


class Lookup(commands.Cog):
    def __init__(self, bot: commands.Bot) -> None:
        self.bot = bot

    def _bungie(self) -> BungieClient | None:
        return getattr(self.bot, "bungie", None)

    # -- /lookup <bungie_name> --
    @app_commands.command(name="lookup", description="Look up a Marathon player by Bungie name")
    @app_commands.guilds(GUILD)
    @app_commands.describe(bungie_name="Bungie name (e.g. PlayerName#1234)")
    async def lookup(self, interaction: discord.Interaction, bungie_name: str) -> None:
        await interaction.response.defer()

        bungie = self._bungie()
        if not bungie:
            return await interaction.followup.send("Bungie API is not connected.")

        # Parse the name
        try:
            display_name, code = parse_bungie_name(bungie_name)
        except ValueError as exc:
            return await interaction.followup.send(f"Invalid format: {exc}")

        # Exact search
        try:
            results = await bungie.search_player_exact(display_name, code, MEMBERSHIP_TYPE_ALL)
        except BungieAPIError as exc:
            return await interaction.followup.send(f"Bungie API error: {exc.message}")
        except Exception:
            return await interaction.followup.send("Failed to reach Bungie API. Try again.")

        if not results:
            return await interaction.followup.send(f"No player found for **{bungie_name}**.")

        # Build the embed
        embed = discord.Embed(
            title=f"Player: {display_name}#{code}",
            colour=0x00BFFF,
        )

        # Show all linked memberships
        marathon_memberships = extract_marathon_memberships(results)
        all_platforms = []
        for m in results:
            platform = format_platform(m.get("membershipType", 0))
            mid = m.get("membershipId", "?")
            icon = ":video_game:" if m.get("membershipType") == 20 else ":link:"
            all_platforms.append(f"{icon} **{platform}** — `{mid}`")

        if all_platforms:
            embed.add_field(
                name="Linked Platforms",
                value="\n".join(all_platforms),
                inline=False,
            )

        # Try to pull Marathon stats if we have a Marathon membership
        if marathon_memberships:
            m = marathon_memberships[0]
            mid = m["membershipId"]
            mtype = m["membershipType"]

            stats = await bungie.get_marathon_account_stats(mtype, int(mid))
            if stats:
                embed.add_field(
                    name="Marathon Stats",
                    value="Stats loaded from Bungie API",
                    inline=False,
                )
                # Parse whatever stats structure Bungie returns
                # This will need refinement once the API is live
                for category, cat_data in stats.items():
                    if isinstance(cat_data, dict):
                        summary = []
                        all_time = cat_data.get("allTime", {})
                        for stat_name in ["kills", "deaths", "assists", "winRatio", "activitiesCleared"]:
                            if stat_name in all_time:
                                val = all_time[stat_name].get("basic", {}).get("displayValue", "?")
                                summary.append(f"**{stat_name}**: {val}")
                        if summary:
                            embed.add_field(
                                name=category.replace("allPvP", "PvP").replace("allPvE", "PvE"),
                                value="\n".join(summary),
                                inline=True,
                            )
            else:
                embed.add_field(
                    name="Marathon Stats",
                    value="No stats available yet — check back after playing some matches!",
                    inline=False,
                )
        else:
            embed.add_field(
                name="Marathon",
                value="No Marathon membership linked yet.",
                inline=False,
            )

        embed.set_footer(text="Data from Bungie API")
        await interaction.followup.send(embed=embed)

    # -- /search <name> --
    @app_commands.command(name="search", description="Search for players by name prefix")
    @app_commands.guilds(GUILD)
    @app_commands.describe(name="Player name to search for")
    async def search(self, interaction: discord.Interaction, name: str) -> None:
        await interaction.response.defer()

        bungie = self._bungie()
        if not bungie:
            return await interaction.followup.send("Bungie API is not connected.")

        try:
            data = await bungie.search_players(name)
        except BungieAPIError as exc:
            return await interaction.followup.send(f"Bungie API error: {exc.message}")
        except Exception:
            return await interaction.followup.send("Failed to reach Bungie API. Try again.")

        results = data.get("searchResults", [])
        if not results:
            return await interaction.followup.send(f"No players found matching **{name}**.")

        lines = []
        for r in results[:15]:
            bname = r.get("bungieGlobalDisplayName", "?")
            bcode = r.get("bungieGlobalDisplayNameCode")
            code_str = f"#{bcode}" if bcode else ""
            memberships = r.get("destinyMemberships", [])
            platforms = [format_platform(m.get("membershipType", 0)) for m in memberships]
            plat_str = ", ".join(platforms) if platforms else "No platforms"
            lines.append(f"**{bname}{code_str}** — {plat_str}")

        embed = discord.Embed(
            title=f"Search: {name}",
            description="\n".join(lines),
            colour=0x00BFFF,
        )

        has_more = data.get("hasMore", False)
        if has_more:
            embed.set_footer(text="Showing first 15 results — refine your search for better matches")
        else:
            embed.set_footer(text=f"{len(results)} result(s) found")

        await interaction.followup.send(embed=embed)


async def setup(bot: commands.Bot) -> None:
    await bot.add_cog(Lookup(bot))
