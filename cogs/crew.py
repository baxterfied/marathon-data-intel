"""Crew Finder — /crewfind, /crewpost, /crewremove LFG system."""

import logging

import discord
from discord import app_commands
from discord.ext import commands

import config

log = logging.getLogger("marathon.cogs.crew")

GUILD = discord.Object(id=config.DISCORD_GUILD_ID)

REGION_CHOICES = [
    app_commands.Choice(name="US East", value="us-east"),
    app_commands.Choice(name="US West", value="us-west"),
    app_commands.Choice(name="EU West", value="eu-west"),
    app_commands.Choice(name="EU Central", value="eu-central"),
    app_commands.Choice(name="Asia", value="asia"),
    app_commands.Choice(name="Any", value="any"),
]

PLAYSTYLE_CHOICES = [
    app_commands.Choice(name="Aggressive", value="aggressive"),
    app_commands.Choice(name="Stealth", value="stealth"),
    app_commands.Choice(name="Support", value="support"),
    app_commands.Choice(name="Balanced", value="balanced"),
    app_commands.Choice(name="Any", value="any"),
]

RUNNER_CHOICES = [
    app_commands.Choice(name="Assassin", value="ASSASSIN"),
    app_commands.Choice(name="Destroyer", value="DESTROYER"),
    app_commands.Choice(name="Recon", value="RECON"),
    app_commands.Choice(name="Rook", value="ROOK"),
    app_commands.Choice(name="Thief", value="THIEF"),
    app_commands.Choice(name="Triage", value="TRIAGE"),
    app_commands.Choice(name="Vandal", value="VANDAL"),
    app_commands.Choice(name="Any", value="any"),
]


class Crew(commands.Cog):
    def __init__(self, bot: commands.Bot) -> None:
        self.bot = bot

    def _pool(self):
        return getattr(self.bot, "pool", None)

    # -- /crewfind --
    @app_commands.command(name="crewfind", description="Search for crew/teammates looking to group up")
    @app_commands.guilds(GUILD)
    @app_commands.describe(
        region="Filter by region",
        playstyle="Filter by playstyle",
        runner="Filter by main runner",
    )
    @app_commands.choices(region=REGION_CHOICES, playstyle=PLAYSTYLE_CHOICES, runner=RUNNER_CHOICES)
    async def crewfind(
        self,
        interaction: discord.Interaction,
        region: app_commands.Choice[str] | None = None,
        playstyle: app_commands.Choice[str] | None = None,
        runner: app_commands.Choice[str] | None = None,
    ) -> None:
        await interaction.response.defer()
        pool = self._pool()
        if not pool:
            return await interaction.followup.send("Database offline.")

        # Build dynamic query
        conditions = ["active = true"]
        params: list = []
        idx = 1

        if region and region.value != "any":
            conditions.append(f"region = ${idx}")
            params.append(region.value)
            idx += 1

        if playstyle and playstyle.value != "any":
            conditions.append(f"playstyle = ${idx}")
            params.append(playstyle.value)
            idx += 1

        if runner and runner.value != "any":
            conditions.append(f"main_runner = ${idx}")
            params.append(runner.value)
            idx += 1

        where = " AND ".join(conditions)
        query = (
            f"SELECT discord_user_id, display_name, region, playstyle, "
            f"main_runner, play_times, message, updated_at "
            f"FROM crew_finder WHERE {where} "
            f"ORDER BY updated_at DESC LIMIT 10"
        )

        try:
            rows = await pool.fetch(query, *params)
        except Exception as exc:
            log.error("crewfind query failed: %s", exc)
            return await interaction.followup.send("Something went wrong searching crew posts.")

        if not rows:
            return await interaction.followup.send("No crew posts match your filters. Try broadening your search or post your own with `/crewpost`!")

        # Build filters description
        filter_parts = []
        if region and region.value != "any":
            filter_parts.append(f"Region: {region.name}")
        if playstyle and playstyle.value != "any":
            filter_parts.append(f"Playstyle: {playstyle.name}")
        if runner and runner.value != "any":
            filter_parts.append(f"Runner: {runner.name}")
        filter_text = " | ".join(filter_parts) if filter_parts else "No filters"

        embed = discord.Embed(
            title="Crew Finder Results",
            description=f"Showing {len(rows)} listing{'s' if len(rows) != 1 else ''} ({filter_text})",
            colour=0x5865F2,
        )

        for r in rows:
            user_mention = f"<@{r['discord_user_id']}>"
            region_val = r["region"].upper() if r["region"] != "any" else "Any"
            playstyle_val = r["playstyle"].capitalize() if r["playstyle"] != "any" else "Any"
            runner_val = r["main_runner"] if r["main_runner"] != "any" else "Any"
            play_times_val = r["play_times"] if r["play_times"] else "Not specified"
            msg = r["message"] if r["message"] else "No message"

            value_lines = (
                f"Region: **{region_val}** | Playstyle: **{playstyle_val}** | Runner: **{runner_val}**\n"
                f"Play Times: {play_times_val}\n"
                f"{msg}\n"
                f"{user_mention}"
            )

            embed.add_field(
                name=r["display_name"],
                value=value_lines,
                inline=False,
            )

        embed.set_footer(text="Use /crewpost to add your own listing")
        await interaction.followup.send(embed=embed)

    # -- /crewpost --
    @app_commands.command(name="crewpost", description="Post or update your crew finder listing")
    @app_commands.guilds(GUILD)
    @app_commands.describe(
        region="Your preferred region",
        playstyle="Your playstyle",
        main_runner="Your main runner",
        play_times="When you usually play (e.g. 'weekday evenings EST')",
        message="A short message for potential crewmates",
    )
    @app_commands.choices(region=REGION_CHOICES, playstyle=PLAYSTYLE_CHOICES, main_runner=RUNNER_CHOICES)
    async def crewpost(
        self,
        interaction: discord.Interaction,
        region: app_commands.Choice[str] = None,
        playstyle: app_commands.Choice[str] = None,
        main_runner: app_commands.Choice[str] = None,
        play_times: str = "any",
        message: str = "",
    ) -> None:
        await interaction.response.defer(ephemeral=True)
        pool = self._pool()
        if not pool:
            return await interaction.followup.send("Database offline.")

        user_id = str(interaction.user.id)
        display_name = interaction.user.display_name
        region_val = region.value if region else "any"
        playstyle_val = playstyle.value if playstyle else "any"
        runner_val = main_runner.value if main_runner else "any"

        # Truncate user input
        play_times = play_times[:200]
        message = message[:500]

        try:
            await pool.execute(
                """
                INSERT INTO crew_finder (discord_user_id, display_name, region, playstyle, main_runner, play_times, message, active, updated_at)
                VALUES ($1, $2, $3, $4, $5, $6, $7, true, now())
                ON CONFLICT (discord_user_id)
                DO UPDATE SET
                    display_name = EXCLUDED.display_name,
                    region = EXCLUDED.region,
                    playstyle = EXCLUDED.playstyle,
                    main_runner = EXCLUDED.main_runner,
                    play_times = EXCLUDED.play_times,
                    message = EXCLUDED.message,
                    active = true,
                    updated_at = now()
                """,
                user_id, display_name, region_val, playstyle_val, runner_val, play_times, message,
            )
        except Exception as exc:
            log.error("crewpost upsert failed: %s", exc)
            return await interaction.followup.send("Failed to save your listing. Please try again.")

        region_display = region_val.upper() if region_val != "any" else "Any"
        playstyle_display = playstyle_val.capitalize() if playstyle_val != "any" else "Any"
        runner_display = runner_val if runner_val != "any" else "Any"

        embed = discord.Embed(
            title="Crew Listing Posted",
            description="Your listing is now live! Others can find you with `/crewfind`.",
            colour=0x2ECC71,
        )
        embed.add_field(name="Region", value=region_display, inline=True)
        embed.add_field(name="Playstyle", value=playstyle_display, inline=True)
        embed.add_field(name="Main Runner", value=runner_display, inline=True)
        embed.add_field(name="Play Times", value=play_times if play_times else "Any", inline=True)
        if message:
            embed.add_field(name="Message", value=message, inline=False)
        embed.set_footer(text="Use /crewremove to take down your listing")

        await interaction.followup.send(embed=embed)

    # -- /crewremove --
    @app_commands.command(name="crewremove", description="Remove your crew finder listing")
    @app_commands.guilds(GUILD)
    async def crewremove(self, interaction: discord.Interaction) -> None:
        await interaction.response.defer(ephemeral=True)
        pool = self._pool()
        if not pool:
            return await interaction.followup.send("Database offline.")

        user_id = str(interaction.user.id)

        try:
            result = await pool.execute(
                "UPDATE crew_finder SET active = false, updated_at = now() WHERE discord_user_id = $1 AND active = true",
                user_id,
            )
        except Exception as exc:
            log.error("crewremove failed: %s", exc)
            return await interaction.followup.send("Something went wrong. Please try again.")

        if result == "UPDATE 0":
            return await interaction.followup.send("You don't have an active crew listing.")

        embed = discord.Embed(
            title="Listing Removed",
            description="Your crew finder listing has been taken down. Use `/crewpost` to create a new one anytime.",
            colour=0xE74C3C,
        )
        await interaction.followup.send(embed=embed)


async def setup(bot: commands.Bot) -> None:
    await bot.add_cog(Crew(bot))
