"""Weapon browsing commands — /weapons, /weapon."""

import discord
from discord import app_commands
from discord.ext import commands

import config

GUILD = discord.Object(id=config.DISCORD_GUILD_ID)

CATEGORY_DISPLAY = {
    "assault_rifle": "Assault Rifles",
    "machine_gun": "Machine Guns",
    "melee": "Melee",
    "pistol": "Pistols",
    "precision_rifle": "Precision Rifles",
    "railgun": "Railguns",
    "shotgun": "Shotguns",
    "sniper_rifle": "Sniper Rifles",
    "smg": "SMGs",
}

CATEGORY_CHOICES = [
    app_commands.Choice(name=v, value=k) for k, v in CATEGORY_DISPLAY.items()
]


class Weapons(commands.Cog):
    def __init__(self, bot: commands.Bot) -> None:
        self.bot = bot

    def _pool(self):
        return getattr(self.bot, "pool", None)

    @app_commands.command(name="weapons", description="Browse the Marathon arsenal")
    @app_commands.guilds(GUILD)
    @app_commands.describe(category="Filter by weapon category")
    @app_commands.choices(category=CATEGORY_CHOICES)
    async def weapons(self, interaction: discord.Interaction, category: str | None = None) -> None:
        await interaction.response.defer()
        pool = self._pool()
        if not pool:
            return await interaction.followup.send("Database offline.")

        if category:
            rows = await pool.fetch(
                "SELECT * FROM weapons WHERE category = $1 ORDER BY name", category
            )
            title = f"Weapons — {CATEGORY_DISPLAY.get(category, category)}"
        else:
            rows = await pool.fetch("SELECT * FROM weapons ORDER BY category, name")
            title = "Marathon Arsenal"

        if not rows:
            return await interaction.followup.send("No weapon data found.")

        # Group by category
        groups: dict[str, list[str]] = {}
        for r in rows:
            cat = CATEGORY_DISPLAY.get(r["category"], r["category"])
            wr = r["win_rate"]
            pr = r["pick_rate"]
            stats_parts = []
            if wr > 0:
                stats_parts.append(f"WR: {wr:.1f}%")
            if pr > 0:
                stats_parts.append(f"PR: {pr:.1f}%")
            stats_str = f" — {' | '.join(stats_parts)}" if stats_parts else ""
            groups.setdefault(cat, []).append(f"**{r['name']}**{stats_str}")

        embed = discord.Embed(title=title, colour=0xFF6600)
        for cat_name, weapons in groups.items():
            embed.add_field(
                name=f"{cat_name} ({len(weapons)})",
                value="\n".join(weapons),
                inline=True,
            )

        embed.set_footer(text=f"{len(rows)} weapons total")
        await interaction.followup.send(embed=embed)

    @app_commands.command(name="weapon", description="Detailed info on a specific weapon")
    @app_commands.guilds(GUILD)
    @app_commands.describe(name="Weapon name (e.g. Ares RG)")
    async def weapon(self, interaction: discord.Interaction, name: str) -> None:
        await interaction.response.defer()
        pool = self._pool()
        if not pool:
            return await interaction.followup.send("Database offline.")

        row = await pool.fetchrow(
            "SELECT * FROM weapons WHERE UPPER(name) = UPPER($1)", name
        )
        if not row:
            # Try partial match
            row = await pool.fetchrow(
                "SELECT * FROM weapons WHERE UPPER(name) LIKE UPPER($1) LIMIT 1",
                f"%{name}%",
            )
        if not row:
            return await interaction.followup.send(f"Weapon `{name}` not found.")

        cat = CATEGORY_DISPLAY.get(row["category"], row["category"])
        embed = discord.Embed(title=row["name"], colour=0xFF6600)
        embed.add_field(name="Category", value=cat, inline=True)

        if row["damage"] > 0:
            embed.add_field(name="Damage", value=f"{row['damage']:.0f}", inline=True)
        if row["fire_rate"] > 0:
            embed.add_field(name="Fire Rate", value=f"{row['fire_rate']:.0f} RPM", inline=True)
        if row["mag_size"] > 0:
            embed.add_field(name="Mag Size", value=str(row["mag_size"]), inline=True)
        if row["reload_s"] > 0:
            embed.add_field(name="Reload", value=f"{row['reload_s']:.1f}s", inline=True)
        if row["range_m"] > 0:
            embed.add_field(name="Range", value=f"{row['range_m']:.0f}m", inline=True)

        if row["win_rate"] > 0 or row["pick_rate"] > 0:
            embed.add_field(
                name="Community Stats",
                value=f"Win Rate: **{row['win_rate']:.1f}%** | Pick Rate: **{row['pick_rate']:.1f}%**",
                inline=False,
            )

        # Pull top loadouts featuring this weapon
        loadouts = await pool.fetch(
            "SELECT runner_name, map_name, win_rate, sample_size FROM loadouts "
            "WHERE UPPER(weapon_primary) = UPPER($1) OR UPPER(weapon_secondary) = UPPER($1) "
            "ORDER BY win_rate DESC LIMIT 3",
            row["name"],
        )
        if loadouts:
            lines = [
                f"**{l['runner_name']}** on {l['map_name']} — {l['win_rate']:.1f}% WR ({l['sample_size']} matches)"
                for l in loadouts
            ]
            embed.add_field(name="Top Loadouts", value="\n".join(lines), inline=False)

        embed.set_footer(text=f"Patch {row['patch']}")
        await interaction.followup.send(embed=embed)


async def setup(bot: commands.Bot) -> None:
    await bot.add_cog(Weapons(bot))
