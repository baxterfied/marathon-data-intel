"""Auto-sync — pulls match data from the Bungie API for tracked players."""

import asyncio
import logging
from datetime import datetime, timezone

import discord
from discord import app_commands
from discord.ext import commands, tasks

import config
from services.bungie import (
    BungieClient,
    BungieAPIError,
    parse_bungie_name,
    extract_marathon_memberships,
    MEMBERSHIP_TYPE_ALL,
    MEMBERSHIP_TYPE_MARATHON,
)
from services.redis_cache import invalidate_match_caches

log = logging.getLogger("marathon.cogs.sync")

GUILD = discord.Object(id=config.DISCORD_GUILD_ID)

# How many activities to fetch per player per sync cycle
ACTIVITIES_PER_SYNC = 25
# Delay between API calls to stay under rate limits
API_CALL_DELAY = 0.15  # ~6-7 req/s, well under 25/s limit


class Sync(commands.Cog):
    def __init__(self, bot: commands.Bot) -> None:
        self.bot = bot
        self._syncing = False

    def _pool(self):
        return getattr(self.bot, "pool", None)

    def _bungie(self) -> BungieClient | None:
        return getattr(self.bot, "bungie", None)

    def _redis(self):
        return getattr(self.bot, "redis", None)

    async def cog_load(self) -> None:
        self.auto_sync_loop.start()

    async def cog_unload(self) -> None:
        self.auto_sync_loop.cancel()

    # ── Sync engine ──

    async def _sync_player(self, bungie: BungieClient, pool, player: dict) -> int:
        """Sync one tracked player's recent matches. Returns count of new matches inserted."""
        membership_id = int(player["membership_id"])
        membership_type = player["membership_type"]
        last_activity_id = player.get("last_activity_id")

        # Step 1: Get profile to find character IDs
        profile = await bungie.get_marathon_profile(membership_type, membership_id)
        if not profile:
            return 0

        # Extract character IDs from profile response
        characters_data = profile.get("characters", {}).get("data", {})
        if not characters_data:
            # Try alternate response structure
            characters_data = profile.get("profile", {}).get("data", {}).get("characterIds", [])
            if isinstance(characters_data, list):
                characters_data = {cid: {} for cid in characters_data}

        if not characters_data:
            log.debug("No characters found for %s", player["display_name"])
            return 0

        new_matches = 0
        newest_activity_id = last_activity_id

        for character_id in characters_data:
            await asyncio.sleep(API_CALL_DELAY)

            history = await bungie.get_activity_history(
                membership_type, membership_id, int(character_id),
                count=ACTIVITIES_PER_SYNC,
            )
            if not history:
                continue

            activities = history.get("activities", [])
            if not activities:
                continue

            for activity in activities:
                instance_id = activity.get("activityDetails", {}).get("instanceId")
                if not instance_id:
                    continue

                # Skip if we've already synced past this point
                if last_activity_id and instance_id <= last_activity_id:
                    continue

                # Track the newest activity we've seen
                if not newest_activity_id or instance_id > newest_activity_id:
                    newest_activity_id = instance_id

                # Fetch the full PGCR for detailed match data
                await asyncio.sleep(API_CALL_DELAY)
                pgcr = await bungie.get_pgcr(int(instance_id))
                if not pgcr:
                    continue

                inserted = await self._process_pgcr(pool, pgcr, instance_id)
                new_matches += inserted

        # Update the player's sync cursor
        if newest_activity_id and newest_activity_id != last_activity_id:
            await pool.execute(
                "UPDATE tracked_players SET last_synced_at = now(), last_activity_id = $1 "
                "WHERE membership_id = $2 AND membership_type = $3",
                newest_activity_id, player["membership_id"], membership_type,
            )

        return new_matches

    async def _process_pgcr(self, pool, pgcr: dict, instance_id: str) -> int:
        """Parse a Post-Game Carnage Report and insert match rows. Returns insert count."""
        activity_details = pgcr.get("activityDetails", {})
        entries = pgcr.get("entries", [])

        if not entries:
            return 0

        # Extract match metadata
        map_hash = activity_details.get("referenceId", 0)
        mode_type = activity_details.get("mode", 0)
        mode_name = _mode_name(mode_type)
        map_name = str(map_hash)  # Will be resolved to name if manifest is available

        period = pgcr.get("period", "")

        inserted = 0
        for entry in entries:
            player_info = entry.get("player", {})
            membership_id = player_info.get("destinyUserInfo", {}).get("membershipId", "")
            display_name = player_info.get("destinyUserInfo", {}).get("displayName", "unknown")
            character_class = player_info.get("characterClass", "unknown")

            values = entry.get("values", {})
            kills = _stat_value(values, "kills")
            deaths = _stat_value(values, "deaths")
            assists = _stat_value(values, "assists")
            standing = _stat_value(values, "standing")  # 0 = victory typically
            completed = _stat_value(values, "completed")
            damage = _stat_value(values, "totalDamageDealt") or _stat_value(values, "score")
            duration = _stat_value(values, "activityDurationSeconds") or _stat_value(values, "timePlayedSeconds")

            # Determine result
            if completed == 0:
                result = "draw"  # DNF
            elif standing == 0:
                result = "win"
            else:
                result = "loss"

            # Map character class to runner name
            runner_name = character_class.upper() if character_class else "UNKNOWN"

            try:
                await pool.execute(
                    "INSERT INTO matches "
                    "(user_hash, runner_name, map_name, mode, result, kills, deaths, assists, damage, "
                    "duration_s, patch, activity_id, source) "
                    "VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13) "
                    "ON CONFLICT (activity_id) WHERE activity_id IS NOT NULL DO NOTHING",
                    str(membership_id), runner_name, map_name, mode_name, result,
                    kills, deaths, assists, damage, duration,
                    "1.0", instance_id, "bungie_api",
                )
                inserted += 1
            except Exception as exc:
                log.debug("Failed to insert match from PGCR %s: %s", instance_id, exc)

        return inserted

    async def _run_sync(self) -> tuple[int, int]:
        """Run a full sync cycle for all tracked players. Returns (players_synced, new_matches)."""
        pool = self._pool()
        bungie = self._bungie()
        if not pool or not bungie:
            return 0, 0

        players = await pool.fetch(
            "SELECT * FROM tracked_players WHERE auto_sync = true ORDER BY last_synced_at ASC NULLS FIRST"
        )
        if not players:
            return 0, 0

        total_matches = 0
        players_synced = 0

        for player in players:
            try:
                count = await self._sync_player(bungie, pool, dict(player))
                total_matches += count
                players_synced += 1
                if count > 0:
                    log.info("Synced %d new matches for %s", count, player["display_name"])
            except BungieAPIError as exc:
                log.warning("Bungie API error syncing %s: %s", player["display_name"], exc.message)
            except Exception as exc:
                log.error("Sync failed for %s: %s", player["display_name"], exc)

            # Small delay between players
            await asyncio.sleep(0.5)

        # Invalidate caches if we got new data
        if total_matches > 0:
            await invalidate_match_caches(self._redis())

        return players_synced, total_matches

    # ── Background loop ──

    @tasks.loop(minutes=10)
    async def auto_sync_loop(self) -> None:
        if self._syncing:
            return
        self._syncing = True
        try:
            players, matches = await self._run_sync()
            if matches > 0:
                log.info("Auto-sync complete: %d players, %d new matches", players, matches)
        except Exception as exc:
            log.error("Auto-sync loop error: %s", exc)
        finally:
            self._syncing = False

    @auto_sync_loop.before_loop
    async def before_sync(self) -> None:
        await self.bot.wait_until_ready()
        # Wait a bit after startup so other services are ready
        await asyncio.sleep(30)

    # ── Slash commands ──

    @app_commands.command(name="track", description="Start tracking a player for auto-sync")
    @app_commands.guilds(GUILD)
    @app_commands.describe(bungie_name="Bungie name (e.g. PlayerName#1234)")
    async def track(self, interaction: discord.Interaction, bungie_name: str) -> None:
        await interaction.response.defer()

        pool = self._pool()
        bungie = self._bungie()
        if not pool:
            return await interaction.followup.send("Database offline.")
        if not bungie:
            return await interaction.followup.send("Bungie API not connected.")

        # Parse and look up the player
        try:
            display_name, code = parse_bungie_name(bungie_name)
        except ValueError as exc:
            return await interaction.followup.send(f"Invalid format: {exc}")

        try:
            results = await bungie.search_player_exact(display_name, code, MEMBERSHIP_TYPE_ALL)
        except BungieAPIError as exc:
            return await interaction.followup.send(f"Bungie API error: {exc.message}")

        if not results:
            return await interaction.followup.send(f"No player found for **{bungie_name}**.")

        # Prefer Marathon membership, fall back to first available
        marathon = extract_marathon_memberships(results)
        target = marathon[0] if marathon else results[0]

        mid = str(target.get("membershipId", ""))
        mtype = target.get("membershipType", 0)
        dname = target.get("displayName", display_name)

        try:
            await pool.execute(
                "INSERT INTO tracked_players (membership_id, membership_type, display_name, bungie_name, added_by) "
                "VALUES ($1, $2, $3, $4, $5) "
                "ON CONFLICT (membership_id, membership_type) DO UPDATE SET "
                "display_name = $3, bungie_name = $4, auto_sync = true",
                mid, mtype, dname, bungie_name, str(interaction.user.id),
            )
        except Exception as exc:
            return await interaction.followup.send(f"Database error: {exc}")

        await interaction.followup.send(
            f"Now tracking **{dname}** (`{bungie_name}`). "
            f"Matches will auto-sync every 10 minutes."
        )

    @app_commands.command(name="untrack", description="Stop tracking a player")
    @app_commands.guilds(GUILD)
    @app_commands.describe(bungie_name="Bungie name to stop tracking")
    async def untrack(self, interaction: discord.Interaction, bungie_name: str) -> None:
        pool = self._pool()
        if not pool:
            return await interaction.response.send_message("Database offline.", ephemeral=True)

        result = await pool.execute(
            "UPDATE tracked_players SET auto_sync = false WHERE bungie_name = $1",
            bungie_name,
        )
        if result == "UPDATE 0":
            return await interaction.response.send_message(
                f"**{bungie_name}** is not being tracked.", ephemeral=True
            )
        await interaction.response.send_message(f"Stopped tracking **{bungie_name}**.")

    @app_commands.command(name="syncnow", description="Trigger an immediate sync for all tracked players")
    @app_commands.guilds(GUILD)
    async def syncnow(self, interaction: discord.Interaction) -> None:
        await interaction.response.defer()

        if self._syncing:
            return await interaction.followup.send("A sync is already in progress. Please wait.")

        players, matches = await self._run_sync()
        await interaction.followup.send(
            f"Sync complete: **{players}** player(s) checked, **{matches}** new match(es) imported."
        )

    @app_commands.command(name="tracked", description="List all tracked players")
    @app_commands.guilds(GUILD)
    async def tracked(self, interaction: discord.Interaction) -> None:
        pool = self._pool()
        if not pool:
            return await interaction.response.send_message("Database offline.", ephemeral=True)

        rows = await pool.fetch(
            "SELECT display_name, bungie_name, auto_sync, last_synced_at "
            "FROM tracked_players ORDER BY display_name"
        )
        if not rows:
            return await interaction.response.send_message("No players are being tracked yet.")

        lines = []
        for r in rows:
            status = ":green_circle:" if r["auto_sync"] else ":red_circle:"
            synced = r["last_synced_at"].strftime("%Y-%m-%d %H:%M") if r["last_synced_at"] else "never"
            lines.append(f"{status} **{r['display_name']}** ({r['bungie_name']}) — last sync: {synced}")

        embed = discord.Embed(
            title="Tracked Players",
            description="\n".join(lines),
            colour=0x00FF88,
        )
        embed.set_footer(text=f"{len(rows)} player(s) | Auto-sync every 10 min")
        await interaction.response.send_message(embed=embed)


# ── Helpers ──

def _stat_value(values: dict, key: str) -> int:
    """Extract a numeric stat value from Bungie's nested values structure."""
    stat = values.get(key, {})
    if isinstance(stat, dict):
        return int(stat.get("basic", {}).get("value", 0))
    return 0


def _mode_name(mode_type: int) -> str:
    """Map Bungie activity mode types to human names."""
    # These will need updating once Marathon's modes are known
    modes = {
        0: "unknown",
        4: "extraction",
        5: "pvp",
        7: "pve",
    }
    return modes.get(mode_type, "extraction")


async def setup(bot: commands.Bot) -> None:
    await bot.add_cog(Sync(bot))
