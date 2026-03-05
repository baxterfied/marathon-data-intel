"""Natural language AI chat — Claude answers Marathon questions using live DB data."""

import logging

import discord
from discord.ext import commands

from services import ai as ai_service

log = logging.getLogger("marathon.cogs.ai_chat")

# Trigger phrases that indicate a Marathon question
TRIGGER_WORDS = [
    "is ", "are ", "should ", "what ", "how ", "why ", "which ", "who ",
    "best ", "worst ", "meta ", "tier ", "runner ", "loadout ", "map ",
    "patch ", "nerf ", "buff ", "counter ", "match ", "stats ", "good",
    "bad ", "viable ", "broken ", "op ", "weak ", "strong ",
    "locus", "glitch", "viper", "iron", "specter", "nova",
    "blaze", "drift", "echo", "titan", "wraith", "sage",
]


class AIChat(commands.Cog):
    def __init__(self, bot: commands.Bot) -> None:
        self.bot = bot

    @commands.Cog.listener()
    async def on_message(self, message: discord.Message) -> None:
        if message.author.bot:
            return

        # Respond when mentioned or when message contains trigger words
        mentioned = self.bot.user in message.mentions if self.bot.user else False
        content_lower = message.content.lower()

        if not mentioned:
            # Check if it looks like a question about Marathon
            has_trigger = any(w in content_lower for w in TRIGGER_WORDS)
            ends_question = message.content.strip().endswith("?")
            if not (has_trigger and ends_question):
                return

        pool = getattr(self.bot, "pool", None)
        redis = getattr(self.bot, "redis", None)
        ai_client = getattr(self.bot, "ai_client", None)

        # Build context from DB
        db_context = ""
        if pool:
            try:
                runners = await pool.fetch("SELECT name, tier, win_rate, pick_rate, role FROM runners ORDER BY tier, name")
                if runners:
                    lines = [f"{r['name']} ({r['role']}): Tier {r['tier']}, WR {r['win_rate']:.1f}%, PR {r['pick_rate']:.1f}%" for r in runners]
                    db_context += "RUNNER DATA:\n" + "\n".join(lines) + "\n\n"

                stats = await pool.fetchrow("SELECT * FROM community_stats_view")
                if stats:
                    db_context += f"COMMUNITY: {stats['total_matches']} matches, {stats['unique_players']} players, avg K/D {stats['avg_kills']}/{stats['avg_deaths']}\n\n"

                latest_patch = await pool.fetchrow("SELECT version, title, summary FROM patch_notes ORDER BY released_at DESC LIMIT 1")
                if latest_patch:
                    db_context += f"LATEST PATCH: {latest_patch['version']} — {latest_patch['title']}\n{latest_patch['summary']}\n"
            except Exception as exc:
                log.debug("DB context fetch failed: %s", exc)

        clean_question = message.content
        if mentioned and self.bot.user:
            clean_question = clean_question.replace(f"<@{self.bot.user.id}>", "").strip()

        async with message.channel.typing():
            response = await ai_service.ask(
                ai_client, redis, message.channel.id, clean_question, db_context=db_context
            )

        if len(response) > 2000:
            response = response[:1997] + "..."

        await message.reply(response, mention_author=False)


async def setup(bot: commands.Bot) -> None:
    await bot.add_cog(AIChat(bot))
