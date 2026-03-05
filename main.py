"""Marathon Data Intel — Discord bot + FastAPI server running together."""

import asyncio
import logging
import sys
from pathlib import Path

import discord
import uvicorn
from discord.ext import commands

import config

# -- Logging --
LOG_DIR = Path(__file__).parent / "logs"
LOG_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    level=getattr(logging, config.LOG_LEVEL, logging.INFO),
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(LOG_DIR / "marathon.log"),
    ],
)
log = logging.getLogger("marathon")

# -- Bot setup --
intents = discord.Intents.all()

bot = commands.Bot(
    command_prefix="!",
    intents=intents,
    activity=discord.Activity(
        type=discord.ActivityType.watching,
        name="Marathon intel",
    ),
)

GUILD = discord.Object(id=config.DISCORD_GUILD_ID)

# -- Cog loader --
COGS_DIR = Path(__file__).parent / "cogs"


async def load_cogs() -> None:
    for cog_file in sorted(COGS_DIR.glob("*.py")):
        if cog_file.name.startswith("_"):
            continue
        ext = f"cogs.{cog_file.stem}"
        try:
            await bot.load_extension(ext)
            log.info("Loaded cog: %s", ext)
        except Exception:
            log.exception("Failed to load cog: %s", ext)


@bot.event
async def on_ready() -> None:
    synced = await bot.tree.sync(guild=GUILD)
    log.info(
        "Marathon Intel online as %s | %d guild command(s) synced",
        bot.user,
        len(synced),
    )


# -- Entrypoint --
async def main() -> None:
    from services import connect_db, close_db, connect_redis, close_redis, connect_ai, close_ai
    from web.api import create_app

    async with bot:
        bot.pool = await connect_db()
        bot.redis = await connect_redis()
        bot.ai_client = await connect_ai()

        await load_cogs()

        # Create FastAPI app
        app = create_app(bot)

        # Run uvicorn alongside the bot
        uvi_config = uvicorn.Config(
            app,
            host="0.0.0.0",
            port=config.WEB_PORT,
            log_level="info",
            access_log=False,
        )
        server = uvicorn.Server(uvi_config)

        try:
            # Start both the bot and the web server concurrently
            await asyncio.gather(
                bot.start(config.DISCORD_TOKEN),
                server.serve(),
            )
        finally:
            await close_ai(bot.ai_client)
            await close_db(bot.pool)
            await close_redis(bot.redis)


if __name__ == "__main__":
    asyncio.run(main())
