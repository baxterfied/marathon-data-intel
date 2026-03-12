"""Scrape Discord channel messages and export to JSON, split by Marathon launch date.

Usage:
    python utils/scrape_channel.py
    python utils/scrape_channel.py --before-days 30 --after-days 14
    python utils/scrape_channel.py --channel 1139164391724556329
"""

import argparse
import asyncio
import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import os

import discord
from dotenv import load_dotenv

# Load .env from project root if available
load_dotenv(Path(__file__).resolve().parent.parent / ".env")

DISCORD_TOKEN = os.getenv("DISCORD_TOKEN", "")

MARATHON_LAUNCH = datetime(2026, 3, 5, tzinfo=timezone.utc)
DEFAULT_CHANNEL_ID = 1139164391724556329


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Scrape Discord channel messages for vibe analysis")
    parser.add_argument("--channel", type=int, default=DEFAULT_CHANNEL_ID, help="Channel ID to scrape")
    parser.add_argument("--before-days", type=int, default=14, help="Days before launch to scrape (default: 14)")
    parser.add_argument("--after-days", type=int, default=7, help="Days after launch to scrape (default: 7)")
    parser.add_argument("--output", type=str, default=None, help="Output file path (default: auto-generated)")
    parser.add_argument("--limit", type=int, default=None, help="Max messages per period (default: no limit)")
    return parser.parse_args()


async def scrape(args: argparse.Namespace) -> None:
    intents = discord.Intents.default()
    intents.message_content = True
    client = discord.Client(intents=intents)

    before_start = MARATHON_LAUNCH - timedelta(days=args.before_days)
    after_end = MARATHON_LAUNCH + timedelta(days=args.after_days)

    result = {
        "channel_id": args.channel,
        "marathon_launch": MARATHON_LAUNCH.isoformat(),
        "scraped_at": datetime.now(timezone.utc).isoformat(),
        "pre_launch": {
            "range": f"{before_start.strftime('%Y-%m-%d')} to {MARATHON_LAUNCH.strftime('%Y-%m-%d')}",
            "messages": [],
        },
        "post_launch": {
            "range": f"{MARATHON_LAUNCH.strftime('%Y-%m-%d')} to {after_end.strftime('%Y-%m-%d')}",
            "messages": [],
        },
    }

    @client.event
    async def on_ready():
        print(f"Logged in as {client.user}")
        channel = client.get_channel(args.channel)
        if channel is None:
            try:
                channel = await client.fetch_channel(args.channel)
            except Exception as exc:
                print(f"ERROR: Could not access channel {args.channel}: {exc}")
                await client.close()
                return

        print(f"Scraping #{channel.name}...")
        print(f"  Pre-launch window:  {before_start.strftime('%b %d')} — {MARATHON_LAUNCH.strftime('%b %d, %Y')}")
        print(f"  Post-launch window: {MARATHON_LAUNCH.strftime('%b %d')} — {after_end.strftime('%b %d, %Y')}")

        # Scrape pre-launch
        print("\nScraping pre-launch messages...", end="", flush=True)
        count = 0
        async for msg in channel.history(
            after=before_start, before=MARATHON_LAUNCH, limit=args.limit, oldest_first=True
        ):
            if msg.author.bot:
                continue
            result["pre_launch"]["messages"].append({
                "id": str(msg.id),
                "author": msg.author.display_name,
                "author_id": str(msg.author.id),
                "content": msg.content,
                "timestamp": msg.created_at.isoformat(),
                "attachments": [a.url for a in msg.attachments],
                "reactions": [
                    {"emoji": str(r.emoji), "count": r.count} for r in msg.reactions
                ] if msg.reactions else [],
            })
            count += 1
            if count % 500 == 0:
                print(f" {count}...", end="", flush=True)
        print(f" {count} total")

        # Scrape post-launch
        print("Scraping post-launch messages...", end="", flush=True)
        count = 0
        async for msg in channel.history(
            after=MARATHON_LAUNCH, before=after_end, limit=args.limit, oldest_first=True
        ):
            if msg.author.bot:
                continue
            result["post_launch"]["messages"].append({
                "id": str(msg.id),
                "author": msg.author.display_name,
                "author_id": str(msg.author.id),
                "content": msg.content,
                "timestamp": msg.created_at.isoformat(),
                "attachments": [a.url for a in msg.attachments],
                "reactions": [
                    {"emoji": str(r.emoji), "count": r.count} for r in msg.reactions
                ] if msg.reactions else [],
            })
            count += 1
            if count % 500 == 0:
                print(f" {count}...", end="", flush=True)
        print(f" {count} total")

        # Summary
        pre_count = len(result["pre_launch"]["messages"])
        post_count = len(result["post_launch"]["messages"])
        result["pre_launch"]["total_messages"] = pre_count
        result["post_launch"]["total_messages"] = post_count

        # Write output
        if args.output:
            out_path = Path(args.output)
        else:
            out_dir = Path(__file__).resolve().parent.parent / "public" / "exports"
            out_dir.mkdir(parents=True, exist_ok=True)
            out_path = out_dir / f"vibe-export-{channel.name}.json"

        out_path.write_text(json.dumps(result, indent=2, ensure_ascii=False), encoding="utf-8")
        print(f"\nExported to {out_path}")
        print(f"  Pre-launch:  {pre_count} messages")
        print(f"  Post-launch: {post_count} messages")

        await client.close()

    if not DISCORD_TOKEN:
        print("ERROR: DISCORD_TOKEN not set. Pass via .env or environment variable.")
        return
    await client.start(DISCORD_TOKEN)


def main() -> None:
    args = parse_args()
    asyncio.run(scrape(args))


if __name__ == "__main__":
    main()
