"""Claude AI integration for Marathon Intel — smarter than Virgil."""

import hashlib
import json
import logging
from typing import Optional

import anthropic
import redis.asyncio as aioredis

import config
from services.redis_cache import TTL_AI_INSIGHT, TTL_HISTORY

log = logging.getLogger("marathon.ai")

CLAUDE_MODEL = "claude-sonnet-4-6-20250514"
MAX_TOKENS = 1500
HISTORY_MAX_TURNS = 10

SYSTEM_PROMPT = """\
You are Marathon Intel — the community's AI-powered data analyst for Bungie's \
Marathon (2025). You have access to community match data, runner stats, meta \
analysis, network performance data, and patch history.

Your expertise:
- Runner tier rankings backed by win rate, pick rate, and ban rate data
- Map-specific strategies and loadout recommendations
- Patch impact analysis showing before/after meta shifts
- Network performance correlation with match outcomes
- Community trends and leaderboard insights

Your personality:
- Data-driven and precise. Always cite numbers when available.
- Competitive gaming community tone — knowledgeable but approachable.
- You speak like a seasoned analyst on a broadcast desk, not a chatbot.
- Direct answers first, deeper analysis if asked.
- Never say "As an AI" — you are Marathon Intel.

Constraints:
- Keep responses under 1800 characters for Discord.
- When you don't have data, say so clearly.
- Base recommendations on actual community data, not speculation.
"""

TIKTOK_SYSTEM = """\
You are Marathon Intel's content engine. Generate engaging TikTok script ideas \
from Marathon community data trends. Scripts should be 30-60 seconds, hook-first, \
data-backed, and designed for the competitive FPS audience. Use dramatic framing \
and community-relevant talking points.
"""

META_REPORT_SYSTEM = """\
You are Marathon Intel generating a weekly meta report. Analyze the runner data \
provided and produce a structured tier list with explanations. Format for Discord \
embed: use S/A/B/C/D tiers, cite win rates and pick rates, note any movers from \
last patch. Keep it under 2000 characters.
"""


async def connect_ai() -> Optional[anthropic.AsyncAnthropic]:
    try:
        client = anthropic.AsyncAnthropic(api_key=config.ANTHROPIC_API_KEY)
        log.info("Anthropic client initialised")
        return client
    except Exception as exc:
        log.warning("Could not initialise Anthropic client: %s", exc)
        return None


async def close_ai(client: Optional[anthropic.AsyncAnthropic]) -> None:
    if client is not None:
        try:
            await client.close()
        except Exception:
            pass
        log.info("Anthropic client closed")


# -- Conversation history --

def _history_key(channel_id: int) -> str:
    return f"marathon:history:{channel_id}"


async def get_history(redis_client: Optional[aioredis.Redis], channel_id: int) -> list[dict]:
    if redis_client is None:
        return []
    try:
        raw = await redis_client.get(_history_key(channel_id))
        if raw:
            return json.loads(raw)
    except Exception:
        pass
    return []


async def append_history(
    redis_client: Optional[aioredis.Redis],
    channel_id: int,
    user_msg: str,
    assistant_msg: str,
) -> None:
    if redis_client is None:
        return
    try:
        history = await get_history(redis_client, channel_id)
        history.append({"role": "user", "content": user_msg})
        history.append({"role": "assistant", "content": assistant_msg})
        max_msgs = HISTORY_MAX_TURNS * 2
        if len(history) > max_msgs:
            history = history[-max_msgs:]
        await redis_client.set(_history_key(channel_id), json.dumps(history), ex=TTL_HISTORY)
    except Exception:
        pass


# -- Response cache --

def _cache_key(question: str) -> str:
    digest = hashlib.sha256(question.strip().lower().encode()).hexdigest()[:16]
    return f"marathon:ask_cache:{digest}"


async def get_cached(redis_client: Optional[aioredis.Redis], question: str) -> Optional[str]:
    if redis_client is None:
        return None
    try:
        return await redis_client.get(_cache_key(question))
    except Exception:
        return None


async def set_cached(redis_client: Optional[aioredis.Redis], question: str, response: str) -> None:
    if redis_client is None:
        return
    try:
        await redis_client.set(_cache_key(question), response, ex=300)
    except Exception:
        pass


# -- Core completion --

async def ask(
    client: Optional[anthropic.AsyncAnthropic],
    redis_client: Optional[aioredis.Redis],
    channel_id: int,
    question: str,
    db_context: str = "",
) -> str:
    if client is None:
        return "AI subsystems are offline. Try again later."

    cached = await get_cached(redis_client, question)
    if cached:
        return cached

    history = await get_history(redis_client, channel_id)

    system = SYSTEM_PROMPT
    if db_context:
        system += f"\n\n--- LIVE DATA ---\n{db_context}"

    messages = history + [{"role": "user", "content": question}]

    try:
        response = await client.messages.create(
            model=CLAUDE_MODEL,
            max_tokens=MAX_TOKENS,
            system=system,
            messages=messages,
            timeout=30.0,
        )
        text = response.content[0].text
    except anthropic.RateLimitError:
        log.warning("Claude rate-limited")
        return "Rate limited — try again in a moment."
    except anthropic.APITimeoutError:
        log.warning("Claude timed out")
        return "Request timed out — try again."
    except Exception as exc:
        log.error("Claude error: %s", exc)
        return "AI error — try again later."

    await append_history(redis_client, channel_id, question, text)
    await set_cached(redis_client, question, text)
    return text


async def generate_insight(
    client: Optional[anthropic.AsyncAnthropic],
    system: str,
    prompt: str,
    max_tokens: int = 2000,
) -> Optional[str]:
    if client is None:
        return None
    try:
        response = await client.messages.create(
            model=CLAUDE_MODEL,
            max_tokens=max_tokens,
            system=system,
            messages=[{"role": "user", "content": prompt}],
            timeout=60.0,
        )
        return response.content[0].text
    except Exception as exc:
        log.error("Insight generation failed: %s", exc)
        return None


async def generate_tiktok_script(client: Optional[anthropic.AsyncAnthropic], data_summary: str) -> Optional[str]:
    return await generate_insight(client, TIKTOK_SYSTEM, data_summary, max_tokens=1000)


async def generate_meta_report(client: Optional[anthropic.AsyncAnthropic], runner_data: str) -> Optional[str]:
    return await generate_insight(client, META_REPORT_SYSTEM, runner_data, max_tokens=2000)


async def generate_patch_analysis(client: Optional[anthropic.AsyncAnthropic], patch_data: str) -> Optional[str]:
    system = (
        "You are Marathon Intel. Analyze the patch notes provided and generate "
        "a concise impact summary. Focus on meta shifts, runner buffs/nerfs, "
        "and predicted tier changes. Keep under 1500 characters for Discord."
    )
    return await generate_insight(client, system, patch_data, max_tokens=1500)
