"""Server status monitoring and blog/RSS watching for Marathon Intel."""

import logging
import time
from typing import Optional
from xml.etree import ElementTree

import httpx

log = logging.getLogger("marathon.monitor")

# Endpoints to health-check
MARATHON_ENDPOINTS = {
    "Bungie API": "https://www.bungie.net/Platform/Settings/",
    "Bungie.net": "https://www.bungie.net/",
    "Marathon Web": "https://www.bungie.net/7/en/Marathon",
}

# Bungie blog RSS feed
BUNGIE_BLOG_RSS = "https://www.bungie.net/en/rss/News"
BUNGIE_BLOG_ALT = "https://www.bungie.net/7/en/News"

MARATHON_KEYWORDS = [
    "marathon", "runner", "patch", "update", "balance",
    "hotfix", "maintenance", "downtime", "season",
]


async def check_endpoint(url: str, timeout: float = 10.0) -> dict:
    """Check if an endpoint is reachable. Returns status dict."""
    start = time.monotonic()
    try:
        async with httpx.AsyncClient(timeout=timeout, follow_redirects=True) as client:
            resp = await client.get(url)
            elapsed = (time.monotonic() - start) * 1000
            return {
                "endpoint": url,
                "status_code": resp.status_code,
                "response_ms": round(elapsed, 1),
                "is_up": 200 <= resp.status_code < 400,
                "error": "",
            }
    except Exception as exc:
        elapsed = (time.monotonic() - start) * 1000
        return {
            "endpoint": url,
            "status_code": 0,
            "response_ms": round(elapsed, 1),
            "is_up": False,
            "error": str(exc)[:200],
        }


async def check_all_endpoints() -> list[dict]:
    """Check all Marathon-related endpoints."""
    results = []
    for name, url in MARATHON_ENDPOINTS.items():
        result = await check_endpoint(url)
        result["name"] = name
        results.append(result)
    return results


async def fetch_blog_posts(timeout: float = 15.0) -> list[dict]:
    """Fetch recent Bungie blog posts from RSS feed."""
    posts = []
    try:
        async with httpx.AsyncClient(timeout=timeout, follow_redirects=True) as client:
            resp = await client.get(BUNGIE_BLOG_RSS)
            if resp.status_code != 200:
                log.debug("Blog RSS returned %d", resp.status_code)
                return posts

            root = ElementTree.fromstring(resp.text)

            # RSS 2.0 format
            for item in root.iter("item"):
                title = item.findtext("title", "")
                link = item.findtext("link", "")
                description = item.findtext("description", "")
                pub_date = item.findtext("pubDate", "")

                if not link:
                    continue

                # Check if Marathon-related
                combined = (title + description).lower()
                is_marathon = any(kw in combined for kw in MARATHON_KEYWORDS)
                is_patch = any(kw in combined for kw in ["patch", "hotfix", "balance", "update"])

                posts.append({
                    "url": link,
                    "title": title,
                    "summary": description[:500] if description else "",
                    "pub_date": pub_date,
                    "is_marathon": is_marathon,
                    "is_patch": is_patch,
                })

    except Exception as exc:
        log.debug("Blog fetch failed: %s", exc)

    return posts
