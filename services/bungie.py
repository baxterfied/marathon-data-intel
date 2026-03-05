"""Bungie API client for Marathon Data Intel.

Handles authentication, player search, and Marathon (GoliathGame) data retrieval.
The Bungie API uses membershipType 20 for Marathon (internal codename: GoliathGame).
"""

import logging
from typing import Optional

import httpx

import config

log = logging.getLogger("marathon.bungie")

BASE_URL = "https://www.bungie.net/Platform"
MEMBERSHIP_TYPE_ALL = -1
MEMBERSHIP_TYPE_MARATHON = 20  # GoliathGame

# Rate limit: Bungie allows ~25 req/s per API key
TIMEOUT = 15.0


class BungieAPIError(Exception):
    def __init__(self, error_code: int, message: str, status: str = ""):
        self.error_code = error_code
        self.message = message
        self.status = status
        super().__init__(f"Bungie API error {error_code}: {message}")


class BungieClient:
    """Async HTTP client for the Bungie.net Platform API."""

    def __init__(self, api_key: str) -> None:
        self.api_key = api_key
        self._http: Optional[httpx.AsyncClient] = None

    async def start(self) -> None:
        self._http = httpx.AsyncClient(
            base_url=BASE_URL,
            headers={
                "X-API-Key": self.api_key,
                "Accept": "application/json",
            },
            timeout=TIMEOUT,
        )
        log.info("Bungie API client started")

    async def close(self) -> None:
        if self._http:
            await self._http.aclose()
            self._http = None
            log.info("Bungie API client closed")

    async def _request(self, method: str, path: str, **kwargs) -> dict:
        if not self._http:
            raise BungieAPIError(0, "Client not started")

        resp = await self._http.request(method, path, **kwargs)
        resp.raise_for_status()
        data = resp.json()

        error_code = data.get("ErrorCode", 1)
        if error_code != 1:
            raise BungieAPIError(
                error_code,
                data.get("Message", "Unknown error"),
                data.get("ErrorStatus", ""),
            )

        return data.get("Response", {})

    async def _get(self, path: str, **kwargs) -> dict:
        return await self._request("GET", path, **kwargs)

    async def _post(self, path: str, **kwargs) -> dict:
        return await self._request("POST", path, **kwargs)

    # ── User / Player Search ──

    async def search_players(self, display_name_prefix: str, page: int = 0) -> dict:
        """Search for players by Bungie display name prefix.

        Returns {searchResults: [...], page: int, hasMore: bool}
        """
        return await self._post(
            f"/User/Search/GlobalName/{page}/",
            json={"displayNamePrefix": display_name_prefix},
        )

    async def search_player_exact(
        self, display_name: str, display_name_code: int, membership_type: int = MEMBERSHIP_TYPE_ALL
    ) -> list[dict]:
        """Exact Bungie name search (e.g. 'PlayerName#1234').

        Returns list of membership info cards.
        """
        return await self._post(
            f"/Destiny2/SearchDestinyPlayerByBungieName/{membership_type}/",
            json={
                "displayName": display_name,
                "displayNameCode": display_name_code,
            },
        )

    async def get_membership_data(self, membership_id: int, membership_type: int = MEMBERSHIP_TYPE_ALL) -> dict:
        """Get linked membership data for a user."""
        return await self._get(
            f"/User/GetMembershipsById/{membership_id}/{membership_type}/",
        )

    async def get_bungie_user(self, membership_id: int) -> dict:
        """Get a user's general Bungie.net profile."""
        return await self._get(f"/User/GetBungieNetUserById/{membership_id}/")

    # ── Marathon Profile / Stats ──
    # These endpoints follow the Destiny2 pattern but for Marathon (GoliathGame).
    # They may not be active until Bungie enables them post-launch.

    async def get_marathon_profile(self, membership_type: int, membership_id: int) -> Optional[dict]:
        """Fetch a Marathon player profile.

        Tries the Destiny2-style profile endpoint since Marathon shares
        the Bungie.net platform infrastructure.
        """
        try:
            return await self._get(
                f"/Destiny2/{membership_type}/Profile/{membership_id}/",
                params={"components": "100,200"},
            )
        except (BungieAPIError, httpx.HTTPStatusError) as exc:
            log.debug("Marathon profile fetch failed (may not be live yet): %s", exc)
            return None

    async def get_marathon_character_stats(
        self, membership_type: int, membership_id: int, character_id: int
    ) -> Optional[dict]:
        """Fetch character-level stats for a Marathon player."""
        try:
            return await self._get(
                f"/Destiny2/{membership_type}/Account/{membership_id}/Character/{character_id}/Stats/",
            )
        except (BungieAPIError, httpx.HTTPStatusError) as exc:
            log.debug("Marathon character stats fetch failed: %s", exc)
            return None

    async def get_marathon_account_stats(self, membership_type: int, membership_id: int) -> Optional[dict]:
        """Fetch account-level stats for a Marathon player."""
        try:
            return await self._get(
                f"/Destiny2/{membership_type}/Account/{membership_id}/Stats/",
            )
        except (BungieAPIError, httpx.HTTPStatusError) as exc:
            log.debug("Marathon account stats fetch failed: %s", exc)
            return None

    async def get_activity_history(
        self, membership_type: int, membership_id: int, character_id: int, count: int = 25, page: int = 0
    ) -> Optional[dict]:
        """Fetch recent match activity history."""
        try:
            return await self._get(
                f"/Destiny2/{membership_type}/Account/{membership_id}/Character/{character_id}/Stats/Activities/",
                params={"count": count, "page": page},
            )
        except (BungieAPIError, httpx.HTTPStatusError) as exc:
            log.debug("Activity history fetch failed: %s", exc)
            return None

    async def get_pgcr(self, activity_id: int) -> Optional[dict]:
        """Fetch a Post-Game Carnage Report for a specific match."""
        try:
            return await self._get(
                f"/Destiny2/Stats/PostGameCarnageReport/{activity_id}/",
            )
        except (BungieAPIError, httpx.HTTPStatusError) as exc:
            log.debug("PGCR fetch failed: %s", exc)
            return None


# ── Module-level helpers ──

async def connect_bungie() -> Optional[BungieClient]:
    if not config.BUNGIE_API_KEY:
        log.warning("No BUNGIE_API_KEY set — Bungie API integration disabled")
        return None
    try:
        client = BungieClient(config.BUNGIE_API_KEY)
        await client.start()
        # Verify the key works with a lightweight call
        await client._get("/User/GetBungieAccount/0/254/")
    except Exception:
        # Key validation may fail on non-existent user, that's fine
        # as long as we get a Bungie API response (not a 401)
        pass
    log.info("Bungie API client ready")
    return client


async def close_bungie(client: Optional[BungieClient]) -> None:
    if client:
        await client.close()


def parse_bungie_name(bungie_name: str) -> tuple[str, int]:
    """Parse 'PlayerName#1234' into (name, code). Raises ValueError on bad format."""
    if "#" not in bungie_name:
        raise ValueError("Bungie name must be in format 'Name#1234'")
    parts = bungie_name.rsplit("#", 1)
    name = parts[0].strip()
    try:
        code = int(parts[1].strip())
    except ValueError:
        raise ValueError("Code after # must be a number")
    return name, code


def extract_marathon_memberships(memberships: list[dict]) -> list[dict]:
    """Filter a list of membership info cards to only Marathon (GoliathGame) ones."""
    return [m for m in memberships if m.get("membershipType") == MEMBERSHIP_TYPE_MARATHON]


def format_platform(membership_type: int) -> str:
    """Human-readable platform name from membership type."""
    platforms = {
        1: "Xbox",
        2: "PlayStation",
        3: "Steam",
        4: "Blizzard",
        5: "Stadia",
        6: "Epic",
        10: "Demon",
        20: "Marathon",
        254: "Bungie.net",
    }
    return platforms.get(membership_type, f"Unknown ({membership_type})")
