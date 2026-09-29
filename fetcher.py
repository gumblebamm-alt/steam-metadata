#!/usr/bin/env python3
"""
fetcher.py - Resilient Async HTTP Client for Steam Info
Fetches appinfo using two sources (tried in order):
  1. api.steamcmd.net/v1/info/{appid}          - fast mirror, may lag a few days
  2. store.steampowered.com/api/appdetails      - official Steam Store API (no key needed)
Features:
- Async semaphore concurrency limiter (default 5 workers).
- Exponential backoff on HTTP 429 (Rate Limit) and 503/502.
- Handles edge cases (empty apps, deleted apps, timeouts).
"""

from __future__ import annotations

import asyncio
import logging
import random
from typing import Any, Dict, Optional, Tuple
import httpx

logger = logging.getLogger("steam_tracker.fetcher")

STEAMCMD_INFO_URL  = "https://api.steamcmd.net/v1/info/{appid}"
STEAM_STORE_URL    = "https://store.steampowered.com/api/appdetails?appids={appid}&filters=basic,packages,platforms"
DEFAULT_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
    "Accept": "application/json",
}


def _merge_buildid_from_store(steamcmd_data: Dict[str, Any], store_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Steam Store API returns the authoritative current buildid for each branch.
    If steamcmd_data has older/missing buildid, patch it with store data.
    store_data format: appdetails response data block.
    """
    if not store_data:
        return steamcmd_data

    # Steam Store API gives us release_date, platforms etc but NOT branches/buildid directly.
    # However api.steamcmd.net usually has branches - so we only fall back to store
    # for basic metadata when steamcmd returns empty.
    result = dict(steamcmd_data)

    # If steamcmd gave us nothing useful, populate from store data
    if not result.get("common") and not result.get("depots"):
        common: Dict[str, Any] = {
            "name": store_data.get("name", ""),
            "type": store_data.get("type", "game"),
            "oslist": ",".join(
                plat for plat, supported in store_data.get("platforms", {}).items() if supported
            ),
        }
        result["common"] = common

    return result


class SteamInfoFetcher:
    def __init__(
        self,
        concurrency: int = 10,
        timeout: float = 15.0,
        max_retries: int = 4,
        request_delay: float = 0.05,
        steam_key: Optional[str] = None,
    ):
        self.semaphore = asyncio.Semaphore(concurrency)
        self.timeout = timeout
        self.max_retries = max_retries
        self.request_delay = request_delay
        self.steam_key = steam_key
        self.client: Optional[httpx.AsyncClient] = None

    async def __aenter__(self) -> SteamInfoFetcher:
        self.client = httpx.AsyncClient(
            headers=DEFAULT_HEADERS,
            timeout=self.timeout,
            follow_redirects=True,
            limits=httpx.Limits(max_keepalive_connections=30, max_connections=50),
        )
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        if self.client:
            await self.client.aclose()

    async def _fetch_from_steamcmd(self, appid: int) -> Optional[Dict[str, Any]]:
        """Fetch from api.steamcmd.net (fast mirror, may lag)."""
        assert self.client is not None
        url = STEAMCMD_INFO_URL.format(appid=appid)
        backoff = 2.0

        for attempt in range(1, self.max_retries + 1):
            if self.request_delay > 0:
                await asyncio.sleep(self.request_delay)
            try:
                resp = await self.client.get(url)

                if resp.status_code == 429:
                    wait_time = backoff + random.uniform(0.5, 2.0)
                    logger.warning(f"[SteamCMD 429] App {appid} attempt {attempt}. Backoff {wait_time:.1f}s")
                    await asyncio.sleep(wait_time)
                    backoff = min(backoff * 2.0, 60.0)
                    continue

                if resp.status_code in (500, 502, 503, 504):
                    wait_time = backoff + random.uniform(0.2, 1.0)
                    await asyncio.sleep(wait_time)
                    backoff = min(backoff * 1.5, 30.0)
                    continue

                if resp.status_code == 200:
                    try:
                        payload = resp.json()
                    except Exception:
                        return None
                    data_block = payload.get("data", {})
                    app_data = data_block.get(str(appid)) or data_block.get(int(appid))
                    return app_data if isinstance(app_data, dict) else {}

                return {}  # 404 / other 4xx → app doesn't exist

            except (httpx.TimeoutException, httpx.NetworkError) as net_err:
                if attempt == self.max_retries:
                    logger.warning(f"[SteamCMD] Network failure app {appid}: {net_err}")
                    return None
                await asyncio.sleep(backoff)
                backoff = min(backoff * 1.5, 20.0)
            except Exception as unk_err:
                logger.warning(f"[SteamCMD] Unexpected error app {appid}: {unk_err}")
                return None

        return None

    async def _fetch_from_steam_store(self, appid: int) -> Optional[Dict[str, Any]]:
        """
        Fetch from official Steam Store API.
        Returns only basic metadata (name, type, platforms) — no branches/buildid.
        Used as fallback when steamcmd returns empty data.
        """
        assert self.client is not None
        url = STEAM_STORE_URL.format(appid=appid)
        try:
            resp = await self.client.get(url)
            if resp.status_code == 200:
                payload = resp.json()
                app_block = payload.get(str(appid), {})
                if app_block.get("success") and app_block.get("data"):
                    return app_block["data"]
        except Exception as err:
            logger.debug(f"[SteamStore] Error fetching app {appid}: {err}")
        return None

    async def fetch_app_info(self, appid: int) -> Tuple[bool, Optional[Dict[str, Any]]]:
        """
        Fetches app details for a single appid.
        Source priority:
          1. api.steamcmd.net  — fast, has branches+buildid, may lag a few days
          2. Steam Store API   — always up-to-date for basic info, no buildid
        Returns: (success: bool, app_data: Optional[Dict])
        """
        if not self.client:
            raise RuntimeError("SteamInfoFetcher must be used as an async context manager.")

        async with self.semaphore:
            # Source 1: SteamCMD mirror
            steamcmd_data = await self._fetch_from_steamcmd(appid)

            if steamcmd_data is None:
                # Network failure — report error
                return False, None

            # If steamcmd returned actual data (has common or depots), use it
            if steamcmd_data.get("common") or steamcmd_data.get("depots"):
                return True, steamcmd_data

            # Source 2: Steam Store API — fills in basic metadata when steamcmd is empty
            store_data = await self._fetch_from_steam_store(appid)
            if store_data:
                merged = _merge_buildid_from_store(steamcmd_data, store_data)
                return True, merged

            # Both sources returned empty — app likely has no public data
            return True, steamcmd_data
