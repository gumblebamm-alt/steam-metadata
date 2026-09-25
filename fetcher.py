#!/usr/bin/env python3
"""
fetcher.py - Resilient Async HTTP Client for Steam Info
Fetches appinfo from https://api.steamcmd.net/v1/info/{appid}
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

STEAMCMD_INFO_URL = "https://api.steamcmd.net/v1/info/{appid}"
DEFAULT_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
    "Accept": "application/json",
}


class SteamInfoFetcher:
    def __init__(
        self,
        concurrency: int = 10,
        timeout: float = 15.0,
        max_retries: int = 4,
        request_delay: float = 0.05,
    ):
        self.semaphore = asyncio.Semaphore(concurrency)
        self.timeout = timeout
        self.max_retries = max_retries
        self.request_delay = request_delay
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

    async def fetch_app_info(self, appid: int) -> Tuple[bool, Optional[Dict[str, Any]]]:
        """
        Fetches app details from api.steamcmd.net for a single appid.
        Returns: (success: bool, app_data: Optional[Dict])
        """
        if not self.client:
            raise RuntimeError("SteamInfoFetcher must be used as an async context manager.")

        url = STEAMCMD_INFO_URL.format(appid=appid)
        backoff = 2.0

        for attempt in range(1, self.max_retries + 1):
            async with self.semaphore:
                if self.request_delay > 0:
                    await asyncio.sleep(self.request_delay)

                try:
                    resp = await self.client.get(url)

                    # Handle Rate Limit (429)
                    if resp.status_code == 429:
                        wait_time = backoff + random.uniform(0.5, 2.0)
                        logger.warning(
                            f"[HTTP 429] Rate limited on app {appid} (attempt {attempt}/{self.max_retries}). "
                            f"Backing off for {wait_time:.1f}s..."
                        )
                        await asyncio.sleep(wait_time)
                        backoff = min(backoff * 2.0, 60.0)
                        continue

                    # Handle Temporary Server Errors (500, 502, 503, 504)
                    if resp.status_code in (500, 502, 503, 504):
                        wait_time = backoff + random.uniform(0.2, 1.0)
                        logger.debug(
                            f"[HTTP {resp.status_code}] Server error on app {appid}. Retrying in {wait_time:.1f}s..."
                        )
                        await asyncio.sleep(wait_time)
                        backoff = min(backoff * 1.5, 30.0)
                        continue

                    if resp.status_code == 200:
                        try:
                            payload = resp.json()
                        except Exception as parse_err:
                            logger.warning(f"JSON parse error for app {appid}: {parse_err}")
                            return False, None

                        data_block = payload.get("data", {})
                        app_data = data_block.get(str(appid)) or data_block.get(int(appid))

                        # Note: If Steam has no depots/branches for this app, app_data might be {}
                        return True, (app_data if isinstance(app_data, dict) else {})

                    # If 404 or other 4xx, the app doesn't exist
                    logger.debug(f"App {appid} returned status {resp.status_code}")
                    return True, {}

                except (httpx.TimeoutException, httpx.NetworkError) as net_err:
                    if attempt == self.max_retries:
                        logger.warning(f"Network failure fetching app {appid} after {self.max_retries} attempts: {net_err}")
                        return False, None
                    await asyncio.sleep(backoff)
                    backoff = min(backoff * 1.5, 20.0)
                except Exception as unk_err:
                    logger.warning(f"Unexpected error fetching app {appid}: {unk_err}")
                    return False, None

        return False, None
