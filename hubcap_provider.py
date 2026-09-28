#!/usr/bin/env python3
"""
hubcap_provider.py - Hubcap Manifest API integration
Uses FREE endpoints (no usage count) to detect recently updated games
and ingest depot/manifest IDs without downloading or spending quota.

Designed with robust rate-limiting and 429 backoff.
"""

from __future__ import annotations

import logging
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import httpx

logger = logging.getLogger("steam_tracker.hubcap")

BASE_URL = "https://hubcapmanifest.com/api/v1"


class HubcapProvider:
    def __init__(self, api_key: str, timeout: float = 30.0):
        self._headers = {
            "Authorization": f"Bearer {api_key}",
            "User-Agent": "steam-metadata-tracker/1.1",
        }
        self._client = httpx.Client(
            timeout=timeout,
            headers=self._headers,
            follow_redirects=True,
        )

    def _iso_to_ts(self, iso: str) -> int:
        """Convert ISO 8601 string to UTC epoch seconds."""
        if not iso:
            return 0
        try:
            dt = datetime.fromisoformat(iso)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return int(dt.timestamp())
        except Exception:
            return 0

    def _get_with_retry(self, url: str, max_retries: int = 4) -> Optional[httpx.Response]:
        """Performs a GET request with exponential backoff on HTTP 429."""
        backoff = 3.0
        for attempt in range(1, max_retries + 1):
            try:
                resp = self._client.get(url)
                if resp.status_code == 200:
                    return resp
                if resp.status_code == 404:
                    return None
                if resp.status_code == 429:
                    logger.warning(
                        f"Hubcap rate-limited (HTTP 429) on {url}. "
                        f"Backing off for {backoff:.1f}s (attempt {attempt}/{max_retries})..."
                    )
                    time.sleep(backoff)
                    backoff *= 2.0
                    continue
                logger.warning(f"Hubcap returned HTTP {resp.status_code} for {url}")
                return None
            except Exception as exc:
                logger.warning(f"Hubcap request error on {url}: {exc}")
                time.sleep(backoff)
                backoff *= 1.5

        return None

    def health(self) -> bool:
        """Returns True if Hubcap API is healthy. FREE."""
        try:
            resp = self._client.get(f"{BASE_URL}/health", timeout=10)
            return resp.status_code == 200 and resp.json().get("status") == "healthy"
        except Exception:
            return False

    def get_recently_updated_games(self, max_games: int = 200) -> List[Dict[str, Any]]:
        """
        Queries GET /api/v1/library?sort_by=updated.
        FREE - no usage count.
        Returns list of games sorted by newest manifest update first.
        """
        games: List[Dict[str, Any]] = []
        limit_per_page = 100
        offset = 0

        while len(games) < max_games:
            fetch_count = min(limit_per_page, max_games - len(games))
            url = f"{BASE_URL}/library?limit={fetch_count}&offset={offset}&sort_by=updated&include_adult=true"
            resp = self._get_with_retry(url)
            if not resp:
                break

            data = resp.json()
            batch = data.get("games", [])
            if not batch:
                break

            games.extend(batch)
            offset += len(batch)

            if len(batch) < fetch_count:
                break

            # Polite pause between pagination requests
            time.sleep(1.0)

        logger.info(f"Retrieved {len(games)} recently updated games from Hubcap feed.")
        return games

    def get_library_page_at_offset(
        self, offset: int, limit: int = 100
    ) -> List[Dict[str, Any]]:
        """
        GET /api/v1/library?sort_by=game_id at a specific offset.
        Used for gap-fill: systematically paginate through Hubcap's full library
        to find games not yet stored in data/.
        FREE - no usage count.
        """
        url = f"{BASE_URL}/library?limit={limit}&offset={offset}&sort_by=game_id&include_adult=true"
        resp = self._get_with_retry(url)
        if not resp:
            return []
        return resp.json().get("games", [])

    def manifest_contents(self, app_id: int) -> Optional[Dict[str, Any]]:
        """
        GET /api/v1/manifest/{app_id}/contents
        Lists depot_id + manifest_id for an app without downloading.
        FREE - no usage count. Includes 429 retry backoff.
        """
        url = f"{BASE_URL}/manifest/{app_id}/contents"
        resp = self._get_with_retry(url)
        if not resp:
            return None
        try:
            return resp.json()
        except Exception:
            return None

    def get_manifest_info_for_storage(self, app_id: int) -> Optional[Dict[str, Any]]:
        """
        Fetches and transforms Hubcap manifest contents for local storage.
        Adds gentle rate limiting to avoid hitting 429.
        """
        contents = self.manifest_contents(app_id)
        # Gentle delay between manifest content calls
        time.sleep(0.5)

        if not contents or not contents.get("manifests"):
            return None

        manifests: Dict[str, Dict[str, Any]] = {}
        for m in contents.get("manifests", []):
            dep = str(m.get("depot_id", ""))
            gid = str(m.get("manifest_id", ""))
            if dep and gid:
                manifests[dep] = {"gid": gid, "size": None, "download": None}

        if not manifests:
            return None

        return {
            "buildId": "",  # Hubcap does not carry Steam PICS buildId
            "branch": contents.get("branch", "public"),
            "timeUpdated": self._iso_to_ts(contents.get("last_modified", "")),
            "firstSeen": int(time.time()),
            "manifests": manifests,
            "_source": "hubcap",
        }
