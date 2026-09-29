#!/usr/bin/env python3
"""
steam_webapi_provider.py — Steam Web API: IStoreService/GetAppList
Detects ALL apps that changed on Steam since the last run using
the official Steam Web API. This complements Hubcap which may miss
some updates (e.g. EA SPORTS FC 27 build 25562691 was not reflected
in Hubcap but was visible via this API immediately).

Endpoint: GET https://api.steampowered.com/IStoreService/GetAppList/v1/
  ?key=...&if_modified_since=UNIX_TS&include_games=1&include_dlc=1

Returns apps with fields: {appid, name, last_modified, price_change_number}
No quota cost. Rate-limit: generous (1 req/12h is well within limits).
Timestamp is persisted in cache/steam_webapi_ts.json between runs.
"""

from __future__ import annotations

import json
import logging
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

import httpx

logger = logging.getLogger("steam_tracker.steam_webapi")

STORE_APPLIST_URL = "https://api.steampowered.com/IStoreService/GetAppList/v1/"
MAX_RESULTS = 50000


class SteamWebAPIProvider:
    def __init__(self, api_key: str, cache_dir: Path):
        self.api_key = api_key
        self.cache_dir = Path(cache_dir)
        self._ts_file = self.cache_dir / "steam_webapi_ts.json"
        self._client = httpx.Client(
            timeout=30.0,
            follow_redirects=True,
            headers={"Accept": "application/json"},
        )

    # ── Timestamp persistence ────────────────────────────────────────────────

    def get_last_run_ts(self) -> int:
        """Returns unix timestamp of last successful Steam Web API call."""
        try:
            if self._ts_file.exists():
                data = json.loads(self._ts_file.read_text(encoding="utf-8"))
                return int(data.get("last_run_ts", 0))
        except Exception:
            pass
        return 0

    def save_last_run_ts(self, ts: int) -> None:
        """Saves the timestamp so next run only fetches apps modified after this."""
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self._ts_file.write_text(
            json.dumps({"last_run_ts": ts, "updated_at": int(time.time())}),
            encoding="utf-8",
        )

    # ── API call ─────────────────────────────────────────────────────────────

    def get_changed_apps_since(
        self,
        since_ts: int,
        include_dlc: bool = True,
        include_software: bool = False,
    ) -> List[Dict[str, Any]]:
        """
        Fetches all apps modified after since_ts via IStoreService/GetAppList.
        Handles pagination automatically (Steam returns max 50k per page).
        Returns list of dicts: [{appid, name, last_modified, price_change_number}, ...]
        """
        all_apps: List[Dict[str, Any]] = []
        last_appid: Optional[int] = None
        page = 0

        logger.info(
            f"[SteamWebAPI] Fetching apps modified since "
            f"{time.strftime('%Y-%m-%d %H:%M UTC', time.gmtime(since_ts))} ..."
        )

        while True:
            params: Dict[str, Any] = {
                "key": self.api_key,
                "if_modified_since": since_ts,
                "include_games": 1,
                "include_dlc": 1 if include_dlc else 0,
                "include_software": 1 if include_software else 0,
                "max_results": MAX_RESULTS,
            }
            if last_appid is not None:
                params["last_appid"] = last_appid

            try:
                resp = self._client.get(STORE_APPLIST_URL, params=params)
            except Exception as e:
                logger.warning(f"[SteamWebAPI] Request failed: {e}")
                break

            if resp.status_code == 429:
                logger.warning("[SteamWebAPI] Rate limited (429). Waiting 60s...")
                time.sleep(60)
                continue

            if not resp.status_code == 200:
                logger.warning(f"[SteamWebAPI] HTTP {resp.status_code}")
                break

            try:
                payload = resp.json()
            except Exception as e:
                logger.warning(f"[SteamWebAPI] JSON parse error: {e}")
                break

            response = payload.get("response", {})
            apps = response.get("apps", [])
            all_apps.extend(apps)
            page += 1

            have_more = response.get("have_more_results", False)
            if not have_more or not apps:
                break

            # Cursor for next page: last appid in current batch
            last_appid = apps[-1].get("appid")
            logger.debug(f"[SteamWebAPI] Page {page}: got {len(apps)} apps, fetching more...")
            time.sleep(0.5)  # polite pause between pages

        logger.info(
            f"[SteamWebAPI] Found {len(all_apps)} apps modified since last run "
            f"(across {page} page(s))."
        )
        return all_apps
