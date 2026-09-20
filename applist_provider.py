#!/usr/bin/env python3
"""
applist_provider.py - Steam AppList Provider
Fetches and caches the full list of Steam applications (~165k-184k apps).
Supports verified community snapshot mirrors and official Valve IStoreService API.
"""

from __future__ import annotations

import json
import logging
import time
from pathlib import Path
from typing import Any, Dict, List, Optional
import httpx

logger = logging.getLogger("steam_tracker.applist")

DUMP_URL_PRIMARY = "https://raw.githubusercontent.com/dgibbs64/SteamCMD-AppID-List/master/steamcmd_appid.json"
DUMP_URL_FALLBACK = "https://raw.githubusercontent.com/jsnli/steamappidlist/main/steam_apps.json"


def fetch_from_steam_web_api(api_key: str, max_results: int = 50000) -> List[Dict[str, Any]]:
    """
    Paginates Valve's official IStoreService/GetAppList/v1/ using last_appid.
    Requires a valid Steam Web API Key.
    """
    apps: List[Dict[str, Any]] = []
    last_appid = 0
    client = httpx.Client(timeout=30.0)

    logger.info("Fetching applist from official Valve IStoreService API...")
    while True:
        url = (
            f"https://api.steampowered.com/IStoreService/GetAppList/v1/"
            f"?key={api_key}&max_results={max_results}&last_appid={last_appid}&include_games=true&include_dlc=true"
        )
        resp = client.get(url)
        if resp.status_code != 200:
            raise RuntimeError(f"Steam Web API returned HTTP {resp.status_code}: {resp.text[:200]}")

        data = resp.json().get("response", {})
        batch = data.get("apps", [])
        if not batch:
            break

        for item in batch:
            appid = item.get("appid")
            name = item.get("name", "")
            if appid is not None:
                apps.append({"appid": int(appid), "name": name})

        last_appid = batch[-1].get("appid", 0)
        logger.info(f"Retrieved {len(apps)} apps so far (last_appid={last_appid})...")

        if not data.get("have_more_results", False):
            break
        time.sleep(0.5)

    return apps


def fetch_from_public_dump() -> List[Dict[str, Any]]:
    """
    Downloads full Steam AppID snapshot from verified automated GitHub mirrors.
    """
    client = httpx.Client(timeout=60.0, follow_redirects=True)
    for url in [DUMP_URL_PRIMARY, DUMP_URL_FALLBACK]:
        try:
            logger.info(f"Downloading applist snapshot from {url}...")
            resp = client.get(url)
            if resp.status_code == 200:
                raw_data = resp.json()
                apps: List[Dict[str, Any]] = []

                if isinstance(raw_data, list):
                    for item in raw_data:
                        aid = item.get("appid") or item.get("AppID") or item.get("id")
                        name = item.get("name") or item.get("Name") or ""
                        if aid is not None:
                            apps.append({"appid": int(aid), "name": name})
                elif isinstance(raw_data, dict):
                    # Check for SteamCMD-AppID-List format: {"applist": {"apps": {"app": [...]}}}
                    applist = raw_data.get("applist", {})
                    app_items = applist.get("apps", [])
                    if isinstance(app_items, dict):
                        app_items = app_items.get("app", [])
                    
                    if not app_items and "apps" in raw_data:
                        app_items = raw_data["apps"]

                    for item in app_items:
                        aid = item.get("appid") or item.get("AppID")
                        name = item.get("name") or item.get("Name") or ""
                        if aid is not None:
                            apps.append({"appid": int(aid), "name": name})

                if apps:
                    logger.info(f"Successfully parsed {len(apps)} apps from snapshot.")
                    return apps
        except Exception as exc:
            logger.warning(f"Failed to fetch from {url}: {exc}")

    raise RuntimeError("All public applist mirrors failed.")


def get_app_list(
    cache_path: Path,
    steam_key: Optional[str] = None,
    force_refresh: bool = False,
) -> List[Dict[str, Any]]:
    """
    Retrieves applist from local cache or downloads fresh list if missing/forced.
    """
    if not force_refresh and cache_path.exists():
        try:
            with open(cache_path, "r", encoding="utf-8") as f:
                cached = json.load(f)
            if isinstance(cached, list) and cached:
                logger.info(f"Loaded {len(cached)} apps from local cache: {cache_path}")
                return cached
        except Exception as exc:
            logger.warning(f"Failed to read cache {cache_path}: {exc}")

    cache_path.parent.mkdir(parents=True, exist_ok=True)

    if steam_key:
        apps = fetch_from_steam_web_api(steam_key)
    else:
        apps = fetch_from_public_dump()

    # Deduplicate by appid while preserving order
    seen = set()
    deduped = []
    for app in apps:
        aid = app["appid"]
        if aid not in seen:
            seen.add(aid)
            deduped.append(app)

    # Save to cache
    with open(cache_path, "w", encoding="utf-8") as f:
        json.dump(deduped, f, indent=2, ensure_ascii=False)
    logger.info(f"Cached {len(deduped)} apps to {cache_path}")

    return deduped


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="[%(asctime)s] [%(levelname)s] %(message)s")
    test_cache = Path("cache/applist.json")
    list_apps = get_app_list(test_cache)
    print(f"Total apps: {len(list_apps)}")
    print("First 3 apps:", list_apps[:3])
    print("Sample Among Us:", [a for a in list_apps if a["appid"] == 945360])
