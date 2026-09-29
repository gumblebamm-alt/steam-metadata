#!/usr/bin/env python3
"""
tracker.py - Steam Metadata & Depot Manifest Historical Tracker
Main orchestrator CLI.

Usage Examples:
    # 1. Quick test on 5 popular games:
    python tracker.py --appids 945360,730,105600,570,3764200

    # 2. Test first 50 apps from Steam catalog:
    python tracker.py --limit 50

    # 3. Full background run with resume and 5 workers:
    python tracker.py --run --concurrency 5

    # 4. Force refresh applist using official Steam API Key:
    python tracker.py --fetch-applist --steam-key YOUR_KEY_HERE

    # 5. Show summary stats of collected metadata:
    python tracker.py --stats
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import signal
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

from applist_provider import get_app_list
from checkpoint import CheckpointManager
from fetcher import SteamInfoFetcher
from hubcap_provider import HubcapProvider
from steam_webapi_provider import SteamWebAPIProvider
from storage import MetadataStorage

# Setup rich / clean logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("steam_tracker")


class SteamTracker:
    def __init__(
        self,
        base_dir: Path,
        concurrency: int = 10,
        delay: float = 0.05,
        batch_save: int = 30,
        steam_key: Optional[str] = None,
        hubcap_key: Optional[str] = None,
        steam_webapi_key: Optional[str] = None,
    ):
        self.base_dir = Path(base_dir)
        self.concurrency = concurrency
        self.delay = delay
        self.batch_save = batch_save
        self.steam_key = steam_key
        self.hubcap: Optional[HubcapProvider] = (
            HubcapProvider(hubcap_key) if hubcap_key else None
        )

        self.storage = MetadataStorage(self.base_dir)
        self.cache_dir = self.base_dir / "cache"
        self.applist_path = self.cache_dir / "applist.json"
        self.checkpoint_path = self.cache_dir / "checkpoint.json"
        self.checkpoint = CheckpointManager(self.checkpoint_path)

        self.steam_webapi: Optional[SteamWebAPIProvider] = (
            SteamWebAPIProvider(steam_webapi_key, self.cache_dir)
            if steam_webapi_key else None
        )

        self._shutdown_requested = False

    def request_shutdown(self, signum=None, frame=None):
        """Handles Ctrl+C gracefully."""
        if not self._shutdown_requested:
            print("\n[!] Graceful shutdown requested. Saving checkpoint before exit...")
            self._shutdown_requested = True

    async def scan_single_app(
        self,
        fetcher: SteamInfoFetcher,
        app: Dict[str, Any],
    ) -> bool:
        """Fetches and saves metadata for a single app."""
        appid = int(app["appid"])
        fallback_name = app.get("name", "")

        success, raw_data = await fetcher.fetch_app_info(appid)
        if not success or raw_data is None:
            self.checkpoint.mark_processed(appid, success=False, is_error=True)
            return False

        saved_ok, is_new_ver = self.storage.save_app_record(
            appid=appid,
            raw_info=raw_data,
            fallback_name=fallback_name,
        )

        self.checkpoint.mark_processed(
            appid=appid,
            success=saved_ok,
            is_new_version=is_new_ver,
            is_error=not saved_ok,
        )
        return True

    async def hubcap_scan(self, app_list: List[Dict[str, Any]], max_feed_check: int = 300) -> List[Dict[str, Any]]:
        """
        Phase 1 (Hubcap - FREE):
        Queries Hubcap's recently updated games feed (/api/v1/library?sort_by=updated).
        For any game that has manifest_updated > local _lastChecked:
          - Fetches depot_id + manifest_id via /manifest/{appid}/contents (with polite rate limiting).
          - Saves manifest entry into local shard JSON.
          - Queues the game for Phase 2 Steam scan so Steam can attach the real buildId and metadata!
        Returns list of priority app dicts: [{"appid": aid, "name": game_name}, ...]
        """
        if not self.hubcap:
            return []

        logger.info(f"[Hubcap] Phase 1: Checking top {max_feed_check} recently updated games on Hubcap...")
        recent_games = self.hubcap.get_recently_updated_games(max_games=max_feed_check)
        if not recent_games:
            logger.info("[Hubcap] No games returned from Hubcap feed.")
            return []

        priority_apps: List[Dict[str, Any]] = []
        for game in recent_games:
            if self._shutdown_requested:
                break

            try:
                aid = int(game.get("game_id", 0))
            except (ValueError, TypeError):
                continue

            if not aid:
                continue

            if not game.get("manifest_available", False):
                continue

            hubcap_ts = self.hubcap._iso_to_ts(game.get("manifest_updated", ""))
            existing = self.storage.load_app_metadata(aid)
            last_checked = existing.get("_lastChecked", 0) if existing else 0

            # If our local record is already newer or equal to Hubcap's update, skip
            if last_checked >= hubcap_ts and existing and existing.get("_history"):
                continue

            # Fetch manifest details (includes gentle delay + 429 retry)
            hub_entry = self.hubcap.get_manifest_info_for_storage(aid)
            if not hub_entry:
                continue

            game_name = game.get("game_name") or ""
            saved, is_new_version = self.storage.save_hubcap_entry(
                appid=aid,
                hubcap_entry=hub_entry,
                fallback_name=game_name,
            )

            # Queue this game as HIGH PRIORITY for Phase 2 (Steam PICS scan)
            # Steam will fetch the real buildId, appinfo, and merge both manifest sets!
            priority_apps.append({"appid": aid, "name": game_name})
            logger.info(f"[Hubcap] Ingested manifests for AppID {aid} ({game_name}) -> queued for Steam buildId sync")

        logger.info(f"[Hubcap] Phase 1 finished: {len(priority_apps)} updated games queued for Steam scan.")
        return priority_apps

    def _load_hubcap_offset(self) -> int:
        """Reads the saved Hubcap library pagination offset from disk."""
        offset_file = self.cache_dir / "hubcap_offset.json"
        try:
            if offset_file.exists():
                data = json.loads(offset_file.read_text(encoding="utf-8"))
                return int(data.get("offset", 0))
        except Exception:
            pass
        return 0

    def _save_hubcap_offset(self, offset: int) -> None:
        """Saves the current Hubcap library pagination offset to disk."""
        offset_file = self.cache_dir / "hubcap_offset.json"
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        offset_file.write_text(
            json.dumps({"offset": offset, "updated_at": int(time.time())}),
            encoding="utf-8"
        )

    async def hubcap_gap_fill(self, batch_size: int = 500) -> List[Dict[str, Any]]:
        """
        Phase 1b (Hubcap - FREE, Gap Fill):
        Systematically pages through Hubcap's full 167k library, batch_size games
        at a time, to find games NOT yet stored in data/.
        Progress (offset) is saved between runs so it resumes from where it left off.
        After scanning all 167k games (~335 runs = ~167 days at 2 runs/day),
        resets to offset 0 to start a fresh full sweep.
        Returns list of new game dicts found: [{"appid": aid, "name": game_name}, ...]
        """
        if not self.hubcap:
            return []

        offset = self._load_hubcap_offset()
        new_games: List[Dict[str, Any]] = []
        scanned = 0
        page_size = 100

        logger.info(f"[Hubcap] Phase 1b (Gap Fill): scanning library from offset {offset} "
                    f"(batch of {batch_size} games)...")

        while scanned < batch_size:
            if self._shutdown_requested:
                break

            games = self.hubcap.get_library_page_at_offset(offset=offset, limit=page_size)
            if not games:
                # Reached end of Hubcap library — reset offset for next full sweep
                logger.info("[Hubcap] Gap Fill: completed full library sweep! Resetting offset to 0.")
                self._save_hubcap_offset(0)
                break

            for game in games:
                try:
                    aid = int(game.get("game_id", 0))
                except (ValueError, TypeError):
                    continue
                if not aid or not game.get("manifest_available", False):
                    continue

                # Only queue games not yet in our data/
                if not self.storage.get_file_path(aid).exists():
                    new_games.append({"appid": aid, "name": game.get("game_name") or ""})

            offset += len(games)
            scanned += len(games)

            if len(games) < page_size:
                # End of library
                logger.info("[Hubcap] Gap Fill: completed full library sweep! Resetting offset to 0.")
                self._save_hubcap_offset(0)
                break

            time.sleep(0.5)  # polite pause

        else:
            # Saved progress mid-sweep for next run
            self._save_hubcap_offset(offset)
            logger.info(f"[Hubcap] Gap Fill: scanned {scanned} games, offset now {offset}. "
                        f"Found {len(new_games)} new games not yet in data/.")

        return new_games

    async def steam_webapi_scan(
        self,
        hubcap_priority_ids: set,
    ) -> List[Dict[str, Any]]:
        """
        Phase 1c (Steam Web API - FREE):
        Calls IStoreService/GetAppList?if_modified_since to get ALL apps
        that changed on Steam since the last run timestamp.

        This catches game updates that Hubcap misses (e.g. EA SPORTS FC 27
        build 25562691 was invisible to Hubcap but visible here immediately).

        Logic:
          - Only queues apps already in data/ that have steam last_modified
            newer than our local _lastChecked (avoids duplicate work with gap fill).
          - Skips apps already queued by Hubcap Phase 1a (hubcap_priority_ids).
          - On first run: uses a 24h lookback window.
          - Saves timestamp → next run only fetches delta.
        Returns: [{appid, name}, ...] deduped priority list.
        """
        if not self.steam_webapi:
            return []

        # Determine lookback window
        since_ts = self.steam_webapi.get_last_run_ts()
        now_ts = int(time.time())
        if since_ts == 0:
            # First ever run — use 24h lookback to catch recent builds
            since_ts = now_ts - 86400
            logger.info("[SteamWebAPI] Phase 1c: First run, using 24h lookback window.")
        else:
            age_hours = (now_ts - since_ts) / 3600
            logger.info(
                f"[SteamWebAPI] Phase 1c: Fetching changes since last run "
                f"({age_hours:.1f}h ago)..."
            )

        # Fetch changed apps in a thread (synchronous httpx)
        changed_apps = await asyncio.to_thread(
            self.steam_webapi.get_changed_apps_since, since_ts
        )

        if not changed_apps:
            logger.info("[SteamWebAPI] Phase 1c: No apps changed since last run.")
            self.steam_webapi.save_last_run_ts(now_ts)
            return []

        # Filter: only apps that are already in data/ AND have newer last_modified
        # (new apps are handled by Hubcap gap fill)
        priority_apps: List[Dict[str, Any]] = []
        skipped_hubcap = 0
        skipped_uptodate = 0
        skipped_notindb = 0

        for app in changed_apps:
            if self._shutdown_requested:
                break

            aid = app.get("appid")
            if not aid:
                continue

            # Skip if Hubcap already queued this one (Phase 1a handles it)
            if aid in hubcap_priority_ids:
                skipped_hubcap += 1
                continue

            steam_last_modified = app.get("last_modified", 0)
            existing = self.storage.load_app_metadata(aid)

            if not existing:
                # Not in our DB yet → gap fill will handle it
                skipped_notindb += 1
                continue

            local_last_checked = existing.get("_lastChecked", 0)
            if steam_last_modified <= local_last_checked:
                # Our data is already up to date
                skipped_uptodate += 1
                continue

            # This app is in our DB and Steam says it changed → re-scan it
            priority_apps.append({"appid": aid, "name": app.get("name", "")})

        logger.info(
            f"[SteamWebAPI] Phase 1c done: {len(priority_apps)} apps need re-scan "
            f"(skipped: {skipped_hubcap} already in Hubcap, "
            f"{skipped_uptodate} up-to-date, {skipped_notindb} not in DB yet)."
        )

        # Save timestamp for next run AFTER successful processing
        self.steam_webapi.save_last_run_ts(now_ts)
        return priority_apps

    async def run_scan(
        self,
        app_list: List[Dict[str, Any]],
        limit: Optional[int] = None,
        resume: bool = True,
    ) -> None:
        """Runs asynchronous scanning over the app list."""
        if resume:
            self.checkpoint.load()

        # Filter out already processed apps
        pending_apps = []
        for app in app_list:
            aid = int(app["appid"])
            if resume and self.checkpoint.is_processed(aid):
                continue
            pending_apps.append(app)

        if limit:
            pending_apps = pending_apps[:limit]

        total_pending = len(pending_apps)
        logger.info(
            f"Starting scan: {total_pending} apps pending "
            f"({len(self.checkpoint.processed_appids)} already processed in checkpoint). "
            f"Workers: {self.concurrency}"
        )

        if total_pending == 0:
            logger.info("No pending apps to scan. All apps in list are already marked as processed.")
            return

        start_time = time.time()
        processed_in_session = 0

        async with SteamInfoFetcher(
            concurrency=self.concurrency,
            request_delay=self.delay,
        ) as fetcher:
            # We process in small chunks of size batch_save
            chunk_size = self.batch_save
            for i in range(0, total_pending, chunk_size):
                if self._shutdown_requested:
                    break

                batch = pending_apps[i : i + chunk_size]
                tasks = [self.scan_single_app(fetcher, app) for app in batch]
                results = await asyncio.gather(*tasks, return_exceptions=True)

                processed_in_session += len(batch)
                self.checkpoint.save()

                # Print progress update
                elapsed = time.time() - start_time
                rate = processed_in_session / max(elapsed, 0.001)
                logger.info(
                    f"Progress: [{self.checkpoint.total_scanned}/{len(app_list)}] "
                    f"(+{processed_in_session} this session | {rate:.1f} apps/s) | "
                    f"New/Updated: {self.checkpoint.new_version_count} | "
                    f"Errors: {self.checkpoint.error_count}"
                )

        self.checkpoint.save()
        total_time = time.time() - start_time
        logger.info(
            f"Scan session ended. Elapsed: {total_time:.1f}s. "
            f"Total processed in database: {self.checkpoint.total_scanned}. "
            f"Checkpoint saved."
        )


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Steam Metadata & Depot Manifest Historical Tracker",
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument("--run", action="store_true", help="Start full catalog scan")
    parser.add_argument("--appids", type=str, help="Scan specific comma-separated AppIDs (e.g. 945360,730)")
    parser.add_argument("--limit", type=int, help="Maximum number of apps to scan in this run")
    parser.add_argument("--concurrency", type=int, default=10, help="Number of concurrent workers (default: 10)")
    parser.add_argument("--delay", type=float, default=0.05, help="Delay between requests per worker (default: 0.05s)")
    parser.add_argument("--no-resume", action="store_true", help="Do not resume from checkpoint")
    parser.add_argument("--reset-checkpoint", action="store_true", help="Clear existing checkpoint")
    parser.add_argument("--fetch-applist", action="store_true", help="Force refresh Steam AppID catalog")
    parser.add_argument("--steam-key", type=str, help="Official Steam Web API key (optional)")
    parser.add_argument(
        "--hubcap-key", type=str,
        help="Hubcap Manifest API key (optional) - use free endpoints to detect updates and reduce Steam requests"
    )
    parser.add_argument(
        "--steam-web-api-key", type=str,
        dest="steam_web_api_key",
        help="Steam Web API key for IStoreService/GetAppList delta detection (Phase 1c)"
    )
    parser.add_argument("--stats", action="store_true", help="Show summary statistics of stored dataset")

    args = parser.parse_args()

    base_dir = Path(__file__).resolve().parent
    tracker = SteamTracker(
        base_dir=base_dir,
        concurrency=args.concurrency,
        delay=args.delay,
        steam_key=args.steam_key,
        hubcap_key=getattr(args, "hubcap_key", None),
        steam_webapi_key=getattr(args, "steam_web_api_key", None),
    )

    # Register Ctrl+C handler
    signal.signal(signal.SIGINT, tracker.request_shutdown)
    signal.signal(signal.SIGTERM, tracker.request_shutdown)

    if args.stats:
        stats = tracker.storage.get_stats()
        print("\n=== Steam Metadata Dataset Statistics ===")
        print(f"Data directory:   {stats['data_directory']}")
        print(f"Total apps saved: {stats['total_files']:,}")
        print(f"Active shards:    {stats['shards_count']} / 1,000")
        if tracker.checkpoint_path.exists():
            tracker.checkpoint.load()
            print(f"Checkpoint scan:  {tracker.checkpoint.total_scanned:,} scanned, "
                  f"{tracker.checkpoint.new_version_count:,} versions, "
                  f"{tracker.checkpoint.error_count:,} errors")
        print("=========================================\n")
        return 0

    if args.reset_checkpoint:
        tracker.checkpoint.reset()
        print("Checkpoint reset successfully.")
        if not (args.run or args.appids or args.limit):
            return 0

    # Handle targeted appids
    if args.appids:
        raw_ids = [s.strip() for s in args.appids.split(",") if s.strip()]
        app_list = [{"appid": int(i), "name": f"App {i}"} for i in raw_ids if i.isdigit()]
        asyncio.run(tracker.run_scan(app_list, resume=not args.no_resume))
        return 0

    # Ensure applist is available
    force_fetch = args.fetch_applist
    app_list = get_app_list(
        cache_path=tracker.applist_path,
        steam_key=args.steam_key,
        force_refresh=force_fetch,
    )

    if args.fetch_applist and not (args.run or args.limit):
        print(f"Applist refreshed. Total apps available: {len(app_list):,}")
        return 0

    if args.run or args.limit:
        async def _run() -> None:
            # ── Phase 1a: Hubcap Recent Feed ──────────────────────────────────
            # Detects games whose manifests recently changed on Hubcap.
            # Fast, FREE, ~29–200 games per 12h window.
            priority_apps: List[Dict[str, Any]] = []
            if tracker.hubcap:
                logger.info("[Phase 1a] Hubcap recent feed: checking top 300 updated games...")
                priority_apps = await tracker.hubcap_scan(app_list, max_feed_check=300)

            # Ensure Hubcap-discovered apps are in the app_list for Phase 2
            existing_appids = {int(a["appid"]) for a in app_list}
            for pa in priority_apps:
                aid = int(pa["appid"])
                if aid not in existing_appids:
                    app_list.append(pa)
                    existing_appids.add(aid)

            # Set of appids Phase 1a already handles (passed to Phase 1c to skip)
            hubcap_priority_ids = {int(pa["appid"]) for pa in priority_apps}

            # ── Phase 1b: Hubcap Gap Fill ─────────────────────────────────────
            # Pages through Hubcap's 167k library 500 at a time.
            # Finds games that Hubcap knows about but we haven't indexed yet.
            # Progress persists in cache/hubcap_offset.json between runs.
            gap_fill_apps: List[Dict[str, Any]] = []
            if tracker.hubcap:
                logger.info("[Phase 1b] Hubcap gap fill: scanning library for unindexed games...")
                gap_fill_apps = await tracker.hubcap_gap_fill(batch_size=500)
                logger.info(f"[Phase 1b] Gap fill found {len(gap_fill_apps)} new games not yet in data/.")

            # ── Phase 1c: Steam Web API Delta ─────────────────────────────────
            # Calls IStoreService/GetAppList?if_modified_since to get ALL apps
            # that changed on Steam since the last run.
            # This catches updates that Hubcap misses (e.g. FC27 build 25562691).
            # Only re-scans apps already in data/ with stale _lastChecked.
            webapi_apps: List[Dict[str, Any]] = []
            if tracker.steam_webapi:
                logger.info("[Phase 1c] Steam Web API delta: detecting updates Hubcap missed...")
                webapi_apps = await tracker.steam_webapi_scan(
                    hubcap_priority_ids=hubcap_priority_ids,
                )
                logger.info(f"[Phase 1c] Steam Web API found {len(webapi_apps)} additional games to re-scan.")

            # ── Phase 2: Targeted Steam PICS Scan ────────────────────────────
            # Merge all three sources, deduplicate by appid.
            # Only scan games that actually need work — never the full 250k list.
            seen_ids: set = set(hubcap_priority_ids)
            target_apps: List[Dict[str, Any]] = list(priority_apps)

            for app in gap_fill_apps + webapi_apps:
                aid = int(app["appid"])
                if aid not in seen_ids:
                    target_apps.append(app)
                    seen_ids.add(aid)

            if not target_apps:
                logger.info("[Phase 2] Everything is up to date — no Steam scan needed this run.")
                return

            logger.info(
                f"[Phase 2] Steam PICS scan: {len(target_apps)} target games "
                f"({len(priority_apps)} Hubcap updated + "
                f"{len(gap_fill_apps)} gap fill + "
                f"{len(webapi_apps)} Steam Web API delta)..."
            )

            # Discard checkpoint for priority apps so Steam always re-scans them
            for app in target_apps:
                tracker.checkpoint.processed_appids.discard(int(app["appid"]))

            await tracker.run_scan(
                app_list=target_apps,
                limit=args.limit,
                resume=False,
            )

        asyncio.run(_run())
        return 0

    # If no specific action specified, print help
    parser.print_help()
    return 0


if __name__ == "__main__":
    sys.exit(main())
