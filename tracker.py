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
import logging
import signal
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

from applist_provider import get_app_list
from checkpoint import CheckpointManager
from fetcher import SteamInfoFetcher
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
    ):
        self.base_dir = Path(base_dir)
        self.concurrency = concurrency
        self.delay = delay
        self.batch_save = batch_save
        self.steam_key = steam_key

        self.storage = MetadataStorage(self.base_dir)
        self.cache_dir = self.base_dir / "cache"
        self.applist_path = self.cache_dir / "applist.json"
        self.checkpoint_path = self.cache_dir / "checkpoint.json"
        self.checkpoint = CheckpointManager(self.checkpoint_path)

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
    parser.add_argument("--stats", action="store_true", help="Show summary statistics of stored dataset")

    args = parser.parse_args()

    base_dir = Path(__file__).resolve().parent
    tracker = SteamTracker(
        base_dir=base_dir,
        concurrency=args.concurrency,
        delay=args.delay,
        steam_key=args.steam_key,
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
        asyncio.run(
            tracker.run_scan(
                app_list=app_list,
                limit=args.limit,
                resume=not args.no_resume,
            )
        )
        return 0

    # If no specific action specified, print help
    parser.print_help()
    return 0


if __name__ == "__main__":
    sys.exit(main())
