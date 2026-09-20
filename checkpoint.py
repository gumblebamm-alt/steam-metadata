#!/usr/bin/env python3
"""
checkpoint.py - Checkpoint and Resume State Manager
Allows scanning ~184k Steam apps over multiple hours/days with Ctrl+C interruption
and seamless resumption without re-scanning previously processed apps.
"""

from __future__ import annotations

import json
import logging
import os
import time
from pathlib import Path
from typing import Any, Dict, Set

logger = logging.getLogger("steam_tracker.checkpoint")


class CheckpointManager:
    def __init__(self, checkpoint_path: Path):
        self.checkpoint_path = Path(checkpoint_path)
        self.processed_appids: Set[int] = set()
        self.total_scanned: int = 0
        self.success_count: int = 0
        self.new_version_count: int = 0
        self.error_count: int = 0
        self.started_at: int = int(time.time())
        self.updated_at: int = int(time.time())
        self.last_appid: int = 0

    def load(self) -> None:
        """Loads state from checkpoint.json if it exists."""
        if not self.checkpoint_path.exists():
            return
        try:
            with open(self.checkpoint_path, "r", encoding="utf-8") as f:
                data = json.load(f)
            self.processed_appids = set(data.get("processed_appids", []))
            self.total_scanned = data.get("total_scanned", len(self.processed_appids))
            self.success_count = data.get("success_count", 0)
            self.new_version_count = data.get("new_version_count", 0)
            self.error_count = data.get("error_count", 0)
            self.started_at = data.get("started_at", int(time.time()))
            self.updated_at = data.get("updated_at", int(time.time()))
            self.last_appid = data.get("last_appid", 0)
            logger.info(
                f"Loaded checkpoint: {len(self.processed_appids)} apps already processed. "
                f"Last AppID: {self.last_appid}"
            )
        except Exception as exc:
            logger.warning(f"Could not load checkpoint from {self.checkpoint_path}: {exc}")

    def is_processed(self, appid: int) -> bool:
        """Checks if appid has already been scanned in this checkpoint session."""
        return int(appid) in self.processed_appids

    def mark_processed(
        self,
        appid: int,
        success: bool = True,
        is_new_version: bool = False,
        is_error: bool = False,
    ) -> None:
        """Records the completion of an app scan."""
        aid = int(appid)
        self.processed_appids.add(aid)
        self.total_scanned += 1
        self.last_appid = aid
        self.updated_at = int(time.time())

        if success:
            self.success_count += 1
            if is_new_version:
                self.new_version_count += 1
        if is_error:
            self.error_count += 1

    def save(self) -> None:
        """Writes checkpoint state atomically to disk."""
        self.checkpoint_path.parent.mkdir(parents=True, exist_ok=True)
        data = {
            "total_scanned": self.total_scanned,
            "success_count": self.success_count,
            "new_version_count": self.new_version_count,
            "error_count": self.error_count,
            "last_appid": self.last_appid,
            "started_at": self.started_at,
            "updated_at": self.updated_at,
            "processed_appids": sorted(list(self.processed_appids)),
        }
        temp_path = self.checkpoint_path.with_suffix(".tmp")
        with open(temp_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)
        os.replace(temp_path, self.checkpoint_path)

    def reset(self) -> None:
        """Clears checkpoint file and resets in-memory tracking."""
        self.processed_appids.clear()
        self.total_scanned = 0
        self.success_count = 0
        self.new_version_count = 0
        self.error_count = 0
        self.started_at = int(time.time())
        self.updated_at = int(time.time())
        self.last_appid = 0
        if self.checkpoint_path.exists():
            self.checkpoint_path.unlink()
        logger.info(f"Checkpoint reset for {self.checkpoint_path}")
