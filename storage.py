#!/usr/bin/env python3
"""
storage.py - Storage Manager for Steam Metadata & Depot Manifests
Features:
- Modulo sharding (appid % 1000) for clean directory structures (max ~180 files per folder).
- Hybrid Full-Raw + Append-Only Architecture:
  Preserves 100% of the raw Steam PICS data (common, config, launch executables, ufs cloud save, depots, extended)
  while continuously accumulating historical versions in '_history'.
- Atomic writes via temp files to avoid corruption on process interrupt.
- O(1) path resolution for Launcher / Depot Downloader client access.
"""

from __future__ import annotations

import json
import logging
import os
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger("steam_tracker.storage")

# Maximum number of version entries kept per app in `_history`.
#
# The launcher only reads the NEWEST entry for a branch (the public build plus
# the manifest set it points at). Keeping every version a game has ever shipped
# made each shard JSON grow without bound: a single popular title accumulated
# dozens of near-duplicate manifest maps, which is what made a git clone of this
# balloon into tens of GB while the downloadable zip stayed small (the zip
# only carries one snapshot, the clone carries every blob in history).
#
# Trimming here keeps the published artifact useful while bounding both the
# working-tree size and the per-commit churn that Actions pushes every run.
MAX_HISTORY_ENTRIES_PER_APP = 5


class MetadataStorage:
    def __init__(self, base_dir: Path):
        self.base_dir = Path(base_dir)
        self.data_dir = self.base_dir / "data"
        self.data_dir.mkdir(parents=True, exist_ok=True)

    def get_shard_id(self, appid: int) -> str:
        """Returns 3-digit zero-padded shard ID (000 - 999)."""
        return f"{int(appid) % 1000:03d}"

    def get_file_path(self, appid: int) -> Path:
        """Returns the full path to data/{shard_id}/{appid}.json."""
        shard = self.get_shard_id(appid)
        return self.data_dir / shard / f"{appid}.json"

    def load_app_metadata(self, appid: int) -> Optional[Dict[str, Any]]:
        """Loads existing app metadata JSON if present."""
        path = self.get_file_path(appid)
        if not path.exists():
            return None
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as exc:
            logger.warning(f"Error reading existing file for app {appid}: {exc}")
            return None

    def parse_steamcmd_info(self, appid: int, raw_info: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Extracts branches, buildids, and depot manifests from raw Steam PICS data.
        """
        if not raw_info or not isinstance(raw_info, dict):
            return None

        depots = raw_info.get("depots", {})
        if not isinstance(depots, dict):
            depots = {}

        branches_raw = depots.get("branches", {})
        if not isinstance(branches_raw, dict):
            branches_raw = {}

        current_branches: Dict[str, Dict[str, Any]] = {}
        for branch_name, branch_data in branches_raw.items():
            if isinstance(branch_data, dict):
                build_id = str(branch_data.get("buildid", ""))
                time_updated = branch_data.get("timeupdated")
                if time_updated is not None:
                    try:
                        time_updated = int(time_updated)
                    except (ValueError, TypeError):
                        time_updated = None

                current_branches[branch_name] = {
                    "buildId": build_id,
                    "timeUpdated": time_updated,
                }

        # Extract manifests grouped by branch:
        # depots: { depot_id: { "manifests": { branch: { "gid": "...", "size": "..." } } } }
        branch_manifests: Dict[str, Dict[str, Dict[str, Any]]] = {}
        for depot_id_str, depot_obj in depots.items():
            if not depot_id_str.isdigit() or not isinstance(depot_obj, dict):
                continue

            manifests_map = depot_obj.get("manifests", {})
            if not isinstance(manifests_map, dict):
                continue

            for b_name, m_data in manifests_map.items():
                if b_name not in branch_manifests:
                    branch_manifests[b_name] = {}

                gid = None
                size = None
                download = None

                if isinstance(m_data, dict):
                    gid = str(m_data.get("gid", ""))
                    if m_data.get("size") is not None:
                        try:
                            size = int(m_data["size"])
                        except (ValueError, TypeError):
                            size = None
                    if m_data.get("download") is not None:
                        try:
                            download = int(m_data["download"])
                        except (ValueError, TypeError):
                            download = None
                elif isinstance(m_data, (str, int)):
                    gid = str(m_data)

                if gid:
                    branch_manifests[b_name][depot_id_str] = {
                        "gid": gid,
                        "size": size,
                        "download": download,
                    }

        return {
            "currentBranches": current_branches,
            "branchManifests": branch_manifests,
        }

    def save_app_record(
        self,
        appid: int,
        raw_info: Dict[str, Any],
        fallback_name: str = "",
    ) -> Tuple[bool, bool]:
        """
        Saves full raw data + append-only history.
        Returns: (success: bool, is_new_version: bool)
        """
        now_ts = int(time.time())
        existing = self.load_app_metadata(appid)

        # Retrieve any previously saved history (supports both '_history' and 'history')
        existing_history: List[Dict[str, Any]] = []
        if existing and isinstance(existing, dict):
            existing_history = existing.get("_history") or existing.get("history") or []

        parsed = self.parse_steamcmd_info(appid, raw_info)
        is_new_version = False
        history: List[Dict[str, Any]] = list(existing_history)

        if parsed:
            branch_manifests = parsed.get("branchManifests", {})
            current_branches = parsed.get("currentBranches", {})

            for branch_name, b_info in current_branches.items():
                build_id = b_info.get("buildId")
                if not build_id:
                    continue

                manifests_for_branch = branch_manifests.get(branch_name, {})

                # Check if this (branch, buildId) already exists in history
                existing_entry = None
                for entry in history:
                    if entry.get("branch") == branch_name and str(entry.get("buildId")) == str(build_id):
                        existing_entry = entry
                        break

                if existing_entry is not None:
                    # Check for missing manifests and add without overwriting
                    entry_manifests = existing_entry.setdefault("manifests", {})
                    for dep_id, dep_data in manifests_for_branch.items():
                        if dep_id not in entry_manifests:
                            entry_manifests[dep_id] = dep_data
                            is_new_version = True
                else:
                    # New version detected! Append to history
                    new_entry = {
                        "buildId": str(build_id),
                        "branch": branch_name,
                        "timeUpdated": b_info.get("timeUpdated"),
                        "firstSeen": now_ts,
                        "manifests": manifests_for_branch,
                    }
                    history.append(new_entry)
                    is_new_version = True
        # Keep only the most recent entries per branch so shard files stay bounded.
        if len(history) > MAX_HISTORY_ENTRIES_PER_APP:
            by_branch: Dict[str, List[Dict[str, Any]]] = {}
            for entry in history:
                by_branch.setdefault(str(entry.get("branch", "")), []).append(entry)
            trimmed: List[Dict[str, Any]] = []
            for branch_entries in by_branch.values():
                # `timeUpdated` is a Steam supplied epoch; fall back to firstSeen.
                def _recency(item: Dict[str, Any]) -> int:
                    try:
                        return int(item.get("timeUpdated") or item.get("firstSeen") or 0)
                    except (TypeError, ValueError):
                        return 0
                branch_entries.sort(key=_recency, reverse=True)
                trimmed.extend(branch_entries[:MAX_HISTORY_ENTRIES_PER_APP])
            history = trimmed

        # Build output dictionary containing full Raw data + metadata extensions
        output_data: Dict[str, Any] = {
            "_lastChecked": now_ts,
            "_history": history,
        }

        if raw_info and isinstance(raw_info, dict):
            # Include 100% of raw Steam PICS data
            for k, v in raw_info.items():
                output_data[k] = v
        else:
            output_data["appid"] = int(appid)
            if fallback_name:
                output_data["name"] = fallback_name

        # Ensure fallback name is present if common.name is missing
        if "common" in output_data and isinstance(output_data["common"], dict):
            if not output_data["common"].get("name") and fallback_name:
                output_data["common"]["name"] = fallback_name
        elif fallback_name and "name" not in output_data:
            output_data["name"] = fallback_name

        self._write_atomically(appid, output_data)
        return True, is_new_version

    def _write_atomically(self, appid: int, data: Dict[str, Any]) -> None:
        """Writes data to a temporary file, then renames to target for atomicity."""
        target_path = self.get_file_path(appid)
        target_path.parent.mkdir(parents=True, exist_ok=True)

        temp_path = target_path.with_suffix(".tmp")
        with open(temp_path, "w", encoding="utf-8") as f:
            # Compact separators: these files are machine-readable mirrors, never
            # hand-edited. Pretty printing roughly doubled every shard file for no
            # benefit and inflated the repo on every commit.
            json.dump(data, f, indent=2, ensure_ascii=False)`n            f.write("`n")

        os.replace(temp_path, target_path)

    def get_stats(self) -> Dict[str, Any]:
        """Calculates total files, shards, and version counts."""
        total_files = 0
        shards_used = set()

        if self.data_dir.exists():
            for shard_entry in self.data_dir.iterdir():
                if shard_entry.is_dir():
                    json_files = list(shard_entry.glob("*.json"))
                    if json_files:
                        shards_used.add(shard_entry.name)
                        total_files += len(json_files)

        return {
            "total_files": total_files,
            "shards_count": len(shards_used),
            "data_directory": str(self.data_dir),
        }
