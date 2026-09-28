#!/usr/bin/env python3
"""
hubcap_provider.py - Hubcap Manifest API integration
Sử dụng các endpoint FREE (no usage count) để phát hiện app update
và lấy depot_id / manifest_id mà không cần query Steam.

Docs: https://hubcapmanifest.com
"""

from __future__ import annotations

import logging
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import httpx

logger = logging.getLogger("steam_tracker.hubcap")

BASE_URL = "https://hubcapmanifest.com/api/v1"
_BATCH_SIZE = 5000   # POST /library hỗ trợ tối đa 5000 app_ids


class HubcapProvider:
    def __init__(self, api_key: str, timeout: float = 30.0):
        self._headers = {
            "Authorization": f"Bearer {api_key}",
            "User-Agent": "steam-metadata-tracker/1.0",
        }
        self._client = httpx.Client(
            timeout=timeout,
            headers=self._headers,
            follow_redirects=True,
        )

    # ── helpers ────────────────────────────────────────────────────────────

    def _iso_to_ts(self, iso: str) -> int:
        """Convert ISO 8601 string (possibly without tz) to UTC epoch int."""
        if not iso:
            return 0
        try:
            dt = datetime.fromisoformat(iso)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return int(dt.timestamp())
        except Exception:
            return 0

    # ── public API ─────────────────────────────────────────────────────────

    def health(self) -> bool:
        """True nếu Hubcap API đang hoạt động bình thường. FREE."""
        try:
            resp = self._client.get(f"{BASE_URL}/health", timeout=10)
            return resp.status_code == 200 and resp.json().get("status") == "healthy"
        except Exception:
            return False

    def batch_library(self, app_ids: List[int]) -> Dict[int, Dict[str, Any]]:
        """
        POST /api/v1/library với tối đa 5000 app_ids một lần.
        FREE - no usage count.
        Trả về dict: app_id (int) -> game_info dict.
        game_info keys hữu ích:
          game_name, manifest_updated (ISO str), manifest_available (bool)
        """
        result: Dict[int, Dict[str, Any]] = {}
        if not app_ids:
            return result

        # Chia batch nếu vượt giới hạn
        for i in range(0, len(app_ids), _BATCH_SIZE):
            chunk = app_ids[i : i + _BATCH_SIZE]
            try:
                resp = self._client.post(
                    f"{BASE_URL}/library",
                    json={"app_ids": chunk, "limit": len(chunk), "include_adult": True},
                )
                if resp.status_code != 200:
                    logger.warning(f"Hubcap library batch returned HTTP {resp.status_code}")
                    continue
                for g in resp.json().get("games", []):
                    try:
                        result[int(g["game_id"])] = g
                    except (KeyError, ValueError):
                        pass
            except Exception as exc:
                logger.warning(f"Hubcap batch_library error: {exc}")

        return result

    def manifest_contents(self, app_id: int) -> Optional[Dict[str, Any]]:
        """
        GET /api/v1/manifest/{app_id}/contents
        Lấy danh sách depot_id + manifest_id KHÔNG cần download.
        FREE - no usage count.

        Trả về:
          {
            "branch": "public",
            "manifests": [
              {"depot_id": "228981", "manifest_id": "9876543210987654321", ...}
            ]
          }
        hay None nếu app không có trong Hubcap.
        """
        try:
            resp = self._client.get(f"{BASE_URL}/manifest/{app_id}/contents")
            if resp.status_code == 404:
                return None
            if resp.status_code != 200:
                logger.warning(
                    f"Hubcap manifest contents for {app_id}: HTTP {resp.status_code}"
                )
                return None
            return resp.json()
        except Exception as exc:
            logger.warning(f"Hubcap manifest_contents({app_id}): {exc}")
            return None

    def depot_key_ids(self) -> List[str]:
        """
        GET /api/v1/depot-keys
        Trả về list depot_id có trong Hubcap (chỉ ID, không có key thật).
        FREE - no usage count.
        """
        try:
            resp = self._client.get(f"{BASE_URL}/depot-keys")
            if resp.status_code != 200:
                return []
            return resp.json().get("depot_ids", [])
        except Exception as exc:
            logger.warning(f"Hubcap depot_key_ids: {exc}")
            return []

    def find_updated_apps(
        self,
        app_ids: List[int],
        last_checked_map: Dict[int, int],
    ) -> List[int]:
        """
        So sánh manifest_updated từ Hubcap với _lastChecked đã lưu.
        Trả về list app_ids thực sự có update mới trên Hubcap.

        app_ids          : danh sách app cần kiểm tra
        last_checked_map : dict app_id -> epoch int lần cuối mình check
        """
        if not app_ids:
            return []

        library = self.batch_library(app_ids)
        updated: List[int] = []

        for aid in app_ids:
            info = library.get(aid)
            if info is None:
                # Hubcap chưa có app này → fallback sang Steam
                updated.append(aid)
                continue
            if not info.get("manifest_available", False):
                continue

            hubcap_ts = self._iso_to_ts(info.get("manifest_updated", ""))
            our_ts = last_checked_map.get(aid, 0)

            if hubcap_ts > our_ts:
                updated.append(aid)

        logger.info(
            f"Hubcap check: {len(updated)}/{len(app_ids)} apps need update"
        )
        return updated

    def get_manifest_info_for_storage(
        self, app_id: int
    ) -> Optional[Dict[str, Any]]:
        """
        Lấy depot_id + manifest_id từ Hubcap và format theo cấu trúc
        _history entry của tracker:
          {
            "buildId": "",          # Hubcap không có buildId
            "branch": "public",
            "timeUpdated": <epoch>,
            "firstSeen": <now>,
            "manifests": {
              "depot_id": {"gid": "manifest_id", "size": None, "download": None}
            },
            "_source": "hubcap"
          }
        Trả về None nếu không có dữ liệu.
        """
        contents = self.manifest_contents(app_id)
        if not contents:
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
            "buildId": "",          # Hubcap không cung cấp buildId
            "branch": contents.get("branch", "public"),
            "timeUpdated": self._iso_to_ts(
                contents.get("last_modified", "")
            ),
            "firstSeen": int(time.time()),
            "manifests": manifests,
            "_source": "hubcap",
        }
