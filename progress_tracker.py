"""Crawl history helper: store successful URLs/combinations with last crawl time.

Used to skip items that已在24小时内抓取过。
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional

from pymongo import MongoClient


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _as_aware(dt: datetime) -> datetime:
    """Ensure datetime is timezone-aware (UTC)."""
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


class MongoCrawlHistory:
    """Persist crawl history so recent URLs can be skipped."""

    def __init__(
        self,
        mongo_host: str,
        mongo_user: str,
        mongo_password: str,
        mongo_port: int,
        mongo_db: str,
        collection: str = "crawl_history",
    ) -> None:
        uri = f"mongodb://{mongo_user}:{mongo_password}@{mongo_host}:{mongo_port}/?authSource=admin"
        self.client = MongoClient(uri)
        self.col = self.client[mongo_db][collection]
        self.col.create_index([("task", 1), ("key", 1)], unique=True)

    def should_crawl(self, task: str, key: str, freshness_hours: int = 24) -> bool:
        doc = self.col.find_one({"task": task, "key": key}, {"last_crawled": 1})
        if not doc or "last_crawled" not in doc:
            return True
        last_ts: datetime = _as_aware(doc["last_crawled"])
        return last_ts < _utc_now() - timedelta(hours=freshness_hours)

    def mark_crawled(self, task: str, key: str, meta: Optional[Dict[str, Any]] = None) -> None:
        now = _utc_now()
        self.col.update_one(
            {"task": task, "key": key},
            {
                "$set": {
                    "last_crawled": now,
                    "meta": meta or {},
                    "updated_at": now,
                },
                "$setOnInsert": {"created_at": now},
            },
            upsert=True,
        )
