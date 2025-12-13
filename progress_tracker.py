"""Lightweight MongoDB-based progress tracker used by scrapers.

The tracker stores per-task cursors in a dedicated collection so a crashed
container or process can resume from the last completed unit of work.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, Optional

from pymongo import MongoClient


class MongoProgressTracker:
    """Minimal helper to persist scraper progress in MongoDB."""

    def __init__(
        self,
        mongo_host: str,
        mongo_user: str,
        mongo_password: str,
        mongo_port: int,
        mongo_db: str,
        collection: str = "scrape_progress",
    ) -> None:
        uri = f"mongodb://{mongo_user}:{mongo_password}@{mongo_host}:{mongo_port}/?authSource=admin"
        self.client = MongoClient(uri)
        self.col = self.client[mongo_db][collection]
        # Ensure single record per task
        self.col.create_index("task", unique=True)

    def load(self, task: str) -> Optional[Dict[str, Any]]:
        return self.col.find_one({"task": task})

    def start(self, task: str, total_items: int, meta: Optional[Dict[str, Any]] = None) -> None:
        now = datetime.now(timezone.utc)
        doc = {
            "task": task,
            "status": "running",
            "cursor": 0,
            "total_items": total_items,
            "meta": meta or {},
            "summary": {},
            "started_at": now,
            "updated_at": now,
        }
        self.col.update_one(
            {"task": task},
            {"$set": doc, "$setOnInsert": {"created_at": now}},
            upsert=True,
        )

    def update(self, task: str, cursor: int, summary: Dict[str, Any]) -> None:
        now = datetime.now(timezone.utc)
        self.col.update_one(
            {"task": task},
            {"$set": {"cursor": cursor, "summary": summary, "updated_at": now, "status": "running"}},
        )

    def complete(self, task: str, cursor: int, summary: Dict[str, Any]) -> None:
        now = datetime.now(timezone.utc)
        self.col.update_one(
            {"task": task},
            {
                "$set": {
                    "cursor": cursor,
                    "summary": summary,
                    "status": "completed",
                    "updated_at": now,
                    "finished_at": now,
                }
            },
        )

    def clear(self, task: str) -> None:
        self.col.delete_one({"task": task})
