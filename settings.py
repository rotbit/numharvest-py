#!/usr/bin/env python3
"""Lightweight config dataclasses for Mongo/PostgreSQL connections."""
import os
from dataclasses import dataclass, field


def _env_int(name: str, default: int) -> int:
    """Read an integer environment variable safely, falling back on bad values."""
    try:
        return int(os.getenv(name, default))
    except (TypeError, ValueError):
        return default


@dataclass(frozen=True)
class MongoSettings:
    host: str = field(default_factory=lambda: os.getenv("MONGO_HOST", "43.159.58.235"))
    user: str = field(default_factory=lambda: os.getenv("MONGO_USER", "extra_numbers"))
    password: str = field(default_factory=lambda: os.getenv("MONGO_PASSWORD", "RsBWd3hTAZeR7kC4"))
    port: int = field(default_factory=lambda: _env_int("MONGO_PORT", 27017))
    db: str = field(default_factory=lambda: os.getenv("MONGO_DB", "extra_numbers"))
    collection: str = field(default_factory=lambda: os.getenv("MONGO_COLLECTION", "numbers"))

    def uri(self, auth_source: str = "extra_numbers") -> str:
        return f"mongodb://{self.user}:{self.password}@{self.host}:{self.port}/?authSource={auth_source}"


@dataclass(frozen=True)
class MysqlSettings:
    host: str = field(default_factory=lambda: os.getenv("MYSQL_HOST", "43.159.58.235"))
    port: int = field(default_factory=lambda: _env_int("MYSQL_PORT", 3306))
    db: str = field(default_factory=lambda: os.getenv("MYSQL_DB", "numbers"))
    user: str = field(default_factory=lambda: os.getenv("MYSQL_USER", "root"))
    password: str = field(default_factory=lambda: os.getenv("MYSQL_PASSWORD", "axad3M3MJN57NWzr"))
