from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from app.core.logging import get_logger


WATCHLISTS_PATH = Path("config/watchlists.yaml")
logger = get_logger(__name__)


@dataclass(frozen=True)
class WatchlistCategory:
    key: str
    label: str
    priority: int
    description: str
    accounts: tuple[str, ...]


@dataclass(frozen=True)
class WatchlistMatch:
    category: str
    label: str
    priority: int


@dataclass(frozen=True)
class Watchlists:
    categories: tuple[WatchlistCategory, ...]
    accounts_by_normalized: dict[str, str]
    matches_by_normalized: dict[str, WatchlistMatch]

    @property
    def deduped_accounts(self) -> list[str]:
        return [self.accounts_by_normalized[key] for key in self.accounts_by_normalized]

    def match_account(self, account: str | None) -> WatchlistMatch | None:
        if not account:
            return None
        return self.matches_by_normalized.get(normalize_account(account))


def load_watchlists(path: Path = WATCHLISTS_PATH) -> Watchlists:
    if not path.exists():
        logger.warning("watchlists config file not found", extra={"path": str(path)})
        return Watchlists(categories=(), accounts_by_normalized={}, matches_by_normalized={})

    payload = _load_yaml(path)
    raw_watchlists = payload.get("watchlists", {}) if isinstance(payload, dict) else {}
    if not isinstance(raw_watchlists, dict):
        raise ValueError("watchlists.yaml must contain a mapping named 'watchlists'")

    categories: list[WatchlistCategory] = []
    accounts_by_normalized: dict[str, str] = {}
    matches_by_normalized: dict[str, WatchlistMatch] = {}

    for key, raw_category in raw_watchlists.items():
        if not isinstance(raw_category, dict):
            continue
        accounts = tuple(
            account.strip().lstrip("@")
            for account in raw_category.get("accounts", [])
            if isinstance(account, str) and account.strip()
        )
        category = WatchlistCategory(
            key=str(key),
            label=str(raw_category.get("label") or key),
            priority=int(raw_category.get("priority") or 0),
            description=str(raw_category.get("description") or ""),
            accounts=accounts,
        )
        categories.append(category)

        for account in accounts:
            normalized = normalize_account(account)
            accounts_by_normalized.setdefault(normalized, account)
            current = matches_by_normalized.get(normalized)
            if current is None or category.priority > current.priority:
                matches_by_normalized[normalized] = WatchlistMatch(
                    category=category.key,
                    label=category.label,
                    priority=category.priority,
                )

    return Watchlists(
        categories=tuple(categories),
        accounts_by_normalized=accounts_by_normalized,
        matches_by_normalized=matches_by_normalized,
    )


def normalize_account(account: str) -> str:
    return account.strip().lstrip("@").lower()


def _load_yaml(path: Path) -> dict[str, Any]:
    import yaml

    payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    return payload if isinstance(payload, dict) else {}
