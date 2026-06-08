from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlparse

import yaml

from app.core.logging import get_logger


logger = get_logger(__name__)
DEFAULT_TELEGRAM_WATCHLIST_PATH = Path("config/telegram_watchlists.yaml")


@dataclass(frozen=True)
class TelegramChannelConfig:
    channel: str
    category: str
    label: str
    priority: int

    @property
    def normalized_channel(self) -> str:
        return normalize_telegram_channel(self.channel)


@dataclass(frozen=True)
class TelegramWatchlistCategory:
    key: str
    label: str
    priority: int
    channels: tuple[str, ...]


class TelegramWatchlists:
    def __init__(self, categories: list[TelegramWatchlistCategory]) -> None:
        self.categories = categories
        self.channels_by_normalized: dict[str, TelegramChannelConfig] = {}
        for category in categories:
            for channel in category.channels:
                normalized = normalize_telegram_channel(channel)
                if not normalized:
                    continue
                existing = self.channels_by_normalized.get(normalized)
                if existing and existing.priority >= category.priority:
                    continue
                self.channels_by_normalized[normalized] = TelegramChannelConfig(
                    channel=channel,
                    category=category.key,
                    label=category.label,
                    priority=category.priority,
                )

    @property
    def deduped_channels(self) -> list[TelegramChannelConfig]:
        return sorted(
            self.channels_by_normalized.values(),
            key=lambda item: (-item.priority, item.category, item.normalized_channel),
        )

    def match_channel(self, channel: str) -> TelegramChannelConfig | None:
        return self.channels_by_normalized.get(normalize_telegram_channel(channel))


def load_telegram_watchlists(path: Path | str = DEFAULT_TELEGRAM_WATCHLIST_PATH) -> TelegramWatchlists:
    config_path = Path(path)
    if not config_path.exists():
        logger.warning("telegram watchlists config missing", extra={"path": str(config_path)})
        return TelegramWatchlists([])

    payload = yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}
    raw_watchlists = payload.get("telegram_watchlists") or {}
    categories: list[TelegramWatchlistCategory] = []
    for key, raw_category in raw_watchlists.items():
        if not isinstance(raw_category, dict):
            continue
        channels = raw_category.get("channels") or []
        categories.append(
            TelegramWatchlistCategory(
                key=str(key),
                label=str(raw_category.get("label") or key),
                priority=int(raw_category.get("priority") or 0),
                channels=tuple(str(channel).strip() for channel in channels if str(channel).strip()),
            )
        )

    return TelegramWatchlists(categories)


def normalize_telegram_channel(value: str | int | None) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    if not text:
        return ""
    if text.startswith("http://") or text.startswith("https://"):
        parts = [part for part in urlparse(text).path.split("/") if part]
        if parts and parts[0] == "s" and len(parts) > 1:
            text = parts[1]
        elif parts:
            text = parts[0]
    if text.startswith("@"):
        text = text[1:]
    return text.strip().lower()
