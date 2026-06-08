from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from app.core.logging import get_logger


logger = get_logger(__name__)
DISCORD_WATCHLISTS_PATH = Path("config/discord_watchlists.yaml")


@dataclass(frozen=True)
class DiscordChannelConfig:
    channel_id: str
    name: str
    type: str


@dataclass(frozen=True)
class DiscordSourceConfig:
    key: str
    label: str
    enabled: bool
    project: str
    ecosystem: str
    priority: int
    channels: tuple[DiscordChannelConfig, ...]


@dataclass(frozen=True)
class DiscordWatchlists:
    sources: tuple[DiscordSourceConfig, ...]

    @property
    def enabled_sources(self) -> tuple[DiscordSourceConfig, ...]:
        return tuple(source for source in self.sources if source.enabled)

    @property
    def enabled_channels(self) -> tuple[tuple[DiscordSourceConfig, DiscordChannelConfig], ...]:
        return tuple(
            (source, channel)
            for source in self.enabled_sources
            for channel in source.channels
            if channel.channel_id
        )

    def source_for_channel(self, channel_id: str) -> tuple[DiscordSourceConfig, DiscordChannelConfig] | None:
        normalized = str(channel_id).strip()
        for source, channel in self.enabled_channels:
            if channel.channel_id == normalized:
                return source, channel
        return None


def load_discord_watchlists(path: Path = DISCORD_WATCHLISTS_PATH) -> DiscordWatchlists:
    if not path.exists():
        logger.warning("discord watchlists config file not found", extra={"path": str(path)})
        return DiscordWatchlists(sources=())

    payload = _load_yaml_mapping(path)
    raw_watchlists = payload.get("discord_watchlists", {}) if isinstance(payload, dict) else {}
    if not isinstance(raw_watchlists, dict):
        raise ValueError("discord_watchlists.yaml must contain a mapping named 'discord_watchlists'")

    sources = []
    for key, raw_source in raw_watchlists.items():
        if not isinstance(raw_source, dict):
            continue
        channels = []
        raw_channels = raw_source.get("channels", [])
        if isinstance(raw_channels, list):
            for raw_channel in raw_channels:
                if not isinstance(raw_channel, dict):
                    continue
                channel_id = str(raw_channel.get("channel_id") or "").strip()
                if not channel_id or channel_id.startswith("填"):
                    continue
                channels.append(
                    DiscordChannelConfig(
                        channel_id=channel_id,
                        name=str(raw_channel.get("name") or channel_id),
                        type=str(raw_channel.get("type") or "announcement"),
                    )
                )
        sources.append(
            DiscordSourceConfig(
                key=str(key),
                label=str(raw_source.get("label") or key),
                enabled=bool(raw_source.get("enabled", False)),
                project=str(raw_source.get("project") or key),
                ecosystem=str(raw_source.get("ecosystem") or ""),
                priority=int(raw_source.get("priority") or 0),
                channels=tuple(channels),
            )
        )

    return DiscordWatchlists(sources=tuple(sources))


def _load_yaml_mapping(path: Path) -> dict[str, Any]:
    try:
        import yaml

        payload = yaml.safe_load(path.read_text(encoding="utf-8"))
        return payload if isinstance(payload, dict) else {}
    except ModuleNotFoundError:
        return _parse_simple_yaml(path.read_text(encoding="utf-8"))


def _parse_simple_yaml(content: str) -> dict[str, Any]:
    try:
        import json

        return json.loads(content)
    except Exception as exc:
        raise RuntimeError("PyYAML is required to parse discord_watchlists.yaml") from exc
