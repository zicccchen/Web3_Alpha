from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import httpx

from app.config.discord_watchlists import DiscordChannelConfig, DiscordSourceConfig, load_discord_watchlists
from app.core.config import get_settings
from app.core.logging import get_logger
from app.schemas.message import SourceMessage
from app.sources.base import SourceCollector


settings = get_settings()
logger = get_logger(__name__)
DISCORD_API_BASE = "https://discord.com/api/v10"


@dataclass(frozen=True)
class DiscordMessage:
    message_id: str
    channel_id: str
    channel_name: str
    channel_type: str
    project: str
    ecosystem: str
    priority: int
    raw_text: str
    author_name: str | None = None
    created_at: datetime | None = None
    original_url: str | None = None


class DiscordCollector(SourceCollector):
    def __init__(self, pipeline, watchlists=None, adapter=None) -> None:
        self.pipeline = pipeline
        self.watchlists = watchlists or load_discord_watchlists()
        self.adapter = adapter or DiscordSessionAdapter()
        self._runner_task: asyncio.Task | None = None
        self._stop_event = asyncio.Event()
        self._last_seen_message_ids: dict[str, int] = {}

    async def start(self) -> None:
        if not settings.discord_enabled:
            logger.info("discord collector disabled", extra={"platform": "discord"})
            return
        if not self.watchlists.enabled_channels:
            logger.warning("no enabled discord channels configured", extra={"platform": "discord"})
            return
        await self.adapter.start()
        self._log_loaded_watchlists()
        self._runner_task = asyncio.create_task(self._run())
        logger.info("discord collector started", extra={"platform": "discord"})

    async def stop(self) -> None:
        self._stop_event.set()
        if self._runner_task:
            await asyncio.gather(self._runner_task, return_exceptions=True)
        await self.adapter.stop()
        logger.info("discord collector stopped", extra={"platform": "discord"})

    async def _run(self) -> None:
        while not self._stop_event.is_set():
            await self.poll_once()
            try:
                await asyncio.wait_for(
                    self._stop_event.wait(),
                    timeout=max(settings.discord_poll_interval_seconds, 10),
                )
            except asyncio.TimeoutError:
                continue

    async def poll_once(self) -> dict:
        stats = {"channel_count": 0, "new_message_count": 0, "failed_channel_count": 0}
        for source, channel in self.watchlists.enabled_channels:
            stats["channel_count"] += 1
            try:
                messages = await self.adapter.fetch_messages(
                    channel.channel_id,
                    after_message_id=self._last_seen_message_ids.get(channel.channel_id),
                )
                for payload in sorted(messages, key=lambda item: int(item.get("id", 0))):
                    message_id = int(payload["id"])
                    if message_id <= self._last_seen_message_ids.get(channel.channel_id, 0):
                        continue
                    self._last_seen_message_ids[channel.channel_id] = message_id
                    discord_message = discord_message_from_payload(payload, source, channel)
                    if not discord_message:
                        continue
                    logger.info(
                        "discord message received",
                        extra={
                            "platform": "discord",
                            "project": source.project,
                            "ecosystem": source.ecosystem,
                            "channel_id": channel.channel_id,
                            "channel_name": channel.name,
                            "message_id": discord_message.message_id,
                        },
                    )
                    await self.pipeline.process(source_message_from_discord_message(discord_message))
                    stats["new_message_count"] += 1
            except Exception:
                stats["failed_channel_count"] += 1
                logger.exception(
                    "discord polling failed",
                    extra={
                        "platform": "discord",
                        "project": source.project,
                        "ecosystem": source.ecosystem,
                        "channel_id": channel.channel_id,
                        "channel_name": channel.name,
                    },
                )
        logger.info("discord polling round completed", extra={"platform": "discord", **stats})
        return stats

    def _log_loaded_watchlists(self) -> None:
        logger.info("Loaded discord watchlists:", extra={"platform": "discord"})
        for source in self.watchlists.sources:
            logger.info(
                f"- {source.key}: {len(source.channels)} channels, enabled={source.enabled}, priority={source.priority}",
                extra={
                    "platform": "discord",
                    "project": source.project,
                    "ecosystem": source.ecosystem,
                    "watchlist_priority": source.priority,
                },
            )


class DiscordSessionAdapter:
    def __init__(self) -> None:
        self._client: httpx.AsyncClient | None = None

    async def start(self) -> None:
        if settings.discord_mode != "session":
            raise RuntimeError(f"unsupported DISCORD_MODE={settings.discord_mode}")
        if not settings.discord_session_token:
            raise RuntimeError("DISCORD_SESSION_TOKEN is not configured")
        self._client = httpx.AsyncClient(
            timeout=httpx.Timeout(float(settings.discord_request_timeout_seconds)),
            headers={
                "Authorization": settings.discord_session_token,
                "User-Agent": settings.discord_user_agent or "Mozilla/5.0 Web3Alpha/1.0",
            },
        )

    async def stop(self) -> None:
        if self._client:
            await self._client.aclose()

    async def fetch_messages(self, channel_id: str, after_message_id: int | None = None) -> list[dict[str, Any]]:
        if not self._client:
            return []
        params: dict[str, str | int] = {"limit": 50}
        if after_message_id:
            params["after"] = str(after_message_id)
        response = await self._client.get(f"{DISCORD_API_BASE}/channels/{channel_id}/messages", params=params)
        response.raise_for_status()
        payload = response.json()
        return payload if isinstance(payload, list) else []


def discord_message_from_payload(
    payload: dict[str, Any],
    source: DiscordSourceConfig,
    channel: DiscordChannelConfig,
) -> DiscordMessage | None:
    raw_text = extract_raw_text(payload)
    if not raw_text.strip():
        return None
    author = payload.get("author") if isinstance(payload.get("author"), dict) else {}
    message_id = str(payload["id"])
    created_at = parse_discord_timestamp(payload.get("timestamp"))
    return DiscordMessage(
        message_id=message_id,
        channel_id=channel.channel_id,
        channel_name=channel.name,
        channel_type=channel.type,
        project=source.project,
        ecosystem=source.ecosystem,
        priority=source.priority,
        raw_text=raw_text,
        author_name=author.get("global_name") or author.get("username"),
        created_at=created_at,
        original_url=discord_message_url(payload, channel.channel_id),
    )


def source_message_from_discord_message(message: DiscordMessage) -> SourceMessage:
    metadata = {
        "project": message.project,
        "ecosystem": message.ecosystem,
        "discord_channel_id": message.channel_id,
        "discord_channel_type": message.channel_type,
        "watchlist_priority": message.priority,
        "original_url": message.original_url,
    }
    return SourceMessage(
        source="discord",
        source_chat_id=message.channel_id,
        source_chat_title=f"{message.project}:{message.channel_name}",
        source_message_id=message.message_id,
        author_name=message.author_name,
        raw_text=_raw_text_with_original_url(message.raw_text, message.original_url),
        created_at=message.created_at,
        watchlist_category=message.project.lower(),
        watchlist_label=f"{message.project} Discord",
        watchlist_priority=message.priority,
        metadata=metadata,
    )


def extract_raw_text(payload: dict[str, Any]) -> str:
    parts: list[str] = []
    content = str(payload.get("content") or "").strip()
    if content:
        parts.append(content)

    attachments = payload.get("attachments") if isinstance(payload.get("attachments"), list) else []
    for attachment in attachments:
        if isinstance(attachment, dict) and (attachment.get("url") or attachment.get("proxy_url")):
            parts.append(str(attachment.get("url") or attachment.get("proxy_url")))

    embeds = payload.get("embeds") if isinstance(payload.get("embeds"), list) else []
    for embed in embeds:
        if not isinstance(embed, dict):
            continue
        for key in ("title", "description", "url"):
            value = embed.get(key)
            if value:
                parts.append(str(value))

    return "\n".join(parts).strip()


def parse_discord_timestamp(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def discord_message_url(payload: dict[str, Any], channel_id: str) -> str | None:
    guild_id = payload.get("guild_id") or "@me"
    message_id = payload.get("id")
    if not message_id:
        return None
    return f"https://discord.com/channels/{guild_id}/{channel_id}/{message_id}"


def _raw_text_with_original_url(raw_text: str, original_url: str | None) -> str:
    if not original_url or original_url in raw_text:
        return raw_text
    return f"{raw_text}\n{original_url}"
