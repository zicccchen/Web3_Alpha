from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime
from typing import Any

try:
    from telethon import TelegramClient
    from telethon.sessions import StringSession
    from telethon.utils import get_peer_id
except ModuleNotFoundError:  # pragma: no cover - exercised only in minimal local test envs.
    TelegramClient = None
    StringSession = None
    get_peer_id = None

from app.config.telegram_watchlists import TelegramChannelConfig, load_telegram_watchlists
from app.core.config import get_settings
from app.core.logging import get_logger
from app.schemas.message import SourceMessage
from app.services.repository import MessageRepository
from app.sources.base import SourceCollector


settings = get_settings()
logger = get_logger(__name__)
COLLECTOR_NAME = "telegram_api"


@dataclass(frozen=True)
class TelegramApiMessage:
    source_chat_id: str
    source_chat_title: str | None
    source_message_id: str
    raw_text: str
    author_name: str | None = None
    message_url: str | None = None
    created_at: datetime | None = None
    is_group: bool = False
    is_channel: bool = False


class TelegramApiCollector(SourceCollector):
    def __init__(self, pipeline, watchlists=None, adapter=None, repository=None) -> None:
        self.pipeline = pipeline
        self.watchlists = watchlists or load_telegram_watchlists()
        self.adapter = adapter or TelethonTelegramAdapter()
        self.repository = repository or MessageRepository()
        self._runner_task: asyncio.Task | None = None
        self._stop_event = asyncio.Event()

    async def start(self) -> None:
        if settings.telegram_source != "api":
            logger.info("telegram api collector disabled by TELEGRAM_SOURCE", extra={"platform": "telegram"})
            return
        if not self.watchlists.deduped_channels:
            logger.warning("no telegram api channels configured", extra={"platform": "telegram"})
            return
        await self.adapter.start()
        self._log_loaded_watchlists()
        self._runner_task = asyncio.create_task(self._run())
        logger.info("telegram api collector started", extra={"platform": "telegram"})

    async def stop(self) -> None:
        self._stop_event.set()
        if self._runner_task:
            await asyncio.gather(self._runner_task, return_exceptions=True)
        await self.adapter.stop()
        logger.info("telegram api collector stopped", extra={"platform": "telegram"})

    async def _run(self) -> None:
        while not self._stop_event.is_set():
            await self.poll_once()
            try:
                await asyncio.wait_for(
                    self._stop_event.wait(),
                    timeout=max(settings.telegram_poll_interval_seconds, 10),
                )
            except asyncio.TimeoutError:
                continue

    async def poll_once(self) -> dict:
        stats = {"channel_count": 0, "new_message_count": 0, "failed_channel_count": 0}
        for channel in self.watchlists.deduped_channels:
            stats["channel_count"] += 1
            try:
                state = await self.repository.get_collector_state(COLLECTOR_NAME, channel.normalized_channel)
                last_message_id = _safe_int(getattr(state, "last_seen_id", None))
                messages = await self.adapter.fetch_messages(
                    channel.channel,
                    after_message_id=last_message_id,
                    limit=max(settings.telegram_fetch_limit, 1),
                )
                max_message_id = last_message_id or 0
                last_seen_time = getattr(state, "last_seen_time", None)
                if not last_message_id:
                    for api_message in messages:
                        message_id = int(api_message.source_message_id)
                        max_message_id = max(max_message_id, message_id)
                        last_seen_time = api_message.created_at or last_seen_time
                    await self.repository.upsert_collector_state(
                        COLLECTOR_NAME,
                        channel.normalized_channel,
                        str(max_message_id) if max_message_id else None,
                        last_seen_time,
                    )
                    logger.info(
                        "telegram api channel cursor initialized without history processing",
                        extra={
                            "platform": "telegram",
                            "channel": channel.channel,
                            "watchlist_category": channel.category,
                            "watchlist_priority": channel.priority,
                            "message_id": str(max_message_id) if max_message_id else None,
                        },
                    )
                    continue
                for api_message in sorted(messages, key=lambda item: int(item.source_message_id)):
                    message_id = int(api_message.source_message_id)
                    if last_message_id and message_id <= last_message_id:
                        continue
                    max_message_id = max(max_message_id, message_id)
                    last_seen_time = api_message.created_at or last_seen_time
                    source_message = source_message_from_telegram_message(api_message, channel)
                    logger.info(
                        "telegram api message received",
                        extra={
                            "platform": "telegram",
                            "channel": source_message.source_chat_title or source_message.source_chat_id,
                            "channel_id": source_message.source_chat_id,
                            "message_id": source_message.source_message_id,
                            "watchlist_category": channel.category,
                            "watchlist_priority": channel.priority,
                        },
                    )
                    await self.pipeline.process(source_message)
                    stats["new_message_count"] += 1
                await self.repository.upsert_collector_state(
                    COLLECTOR_NAME,
                    channel.normalized_channel,
                    str(max_message_id) if max_message_id else getattr(state, "last_seen_id", None),
                    last_seen_time,
                )
            except Exception:
                stats["failed_channel_count"] += 1
                logger.exception(
                    "telegram api polling failed",
                    extra={
                        "platform": "telegram",
                        "channel": channel.channel,
                        "watchlist_category": channel.category,
                        "watchlist_priority": channel.priority,
                    },
                )
        logger.info("telegram api polling round completed", extra={"platform": "telegram", **stats})
        return stats

    def _log_loaded_watchlists(self) -> None:
        logger.info("Loaded telegram watchlists:", extra={"platform": "telegram"})
        for category in self.watchlists.categories:
            logger.info(
                f"- {category.key}: {len(category.channels)} channels, priority={category.priority}",
                extra={
                    "platform": "telegram",
                    "watchlist_category": category.key,
                    "watchlist_priority": category.priority,
                },
            )
        logger.info(
            "telegram api deduped channel count",
            extra={"platform": "telegram", "channel_count": len(self.watchlists.deduped_channels)},
        )


class TelethonTelegramAdapter:
    def __init__(self) -> None:
        self.client: TelegramClient | None = None

    async def start(self) -> None:
        if TelegramClient is None:
            raise RuntimeError("telethon is required when TELEGRAM_SOURCE=api")
        if not settings.telegram_api_id or not settings.telegram_api_hash:
            raise RuntimeError("TELEGRAM_API_ID and TELEGRAM_API_HASH are required when TELEGRAM_SOURCE=api")
        session = StringSession(settings.telegram_session_string) if settings.telegram_session_string else settings.telegram_session_name
        self.client = TelegramClient(session, settings.telegram_api_id, settings.telegram_api_hash)
        await self.client.connect()
        if not await self.client.is_user_authorized():
            raise RuntimeError(
                "Telegram session is not authorized. Login once to create TELEGRAM_SESSION_NAME or provide TELEGRAM_SESSION_STRING."
            )

    async def stop(self) -> None:
        if self.client and self.client.is_connected():
            await self.client.disconnect()

    async def fetch_messages(
        self,
        channel: str,
        after_message_id: int | None,
        limit: int,
    ) -> list[TelegramApiMessage]:
        if not self.client:
            return []
        entity = await self.client.get_entity(channel)
        messages = []
        if after_message_id:
            iterator = self.client.iter_messages(entity, min_id=after_message_id, reverse=True, limit=limit)
        else:
            iterator = self.client.iter_messages(entity, limit=limit)
        async for message in iterator:
            api_message = telegram_api_message_from_telethon(message, entity)
            if api_message:
                messages.append(api_message)
        return messages


def telegram_api_message_from_telethon(message: Any, entity: Any) -> TelegramApiMessage | None:
    raw_text = extract_telegram_raw_text(message)
    if not raw_text.strip():
        return None
    source_chat_id = str(_entity_peer_id(entity) or getattr(entity, "id", "") or getattr(message, "chat_id", ""))
    title = getattr(entity, "title", None) or getattr(entity, "username", None) or source_chat_id
    return TelegramApiMessage(
        source_chat_id=source_chat_id,
        source_chat_title=title,
        source_message_id=str(message.id),
        raw_text=raw_text,
        author_name=_telegram_author_name(message),
        message_url=telegram_message_url(message, entity),
        created_at=getattr(message, "date", None),
        is_group=bool(getattr(entity, "megagroup", False) or getattr(entity, "gigagroup", False)),
        is_channel=bool(getattr(entity, "broadcast", False)),
    )


def source_message_from_telegram_message(
    message: TelegramApiMessage,
    channel: TelegramChannelConfig,
) -> SourceMessage:
    metadata = {
        "original_url": message.message_url,
        "telegram_channel": channel.channel,
        "telegram_is_group": message.is_group,
        "telegram_is_channel": message.is_channel,
    }
    return SourceMessage(
        source="telegram",
        source_chat_id=message.source_chat_id,
        source_chat_title=message.source_chat_title or channel.channel,
        source_message_id=message.source_message_id,
        author_name=message.author_name,
        raw_text=_raw_text_with_original_url(message.raw_text, message.message_url),
        created_at=message.created_at,
        watchlist_category=channel.category,
        watchlist_label=channel.label,
        watchlist_priority=channel.priority,
        metadata=metadata,
    )


def extract_telegram_raw_text(message: Any) -> str:
    parts: list[str] = []
    text = getattr(message, "raw_text", None) or getattr(message, "message", None)
    if text:
        parts.append(str(text).strip())
    media = getattr(message, "media", None)
    if media and not parts:
        caption = getattr(message, "text", None)
        if caption:
            parts.append(str(caption).strip())
    fwd_from = getattr(message, "fwd_from", None)
    if fwd_from:
        from_name = getattr(fwd_from, "from_name", None)
        if from_name:
            parts.append(f"Forwarded from: {from_name}")
    return "\n".join(part for part in parts if part).strip()


def telegram_message_url(message: Any, entity: Any) -> str | None:
    username = getattr(entity, "username", None)
    if username:
        return f"https://t.me/{username}/{message.id}"
    peer_id = str(_entity_peer_id(entity) or "").removeprefix("-100")
    if peer_id:
        return f"https://t.me/c/{peer_id}/{message.id}"
    return None


def _telegram_author_name(message: Any) -> str | None:
    sender = getattr(message, "sender", None)
    if not sender:
        return None
    return (
        getattr(sender, "username", None)
        or getattr(sender, "first_name", None)
        or getattr(sender, "title", None)
    )


def _entity_peer_id(entity: Any) -> int | None:
    if get_peer_id is None:
        return None
    try:
        return get_peer_id(entity)
    except Exception:
        return None


def _raw_text_with_original_url(raw_text: str, original_url: str | None) -> str:
    if not original_url or original_url in raw_text:
        return raw_text
    return f"{raw_text}\n\n原文: {original_url}"


def _safe_int(value: str | int | None) -> int | None:
    if value in {None, ""}:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None
