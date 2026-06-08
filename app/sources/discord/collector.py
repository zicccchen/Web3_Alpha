import asyncio
from dataclasses import dataclass
from typing import Any

import httpx

from app.core.config import get_settings
from app.core.logging import get_logger
from app.schemas.message import SourceMessage
from app.sources.base import SourceCollector


settings = get_settings()
logger = get_logger(__name__)
DISCORD_API_BASE = "https://discord.com/api/v10"


@dataclass(frozen=True)
class DiscordChannelInfo:
    channel_id: str
    channel_name: str | None = None
    guild_id: str | None = None
    guild_name: str | None = None

    @property
    def title(self) -> str:
        if self.guild_name and self.channel_name:
            return f"{self.guild_name}#{self.channel_name}"
        return self.channel_name or self.channel_id


class DiscordCollector(SourceCollector):
    def __init__(self, pipeline) -> None:
        self.pipeline = pipeline
        self._client: httpx.AsyncClient | None = None
        self._runner_task: asyncio.Task | None = None
        self._stop_event = asyncio.Event()
        self._last_seen_message_ids: dict[str, int] = {}
        self._channel_info_cache: dict[str, DiscordChannelInfo] = {}

    async def start(self) -> None:
        if not settings.discord_bot_token:
            logger.warning("DISCORD_BOT_TOKEN is not configured, discord collector will not poll")
            return
        if not settings.discord_channel_ids:
            logger.warning("DISCORD_CHANNEL_IDS is not configured, discord collector will not poll")
            return

        self._client = httpx.AsyncClient(
            timeout=httpx.Timeout(20.0),
            headers={
                "Authorization": f"Bot {settings.discord_bot_token}",
                "User-Agent": "Web3AlphaMVP/1.0",
            },
        )
        self._runner_task = asyncio.create_task(self._run())
        logger.info(
            "discord collector started",
            extra={"platform": "discord", "channels": settings.discord_channel_ids},
        )

    async def stop(self) -> None:
        self._stop_event.set()
        if self._runner_task:
            await asyncio.gather(self._runner_task, return_exceptions=True)
        if self._client:
            await self._client.aclose()
        logger.info("discord collector stopped", extra={"platform": "discord"})

    async def _run(self) -> None:
        while not self._stop_event.is_set():
            await asyncio.gather(
                *(self._poll_channel(channel_id) for channel_id in settings.discord_channel_ids),
                return_exceptions=True,
            )
            try:
                await asyncio.wait_for(
                    self._stop_event.wait(),
                    timeout=max(settings.discord_poll_interval_seconds, 10),
                )
            except asyncio.TimeoutError:
                continue

    async def _poll_channel(self, raw_channel_id: str) -> None:
        channel_id = raw_channel_id.strip()
        if not channel_id:
            return

        try:
            channel_info = await self._fetch_channel_info(channel_id)
            messages = await self._fetch_messages(channel_id, self._last_seen_message_ids.get(channel_id))
            for payload in sorted(messages, key=lambda item: int(item.get("id", 0))):
                message_id = int(payload["id"])
                if message_id <= self._last_seen_message_ids.get(channel_id, 0):
                    continue
                source_message = self.source_message_from_payload(payload, channel_info)
                self._last_seen_message_ids[channel_id] = max(
                    self._last_seen_message_ids.get(channel_id, 0),
                    message_id,
                )
                if not source_message:
                    continue
                logger.info(
                    "discord message received",
                    extra={
                        "platform": "discord",
                        "channel_id": channel_id,
                        "message_id": message_id,
                    },
                )
                await self.pipeline.process(source_message)
        except Exception:
            logger.exception("discord polling failed", extra={"platform": "discord", "channel_id": channel_id})

    async def _fetch_messages(self, channel_id: str, after_message_id: int | None) -> list[dict[str, Any]]:
        if not self._client:
            return []
        params: dict[str, str | int] = {"limit": 50}
        if after_message_id:
            params["after"] = str(after_message_id)
        response = await self._client.get(f"{DISCORD_API_BASE}/channels/{channel_id}/messages", params=params)
        response.raise_for_status()
        payload = response.json()
        return payload if isinstance(payload, list) else []

    async def _fetch_channel_info(self, channel_id: str) -> DiscordChannelInfo:
        if channel_id in self._channel_info_cache:
            return self._channel_info_cache[channel_id]
        if not self._client:
            return DiscordChannelInfo(channel_id=channel_id)

        response = await self._client.get(f"{DISCORD_API_BASE}/channels/{channel_id}")
        response.raise_for_status()
        channel_payload = response.json()
        guild_id = channel_payload.get("guild_id")
        guild_name = await self._fetch_guild_name(guild_id) if guild_id else None
        info = DiscordChannelInfo(
            channel_id=channel_id,
            channel_name=channel_payload.get("name"),
            guild_id=guild_id,
            guild_name=guild_name,
        )
        self._channel_info_cache[channel_id] = info
        return info

    async def _fetch_guild_name(self, guild_id: str) -> str | None:
        if not self._client:
            return None
        response = await self._client.get(f"{DISCORD_API_BASE}/guilds/{guild_id}")
        if response.status_code >= 400:
            return None
        payload = response.json()
        return payload.get("name")

    def source_message_from_payload(
        self,
        payload: dict[str, Any],
        channel_info: DiscordChannelInfo,
    ) -> SourceMessage | None:
        raw_text = self.extract_raw_text(payload)
        if not raw_text.strip():
            return None
        author = payload.get("author") if isinstance(payload.get("author"), dict) else {}
        return SourceMessage(
            source="discord",
            source_chat_id=channel_info.channel_id,
            source_chat_title=channel_info.title,
            source_message_id=int(payload["id"]),
            author_name=author.get("username") or author.get("global_name"),
            raw_text=raw_text,
        )

    @staticmethod
    def extract_raw_text(payload: dict[str, Any]) -> str:
        parts: list[str] = []
        content = str(payload.get("content") or "").strip()
        if content:
            parts.append(content)

        attachments = payload.get("attachments") if isinstance(payload.get("attachments"), list) else []
        for attachment in attachments:
            if not isinstance(attachment, dict):
                continue
            url = attachment.get("url") or attachment.get("proxy_url")
            if url:
                parts.append(str(url))

        embeds = payload.get("embeds") if isinstance(payload.get("embeds"), list) else []
        for embed in embeds:
            if not isinstance(embed, dict):
                continue
            url = embed.get("url")
            if url:
                parts.append(str(url))

        return "\n".join(parts).strip()
