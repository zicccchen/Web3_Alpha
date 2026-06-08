import asyncio
import re
from dataclasses import dataclass
from html import unescape
from html.parser import HTMLParser
from urllib.parse import urlparse

import httpx

from app.core.config import get_settings
from app.core.logging import get_logger
from app.schemas.message import SourceMessage
from app.sources.base import SourceCollector


settings = get_settings()
logger = get_logger(__name__)


@dataclass(frozen=True)
class PublicTelegramMessage:
    channel: str
    message_id: int
    text: str


class TelegramPublicPageParser(HTMLParser):
    def __init__(self, channel: str) -> None:
        super().__init__(convert_charrefs=True)
        self.channel = channel
        self.messages: list[PublicTelegramMessage] = []
        self._current_post: str | None = None
        self._current_text_parts: list[str] = []
        self._capture_text_depth = 0

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        attr_map = dict(attrs)
        data_post = attr_map.get("data-post")
        if data_post:
            self._flush_current()
            self._current_post = data_post
            self._current_text_parts = []

        class_name = attr_map.get("class", "")
        if tag == "div" and "tgme_widget_message_text" in class_name and self._current_post:
            self._capture_text_depth = 1
        elif self._capture_text_depth and tag == "div":
            self._capture_text_depth += 1
        elif self._capture_text_depth and tag == "br":
            self._current_text_parts.append("\n")

    def handle_endtag(self, tag: str) -> None:
        if self._capture_text_depth and tag == "div":
            self._capture_text_depth -= 1

    def handle_data(self, data: str) -> None:
        if self._capture_text_depth:
            self._current_text_parts.append(data)

    def close(self) -> None:
        super().close()
        self._flush_current()

    def _flush_current(self) -> None:
        if not self._current_post:
            return

        post_channel, _, post_id = self._current_post.partition("/")
        if post_channel.lower() != self.channel.lower() or not post_id.isdigit():
            self._current_post = None
            self._current_text_parts = []
            return

        text = unescape("".join(self._current_text_parts)).strip()
        text = re.sub(r"\n{3,}", "\n\n", text)
        if text:
            self.messages.append(
                PublicTelegramMessage(
                    channel=self.channel,
                    message_id=int(post_id),
                    text=text,
                )
            )

        self._current_post = None
        self._current_text_parts = []


class PublicTelegramCollector(SourceCollector):
    def __init__(self, pipeline) -> None:
        self.pipeline = pipeline
        self._client: httpx.AsyncClient | None = None
        self._runner_task: asyncio.Task | None = None
        self._stop_event = asyncio.Event()

    async def start(self) -> None:
        if not settings.public_telegram_channels:
            logger.warning("no public telegram channels configured, collector will not poll")
            return

        self._client = httpx.AsyncClient(
            follow_redirects=True,
            timeout=httpx.Timeout(20.0),
            headers={
                "User-Agent": (
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0 Safari/537.36"
                )
            },
        )
        self._runner_task = asyncio.create_task(self._run())
        logger.info(
            "public telegram collector started",
            extra={"channels": settings.public_telegram_channels},
        )

    async def stop(self) -> None:
        self._stop_event.set()
        if self._runner_task:
            await asyncio.gather(self._runner_task, return_exceptions=True)
        if self._client:
            await self._client.aclose()
        logger.info("public telegram collector stopped")

    async def _run(self) -> None:
        while not self._stop_event.is_set():
            await asyncio.gather(
                *(self._poll_channel(channel) for channel in settings.public_telegram_channels),
                return_exceptions=True,
            )
            try:
                await asyncio.wait_for(
                    self._stop_event.wait(),
                    timeout=max(settings.public_poll_interval_seconds, 10),
                )
            except asyncio.TimeoutError:
                continue

    async def _poll_channel(self, raw_channel: str) -> None:
        channel = self._normalize_channel(raw_channel)
        if not channel:
            logger.warning("invalid public telegram channel skipped", extra={"channel": raw_channel})
            return

        try:
            messages = await self._fetch_messages(channel)
            fetch_limit = max(settings.public_fetch_limit, 1)
            for message in messages[-fetch_limit:]:
                source_message = SourceMessage(
                    source="telegram_public",
                    source_chat_id=channel,
                    source_chat_title=f"@{channel}",
                    source_message_id=message.message_id,
                    author_name=None,
                    raw_text=message.text,
                )
                await self.pipeline.process(source_message)
        except Exception:
            logger.exception("public telegram polling failed", extra={"channel": channel})

    async def _fetch_messages(self, channel: str) -> list[PublicTelegramMessage]:
        if not self._client:
            return []

        response = await self._client.get(f"https://t.me/s/{channel}")
        response.raise_for_status()

        parser = TelegramPublicPageParser(channel)
        parser.feed(response.text)
        parser.close()
        return parser.messages

    def _normalize_channel(self, channel: str) -> str | None:
        value = channel.strip()
        if not value:
            return None
        if value.startswith("@"):
            return value[1:]
        if value.startswith("http://") or value.startswith("https://"):
            path_parts = [part for part in urlparse(value).path.split("/") if part]
            if not path_parts:
                return None
            if path_parts[0] == "s" and len(path_parts) > 1:
                return path_parts[1]
            return path_parts[0]
        return value
