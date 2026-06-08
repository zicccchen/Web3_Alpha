import asyncio
from dataclasses import dataclass
from hashlib import sha256
from html import unescape
import re
from typing import Any
from urllib.parse import urlparse
from xml.etree import ElementTree

import httpx

from app.core.config import get_settings
from app.core.logging import get_logger
from app.schemas.message import SourceMessage
from app.services.watchlists import Watchlists, load_watchlists, normalize_account
from app.sources.base import SourceCollector


settings = get_settings()
logger = get_logger(__name__)
TAG_RE = re.compile(r"<[^>]+>")


@dataclass(frozen=True)
class XFeedEntry:
    title: str
    content: str
    link: str
    published: str | None
    guid: str | None

    @property
    def source_message_id(self) -> str:
        if self.guid:
            return self.guid
        if self.link:
            return f"link:{sha256(self.link.encode('utf-8')).hexdigest()[:24]}"
        return f"text:{sha256(self.raw_text.encode('utf-8')).hexdigest()[:24]}"

    @property
    def raw_text(self) -> str:
        parts = [self.title.strip(), self.content.strip(), self.link.strip()]
        return "\n".join(part for part in parts if part)


@dataclass(frozen=True)
class XFeed:
    url: str
    title: str
    entries: list[XFeedEntry]

    @property
    def source_channel(self) -> str:
        if self.title:
            return self.title
        hostname = urlparse(self.url).hostname
        return hostname or self.url


class XFeedCollector(SourceCollector):
    def __init__(self, pipeline) -> None:
        self.pipeline = pipeline
        self._client: httpx.AsyncClient | None = None
        self._runner_task: asyncio.Task | None = None
        self._stop_event = asyncio.Event()
        self._seen_ids_by_feed: dict[str, set[str]] = {}
        self.watchlists: Watchlists = load_watchlists()
        self.feed_urls: list[str] = []
        self._account_by_feed_url: dict[str, str] = {}

    async def start(self) -> None:
        if not settings.x_feed_enabled:
            logger.info("x feed collector disabled", extra={"platform": "x"})
            return
        if settings.x_feed_mode not in {"rss", "manual_feed"}:
            logger.warning("unsupported X_FEED_MODE, x feed collector disabled", extra={"platform": "x", "mode": settings.x_feed_mode})
            return
        self.feed_urls = self._feed_urls()
        if not self.feed_urls:
            logger.warning("X_FEED_URLS is not configured, x feed collector will not poll", extra={"platform": "x"})
            return

        self._log_loaded_watchlists()
        self._client = httpx.AsyncClient(timeout=httpx.Timeout(settings.x_feed_request_timeout_seconds))
        self._runner_task = asyncio.create_task(self._run())
        logger.info("x feed collector started", extra={"platform": "x", "mode": settings.x_feed_mode, "feed_count": len(self.feed_urls)})

    async def stop(self) -> None:
        self._stop_event.set()
        if self._runner_task:
            await asyncio.gather(self._runner_task, return_exceptions=True)
        if self._client:
            await self._client.aclose()
        logger.info("x feed collector stopped", extra={"platform": "x"})

    async def _run(self) -> None:
        while not self._stop_event.is_set():
            await self.poll_once()
            try:
                await asyncio.wait_for(
                    self._stop_event.wait(),
                    timeout=max(settings.x_feed_poll_interval_seconds, 30),
                )
            except asyncio.TimeoutError:
                continue

    async def poll_once(self) -> dict[str, int]:
        success_count = 0
        failed_count = 0
        new_message_count = 0

        if not self.feed_urls:
            self.feed_urls = self._feed_urls()

        for feed_url in self.feed_urls:
            try:
                feed = await self.fetch_feed(feed_url)
                success_count += 1
                for source_message in self.source_messages_from_feed(feed):
                    new_message_count += 1
                    await self.pipeline.process(source_message)
            except Exception:
                failed_count += 1
                logger.exception("x feed polling failed", extra={"platform": "x", "feed_url": feed_url})

        stats = {
            "success_feed_count": success_count,
            "failed_feed_count": failed_count,
            "new_message_count": new_message_count,
        }
        logger.info("x feed polling round completed", extra={"platform": "x", **stats})
        return stats

    async def fetch_feed(self, feed_url: str) -> XFeed:
        if not self._client:
            raise RuntimeError("x feed http client is not initialized")
        response = await self._client.get(feed_url)
        response.raise_for_status()
        return parse_x_feed(response.text, feed_url)

    def source_messages_from_feed(self, feed: XFeed) -> list[SourceMessage]:
        seen_ids = self._seen_ids_by_feed.setdefault(feed.url, set())
        messages: list[SourceMessage] = []
        for entry in feed.entries:
            source_message_id = entry.source_message_id
            link_id = f"link:{sha256(entry.link.encode('utf-8')).hexdigest()[:24]}" if entry.link else None
            if source_message_id in seen_ids or (link_id and link_id in seen_ids):
                continue
            seen_ids.add(source_message_id)
            if link_id:
                seen_ids.add(link_id)
            account = self._account_for_feed(feed)
            watchlist_match = self.watchlists.match_account(account)
            messages.append(
                SourceMessage(
                    source="x",
                    source_chat_id=feed.url,
                    source_chat_title=feed.source_channel,
                    source_message_id=source_message_id,
                    author_name=None,
                    raw_text=entry.raw_text,
                    watchlist_category=watchlist_match.category if watchlist_match else None,
                    watchlist_label=watchlist_match.label if watchlist_match else None,
                    watchlist_priority=watchlist_match.priority if watchlist_match else None,
                )
            )
        return messages

    def _feed_urls(self) -> list[str]:
        urls: list[str] = []
        seen: set[str] = set()
        for account in self.watchlists.deduped_accounts:
            feed_url = self._feed_url_for_account(account)
            normalized = _normalize_feed_url(feed_url)
            if normalized in seen:
                continue
            seen.add(normalized)
            urls.append(feed_url)
            self._account_by_feed_url[normalized] = normalize_account(account)

        for feed_url in settings.x_feed_urls:
            normalized = _normalize_feed_url(feed_url)
            if normalized in seen:
                continue
            seen.add(normalized)
            urls.append(feed_url)
            account = _account_from_feed_url(feed_url)
            if account:
                self._account_by_feed_url[normalized] = account
        return urls

    def _feed_url_for_account(self, account: str) -> str:
        return f"{settings.x_feed_base_url.rstrip('/')}/{account.lstrip('@')}"

    def _account_for_feed(self, feed: XFeed) -> str | None:
        normalized_url = _normalize_feed_url(feed.url)
        account = self._account_by_feed_url.get(normalized_url)
        if account:
            return account
        return _account_from_feed_url(feed.url)

    def _log_loaded_watchlists(self) -> None:
        logger.info("Loaded watchlists:", extra={"platform": "x", "deduped_account_count": len(self.watchlists.deduped_accounts)})
        for category in self.watchlists.categories:
            logger.info(
                f"- {category.key}: {len(category.accounts)} accounts, priority={category.priority}",
                extra={
                    "platform": "x",
                    "watchlist_category": category.key,
                    "watchlist_priority": category.priority,
                    "watchlist_account_count": len(category.accounts),
                },
            )
        logger.info(
            "x feed deduped watchlist accounts loaded",
            extra={"platform": "x", "deduped_account_count": len(self.watchlists.deduped_accounts)},
        )


def parse_x_feed(content: str, feed_url: str) -> XFeed:
    root = ElementTree.fromstring(content)
    if _local_name(root.tag) == "feed":
        return _parse_atom_feed(root, feed_url)
    return _parse_rss_feed(root, feed_url)


def _parse_rss_feed(root: ElementTree.Element, feed_url: str) -> XFeed:
    channel_node = _first_child(root, "channel")
    channel = channel_node if channel_node is not None else root
    title = _text(_first_child(channel, "title"))
    entries = []
    for item in _children(channel, "item"):
        entries.append(
            XFeedEntry(
                title=_clean_text(_text(_first_child(item, "title"))),
                content=_clean_text(_text(_first_child(item, "description")) or _text(_first_child(item, "encoded"))),
                link=_text(_first_child(item, "link")),
                published=_text(_first_child(item, "pubDate")) or _text(_first_child(item, "published")),
                guid=_text(_first_child(item, "guid")),
            )
        )
    return XFeed(url=feed_url, title=_clean_text(title), entries=entries)


def _parse_atom_feed(root: ElementTree.Element, feed_url: str) -> XFeed:
    title = _text(_first_child(root, "title"))
    entries = []
    for entry in _children(root, "entry"):
        entries.append(
            XFeedEntry(
                title=_clean_text(_text(_first_child(entry, "title"))),
                content=_clean_text(_text(_first_child(entry, "content")) or _text(_first_child(entry, "summary"))),
                link=_atom_link(entry),
                published=_text(_first_child(entry, "published")) or _text(_first_child(entry, "updated")),
                guid=_text(_first_child(entry, "id")),
            )
        )
    return XFeed(url=feed_url, title=_clean_text(title), entries=entries)


def _atom_link(entry: ElementTree.Element) -> str:
    for child in entry:
        if _local_name(child.tag) != "link":
            continue
        href = child.attrib.get("href")
        if href:
            return href
    return ""


def _first_child(parent: ElementTree.Element, local_name: str) -> ElementTree.Element | None:
    for child in parent:
        if _local_name(child.tag) == local_name:
            return child
    return None


def _children(parent: ElementTree.Element, local_name: str) -> list[ElementTree.Element]:
    return [child for child in parent if _local_name(child.tag) == local_name]


def _local_name(tag: str) -> str:
    return tag.rsplit("}", 1)[-1]


def _text(element: ElementTree.Element | None) -> str:
    if element is None:
        return ""
    return "".join(element.itertext()).strip()


def _clean_text(value: str) -> str:
    return re.sub(r"\s+", " ", unescape(TAG_RE.sub(" ", value))).strip()


def _normalize_feed_url(feed_url: str) -> str:
    return feed_url.strip().rstrip("/")


def _account_from_feed_url(feed_url: str) -> str | None:
    path_parts = [part for part in urlparse(feed_url).path.split("/") if part]
    if len(path_parts) >= 3 and path_parts[-3:-1] == ["twitter", "user"]:
        return normalize_account(path_parts[-1])
    if len(path_parts) >= 2 and path_parts[-2:] and path_parts[-2] == "user":
        return normalize_account(path_parts[-1])
    return None
