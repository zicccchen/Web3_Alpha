from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.api.routes import router
from app.core.config import get_settings
from app.core.logging import configure_logging, get_logger
from app.db.session import init_db
from app.services.pipeline import MessagePipeline
from app.collectors.discord_collector import DiscordCollector
from app.collectors.telegram_api_collector import TelegramApiCollector
from app.sources.public_telegram.collector import PublicTelegramCollector
from app.sources.telegram.collector import TelegramCollector
from app.sources.x_feed.collector import XFeedCollector


settings = get_settings()
configure_logging(settings.log_level)
logger = get_logger(__name__)


@asynccontextmanager
async def lifespan(_: FastAPI):
    logger.info("starting application", extra={"app_name": settings.app_name})
    await init_db()
    pipeline = MessagePipeline()
    collector = build_collector(pipeline)
    await collector.start()
    try:
        yield
    finally:
        await collector.stop()
        await pipeline.close()
        logger.info("application stopped")


app = FastAPI(title=settings.app_name, lifespan=lifespan)
app.include_router(router)


def build_collector(pipeline: MessagePipeline):
    source_collectors = [item.strip() for item in settings.source_collector.lower().split(",") if item.strip()]
    if settings.x_feed_enabled and "x_feed" not in source_collectors:
        source_collectors.append("x_feed")
    if settings.discord_enabled and "discord" not in source_collectors:
        source_collectors.append("discord")
    if len(source_collectors) > 1:
        return CompositeCollector([build_single_collector(source, pipeline) for source in source_collectors])
    source_collector = source_collectors[0] if source_collectors else "public_telegram"
    return build_single_collector(source_collector, pipeline)


def build_single_collector(source_collector: str, pipeline: MessagePipeline):
    if source_collector == "public_telegram":
        return PublicTelegramCollector(pipeline=pipeline)
    if source_collector == "telegram":
        if settings.telegram_source == "public":
            return PublicTelegramCollector(pipeline=pipeline)
        if settings.telegram_source == "api":
            return TelegramApiCollector(pipeline=pipeline)
        return TelegramCollector(pipeline=pipeline)
    if source_collector == "telegram_api":
        return TelegramApiCollector(pipeline=pipeline)
    if source_collector == "discord":
        return DiscordCollector(pipeline=pipeline)
    if source_collector in {"x_feed", "x", "rss"}:
        return XFeedCollector(pipeline=pipeline)
    raise RuntimeError(f"Unsupported SOURCE_COLLECTOR: {settings.source_collector}")


class CompositeCollector:
    def __init__(self, collectors: list) -> None:
        self.collectors = collectors

    async def start(self) -> None:
        for collector in self.collectors:
            await collector.start()

    async def stop(self) -> None:
        for collector in reversed(self.collectors):
            await collector.stop()
