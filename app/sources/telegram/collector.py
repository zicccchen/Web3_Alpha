import asyncio

from telethon import TelegramClient, events
from telethon.sessions import StringSession

from app.core.config import get_settings
from app.core.logging import get_logger
from app.schemas.message import SourceMessage
from app.sources.base import SourceCollector


settings = get_settings()
logger = get_logger(__name__)


class TelegramCollector(SourceCollector):
    def __init__(self, pipeline) -> None:
        self.pipeline = pipeline
        if not settings.telegram_api_id or not settings.telegram_api_hash or not settings.telegram_session_string:
            raise RuntimeError("Telegram API credentials are required when SOURCE_COLLECTOR=telegram.")
        self.client = TelegramClient(
            StringSession(settings.telegram_session_string),
            settings.telegram_api_id,
            settings.telegram_api_hash,
        )
        self._runner_task: asyncio.Task | None = None

    async def start(self) -> None:
        if not settings.telegram_channels:
            logger.warning("no telegram channels configured, collector will not subscribe")
            return

        @self.client.on(events.NewMessage(chats=settings.telegram_channels))
        async def handler(event) -> None:
            try:
                text = event.raw_text or ""
                if not text.strip():
                    return
                chat = await event.get_chat()
                sender = await event.get_sender()
                source_message = SourceMessage(
                    source="telegram",
                    source_chat_id=str(event.chat_id),
                    source_chat_title=getattr(chat, "title", None),
                    source_message_id=event.message.id,
                    author_name=getattr(sender, "username", None) or getattr(sender, "first_name", None),
                    raw_text=text,
                )
                await self.pipeline.process(source_message)
            except Exception:
                logger.exception("telegram message handling failed")

        await self.client.connect()
        if not await self.client.is_user_authorized():
            raise RuntimeError("Telegram session is not authorized. Please generate a valid StringSession.")
        self._runner_task = asyncio.create_task(self.client.run_until_disconnected())
        logger.info("telegram collector started", extra={"channels": settings.telegram_channels})

    async def stop(self) -> None:
        if self.client.is_connected():
            await self.client.disconnect()
        if self._runner_task:
            await asyncio.gather(self._runner_task, return_exceptions=True)
        logger.info("telegram collector stopped")
