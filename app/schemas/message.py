from datetime import datetime

from pydantic import BaseModel


class SourceMessage(BaseModel):
    source: str
    source_chat_id: str
    source_chat_title: str | None = None
    source_message_id: str | int
    author_name: str | None = None
    raw_text: str
    created_at: datetime | None = None
    watchlist_category: str | None = None
    watchlist_label: str | None = None
    watchlist_priority: int | None = None
    metadata: dict | None = None
