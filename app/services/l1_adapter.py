from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from app.schemas.message import SourceMessage


@dataclass(frozen=True)
class L1Record:
    source_platform: str
    source: str
    source_channel: str
    source_message_id: str
    raw_text: str
    event_time: datetime | None = None
    author_name: str | None = None
    payload: dict[str, Any] = field(default_factory=dict)
    raw_metadata: dict[str, Any] = field(default_factory=dict)
    watchlist_category: str | None = None
    watchlist_label: str | None = None
    watchlist_priority: int | None = None


class L1Adapter:
    """Boundary for future external L1 data lake records.

    The adapter outputs the existing SourceMessage contract so downstream
    Cleaner, AI Decision, Event Cluster, Feedback, and Calibration stay stable.
    """

    def to_source_message(self, record: L1Record) -> SourceMessage:
        return SourceMessage(
            source=record.source_platform,
            source_chat_id=record.source_channel,
            source_chat_title=record.raw_metadata.get("source_chat_title") or record.source_channel,
            source_message_id=record.source_message_id,
            author_name=record.author_name,
            raw_text=record.raw_text,
            created_at=record.event_time,
            watchlist_category=record.watchlist_category,
            watchlist_label=record.watchlist_label,
            watchlist_priority=record.watchlist_priority,
            metadata={
                "l1_source": record.source,
                "payload": record.payload,
                **record.raw_metadata,
            },
        )
