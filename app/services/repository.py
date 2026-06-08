import json
from datetime import datetime, timedelta, timezone

from sqlalchemy import func, select, update
from sqlalchemy.exc import IntegrityError

from app.core.config import get_settings
from app.core.logging import get_logger
from app.db.models import Analysis, CollectorState, Event, EventRecord, Record, TelegramMessage
from app.db.session import SessionLocal


logger = get_logger(__name__)
settings = get_settings()


class MessageRepository:
    async def exists_by_source_message(
        self,
        source_platform: str,
        source_chat_id: str,
        source_message_id: str,
    ) -> bool:
        async with SessionLocal() as session:
            result = await session.execute(
                select(TelegramMessage.id)
                .where(TelegramMessage.source_platform == source_platform)
                .where(TelegramMessage.source_chat_id == source_chat_id)
                .where(TelegramMessage.source_message_id == str(source_message_id))
            )
            return result.scalar_one_or_none() is not None

    async def exists_by_dedup_key(self, dedup_key: str) -> bool:
        async with SessionLocal() as session:
            result = await session.execute(
                select(TelegramMessage.id).where(TelegramMessage.dedup_key == dedup_key)
            )
            return result.scalar_one_or_none() is not None

    async def get_collector_state(self, collector_name: str, source_key: str) -> CollectorState | None:
        async with SessionLocal() as session:
            result = await session.execute(
                select(CollectorState)
                .where(CollectorState.collector_name == collector_name)
                .where(CollectorState.source_key == source_key)
            )
            return result.scalar_one_or_none()

    async def upsert_collector_state(
        self,
        collector_name: str,
        source_key: str,
        last_seen_id: str | None,
        last_seen_time: datetime | None,
    ) -> None:
        async with SessionLocal() as session:
            try:
                result = await session.execute(
                    select(CollectorState)
                    .where(CollectorState.collector_name == collector_name)
                    .where(CollectorState.source_key == source_key)
                )
                state = result.scalar_one_or_none()
                now = datetime.now(timezone.utc)
                if state:
                    state.last_seen_id = last_seen_id
                    state.last_seen_time = last_seen_time
                    state.last_fetch_at = now
                    state.updated_at = now
                else:
                    session.add(
                        CollectorState(
                            collector_name=collector_name,
                            source_key=source_key,
                            last_seen_id=last_seen_id,
                            last_seen_time=last_seen_time,
                            last_fetch_at=now,
                        )
                    )
                await session.commit()
            except IntegrityError:
                await session.rollback()
                await session.execute(
                    update(CollectorState)
                    .where(CollectorState.collector_name == collector_name)
                    .where(CollectorState.source_key == source_key)
                    .values(
                        last_seen_id=last_seen_id,
                        last_seen_time=last_seen_time,
                        last_fetch_at=datetime.now(timezone.utc),
                        updated_at=datetime.now(timezone.utc),
                    )
                )
                await session.commit()
            except Exception:
                await session.rollback()
                logger.exception(
                    "failed to upsert collector state",
                    extra={"collector_name": collector_name, "source_key": source_key},
                )

    async def save(self, message_data: dict) -> TelegramMessage | None:
        async with SessionLocal() as session:
            record = TelegramMessage(**message_data)
            session.add(record)
            try:
                await session.flush()
                analysis = _analysis_payload(message_data.get("analysis_json"))
                layered_record = _record_from_message_data(record, message_data, analysis)
                session.add(layered_record)
                await session.flush()
                layered_analysis = _analysis_from_message_data(layered_record.record_id, record.id, message_data, analysis)
                session.add(layered_analysis)
                await session.flush()
                if message_data.get("event_id"):
                    session.add(
                        EventRecord(
                            event_id=int(message_data["event_id"]),
                            record_id=layered_record.record_id,
                            analysis_id=layered_analysis.analysis_id,
                            event_similarity=message_data.get("event_similarity"),
                            event_match_reason=message_data.get("event_match_reason"),
                            legacy_message_id=record.id,
                            created_at=message_data.get("created_at") or datetime.now(timezone.utc),
                        )
                    )
                await session.commit()
                await session.refresh(record)
                return record
            except IntegrityError:
                await session.rollback()
                logger.warning("duplicate message skipped by database", extra={"dedup_key": message_data["dedup_key"]})
                return None
            except Exception:
                await session.rollback()
                logger.exception("failed to save telegram message")
                return None

    async def recent_events(self, since: datetime, limit: int = 500) -> list[Event]:
        async with SessionLocal() as session:
            result = await session.execute(
                select(Event)
                .where(Event.last_seen_at >= since)
                .where(Event.status == "active")
                .order_by(Event.last_seen_at.desc())
                .limit(limit)
            )
            return list(result.scalars().all())

    async def get_event(self, event_id: int) -> Event | None:
        async with SessionLocal() as session:
            return await session.get(Event, event_id)

    async def create_event(self, event_key: str, event_title: str, event_summary: str | None = None) -> Event:
        async with SessionLocal() as session:
            now = datetime.now(timezone.utc)
            event = Event(
                event_key=event_key,
                event_title=event_title,
                event_summary=event_summary,
                first_seen_at=now,
                last_seen_at=now,
                message_count=0,
                source_count=0,
                max_score=0,
                latest_summary=event_summary,
                status="active",
            )
            session.add(event)
            try:
                await session.commit()
                await session.refresh(event)
                setattr(event, "_was_created", True)
                return event
            except IntegrityError:
                await session.rollback()
                result = await session.execute(select(Event).where(Event.event_key == event_key))
                existing = result.scalar_one_or_none()
                if existing:
                    setattr(existing, "_was_created", False)
                    return existing
                raise

    async def update_event_stats(self, event_id: int) -> None:
        async with SessionLocal() as session:
            try:
                message_count = await session.scalar(
                    select(func.count()).select_from(TelegramMessage).where(TelegramMessage.event_id == event_id)
                )
                source_count = await session.scalar(
                    select(func.count(func.distinct(TelegramMessage.source_chat_id))).where(TelegramMessage.event_id == event_id)
                )
                last_seen_at = await session.scalar(
                    select(func.max(TelegramMessage.created_at)).where(TelegramMessage.event_id == event_id)
                )
                max_score = await session.scalar(
                    select(func.max(TelegramMessage.score)).where(TelegramMessage.event_id == event_id)
                )
                latest_message = await session.execute(
                    select(TelegramMessage.summary_zh)
                    .where(TelegramMessage.event_id == event_id)
                    .order_by(TelegramMessage.created_at.desc(), TelegramMessage.id.desc())
                    .limit(1)
                )
                latest_summary = latest_message.scalar_one_or_none()
                await session.execute(
                    update(Event)
                    .where(Event.id == event_id)
                    .values(
                        message_count=int(message_count or 0),
                        source_count=int(source_count or 0),
                        last_seen_at=last_seen_at or datetime.now(timezone.utc),
                        max_score=float(max_score or 0),
                        latest_summary=latest_summary,
                    )
                )
                await session.commit()
            except Exception:
                await session.rollback()
                logger.exception("failed to update event stats", extra={"event_id": event_id})

    async def update_push_status(self, message_id: int, status: str, error: str | None = None) -> None:
        pushed_at = datetime.now(timezone.utc) if status == "sent" else None
        async with SessionLocal() as session:
            try:
                await session.execute(
                    update(TelegramMessage)
                    .where(TelegramMessage.id == message_id)
                    .values(
                        push_sent=status == "sent",
                        push_status=status,
                        push_error=error,
                        pushed_at=pushed_at,
                    )
                )
                if status == "sent":
                    event_id = await session.scalar(select(TelegramMessage.event_id).where(TelegramMessage.id == message_id))
                    if event_id:
                        await session.execute(
                            update(Event)
                            .where(Event.id == event_id)
                            .values(last_pushed_at=pushed_at)
                        )
                await session.commit()
            except Exception:
                await session.rollback()
                logger.exception("failed to update push status", extra={"message_id": message_id, "push_status": status})

    async def mark_event_upgrade_pushed(self, event_id: int, upgrade_summary: str | None = None) -> None:
        now = datetime.now(timezone.utc)
        async with SessionLocal() as session:
            try:
                event = await session.get(Event, event_id)
                if not event:
                    return
                await session.execute(
                    update(Event)
                    .where(Event.id == event_id)
                    .values(
                        upgrade_count=int(getattr(event, "upgrade_count", 0) or 0) + 1,
                        last_upgrade_at=now,
                        last_upgrade_summary=upgrade_summary or event.latest_summary or event.event_summary,
                        last_pushed_at=now,
                    )
                )
                await session.commit()
            except Exception:
                await session.rollback()
                logger.exception("failed to mark event upgrade pushed", extra={"event_id": event_id})

    async def mark_push_sent(self, message_id: int) -> None:
        await self.update_push_status(message_id, "sent")

    async def count_sent_since(self, since: datetime, signal_level: str | None = None) -> int:
        async with SessionLocal() as session:
            query = (
                select(func.count())
                .select_from(TelegramMessage)
                .where(TelegramMessage.push_status == "sent")
                .where(TelegramMessage.pushed_at >= since)
            )
            if signal_level:
                query = query.where(TelegramMessage.signal_level == signal_level)
            result = await session.execute(query)
            return int(result.scalar() or 0)

    async def push_rate_counts(self) -> dict[str, int]:
        now = datetime.now(timezone.utc)
        day_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        hour_start = now - timedelta(hours=1)
        return {
            "sent_today_count": await self.count_sent_since(day_start),
            "sent_last_hour_count": await self.count_sent_since(hour_start),
            "s_sent_last_hour_count": await self.count_sent_since(hour_start, "S"),
            "a_sent_last_hour_count": await self.count_sent_since(hour_start, "A"),
        }

    async def recent_high_score_messages(
        self,
        since: datetime,
        min_score: float,
        limit: int = 200,
    ) -> list[TelegramMessage]:
        async with SessionLocal() as session:
            result = await session.execute(
                select(TelegramMessage)
                .where(TelegramMessage.created_at >= since)
                .where(TelegramMessage.score >= min_score)
                .order_by(TelegramMessage.created_at.desc())
                .limit(limit)
            )
            return list(result.scalars().all())

    @staticmethod
    def serialize_analysis(analysis: dict) -> str:
        return json.dumps(analysis, ensure_ascii=False)


def _record_from_message_data(record: TelegramMessage, message_data: dict, analysis: dict) -> Record:
    source_metadata = analysis.get("source_metadata") if isinstance(analysis.get("source_metadata"), dict) else {}
    raw_metadata = {
        "source_context": analysis.get("source_context") if isinstance(analysis.get("source_context"), dict) else {},
        "source_metadata": source_metadata,
    }
    return Record(
        record_id=record.id,
        source_platform=message_data.get("source_platform") or message_data.get("source") or "unknown",
        source=message_data.get("source"),
        source_channel=message_data.get("source_chat_id"),
        source_message_id=str(message_data.get("source_message_id")),
        event_time=message_data.get("created_at"),
        collected_at=datetime.now(timezone.utc),
        raw_text=message_data.get("raw_text") or "",
        cleaned_text=message_data.get("cleaned_text") or "",
        payload=json.dumps(source_metadata, ensure_ascii=False),
        raw_metadata=json.dumps(raw_metadata, ensure_ascii=False),
        dedup_key=message_data.get("dedup_key"),
        watchlist_category=message_data.get("watchlist_category"),
        watchlist_label=message_data.get("watchlist_label"),
        watchlist_priority=message_data.get("watchlist_priority"),
        legacy_message_id=record.id,
        created_at=message_data.get("created_at") or datetime.now(timezone.utc),
    )


def _analysis_from_message_data(record_id: int, legacy_message_id: int, message_data: dict, analysis: dict) -> Analysis:
    score_breakdown = analysis.get("score_breakdown") if isinstance(analysis.get("score_breakdown"), dict) else None
    source_profile = None
    source_context = analysis.get("source_context")
    if isinstance(source_context, dict) and isinstance(source_context.get("source_profile"), dict):
        source_profile = source_context["source_profile"]
    elif isinstance(score_breakdown, dict) and isinstance(score_breakdown.get("source_profile"), dict):
        source_profile = score_breakdown["source_profile"]
    return Analysis(
        record_id=record_id,
        model_name=_model_name(),
        model_version=_model_version(),
        prompt_version="ai_decision_v1",
        signal_type=analysis.get("signal_type"),
        ai_decision=message_data.get("ai_decision"),
        ai_confidence=message_data.get("ai_confidence"),
        ai_reason=message_data.get("ai_reason"),
        user_value_summary=message_data.get("user_value_summary"),
        action_suggestion=message_data.get("action_suggestion"),
        urgency=message_data.get("urgency"),
        relevance=message_data.get("relevance"),
        actionability=message_data.get("actionability"),
        risk_level=message_data.get("risk_level"),
        source_profile=json.dumps(source_profile, ensure_ascii=False) if source_profile else None,
        score=float(message_data.get("score") or 0),
        score_breakdown=json.dumps(score_breakdown, ensure_ascii=False) if score_breakdown else None,
        legacy_message_id=legacy_message_id,
        created_at=message_data.get("created_at") or datetime.now(timezone.utc),
    )


def _analysis_payload(raw_value: str | None) -> dict:
    if not raw_value:
        return {}
    try:
        payload = json.loads(raw_value)
        return payload if isinstance(payload, dict) else {}
    except json.JSONDecodeError:
        return {}


def _model_name() -> str:
    return settings.ai_provider


def _model_version() -> str | None:
    if settings.ai_provider.lower() == "anthropic":
        return settings.anthropic_model
    return settings.openai_model
