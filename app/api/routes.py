from datetime import datetime, timedelta, timezone
from hashlib import sha256
import json
import re

from fastapi import APIRouter, Depends, Query, Request
from sqlalchemy import desc, func, or_, select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.config.telegram_watchlists import load_telegram_watchlists
from app.db.models import CollectorState, Event, Feedback, TelegramMessage
from app.db.session import get_db_session
from app.core.logging import get_logger
from app.services.calibration import build_calibration_report
from app.services.duplicates import DuplicateBackfillResult, apply_backfill_marks, backfill_possible_duplicates
from app.services.event_backfill import format_event_backfill_payload, plan_event_backfill
from app.services.event_cluster import extract_event_features, normalize_event_title, rank_event_candidates


router = APIRouter()
logger = get_logger(__name__)
URL_RE = re.compile(r"https?://\S+")
last_duplicate_backfill_threshold: float | None = None


@router.get("/health")
async def healthcheck() -> dict[str, str]:
    return {"status": "ok"}


@router.post("/feishu/feedback")
async def feishu_feedback(request: Request, db: AsyncSession = Depends(get_db_session)) -> dict:
    payload = await request.json()
    if "challenge" in payload:
        return {"challenge": payload["challenge"]}

    value = _extract_feishu_action_value(payload)
    action = value.get("action")
    if action not in {"good", "bad", "ignore"}:
        return {
            "toast": {
                "type": "warning",
                "content": "无效反馈",
            }
        }

    now = datetime.now(timezone.utc)
    message_id = _safe_int(value.get("message_id"))
    event_id = _safe_int(value.get("event_id"))
    saved_targets: list[tuple[str, int]] = []
    feedback_rows: list[dict] = []
    if message_id is not None:
        message_result = await db.execute(
            update(TelegramMessage)
            .where(TelegramMessage.id == message_id)
            .values(feedback=action, feedback_at=now)
        )
        if _updated_row_count(message_result) > 0:
            saved_targets.append(("message", message_id))
            feedback_rows.append(
                {
                    "feedback_dedup_key": _feedback_dedup_key(
                        payload,
                        target_type="record",
                        target_id=message_id,
                        feedback=action,
                        now=now,
                    ),
                    "target_type": "record",
                    "record_id": message_id,
                    "event_id": event_id,
                    "feedback": action,
                    "note": None,
                    "feedback_source": "feishu",
                    "legacy_message_id": message_id,
                    "created_at": now,
                }
            )
    if event_id is not None:
        event_result = await db.execute(
            update(Event)
            .where(Event.id == event_id)
            .values(feedback=action, feedback_at=now)
        )
        if _updated_row_count(event_result) > 0:
            saved_targets.append(("event", event_id))
            feedback_rows.append(
                {
                    "feedback_dedup_key": _feedback_dedup_key(
                        payload,
                        target_type="event",
                        target_id=event_id,
                        feedback=action,
                        now=now,
                    ),
                    "target_type": "event",
                    "record_id": None,
                    "event_id": event_id,
                    "feedback": action,
                    "note": None,
                    "feedback_source": "feishu",
                    "legacy_message_id": message_id,
                    "created_at": now,
                }
            )
    inserted_feedback_count = 0
    for feedback_row in feedback_rows:
        result = await db.execute(
            pg_insert(Feedback)
            .values(**feedback_row)
            .on_conflict_do_nothing(index_elements=["feedback_dedup_key"])
        )
        inserted_feedback_count += _updated_row_count(result)
    await db.commit()
    if not saved_targets:
        logger.warning(
            "feishu feedback ignored: invalid target_id",
            extra={
                "feedback": action,
                "message_id": message_id,
                "event_id": event_id,
            },
        )
        return {
            "toast": {
                "type": "warning",
                "content": "feedback ignored: invalid target_id",
            }
        }

    target_type, target_id = _preferred_feedback_target(saved_targets)
    if feedback_rows and inserted_feedback_count == 0:
        return {
            "toast": {
                "type": "success",
                "content": f"feedback already saved: target_type={target_type}, target_id={target_id}, feedback={action}",
            }
        }
    return {
        "toast": {
            "type": "success",
            "content": f"feedback saved: target_type={target_type}, target_id={target_id}, feedback={action}",
        }
    }


@router.get("/feedback/stats")
async def feedback_stats(db: AsyncSession = Depends(get_db_session)) -> dict:
    feedback_count = await db.scalar(
        select(func.count()).select_from(TelegramMessage).where(TelegramMessage.feedback.in_(("good", "bad", "ignore")))
    )
    event_feedback_count = await db.scalar(
        select(func.count()).select_from(Event).where(Event.feedback.in_(("good", "bad", "ignore")))
    )
    decision_feedback_rows = await db.execute(
        select(
            TelegramMessage.ai_decision,
            TelegramMessage.feedback,
            func.count().label("count"),
        )
        .where(TelegramMessage.feedback.in_(("good", "bad", "ignore")))
        .group_by(TelegramMessage.ai_decision, TelegramMessage.feedback)
    )
    message_rows = await db.execute(
        select(TelegramMessage)
        .where(TelegramMessage.feedback.in_(("good", "bad", "ignore")))
        .where(TelegramMessage.feedback_at.is_not(None))
        .order_by(TelegramMessage.feedback_at.desc(), TelegramMessage.id.desc())
        .limit(10)
    )
    event_rows = await db.execute(
        select(Event)
        .where(Event.feedback.in_(("good", "bad", "ignore")))
        .where(Event.feedback_at.is_not(None))
        .order_by(Event.feedback_at.desc(), Event.id.desc())
        .limit(10)
    )
    latest_items = [
        *[_format_feedback_message_item(message) for message in message_rows.scalars().all()],
        *[_format_feedback_event_item(event) for event in event_rows.scalars().all()],
    ]
    latest_items.sort(key=lambda item: item["feedback_at"] or "", reverse=True)
    return {
        "message_feedback_count": int(feedback_count or 0),
        "event_feedback_count": int(event_feedback_count or 0),
        "decision_feedback_counts": [
            {
                "ai_decision": row["ai_decision"] or "unknown",
                "feedback": row["feedback"],
                "count": int(row["count"]),
            }
            for row in decision_feedback_rows.mappings()
        ],
        "latest_feedback_items": latest_items[:10],
    }


@router.get("/messages")
async def list_messages(
    limit: int = Query(default=20, ge=1, le=200),
    db: AsyncSession = Depends(get_db_session),
) -> list[dict]:
    result = await db.execute(
        select(TelegramMessage).order_by(desc(TelegramMessage.created_at)).limit(limit)
    )
    rows = result.scalars().all()
    return [row.to_dict() for row in rows]


@router.get("/messages/top")
async def top_messages(
    limit: int = Query(default=20, ge=1, le=200),
    hours: int | None = Query(default=None, ge=1, le=24 * 365),
    db: AsyncSession = Depends(get_db_session),
) -> list[dict]:
    query = select(TelegramMessage).order_by(desc(TelegramMessage.score), desc(TelegramMessage.created_at)).limit(limit)
    if hours is not None:
        since = datetime.now(timezone.utc) - timedelta(hours=hours)
        query = query.where(TelegramMessage.created_at >= since)

    result = await db.execute(query)
    return [format_top_message(row) for row in result.scalars().all()]


@router.post("/duplicates/backfill")
async def backfill_duplicates(
    hours: int = Query(default=24, ge=1, le=24 * 365),
    threshold: float = Query(default=0.82, ge=0, le=1),
    dry_run: bool = Query(default=False),
    db: AsyncSession = Depends(get_db_session),
) -> dict:
    global last_duplicate_backfill_threshold

    since = datetime.now(timezone.utc) - timedelta(hours=hours)
    result = await db.execute(
        select(TelegramMessage)
        .where(TelegramMessage.created_at >= since)
        .order_by(TelegramMessage.created_at.asc(), TelegramMessage.id.asc())
    )
    rows = list(result.scalars().all())
    backfill_result = backfill_possible_duplicates(rows, threshold=threshold, dry_run=dry_run)

    if not dry_run:
        apply_backfill_marks(rows, backfill_result)
        await db.commit()
        last_duplicate_backfill_threshold = threshold

    return format_duplicate_backfill_payload(backfill_result, hours=hours)


@router.get("/duplicates/stats")
async def duplicate_stats(db: AsyncSession = Depends(get_db_session)) -> dict:
    total_messages = await db.scalar(select(func.count()).select_from(TelegramMessage))
    duplicate_count = await db.scalar(
        select(func.count()).select_from(TelegramMessage).where(TelegramMessage.possible_duplicate.is_(True))
    )
    average_similarity = await db.scalar(
        select(func.avg(TelegramMessage.similarity_score)).where(TelegramMessage.possible_duplicate.is_(True))
    )
    channel_expr = func.coalesce(TelegramMessage.source_chat_title, TelegramMessage.source_chat_id)
    channel_rows = await db.execute(
        select(
            channel_expr.label("channel"),
            func.count().label("count"),
        )
        .where(TelegramMessage.possible_duplicate.is_(True))
        .group_by(channel_expr)
        .order_by(desc(func.count()))
        .limit(10)
    )
    duplicate_count = int(duplicate_count or 0)
    total_messages = int(total_messages or 0)
    return {
        "duplicate_count": duplicate_count,
        "duplicate_rate": round(duplicate_count / total_messages, 4) if total_messages else 0,
        "average_similarity": round(float(average_similarity), 4) if average_similarity else 0,
        "threshold_used": last_duplicate_backfill_threshold,
        "top_duplicate_channels": [
            {"channel": row["channel"], "count": int(row["count"])}
            for row in channel_rows.mappings()
        ],
    }


@router.get("/duplicates")
async def duplicate_messages(
    limit: int = Query(default=50, ge=1, le=200),
    db: AsyncSession = Depends(get_db_session),
) -> list[dict]:
    result = await db.execute(
        select(TelegramMessage)
        .where(TelegramMessage.possible_duplicate.is_(True))
        .order_by(desc(TelegramMessage.created_at))
        .limit(limit)
    )
    return [row.to_dict() for row in result.scalars().all()]


@router.get("/events")
async def list_events(
    limit: int = Query(default=50, ge=1, le=200),
    db: AsyncSession = Depends(get_db_session),
) -> list[dict]:
    result = await db.execute(select(Event).where(Event.status == "active").order_by(desc(Event.last_seen_at)).limit(limit))
    return [row.to_dict() for row in result.scalars().all()]


@router.post("/events/backfill")
async def backfill_events(
    hours: int = Query(default=24, ge=1, le=24 * 365),
    dry_run: bool = Query(default=False),
    db: AsyncSession = Depends(get_db_session),
) -> dict:
    since = datetime.now(timezone.utc) - timedelta(hours=hours)
    message_rows = await db.execute(
        select(TelegramMessage)
        .where(TelegramMessage.created_at >= since)
        .order_by(TelegramMessage.created_at.asc(), TelegramMessage.id.asc())
    )
    event_rows = await db.execute(
        select(Event)
        .where(Event.last_seen_at >= since - timedelta(hours=48))
        .where(Event.status == "active")
    )
    result = plan_event_backfill(message_rows.scalars().all(), existing_events=event_rows.scalars().all(), dry_run=dry_run)

    if dry_run:
        return format_event_backfill_payload(result, hours=hours)

    event_ids: set[int] = set()
    for plan in result.events:
        event_id = plan.existing_event_id
        if event_id is None:
            event = Event(
                event_key=plan.event_key,
                event_title=plan.event_title,
                event_summary=plan.event_summary,
                first_seen_at=plan.first_seen_at or datetime.now(timezone.utc),
                last_seen_at=plan.last_seen_at or datetime.now(timezone.utc),
                message_count=0,
                source_count=0,
                max_score=0,
                latest_summary=plan.event_summary,
            )
            db.add(event)
            await db.flush()
            event_id = int(event.id)
        event_ids.add(event_id)
        for assignment in plan.assignments:
            await db.execute(
                update(TelegramMessage)
                .where(TelegramMessage.id == getattr(assignment.message, "id"))
                .values(
                    event_id=event_id,
                    event_similarity=assignment.event_similarity,
                    event_match_reason=assignment.event_match_reason,
                )
            )

    await _refresh_event_stats(db, event_ids)
    await db.commit()
    return format_event_backfill_payload(result, hours=hours)


@router.get("/events/stats")
async def event_stats(db: AsyncSession = Depends(get_db_session)) -> dict:
    event_count = await db.scalar(select(func.count()).select_from(Event).where(Event.status == "active"))
    messages_with_event_count = await db.scalar(
        select(func.count()).select_from(TelegramMessage).where(TelegramMessage.event_id.is_not(None))
    )
    average_messages_per_event = await db.scalar(select(func.avg(Event.message_count)).select_from(Event))
    top_rows = await db.execute(
        select(Event)
        .where(Event.status == "active")
        .order_by(desc(Event.message_count), desc(Event.last_seen_at))
        .limit(10)
    )
    return {
        "event_count": int(event_count or 0),
        "messages_with_event_count": int(messages_with_event_count or 0),
        "average_messages_per_event": round(float(average_messages_per_event), 2) if average_messages_per_event else 0,
        "top_events_by_message_count": [event.to_dict() for event in top_rows.scalars().all()],
    }


@router.get("/events/debug-match/{message_id}")
async def debug_event_match(message_id: int, db: AsyncSession = Depends(get_db_session)) -> dict:
    message = await db.get(TelegramMessage, message_id)
    if not message:
        return {"error": "message_not_found", "message_id": message_id}
    since = datetime.now(timezone.utc) - timedelta(hours=48)
    event_title = _message_event_title(message)
    message_summary = message.summary_zh or ""
    message_text = message.cleaned_text or message.raw_text or ""
    candidate_query = (
        select(Event)
        .where(Event.status == "active")
        .where(Event.last_seen_at >= since)
        .order_by(desc(Event.last_seen_at))
        .limit(500)
    )
    if message.event_id:
        candidate_query = candidate_query.where(Event.id != message.event_id)
    candidate_rows = await db.execute(candidate_query)
    details = rank_event_candidates(
        event_title,
        message_summary,
        message_text,
        candidate_rows.scalars().all(),
        message_created_at=message.created_at,
        limit=10,
    )
    features = extract_event_features(" ".join(part for part in (event_title, message_summary, message_text) if part))
    return {
        "message_id": message.id,
        "message_summary": message.summary_zh,
        "message_event_title": event_title,
        "message_entities": sorted(features.entities),
        "message_projects": sorted(features.projects),
        "message_tokens": sorted(features.tokens),
        "message_numbers": sorted(features.numbers),
        "message_key_phrases": sorted(features.key_phrases),
        "candidates": [detail.as_dict() for detail in details],
    }


@router.post("/events/merge")
async def merge_events(request: Request, db: AsyncSession = Depends(get_db_session)) -> dict:
    payload = await request.json()
    source_event_id = _safe_int(payload.get("source_event_id"))
    target_event_id = _safe_int(payload.get("target_event_id"))
    reason = str(payload.get("reason") or "").strip()
    if not source_event_id or not target_event_id or source_event_id == target_event_id:
        return {"error": "invalid_event_ids"}
    source_event = await db.get(Event, source_event_id)
    target_event = await db.get(Event, target_event_id)
    if not source_event or not target_event:
        return {"error": "event_not_found", "source_event_id": source_event_id, "target_event_id": target_event_id}

    moved_result = await db.execute(
        update(TelegramMessage)
        .where(TelegramMessage.event_id == source_event_id)
        .values(
            event_id=target_event_id,
            event_match_reason=func.concat(
                func.coalesce(TelegramMessage.event_match_reason, ""),
                f";merged_from_event:{source_event_id}",
            ),
        )
    )
    await db.execute(
        update(Event)
        .where(Event.id == source_event_id)
        .values(
            status="merged",
            merged_into_event_id=target_event_id,
            merged_reason=reason or f"merged into event {target_event_id}",
            last_seen_at=datetime.now(timezone.utc),
        )
    )
    await _refresh_event_stats(db, {target_event_id})
    await db.commit()
    logger.info(
        "events merged",
        extra={
            "source_event_id": source_event_id,
            "target_event_id": target_event_id,
            "merge_reason": reason,
            "moved_message_count": _updated_row_count(moved_result),
        },
    )
    return {
        "source_event_id": source_event_id,
        "target_event_id": target_event_id,
        "moved_message_count": _updated_row_count(moved_result),
        "reason": reason,
        "status": "merged",
    }


@router.get("/events/{event_id}")
async def event_detail(event_id: int, db: AsyncSession = Depends(get_db_session)) -> dict:
    event = await db.get(Event, event_id)
    if not event:
        return {"error": "event_not_found"}
    result = await db.execute(
        select(TelegramMessage)
        .where(TelegramMessage.event_id == event_id)
        .order_by(TelegramMessage.created_at.asc(), TelegramMessage.id.asc())
    )
    payload = event.to_dict()
    payload["messages"] = [format_top_message(row) for row in result.scalars().all()]
    return payload


@router.get("/calibration/report")
async def calibration_report(
    days: int = Query(default=7, ge=1, le=90),
    db: AsyncSession = Depends(get_db_session),
) -> dict:
    since = datetime.now(timezone.utc) - timedelta(days=days)
    result = await db.execute(
        select(TelegramMessage, Event.event_title)
        .outerjoin(Event, TelegramMessage.event_id == Event.id)
        .where(TelegramMessage.feedback.in_(("good", "bad", "ignore")))
        .where(TelegramMessage.feedback_at >= since)
        .order_by(TelegramMessage.feedback_at.desc(), TelegramMessage.id.desc())
    )
    messages = [
        _format_calibration_message(message, event_title)
        for message, event_title in result.all()
    ]
    return build_calibration_report(messages, days=days)


async def _refresh_event_stats(db: AsyncSession, event_ids: set[int]) -> None:
    for event_id in event_ids:
        stats_row = await db.execute(
            select(
                func.count(TelegramMessage.id).label("message_count"),
                func.count(func.distinct(TelegramMessage.source_chat_id)).label("source_count"),
                func.min(TelegramMessage.created_at).label("first_seen_at"),
                func.max(TelegramMessage.created_at).label("last_seen_at"),
                func.max(TelegramMessage.score).label("max_score"),
            ).where(TelegramMessage.event_id == event_id)
        )
        stats = stats_row.mappings().one()
        latest_summary_row = await db.execute(
            select(TelegramMessage.summary_zh)
            .where(TelegramMessage.event_id == event_id)
            .order_by(TelegramMessage.created_at.desc(), TelegramMessage.id.desc())
            .limit(1)
        )
        await db.execute(
            update(Event)
            .where(Event.id == event_id)
            .values(
                message_count=int(stats["message_count"] or 0),
                source_count=int(stats["source_count"] or 0),
                first_seen_at=stats["first_seen_at"] or datetime.now(timezone.utc),
                last_seen_at=stats["last_seen_at"] or datetime.now(timezone.utc),
                max_score=float(stats["max_score"] or 0),
                latest_summary=latest_summary_row.scalar_one_or_none(),
            )
        )


@router.get("/stats")
async def stats(db: AsyncSession = Depends(get_db_session)) -> dict:
    since = datetime.now(timezone.utc) - timedelta(hours=24)
    now = datetime.now(timezone.utc)
    day_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    hour_start = now - timedelta(hours=1)

    total_messages = await db.scalar(select(func.count()).select_from(TelegramMessage))
    event_count = await db.scalar(select(func.count()).select_from(Event))
    average_messages_per_event = await db.scalar(select(func.avg(Event.message_count)).select_from(Event))
    messages_last_24h = await db.scalar(
        select(func.count()).select_from(TelegramMessage).where(TelegramMessage.created_at >= since)
    )
    ai_failed_count = await db.scalar(
        select(func.count())
        .select_from(TelegramMessage)
        .where(TelegramMessage.analysis_json.contains('"ai_error"'))
    )
    ai_success_count = await db.scalar(
        select(func.count())
        .select_from(TelegramMessage)
        .where(TelegramMessage.analysis_json.is_not(None))
        .where(~TelegramMessage.analysis_json.contains('"ai_error"'))
    )
    pushed_sent_count = await db.scalar(
        select(func.count()).select_from(TelegramMessage).where(TelegramMessage.push_status == "sent")
    )
    pushed_failed_count = await db.scalar(
        select(func.count()).select_from(TelegramMessage).where(TelegramMessage.push_status == "failed")
    )
    skipped_low_score_count = await db.scalar(
        select(func.count()).select_from(TelegramMessage).where(TelegramMessage.push_status == "skipped_low_score")
    )
    rate_limited_count = await db.scalar(
        select(func.count()).select_from(TelegramMessage).where(TelegramMessage.push_status == "skipped_rate_limited")
    )
    skipped_duplicate_count = await db.scalar(
        select(func.count()).select_from(TelegramMessage).where(TelegramMessage.push_status == "skipped_duplicate")
    )
    event_duplicate_skipped_count = await db.scalar(
        select(func.count()).select_from(TelegramMessage).where(TelegramMessage.push_status == "skipped_event_duplicate")
    )
    push_decision_count = await db.scalar(
        select(func.count()).select_from(TelegramMessage).where(TelegramMessage.ai_decision == "push")
    )
    watch_decision_count = await db.scalar(
        select(func.count()).select_from(TelegramMessage).where(TelegramMessage.ai_decision == "watch")
    )
    ignore_decision_count = await db.scalar(
        select(func.count()).select_from(TelegramMessage).where(TelegramMessage.ai_decision == "ignore")
    )
    push_sent_count = await db.scalar(
        select(func.count())
        .select_from(TelegramMessage)
        .where(TelegramMessage.ai_decision == "push")
        .where(TelegramMessage.push_status == "sent")
    )
    watch_sent_count = await db.scalar(
        select(func.count())
        .select_from(TelegramMessage)
        .where(TelegramMessage.ai_decision == "watch")
        .where(TelegramMessage.push_status == "sent")
    )
    ignore_skipped_count = await db.scalar(
        select(func.count()).select_from(TelegramMessage).where(TelegramMessage.push_status == "skipped_ignore")
    )
    source_profile_rows = await db.execute(select(TelegramMessage.analysis_json).where(TelegramMessage.analysis_json.is_not(None)))
    source_profile_stats = _source_profile_stats([row[0] for row in source_profile_rows.all()])
    sent_today_count = await db.scalar(
        select(func.count())
        .select_from(TelegramMessage)
        .where(TelegramMessage.push_status == "sent")
        .where(TelegramMessage.pushed_at >= day_start)
    )
    sent_last_hour_count = await db.scalar(
        select(func.count())
        .select_from(TelegramMessage)
        .where(TelegramMessage.push_status == "sent")
        .where(TelegramMessage.pushed_at >= hour_start)
    )
    s_sent_last_hour_count = await db.scalar(
        select(func.count())
        .select_from(TelegramMessage)
        .where(TelegramMessage.push_status == "sent")
        .where(TelegramMessage.signal_level == "S")
        .where(TelegramMessage.pushed_at >= hour_start)
    )
    a_sent_last_hour_count = await db.scalar(
        select(func.count())
        .select_from(TelegramMessage)
        .where(TelegramMessage.push_status == "sent")
        .where(TelegramMessage.signal_level == "A")
        .where(TelegramMessage.pushed_at >= hour_start)
    )
    average_score_last_24h = await db.scalar(
        select(func.avg(TelegramMessage.score)).where(TelegramMessage.created_at >= since)
    )
    s_count = await db.scalar(select(func.count()).select_from(TelegramMessage).where(TelegramMessage.signal_level == "S"))
    a_count = await db.scalar(select(func.count()).select_from(TelegramMessage).where(TelegramMessage.signal_level == "A"))
    b_count = await db.scalar(select(func.count()).select_from(TelegramMessage).where(TelegramMessage.signal_level == "B"))
    c_count = await db.scalar(select(func.count()).select_from(TelegramMessage).where(TelegramMessage.signal_level == "C"))
    watchlist_count_rows = await db.execute(
        select(
            TelegramMessage.watchlist_category,
            TelegramMessage.watchlist_label,
            TelegramMessage.watchlist_priority,
            func.count().label("count"),
        )
        .where(TelegramMessage.watchlist_category.is_not(None))
        .group_by(
            TelegramMessage.watchlist_category,
            TelegramMessage.watchlist_label,
            TelegramMessage.watchlist_priority,
        )
        .order_by(desc(TelegramMessage.watchlist_priority), desc(func.count()))
    )
    watchlist_avg_score_rows = await db.execute(
        select(
            TelegramMessage.watchlist_category,
            TelegramMessage.watchlist_label,
            TelegramMessage.watchlist_priority,
            func.avg(TelegramMessage.score).label("average_score"),
        )
        .where(TelegramMessage.watchlist_category.is_not(None))
        .group_by(
            TelegramMessage.watchlist_category,
            TelegramMessage.watchlist_label,
            TelegramMessage.watchlist_priority,
        )
        .order_by(desc(TelegramMessage.watchlist_priority))
    )
    telegram_channel_count = await db.scalar(
        select(func.count(func.distinct(TelegramMessage.source_chat_id))).where(TelegramMessage.source_platform == "telegram")
    )
    telegram_group_count = await db.scalar(
        select(func.count(func.distinct(TelegramMessage.source_chat_id)))
        .where(TelegramMessage.source_platform == "telegram")
        .where(TelegramMessage.analysis_json.contains('"telegram_is_group": true'))
    )
    telegram_messages_24h = await db.scalar(
        select(func.count())
        .select_from(TelegramMessage)
        .where(TelegramMessage.source_platform == "telegram")
        .where(TelegramMessage.created_at >= since)
    )
    telegram_events_24h = await db.scalar(
        select(func.count(func.distinct(TelegramMessage.event_id)))
        .where(TelegramMessage.source_platform == "telegram")
        .where(TelegramMessage.created_at >= since)
        .where(TelegramMessage.event_id.is_not(None))
    )

    return format_stats_payload(
        total_messages=total_messages,
        messages_last_24h=messages_last_24h,
        ai_success_count=ai_success_count,
        ai_failed_count=ai_failed_count,
        pushed_sent_count=pushed_sent_count,
        pushed_failed_count=pushed_failed_count,
        skipped_low_score_count=skipped_low_score_count,
        rate_limited_count=rate_limited_count,
        skipped_duplicate_count=skipped_duplicate_count,
        duplicate_skipped_count=skipped_duplicate_count,
        event_duplicate_skipped_count=event_duplicate_skipped_count,
        push_decision_count=push_decision_count,
        watch_decision_count=watch_decision_count,
        ignore_decision_count=ignore_decision_count,
        push_sent_count=push_sent_count,
        watch_sent_count=watch_sent_count,
        ignore_skipped_count=ignore_skipped_count,
        known_source_count=source_profile_stats["known_source_count"],
        unknown_source_count=source_profile_stats["unknown_source_count"],
        role_distribution=source_profile_stats["role_distribution"],
        sent_today_count=sent_today_count,
        sent_last_hour_count=sent_last_hour_count,
        s_sent_last_hour_count=s_sent_last_hour_count,
        a_sent_last_hour_count=a_sent_last_hour_count,
        average_score_last_24h=average_score_last_24h,
        s_count=s_count,
        a_count=a_count,
        b_count=b_count,
        c_count=c_count,
        event_count=event_count,
        average_messages_per_event=average_messages_per_event,
        watchlist_counts=[
            {
                "category": row["watchlist_category"],
                "label": row["watchlist_label"],
                "priority": row["watchlist_priority"],
                "count": int(row["count"]),
            }
            for row in watchlist_count_rows.mappings()
        ],
        watchlist_avg_scores=[
            {
                "category": row["watchlist_category"],
                "label": row["watchlist_label"],
                "priority": row["watchlist_priority"],
                "average_score": round(float(row["average_score"]), 2) if row["average_score"] else 0,
            }
            for row in watchlist_avg_score_rows.mappings()
        ],
        telegram_channel_count=telegram_channel_count,
        telegram_group_count=telegram_group_count,
        telegram_messages_24h=telegram_messages_24h,
        telegram_events_24h=telegram_events_24h,
    )


@router.get("/sources/telegram")
async def telegram_sources(db: AsyncSession = Depends(get_db_session)) -> list[dict]:
    since = datetime.now(timezone.utc) - timedelta(hours=24)
    watchlists = load_telegram_watchlists()
    rows = []
    for channel in watchlists.deduped_channels:
        state_result = await db.execute(
            select(CollectorState)
            .where(CollectorState.collector_name == "telegram_api")
            .where(CollectorState.source_key == channel.normalized_channel)
        )
        state = state_result.scalar_one_or_none()
        channel_filter = _telegram_channel_filter(channel.channel)
        messages_24h = await db.scalar(
            select(func.count())
            .select_from(TelegramMessage)
            .where(TelegramMessage.source_platform == "telegram")
            .where(TelegramMessage.created_at >= since)
            .where(channel_filter)
        )
        events_24h = await db.scalar(
            select(func.count(func.distinct(TelegramMessage.event_id)))
            .where(TelegramMessage.source_platform == "telegram")
            .where(TelegramMessage.created_at >= since)
            .where(TelegramMessage.event_id.is_not(None))
            .where(channel_filter)
        )
        push_count = await db.scalar(
            select(func.count())
            .select_from(TelegramMessage)
            .where(TelegramMessage.source_platform == "telegram")
            .where(TelegramMessage.push_status == "sent")
            .where(channel_filter)
        )
        rows.append(
            {
                "channel": channel.channel,
                "category": channel.category,
                "label": channel.label,
                "priority": channel.priority,
                "last_fetch_at": state.last_fetch_at.isoformat() if state and state.last_fetch_at else None,
                "last_message_id": state.last_seen_id if state else None,
                "messages_24h": int(messages_24h or 0),
                "events_24h": int(events_24h or 0),
                "push_count": int(push_count or 0),
            }
        )
    return rows


@router.get("/sources/stats")
async def source_stats(db: AsyncSession = Depends(get_db_session)) -> dict:
    telegram_count = await db.scalar(
        select(func.count())
        .select_from(TelegramMessage)
        .where(TelegramMessage.source_platform.in_(("telegram", "telegram_public")))
    )
    x_count = await db.scalar(
        select(func.count()).select_from(TelegramMessage).where(TelegramMessage.source_platform == "x")
    )
    discord_count = await db.scalar(
        select(func.count()).select_from(TelegramMessage).where(TelegramMessage.source_platform == "discord")
    )
    project_expr = func.coalesce(TelegramMessage.watchlist_category, "unknown")
    label_expr = func.coalesce(TelegramMessage.watchlist_label, "unknown")
    channel_name_expr = func.coalesce(TelegramMessage.source_chat_title, TelegramMessage.source_chat_id)
    discord_project_rows = await db.execute(
        select(
            project_expr.label("project"),
            label_expr.label("label"),
            func.count().label("count"),
        )
        .where(TelegramMessage.source_platform == "discord")
        .group_by(project_expr, label_expr)
        .order_by(desc(func.count()))
    )
    discord_channel_rows = await db.execute(
        select(
            TelegramMessage.source_chat_id.label("channel_id"),
            channel_name_expr.label("channel_name"),
            func.count().label("count"),
        )
        .where(TelegramMessage.source_platform == "discord")
        .group_by(TelegramMessage.source_chat_id, channel_name_expr)
        .order_by(desc(func.count()))
    )
    return {
        "telegram_count": int(telegram_count or 0),
        "x_count": int(x_count or 0),
        "discord_count": int(discord_count or 0),
        "discord_project_counts": [
            {"project": row["project"], "label": row["label"], "count": int(row["count"])}
            for row in discord_project_rows.mappings()
        ],
        "discord_channel_counts": [
            {"channel_id": row["channel_id"], "channel_name": row["channel_name"], "count": int(row["count"])}
            for row in discord_channel_rows.mappings()
        ],
    }


def format_top_message(message: TelegramMessage) -> dict:
    return {
        "id": message.id,
        "source_platform": getattr(message, "source_platform", getattr(message, "source", None)),
        "source_channel": message.source_chat_title or message.source_chat_id,
        "source_message_id": message.source_message_id,
        "text": message.raw_text,
        "summary": message.summary_zh,
        "category": message.category,
        "score": message.score,
        "signal_level": message.signal_level,
        "score_breakdown": message.score_breakdown(),
        "push_status": message.push_status,
        "possible_duplicate": message.possible_duplicate,
        "duplicate_of_message_id": message.duplicate_of_message_id,
        "similarity_score": message.similarity_score,
        "event_id": getattr(message, "event_id", None),
        "event_similarity": getattr(message, "event_similarity", None),
        "event_match_reason": getattr(message, "event_match_reason", None),
        "ai_decision": getattr(message, "ai_decision", None),
        "ai_confidence": getattr(message, "ai_confidence", None),
        "ai_reason": getattr(message, "ai_reason", None),
        "user_value_summary": getattr(message, "user_value_summary", None),
        "action_suggestion": getattr(message, "action_suggestion", None),
        "urgency": getattr(message, "urgency", None),
        "relevance": getattr(message, "relevance", None),
        "actionability": getattr(message, "actionability", None),
        "risk_level": getattr(message, "risk_level", None),
        "feedback": getattr(message, "feedback", None),
        "feedback_at": message.feedback_at.isoformat() if getattr(message, "feedback_at", None) else None,
        "watchlist_category": getattr(message, "watchlist_category", None),
        "watchlist_label": getattr(message, "watchlist_label", None),
        "watchlist_priority": getattr(message, "watchlist_priority", None),
        "source_profile": message.source_profile() if hasattr(message, "source_profile") else None,
        "created_at": message.created_at.isoformat() if message.created_at else None,
        "original_url": extract_original_url(message.raw_text),
    }


def _format_calibration_message(message: TelegramMessage, event_title: str | None) -> dict:
    return {
        "id": message.id,
        "feedback": message.feedback,
        "feedback_at": message.feedback_at.isoformat() if message.feedback_at else None,
        "event_id": message.event_id,
        "event_title": event_title,
        "summary_zh": message.summary_zh,
        "score": message.score,
        "score_breakdown": message.score_breakdown(),
        "ai_decision": getattr(message, "ai_decision", None),
        "ai_confidence": message.ai_confidence,
        "ai_reason": message.ai_reason,
        "user_value_summary": message.user_value_summary,
        "action_suggestion": message.action_suggestion,
        "urgency": message.urgency,
        "relevance": message.relevance,
        "actionability": message.actionability,
        "risk_level": message.risk_level,
        "watchlist_category": message.watchlist_category,
        "watchlist_label": message.watchlist_label,
        "watchlist_priority": message.watchlist_priority,
    }


def _format_feedback_message_item(message: TelegramMessage) -> dict:
    return {
        "target_type": "message",
        "target_id": message.id,
        "feedback": message.feedback,
        "feedback_at": message.feedback_at.isoformat() if message.feedback_at else None,
        "title": None,
        "summary": message.summary_zh,
        "ai_decision": getattr(message, "ai_decision", None),
    }


def _format_feedback_event_item(event: Event) -> dict:
    return {
        "target_type": "event",
        "target_id": event.id,
        "feedback": event.feedback,
        "feedback_at": event.feedback_at.isoformat() if event.feedback_at else None,
        "title": event.event_title,
        "summary": event.latest_summary or event.event_summary,
    }


def _updated_row_count(result) -> int:
    rowcount = getattr(result, "rowcount", None)
    return int(rowcount or 0)


def _preferred_feedback_target(saved_targets: list[tuple[str, int]]) -> tuple[str, int]:
    for target in saved_targets:
        if target[0] == "event":
            return target
    return saved_targets[0]


def extract_original_url(text: str) -> str | None:
    match = URL_RE.search(text)
    return match.group(0).rstrip("。，,.)]") if match else None


def _message_event_title(message: TelegramMessage) -> str:
    payload = {}
    if message.analysis_json:
        try:
            raw_payload = json.loads(message.analysis_json)
            payload = raw_payload if isinstance(raw_payload, dict) else {}
        except json.JSONDecodeError:
            payload = {}
    return (
        normalize_event_title(payload.get("event_title"))
        or normalize_event_title(message.summary_zh)
        or normalize_event_title((message.cleaned_text or message.raw_text or "")[:80])
        or "未知事件"
    )


def _extract_feishu_action_value(payload: dict) -> dict:
    candidates = [
        payload.get("event", {}).get("action", {}).get("value"),
        payload.get("action", {}).get("value"),
        payload.get("value"),
    ]
    for candidate in candidates:
        if isinstance(candidate, dict):
            return candidate
    return {}


def _feedback_dedup_key(payload: dict, *, target_type: str, target_id: int, feedback: str, now: datetime) -> str:
    stable_action_id = _extract_feishu_stable_action_id(payload)
    if stable_action_id:
        raw = f"{target_type}:{target_id}:{feedback}:action:{stable_action_id}"
    else:
        actor_id = _extract_feishu_actor_id(payload) or "unknown"
        bucket = int(now.timestamp() // 300)
        raw = f"{target_type}:{target_id}:{feedback}:actor:{actor_id}:bucket:{bucket}"
    return sha256(raw.encode("utf-8")).hexdigest()


def _extract_feishu_stable_action_id(payload: dict) -> str | None:
    candidates = [
        payload.get("event", {}).get("action", {}).get("action_id"),
        payload.get("event", {}).get("context", {}).get("open_message_id"),
        payload.get("event", {}).get("event_id"),
        payload.get("header", {}).get("event_id"),
        payload.get("action_id"),
        payload.get("event_id"),
    ]
    for candidate in candidates:
        if candidate not in {None, ""}:
            return str(candidate)
    return None


def _extract_feishu_actor_id(payload: dict) -> str | None:
    operator = payload.get("event", {}).get("operator")
    if isinstance(operator, dict):
        for key in ("open_id", "user_id", "union_id"):
            if operator.get(key):
                return str(operator[key])
    user = payload.get("operator") or payload.get("user")
    if isinstance(user, dict):
        for key in ("open_id", "user_id", "union_id"):
            if user.get(key):
                return str(user[key])
    for key in ("open_id", "user_id", "union_id"):
        if payload.get(key):
            return str(payload[key])
    return None


def _safe_int(value) -> int | None:
    if value in {None, ""}:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def format_duplicate_backfill_payload(result: DuplicateBackfillResult, hours: int) -> dict:
    return {
        "hours": hours,
        "threshold": result.threshold,
        "dry_run": result.dry_run,
        "scanned_count": result.scanned_count,
        "matched_count": result.matched_count,
        "samples": [
            {
                "message_id": match.message_id,
                "duplicate_of_message_id": match.duplicate_of_message_id,
                "similarity_score": match.similarity_score,
                "summary": match.summary,
                "duplicate_summary": match.duplicate_summary,
            }
            for match in result.samples
        ],
    }


def format_stats_payload(**values) -> dict:
    average_score = values.get("average_score_last_24h")
    return {
        "total_messages": values.get("total_messages") or 0,
        "messages_last_24h": values.get("messages_last_24h") or 0,
        "ai_success_count": values.get("ai_success_count") or 0,
        "ai_failed_count": values.get("ai_failed_count") or 0,
        "pushed_sent_count": values.get("pushed_sent_count") or 0,
        "pushed_failed_count": values.get("pushed_failed_count") or 0,
        "skipped_low_score_count": values.get("skipped_low_score_count") or 0,
        "rate_limited_count": values.get("rate_limited_count") or 0,
        "skipped_duplicate_count": values.get("skipped_duplicate_count") or 0,
        "duplicate_skipped_count": values.get("duplicate_skipped_count") or values.get("skipped_duplicate_count") or 0,
        "event_duplicate_skipped_count": values.get("event_duplicate_skipped_count") or 0,
        "push_decision_count": values.get("push_decision_count") or 0,
        "watch_decision_count": values.get("watch_decision_count") or 0,
        "ignore_decision_count": values.get("ignore_decision_count") or 0,
        "push_sent_count": values.get("push_sent_count") or 0,
        "watch_sent_count": values.get("watch_sent_count") or 0,
        "ignore_skipped_count": values.get("ignore_skipped_count") or 0,
        "known_source_count": values.get("known_source_count") or 0,
        "unknown_source_count": values.get("unknown_source_count") or 0,
        "role_distribution": values.get("role_distribution") or {},
        "sent_today_count": values.get("sent_today_count") or 0,
        "sent_last_hour_count": values.get("sent_last_hour_count") or 0,
        "s_sent_last_hour_count": values.get("s_sent_last_hour_count") or 0,
        "a_sent_last_hour_count": values.get("a_sent_last_hour_count") or 0,
        "average_score_last_24h": round(float(average_score), 2) if average_score else 0,
        "s_count": values.get("s_count") or 0,
        "a_count": values.get("a_count") or 0,
        "b_count": values.get("b_count") or 0,
        "c_count": values.get("c_count") or 0,
        "event_count": values.get("event_count") or 0,
        "average_messages_per_event": round(float(values.get("average_messages_per_event")), 2)
        if values.get("average_messages_per_event")
        else 0,
        "telegram_channel_count": values.get("telegram_channel_count") or 0,
        "telegram_group_count": values.get("telegram_group_count") or 0,
        "telegram_messages_24h": values.get("telegram_messages_24h") or 0,
        "telegram_events_24h": values.get("telegram_events_24h") or 0,
        "watchlist_counts": values.get("watchlist_counts") or [],
        "watchlist_avg_scores": values.get("watchlist_avg_scores") or [],
    }


def _telegram_channel_filter(channel: str):
    normalized = channel.strip().lstrip("@")
    metadata_marker = f'"telegram_channel": "{channel}"'
    metadata_marker_without_at = f'"telegram_channel": "{normalized}"'
    return or_(
        TelegramMessage.analysis_json.contains(metadata_marker),
        TelegramMessage.analysis_json.contains(metadata_marker_without_at),
        func.lower(TelegramMessage.source_chat_title) == normalized.lower(),
        func.lower(TelegramMessage.source_chat_title) == f"@{normalized.lower()}",
    )


def _source_profile_stats(analysis_json_values: list[str | None]) -> dict:
    known_source_count = 0
    unknown_source_count = 0
    role_distribution: dict[str, int] = {}
    for raw_value in analysis_json_values:
        profile = _source_profile_from_analysis_json(raw_value)
        role = str((profile or {}).get("role") or "unknown")
        if profile and role != "unknown":
            known_source_count += 1
        else:
            unknown_source_count += 1
        role_distribution[role] = role_distribution.get(role, 0) + 1
    return {
        "known_source_count": known_source_count,
        "unknown_source_count": unknown_source_count,
        "role_distribution": dict(sorted(role_distribution.items(), key=lambda item: (-item[1], item[0]))),
    }


def _source_profile_from_analysis_json(raw_value: str | None) -> dict | None:
    if not raw_value:
        return None
    try:
        payload = json.loads(raw_value)
    except json.JSONDecodeError:
        return None
    if not isinstance(payload, dict):
        return None
    source_context = payload.get("source_context")
    if isinstance(source_context, dict) and isinstance(source_context.get("source_profile"), dict):
        return source_context["source_profile"]
    breakdown = payload.get("score_breakdown")
    if isinstance(breakdown, dict) and isinstance(breakdown.get("source_profile"), dict):
        return breakdown["source_profile"]
    return None
