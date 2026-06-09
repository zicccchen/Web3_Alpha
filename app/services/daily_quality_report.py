from __future__ import annotations

from datetime import datetime, timedelta, timezone
import json

from sqlalchemy import desc, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models import Analysis, Event, EventRecord, Feedback, Record, TelegramMessage
from app.services.notifier import NotificationResult


DEFAULT_PLATFORM_COUNTS = {
    "telegram": 0,
    "telegram_public": 0,
    "x": 0,
    "discord": 0,
}


async def build_daily_quality_report(db: AsyncSession, *, hours: int = 24) -> dict:
    end = datetime.now(timezone.utc)
    start = end - timedelta(hours=hours)

    records_count = await _count_since(db, Record, Record.created_at, start)
    analyses_count = await _count_since(db, Analysis, Analysis.created_at, start)
    events_count = await _count_since(db, Event, Event.first_seen_at, start)
    event_records_count = await _count_since(db, EventRecord, EventRecord.created_at, start)
    ai_failed_count = await db.scalar(
        select(func.count())
        .select_from(TelegramMessage)
        .where(TelegramMessage.created_at >= start)
        .where(TelegramMessage.analysis_json.contains('"ai_error"'))
    )

    by_platform = dict(DEFAULT_PLATFORM_COUNTS)
    platform_rows = await db.execute(
        select(Record.source_platform, func.count().label("count"))
        .where(Record.created_at >= start)
        .group_by(Record.source_platform)
        .order_by(desc(func.count()))
    )
    for row in platform_rows.mappings():
        by_platform[str(row["source_platform"])] = int(row["count"] or 0)

    top_sources_by_records = await _top_record_sources(db, start)
    top_sources_by_push = await _top_push_sources(db, start)

    decision_counts = await _decision_counts(db, start)
    feedback = await _feedback_stats(db, start, decision_counts)
    source_profiles = await _source_profile_stats(db, start)
    events = await _event_stats(db, start)

    report = {
        "time_range": {
            "hours": hours,
            "start": start.isoformat(),
            "end": end.isoformat(),
        },
        "pipeline": {
            "records_count": int(records_count or 0),
            "analyses_count": int(analyses_count or 0),
            "events_count": int(events_count or 0),
            "event_records_count": int(event_records_count or 0),
            "ai_failed_count": int(ai_failed_count or 0),
        },
        "sources": {
            "by_platform": by_platform,
            "top_sources_by_records": top_sources_by_records,
            "top_sources_by_push": top_sources_by_push,
            "source_error_count": 0,
        },
        "decisions": decision_counts,
        "events": events,
        "feedback": feedback,
        "source_profiles": source_profiles,
        "quality_warnings": [],
    }
    report["quality_warnings"] = build_quality_warnings(report)
    return report


async def push_daily_quality_report(report: dict, notifier) -> NotificationResult:
    card = build_daily_quality_report_card(report)
    if hasattr(notifier, "notify_card"):
        return await notifier.notify_card(card)
    if hasattr(notifier, "_notify_with_app_bot") and notifier.app_id and notifier.app_secret and notifier.chat_id:
        return await notifier._notify_with_app_bot(card)
    return await notifier._notify_with_webhook(card)


def build_quality_warnings(report: dict) -> list[str]:
    warnings: list[str] = []
    records_count = int(report.get("pipeline", {}).get("records_count") or 0)
    unknown_source_rate = float(report.get("source_profiles", {}).get("unknown_source_rate") or 0)
    failed_push_count = int(report.get("decisions", {}).get("failed_push_count") or 0)
    ai_failed_count = int(report.get("pipeline", {}).get("ai_failed_count") or 0)
    skipped_event_duplicate_count = int(report.get("decisions", {}).get("skipped_event_duplicate_count") or 0)
    push_count = int(report.get("decisions", {}).get("push_count") or 0)
    watch_count = int(report.get("decisions", {}).get("watch_count") or 0)
    push_bad_rate = float(report.get("feedback", {}).get("push_bad_rate") or 0)
    feedback_rate = float(report.get("feedback", {}).get("feedback_rate") or 0)

    if unknown_source_rate > 0.30:
        warnings.append("Unknown source rate is high. Consider adding source_profiles.")
    if failed_push_count > 0:
        warnings.append("Feishu push failures detected.")
    if ai_failed_count > 0:
        warnings.append("AI analysis failures detected.")
    if records_count >= 30 and skipped_event_duplicate_count < max(1, int(records_count * 0.03)):
        warnings.append("Event dedup may be weak. Check Event Cluster.")
    if records_count > 0 and (push_count + watch_count) / records_count > 0.60:
        warnings.append("Push/Watch ratio is high. User profile or AI prompt may be too loose.")
    if push_bad_rate > 0.30:
        warnings.append("Push bad rate is high. Need calibration.")
    if records_count > 0 and feedback_rate < 0.05:
        warnings.append("Feedback rate is low. Calibration data may be insufficient.")
    return warnings


def build_daily_quality_report_card(report: dict) -> dict:
    pipeline = report.get("pipeline", {})
    decisions = report.get("decisions", {})
    events = report.get("events", {})
    feedback = report.get("feedback", {})
    source_profiles = report.get("source_profiles", {})
    warnings = report.get("quality_warnings") or []
    top_events = events.get("top_events") or []
    top_event_lines = [
        f"{idx}. #{event.get('event_id')} {event.get('event_title')} "
        f"({event.get('message_count')} msgs/{event.get('source_count')} sources)"
        for idx, event in enumerate(top_events[:5], start=1)
    ]
    warning_lines = [f"- {item}" for item in warnings] or ["- none"]
    return {
        "config": {"wide_screen_mode": True},
        "header": {
            "template": "blue",
            "title": {"tag": "plain_text", "content": "【Web3 Alpha Daily Quality Report】"},
        },
        "elements": [
            {"tag": "markdown", "content": f"**统计窗口**：最近 {report.get('time_range', {}).get('hours', 24)} 小时"},
            {"tag": "markdown", "content": f"**今日采集 records**：{pipeline.get('records_count', 0)}"},
            {"tag": "markdown", "content": f"**新事件数**：{events.get('new_event_count', 0)}"},
            {
                "tag": "markdown",
                "content": (
                    f"**Push / Watch / Ignore**："
                    f"{decisions.get('push_count', 0)} / {decisions.get('watch_count', 0)} / {decisions.get('ignore_count', 0)}"
                ),
            },
            {
                "tag": "markdown",
                "content": (
                    f"**飞书推送成功/失败**："
                    f"{decisions.get('push_sent_count', 0) + decisions.get('watch_sent_count', 0)} / "
                    f"{decisions.get('failed_push_count', 0)}"
                ),
            },
            {"tag": "markdown", "content": f"**事件重复拦截数**：{events.get('event_duplicate_skipped_count', 0)}"},
            {"tag": "markdown", "content": f"**事件升级推送数**：{events.get('upgrade_sent_count', 0)}"},
            {
                "tag": "markdown",
                "content": (
                    f"**反馈 good / bad / ignore**："
                    f"{feedback.get('good_count', 0)} / {feedback.get('bad_count', 0)} / {feedback.get('ignore_count', 0)}"
                ),
            },
            {
                "tag": "markdown",
                "content": f"**unknown source rate**：{round(float(source_profiles.get('unknown_source_rate', 0)) * 100, 2)}%",
            },
            {"tag": "markdown", "content": "**Top 5 Events**\n" + ("\n".join(top_event_lines) if top_event_lines else "none")},
            {"tag": "markdown", "content": "**Quality Warnings**\n" + "\n".join(warning_lines)},
        ],
    }


async def _count_since(db: AsyncSession, model, timestamp_column, start: datetime) -> int:
    return int(
        await db.scalar(select(func.count()).select_from(model).where(timestamp_column >= start))
        or 0
    )


async def _top_record_sources(db: AsyncSession, start: datetime) -> list[dict]:
    rows = await db.execute(
        select(Record.source_platform, Record.source_channel, func.count().label("count"))
        .where(Record.created_at >= start)
        .group_by(Record.source_platform, Record.source_channel)
        .order_by(desc(func.count()))
        .limit(10)
    )
    return [
        {
            "source_platform": row["source_platform"],
            "source": row["source_channel"],
            "count": int(row["count"] or 0),
        }
        for row in rows.mappings()
    ]


async def _top_push_sources(db: AsyncSession, start: datetime) -> list[dict]:
    source_expr = func.coalesce(TelegramMessage.source_chat_title, TelegramMessage.source_chat_id)
    rows = await db.execute(
        select(TelegramMessage.source_platform, source_expr.label("source"), func.count().label("count"))
        .where(TelegramMessage.created_at >= start)
        .where(TelegramMessage.push_status == "sent")
        .group_by(TelegramMessage.source_platform, source_expr)
        .order_by(desc(func.count()))
        .limit(10)
    )
    return [
        {
            "source_platform": row["source_platform"],
            "source": row["source"],
            "count": int(row["count"] or 0),
        }
        for row in rows.mappings()
    ]


async def _decision_counts(db: AsyncSession, start: datetime) -> dict:
    decision_rows = await db.execute(
        select(Analysis.ai_decision, func.count().label("count"))
        .where(Analysis.created_at >= start)
        .group_by(Analysis.ai_decision)
    )
    decisions = {"push": 0, "watch": 0, "ignore": 0}
    for row in decision_rows.mappings():
        decision = str(row["ai_decision"] or "unknown")
        if decision in decisions:
            decisions[decision] = int(row["count"] or 0)
    push_sent_count = await _message_count(
        db,
        start,
        TelegramMessage.ai_decision == "push",
        TelegramMessage.push_status == "sent",
    )
    watch_sent_count = await _message_count(
        db,
        start,
        TelegramMessage.ai_decision == "watch",
        TelegramMessage.push_status == "sent",
    )
    return {
        "push_count": decisions["push"],
        "watch_count": decisions["watch"],
        "ignore_count": decisions["ignore"],
        "push_sent_count": push_sent_count,
        "watch_sent_count": watch_sent_count,
        "skipped_ignore_count": await _message_count(db, start, TelegramMessage.push_status == "skipped_ignore"),
        "skipped_event_duplicate_count": await _message_count(
            db,
            start,
            TelegramMessage.push_status == "skipped_event_duplicate",
        ),
        "failed_push_count": await _message_count(db, start, TelegramMessage.push_status == "failed"),
    }


async def _message_count(db: AsyncSession, start: datetime, *filters) -> int:
    query = select(func.count()).select_from(TelegramMessage).where(TelegramMessage.created_at >= start)
    for condition in filters:
        query = query.where(condition)
    return int(await db.scalar(query) or 0)


async def _event_stats(db: AsyncSession, start: datetime) -> dict:
    top_events_rows = await db.execute(
        select(Event)
        .where(Event.last_seen_at >= start)
        .order_by(
            desc(Event.last_pushed_at.is_not(None)),
            desc(Event.max_score),
            desc(Event.message_count),
            desc(Event.source_count),
            desc(Event.last_seen_at),
        )
        .limit(10)
    )
    top_events = []
    for event in top_events_rows.scalars().all():
        top_events.append(
            {
                "event_id": event.id,
                "event_title": event.event_title,
                "latest_summary": event.latest_summary or event.event_summary,
                "message_count": int(event.message_count or 0),
                "source_count": int(event.source_count or 0),
                "max_score": float(event.max_score or 0),
                "ai_decision": await _latest_event_decision(db, event.id),
                "feedback": event.feedback,
                "last_seen_at": event.last_seen_at.isoformat() if event.last_seen_at else None,
            }
        )
    return {
        "new_event_count": await _count_since(db, Event, Event.first_seen_at, start),
        "merged_event_count": int(
            await db.scalar(
                select(func.count())
                .select_from(Event)
                .where(Event.status == "merged")
                .where(Event.last_seen_at >= start)
            )
            or 0
        ),
        "event_duplicate_skipped_count": await _message_count(
            db,
            start,
            TelegramMessage.push_status == "skipped_event_duplicate",
        ),
        "upgrade_sent_count": int(
            await db.scalar(
                select(func.count())
                .select_from(Event)
                .where(Event.last_upgrade_at >= start)
                .where(Event.last_pushed_at >= start)
            )
            or 0
        ),
        "top_events": top_events,
    }


async def _latest_event_decision(db: AsyncSession, event_id: int) -> str | None:
    row = await db.execute(
        select(TelegramMessage.ai_decision)
        .where(TelegramMessage.event_id == event_id)
        .order_by(desc(TelegramMessage.created_at), desc(TelegramMessage.id))
        .limit(1)
    )
    return row.scalar_one_or_none()


async def _feedback_stats(db: AsyncSession, start: datetime, decision_counts: dict) -> dict:
    rows = await db.execute(
        select(Feedback.feedback, func.count().label("count"))
        .where(Feedback.created_at >= start)
        .group_by(Feedback.feedback)
    )
    counts = {"good": 0, "bad": 0, "ignore": 0}
    for row in rows.mappings():
        feedback = str(row["feedback"])
        if feedback in counts:
            counts[feedback] = int(row["count"] or 0)
    total_feedback = sum(counts.values())
    sent_count = int(decision_counts.get("push_sent_count") or 0) + int(decision_counts.get("watch_sent_count") or 0)

    decision_feedback_rows = await db.execute(
        select(TelegramMessage.ai_decision, Feedback.feedback, func.count().label("count"))
        .join(TelegramMessage, TelegramMessage.id == Feedback.legacy_message_id)
        .where(Feedback.created_at >= start)
        .group_by(TelegramMessage.ai_decision, Feedback.feedback)
    )
    push_total = push_bad = watch_total = watch_good = 0
    for row in decision_feedback_rows.mappings():
        decision = row["ai_decision"]
        feedback = row["feedback"]
        count = int(row["count"] or 0)
        if decision == "push":
            push_total += count
            if feedback == "bad":
                push_bad += count
        if decision == "watch":
            watch_total += count
            if feedback == "good":
                watch_good += count

    latest_rows = await db.execute(
        select(Feedback)
        .where(Feedback.created_at >= start)
        .order_by(desc(Feedback.created_at), desc(Feedback.feedback_id))
        .limit(10)
    )
    latest_feedback = [
        {
            "feedback_id": item.feedback_id,
            "target_type": item.target_type,
            "record_id": item.record_id,
            "event_id": item.event_id,
            "feedback": item.feedback,
            "feedback_source": item.feedback_source,
            "created_at": item.created_at.isoformat() if item.created_at else None,
        }
        for item in latest_rows.scalars().all()
    ]
    return {
        "good_count": counts["good"],
        "bad_count": counts["bad"],
        "ignore_count": counts["ignore"],
        "feedback_rate": _ratio(total_feedback, sent_count),
        "push_bad_rate": _ratio(push_bad, push_total),
        "watch_good_rate": _ratio(watch_good, watch_total),
        "latest_feedback": latest_feedback,
    }


async def _source_profile_stats(db: AsyncSession, start: datetime) -> dict:
    rows = await db.execute(
        select(Analysis.source_profile, Record.source_channel)
        .join(Record, Record.record_id == Analysis.record_id)
        .where(Analysis.created_at >= start)
    )
    known = 0
    unknown = 0
    unknown_sources: dict[str, int] = {}
    for raw_profile, source_channel in rows.all():
        profile = _json(raw_profile)
        role = str((profile or {}).get("role") or "unknown") if isinstance(profile, dict) else "unknown"
        if profile and role != "unknown":
            known += 1
        else:
            unknown += 1
            source = str(source_channel or "unknown")
            unknown_sources[source] = unknown_sources.get(source, 0) + 1
    total = known + unknown
    return {
        "known_source_count": known,
        "unknown_source_count": unknown,
        "unknown_source_rate": _ratio(unknown, total),
        "top_unknown_sources": [
            {"source": source, "count": count}
            for source, count in sorted(unknown_sources.items(), key=lambda item: (-item[1], item[0]))[:10]
        ],
    }


def _ratio(numerator: int | float, denominator: int | float) -> float:
    if not denominator:
        return 0
    return round(float(numerator) / float(denominator), 4)


def _json(value: str | None):
    if not value:
        return None
    try:
        return json.loads(value)
    except json.JSONDecodeError:
        return None
