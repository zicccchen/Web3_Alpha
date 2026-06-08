from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from app.services.scorer import load_score_rules, load_signal_rules


VALID_FEEDBACK = {"good", "bad", "ignore"}


@dataclass
class CalibrationBucket:
    key: str
    label: str | None = None
    good: int = 0
    bad: int = 0
    ignore: int = 0
    score_sum: float = 0

    def add(self, feedback: str, score: float) -> None:
        if feedback == "good":
            self.good += 1
        elif feedback == "bad":
            self.bad += 1
        elif feedback == "ignore":
            self.ignore += 1
        self.score_sum += float(score or 0)

    @property
    def total(self) -> int:
        return self.good + self.bad + self.ignore

    @property
    def quality_score(self) -> float:
        if not self.total:
            return 0
        return round((self.good - self.bad - self.ignore * 0.5) / self.total, 4)

    def to_dict(self) -> dict:
        return {
            "key": self.key,
            "label": self.label or self.key,
            "good": self.good,
            "bad": self.bad,
            "ignore": self.ignore,
            "total": self.total,
            "good_rate": round(self.good / self.total, 4) if self.total else 0,
            "bad_rate": round(self.bad / self.total, 4) if self.total else 0,
            "ignore_rate": round(self.ignore / self.total, 4) if self.total else 0,
            "quality_score": self.quality_score,
            "average_score": round(self.score_sum / self.total, 2) if self.total else 0,
        }


@dataclass
class EventBucket:
    event_id: int | None
    event_title: str
    good: int = 0
    bad: int = 0
    ignore: int = 0
    max_score: float = 0
    summaries: list[str] = field(default_factory=list)

    def add(self, feedback: str, score: float, summary: str | None) -> None:
        if feedback == "good":
            self.good += 1
        elif feedback == "bad":
            self.bad += 1
        elif feedback == "ignore":
            self.ignore += 1
        self.max_score = max(self.max_score, float(score or 0))
        if summary and len(self.summaries) < 3 and summary not in self.summaries:
            self.summaries.append(summary)

    @property
    def total_feedback(self) -> int:
        return self.good + self.bad + self.ignore

    def to_dict(self) -> dict:
        return {
            "event_id": self.event_id,
            "event_title": self.event_title,
            "good": self.good,
            "bad": self.bad,
            "ignore": self.ignore,
            "total_feedback": self.total_feedback,
            "max_score": round(self.max_score, 2),
            "sample_summaries": self.summaries,
        }


def build_calibration_report(messages: list[dict], days: int) -> dict:
    feedback_counts = {"good": 0, "bad": 0, "ignore": 0}
    signal_buckets: dict[str, CalibrationBucket] = {}
    keyword_buckets: dict[str, CalibrationBucket] = {}
    watchlist_buckets: dict[str, CalibrationBucket] = {}
    decision_buckets: dict[str, CalibrationBucket] = {}
    event_buckets: dict[str, EventBucket] = {}

    for message in messages:
        feedback = str(message.get("feedback") or "").lower()
        if feedback not in VALID_FEEDBACK:
            continue

        score = float(message.get("score") or 0)
        feedback_counts[feedback] += 1
        breakdown = message.get("score_breakdown") or {}
        ai_decision = str(message.get("ai_decision") or "unknown")
        _bucket(decision_buckets, ai_decision, ai_decision).add(feedback, score)

        signal_type = str(breakdown.get("signal_type") or "unknown")
        signal_label = str(breakdown.get("signal_label") or signal_type)
        _bucket(signal_buckets, signal_type, signal_label).add(feedback, score)

        for keyword in _list_value(breakdown.get("matched_keywords")):
            _bucket(keyword_buckets, keyword.lower(), keyword.lower()).add(feedback, score)

        watchlist_key = str(message.get("watchlist_category") or "uncategorized")
        watchlist_label = str(message.get("watchlist_label") or "未分类")
        _bucket(watchlist_buckets, watchlist_key, watchlist_label).add(feedback, score)

        event_key = _event_key(message)
        event_title = str(message.get("event_title") or message.get("summary_zh") or event_key)
        if event_key not in event_buckets:
            event_buckets[event_key] = EventBucket(
                event_id=_safe_int(message.get("event_id")),
                event_title=event_title,
            )
        event_buckets[event_key].add(feedback, score, message.get("summary_zh"))

    signal_rankings = _rank_buckets(signal_buckets)
    keyword_rankings = _rank_buckets(keyword_buckets)
    watchlist_rankings = _rank_buckets(watchlist_buckets)

    return {
        "days": days,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "feedback_counts": feedback_counts,
        "feedback_total": sum(feedback_counts.values()),
        "decision_rankings": _rank_buckets(decision_buckets),
        "signal_type_rankings": signal_rankings,
        "keyword_rankings": keyword_rankings,
        "watchlist_rankings": watchlist_rankings,
        "top_good_events": _top_events(event_buckets.values(), "good"),
        "top_bad_events": _top_events(event_buckets.values(), "bad"),
        "recommended_keyword_bonus_adjustments": _recommend_keyword_bonus(keyword_rankings),
        "recommended_signal_bonus_adjustments": _recommend_signal_bonus(signal_rankings),
    }


def _bucket(buckets: dict[str, CalibrationBucket], key: str, label: str | None = None) -> CalibrationBucket:
    if key not in buckets:
        buckets[key] = CalibrationBucket(key=key, label=label)
    return buckets[key]


def _rank_buckets(buckets: dict[str, CalibrationBucket]) -> list[dict]:
    return [
        bucket.to_dict()
        for bucket in sorted(
            buckets.values(),
            key=lambda item: (item.quality_score, item.good, -item.bad, item.total),
            reverse=True,
        )
    ]


def _top_events(events: list[EventBucket] | Any, feedback: str) -> list[dict]:
    return [
        event.to_dict()
        for event in sorted(
            [event for event in events if getattr(event, feedback) > 0],
            key=lambda event: (getattr(event, feedback), event.max_score, event.total_feedback),
            reverse=True,
        )[:10]
    ]


def _recommend_keyword_bonus(keyword_rankings: list[dict]) -> list[dict]:
    rules = load_score_rules()
    recommendations = []
    for row in keyword_rankings:
        if row["total"] < 2:
            continue
        current_bonus = float(rules.keywords.get(row["key"], 0))
        delta = _recommended_delta(row)
        if delta == 0:
            continue
        recommendations.append(
            {
                "keyword": row["key"],
                "current_bonus": current_bonus,
                "suggested_bonus": max(0, round(current_bonus + delta, 2)),
                "suggested_delta": delta,
                "reason": _recommendation_reason(row),
                "stats": row,
            }
        )
    return recommendations[:20]


def _recommend_signal_bonus(signal_rankings: list[dict]) -> list[dict]:
    rules = load_signal_rules()
    recommendations = []
    for row in signal_rankings:
        if row["total"] < 2:
            continue
        current_bonus = float(rules.signal_types.get(row["key"], rules.signal_types["unknown"]).bonus)
        delta = _recommended_delta(row)
        if delta == 0:
            continue
        recommendations.append(
            {
                "signal_type": row["key"],
                "label": row["label"],
                "current_bonus": current_bonus,
                "suggested_bonus": max(0, round(current_bonus + delta, 2)),
                "suggested_delta": delta,
                "reason": _recommendation_reason(row),
                "stats": row,
            }
        )
    return recommendations[:20]


def _recommended_delta(row: dict) -> float:
    if row["good_rate"] >= 0.7 and row["bad_rate"] <= 0.15 and row["ignore_rate"] <= 0.3:
        return 2
    if row["bad_rate"] >= 0.5 or row["ignore_rate"] >= 0.7:
        return -2
    if row["good_rate"] >= 0.55 and row["bad_rate"] <= 0.25:
        return 1
    if row["bad_rate"] >= 0.35 or row["ignore_rate"] >= 0.55:
        return -1
    return 0


def _recommendation_reason(row: dict) -> str:
    return (
        f"近样本 total={row['total']} good_rate={row['good_rate']} "
        f"bad_rate={row['bad_rate']} ignore_rate={row['ignore_rate']}"
    )


def _list_value(value) -> list[str]:
    if not isinstance(value, list):
        return []
    return [str(item).strip() for item in value if str(item).strip()]


def _event_key(message: dict) -> str:
    event_id = message.get("event_id")
    if event_id is not None:
        return f"event:{event_id}"
    message_id = message.get("id")
    return f"message:{message_id}" if message_id is not None else str(message.get("summary_zh") or "unknown")


def _safe_int(value) -> int | None:
    try:
        return int(value) if value is not None else None
    except (TypeError, ValueError):
        return None
