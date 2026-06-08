from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
import json
from types import SimpleNamespace
from typing import Iterable

from app.services.event_cluster import (
    best_event_match,
    event_key_for_title,
    extract_event_features,
    normalize_event_title,
    rank_event_candidates,
    score_event_candidate,
)


@dataclass
class BackfillMessageAssignment:
    message: object
    event_index: int
    event_similarity: float
    event_match_reason: str


@dataclass
class BackfillEventPlan:
    event_title: str
    event_summary: str
    event_key: str
    existing_event_id: int | None = None
    messages: list[object] = field(default_factory=list)
    assignments: list[BackfillMessageAssignment] = field(default_factory=list)

    @property
    def message_count(self) -> int:
        return len(self.messages)

    @property
    def source_count(self) -> int:
        return len({getattr(message, "source_chat_id", "") for message in self.messages})

    @property
    def first_seen_at(self):
        values = [getattr(message, "created_at", None) for message in self.messages if getattr(message, "created_at", None)]
        return min(values) if values else None

    @property
    def last_seen_at(self):
        values = [getattr(message, "created_at", None) for message in self.messages if getattr(message, "created_at", None)]
        return max(values) if values else None


@dataclass
class EventBackfillResult:
    scanned_count: int
    event_count: int
    created_event_count: int
    updated_message_count: int
    dry_run: bool
    events: list[BackfillEventPlan]
    duplicate_event_pairs: list[dict] = field(default_factory=list)


def plan_event_backfill(messages: Iterable[object], existing_events: Iterable[object] = (), dry_run: bool = True) -> EventBackfillResult:
    plans: list[BackfillEventPlan] = []
    candidates: list[object] = list(existing_events)
    scanned_count = 0

    for message in messages:
        scanned_count += 1
        event_title = _message_event_title(message)
        event_summary = getattr(message, "summary_zh", None) or _message_text(message)[:160]
        match = best_event_match(event_title, event_summary, _message_text(message), candidates)

        if match:
            event_index = int(getattr(match.event, "_backfill_event_index", -1))
            if event_index >= 0:
                plan = plans[event_index]
            else:
                plan = BackfillEventPlan(
                    event_title=getattr(match.event, "event_title", event_title),
                    event_summary=getattr(match.event, "event_summary", event_summary) or event_summary,
                    event_key=getattr(match.event, "event_key", event_key_for_title(getattr(match.event, "event_title", event_title))),
                    existing_event_id=int(getattr(match.event, "id")),
                )
                plans.append(plan)
                event_index = len(plans) - 1
                candidates.append(_candidate_from_plan(plan, event_index))
            assignment = BackfillMessageAssignment(
                message=message,
                event_index=event_index,
                event_similarity=match.similarity,
                event_match_reason=match.reason,
            )
        else:
            plan = BackfillEventPlan(
                event_title=event_title,
                event_summary=event_summary,
                event_key=event_key_for_title(event_title),
            )
            plans.append(plan)
            event_index = len(plans) - 1
            candidates.append(_candidate_from_plan(plan, event_index))
            assignment = BackfillMessageAssignment(
                message=message,
                event_index=event_index,
                event_similarity=1.0,
                event_match_reason="new_event",
            )

        plans[assignment.event_index].messages.append(message)
        plans[assignment.event_index].assignments.append(assignment)

    created_event_count = sum(1 for plan in plans if plan.existing_event_id is None)
    duplicate_event_pairs = find_duplicate_event_pairs(plans, candidates)
    return EventBackfillResult(
        scanned_count=scanned_count,
        event_count=len(plans),
        created_event_count=created_event_count,
        updated_message_count=sum(plan.message_count for plan in plans),
        dry_run=dry_run,
        events=plans,
        duplicate_event_pairs=duplicate_event_pairs,
    )


def format_event_backfill_payload(result: EventBackfillResult, hours: int, sample_limit: int = 20) -> dict:
    return {
        "hours": hours,
        "dry_run": result.dry_run,
        "scanned_count": result.scanned_count,
        "event_count": result.event_count,
        "would_create_event_count": result.created_event_count if result.dry_run else 0,
        "would_merge_event_count": len(result.duplicate_event_pairs),
        "created_event_count": result.created_event_count,
        "updated_message_count": result.updated_message_count,
        "top_potential_merges": result.duplicate_event_pairs[:sample_limit],
        "duplicate_event_pairs": result.duplicate_event_pairs,
        "samples": [
            {
                "event_title": event.event_title,
                "message_count": event.message_count,
                "source_count": event.source_count,
                "sample_messages": [_format_sample_message(message) for message in event.messages[:5]],
            }
            for event in result.events[:sample_limit]
        ],
    }


def find_duplicate_event_pairs(plans: list[BackfillEventPlan], candidates: list[object]) -> list[dict]:
    event_like_by_id: dict[int, object] = {
        int(getattr(candidate, "id")): candidate
        for candidate in candidates
        if getattr(candidate, "id", None) is not None
    }
    for index, plan in enumerate(plans):
        candidate = _candidate_from_plan(plan, index)
        event_like_by_id[int(getattr(candidate, "id"))] = candidate
    event_like = list(event_like_by_id.values())
    pairs: list[dict] = []
    feature_cache = {
        int(getattr(event, "id")): extract_event_features(_event_text(event))
        for event in event_like
        if getattr(event, "id", None) is not None
    }
    seen_pairs: set[tuple[int, int]] = set()
    for index, left in enumerate(event_like):
        left_id = int(getattr(left, "id"))
        left_features = feature_cache.get(left_id)
        if not left_features:
            continue
        for right in event_like[index + 1 :]:
            right_id = int(getattr(right, "id"))
            right_features = feature_cache.get(right_id)
            if not right_features:
                continue
            if not _has_candidate_overlap(left_features, right_features):
                continue
            detail = score_event_candidate(
                getattr(left, "event_title", "") or "",
                getattr(left, "event_summary", "") or "",
                getattr(left, "event_summary", "") or "",
                _event_text(left),
                left_features,
                right,
            )
            if not detail.matched:
                continue
            pair_key = tuple(sorted((left_id, right_id)))
            if pair_key in seen_pairs:
                continue
            seen_pairs.add(pair_key)
            pairs.append(
                {
                    "source_event_id": left_id,
                    "target_event_id": right_id,
                    "source_event_title": getattr(left, "event_title", None),
                    "target_event_title": getattr(right, "event_title", None),
                    "final_match_score": detail.final_match_score,
                    "reason": detail.reason,
                }
            )
    pairs.sort(key=lambda item: item["final_match_score"], reverse=True)
    for item in pairs:
        item.pop("_pair_key", None)
    return pairs


def _event_text(event: object) -> str:
    return " ".join(
        part for part in (getattr(event, "event_title", None), getattr(event, "event_summary", None)) if part
    )


def _has_candidate_overlap(left, right) -> bool:
    left_entities = left.entities | left.projects | left.tokens
    right_entities = right.entities | right.projects | right.tokens
    if not (left_entities & right_entities):
        return False
    return bool((left.key_phrases & right.key_phrases) or (left.tokens & right.tokens) or (left.numbers & right.numbers))


def apply_event_backfill_marks(result: EventBackfillResult, next_event_id: int = 1) -> list[object]:
    if result.dry_run:
        return []

    written_events: list[object] = []
    event_ids_by_index: dict[int, int] = {}
    for index, plan in enumerate(result.events):
        event_id = plan.existing_event_id or next_event_id
        if plan.existing_event_id is None:
            next_event_id += 1
        event_ids_by_index[index] = event_id
        written_events.append(
            SimpleNamespace(
                id=event_id,
                event_key=plan.event_key,
                event_title=plan.event_title,
                event_summary=plan.event_summary,
                message_count=plan.message_count,
                source_count=plan.source_count,
                max_score=max((float(getattr(message, "score", 0) or 0) for message in plan.messages), default=0),
                latest_summary=getattr(plan.messages[-1], "summary_zh", None) if plan.messages else None,
            )
        )

    for plan in result.events:
        for assignment in plan.assignments:
            setattr(assignment.message, "event_id", event_ids_by_index[assignment.event_index])
            setattr(assignment.message, "event_similarity", assignment.event_similarity)
            setattr(assignment.message, "event_match_reason", assignment.event_match_reason)

    return written_events


def _candidate_from_plan(plan: BackfillEventPlan, event_index: int) -> object:
    return SimpleNamespace(
        id=plan.existing_event_id or -(event_index + 1),
        _backfill_event_index=event_index,
        event_key=plan.event_key,
        event_title=plan.event_title,
        event_summary=plan.event_summary,
    )


def _message_event_title(message: object) -> str:
    payload = _analysis_payload(message)
    title = payload.get("event_title") if isinstance(payload, dict) else None
    return (
        normalize_event_title(title)
        or normalize_event_title(getattr(message, "summary_zh", None))
        or normalize_event_title(_message_text(message)[:80])
        or "未知事件"
    )


def _analysis_payload(message: object) -> dict:
    value = getattr(message, "analysis_json", None)
    if not value:
        return {}
    if isinstance(value, dict):
        return value
    try:
        payload = json.loads(value)
        return payload if isinstance(payload, dict) else {}
    except json.JSONDecodeError:
        return {}


def _message_text(message: object) -> str:
    return getattr(message, "cleaned_text", None) or getattr(message, "raw_text", None) or ""


def _format_sample_message(message: object) -> dict:
    return {
        "id": getattr(message, "id", None),
        "source_channel": getattr(message, "source_chat_title", None) or getattr(message, "source_chat_id", None),
        "summary": getattr(message, "summary_zh", None),
        "created_at": _format_datetime(getattr(message, "created_at", None)),
    }


def _format_datetime(value) -> str | None:
    if isinstance(value, datetime):
        return value.isoformat()
    return None
