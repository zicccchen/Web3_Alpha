from __future__ import annotations

import json
from datetime import datetime

from sqlalchemy import BigInteger, Boolean, DateTime, Float, String, Text, UniqueConstraint, func
from sqlalchemy import ForeignKey
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base
from app.services.scorer import load_signal_rules, normalize_signal_type


class TelegramMessage(Base):
    __tablename__ = "telegram_messages"
    __table_args__ = (UniqueConstraint("dedup_key", name="uq_telegram_messages_dedup_key"),)

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    source: Mapped[str] = mapped_column(String(32), default="telegram", nullable=False)
    source_platform: Mapped[str] = mapped_column(String(32), default="telegram", nullable=False, index=True)
    source_chat_id: Mapped[str] = mapped_column(String(128), nullable=False, index=True)
    source_chat_title: Mapped[str | None] = mapped_column(String(255), nullable=True)
    source_message_id: Mapped[str] = mapped_column(String(128), nullable=False, index=True)
    author_name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    raw_text: Mapped[str] = mapped_column(Text, nullable=False)
    cleaned_text: Mapped[str] = mapped_column(Text, nullable=False)
    dedup_key: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    language: Mapped[str] = mapped_column(String(16), default="unknown", nullable=False)
    summary_zh: Mapped[str | None] = mapped_column(Text, nullable=True)
    category: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)
    score: Mapped[float] = mapped_column(Float, default=0, nullable=False, index=True)
    signal_level: Mapped[str] = mapped_column(String(1), default="C", nullable=False, index=True)
    analysis_json: Mapped[str | None] = mapped_column(Text, nullable=True)
    push_sent: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    push_status: Mapped[str] = mapped_column(String(32), default="pending", nullable=False, index=True)
    push_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    pushed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    possible_duplicate: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False, index=True)
    duplicate_of_message_id: Mapped[int | None] = mapped_column(BigInteger, nullable=True, index=True)
    similarity_score: Mapped[float | None] = mapped_column(Float, nullable=True)
    event_id: Mapped[int | None] = mapped_column(BigInteger, ForeignKey("events.id"), nullable=True, index=True)
    event_similarity: Mapped[float | None] = mapped_column(Float, nullable=True)
    event_match_reason: Mapped[str | None] = mapped_column(Text, nullable=True)
    ai_decision: Mapped[str | None] = mapped_column(String(16), nullable=True, index=True)
    ai_confidence: Mapped[float | None] = mapped_column(Float, nullable=True)
    ai_reason: Mapped[str | None] = mapped_column(Text, nullable=True)
    user_value_summary: Mapped[str | None] = mapped_column(Text, nullable=True)
    action_suggestion: Mapped[str | None] = mapped_column(Text, nullable=True)
    urgency: Mapped[str | None] = mapped_column(String(16), nullable=True, index=True)
    relevance: Mapped[str | None] = mapped_column(String(16), nullable=True, index=True)
    actionability: Mapped[str | None] = mapped_column(String(16), nullable=True, index=True)
    risk_level: Mapped[str | None] = mapped_column(String(16), nullable=True, index=True)
    feedback: Mapped[str | None] = mapped_column(String(16), nullable=True, index=True)
    feedback_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    watchlist_category: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)
    watchlist_label: Mapped[str | None] = mapped_column(String(128), nullable=True)
    watchlist_priority: Mapped[int | None] = mapped_column(BigInteger, nullable=True, index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False
    )

    def to_dict(self) -> dict:
        payload = {
            "id": self.id,
            "source": self.source,
            "source_platform": self.source_platform,
            "source_chat_id": self.source_chat_id,
            "source_chat_title": self.source_chat_title,
            "source_message_id": self.source_message_id,
            "author_name": self.author_name,
            "raw_text": self.raw_text,
            "cleaned_text": self.cleaned_text,
            "dedup_key": self.dedup_key,
            "language": self.language,
            "summary_zh": self.summary_zh,
            "category": self.category,
            "score": self.score,
            "signal_level": self.signal_level,
            "analysis_json": self.analysis_json,
            "score_breakdown": self.score_breakdown(),
            "push_sent": self.push_sent,
            "push_status": self.push_status,
            "push_error": self.push_error,
            "pushed_at": self.pushed_at.isoformat() if self.pushed_at else None,
            "possible_duplicate": self.possible_duplicate,
            "duplicate_of_message_id": self.duplicate_of_message_id,
            "similarity_score": self.similarity_score,
            "event_id": self.event_id,
            "event_similarity": self.event_similarity,
            "event_match_reason": self.event_match_reason,
            "ai_decision": self.ai_decision,
            "ai_confidence": self.ai_confidence,
            "ai_reason": self.ai_reason,
            "user_value_summary": self.user_value_summary,
            "action_suggestion": self.action_suggestion,
            "urgency": self.urgency,
            "relevance": self.relevance,
            "actionability": self.actionability,
            "risk_level": self.risk_level,
            "feedback": self.feedback,
            "feedback_at": self.feedback_at.isoformat() if self.feedback_at else None,
            "watchlist_category": self.watchlist_category,
            "watchlist_label": self.watchlist_label,
            "watchlist_priority": self.watchlist_priority,
            "source_profile": self.source_profile(),
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None,
        }
        return payload

    def source_profile(self) -> dict | None:
        analysis = self._analysis_payload()
        source_context = analysis.get("source_context")
        if isinstance(source_context, dict) and isinstance(source_context.get("source_profile"), dict):
            return _api_source_profile(source_context["source_profile"])
        breakdown = analysis.get("score_breakdown")
        if isinstance(breakdown, dict) and isinstance(breakdown.get("source_profile"), dict):
            return _api_source_profile(breakdown["source_profile"])
        return None

    def score_breakdown(self) -> dict:
        analysis = self._analysis_payload()
        breakdown = analysis.get("score_breakdown")
        if isinstance(breakdown, dict):
            signal_type = normalize_signal_type(breakdown.get("signal_type", analysis.get("signal_type")))
            signal_rule = load_signal_rules().signal_types[signal_type]
            payload = dict(breakdown)
            payload.setdefault("ai_score", float(breakdown.get("content_score", analysis.get("importance_score", self.score))))
            payload.setdefault("keyword_bonus", float(analysis.get("keyword_bonus", 0)))
            payload.setdefault("source_bonus", float(breakdown.get("source_score", 0) or 0))
            payload["risk_penalty"] = abs(float(breakdown.get("risk_penalty", 0) or 0))
            payload["signal_type"] = signal_type
            payload.setdefault("signal_label", signal_rule.label)
            payload.setdefault("signal_bonus", float(breakdown.get("signal_score", signal_rule.bonus) or 0))
            payload.setdefault("final_score", self.score)
            payload.setdefault("matched_keywords", [])
            payload.setdefault("matched_risk_keywords", [])
            return payload

        ai_score = float(analysis.get("importance_score", self.score))
        keyword_bonus = float(analysis.get("keyword_bonus", 0))
        signal_type = normalize_signal_type(analysis.get("signal_type"))
        signal_rule = load_signal_rules().signal_types[signal_type]
        return {
            "ai_score": ai_score,
            "keyword_bonus": keyword_bonus,
            "source_bonus": 0,
            "risk_penalty": 0,
            "signal_type": signal_type,
            "signal_label": signal_rule.label,
            "signal_bonus": signal_rule.bonus,
            "final_score": self.score,
            "matched_keywords": [],
            "matched_risk_keywords": [],
        }

    def _analysis_payload(self) -> dict:
        if not self.analysis_json:
            return {}
        try:
            payload = json.loads(self.analysis_json)
            return payload if isinstance(payload, dict) else {}
        except json.JSONDecodeError:
            return {}


class Record(Base):
    __tablename__ = "records"
    __table_args__ = (
        UniqueConstraint("source_platform", "source_channel", "source_message_id", name="uq_records_source_identity"),
        UniqueConstraint("dedup_key", name="uq_records_dedup_key"),
    )

    record_id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    source_platform: Mapped[str] = mapped_column(String(32), nullable=False, index=True)
    source: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)
    source_channel: Mapped[str] = mapped_column(String(255), nullable=False, index=True)
    source_message_id: Mapped[str] = mapped_column(String(128), nullable=False, index=True)
    event_time: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True, index=True)
    collected_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    raw_text: Mapped[str] = mapped_column(Text, nullable=False)
    cleaned_text: Mapped[str] = mapped_column(Text, nullable=False)
    payload: Mapped[str | None] = mapped_column(Text, nullable=True)
    raw_metadata: Mapped[str | None] = mapped_column(Text, nullable=True)
    dedup_key: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    watchlist_category: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)
    watchlist_label: Mapped[str | None] = mapped_column(String(128), nullable=True)
    watchlist_priority: Mapped[int | None] = mapped_column(BigInteger, nullable=True, index=True)
    legacy_message_id: Mapped[int | None] = mapped_column(BigInteger, nullable=True, unique=True, index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)

    def to_dict(self) -> dict:
        return {
            "record_id": self.record_id,
            "source_platform": self.source_platform,
            "source": self.source,
            "source_channel": self.source_channel,
            "source_message_id": self.source_message_id,
            "event_time": self.event_time.isoformat() if self.event_time else None,
            "collected_at": self.collected_at.isoformat() if self.collected_at else None,
            "raw_text": self.raw_text,
            "cleaned_text": self.cleaned_text,
            "payload": _json_payload(self.payload),
            "raw_metadata": _json_payload(self.raw_metadata),
            "dedup_key": self.dedup_key,
            "watchlist_category": self.watchlist_category,
            "watchlist_label": self.watchlist_label,
            "watchlist_priority": self.watchlist_priority,
            "legacy_message_id": self.legacy_message_id,
            "created_at": self.created_at.isoformat() if self.created_at else None,
        }


class Analysis(Base):
    __tablename__ = "analyses"

    analysis_id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    record_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("records.record_id"), nullable=False, index=True)
    model_name: Mapped[str | None] = mapped_column(String(128), nullable=True)
    model_version: Mapped[str | None] = mapped_column(String(64), nullable=True)
    prompt_version: Mapped[str | None] = mapped_column(String(64), nullable=True)
    signal_type: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)
    ai_decision: Mapped[str | None] = mapped_column(String(16), nullable=True, index=True)
    ai_confidence: Mapped[float | None] = mapped_column(Float, nullable=True)
    ai_reason: Mapped[str | None] = mapped_column(Text, nullable=True)
    user_value_summary: Mapped[str | None] = mapped_column(Text, nullable=True)
    action_suggestion: Mapped[str | None] = mapped_column(Text, nullable=True)
    urgency: Mapped[str | None] = mapped_column(String(16), nullable=True, index=True)
    relevance: Mapped[str | None] = mapped_column(String(16), nullable=True, index=True)
    actionability: Mapped[str | None] = mapped_column(String(16), nullable=True, index=True)
    risk_level: Mapped[str | None] = mapped_column(String(16), nullable=True, index=True)
    source_profile: Mapped[str | None] = mapped_column(Text, nullable=True)
    score: Mapped[float] = mapped_column(Float, default=0, nullable=False, index=True)
    score_breakdown: Mapped[str | None] = mapped_column(Text, nullable=True)
    legacy_message_id: Mapped[int | None] = mapped_column(BigInteger, nullable=True, index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)

    def to_dict(self) -> dict:
        return {
            "analysis_id": self.analysis_id,
            "record_id": self.record_id,
            "model_name": self.model_name,
            "model_version": self.model_version,
            "prompt_version": self.prompt_version,
            "signal_type": self.signal_type,
            "ai_decision": self.ai_decision,
            "ai_confidence": self.ai_confidence,
            "ai_reason": self.ai_reason,
            "user_value_summary": self.user_value_summary,
            "action_suggestion": self.action_suggestion,
            "urgency": self.urgency,
            "relevance": self.relevance,
            "actionability": self.actionability,
            "risk_level": self.risk_level,
            "source_profile": _json_payload(self.source_profile),
            "score": self.score,
            "score_breakdown": _json_payload(self.score_breakdown),
            "legacy_message_id": self.legacy_message_id,
            "created_at": self.created_at.isoformat() if self.created_at else None,
        }


class EventRecord(Base):
    __tablename__ = "event_records"
    __table_args__ = (UniqueConstraint("event_id", "record_id", "analysis_id", name="uq_event_records_link"),)

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    event_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("events.id"), nullable=False, index=True)
    record_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("records.record_id"), nullable=False, index=True)
    analysis_id: Mapped[int | None] = mapped_column(BigInteger, ForeignKey("analyses.analysis_id"), nullable=True, index=True)
    event_similarity: Mapped[float | None] = mapped_column(Float, nullable=True)
    event_match_reason: Mapped[str | None] = mapped_column(Text, nullable=True)
    legacy_message_id: Mapped[int | None] = mapped_column(BigInteger, nullable=True, index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)


class Feedback(Base):
    __tablename__ = "feedbacks"

    feedback_id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    feedback_dedup_key: Mapped[str | None] = mapped_column(String(64), nullable=True, unique=True, index=True)
    target_type: Mapped[str] = mapped_column(String(16), nullable=False, index=True)
    record_id: Mapped[int | None] = mapped_column(BigInteger, ForeignKey("records.record_id"), nullable=True, index=True)
    event_id: Mapped[int | None] = mapped_column(BigInteger, ForeignKey("events.id"), nullable=True, index=True)
    feedback: Mapped[str] = mapped_column(String(16), nullable=False, index=True)
    note: Mapped[str | None] = mapped_column(Text, nullable=True)
    feedback_source: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)
    legacy_message_id: Mapped[int | None] = mapped_column(BigInteger, nullable=True, index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)


def _json_payload(value: str | None) -> dict | list | str | None:
    if not value:
        return None
    try:
        return json.loads(value)
    except json.JSONDecodeError:
        return value


def _api_source_profile(profile: dict) -> dict:
    return {
        "label": profile.get("label") or profile.get("key"),
        "role": profile.get("role") or "unknown",
        "ecosystem": profile.get("ecosystem") or "unknown",
        "importance": int(profile.get("importance") or profile.get("score") or 0),
        "specialty": profile.get("specialty") if isinstance(profile.get("specialty"), list) else [],
    }


class Event(Base):
    __tablename__ = "events"
    __table_args__ = (UniqueConstraint("event_key", name="uq_events_event_key"),)

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    event_key: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    event_title: Mapped[str] = mapped_column(String(255), nullable=False, index=True)
    event_summary: Mapped[str | None] = mapped_column(Text, nullable=True)
    first_seen_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    last_seen_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False, index=True)
    message_count: Mapped[int] = mapped_column(BigInteger, default=0, nullable=False)
    source_count: Mapped[int] = mapped_column(BigInteger, default=0, nullable=False)
    max_score: Mapped[float] = mapped_column(Float, default=0, nullable=False, index=True)
    latest_summary: Mapped[str | None] = mapped_column(Text, nullable=True)
    upgrade_count: Mapped[int] = mapped_column(BigInteger, default=0, nullable=False)
    last_upgrade_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_upgrade_summary: Mapped[str | None] = mapped_column(Text, nullable=True)
    last_pushed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    feedback: Mapped[str | None] = mapped_column(String(16), nullable=True, index=True)
    feedback_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    status: Mapped[str] = mapped_column(String(16), default="active", nullable=False, index=True)
    merged_into_event_id: Mapped[int | None] = mapped_column(BigInteger, nullable=True, index=True)
    merged_reason: Mapped[str | None] = mapped_column(Text, nullable=True)

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "event_key": self.event_key,
            "event_title": self.event_title,
            "event_summary": self.event_summary,
            "first_seen_at": self.first_seen_at.isoformat() if self.first_seen_at else None,
            "last_seen_at": self.last_seen_at.isoformat() if self.last_seen_at else None,
            "message_count": self.message_count,
            "source_count": self.source_count,
            "max_score": self.max_score,
            "latest_summary": self.latest_summary,
            "upgrade_count": self.upgrade_count,
            "last_upgrade_at": self.last_upgrade_at.isoformat() if self.last_upgrade_at else None,
            "last_upgrade_summary": self.last_upgrade_summary,
            "last_pushed_at": self.last_pushed_at.isoformat() if self.last_pushed_at else None,
            "feedback": self.feedback,
            "feedback_at": self.feedback_at.isoformat() if self.feedback_at else None,
            "status": self.status,
            "merged_into_event_id": self.merged_into_event_id,
            "merged_reason": self.merged_reason,
        }


class CollectorState(Base):
    __tablename__ = "collector_state"
    __table_args__ = (UniqueConstraint("collector_name", "source_key", name="uq_collector_state_name_key"),)

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    collector_name: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    source_key: Mapped[str] = mapped_column(String(255), nullable=False, index=True)
    last_seen_id: Mapped[str | None] = mapped_column(String(128), nullable=True)
    last_seen_time: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_fetch_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False
    )
