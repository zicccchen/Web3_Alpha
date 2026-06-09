from hashlib import sha256
from datetime import datetime, timedelta, timezone

from app.core.config import get_settings
from app.core.logging import get_logger
from app.schemas.message import SourceMessage
from app.services.cleaner import clean_message
from app.services.duplicates import NO_DUPLICATE, find_possible_duplicate
from app.services.event_cluster import EventClusterer, extract_event_features
from app.services.scorer import score_message
from app.services.signal import signal_level_for_score
from app.services.source_profiles import match_source_profile
from app.services.source_decision_overrides import apply_source_decision_overrides
from app.services.analyzer import normalize_decision


logger = get_logger(__name__)
settings = get_settings()


class MessagePipeline:
    def __init__(self, cache=None, analyzer=None, notifier=None, repository=None) -> None:
        if cache is None:
            from app.services.cache import DedupCache

            cache = DedupCache()
        if analyzer is None:
            from app.services.analyzer import AIAnalyzer

            analyzer = AIAnalyzer()
        if notifier is None:
            from app.services.notifier import FeishuNotifier

            notifier = FeishuNotifier()
        if repository is None:
            from app.services.repository import MessageRepository

            repository = MessageRepository()
        self.cache = cache
        self.analyzer = analyzer
        self.notifier = notifier
        self.repository = repository
        self.event_clusterer = EventClusterer(repository)

    async def process(self, message: SourceMessage) -> None:
        log_context = self._log_context(message)
        try:
            cleaned = clean_message(message.raw_text)
            dedup_key = self._source_dedup_key(message, cleaned.dedup_key)
            log_context["dedup_key"] = dedup_key
            if not cleaned.cleaned_text:
                log_context["push_status"] = "skipped_low_score"
                logger.info("empty message skipped", extra=log_context)
                return

            if await self.cache.seen(dedup_key):
                logger.info("message skipped by redis dedup", extra=log_context)
                return

            if await self.repository.exists_by_source_message(
                message.source,
                message.source_chat_id,
                str(message.source_message_id),
            ):
                logger.info("message skipped by source identity dedup", extra=log_context)
                await self.cache.mark(dedup_key)
                return

            if await self.repository.exists_by_dedup_key(dedup_key):
                logger.info("message skipped by database dedup", extra=log_context)
                await self.cache.mark(dedup_key)
                return

            source_context = self._source_context(message)
            source_profile = match_source_profile(source_context)
            if source_profile:
                source_context["source_profile"] = source_profile.as_dict()
            analysis = await self.analyzer.analyze(cleaned.cleaned_text, source_context=source_context)
            analysis = apply_source_decision_overrides(
                cleaned.cleaned_text,
                analysis,
                source_profile=source_profile.as_dict() if source_profile else None,
            )
            score = score_message(
                cleaned.cleaned_text,
                float(analysis["importance_score"]),
                signal_type=analysis.get("signal_type", "unknown"),
                analysis=analysis,
                source_profile=source_profile.as_dict() if source_profile else None,
                watchlist_category=message.watchlist_category,
                watchlist_priority=message.watchlist_priority,
            )
            signal_level = signal_level_for_score(score.final_score)
            ai_decision = normalize_decision(analysis.get("decision"))
            effective_decision = ai_decision
            should_push = _should_push_decision(effective_decision)
            push_status = "pending" if should_push else _push_status_for_decision(ai_decision)
            score_breakdown = score.breakdown()
            event_match = await self._event_match(cleaned.cleaned_text, analysis, message=message)
            duplicate_match = await self._possible_duplicate(
                cleaned.cleaned_text,
                summary=analysis.get("summary_zh"),
                category=analysis.get("category"),
            )
            push_error = None
            event_upgrade_level = None
            should_mark_event_upgrade = False
            if event_match and not event_match.is_new_event and should_push:
                event_update = await self._event_update(event_match.event_id, cleaned.cleaned_text, analysis)
                analysis["event_update"] = event_update
                update_level = event_update.get("event_update_level", "minor")
                event_upgrade_level = update_level
                upgrade_decision = normalize_decision(event_update.get("decision", ai_decision))
                if update_level in {"major", "critical"} and _should_push_decision(upgrade_decision):
                    effective_decision = upgrade_decision
                    analysis["effective_decision"] = effective_decision
                    should_mark_event_upgrade = True
                else:
                    should_push = False
                    push_status = "skipped_event_duplicate"
                    push_error = (
                        f"event_id={event_match.event_id}, "
                        f"event_similarity={event_match.event_similarity}, "
                        f"event_match_reason={event_match.event_match_reason}, "
                        f"event_update_level={update_level}, "
                        f"event_update_decision={upgrade_decision}"
                    )
            if duplicate_match.possible_duplicate and should_push and (
                not event_match or event_match.is_new_event
            ):
                should_push = False
                push_status = "skipped_duplicate"
                push_error = (
                    f"duplicate_of_message_id={duplicate_match.duplicate_of_message_id}, "
                    f"similarity_score={duplicate_match.similarity_score}"
                )
            log_context["score"] = score.final_score
            log_context["signal_level"] = signal_level
            log_context["push_status"] = push_status
            log_context["ai_decision"] = ai_decision
            log_context["rate_limit_reason"] = None
            log_context["score_breakdown"] = score_breakdown
            log_context["possible_duplicate"] = duplicate_match.possible_duplicate
            log_context["duplicate_of_message_id"] = duplicate_match.duplicate_of_message_id
            log_context["similarity_score"] = duplicate_match.similarity_score
            log_context["event_id"] = event_match.event_id if event_match else None

            record = await self.repository.save(
                {
                    "source": message.source,
                    "source_platform": message.source,
                    "source_chat_id": message.source_chat_id,
                    "source_chat_title": message.source_chat_title,
                    "source_message_id": str(message.source_message_id),
                    "author_name": message.author_name,
                    "raw_text": message.raw_text,
                    "cleaned_text": cleaned.cleaned_text,
                    "dedup_key": dedup_key,
                    "language": cleaned.language,
                    "summary_zh": analysis["summary_zh"],
                    "category": analysis["category"],
                    "score": score.final_score,
                    "signal_level": signal_level,
                    "analysis_json": self.repository.serialize_analysis(
                        {
                            **analysis,
                            "keyword_bonus": score.keyword_bonus,
                            "score_breakdown": score_breakdown,
                            "source_metadata": message.metadata or {},
                            "source_context": source_context,
                        }
                    ),
                    "push_sent": False,
                    "push_status": push_status,
                    "push_error": push_error,
                    "possible_duplicate": duplicate_match.possible_duplicate,
                    "duplicate_of_message_id": duplicate_match.duplicate_of_message_id,
                    "similarity_score": duplicate_match.similarity_score,
                    "event_id": event_match.event_id if event_match else None,
                    "event_similarity": event_match.event_similarity if event_match else None,
                    "event_match_reason": event_match.event_match_reason if event_match else None,
                    "ai_decision": ai_decision,
                    "ai_confidence": float(analysis.get("confidence") or 0),
                    "ai_reason": analysis.get("reason"),
                    "user_value_summary": analysis.get("user_value_summary"),
                    "action_suggestion": analysis.get("action_suggestion"),
                    "urgency": analysis.get("urgency"),
                    "relevance": analysis.get("relevance"),
                    "actionability": analysis.get("actionability"),
                    "risk_level": analysis.get("risk_level"),
                    "watchlist_category": message.watchlist_category,
                    "watchlist_label": message.watchlist_label,
                    "watchlist_priority": message.watchlist_priority,
                    **({"created_at": message.created_at} if message.created_at else {}),
                }
            )
            if not record:
                return

            await self.cache.mark(dedup_key)
            if event_match:
                await self.repository.update_event_stats(event_match.event_id)
            if not should_push:
                if push_status == "skipped_event_duplicate":
                    logger.info("message saved without push due to existing event", extra=log_context)
                elif push_status == "skipped_duplicate":
                    logger.info("message saved without push due to duplicate signal", extra=log_context)
                elif push_status == "skipped_ignore":
                    logger.info("message saved without push due to ignore decision", extra=log_context)
                else:
                    logger.info("message saved without push", extra=log_context)
                return

            rate_limit_reason = await self._rate_limit_reason(signal_level)
            if rate_limit_reason:
                log_context["push_status"] = "skipped_rate_limited"
                log_context["rate_limit_reason"] = rate_limit_reason
                await self.repository.update_push_status(record.id, "skipped_rate_limited", rate_limit_reason)
                logger.info("message push skipped by rate limit", extra=log_context)
                return

            result = await self.notifier.notify(
                {
                    "message_id": record.id,
                    "event_id": event_match.event_id if event_match else None,
                    "source_chat_id": message.source_chat_id,
                    "source_chat_title": message.source_chat_title,
                    "source_platform": message.source,
                    "source_project": source_context.get("project"),
                    "source_ecosystem": source_context.get("ecosystem"),
                    "source_channel": source_context.get("channel"),
                    "category": analysis["category"],
                    "score": score.final_score,
                    "ai_decision": effective_decision,
                    "original_ai_decision": ai_decision,
                    "ai_confidence": float(analysis.get("confidence") or 0),
                    "ai_reason": analysis.get("reason"),
                    "user_value_summary": analysis.get("user_value_summary"),
                    "action_suggestion": analysis.get("action_suggestion"),
                    "urgency": analysis.get("urgency"),
                    "relevance": analysis.get("relevance"),
                    "actionability": analysis.get("actionability"),
                    "risk_level": analysis.get("risk_level"),
                    "summary_zh": analysis["summary_zh"],
                    "reason": analysis["reason"],
                    "event_upgrade": bool(event_match and not event_match.is_new_event),
                    "event_update_level": (analysis.get("event_update") or {}).get("event_update_level"),
                }
            )
            if result.sent:
                log_context["push_status"] = "sent"
                await self.repository.update_push_status(record.id, "sent")
                if should_mark_event_upgrade and event_match:
                    await self.repository.mark_event_upgrade_pushed(
                        event_match.event_id,
                        upgrade_summary=analysis.get("summary_zh") or cleaned.cleaned_text[:160],
                    )
                logger.info("message pushed to feishu", extra=log_context)
            else:
                log_context["push_status"] = "failed"
                await self.repository.update_push_status(record.id, "failed", result.error)
                logger.warning(
                    "message push failed",
                    extra={**log_context, "push_error": result.error},
                )
        except Exception:
            logger.exception("message pipeline failed", extra=log_context)

    async def close(self) -> None:
        await self.cache.close()

    def _log_context(self, message: SourceMessage) -> dict:
        return {
            "channel": message.source_chat_id,
            "platform": message.source,
            "channel_id": message.source_chat_id,
            "message_id": str(message.source_message_id),
            "source_message_id": str(message.source_message_id),
            "dedup_key": None,
            "score": None,
            "signal_level": None,
            "push_status": None,
            "rate_limit_reason": None,
            "score_breakdown": None,
            "possible_duplicate": None,
            "duplicate_of_message_id": None,
            "similarity_score": None,
            "project": (message.metadata or {}).get("project"),
            "ecosystem": (message.metadata or {}).get("ecosystem"),
            "channel_name": message.source_chat_title,
            "event_id": None,
            "ai_decision": None,
        }

    def _source_context(self, message: SourceMessage) -> dict:
        metadata = message.metadata or {}
        return {
            "source_platform": message.source,
            "project": metadata.get("project") or message.watchlist_label or message.watchlist_category,
            "ecosystem": metadata.get("ecosystem"),
            "watchlist_category": message.watchlist_category,
            "watchlist_priority": message.watchlist_priority,
            "channel": message.source_chat_title or message.source_chat_id,
            "channel_id": message.source_chat_id,
            "author_name": message.author_name,
            "discord_channel_type": metadata.get("discord_channel_type"),
            "original_url": metadata.get("original_url"),
        }

    async def _rate_limit_reason(self, signal_level: str) -> str | None:
        counts = await self.repository.push_rate_counts()
        if counts["sent_today_count"] >= settings.push_daily_limit:
            return "daily_limit_reached"
        if signal_level == "S" and counts["s_sent_last_hour_count"] >= settings.push_s_level_hourly_limit:
            return "hourly_limit_reached"
        if signal_level == "A" and counts["a_sent_last_hour_count"] >= settings.push_a_level_hourly_limit:
            return "hourly_limit_reached"
        return None

    async def _possible_duplicate(self, cleaned_text: str, summary: str | None = None, category: str | None = None):
        try:
            since = datetime.now(timezone.utc) - timedelta(hours=24)
            candidates = await self.repository.recent_high_score_messages(
                since=since,
                min_score=settings.push_score_threshold,
            )
            return find_possible_duplicate(cleaned_text, candidates, summary=summary, category=category)
        except Exception:
            logger.exception("possible duplicate check failed")
            return NO_DUPLICATE

    async def _event_match(self, cleaned_text: str, analysis: dict, message: SourceMessage | None = None):
        try:
            event_title = analysis.get("event_title") or analysis.get("summary_zh") or cleaned_text[:40]
            return await self.event_clusterer.match_or_create(
                event_title=event_title,
                event_summary=analysis.get("summary_zh", ""),
                message_text=cleaned_text,
                message=message,
            )
        except Exception:
            logger.exception("event cluster matching failed")
            return None

    async def _event_update(self, event_id: int, cleaned_text: str, analysis: dict) -> dict:
        try:
            event = await self.repository.get_event(event_id)
            if not event:
                return {"event_update_level": "minor", "reason": "未找到已存在事件，按保守策略不重复推送"}
            if _is_repeated_event_update(event, cleaned_text, analysis):
                return {
                    "event_update_level": "minor",
                    "decision": "ignore",
                    "reason": "新消息与该事件最新摘要共享核心实体和事件动作，判定为同一事实重复报道，不再次推送。",
                }
            return await self.analyzer.analyze_event_update(
                event_title=event.event_title,
                event_summary=event.event_summary,
                latest_summary=getattr(event, "latest_summary", None),
                message_summary=analysis.get("summary_zh", ""),
                message_text=cleaned_text,
            )
        except Exception:
            logger.exception("event update check failed")
            return {"event_update_level": "minor", "reason": "事件升级判断异常，按保守策略不重复推送"}

    @staticmethod
    def _source_dedup_key(message: SourceMessage, content_dedup_key: str) -> str:
        payload = f"{message.source}:{message.source_chat_id}:{message.source_message_id}:{content_dedup_key}"
        return sha256(payload.encode("utf-8")).hexdigest()


def _push_status_for_decision(decision: str) -> str:
    if decision == "ignore":
        return "skipped_ignore"
    return "pending"


def _should_push_decision(decision: str) -> bool:
    return decision in {"push", "watch"}


def _is_repeated_event_update(event, cleaned_text: str, analysis: dict) -> bool:
    latest_summary = getattr(event, "latest_summary", None)
    if not latest_summary:
        return False
    current_text = " ".join(
        part
        for part in (
            analysis.get("event_title"),
            analysis.get("summary_zh"),
            cleaned_text,
        )
        if part
    )
    current_features = extract_event_features(current_text)
    latest_features = extract_event_features(str(latest_summary))
    current_entities = current_features.entities | current_features.projects | current_features.tokens
    latest_entities = latest_features.entities | latest_features.projects | latest_features.tokens
    shared_entities = _core_event_entities(current_entities & latest_entities)
    shared_actions = current_features.key_phrases & latest_features.key_phrases
    if len(shared_entities) >= 2 and shared_actions:
        return True
    if len(shared_entities) >= 3:
        return True
    return False


def _core_event_entities(entities: set[str]) -> set[str]:
    generic = {"btc", "eth", "sol", "usdt", "usdc", "bnb", "usd", "u"}
    return {entity for entity in entities if entity not in generic}
