from collections.abc import AsyncGenerator
import json

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy import text

from app.core.config import get_settings
from app.db.base import Base
from app.db import models  # noqa: F401


settings = get_settings()
engine = create_async_engine(settings.database_url, future=True, echo=False)
SessionLocal = async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)


async def init_db() -> None:
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
        await conn.execute(
            text(
                """
                DO $$
                BEGIN
                    IF EXISTS (
                        SELECT 1
                        FROM information_schema.columns
                        WHERE table_name = 'telegram_messages'
                          AND column_name = 'source_message_id'
                          AND data_type <> 'character varying'
                    ) THEN
                        ALTER TABLE telegram_messages
                        ALTER COLUMN source_message_id TYPE VARCHAR(128)
                        USING source_message_id::text;
                    END IF;
                END $$;
                """
            )
        )
        await conn.execute(
            text("ALTER TABLE telegram_messages ADD COLUMN IF NOT EXISTS source_platform VARCHAR(32) NOT NULL DEFAULT 'telegram'")
        )
        await conn.execute(
            text("ALTER TABLE telegram_messages ADD COLUMN IF NOT EXISTS push_status VARCHAR(32) NOT NULL DEFAULT 'pending'")
        )
        await conn.execute(text("ALTER TABLE telegram_messages ADD COLUMN IF NOT EXISTS push_error TEXT"))
        await conn.execute(text("ALTER TABLE telegram_messages ADD COLUMN IF NOT EXISTS pushed_at TIMESTAMPTZ"))
        await conn.execute(text("ALTER TABLE telegram_messages ADD COLUMN IF NOT EXISTS signal_level VARCHAR(1) NOT NULL DEFAULT 'C'"))
        await conn.execute(
            text("ALTER TABLE telegram_messages ADD COLUMN IF NOT EXISTS possible_duplicate BOOLEAN NOT NULL DEFAULT FALSE")
        )
        await conn.execute(text("ALTER TABLE telegram_messages ADD COLUMN IF NOT EXISTS duplicate_of_message_id BIGINT"))
        await conn.execute(text("ALTER TABLE telegram_messages ADD COLUMN IF NOT EXISTS similarity_score DOUBLE PRECISION"))
        await conn.execute(text("ALTER TABLE telegram_messages ADD COLUMN IF NOT EXISTS event_id BIGINT"))
        await conn.execute(text("ALTER TABLE telegram_messages ADD COLUMN IF NOT EXISTS event_similarity DOUBLE PRECISION"))
        await conn.execute(text("ALTER TABLE telegram_messages ADD COLUMN IF NOT EXISTS event_match_reason TEXT"))
        await conn.execute(text("ALTER TABLE telegram_messages ADD COLUMN IF NOT EXISTS ai_decision VARCHAR(16)"))
        await conn.execute(text("ALTER TABLE telegram_messages ADD COLUMN IF NOT EXISTS ai_confidence DOUBLE PRECISION"))
        await conn.execute(text("ALTER TABLE telegram_messages ADD COLUMN IF NOT EXISTS ai_reason TEXT"))
        await conn.execute(text("ALTER TABLE telegram_messages ADD COLUMN IF NOT EXISTS user_value_summary TEXT"))
        await conn.execute(text("ALTER TABLE telegram_messages ADD COLUMN IF NOT EXISTS action_suggestion TEXT"))
        await conn.execute(text("ALTER TABLE telegram_messages ADD COLUMN IF NOT EXISTS urgency VARCHAR(16)"))
        await conn.execute(text("ALTER TABLE telegram_messages ADD COLUMN IF NOT EXISTS relevance VARCHAR(16)"))
        await conn.execute(text("ALTER TABLE telegram_messages ADD COLUMN IF NOT EXISTS actionability VARCHAR(16)"))
        await conn.execute(text("ALTER TABLE telegram_messages ADD COLUMN IF NOT EXISTS risk_level VARCHAR(16)"))
        await conn.execute(text("ALTER TABLE telegram_messages ADD COLUMN IF NOT EXISTS feedback VARCHAR(16)"))
        await conn.execute(text("ALTER TABLE telegram_messages ADD COLUMN IF NOT EXISTS feedback_at TIMESTAMPTZ"))
        await conn.execute(text("ALTER TABLE telegram_messages ADD COLUMN IF NOT EXISTS watchlist_category VARCHAR(64)"))
        await conn.execute(text("ALTER TABLE telegram_messages ADD COLUMN IF NOT EXISTS watchlist_label VARCHAR(128)"))
        await conn.execute(text("ALTER TABLE telegram_messages ADD COLUMN IF NOT EXISTS watchlist_priority BIGINT"))
        await conn.execute(text("ALTER TABLE events ADD COLUMN IF NOT EXISTS max_score DOUBLE PRECISION NOT NULL DEFAULT 0"))
        await conn.execute(text("ALTER TABLE events ADD COLUMN IF NOT EXISTS latest_summary TEXT"))
        await conn.execute(text("ALTER TABLE events ADD COLUMN IF NOT EXISTS feedback VARCHAR(16)"))
        await conn.execute(text("ALTER TABLE events ADD COLUMN IF NOT EXISTS feedback_at TIMESTAMPTZ"))
        await conn.execute(text("ALTER TABLE events ADD COLUMN IF NOT EXISTS status VARCHAR(16) NOT NULL DEFAULT 'active'"))
        await conn.execute(text("ALTER TABLE events ADD COLUMN IF NOT EXISTS merged_into_event_id BIGINT"))
        await conn.execute(text("ALTER TABLE events ADD COLUMN IF NOT EXISTS merged_reason TEXT"))
        await conn.execute(text("ALTER TABLE events ADD COLUMN IF NOT EXISTS upgrade_count BIGINT NOT NULL DEFAULT 0"))
        await conn.execute(text("ALTER TABLE events ADD COLUMN IF NOT EXISTS last_upgrade_at TIMESTAMPTZ"))
        await conn.execute(text("ALTER TABLE events ADD COLUMN IF NOT EXISTS last_upgrade_summary TEXT"))
        await conn.execute(text("ALTER TABLE events ADD COLUMN IF NOT EXISTS last_pushed_at TIMESTAMPTZ"))
        await conn.execute(text("ALTER TABLE feedbacks ADD COLUMN IF NOT EXISTS feedback_dedup_key VARCHAR(64)"))
        await conn.execute(
            text(
                """
                WITH event_stats AS (
                    SELECT
                        event_id,
                        COUNT(*) AS message_count,
                        COUNT(DISTINCT source_chat_id) AS source_count,
                        MIN(created_at) AS first_seen_at,
                        MAX(created_at) AS last_seen_at,
                        MAX(score) AS max_score
                    FROM telegram_messages
                    WHERE event_id IS NOT NULL
                    GROUP BY event_id
                ),
                latest_messages AS (
                    SELECT DISTINCT ON (event_id)
                        event_id,
                        summary_zh AS latest_summary
                    FROM telegram_messages
                    WHERE event_id IS NOT NULL
                    ORDER BY event_id, created_at DESC, id DESC
                )
                UPDATE events e
                SET
                    message_count = event_stats.message_count,
                    source_count = event_stats.source_count,
                    first_seen_at = event_stats.first_seen_at,
                    last_seen_at = event_stats.last_seen_at,
                    max_score = COALESCE(event_stats.max_score, 0),
                    latest_summary = latest_messages.latest_summary
                FROM event_stats
                LEFT JOIN latest_messages ON latest_messages.event_id = event_stats.event_id
                WHERE e.id = event_stats.event_id
                """
            )
        )
        await conn.execute(
            text("CREATE INDEX IF NOT EXISTS idx_telegram_messages_push_status ON telegram_messages (push_status)")
        )
        await conn.execute(
            text("CREATE INDEX IF NOT EXISTS idx_telegram_messages_source_platform ON telegram_messages (source_platform)")
        )
        await conn.execute(
            text("CREATE INDEX IF NOT EXISTS idx_telegram_messages_signal_level ON telegram_messages (signal_level)")
        )
        await conn.execute(
            text("CREATE INDEX IF NOT EXISTS idx_telegram_messages_possible_duplicate ON telegram_messages (possible_duplicate)")
        )
        await conn.execute(
            text(
                "CREATE INDEX IF NOT EXISTS idx_telegram_messages_duplicate_of_message_id "
                "ON telegram_messages (duplicate_of_message_id)"
            )
        )
        await conn.execute(
            text("CREATE INDEX IF NOT EXISTS idx_telegram_messages_event_id ON telegram_messages (event_id)")
        )
        await conn.execute(
            text("CREATE INDEX IF NOT EXISTS idx_telegram_messages_feedback ON telegram_messages (feedback)")
        )
        await conn.execute(
            text("CREATE INDEX IF NOT EXISTS idx_telegram_messages_ai_decision ON telegram_messages (ai_decision)")
        )
        await conn.execute(text("CREATE INDEX IF NOT EXISTS idx_events_last_seen_at ON events (last_seen_at)"))
        await conn.execute(text("CREATE INDEX IF NOT EXISTS idx_events_max_score ON events (max_score)"))
        await conn.execute(text("CREATE INDEX IF NOT EXISTS idx_events_feedback ON events (feedback)"))
        await conn.execute(text("CREATE INDEX IF NOT EXISTS idx_events_status ON events (status)"))
        await conn.execute(text("CREATE INDEX IF NOT EXISTS idx_events_merged_into_event_id ON events (merged_into_event_id)"))
        await conn.execute(text("CREATE INDEX IF NOT EXISTS idx_events_last_pushed_at ON events (last_pushed_at)"))
        await conn.execute(text("CREATE UNIQUE INDEX IF NOT EXISTS idx_events_event_key ON events (event_key)"))
        await conn.execute(text("DROP INDEX IF EXISTS idx_feedbacks_feedback_dedup_key"))
        await conn.execute(
            text("CREATE UNIQUE INDEX idx_feedbacks_feedback_dedup_key ON feedbacks (feedback_dedup_key)")
        )
        await conn.execute(
            text("CREATE INDEX IF NOT EXISTS idx_telegram_messages_watchlist_category ON telegram_messages (watchlist_category)")
        )
        await conn.execute(
            text("CREATE INDEX IF NOT EXISTS idx_telegram_messages_watchlist_priority ON telegram_messages (watchlist_priority)")
        )
        await conn.execute(text("UPDATE telegram_messages SET push_status = 'sent' WHERE push_sent = TRUE"))
        await conn.execute(text("UPDATE telegram_messages SET source_platform = source WHERE source_platform = 'telegram' AND source <> 'telegram'"))
        await conn.execute(
            text(
                "UPDATE telegram_messages SET push_status = 'skipped_low_score' "
                "WHERE push_sent = FALSE AND score < :threshold AND push_status = 'pending'"
            ),
            {"threshold": settings.push_score_threshold},
        )
        await conn.execute(
            text(
                """
                WITH recalculated AS (
                    SELECT
                        id,
                        LEAST(
                            100.0,
                            GREATEST(
                                0.0,
                                COALESCE((analysis_json::jsonb->'score_breakdown'->>'ai_score')::double precision,
                                         (analysis_json::jsonb->>'importance_score')::double precision,
                                         score)
                                + COALESCE((analysis_json::jsonb->'score_breakdown'->>'keyword_bonus')::double precision,
                                           (analysis_json::jsonb->>'keyword_bonus')::double precision,
                                           0.0)
                                + COALESCE((analysis_json::jsonb->'score_breakdown'->>'source_bonus')::double precision, 0.0)
                                - ABS(COALESCE((analysis_json::jsonb->'score_breakdown'->>'risk_penalty')::double precision, 0.0))
                            )
                        ) AS final_score
                    FROM telegram_messages
                    WHERE analysis_json IS NOT NULL
                )
                UPDATE telegram_messages tm
                SET
                    score = ROUND(recalculated.final_score::numeric, 2)::double precision,
                    analysis_json = jsonb_set(
                        jsonb_set(
                            tm.analysis_json::jsonb,
                            '{score_breakdown}',
                            COALESCE(tm.analysis_json::jsonb->'score_breakdown', '{}'::jsonb),
                            true
                        ),
                        '{score_breakdown,final_score}',
                        to_jsonb(ROUND(recalculated.final_score::numeric, 2)),
                        true
                    )::text
                FROM recalculated
                WHERE tm.id = recalculated.id
                """
            )
        )
        await _recalculate_score_breakdowns(conn)
        await conn.execute(
            text(
                """
                UPDATE telegram_messages
                SET signal_level = CASE
                    WHEN score >= 90 THEN 'S'
                    WHEN score >= 75 THEN 'A'
                    WHEN score >= 60 THEN 'B'
                    ELSE 'C'
                END
                """
            )
        )
        await conn.execute(
            text(
                "UPDATE telegram_messages SET push_status = 'pending' "
                "WHERE push_sent = FALSE AND push_status = 'skipped_low_score' AND score >= :threshold"
            ),
            {"threshold": settings.push_score_threshold},
        )
        await conn.execute(
            text(
                "CREATE UNIQUE INDEX IF NOT EXISTS idx_collector_state_name_key "
                "ON collector_state (collector_name, source_key)"
            )
        )
        await conn.execute(text("ALTER TABLE collector_state ADD COLUMN IF NOT EXISTS last_fetch_at TIMESTAMPTZ"))
        await _migrate_legacy_messages_to_layered_model(conn)


async def get_db_session() -> AsyncGenerator[AsyncSession, None]:
    async with SessionLocal() as session:
        yield session


async def _recalculate_score_breakdowns(conn) -> None:
    from app.services.scorer import score_message

    result = await conn.execute(
        text("SELECT id, cleaned_text, score, analysis_json FROM telegram_messages WHERE analysis_json IS NOT NULL")
    )
    for row in result.mappings():
        try:
            analysis = json.loads(row["analysis_json"])
        except json.JSONDecodeError:
            continue
        if not isinstance(analysis, dict):
            continue

        ai_score = float(analysis.get("importance_score", row["score"]))
        scored = score_message(row["cleaned_text"], ai_score, signal_type=analysis.get("signal_type", "unknown"))
        analysis["keyword_bonus"] = scored.keyword_bonus
        analysis["signal_type"] = scored.signal_type
        analysis["score_breakdown"] = scored.breakdown()
        await conn.execute(
            text(
                "UPDATE telegram_messages "
                "SET score = :score, analysis_json = :analysis_json "
                "WHERE id = :id"
            ),
            {
                "id": row["id"],
                "score": scored.final_score,
                "analysis_json": json.dumps(analysis, ensure_ascii=False),
            },
        )


async def _migrate_legacy_messages_to_layered_model(conn) -> None:
    await conn.execute(
        text(
            """
            WITH source_rows AS (
                SELECT DISTINCT ON (tm.source_platform, tm.source_chat_id, tm.source_message_id)
                    tm.*
                FROM telegram_messages tm
                ORDER BY tm.source_platform, tm.source_chat_id, tm.source_message_id, tm.id
            )
            INSERT INTO records (
                record_id,
                source_platform,
                source,
                source_channel,
                source_message_id,
                event_time,
                collected_at,
                raw_text,
                cleaned_text,
                payload,
                raw_metadata,
                dedup_key,
                watchlist_category,
                watchlist_label,
                watchlist_priority,
                legacy_message_id,
                created_at
            )
            SELECT
                sr.id,
                sr.source_platform,
                sr.source,
                sr.source_chat_id,
                sr.source_message_id,
                sr.created_at,
                sr.created_at,
                sr.raw_text,
                sr.cleaned_text,
                COALESCE(sr.analysis_json::jsonb->'source_metadata', '{}'::jsonb)::text,
                COALESCE(sr.analysis_json::jsonb->'source_metadata', '{}'::jsonb)::text,
                sr.dedup_key,
                sr.watchlist_category,
                sr.watchlist_label,
                sr.watchlist_priority,
                sr.id,
                sr.created_at
            FROM source_rows sr
            WHERE NOT EXISTS (
                SELECT 1 FROM records r WHERE r.record_id = sr.id
            )
              AND NOT EXISTS (
                SELECT 1
                FROM records r
                WHERE r.source_platform = sr.source_platform
                  AND r.source_channel = sr.source_chat_id
                  AND r.source_message_id = sr.source_message_id
            )
              AND NOT EXISTS (
                SELECT 1 FROM records r WHERE r.dedup_key = sr.dedup_key
            )
            ON CONFLICT DO NOTHING
            """
        )
    )
    await conn.execute(
        text(
            """
            SELECT setval(
                pg_get_serial_sequence('records', 'record_id'),
                GREATEST((SELECT COALESCE(MAX(record_id), 1) FROM records), 1),
                true
            )
            WHERE pg_get_serial_sequence('records', 'record_id') IS NOT NULL
            """
        )
    )
    await conn.execute(
        text(
            """
            INSERT INTO analyses (
                record_id,
                model_name,
                model_version,
                prompt_version,
                signal_type,
                ai_decision,
                ai_confidence,
                ai_reason,
                user_value_summary,
                action_suggestion,
                urgency,
                relevance,
                actionability,
                risk_level,
                source_profile,
                score,
                score_breakdown,
                legacy_message_id,
                created_at
            )
            SELECT
                r.record_id,
                NULL,
                NULL,
                'ai_decision_v1',
                COALESCE(tm.analysis_json::jsonb->>'signal_type', tm.score_breakdown_signal_type),
                tm.ai_decision,
                tm.ai_confidence,
                tm.ai_reason,
                tm.user_value_summary,
                tm.action_suggestion,
                tm.urgency,
                tm.relevance,
                tm.actionability,
                tm.risk_level,
                COALESCE(
                    tm.analysis_json::jsonb->'source_context'->'source_profile',
                    tm.analysis_json::jsonb->'score_breakdown'->'source_profile'
                )::text,
                tm.score,
                (tm.analysis_json::jsonb->'score_breakdown')::text,
                tm.id,
                tm.created_at
            FROM (
                SELECT
                    *,
                    analysis_json::jsonb->'score_breakdown'->>'signal_type' AS score_breakdown_signal_type
                FROM telegram_messages
                WHERE analysis_json IS NOT NULL
            ) tm
            JOIN records r ON r.legacy_message_id = tm.id
            WHERE NOT EXISTS (
                SELECT 1
                FROM analyses a
                WHERE a.legacy_message_id = tm.id
            )
            """
        )
    )
    await conn.execute(
        text(
            """
            INSERT INTO event_records (
                event_id,
                record_id,
                analysis_id,
                event_similarity,
                event_match_reason,
                legacy_message_id,
                created_at
            )
            SELECT
                tm.event_id,
                r.record_id,
                a.analysis_id,
                tm.event_similarity,
                tm.event_match_reason,
                tm.id,
                tm.created_at
            FROM telegram_messages tm
            JOIN records r ON r.legacy_message_id = tm.id
            LEFT JOIN analyses a ON a.legacy_message_id = tm.id
            WHERE tm.event_id IS NOT NULL
              AND NOT EXISTS (
                  SELECT 1
                  FROM event_records er
                  WHERE er.event_id = tm.event_id
                    AND er.record_id = r.record_id
                    AND COALESCE(er.analysis_id, 0) = COALESCE(a.analysis_id, 0)
              )
            """
        )
    )
    await conn.execute(
        text(
            """
            INSERT INTO feedbacks (
                target_type,
                record_id,
                event_id,
                feedback,
                note,
                feedback_source,
                legacy_message_id,
                created_at
            )
            SELECT
                'record',
                r.record_id,
                tm.event_id,
                tm.feedback,
                NULL,
                'legacy_message',
                tm.id,
                COALESCE(tm.feedback_at, tm.updated_at, tm.created_at)
            FROM telegram_messages tm
            JOIN records r ON r.legacy_message_id = tm.id
            WHERE tm.feedback IN ('good', 'bad', 'ignore')
              AND NOT EXISTS (
                  SELECT 1
                  FROM feedbacks f
                  WHERE f.target_type = 'record'
                    AND f.legacy_message_id = tm.id
                    AND f.feedback = tm.feedback
              )
            """
        )
    )
    await conn.execute(
        text(
            """
            INSERT INTO feedbacks (
                target_type,
                record_id,
                event_id,
                feedback,
                note,
                feedback_source,
                legacy_message_id,
                created_at
            )
            SELECT
                'event',
                NULL,
                e.id,
                e.feedback,
                NULL,
                'legacy_event',
                NULL,
                COALESCE(e.feedback_at, e.last_seen_at, e.first_seen_at)
            FROM events e
            WHERE e.feedback IN ('good', 'bad', 'ignore')
              AND NOT EXISTS (
                  SELECT 1
                  FROM feedbacks f
                  WHERE f.target_type = 'event'
                    AND f.event_id = e.id
                    AND f.feedback = e.feedback
              )
            """
        )
    )
