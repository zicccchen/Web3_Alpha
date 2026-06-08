-- Reference schema for Web3 Alpha.
-- Runtime table creation and lightweight migrations are handled in app/db/session.py.

CREATE TABLE IF NOT EXISTS events (
    id BIGSERIAL PRIMARY KEY,
    event_key VARCHAR(64) NOT NULL UNIQUE,
    event_title VARCHAR(255) NOT NULL,
    event_summary TEXT,
    first_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    message_count BIGINT NOT NULL DEFAULT 0,
    source_count BIGINT NOT NULL DEFAULT 0,
    max_score DOUBLE PRECISION NOT NULL DEFAULT 0,
    latest_summary TEXT,
    upgrade_count BIGINT NOT NULL DEFAULT 0,
    last_upgrade_at TIMESTAMPTZ,
    last_upgrade_summary TEXT,
    last_pushed_at TIMESTAMPTZ,
    feedback VARCHAR(16),
    feedback_at TIMESTAMPTZ,
    status VARCHAR(16) NOT NULL DEFAULT 'active',
    merged_into_event_id BIGINT,
    merged_reason TEXT
);

CREATE INDEX IF NOT EXISTS idx_events_last_seen_at ON events (last_seen_at);
CREATE INDEX IF NOT EXISTS idx_events_status ON events (status);
CREATE INDEX IF NOT EXISTS idx_events_max_score ON events (max_score);
CREATE INDEX IF NOT EXISTS idx_events_feedback ON events (feedback);
CREATE INDEX IF NOT EXISTS idx_events_merged_into_event_id ON events (merged_into_event_id);
CREATE INDEX IF NOT EXISTS idx_events_last_pushed_at ON events (last_pushed_at);

CREATE TABLE IF NOT EXISTS telegram_messages (
    id BIGSERIAL PRIMARY KEY,
    source VARCHAR(32) NOT NULL DEFAULT 'telegram',
    source_platform VARCHAR(32) NOT NULL DEFAULT 'telegram',
    source_chat_id VARCHAR(128) NOT NULL,
    source_chat_title VARCHAR(255),
    source_message_id VARCHAR(128) NOT NULL,
    author_name VARCHAR(255),
    raw_text TEXT NOT NULL,
    cleaned_text TEXT NOT NULL,
    dedup_key VARCHAR(64) NOT NULL UNIQUE,
    language VARCHAR(16) NOT NULL DEFAULT 'unknown',
    summary_zh TEXT,
    category VARCHAR(64),
    score DOUBLE PRECISION NOT NULL DEFAULT 0,
    signal_level VARCHAR(1) NOT NULL DEFAULT 'C',
    analysis_json TEXT,
    push_sent BOOLEAN NOT NULL DEFAULT FALSE,
    push_status VARCHAR(32) NOT NULL DEFAULT 'pending',
    push_error TEXT,
    pushed_at TIMESTAMPTZ,
    possible_duplicate BOOLEAN NOT NULL DEFAULT FALSE,
    duplicate_of_message_id BIGINT,
    similarity_score DOUBLE PRECISION,
    event_id BIGINT REFERENCES events(id),
    event_similarity DOUBLE PRECISION,
    event_match_reason TEXT,
    ai_decision VARCHAR(16),
    ai_confidence DOUBLE PRECISION,
    ai_reason TEXT,
    user_value_summary TEXT,
    action_suggestion TEXT,
    urgency VARCHAR(16),
    relevance VARCHAR(16),
    actionability VARCHAR(16),
    risk_level VARCHAR(16),
    feedback VARCHAR(16),
    feedback_at TIMESTAMPTZ,
    watchlist_category VARCHAR(64),
    watchlist_label VARCHAR(128),
    watchlist_priority BIGINT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_telegram_messages_chat_id ON telegram_messages (source_chat_id);
CREATE INDEX IF NOT EXISTS idx_telegram_messages_source_platform ON telegram_messages (source_platform);
CREATE INDEX IF NOT EXISTS idx_telegram_messages_message_id ON telegram_messages (source_message_id);
CREATE INDEX IF NOT EXISTS idx_telegram_messages_dedup_key ON telegram_messages (dedup_key);
CREATE INDEX IF NOT EXISTS idx_telegram_messages_category ON telegram_messages (category);
CREATE INDEX IF NOT EXISTS idx_telegram_messages_score ON telegram_messages (score);
CREATE INDEX IF NOT EXISTS idx_telegram_messages_signal_level ON telegram_messages (signal_level);
CREATE INDEX IF NOT EXISTS idx_telegram_messages_push_status ON telegram_messages (push_status);
CREATE INDEX IF NOT EXISTS idx_telegram_messages_possible_duplicate ON telegram_messages (possible_duplicate);
CREATE INDEX IF NOT EXISTS idx_telegram_messages_duplicate_of_message_id ON telegram_messages (duplicate_of_message_id);
CREATE INDEX IF NOT EXISTS idx_telegram_messages_event_id ON telegram_messages (event_id);
CREATE INDEX IF NOT EXISTS idx_telegram_messages_feedback ON telegram_messages (feedback);
CREATE INDEX IF NOT EXISTS idx_telegram_messages_ai_decision ON telegram_messages (ai_decision);
CREATE INDEX IF NOT EXISTS idx_telegram_messages_watchlist_category ON telegram_messages (watchlist_category);
CREATE INDEX IF NOT EXISTS idx_telegram_messages_watchlist_priority ON telegram_messages (watchlist_priority);

CREATE TABLE IF NOT EXISTS records (
    record_id BIGSERIAL PRIMARY KEY,
    source_platform VARCHAR(32) NOT NULL,
    source VARCHAR(64),
    source_channel VARCHAR(255) NOT NULL,
    source_message_id VARCHAR(128) NOT NULL,
    event_time TIMESTAMPTZ,
    collected_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    raw_text TEXT NOT NULL,
    cleaned_text TEXT NOT NULL,
    payload TEXT,
    raw_metadata TEXT,
    dedup_key VARCHAR(64) NOT NULL,
    watchlist_category VARCHAR(64),
    watchlist_label VARCHAR(128),
    watchlist_priority BIGINT,
    legacy_message_id BIGINT UNIQUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_records_source_identity UNIQUE (source_platform, source_channel, source_message_id),
    CONSTRAINT uq_records_dedup_key UNIQUE (dedup_key)
);

CREATE INDEX IF NOT EXISTS idx_records_source_platform ON records (source_platform);
CREATE INDEX IF NOT EXISTS idx_records_source ON records (source);
CREATE INDEX IF NOT EXISTS idx_records_source_channel ON records (source_channel);
CREATE INDEX IF NOT EXISTS idx_records_source_message_id ON records (source_message_id);
CREATE INDEX IF NOT EXISTS idx_records_event_time ON records (event_time);
CREATE INDEX IF NOT EXISTS idx_records_dedup_key ON records (dedup_key);
CREATE INDEX IF NOT EXISTS idx_records_watchlist_category ON records (watchlist_category);
CREATE INDEX IF NOT EXISTS idx_records_watchlist_priority ON records (watchlist_priority);
CREATE INDEX IF NOT EXISTS idx_records_legacy_message_id ON records (legacy_message_id);

CREATE TABLE IF NOT EXISTS analyses (
    analysis_id BIGSERIAL PRIMARY KEY,
    record_id BIGINT NOT NULL REFERENCES records(record_id),
    model_name VARCHAR(128),
    model_version VARCHAR(64),
    prompt_version VARCHAR(64),
    signal_type VARCHAR(64),
    ai_decision VARCHAR(16),
    ai_confidence DOUBLE PRECISION,
    ai_reason TEXT,
    user_value_summary TEXT,
    action_suggestion TEXT,
    urgency VARCHAR(16),
    relevance VARCHAR(16),
    actionability VARCHAR(16),
    risk_level VARCHAR(16),
    source_profile TEXT,
    score DOUBLE PRECISION NOT NULL DEFAULT 0,
    score_breakdown TEXT,
    legacy_message_id BIGINT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_analyses_record_id ON analyses (record_id);
CREATE INDEX IF NOT EXISTS idx_analyses_signal_type ON analyses (signal_type);
CREATE INDEX IF NOT EXISTS idx_analyses_ai_decision ON analyses (ai_decision);
CREATE INDEX IF NOT EXISTS idx_analyses_score ON analyses (score);
CREATE INDEX IF NOT EXISTS idx_analyses_legacy_message_id ON analyses (legacy_message_id);

CREATE TABLE IF NOT EXISTS event_records (
    id BIGSERIAL PRIMARY KEY,
    event_id BIGINT NOT NULL REFERENCES events(id),
    record_id BIGINT NOT NULL REFERENCES records(record_id),
    analysis_id BIGINT REFERENCES analyses(analysis_id),
    event_similarity DOUBLE PRECISION,
    event_match_reason TEXT,
    legacy_message_id BIGINT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_event_records_link UNIQUE (event_id, record_id, analysis_id)
);

CREATE INDEX IF NOT EXISTS idx_event_records_event_id ON event_records (event_id);
CREATE INDEX IF NOT EXISTS idx_event_records_record_id ON event_records (record_id);
CREATE INDEX IF NOT EXISTS idx_event_records_analysis_id ON event_records (analysis_id);
CREATE INDEX IF NOT EXISTS idx_event_records_legacy_message_id ON event_records (legacy_message_id);

CREATE TABLE IF NOT EXISTS feedbacks (
    feedback_id BIGSERIAL PRIMARY KEY,
    feedback_dedup_key VARCHAR(64) UNIQUE,
    target_type VARCHAR(16) NOT NULL,
    record_id BIGINT REFERENCES records(record_id),
    event_id BIGINT REFERENCES events(id),
    feedback VARCHAR(16) NOT NULL,
    note TEXT,
    feedback_source VARCHAR(64),
    legacy_message_id BIGINT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_feedbacks_target_type ON feedbacks (target_type);
CREATE UNIQUE INDEX IF NOT EXISTS idx_feedbacks_feedback_dedup_key ON feedbacks (feedback_dedup_key);
CREATE INDEX IF NOT EXISTS idx_feedbacks_record_id ON feedbacks (record_id);
CREATE INDEX IF NOT EXISTS idx_feedbacks_event_id ON feedbacks (event_id);
CREATE INDEX IF NOT EXISTS idx_feedbacks_feedback ON feedbacks (feedback);
CREATE INDEX IF NOT EXISTS idx_feedbacks_feedback_source ON feedbacks (feedback_source);
CREATE INDEX IF NOT EXISTS idx_feedbacks_legacy_message_id ON feedbacks (legacy_message_id);

CREATE TABLE IF NOT EXISTS collector_state (
    id BIGSERIAL PRIMARY KEY,
    collector_name VARCHAR(64) NOT NULL,
    source_key VARCHAR(255) NOT NULL,
    last_seen_id VARCHAR(128),
    last_seen_time TIMESTAMPTZ,
    last_fetch_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_collector_state_name_key UNIQUE (collector_name, source_key)
);

CREATE INDEX IF NOT EXISTS idx_collector_state_collector_name ON collector_state (collector_name);
CREATE INDEX IF NOT EXISTS idx_collector_state_source_key ON collector_state (source_key);
