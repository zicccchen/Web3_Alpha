import unittest
import json
from datetime import datetime, timezone
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from unittest.mock import patch

from app.api.routes import _extract_feishu_action_value, _feedback_dedup_key, _format_feedback_event_item, _format_feedback_message_item, _source_profile_stats, extract_original_url, feishu_feedback, format_duplicate_backfill_payload, format_stats_payload, format_top_message
from app.core.config import Settings
from app.db.models import Analysis, Event, EventRecord, Feedback, Record
from app.config.discord_watchlists import (
    DiscordChannelConfig,
    DiscordSourceConfig,
    DiscordWatchlists,
    load_discord_watchlists,
)
from app.config.telegram_watchlists import (
    TelegramChannelConfig,
    load_telegram_watchlists,
    normalize_telegram_channel,
)
from app.collectors.discord_collector import (
    DiscordCollector,
    discord_message_from_payload,
    source_message_from_discord_message,
)
from app.collectors.telegram_api_collector import (
    TelegramApiCollector,
    TelegramApiMessage,
    _telegram_entity_ref,
    extract_telegram_raw_text,
    source_message_from_telegram_message,
)
from app.services import pipeline as pipeline_module
from app.services.calibration import build_calibration_report
from app.schemas.message import SourceMessage
from app.services.notifier import NotificationResult
from app.services.notifier import build_app_bot_message
from app.services.notifier import build_feedback_card
from app.services.pipeline import MessagePipeline
from app.services.cleaner import clean_message
from app.services.duplicates import (
    apply_backfill_marks,
    backfill_possible_duplicates,
    duplicate_similarity,
    find_possible_duplicate,
    text_similarity,
)
from app.services.event_cluster import EventClusterer, best_event_match, extract_event_features, rank_event_candidates
from app.services.event_backfill import apply_event_backfill_marks, format_event_backfill_payload, plan_event_backfill
from app.services.l1_adapter import L1Adapter, L1Record
from app.services.repository import _analysis_from_message_data, _record_from_message_data
from app.services.analyzer import AIAnalyzer, build_analysis_input
from app.services.user_profile import load_user_profile
from app.services.scorer import (
    ScoreRules,
    SignalRules,
    SignalTypeRule,
    load_score_rules,
    normalize_signal_type,
    score_message,
)
from app.services.source_profiles import load_source_profiles, match_source_profile, normalize_source_key
from app.services.signal import signal_level_for_score
from app.services.watchlists import Watchlists, WatchlistCategory, WatchlistMatch, load_watchlists
from app.sources.public_telegram.collector import TelegramPublicPageParser
from app.sources.x_feed.collector import XFeed, XFeedCollector, XFeedEntry, parse_x_feed


class CleanerTests(unittest.TestCase):
    def test_clean_message_generates_stable_dedup_key(self) -> None:
        first = clean_message("  Hello   Alpha https://example.com/path ")
        second = clean_message("hello alpha")

        self.assertEqual(first.cleaned_text, "hello alpha")
        self.assertEqual(first.dedup_key, second.dedup_key)
        self.assertEqual(first.language, "en")


class DataModelRefactorTests(unittest.TestCase):
    def test_layered_tables_define_required_columns(self) -> None:
        self.assertIn("records", Record.__tablename__)
        self.assertIn("analyses", Analysis.__tablename__)
        self.assertIn("event_records", EventRecord.__tablename__)
        self.assertIn("feedbacks", Feedback.__tablename__)

        for column in (
            "record_id",
            "source_platform",
            "source_channel",
            "source_message_id",
            "event_time",
            "raw_text",
            "cleaned_text",
            "payload",
            "raw_metadata",
            "dedup_key",
            "watchlist_category",
            "watchlist_label",
            "watchlist_priority",
        ):
            self.assertIn(column, Record.__table__.c)

        for column in (
            "analysis_id",
            "record_id",
            "model_name",
            "prompt_version",
            "signal_type",
            "ai_decision",
            "source_profile",
            "score",
            "score_breakdown",
        ):
            self.assertIn(column, Analysis.__table__.c)

        for column in ("event_id", "record_id", "analysis_id", "event_similarity", "event_match_reason"):
            self.assertIn(column, EventRecord.__table__.c)

        for column in ("feedback_id", "target_type", "record_id", "event_id", "feedback", "note", "feedback_source"):
            self.assertIn(column, Feedback.__table__.c)

        for column in ("upgrade_count", "last_upgrade_at", "last_upgrade_summary", "last_pushed_at"):
            self.assertIn(column, Event.__table__.c)

    def test_repository_builds_record_and_analysis_mirrors_from_legacy_message_data(self) -> None:
        legacy = SimpleNamespace(id=123)
        analysis_json = json.dumps(
            {
                "signal_type": "airdrop",
                "source_metadata": {"original_url": "https://example.com"},
                "source_context": {
                    "source_profile": {
                        "label": "Base",
                        "role": "official",
                        "importance": 10,
                    }
                },
                "score_breakdown": {"final_score": 88, "signal_type": "airdrop"},
            },
            ensure_ascii=False,
        )
        message_data = {
            "source": "x",
            "source_platform": "x",
            "source_chat_id": "Twitter @base",
            "source_message_id": "tweet-1",
            "raw_text": "Base season points",
            "cleaned_text": "base season points",
            "dedup_key": "dedup",
            "analysis_json": analysis_json,
            "ai_decision": "watch",
            "ai_confidence": 91,
            "ai_reason": "Base official hint",
            "user_value_summary": "需要跟踪",
            "action_suggestion": "watch",
            "urgency": "medium",
            "relevance": "high",
            "actionability": "low",
            "risk_level": "low",
            "score": 88,
            "watchlist_category": "base_core",
            "watchlist_label": "Base核心生态",
            "watchlist_priority": 10,
            "created_at": datetime(2026, 6, 7, tzinfo=timezone.utc),
        }
        record = _record_from_message_data(legacy, message_data, json.loads(analysis_json))
        analysis = _analysis_from_message_data(record.record_id, legacy.id, message_data, json.loads(analysis_json))

        self.assertEqual(record.record_id, 123)
        self.assertEqual(record.source_platform, "x")
        self.assertEqual(record.source_channel, "Twitter @base")
        self.assertEqual(record.watchlist_category, "base_core")
        self.assertEqual(json.loads(record.payload)["original_url"], "https://example.com")
        self.assertEqual(analysis.record_id, 123)
        self.assertEqual(analysis.ai_decision, "watch")
        self.assertEqual(json.loads(analysis.source_profile)["role"], "official")
        self.assertEqual(json.loads(analysis.score_breakdown)["final_score"], 88)

    def test_l1_adapter_outputs_existing_source_message_contract(self) -> None:
        record = L1Record(
            source_platform="l1",
            source="company_l1",
            source_channel="alpha_feed",
            source_message_id="abc-1",
            raw_text="Project opened points campaign",
            event_time=datetime(2026, 6, 7, tzinfo=timezone.utc),
            payload={"provider": "internal"},
            raw_metadata={"source_chat_title": "Company L1 Alpha"},
            watchlist_category="airdrop_alpha",
            watchlist_label="撸毛Alpha",
            watchlist_priority=9,
        )
        message = L1Adapter().to_source_message(record)

        self.assertIsInstance(message, SourceMessage)
        self.assertEqual(message.source, "l1")
        self.assertEqual(message.source_chat_id, "alpha_feed")
        self.assertEqual(message.source_chat_title, "Company L1 Alpha")
        self.assertEqual(message.source_message_id, "abc-1")
        self.assertEqual(message.metadata["l1_source"], "company_l1")
        self.assertEqual(message.watchlist_priority, 9)


class ScorerTests(unittest.TestCase):
    def test_score_message_uses_v2_breakdown_and_caps_score(self) -> None:
        scored = score_message("Binance listing and mainnet launch", 95, signal_type="exchange_listing")

        self.assertEqual(scored.keyword_bonus, 3)
        self.assertEqual(scored.signal_bonus, 6)
        self.assertEqual(scored.content_score, 75)
        self.assertEqual(scored.final_score, 81.0)
        self.assertEqual(scored.breakdown()["final_score"], scored.final_score)
        self.assertIn("content_score", scored.breakdown())
        self.assertIn("source_score", scored.breakdown())
        self.assertIn("context_score", scored.breakdown())
        self.assertIn("signal_score", scored.breakdown())
        self.assertIn("ai_score", scored.breakdown())
        self.assertIn("matched_keywords", scored.breakdown())
        self.assertIn("signal_type", scored.breakdown())

    def test_score_formula_uses_source_and_context_scores(self) -> None:
        scored = score_message("binance", 92, signal_type="exchange_listing")

        self.assertEqual(scored.keyword_bonus, 2)
        self.assertEqual(scored.signal_bonus, 6)
        self.assertEqual(scored.source_bonus, 0)
        self.assertEqual(scored.risk_penalty, 0)
        self.assertEqual(scored.final_score, 81.0)

        profiled = score_message(
            "base airdrop",
            60,
            signal_type="airdrop",
            source_profile={"key": "base", "score": 14, "role": "Base官方", "description": ""},
            watchlist_category="base_core",
            watchlist_priority=10,
        )
        self.assertEqual(profiled.source_score, 14)
        self.assertEqual(profiled.context_score, 10)
        self.assertEqual(profiled.final_score, 100.0)

    def test_score_formula_uses_ai_source_and_context_scores(self) -> None:
        scored = score_message(
            "high quality x alpha",
            55,
            signal_type="points_program",
            analysis={"content_score": 55, "source_score": 6, "context_score": 8, "signal_score": 12, "risk_penalty": 0},
        )

        self.assertEqual(scored.source_score, 6)
        self.assertEqual(scored.context_score, 8)
        self.assertEqual(scored.final_score, 81.0)

    def test_score_formula_subtracts_risk_penalty(self) -> None:
        scored = score_message("claim rumor", 50)

        self.assertEqual(scored.keyword_bonus, 2.5)
        self.assertEqual(scored.risk_penalty, 10)
        self.assertEqual(scored.final_score, 42.5)

    def test_score_is_clamped_between_zero_and_one_hundred(self) -> None:
        high = score_message(
            "airdrop listing hack exploit funding launch governance token mainnet partnership",
            99,
            signal_type="airdrop",
            source_profile={"key": "base", "score": 15},
            watchlist_category="base_core",
            watchlist_priority=10,
        )
        low = score_message("private key seed phrase", 10)

        self.assertEqual(high.final_score, 100.0)
        self.assertEqual(low.final_score, 0.0)

    def test_keyword_bonus_accumulates_until_configured_max(self) -> None:
        scored = score_message("airdrop claim snapshot", 50)

        self.assertEqual(scored.keyword_bonus, 3)
        self.assertEqual(scored.keyword_auxiliary_bonus, 3)
        self.assertEqual(scored.matched_keywords, ["airdrop", "claim", "snapshot"])

    def test_risk_keywords_accumulate_until_configured_max(self) -> None:
        scored = score_message("rumor unconfirmed", 50)

        self.assertEqual(scored.risk_penalty, 20)
        self.assertEqual(scored.matched_risk_keywords, ["rumor", "unconfirmed"])

    def test_token_does_not_trigger_keyword_bonus(self) -> None:
        scored = score_message("token", 50)

        self.assertEqual(scored.keyword_bonus, 0)
        self.assertEqual(scored.final_score, 50.0)

    def test_source_profiles_load_and_match(self) -> None:
        with TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "source_profiles.yaml"
            path.write_text(
                """
sources:
  base:
    label: Base
    role: official
    ecosystem: Base
    importance: 10
    specialty:
      - ecosystem
      - builder
    description: Base official
  ai_9684xtpa:
    label: ai_9684xtpa
    role: onchain_monitor
    ecosystem: Multi-chain
    importance: 8
    specialty:
      - smart_money
    description: whale tracker
  default:
    role: unknown
    ecosystem: unknown
    importance: 0
    specialty: []
""",
                encoding="utf-8",
            )
            profiles = load_source_profiles(path)

        self.assertEqual(profiles["base"].importance, 10)
        self.assertEqual(profiles["base"].score, 10)
        self.assertEqual(profiles["ai_9684xtpa"].role, "onchain_monitor")
        self.assertEqual(profiles["ai_9684xtpa"].specialty, ("smart_money",))
        self.assertEqual(match_source_profile({"channel": "Twitter @Base"}, profiles).key, "base")
        self.assertEqual(match_source_profile({"channel": "@ai_9684xtpa"}, profiles).key, "ai_9684xtpa")
        self.assertEqual(
            match_source_profile({"channel_id": "http://rsshub:1200/twitter/user/base"}, profiles).key,
            "base",
        )
        self.assertEqual(match_source_profile({"channel": "random"}, profiles).key, "default")

    def test_source_profile_handle_normalization(self) -> None:
        for value in ("@base", "base", "Twitter @Base", "rss:base", "rss:Twitter @Base"):
            with self.subTest(value=value):
                self.assertEqual(normalize_source_key(value), "base")

    def test_analysis_input_includes_source_profile_context(self) -> None:
        prompt = build_analysis_input(
            "👀",
            source_context={
                "source_platform": "x",
                "channel": "Twitter @JessePollak",
                "source_profile": {
                    "key": "jessepollak",
                    "label": "Jesse Pollak",
                    "role": "founder",
                    "ecosystem": "Base",
                    "importance": 10,
                    "specialty": ["base", "builder", "ecosystem"],
                    "description": "Base 创始人。",
                },
            },
        )

        self.assertIn("source_profile_context:", prompt)
        self.assertIn("- Handle: jessepollak", prompt)
        self.assertIn("- Role: founder", prompt)
        self.assertIn("- Ecosystem: Base", prompt)
        self.assertIn("- Importance: 10", prompt)
        self.assertIn("不能只因为 importance 高就自动 push", prompt)

    def test_score_formula_with_manual_rule_case_matches_expected_55(self) -> None:
        rules = ScoreRules(
            keywords={"claim": 10},
            risk_keywords={"rumor": 5},
            keyword_bonus_max=12,
            risk_penalty_max=40,
            final_score_min=0,
            final_score_max=100,
        )
        scored = score_message("claim rumor", 50, rules=rules)

        self.assertEqual(scored.final_score, 47.5)

    def test_missing_config_file_uses_fallback_rules(self) -> None:
        rules = load_score_rules(Path("/definitely/missing/score_rules.yaml"))
        scored = score_message("airdrop claim", 92, rules=rules)

        self.assertEqual(scored.keyword_bonus, 3)
        self.assertEqual(scored.final_score, 75.0)

    def test_signal_type_airdrop_adds_signal_bonus(self) -> None:
        scored = score_message("new user eligibility event", 70, signal_type="airdrop")

        self.assertEqual(scored.signal_type, "airdrop")
        self.assertEqual(scored.signal_label, "空投机会")
        self.assertEqual(scored.signal_bonus, 15)
        self.assertEqual(scored.final_score, 85.0)

    def test_signal_type_points_program_adds_signal_bonus(self) -> None:
        scored = score_message("new loyalty program", 70, signal_type="points_program")

        self.assertEqual(scored.signal_bonus, 12)
        self.assertEqual(scored.final_score, 82.0)

    def test_low_signal_types_have_zero_signal_bonus(self) -> None:
        for signal_type in ("ipo", "macro", "unknown"):
            with self.subTest(signal_type=signal_type):
                scored = score_message("Binance launch pre-IPO oil market update", 80, signal_type=signal_type)

                self.assertEqual(scored.signal_bonus, 0)
                self.assertEqual(scored.keyword_bonus, 0)
                self.assertLess(scored.final_score, 90)

    def test_invalid_signal_type_normalizes_to_unknown(self) -> None:
        scored = score_message("airdrop", 70, signal_type="not_a_real_type")

        self.assertEqual(scored.signal_type, "unknown")
        self.assertEqual(normalize_signal_type("not_a_real_type"), "unknown")

    def test_final_score_includes_signal_bonus(self) -> None:
        rules = ScoreRules(
            keywords={"claim": 10},
            risk_keywords={"rumor": 5},
            keyword_bonus_max=12,
            risk_penalty_max=40,
            final_score_min=0,
            final_score_max=100,
        )
        signal_rules = SignalRules(
            signal_types={"airdrop": SignalTypeRule(bonus=15, label="空投机会"), "unknown": SignalTypeRule(bonus=0, label="未知")},
            signal_bonus_max=15,
        )
        scored = score_message("claim rumor", 50, signal_type="airdrop", rules=rules, signal_rules=signal_rules)

        self.assertEqual(scored.final_score, 62.5)

    def test_score_breakdown_includes_signal_fields(self) -> None:
        breakdown = score_message("airdrop", 70, signal_type="airdrop").breakdown()

        self.assertEqual(breakdown["signal_type"], "airdrop")
        self.assertEqual(breakdown["signal_label"], "空投机会")
        self.assertEqual(breakdown["signal_bonus"], 15)

    def test_pre_ipo_message_does_not_enter_s_level_from_generic_keywords(self) -> None:
        scored = score_message(
            "Binance Futures Will Launch ANTHROPICUSDT Perpetual Contract Pre-IPO Trading",
            80,
            signal_type="ipo",
        )

        self.assertEqual(scored.signal_bonus, 0)
        self.assertEqual(scored.keyword_bonus, 0)
        self.assertLess(scored.final_score, 90)


class AnalyzerTests(unittest.TestCase):
    def test_parse_json_normalizes_invalid_signal_type(self) -> None:
        analyzer = AIAnalyzer.__new__(AIAnalyzer)
        payload = analyzer._parse_json(
            '{"summary_zh":"s","category":"其他","signal_type":"bad","importance_score":50,"reason":"r"}'
        )

        self.assertEqual(payload["signal_type"], "unknown")

    def test_parse_json_keeps_v2_score_fields(self) -> None:
        analyzer = AIAnalyzer.__new__(AIAnalyzer)
        payload = analyzer._parse_json(
            """
            {
              "event_title":"Base支付事件",
              "summary_zh":"Base推动USDC支付",
              "category":"Alpha机会",
              "signal_type":"partnership",
              "content_score":50,
              "source_score":12,
              "context_score":8,
              "signal_score":4,
              "risk_penalty":1,
              "importance_score":53,
              "reason":"来源和生态相关"
            }
            """
        )

        self.assertEqual(payload["content_score"], 50)
        self.assertEqual(payload["source_score"], 12)
        self.assertEqual(payload["context_score"], 8)
        self.assertEqual(payload["signal_score"], 4)
        self.assertEqual(payload["risk_penalty"], 1)

    def test_parse_json_invalid_decision_defaults_to_watch(self) -> None:
        analyzer = AIAnalyzer.__new__(AIAnalyzer)
        payload = analyzer._parse_json(
            '{"summary_zh":"s","category":"其他","signal_type":"unknown","decision":"send","importance_score":50,"reason":"r"}'
        )

        self.assertEqual(payload["decision"], "watch")

    def test_user_profile_missing_file_uses_fallback(self) -> None:
        profile = load_user_profile(Path("/definitely/missing/user_profile.yaml"))

        self.assertIn("push", profile.decision_levels)
        self.assertTrue(profile.user_goals)

    def test_build_analysis_input_includes_source_context_separately(self) -> None:
        payload = build_analysis_input(
            "预计一月内发币，建议冲积分",
            {
                "source_platform": "discord",
                "project": "kui4",
                "ecosystem": "Unknown",
                "channel": "kui4:kui4",
                "channel_id": "1402517669550231623",
            },
        )

        self.assertIn("来源上下文", payload)
        self.assertIn("- 来源项目: kui4", payload)
        self.assertIn("消息正文：", payload)
        self.assertIn("预计一月内发币", payload)


class PublicTelegramParserTests(unittest.TestCase):
    def test_parser_extracts_message_text_and_id(self) -> None:
        html = """
        <div class="tgme_widget_message" data-post="alpha/42">
          <div class="tgme_widget_message_text js-message_text">hello<br>world</div>
        </div>
        """
        parser = TelegramPublicPageParser("alpha")
        parser.feed(html)
        parser.close()

        self.assertEqual(len(parser.messages), 1)
        self.assertEqual(parser.messages[0].message_id, 42)
        self.assertEqual(parser.messages[0].text, "hello\nworld")


class TelegramApiCollectorTests(unittest.IsolatedAsyncioTestCase):
    def test_load_telegram_watchlists_reads_yaml(self) -> None:
        watchlists = load_telegram_watchlists(Path("config/telegram_watchlists.yaml"))

        self.assertIn("media", [category.key for category in watchlists.categories])
        self.assertIn("theblockbeats", watchlists.channels_by_normalized)
        self.assertEqual(watchlists.match_channel("@TechFlowDaily").category, "media")
        self.assertEqual(watchlists.match_channel("@TechFlowDaily").label, "Telegram媒体")
        self.assertEqual(watchlists.match_channel("@TechFlowDaily").priority, 8)

    def test_telegram_watchlists_dedup_by_highest_priority(self) -> None:
        with TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "telegram_watchlists.yaml"
            path.write_text(
                """
telegram_watchlists:
  low:
    label: Low
    priority: 1
    channels: [BaseBuilders]
  high:
    label: High
    priority: 9
    channels: ["@basebuilders"]
""",
                encoding="utf-8",
            )

            watchlists = load_telegram_watchlists(path)

        self.assertEqual(len(watchlists.deduped_channels), 1)
        self.assertEqual(watchlists.match_channel("https://t.me/BaseBuilders").category, "high")
        self.assertEqual(normalize_telegram_channel("https://t.me/s/BaseBuilders"), "basebuilders")

    def test_private_group_numeric_id_resolves_to_int(self) -> None:
        self.assertEqual(_telegram_entity_ref("-1002422638120"), -1002422638120)
        self.assertEqual(_telegram_entity_ref("TechFlowDaily"), "TechFlowDaily")

    async def test_api_collector_can_start_and_stop(self) -> None:
        pipeline = MockPipeline()
        adapter = MockTelegramAdapter()
        repository = MockCollectorStateRepository()
        watchlists = load_telegram_watchlists(_telegram_watchlist_path(["BaseBuilders"]))
        collector = TelegramApiCollector(pipeline, watchlists=watchlists, adapter=adapter, repository=repository)

        with patch.object(pipeline_module.settings, "telegram_source", "api"):
            await collector.start()
            await collector.stop()

        self.assertTrue(adapter.started)
        self.assertTrue(adapter.stopped)

    async def test_incremental_sync_updates_last_message_id(self) -> None:
        pipeline = MockPipeline()
        adapter = MockTelegramAdapter(
            messages={
                "BaseBuilders": [
                    TelegramApiMessage(
                        source_chat_id="-1001",
                        source_chat_title="BaseBuilders",
                        source_message_id="101",
                        raw_text="old",
                    ),
                    TelegramApiMessage(
                        source_chat_id="-1001",
                        source_chat_title="BaseBuilders",
                        source_message_id="102",
                        raw_text="new alpha",
                    ),
                ]
            }
        )
        repository = MockCollectorStateRepository(states={"basebuilders": SimpleNamespace(last_seen_id="101", last_seen_time=None)})
        watchlists = load_telegram_watchlists(_telegram_watchlist_path(["BaseBuilders"]))
        collector = TelegramApiCollector(pipeline, watchlists=watchlists, adapter=adapter, repository=repository)

        await collector.poll_once()

        self.assertEqual([message.source_message_id for message in pipeline.messages], ["102"])
        self.assertEqual(repository.states["basebuilders"].last_seen_id, "102")

    async def test_group_and_channel_messages_convert_to_pipeline_message(self) -> None:
        channel = TelegramChannelConfig(channel="AlphaGroup", category="community", label="Community", priority=7)
        api_message = TelegramApiMessage(
            source_chat_id="-1002",
            source_chat_title="Alpha Group",
            source_message_id="88",
            raw_text="group alpha",
            author_name="builder",
            message_url="https://t.me/c/1002/88",
            is_group=True,
            is_channel=False,
        )

        source_message = source_message_from_telegram_message(api_message, channel)

        self.assertEqual(source_message.source, "telegram")
        self.assertEqual(source_message.source_chat_id, "-1002")
        self.assertEqual(source_message.source_message_id, "88")
        self.assertEqual(source_message.author_name, "builder")
        self.assertEqual(source_message.watchlist_category, "community")
        self.assertEqual(source_message.watchlist_priority, 7)
        self.assertTrue(source_message.metadata["telegram_is_group"])
        self.assertIn("https://t.me/c/1002/88", source_message.raw_text)

    def test_media_caption_is_preserved(self) -> None:
        message = SimpleNamespace(raw_text="图片说明：Base Season 开始", media=object(), fwd_from=None)

        self.assertEqual(extract_telegram_raw_text(message), "图片说明：Base Season 开始")

    async def test_telegram_message_enters_mock_pipeline(self) -> None:
        pipeline = MockPipeline()
        adapter = MockTelegramAdapter(
            messages={
                "BaseBuilders": [
                    TelegramApiMessage(
                        source_chat_id="-1001",
                        source_chat_title="BaseBuilders",
                        source_message_id="201",
                        raw_text="Base builder incentive update",
                        is_channel=True,
                    )
                ]
            }
        )
        repository = MockCollectorStateRepository(states={"basebuilders": SimpleNamespace(last_seen_id="200", last_seen_time=None)})
        watchlists = load_telegram_watchlists(_telegram_watchlist_path(["BaseBuilders"]))
        collector = TelegramApiCollector(pipeline, watchlists=watchlists, adapter=adapter, repository=repository)

        stats = await collector.poll_once()

        self.assertEqual(stats["new_message_count"], 1)
        self.assertEqual(pipeline.messages[0].source, "telegram")
        self.assertEqual(pipeline.messages[0].watchlist_label, "Base Alpha")

    async def test_cold_start_initializes_cursor_without_processing_history(self) -> None:
        pipeline = MockPipeline()
        adapter = MockTelegramAdapter(
            messages={
                "BaseBuilders": [
                    TelegramApiMessage(
                        source_chat_id="-1001",
                        source_chat_title="BaseBuilders",
                        source_message_id="301",
                        raw_text="historical alpha",
                    )
                ]
            }
        )
        repository = MockCollectorStateRepository()
        watchlists = load_telegram_watchlists(_telegram_watchlist_path(["BaseBuilders"]))
        collector = TelegramApiCollector(pipeline, watchlists=watchlists, adapter=adapter, repository=repository)

        stats = await collector.poll_once()

        self.assertEqual(stats["new_message_count"], 0)
        self.assertEqual(pipeline.messages, [])
        self.assertEqual(repository.states["basebuilders"].last_seen_id, "301")


class ConfigTests(unittest.TestCase):
    def test_settings_accept_comma_separated_public_channels_and_empty_optional_values(self) -> None:
        settings = Settings(
            DATABASE_URL="postgresql+asyncpg://postgres:postgres@postgres:5432/web3_alpha",
            REDIS_URL="redis://redis:6379/0",
            TELEGRAM_API_ID="",
            PUBLIC_TELEGRAM_CHANNELS="@a,https://t.me/b",
            DISCORD_CHANNEL_IDS="111,222",
        )

        self.assertIsNone(settings.telegram_api_id)
        self.assertEqual(settings.public_telegram_channels, ["@a", "https://t.me/b"])
        self.assertEqual(settings.discord_channel_ids, ["111", "222"])


class WatchlistTests(unittest.TestCase):
    def test_load_watchlists_reads_yaml(self) -> None:
        watchlists = load_watchlists(Path("config/watchlists.yaml"))

        self.assertIn("base", watchlists.accounts_by_normalized)
        self.assertEqual(watchlists.match_account("base").category, "base_core")
        self.assertEqual(watchlists.match_account("base").label, "Base核心生态")
        self.assertEqual(watchlists.match_account("base").priority, 10)

    def test_watchlists_dedup_accounts(self) -> None:
        with TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "watchlists.yaml"
            path.write_text(
                """
watchlists:
  high:
    label: High
    priority: 10
    description: ""
    accounts: [base, zora]
  low:
    label: Low
    priority: 1
    description: ""
    accounts: [Base, dwr]
""",
                encoding="utf-8",
            )

            watchlists = load_watchlists(path)

        self.assertEqual(sorted(account.lower() for account in watchlists.deduped_accounts), ["base", "dwr", "zora"])

    def test_same_account_uses_highest_priority_category(self) -> None:
        with TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "watchlists.yaml"
            path.write_text(
                """
watchlists:
  low:
    label: Low
    priority: 1
    description: ""
    accounts: [base]
  high:
    label: High
    priority: 9
    description: ""
    accounts: [Base]
""",
                encoding="utf-8",
            )

            match = load_watchlists(path).match_account("@BASE")

        self.assertEqual(match.category, "high")
        self.assertEqual(match.label, "High")
        self.assertEqual(match.priority, 9)


class DiscordCollectorTests(unittest.IsolatedAsyncioTestCase):
    def test_load_discord_watchlists_reads_multi_project_config(self) -> None:
        with TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "discord_watchlists.yaml"
            path.write_text(
                """
discord_watchlists:
  base:
    label: Base Discord
    enabled: true
    project: Base
    ecosystem: Base
    priority: 10
    channels:
      - channel_id: "111"
        name: announcements
        type: announcement
  zora:
    label: Zora Discord
    enabled: false
    project: Zora
    ecosystem: Base
    priority: 8
    channels:
      - channel_id: "222"
        name: updates
        type: announcement
""",
                encoding="utf-8",
            )

            watchlists = load_discord_watchlists(path)

        self.assertEqual(len(watchlists.sources), 2)
        self.assertEqual(len(watchlists.enabled_sources), 1)
        self.assertEqual(watchlists.enabled_channels[0][0].project, "Base")
        self.assertEqual(watchlists.enabled_channels[0][1].channel_id, "111")
        self.assertIsNone(watchlists.source_for_channel("222"))

    def test_discord_message_converts_to_pipeline_message(self) -> None:
        source = DiscordSourceConfig(
            key="base",
            label="Base Discord",
            enabled=True,
            project="Base",
            ecosystem="Base",
            priority=10,
            channels=(),
        )
        channel = DiscordChannelConfig(channel_id="999", name="announcements", type="announcement")
        payload = {
            "id": "123456789012345678",
            "guild_id": "555",
            "content": "New airdrop info https://example.com/news",
            "timestamp": "2026-06-05T10:00:00+00:00",
            "author": {"username": "alice"},
            "attachments": [{"url": "https://cdn.example.com/a.png"}],
        }

        discord_message = discord_message_from_payload(payload, source, channel)
        source_message = source_message_from_discord_message(discord_message)

        self.assertEqual(source_message.source, "discord")
        self.assertEqual(source_message.source_chat_id, "999")
        self.assertEqual(source_message.source_chat_title, "Base:announcements")
        self.assertEqual(source_message.source_message_id, "123456789012345678")
        self.assertEqual(source_message.author_name, "alice")
        self.assertEqual(source_message.watchlist_category, "base")
        self.assertEqual(source_message.watchlist_priority, 10)
        self.assertEqual(source_message.metadata["project"], "Base")
        self.assertEqual(source_message.metadata["discord_channel_type"], "announcement")
        self.assertIn("https://discord.com/channels/555/999/123456789012345678", source_message.raw_text)
        self.assertIn("https://cdn.example.com/a.png", source_message.raw_text)

    def test_discord_dedup_key_includes_channel_and_message_id(self) -> None:
        message = SourceMessage(
            source="discord",
            source_chat_id="999",
            source_chat_title="Base:announcements",
            source_message_id="12",
            raw_text="Discord alpha update",
        )
        other_channel = message.model_copy(update={"source_chat_id": "888"})
        other_message = message.model_copy(update={"source_message_id": "13"})
        cleaned = clean_message(message.raw_text)

        self.assertNotEqual(
            MessagePipeline._source_dedup_key(message, cleaned.dedup_key),
            MessagePipeline._source_dedup_key(other_channel, cleaned.dedup_key),
        )
        self.assertNotEqual(
            MessagePipeline._source_dedup_key(message, cleaned.dedup_key),
            MessagePipeline._source_dedup_key(other_message, cleaned.dedup_key),
        )

    async def test_discord_message_enters_mock_pipeline(self) -> None:
        class FakeDiscordAdapter:
            async def start(self):
                pass

            async def stop(self):
                pass

            async def fetch_messages(self, channel_id: str, after_message_id: int | None = None):
                return [
                    {"id": "10", "content": "first", "author": {"username": "a"}},
                    {"id": "11", "content": "second", "author": {"username": "a"}},
                ]

        watchlists = DiscordWatchlists(
            sources=(
                DiscordSourceConfig(
                    key="base",
                    label="Base Discord",
                    enabled=True,
                    project="Base",
                    ecosystem="Base",
                    priority=10,
                    channels=(DiscordChannelConfig(channel_id="999", name="announcements", type="announcement"),),
                ),
                DiscordSourceConfig(
                    key="zora",
                    label="Zora Discord",
                    enabled=False,
                    project="Zora",
                    ecosystem="Base",
                    priority=8,
                    channels=(DiscordChannelConfig(channel_id="222", name="updates", type="announcement"),),
                ),
            )
        )
        pipeline = SimpleNamespace(messages=[])

        async def process(message):
            pipeline.messages.append(message)

        pipeline.process = process
        collector = DiscordCollector(pipeline=pipeline, watchlists=watchlists, adapter=FakeDiscordAdapter())

        first_stats = await collector.poll_once()
        second_stats = await collector.poll_once()

        self.assertEqual(first_stats["channel_count"], 1)
        self.assertEqual(first_stats["new_message_count"], 2)
        self.assertEqual(second_stats["new_message_count"], 0)
        self.assertEqual([message.source_message_id for message in pipeline.messages], ["10", "11"])
        self.assertEqual({message.source_chat_id for message in pipeline.messages}, {"999"})


class XFeedCollectorTests(unittest.IsolatedAsyncioTestCase):
    def test_mock_rss_feed_parses_messages(self) -> None:
        feed = parse_x_feed(
            """
            <rss><channel>
              <title>Base RSS</title>
              <item>
                <title>Base airdrop update</title>
                <description>Claim window opened</description>
                <link>https://example.com/base/1</link>
                <guid>tweet-1</guid>
                <pubDate>Tue, 02 Jun 2026 01:00:00 GMT</pubDate>
              </item>
            </channel></rss>
            """,
            "https://example.com/base/rss",
        )

        self.assertEqual(feed.title, "Base RSS")
        self.assertEqual(len(feed.entries), 1)
        self.assertEqual(feed.entries[0].source_message_id, "tweet-1")
        self.assertIn("Claim window opened", feed.entries[0].raw_text)

    def test_x_feed_guid_dedup_skips_repeated_entries(self) -> None:
        collector = XFeedCollector(pipeline=SimpleNamespace())
        feed = XFeed(
            url="https://example.com/rss",
            title="Example",
            entries=[
                XFeedEntry(title="one", content="", link="https://example.com/1", published=None, guid="same-guid"),
                XFeedEntry(title="two", content="", link="https://example.com/2", published=None, guid="same-guid"),
            ],
        )

        messages = collector.source_messages_from_feed(feed)

        self.assertEqual(len(messages), 1)
        self.assertEqual(messages[0].source_message_id, "same-guid")

    def test_x_feed_link_hash_fallback_is_stable(self) -> None:
        entry = XFeedEntry(title="one", content="", link="https://example.com/1", published=None, guid=None)
        same = XFeedEntry(title="one again", content="", link="https://example.com/1", published=None, guid=None)

        self.assertTrue(entry.source_message_id.startswith("link:"))
        self.assertEqual(entry.source_message_id, same.source_message_id)

    async def test_single_feed_failure_does_not_affect_other_feeds(self) -> None:
        class FakeCollector(XFeedCollector):
            async def fetch_feed(self, feed_url: str) -> XFeed:
                if "bad" in feed_url:
                    raise RuntimeError("feed failed")
                return XFeed(
                    url=feed_url,
                    title="Good",
                    entries=[XFeedEntry(title="good", content="", link="https://example.com/good", published=None, guid="good-1")],
                )

        pipeline = SimpleNamespace(messages=[])

        async def process(message):
            pipeline.messages.append(message)

        pipeline.process = process
        collector = FakeCollector(pipeline=pipeline)
        collector.watchlists = Watchlists(categories=(), accounts_by_normalized={}, matches_by_normalized={})

        with patch("app.sources.x_feed.collector.settings.x_feed_urls", ["https://bad.example/rss", "https://good.example/rss"]):
            stats = await collector.poll_once()

        self.assertEqual(stats["success_feed_count"], 1)
        self.assertEqual(stats["failed_feed_count"], 1)
        self.assertEqual(stats["new_message_count"], 1)
        self.assertEqual(len(pipeline.messages), 1)

    async def test_x_message_enters_pipeline_mock(self) -> None:
        pipeline = SimpleNamespace(messages=[])

        async def process(message):
            pipeline.messages.append(message)

        pipeline.process = process
        collector = XFeedCollector(pipeline=pipeline)
        feed = XFeed(
            url="https://example.com/base/rss",
            title="Base RSS",
            entries=[XFeedEntry(title="Base airdrop", content="Claim now", link="https://example.com/base/1", published=None, guid="tweet-1")],
        )

        for message in collector.source_messages_from_feed(feed):
            await pipeline.process(message)

        self.assertEqual(len(pipeline.messages), 1)
        self.assertEqual(pipeline.messages[0].source, "x")
        self.assertEqual(pipeline.messages[0].source_chat_id, "https://example.com/base/rss")
        self.assertEqual(pipeline.messages[0].source_chat_title, "Base RSS")
        self.assertEqual(pipeline.messages[0].source_message_id, "tweet-1")
        self.assertIn("https://example.com/base/1", pipeline.messages[0].raw_text)

    def test_x_message_carries_watchlist_metadata(self) -> None:
        collector = XFeedCollector(pipeline=SimpleNamespace())
        collector.watchlists = Watchlists(
            categories=(
                WatchlistCategory(
                    key="base_core",
                    label="Base核心生态",
                    priority=10,
                    description="",
                    accounts=("base",),
                ),
            ),
            accounts_by_normalized={"base": "base"},
            matches_by_normalized={
                "base": WatchlistMatch(category="base_core", label="Base核心生态", priority=10),
            },
        )
        collector.feed_urls = ["http://rsshub:1200/twitter/user/base"]
        collector._account_by_feed_url = {"http://rsshub:1200/twitter/user/base": "base"}
        feed = XFeed(
            url="http://rsshub:1200/twitter/user/base",
            title="Twitter @Base",
            entries=[XFeedEntry(title="Base update", content="", link="https://x.com/base/status/1", published=None, guid="tweet-1")],
        )

        message = collector.source_messages_from_feed(feed)[0]

        self.assertEqual(message.watchlist_category, "base_core")
        self.assertEqual(message.watchlist_label, "Base核心生态")
        self.assertEqual(message.watchlist_priority, 10)


class TopMessagesApiTests(unittest.TestCase):
    def test_extract_original_url_returns_first_url_without_trailing_punctuation(self) -> None:
        url = extract_original_url("原文链接 https://example.com/news/1。 备用 https://example.com/2")

        self.assertEqual(url, "https://example.com/news/1")

    def test_format_top_message_returns_expected_fields(self) -> None:
        message = SimpleNamespace(
            id=1,
            source_platform="telegram_public",
            source_chat_title="@alpha",
            source_chat_id="alpha",
            source_message_id=42,
            raw_text="hello https://example.com/a",
            summary_zh="summary",
            category="Alpha机会",
            score=91.2,
            signal_level="S",
            score_breakdown=lambda: {
                "ai_score": 90,
                "keyword_bonus": 1.2,
                "source_bonus": 0,
                "risk_penalty": 0,
                "final_score": 91.2,
            },
            push_status="sent",
            possible_duplicate=True,
            duplicate_of_message_id=99,
            similarity_score=0.95,
            event_id=7,
            event_similarity=1.0,
            event_match_reason="new_event",
            feedback="good",
            feedback_at=None,
            source_profile=lambda: {
                "label": "Base",
                "role": "official",
                "ecosystem": "Base",
                "importance": 10,
                "specialty": ["ecosystem"],
            },
            created_at=None,
        )

        payload = format_top_message(message)

        self.assertEqual(
            set(payload),
            {
                "id",
                "source_platform",
                "source_channel",
                "source_message_id",
                "text",
                "summary",
                "category",
                "score",
                "signal_level",
                "score_breakdown",
                "push_status",
                "possible_duplicate",
                "duplicate_of_message_id",
                "similarity_score",
                "event_id",
                "event_similarity",
                "event_match_reason",
                "ai_decision",
                "ai_confidence",
                "ai_reason",
                "user_value_summary",
                "action_suggestion",
                "urgency",
                "relevance",
                "actionability",
                "risk_level",
                "feedback",
                "feedback_at",
                "watchlist_category",
                "watchlist_label",
                "watchlist_priority",
                "source_profile",
                "created_at",
                "original_url",
            },
        )
        self.assertEqual(payload["source_channel"], "@alpha")
        self.assertEqual(payload["source_platform"], "telegram_public")
        self.assertEqual(payload["feedback"], "good")
        self.assertEqual(payload["signal_level"], "S")
        self.assertEqual(payload["push_status"], "sent")
        self.assertTrue(payload["possible_duplicate"])
        self.assertEqual(payload["duplicate_of_message_id"], 99)
        self.assertEqual(payload["similarity_score"], 0.95)
        self.assertIsNone(payload["watchlist_category"])
        self.assertIsNone(payload["watchlist_label"])
        self.assertIsNone(payload["watchlist_priority"])
        self.assertEqual(payload["source_profile"]["role"], "official")
        self.assertEqual(payload["original_url"], "https://example.com/a")

    def test_feishu_interactive_card_contains_feedback_buttons(self) -> None:
        card = build_feedback_card(
            {
                "message_id": 123,
                "event_id": 456,
                "source_chat_title": "@alpha",
                "source_platform": "discord",
                "source_project": "kui4",
                "source_ecosystem": "Unknown",
                "category": "安全风险",
                "score": 95,
                "summary_zh": "ZEC漏洞事件",
                "reason": "高风险",
            }
        )

        actions = card["elements"][-1]["actions"]
        values = [action["value"] for action in actions]

        self.assertEqual(card["header"]["title"]["content"], "Web3 Alpha 告警 【🚨 Push｜重点关注】")
        self.assertEqual(card["elements"][0]["content"], "**【🚨 Push｜重点关注】**")
        self.assertEqual(card["elements"][1]["content"], "**来源**：discord / kui4 / Unknown")
        self.assertEqual([value["action"] for value in values], ["good", "bad", "ignore"])
        self.assertEqual(values[0]["message_id"], 123)
        self.assertEqual(values[0]["event_id"], 456)

    def test_feishu_interactive_card_shows_watch_label(self) -> None:
        card = build_feedback_card(
            {
                "message_id": 123,
                "event_id": 456,
                "source_chat_title": "@alpha",
                "ai_decision": "watch",
                "category": "融资合作",
                "score": 65,
                "summary_zh": "项目融资，值得跟踪",
                "reason": "有后续 TGE 跟踪价值",
                "action_suggestion": "none",
            }
        )

        self.assertEqual(card["header"]["title"]["content"], "Web3 Alpha 告警 【👀 Watch｜观察】")
        self.assertEqual(card["elements"][0]["content"], "**【👀 Watch｜观察】**")
        contents = [element["content"] for element in card["elements"] if "content" in element]
        self.assertIn("**决策**：Watch", contents)
        self.assertIn("**建议动作**：建议跟踪", contents)

    def test_feishu_app_bot_message_wraps_card_as_content_string(self) -> None:
        card = build_feedback_card(
            {
                "message_id": 123,
                "event_id": 456,
                "source_chat_title": "@alpha",
                "category": "安全风险",
                "score": 95,
                "summary_zh": "ZEC漏洞事件",
                "reason": "高风险",
            }
        )

        message = build_app_bot_message("oc_test", card)
        content = json.loads(message["content"])

        self.assertEqual(message["receive_id"], "oc_test")
        self.assertEqual(message["msg_type"], "interactive")
        self.assertEqual(content["header"]["title"]["content"], "Web3 Alpha 告警 【🚨 Push｜重点关注】")
        self.assertEqual(content["elements"][-1]["actions"][0]["value"]["action"], "good")

    def test_extract_feishu_action_value_supports_event_callback_shape(self) -> None:
        payload = {
            "event": {
                "action": {
                    "value": {
                        "action": "bad",
                        "message_id": 123,
                        "event_id": 456,
                    }
                }
            }
        }

        self.assertEqual(_extract_feishu_action_value(payload)["action"], "bad")


class FakeFeishuRequest:
    def __init__(self, payload: dict) -> None:
        self.payload = payload

    async def json(self) -> dict:
        return self.payload


class FakeFeedbackDb:
    def __init__(self, rowcounts: list[int] | None = None) -> None:
        self.executed = []
        self.committed = False
        self.rowcounts = rowcounts or []

    async def execute(self, query) -> SimpleNamespace:
        self.executed.append(query)
        rowcount = self.rowcounts.pop(0) if self.rowcounts else 0
        return SimpleNamespace(rowcount=rowcount)

    async def commit(self) -> None:
        self.committed = True


class FeishuFeedbackRouteTests(unittest.IsolatedAsyncioTestCase):
    async def test_feishu_feedback_returns_saved_toast_for_existing_event(self) -> None:
        db = FakeFeedbackDb(rowcounts=[1, 1, 1, 1])
        response = await feishu_feedback(
            FakeFeishuRequest(
                {
                    "event": {
                        "action": {
                            "value": {
                                "action": "good",
                                "message_id": 123,
                                "event_id": 456,
                            }
                        }
                    }
                }
            ),
            db,
        )

        self.assertTrue(db.committed)
        self.assertEqual(response["toast"]["type"], "success")
        self.assertEqual(
            response["toast"]["content"],
            "feedback saved: target_type=event, target_id=456, feedback=good",
        )

    def test_feedback_dedup_key_is_stable_for_same_action(self) -> None:
        payload = {
            "event": {
                "event_id": "evt_1",
                "operator": {"open_id": "user_1"},
            }
        }
        now = datetime(2026, 6, 8, 2, 0, tzinfo=timezone.utc)

        first = _feedback_dedup_key(payload, target_type="event", target_id=1, feedback="good", now=now)
        second = _feedback_dedup_key(payload, target_type="event", target_id=1, feedback="good", now=now)

        self.assertEqual(first, second)

    def test_feedback_dedup_key_allows_different_feedback_and_targets(self) -> None:
        payload = {"event": {"operator": {"open_id": "user_1"}}}
        now = datetime(2026, 6, 8, 2, 0, tzinfo=timezone.utc)

        bad = _feedback_dedup_key(payload, target_type="event", target_id=1, feedback="bad", now=now)
        good = _feedback_dedup_key(payload, target_type="event", target_id=1, feedback="good", now=now)
        other_target = _feedback_dedup_key(payload, target_type="event", target_id=2, feedback="bad", now=now)

        self.assertNotEqual(bad, good)
        self.assertNotEqual(bad, other_target)

    def test_feedback_dedup_key_uses_five_minute_bucket_without_action_id(self) -> None:
        payload = {"event": {"operator": {"open_id": "user_1"}}}
        first = _feedback_dedup_key(
            payload,
            target_type="record",
            target_id=1,
            feedback="ignore",
            now=datetime(2026, 6, 8, 2, 0, tzinfo=timezone.utc),
        )
        second = _feedback_dedup_key(
            payload,
            target_type="record",
            target_id=1,
            feedback="ignore",
            now=datetime(2026, 6, 8, 2, 4, tzinfo=timezone.utc),
        )
        third = _feedback_dedup_key(
            payload,
            target_type="record",
            target_id=1,
            feedback="ignore",
            now=datetime(2026, 6, 8, 2, 6, tzinfo=timezone.utc),
        )

        self.assertEqual(first, second)
        self.assertNotEqual(first, third)

    async def test_feishu_feedback_ignores_invalid_target_id(self) -> None:
        db = FakeFeedbackDb(rowcounts=[0, 0])
        response = await feishu_feedback(
            FakeFeishuRequest(
                {
                    "event": {
                        "action": {
                            "value": {
                                "action": "ignore",
                                "message_id": 0,
                                "event_id": 0,
                            }
                        }
                    }
                }
            ),
            db,
        )

        self.assertTrue(db.committed)
        self.assertEqual(response["toast"]["type"], "warning")
        self.assertEqual(response["toast"]["content"], "feedback ignored: invalid target_id")

    def test_feedback_stats_item_formatters(self) -> None:
        now = datetime(2026, 6, 5, tzinfo=timezone.utc)
        message_item = _format_feedback_message_item(
            SimpleNamespace(
                id=123,
                feedback="bad",
                feedback_at=now,
                summary_zh="消息总结",
            )
        )
        event_item = _format_feedback_event_item(
            SimpleNamespace(
                id=456,
                feedback="good",
                feedback_at=now,
                event_title="事件标题",
                latest_summary="事件总结",
                event_summary="旧总结",
            )
        )

        self.assertEqual(message_item["target_type"], "message")
        self.assertEqual(message_item["target_id"], 123)
        self.assertEqual(message_item["summary"], "消息总结")
        self.assertEqual(event_item["target_type"], "event")
        self.assertEqual(event_item["target_id"], 456)
        self.assertEqual(event_item["title"], "事件标题")


class CalibrationReportTests(unittest.TestCase):
    def test_calibration_report_ranks_feedback_dimensions(self) -> None:
        report = build_calibration_report(
            [
                self._message(
                    feedback="good",
                    event_id=1,
                    event_title="ZEC漏洞事件",
                    signal_type="exploit",
                    signal_label="漏洞攻击",
                    keywords=["exploit", "hack"],
                    watchlist_category="trading_signal",
                    watchlist_label="交易信号",
                    score=96,
                ),
                self._message(
                    feedback="good",
                    event_id=1,
                    event_title="ZEC漏洞事件",
                    signal_type="exploit",
                    signal_label="漏洞攻击",
                    keywords=["exploit"],
                    watchlist_category="trading_signal",
                    watchlist_label="交易信号",
                    score=92,
                ),
                self._message(
                    feedback="bad",
                    event_id=2,
                    event_title="普通宏观新闻",
                    signal_type="macro",
                    signal_label="宏观资讯",
                    keywords=["funding"],
                    watchlist_category=None,
                    watchlist_label=None,
                    score=82,
                    ai_decision="watch",
                ),
                self._message(
                    feedback="ignore",
                    event_id=3,
                    event_title="低价值消息",
                    signal_type="macro",
                    signal_label="宏观资讯",
                    keywords=["funding"],
                    watchlist_category=None,
                    watchlist_label=None,
                    score=78,
                ),
            ],
            days=7,
        )

        self.assertEqual(report["feedback_counts"], {"good": 2, "bad": 1, "ignore": 1})
        self.assertEqual(report["decision_rankings"][0]["key"], "push")
        self.assertIn("watch", {item["key"] for item in report["decision_rankings"]})
        self.assertEqual(report["signal_type_rankings"][0]["key"], "exploit")
        self.assertEqual(report["keyword_rankings"][0]["key"], "exploit")
        self.assertEqual(report["watchlist_rankings"][0]["key"], "trading_signal")
        self.assertEqual(report["top_good_events"][0]["event_title"], "ZEC漏洞事件")
        self.assertEqual(report["top_bad_events"][0]["event_title"], "普通宏观新闻")
        self.assertEqual(report["recommended_keyword_bonus_adjustments"][0]["keyword"], "exploit")
        self.assertEqual(report["recommended_signal_bonus_adjustments"][0]["signal_type"], "exploit")

    def test_calibration_report_recommends_decrease_for_bad_or_ignored_items(self) -> None:
        report = build_calibration_report(
            [
                self._message(feedback="bad", signal_type="macro", keywords=["funding"]),
                self._message(feedback="ignore", signal_type="macro", keywords=["funding"]),
            ],
            days=7,
        )

        keyword_recommendation = report["recommended_keyword_bonus_adjustments"][0]
        signal_recommendation = report["recommended_signal_bonus_adjustments"][0]

        self.assertEqual(keyword_recommendation["keyword"], "funding")
        self.assertLess(keyword_recommendation["suggested_delta"], 0)
        self.assertEqual(signal_recommendation["signal_type"], "macro")
        self.assertLess(signal_recommendation["suggested_delta"], 0)

    def _message(
        self,
        feedback: str,
        event_id: int = 1,
        event_title: str = "测试事件",
        signal_type: str = "exploit",
        signal_label: str = "漏洞攻击",
        keywords: list[str] | None = None,
        watchlist_category: str | None = "base_core",
        watchlist_label: str | None = "Base核心生态",
        score: float = 90,
        ai_decision: str = "push",
    ) -> dict:
        return {
            "id": event_id,
            "feedback": feedback,
            "event_id": event_id,
            "event_title": event_title,
            "summary_zh": event_title,
            "score": score,
            "ai_decision": ai_decision,
            "score_breakdown": {
                "signal_type": signal_type,
                "signal_label": signal_label,
                "matched_keywords": keywords or ["exploit"],
            },
            "watchlist_category": watchlist_category,
            "watchlist_label": watchlist_label,
        }


class MessageFormatTests(unittest.TestCase):
    def test_format_top_message_returns_rate_limited_status(self) -> None:
        message = SimpleNamespace(
            id=2,
            source_chat_title="@alpha",
            source_chat_id="alpha",
            source_message_id=43,
            raw_text="hello",
            summary_zh="summary",
            category="Alpha机会",
            score=80,
            signal_level="A",
            score_breakdown=lambda: {
                "ai_score": 80,
                "keyword_bonus": 0,
                "source_bonus": 0,
                "risk_penalty": 0,
                "final_score": 80,
            },
            push_status="skipped_rate_limited",
            possible_duplicate=False,
            duplicate_of_message_id=None,
            similarity_score=None,
            created_at=None,
        )

        payload = format_top_message(message)

        self.assertEqual(payload["push_status"], "skipped_rate_limited")

    def test_format_stats_payload_includes_signal_level_counts(self) -> None:
        payload = format_stats_payload(
            s_count=1,
            a_count=2,
            b_count=3,
            c_count=4,
            rate_limited_count=5,
            skipped_duplicate_count=6,
            duplicate_skipped_count=6,
            push_decision_count=7,
            watch_decision_count=8,
            ignore_decision_count=9,
            push_sent_count=4,
            watch_sent_count=5,
            ignore_skipped_count=6,
            known_source_count=10,
            unknown_source_count=2,
            role_distribution={"official": 7, "unknown": 2, "founder": 3},
            watchlist_counts=[{"category": "base_core", "label": "Base核心生态", "priority": 10, "count": 3}],
            watchlist_avg_scores=[{"category": "base_core", "label": "Base核心生态", "priority": 10, "average_score": 66.6}],
            telegram_channel_count=3,
            telegram_group_count=1,
            telegram_messages_24h=12,
            telegram_events_24h=4,
        )

        self.assertEqual(payload["s_count"], 1)
        self.assertEqual(payload["a_count"], 2)
        self.assertEqual(payload["b_count"], 3)
        self.assertEqual(payload["c_count"], 4)
        self.assertEqual(payload["rate_limited_count"], 5)
        self.assertEqual(payload["skipped_duplicate_count"], 6)
        self.assertEqual(payload["duplicate_skipped_count"], 6)
        self.assertEqual(payload["push_decision_count"], 7)
        self.assertEqual(payload["watch_decision_count"], 8)
        self.assertEqual(payload["ignore_decision_count"], 9)
        self.assertEqual(payload["push_sent_count"], 4)
        self.assertEqual(payload["watch_sent_count"], 5)
        self.assertEqual(payload["ignore_skipped_count"], 6)
        self.assertEqual(payload["known_source_count"], 10)
        self.assertEqual(payload["unknown_source_count"], 2)
        self.assertEqual(payload["role_distribution"]["official"], 7)
        self.assertEqual(payload["watchlist_counts"][0]["category"], "base_core")
        self.assertEqual(payload["watchlist_avg_scores"][0]["average_score"], 66.6)
        self.assertEqual(payload["telegram_channel_count"], 3)
        self.assertEqual(payload["telegram_group_count"], 1)
        self.assertEqual(payload["telegram_messages_24h"], 12)
        self.assertEqual(payload["telegram_events_24h"], 4)

    def test_source_profile_stats_counts_roles(self) -> None:
        stats = _source_profile_stats(
            [
                json.dumps({"source_context": {"source_profile": {"role": "official"}}}),
                json.dumps({"score_breakdown": {"source_profile": {"role": "founder"}}}),
                json.dumps({"source_context": {"source_profile": {"role": "unknown"}}}),
                "{}",
            ]
        )

        self.assertEqual(stats["known_source_count"], 2)
        self.assertEqual(stats["unknown_source_count"], 2)
        self.assertEqual(stats["role_distribution"]["official"], 1)
        self.assertEqual(stats["role_distribution"]["founder"], 1)
        self.assertEqual(stats["role_distribution"]["unknown"], 2)


class SignalLevelTests(unittest.TestCase):
    def test_signal_level_for_score(self) -> None:
        self.assertEqual(signal_level_for_score(95), "S")
        self.assertEqual(signal_level_for_score(80), "A")
        self.assertEqual(signal_level_for_score(65), "B")
        self.assertEqual(signal_level_for_score(40), "C")


class DuplicateDetectionTests(unittest.TestCase):
    def test_text_similarity_detects_near_identical_messages(self) -> None:
        left = "Binance listing mainnet launch for Alpha Protocol. https://example.com/a"
        right = "binance listing mainnet launch for alpha protocol"

        self.assertGreater(text_similarity(left, right), 0.90)

    def test_text_similarity_keeps_different_messages_below_threshold(self) -> None:
        left = "Binance listing mainnet launch for Alpha Protocol"
        right = "Security incident caused withdrawals to be paused"

        self.assertLess(text_similarity(left, right), 0.90)

    def test_find_possible_duplicate_returns_best_match(self) -> None:
        match = find_possible_duplicate(
            "Binance listing mainnet launch for Alpha Protocol",
            [
                SimpleNamespace(id=10, cleaned_text="Security incident caused withdrawals to be paused"),
                SimpleNamespace(id=11, cleaned_text="binance listing mainnet launch for alpha protocol"),
            ],
        )

        self.assertTrue(match.possible_duplicate)
        self.assertEqual(match.duplicate_of_message_id, 11)
        self.assertGreater(match.similarity_score, 0.90)

    def test_event_similarity_detects_same_blackrock_etf_outflow_event(self) -> None:
        score = duplicate_similarity(
            "贝莱德 ETF 过去 10 日净流出 30,119 枚 BTC 和 161,829 枚 ETH，资金流出影响市场情绪。",
            "贝莱德ETF近10日大额净流出BTC与ETH，机构配置变化对加密市场情绪和价格有影响。",
            left_summary="贝莱德ETF近10日大额流出30,119枚BTC和161,829枚ETH",
            right_summary="贝莱德ETF近10日大额净流出BTC与ETH",
            same_category=True,
        )

        self.assertGreaterEqual(score, 0.82)

    def test_event_similarity_ignores_generic_btc_eth_overlap_without_anchor(self) -> None:
        score = duplicate_similarity(
            "Bitget PoolX 上线 SLX，锁仓 BTC/ETH 可分 1,000,000 枚 SLX",
            "Coinbase 为印度用户新增 ETH/INR 和 SOL/INR 交易对支持",
            same_category=True,
        )

        self.assertLess(score, 0.82)

    def test_duplicate_similarity_ignores_similar_digest_templates_without_same_event(self) -> None:
        left = "📡 20点情报 新增 7 重点信息 Coinbase为印度用户新增ETH交易对 Grayscale ETF进展 BTC异动"
        right = "📡 21点情报 新增 7 重点信息 Drift遭攻击后重启 Hyperliquid多头浮亏 BTC异动"

        self.assertLess(duplicate_similarity(left, right, same_category=True), 0.82)

    def test_duplicate_similarity_prefers_summary_over_multi_topic_raw(self) -> None:
        left_raw = "📡 情报 Coinbase为印度用户新增ETH交易对 Grayscale推ETF Kalshi上线合约"
        right_raw = "📡 情报 Coinbase为印度用户新增ETH交易对 BTC大额异动 ETH跌破1800"

        score = duplicate_similarity(
            left_raw,
            right_raw,
            left_summary="Grayscale推Hyperliquid ETF，Kalshi上线比特币永续",
            right_summary="Coinbase为印度用户新增ETH和SOL卢比交易对",
            same_category=True,
        )

        self.assertLess(score, 0.82)

    def test_duplicate_similarity_does_not_match_eth_inside_ethena(self) -> None:
        score = duplicate_similarity(
            "Coinbase与Ethena合作推出链上储蓄产品，市场ETF流出。",
            "Coinbase为印度用户新增ETH和SOL卢比交易对",
            left_summary="Coinbase与Ethena合作，市场ETF流出",
            right_summary="Coinbase为印度用户新增ETH和SOL卢比交易对",
            same_category=True,
        )

        self.assertLess(score, 0.82)

    def test_duplicate_similarity_detects_same_helion_funding_event(self) -> None:
        score = duplicate_similarity(
            "Helion完成4.65亿美元融资，估值升至155亿美元，推进核聚变商业化。",
            "Helion获4.65亿美元融资，估值升至155亿美元，推进核聚变发电商业化。",
            left_summary="Helion完成4.65亿美元融资，估值升至155亿美元，推进核聚变商业化。",
            right_summary="Helion获4.65亿美元融资，估值升至155亿美元，推进核聚变发电商业化。",
            same_category=True,
        )

        self.assertGreaterEqual(score, 0.82)

    def test_duplicate_similarity_detects_same_zec_security_event_without_numbers(self) -> None:
        score = duplicate_similarity(
            "ZEC暴露无限增发漏洞引发暴跌，Arthur Hayes清仓，市场担忧隐私币信任基础受损。",
            "Cypherpunk回应ZEC漏洞风波，称应以形式化验证提升安全，ZEC可能存在无限增发漏洞并引发暴跌。",
            left_summary="ZEC曝无限增发漏洞引发暴跌，Arthur Hayes清仓，市场担忧隐私币信任基础受损。",
            right_summary="Cypherpunk回应ZEC漏洞风波，强调形式化验证可提升安全。",
            same_category=True,
        )

        self.assertGreaterEqual(score, 0.82)

    def test_duplicate_similarity_does_not_match_generic_security_events_without_entity_overlap(self) -> None:
        score = duplicate_similarity(
            "ZEC曝无限增发漏洞引发暴跌，Arthur Hayes清仓。",
            "某DeFi协议遭黑客攻击，团队暂停提现并排查安全漏洞。",
            left_summary="ZEC曝无限增发漏洞引发暴跌。",
            right_summary="DeFi协议遭黑客攻击并暂停提现。",
            same_category=True,
        )

        self.assertLess(score, 0.82)

    def test_find_possible_duplicate_uses_summary_and_category_for_event_match(self) -> None:
        match = find_possible_duplicate(
            "贝莱德 ETF 过去 10 日净流出 30,119 枚 BTC 和 161,829 枚 ETH。",
            [
                SimpleNamespace(
                    id=20,
                    cleaned_text="美国初请失业金人数高于预期，劳动力市场略显走弱",
                    summary_zh="美国初请失业金人数升至22.5万",
                    category="宏观市场",
                ),
                SimpleNamespace(
                    id=21,
                    cleaned_text="贝莱德ETF连续大额资金流出，可能反映机构对BTC和ETH短期配置变化",
                    summary_zh="贝莱德ETF近10日大额净流出BTC与ETH",
                    category="宏观市场",
                ),
            ],
            summary="贝莱德ETF近10日大额流出30,119枚BTC和161,829枚ETH",
            category="宏观市场",
        )

        self.assertTrue(match.possible_duplicate)
        self.assertEqual(match.duplicate_of_message_id, 21)

    def test_duplicate_backfill_dry_run_does_not_write_marks(self) -> None:
        messages = [
            SimpleNamespace(id=1, cleaned_text="Binance listing mainnet launch", summary_zh="first", possible_duplicate=False),
            SimpleNamespace(id=2, cleaned_text="binance listing mainnet launch", summary_zh="second", possible_duplicate=False),
        ]

        result = backfill_possible_duplicates(messages, threshold=0.82, dry_run=True)
        changed_count = apply_backfill_marks(messages, result)

        self.assertEqual(result.matched_count, 1)
        self.assertEqual(changed_count, 0)
        self.assertFalse(messages[1].possible_duplicate)

    def test_duplicate_backfill_non_dry_run_writes_possible_duplicate(self) -> None:
        messages = [
            SimpleNamespace(
                id=1,
                cleaned_text="Binance listing mainnet launch",
                summary_zh="first",
                possible_duplicate=False,
                duplicate_of_message_id=None,
                similarity_score=None,
            ),
            SimpleNamespace(
                id=2,
                cleaned_text="binance listing mainnet launch",
                summary_zh="second",
                possible_duplicate=False,
                duplicate_of_message_id=None,
                similarity_score=None,
            ),
        ]

        result = backfill_possible_duplicates(messages, threshold=0.82, dry_run=False)
        changed_count = apply_backfill_marks(messages, result)

        self.assertEqual(changed_count, 1)
        self.assertTrue(messages[1].possible_duplicate)
        self.assertEqual(messages[1].duplicate_of_message_id, 1)
        self.assertGreaterEqual(messages[1].similarity_score, 0.82)

    def test_duplicate_backfill_marks_similar_but_not_different_messages(self) -> None:
        messages = [
            SimpleNamespace(id=1, cleaned_text="Binance listing mainnet launch", summary_zh="first"),
            SimpleNamespace(id=2, cleaned_text="Security incident caused withdrawals to be paused", summary_zh="different"),
            SimpleNamespace(id=3, cleaned_text="binance listing mainnet launch", summary_zh="similar"),
        ]

        result = backfill_possible_duplicates(messages, threshold=0.82, dry_run=True)

        self.assertEqual(result.matched_count, 1)
        self.assertEqual(result.matches[0].message_id, 3)
        self.assertEqual(result.matches[0].duplicate_of_message_id, 1)

    def test_format_duplicate_backfill_payload_returns_first_twenty_samples(self) -> None:
        messages = [
            SimpleNamespace(id=1, cleaned_text="Binance listing mainnet launch", summary_zh="first"),
            SimpleNamespace(id=2, cleaned_text="binance listing mainnet launch", summary_zh="second"),
        ]
        result = backfill_possible_duplicates(messages, threshold=0.82, dry_run=True)
        payload = format_duplicate_backfill_payload(result, hours=24)

        self.assertEqual(payload["hours"], 24)
        self.assertEqual(payload["threshold"], 0.82)
        self.assertEqual(payload["matched_count"], 1)
        self.assertEqual(payload["samples"][0]["message_id"], 2)
        self.assertEqual(payload["samples"][0]["duplicate_summary"], "first")


def _event_message(message_id: int, channel: str, event_title: str, summary: str) -> SimpleNamespace:
    return SimpleNamespace(
        id=message_id,
        source_chat_id=channel,
        source_chat_title=f"@{channel}",
        cleaned_text=summary,
        raw_text=summary,
        summary_zh=summary,
        analysis_json=json.dumps({"event_title": event_title, "summary_zh": summary}, ensure_ascii=False),
        created_at=datetime(2026, 6, 5, 8, message_id, tzinfo=timezone.utc),
        push_status="sent",
        score=95,
        signal_level="S",
    )


class EventClusterTests(unittest.TestCase):
    def test_piggybank_lab_vault_messages_match_existing_event(self) -> None:
        existing = SimpleNamespace(
            id=685,
            event_title="Piggybank投资LAB做空亏损事件",
            event_summary="Piggybank称因投资LAB后做空气亏，短期或导致各vault净值下降。",
            last_seen_at=datetime(2026, 6, 7, 8, 0, tzinfo=timezone.utc),
        )

        match = best_event_match(
            "Piggybank LAB做空亏损事件",
            "Piggybank因投资LAB后做空气亏，准备平仓并可能导致各vault净值下滑。",
            "Piggybank 因投资 LAB 后做空气亏，准备平仓并可能导致各 vault 净值下滑。",
            [existing],
        )

        self.assertIsNotNone(match)
        self.assertEqual(match.event.id, 685)
        self.assertGreaterEqual(match.similarity, 0.68)
        self.assertIn(match.reason, {"strong_entity_token_time_match", "multi_factor_score"})

    def test_event_feature_extraction_keeps_projects_tokens_numbers_and_phrases(self) -> None:
        features = extract_event_features("Piggybank 因投资 LAB 后做空气亏，准备平仓，vault 净值或下滑 50%。")

        self.assertIn("piggybank", features.entities)
        self.assertIn("lab", features.tokens)
        self.assertIn("short_loss", features.key_phrases)
        self.assertIn("close_position", features.key_phrases)
        self.assertIn("vault_nav", features.key_phrases)
        self.assertIn("50%", features.numbers)

    def test_zec_messages_match_existing_event(self) -> None:
        existing = SimpleNamespace(
            id=1,
            event_title="ZEC无限增发漏洞事件",
            event_summary="ZEC曝无限增发漏洞引发暴跌，Arthur Hayes清仓。",
        )

        match = best_event_match(
            "ZEC漏洞风波事件",
            "Cypherpunk回应ZEC漏洞风波，强调形式化验证可提升安全。",
            "ZEC可能存在无限增发漏洞并引发暴跌。",
            [existing],
        )

        self.assertIsNotNone(match)
        self.assertEqual(match.event.id, 1)
        self.assertGreaterEqual(match.similarity, 0.68)

    def test_rank_event_candidates_returns_debug_details(self) -> None:
        existing = SimpleNamespace(
            id=685,
            event_title="Piggybank投资LAB做空亏损事件",
            event_summary="Piggybank称因投资LAB后做空气亏，短期或导致各vault净值下降。",
            last_seen_at=datetime(2026, 6, 7, 8, 0, tzinfo=timezone.utc),
        )

        details = rank_event_candidates(
            "Piggybank LAB做空亏损事件",
            "Piggybank因投资LAB后做空气亏，准备平仓并可能导致各vault净值下滑。",
            "Piggybank 因投资 LAB 后做空气亏，准备平仓并可能导致各 vault 净值下滑。",
            [existing],
            message_created_at=datetime(2026, 6, 7, 9, 0, tzinfo=timezone.utc),
            limit=10,
        )
        payload = details[0].as_dict()

        self.assertEqual(payload["event_id"], 685)
        self.assertTrue(payload["would_match"])
        self.assertIn("piggybank", payload["entity_overlap"])
        self.assertIn("vault_nav", payload["key_phrase_overlap"])
        self.assertIn("final_match_score", payload)

    def test_event_backfill_dry_run_does_not_write_marks(self) -> None:
        messages = [
            _event_message(1, "Blockbeats", "ZEC无限增发漏洞事件", "ZEC曝无限增发漏洞引发暴跌。"),
            _event_message(2, "TechFlow", "ZEC漏洞风波事件", "TechFlow报道ZEC可能存在无限增发漏洞。"),
        ]

        result = plan_event_backfill(messages, dry_run=True)
        written_events = apply_event_backfill_marks(result)
        payload = format_event_backfill_payload(result, hours=24)

        self.assertEqual(written_events, [])
        self.assertFalse(hasattr(messages[0], "event_id"))
        self.assertEqual(result.created_event_count, 1)
        self.assertEqual(payload["samples"][0]["message_count"], 2)

    def test_event_backfill_writes_events_and_message_event_id_without_touching_push_fields(self) -> None:
        messages = [
            _event_message(1, "Blockbeats", "ZEC无限增发漏洞事件", "ZEC曝无限增发漏洞引发暴跌。"),
            _event_message(2, "Odaily", "ZEC无限增发漏洞事件", "Odaily报道ZEC无限增发漏洞风险。"),
        ]

        result = plan_event_backfill(messages, dry_run=False)
        written_events = apply_event_backfill_marks(result)

        self.assertEqual(len(written_events), 1)
        self.assertEqual(messages[0].event_id, written_events[0].id)
        self.assertEqual(messages[1].event_id, written_events[0].id)
        self.assertEqual(messages[0].push_status, "sent")
        self.assertEqual(messages[0].score, 95)
        self.assertEqual(messages[0].signal_level, "S")

    def test_event_backfill_clusters_multiple_zec_reports(self) -> None:
        messages = [
            _event_message(1, "Blockbeats", "ZEC无限增发漏洞事件", "ZEC曝无限增发漏洞引发暴跌，Arthur Hayes清仓。"),
            _event_message(2, "TechFlow", "ZEC漏洞与无限增发事件", "ZEC可能存在无限增发漏洞，市场担忧隐私币信任基础受损。"),
            _event_message(3, "Odaily", "ZEC无限增发漏洞事件", "ZEC漏洞与无限增发风险持续发酵，币价暴跌。"),
            _event_message(4, "PANews", "ZEC漏洞风波事件", "Cypherpunk回应ZEC漏洞风波，称形式化验证可提升安全。"),
        ]

        result = plan_event_backfill(messages, dry_run=True)

        self.assertEqual(result.event_count, 1)
        self.assertEqual(result.events[0].message_count, 4)
        self.assertEqual(result.events[0].source_count, 4)

    def test_event_backfill_keeps_different_events_separate(self) -> None:
        messages = [
            _event_message(1, "Blockbeats", "ZEC无限增发漏洞事件", "ZEC曝无限增发漏洞引发暴跌。"),
            _event_message(2, "TechFlow", "某项目融资事件", "Helion完成4.65亿美元融资，估值升至155亿美元。"),
        ]

        result = plan_event_backfill(messages, dry_run=True)

        self.assertEqual(result.event_count, 2)

    def test_generic_asset_overlap_does_not_merge_different_events(self) -> None:
        existing = SimpleNamespace(
            id=1,
            event_title="USDT市值超越ETH事件",
            event_summary="USDT市值超过ETH，稳定币规模继续增长。",
        )

        match = best_event_match(
            "LonglingCapital疑似减持ETH并提取USDT降杠杆",
            "LonglingCapital疑似向币安转入ETH并提取USDT降低杠杆。",
            "LonglingCapital减持ETH并提取USDT，属于巨鲸仓位变化。",
            [existing],
        )

        self.assertIsNone(match)

    def test_single_project_overlap_without_same_action_does_not_merge(self) -> None:
        existing = SimpleNamespace(
            id=1,
            event_title="Avantis上线BaseMCP",
            event_summary="Avantis宣布上线BaseMCP相关功能。",
        )

        match = best_event_match(
            "BaseMCP征集用户反馈",
            "BaseMCP团队征集用户反馈。",
            "BaseMCP邀请用户提供产品反馈。",
            [existing],
        )

        self.assertIsNone(match)

    def test_event_backfill_clusters_piggybank_reports(self) -> None:
        messages = [
            _event_message(
                1,
                "TechFlow",
                "Piggybank投资LAB做空亏损事件",
                "Piggybank称因投资LAB后做空气亏，短期或导致各vault净值下降。",
            ),
            _event_message(
                2,
                "Blockbeats",
                "Piggybank LAB平仓事件",
                "Piggybank因投资LAB后做空气亏，准备平仓并可能导致各vault净值下滑。",
            ),
        ]

        result = plan_event_backfill(messages, dry_run=True)

        self.assertEqual(result.event_count, 1)
        self.assertEqual(result.events[0].message_count, 2)

    def test_event_backfill_dry_run_finds_duplicate_event_pairs(self) -> None:
        messages = [
            _event_message(1, "Other", "ZEC无限增发漏洞事件", "ZEC曝无限增发漏洞引发暴跌。"),
        ]
        existing_events = [
            SimpleNamespace(
                id=685,
                event_title="Piggybank投资LAB做空亏损事件",
                event_summary="Piggybank称因投资LAB后做空气亏，短期或导致各vault净值下降。",
                last_seen_at=datetime(2026, 6, 5, 8, 1, tzinfo=timezone.utc),
            ),
            SimpleNamespace(
                id=687,
                event_title="Piggybank LAB平仓事件",
                event_summary="Piggybank因投资LAB后做空气亏，准备平仓并可能导致各vault净值下滑。",
                last_seen_at=datetime(2026, 6, 5, 8, 3, tzinfo=timezone.utc),
            )
        ]

        result = plan_event_backfill(messages, existing_events=existing_events, dry_run=True)
        payload = format_event_backfill_payload(result, hours=24)

        self.assertGreaterEqual(payload["would_merge_event_count"], 1)
        self.assertTrue(payload["duplicate_event_pairs"])


class EventClusterConcurrencyTests(unittest.IsolatedAsyncioTestCase):
    async def test_existing_event_key_conflict_is_not_treated_as_new_event(self) -> None:
        class ConflictRepository:
            async def recent_events(self, since):
                return []

            async def create_event(self, event_key: str, event_title: str, event_summary: str | None = None):
                event = SimpleNamespace(
                    id=909,
                    event_key=event_key,
                    event_title=event_title,
                    event_summary=event_summary,
                )
                setattr(event, "_was_created", False)
                return event

        match = await EventClusterer(ConflictRepository()).match_or_create(
            event_title="BTC Hyperliquid清算事件",
            event_summary="BTC短时上探6.4万美元，Hyperliquid出现多笔百万美元级清算。",
            message_text="BTC short squeeze on Hyperliquid",
        )

        self.assertEqual(match.event_id, 909)
        self.assertFalse(match.is_new_event)
        self.assertEqual(match.event_match_reason, "existing_event_key_conflict")


class MockCache:
    def __init__(self) -> None:
        self.keys: set[str] = set()

    async def seen(self, dedup_key: str) -> bool:
        return dedup_key in self.keys

    async def mark(self, dedup_key: str) -> None:
        self.keys.add(dedup_key)

    async def close(self) -> None:
        return None


class MockAnalyzer:
    def __init__(self, importance_score: float = 95, decision: str = "push") -> None:
        self.importance_score = importance_score
        self.decision = decision
        self.last_source_context: dict | None = None

    async def analyze(self, text: str, source_context: dict | None = None) -> dict:
        self.last_source_context = source_context
        return {
            "event_title": "测试上币事件",
            "summary_zh": "测试上币消息",
            "category": "上线上币",
            "signal_type": "exchange_listing",
            "importance_score": self.importance_score,
            "content_score": self.importance_score,
            "source_score": 0,
            "context_score": 0,
            "signal_score": 6,
            "risk_penalty": 0,
            "decision": self.decision,
            "confidence": 90,
            "user_value_summary": "测试价值",
            "action_suggestion": "测试动作",
            "urgency": "medium",
            "relevance": "high",
            "actionability": "high",
            "risk_level": "low",
            "reason": "测试原因",
        }

    async def analyze_event_update(self, **kwargs) -> dict:
        return {"event_update_level": "minor", "reason": "重复报道"}


class SequenceAnalyzer:
    def __init__(self, analyses: list[dict], event_updates: list[dict] | None = None) -> None:
        self.analyses = list(analyses)
        self.event_updates = list(event_updates or [])
        self.source_contexts: list[dict | None] = []

    async def analyze(self, text: str, source_context: dict | None = None) -> dict:
        self.source_contexts.append(source_context)
        payload = self.analyses.pop(0)
        return {
            "event_title": payload["event_title"],
            "summary_zh": payload["summary_zh"],
            "category": payload.get("category", "安全风险"),
            "signal_type": payload.get("signal_type", "exploit"),
            "importance_score": payload.get("importance_score", 95),
            "content_score": payload.get("content_score", payload.get("importance_score", 95)),
            "source_score": payload.get("source_score", 0),
            "context_score": payload.get("context_score", 0),
            "signal_score": payload.get("signal_score", 10),
            "risk_penalty": payload.get("risk_penalty", 0),
            "decision": payload.get("decision", "push"),
            "confidence": payload.get("confidence", 90),
            "user_value_summary": payload.get("user_value_summary", "测试价值"),
            "action_suggestion": payload.get("action_suggestion", "测试动作"),
            "urgency": payload.get("urgency", "medium"),
            "relevance": payload.get("relevance", "high"),
            "actionability": payload.get("actionability", "high"),
            "risk_level": payload.get("risk_level", "low"),
            "reason": payload.get("reason", "测试原因"),
        }

    async def analyze_event_update(self, **kwargs) -> dict:
        if self.event_updates:
            return self.event_updates.pop(0)
        return {"event_update_level": "minor", "reason": "重复报道"}


class MockNotifier:
    def __init__(self, sent: bool = True) -> None:
        self.payloads: list[dict] = []
        self.sent = sent

    async def notify(self, payload: dict) -> NotificationResult:
        self.payloads.append(payload)
        return NotificationResult(sent=self.sent, error=None if self.sent else "failed")


class MockPipeline:
    def __init__(self) -> None:
        self.messages: list[SourceMessage] = []

    async def process(self, message: SourceMessage) -> None:
        self.messages.append(message)


class MockTelegramAdapter:
    def __init__(self, messages: dict[str, list[TelegramApiMessage]] | None = None) -> None:
        self.messages = messages or {}
        self.started = False
        self.stopped = False
        self.fetch_calls: list[tuple[str, int | None, int]] = []

    async def start(self) -> None:
        self.started = True

    async def stop(self) -> None:
        self.stopped = True

    async def fetch_messages(self, channel: str, after_message_id: int | None, limit: int):
        self.fetch_calls.append((channel, after_message_id, limit))
        return self.messages.get(channel, [])[:limit]


class MockCollectorStateRepository:
    def __init__(self, states: dict[str, SimpleNamespace] | None = None) -> None:
        self.states = states or {}

    async def get_collector_state(self, collector_name: str, source_key: str):
        return self.states.get(source_key)

    async def upsert_collector_state(self, collector_name: str, source_key: str, last_seen_id: str | None, last_seen_time):
        self.states[source_key] = SimpleNamespace(
            collector_name=collector_name,
            source_key=source_key,
            last_seen_id=last_seen_id,
            last_seen_time=last_seen_time,
            last_fetch_at=datetime.now(timezone.utc),
        )


def _telegram_watchlist_path(channels: list[str]) -> Path:
    path = Path("/tmp/web3_alpha_telegram_watchlist_test.yaml")
    channel_lines = "\n".join(f"      - {channel}" for channel in channels)
    path.write_text(
        f"""
telegram_watchlists:
  base_alpha:
    label: Base Alpha
    priority: 10
    channels:
{channel_lines}
""",
        encoding="utf-8",
    )
    return path


class MockRepository:
    def __init__(
        self,
        push_counts: dict | None = None,
        duplicate_candidates: list | None = None,
        existing_source_messages: set[tuple[str, str, str]] | None = None,
    ) -> None:
        self.records: list[dict] = []
        self.events: list[SimpleNamespace] = []
        self.push_updates: list[tuple[int, str, str | None]] = []
        self.event_upgrade_updates: list[tuple[int, str | None]] = []
        self.duplicate_candidates = duplicate_candidates or []
        self.existing_source_messages = existing_source_messages or set()
        self.push_counts = push_counts or {
            "sent_today_count": 0,
            "sent_last_hour_count": 0,
            "s_sent_last_hour_count": 0,
            "a_sent_last_hour_count": 0,
        }

    async def exists_by_source_message(self, source_platform: str, source_chat_id: str, source_message_id: str) -> bool:
        return (source_platform, source_chat_id, str(source_message_id)) in self.existing_source_messages

    async def exists_by_dedup_key(self, dedup_key: str) -> bool:
        return False

    async def save(self, message_data: dict):
        self.records.append(message_data)
        return SimpleNamespace(id=len(self.records))

    async def update_push_status(self, message_id: int, status: str, error: str | None = None) -> None:
        self.push_updates.append((message_id, status, error))

    async def push_rate_counts(self) -> dict[str, int]:
        return self.push_counts

    async def recent_high_score_messages(self, since, min_score: float, limit: int = 200):
        return self.duplicate_candidates

    async def recent_events(self, since, limit: int = 500):
        return self.events[:limit]

    async def get_event(self, event_id: int):
        for event in self.events:
            if event.id == event_id:
                return event
        return None

    async def create_event(self, event_key: str, event_title: str, event_summary: str | None = None):
        event = SimpleNamespace(
            id=len(self.events) + 1,
            event_key=event_key,
            event_title=event_title,
            event_summary=event_summary,
            message_count=0,
            source_count=0,
            max_score=0,
            latest_summary=event_summary,
            upgrade_count=0,
            last_upgrade_at=None,
            last_upgrade_summary=None,
            last_pushed_at=None,
        )
        self.events.append(event)
        return event

    async def update_event_stats(self, event_id: int) -> None:
        event = next(item for item in self.events if item.id == event_id)
        event_messages = [record for record in self.records if record.get("event_id") == event_id]
        event.message_count = len(event_messages)
        event.source_count = len({record["source_chat_id"] for record in event_messages})
        event.max_score = max((record["score"] for record in event_messages), default=0)
        event.latest_summary = event_messages[-1]["summary_zh"] if event_messages else None

    async def mark_event_upgrade_pushed(self, event_id: int, upgrade_summary: str | None = None) -> None:
        event = next(item for item in self.events if item.id == event_id)
        event.upgrade_count = int(getattr(event, "upgrade_count", 0) or 0) + 1
        event.last_upgrade_at = datetime.now(timezone.utc)
        event.last_upgrade_summary = upgrade_summary
        event.last_pushed_at = datetime.now(timezone.utc)
        self.event_upgrade_updates.append((event_id, upgrade_summary))

    @staticmethod
    def serialize_analysis(analysis: dict) -> str:
        import json

        return json.dumps(analysis, ensure_ascii=False)


class PipelineSmokeTests(unittest.IsolatedAsyncioTestCase):
    async def test_pipeline_smoke_high_score_message_is_saved_and_pushed(self) -> None:
        cache = MockCache()
        analyzer = MockAnalyzer()
        notifier = MockNotifier()
        repository = MockRepository()
        pipeline = MessagePipeline(
            cache=cache,
            analyzer=analyzer,
            notifier=notifier,
            repository=repository,
        )

        await pipeline.process(
            SourceMessage(
                source="telegram_public",
                source_chat_id="alpha",
                source_chat_title="@alpha",
                source_message_id=1001,
                raw_text="Binance listing mainnet launch test",
            )
        )

        self.assertEqual(len(repository.records), 1)
        self.assertEqual(repository.records[0]["push_status"], "pending")
        self.assertEqual(repository.records[0]["signal_level"], "A")
        analysis = json.loads(repository.records[0]["analysis_json"])
        self.assertIn("score_breakdown", analysis)
        self.assertEqual(analysis["score_breakdown"]["final_score"], repository.records[0]["score"])
        self.assertEqual(repository.push_updates, [(1, "sent", None)])
        self.assertEqual(len(notifier.payloads), 1)
        self.assertEqual(next(iter(cache.keys)), repository.records[0]["dedup_key"])

    async def test_pipeline_clusters_zec_reports_and_pushes_only_first_event(self) -> None:
        cache = MockCache()
        notifier = MockNotifier()
        repository = MockRepository()
        analyzer = SequenceAnalyzer(
            [
                {
                    "event_title": "ZEC无限增发漏洞事件",
                    "summary_zh": "ZEC曝无限增发漏洞引发暴跌，Arthur Hayes清仓。",
                },
                {
                    "event_title": "ZEC无限增发漏洞事件",
                    "summary_zh": "Blockbeats报道ZEC无限增发漏洞导致市场担忧。",
                },
                {
                    "event_title": "ZEC漏洞与无限增发事件",
                    "summary_zh": "TechFlow报道ZEC可能存在无限增发漏洞并引发暴跌。",
                },
                {
                    "event_title": "ZEC漏洞风波事件",
                    "summary_zh": "PANews报道Cypherpunk回应ZEC漏洞风波，称形式化验证可提升安全。",
                },
            ]
        )
        pipeline = MessagePipeline(
            cache=cache,
            analyzer=analyzer,
            notifier=notifier,
            repository=repository,
        )

        messages = [
            ("theblockbeats", "ZEC暴露无限增发漏洞引发暴跌，Arthur Hayes清仓。"),
            ("TechFlowDaily", "ZEC可能存在无限增发漏洞，市场担忧隐私币信任基础受损。"),
            ("Odaily", "ZEC漏洞与无限增发风险持续发酵，币价暴跌。"),
            ("PANews", "Cypherpunk回应ZEC漏洞风波，称应以形式化验证提升安全。"),
        ]
        for index, (channel, text) in enumerate(messages, start=1):
            await pipeline.process(
                SourceMessage(
                    source="telegram_public",
                    source_chat_id=channel,
                    source_chat_title=f"@{channel}",
                    source_message_id=10_000 + index,
                    raw_text=text,
                )
            )

        self.assertEqual(len(repository.events), 1)
        self.assertEqual(repository.events[0].message_count, 4)
        self.assertEqual(repository.events[0].source_count, 4)
        self.assertEqual([record["event_id"] for record in repository.records], [1, 1, 1, 1])
        self.assertEqual(
            [record["push_status"] for record in repository.records],
            ["pending", "skipped_event_duplicate", "skipped_event_duplicate", "skipped_event_duplicate"],
        )
        self.assertEqual(repository.push_updates, [(1, "sent", None)])
        self.assertEqual(len(notifier.payloads), 1)
        self.assertEqual(repository.events[0].max_score, 85.0)
        self.assertEqual(repository.events[0].latest_summary, "PANews报道Cypherpunk回应ZEC漏洞风波，称形式化验证可提升安全。")

    async def test_pipeline_existing_event_second_message_is_skipped_event_duplicate(self) -> None:
        cache = MockCache()
        notifier = MockNotifier()
        repository = MockRepository()
        analyzer = SequenceAnalyzer(
            [
                {
                    "event_title": "ZEC无限增发漏洞事件",
                    "summary_zh": "ZEC曝无限增发漏洞引发暴跌。",
                },
                {
                    "event_title": "ZEC漏洞风波事件",
                    "summary_zh": "TechFlow报道ZEC可能存在无限增发漏洞。",
                },
            ]
        )
        pipeline = MessagePipeline(cache=cache, analyzer=analyzer, notifier=notifier, repository=repository)

        await pipeline.process(
            SourceMessage(
                source="telegram_public",
                source_chat_id="theblockbeats",
                source_chat_title="@theblockbeats",
                source_message_id=20_001,
                raw_text="ZEC曝无限增发漏洞引发暴跌。",
            )
        )
        await pipeline.process(
            SourceMessage(
                source="telegram_public",
                source_chat_id="TechFlowDaily",
                source_chat_title="@TechFlowDaily",
                source_message_id=20_002,
                raw_text="TechFlow报道ZEC可能存在无限增发漏洞。",
            )
        )

        self.assertEqual(repository.records[0]["push_status"], "pending")
        self.assertEqual(repository.push_updates, [(1, "sent", None)])
        self.assertEqual(repository.records[1]["push_status"], "skipped_event_duplicate")
        self.assertIn("event_id=1", repository.records[1]["push_error"])
        self.assertIn("event_match_reason=", repository.records[1]["push_error"])
        self.assertEqual(len(notifier.payloads), 1)

    async def test_pipeline_existing_event_major_update_is_pushed(self) -> None:
        cache = MockCache()
        notifier = MockNotifier()
        repository = MockRepository()
        analyzer = SequenceAnalyzer(
            [
                {
                    "event_title": "ZEC无限增发漏洞事件",
                    "summary_zh": "ZEC曝无限增发漏洞引发暴跌。",
                },
                {
                    "event_title": "ZEC无限增发漏洞事件",
                    "summary_zh": "官方确认漏洞已修复，但是否被利用仍无法验证。",
                },
            ],
            event_updates=[{"event_update_level": "major", "reason": "官方确认修复，属于重要新增事实"}],
        )
        pipeline = MessagePipeline(cache=cache, analyzer=analyzer, notifier=notifier, repository=repository)

        await pipeline.process(
            SourceMessage(
                source="telegram_public",
                source_chat_id="Odaily_News",
                source_chat_title="@Odaily_News",
                source_message_id=30_001,
                raw_text="ZEC曝无限增发漏洞引发暴跌。",
            )
        )
        await pipeline.process(
            SourceMessage(
                source="telegram_public",
                source_chat_id="theblockbeats",
                source_chat_title="@theblockbeats",
                source_message_id=30_002,
                raw_text="官方确认ZEC无限增发漏洞已修复，但是否被利用仍无法验证。",
            )
        )

        self.assertEqual(repository.records[0]["push_status"], "pending")
        self.assertEqual(repository.records[1]["push_status"], "pending")
        self.assertEqual(repository.push_updates, [(1, "sent", None), (2, "sent", None)])
        self.assertEqual(len(notifier.payloads), 2)
        analysis = json.loads(repository.records[1]["analysis_json"])
        self.assertEqual(analysis["event_update"]["event_update_level"], "major")
        self.assertEqual(repository.events[0].upgrade_count, 1)
        self.assertIsNotNone(repository.events[0].last_upgrade_at)
        self.assertEqual(repository.events[0].last_upgrade_summary, "官方确认漏洞已修复，但是否被利用仍无法验证。")
        self.assertIsNotNone(repository.events[0].last_pushed_at)

    async def test_pipeline_existing_event_critical_update_increments_upgrade_count(self) -> None:
        cache = MockCache()
        notifier = MockNotifier()
        repository = MockRepository()
        analyzer = SequenceAnalyzer(
            [
                {"event_title": "ZEC无限增发漏洞事件", "summary_zh": "ZEC曝无限增发漏洞。"},
                {"event_title": "ZEC无限增发漏洞事件", "summary_zh": "官方确认漏洞已被利用并造成重大影响。"},
            ],
            event_updates=[{"event_update_level": "critical", "decision": "push", "reason": "确认被利用"}],
        )
        pipeline = MessagePipeline(cache=cache, analyzer=analyzer, notifier=notifier, repository=repository)

        await pipeline.process(
            SourceMessage(
                source="telegram_public",
                source_chat_id="Odaily_News",
                source_chat_title="@Odaily_News",
                source_message_id=30_011,
                raw_text="ZEC曝无限增发漏洞。",
            )
        )
        await pipeline.process(
            SourceMessage(
                source="telegram_public",
                source_chat_id="theblockbeats",
                source_chat_title="@theblockbeats",
                source_message_id=30_012,
                raw_text="官方确认ZEC无限增发漏洞已被利用并造成重大影响。",
            )
        )

        self.assertEqual(repository.events[0].upgrade_count, 1)
        self.assertEqual(repository.event_upgrade_updates[0][0], 1)

    async def test_pipeline_existing_event_minor_update_does_not_increment_upgrade_count(self) -> None:
        cache = MockCache()
        notifier = MockNotifier()
        repository = MockRepository()
        analyzer = SequenceAnalyzer(
            [
                {"event_title": "ZEC无限增发漏洞事件", "summary_zh": "ZEC曝无限增发漏洞。"},
                {"event_title": "ZEC无限增发漏洞事件", "summary_zh": "媒体重复报道ZEC漏洞。"},
            ],
            event_updates=[{"event_update_level": "minor", "decision": "ignore", "reason": "重复报道"}],
        )
        pipeline = MessagePipeline(cache=cache, analyzer=analyzer, notifier=notifier, repository=repository)

        await pipeline.process(
            SourceMessage(
                source="telegram_public",
                source_chat_id="Odaily_News",
                source_chat_title="@Odaily_News",
                source_message_id=30_021,
                raw_text="ZEC曝无限增发漏洞。",
            )
        )
        await pipeline.process(
            SourceMessage(
                source="telegram_public",
                source_chat_id="theblockbeats",
                source_chat_title="@theblockbeats",
                source_message_id=30_022,
                raw_text="媒体重复报道ZEC无限增发漏洞。",
            )
        )

        self.assertEqual(repository.events[0].upgrade_count, 0)
        self.assertEqual(repository.event_upgrade_updates, [])
        self.assertEqual(repository.records[1]["push_status"], "skipped_event_duplicate")

    async def test_pipeline_existing_event_upgrade_push_failure_does_not_update_last_pushed_at(self) -> None:
        cache = MockCache()
        notifier = MockNotifier(sent=False)
        repository = MockRepository()
        analyzer = SequenceAnalyzer(
            [
                {"event_title": "ZEC无限增发漏洞事件", "summary_zh": "ZEC曝无限增发漏洞。"},
                {"event_title": "ZEC无限增发漏洞事件", "summary_zh": "官方确认漏洞已修复。"},
            ],
            event_updates=[{"event_update_level": "major", "decision": "push", "reason": "官方确认修复"}],
        )
        pipeline = MessagePipeline(cache=cache, analyzer=analyzer, notifier=notifier, repository=repository)

        await pipeline.process(
            SourceMessage(
                source="telegram_public",
                source_chat_id="Odaily_News",
                source_chat_title="@Odaily_News",
                source_message_id=30_031,
                raw_text="ZEC曝无限增发漏洞。",
            )
        )
        await pipeline.process(
            SourceMessage(
                source="telegram_public",
                source_chat_id="theblockbeats",
                source_chat_title="@theblockbeats",
                source_message_id=30_032,
                raw_text="官方确认ZEC无限增发漏洞已修复。",
            )
        )

        self.assertEqual(repository.events[0].upgrade_count, 0)
        self.assertIsNone(repository.events[0].last_pushed_at)
        self.assertEqual(repository.event_upgrade_updates, [])

    async def test_pipeline_existing_event_major_watch_update_is_pushed(self) -> None:
        cache = MockCache()
        notifier = MockNotifier()
        repository = MockRepository()
        analyzer = SequenceAnalyzer(
            [
                {
                    "event_title": "Base Season活动事件",
                    "summary_zh": "Base 发布 Season 活动。",
                    "decision": "push",
                },
                {
                    "event_title": "Base Season活动事件",
                    "summary_zh": "Base Season 活动新增积分规则，值得后续跟踪。",
                    "decision": "watch",
                },
            ],
            event_updates=[{"event_update_level": "major", "decision": "watch", "reason": "新增积分规则，值得观察提醒"}],
        )
        pipeline = MessagePipeline(cache=cache, analyzer=analyzer, notifier=notifier, repository=repository)

        await pipeline.process(
            SourceMessage(
                source="telegram_public",
                source_chat_id="base",
                source_chat_title="@base",
                source_message_id=30_101,
                raw_text="Base 发布 Season 活动。",
            )
        )
        await pipeline.process(
            SourceMessage(
                source="telegram_public",
                source_chat_id="base",
                source_chat_title="@base",
                source_message_id=30_102,
                raw_text="Base Season 活动新增积分规则，值得后续跟踪。",
            )
        )

        self.assertEqual(repository.records[1]["push_status"], "pending")
        self.assertEqual(repository.push_updates, [(1, "sent", None), (2, "sent", None)])
        self.assertEqual(notifier.payloads[1]["ai_decision"], "watch")
        self.assertTrue(notifier.payloads[1]["event_upgrade"])

    async def test_pipeline_possible_duplicate_major_event_update_is_pushed(self) -> None:
        cache = MockCache()
        notifier = MockNotifier()
        repository = MockRepository(
            duplicate_candidates=[
                SimpleNamespace(
                    id=1,
                    cleaned_text="Base Season 活动新增积分规则，值得后续跟踪。",
                    summary_zh="Base Season活动新增积分规则。",
                    category="Alpha机会",
                    score=90,
                )
            ]
        )
        analyzer = SequenceAnalyzer(
            [
                {
                    "event_title": "Base Season活动事件",
                    "summary_zh": "Base 发布 Season 活动。",
                    "decision": "push",
                },
                {
                    "event_title": "Base Season活动事件",
                    "summary_zh": "Base Season活动新增积分规则。",
                    "decision": "watch",
                },
            ],
            event_updates=[{"event_update_level": "major", "decision": "watch", "reason": "新增积分规则，值得提醒"}],
        )
        pipeline = MessagePipeline(cache=cache, analyzer=analyzer, notifier=notifier, repository=repository)

        await pipeline.process(
            SourceMessage(
                source="telegram_public",
                source_chat_id="base",
                source_chat_title="@base",
                source_message_id=30_201,
                raw_text="Base 发布 Season 活动。",
            )
        )
        await pipeline.process(
            SourceMessage(
                source="telegram_public",
                source_chat_id="base",
                source_chat_title="@base",
                source_message_id=30_202,
                raw_text="Base Season 活动新增积分规则，值得后续跟踪。",
            )
        )

        self.assertTrue(repository.records[1]["possible_duplicate"])
        self.assertEqual(repository.records[1]["push_status"], "pending")
        self.assertEqual(repository.push_updates, [(1, "sent", None), (2, "sent", None)])
        self.assertEqual(notifier.payloads[1]["ai_decision"], "watch")
        self.assertTrue(notifier.payloads[1]["event_upgrade"])

    async def test_pipeline_s_level_duplicate_is_skipped_without_push(self) -> None:
        cache = MockCache()
        analyzer = MockAnalyzer(importance_score=95)
        notifier = MockNotifier()
        repository = MockRepository(
            duplicate_candidates=[
                SimpleNamespace(
                    id=99,
                    cleaned_text="binance listing mainnet launch for alpha protocol",
                    score=95,
                )
            ]
        )
        pipeline = MessagePipeline(
            cache=cache,
            analyzer=analyzer,
            notifier=notifier,
            repository=repository,
        )

        await pipeline.process(
            SourceMessage(
                source="telegram_public",
                source_chat_id="alpha",
                source_chat_title="@alpha",
                source_message_id=1007,
                raw_text="Binance listing mainnet launch for Alpha Protocol",
            )
        )

        self.assertTrue(repository.records[0]["possible_duplicate"])
        self.assertEqual(repository.records[0]["duplicate_of_message_id"], 99)
        self.assertGreater(repository.records[0]["similarity_score"], 0.90)
        self.assertEqual(repository.records[0]["push_status"], "skipped_duplicate")
        self.assertEqual(repository.records[0]["event_match_reason"], "new_event")
        self.assertEqual(repository.push_updates, [])
        self.assertEqual(len(notifier.payloads), 0)

    async def test_pipeline_a_level_duplicate_is_skipped_without_push(self) -> None:
        cache = MockCache()
        analyzer = MockAnalyzer(importance_score=80)
        notifier = MockNotifier()
        repository = MockRepository(
            duplicate_candidates=[
                SimpleNamespace(
                    id=99,
                    cleaned_text="plain market update",
                    score=80,
                )
            ]
        )
        pipeline = MessagePipeline(
            cache=cache,
            analyzer=analyzer,
            notifier=notifier,
            repository=repository,
        )

        await pipeline.process(
            SourceMessage(
                source="telegram_public",
                source_chat_id="alpha",
                source_chat_title="@alpha",
                source_message_id=1008,
                raw_text="plain market update",
            )
        )

        self.assertEqual(repository.records[0]["signal_level"], "A")
        self.assertTrue(repository.records[0]["possible_duplicate"])
        self.assertEqual(repository.records[0]["push_status"], "skipped_duplicate")
        self.assertEqual(repository.records[0]["event_match_reason"], "new_event")
        self.assertEqual(repository.push_updates, [])
        self.assertEqual(len(notifier.payloads), 0)

    async def test_pipeline_existing_source_message_is_hard_deduped_before_ai(self) -> None:
        cache = MockCache()
        analyzer = MockAnalyzer(importance_score=95)
        notifier = MockNotifier()
        repository = MockRepository(
            existing_source_messages={("telegram_public", "alpha", "1009")}
        )
        pipeline = MessagePipeline(
            cache=cache,
            analyzer=analyzer,
            notifier=notifier,
            repository=repository,
        )

        await pipeline.process(
            SourceMessage(
                source="telegram_public",
                source_chat_id="alpha",
                source_chat_title="@alpha",
                source_message_id=1009,
                raw_text="same telegram message edited text",
            )
        )

        self.assertEqual(repository.records, [])
        self.assertIsNone(analyzer.last_source_context)
        self.assertEqual(repository.push_updates, [])
        self.assertEqual(notifier.payloads, [])

    async def test_pipeline_smoke_a_level_message_is_saved_and_pushed(self) -> None:
        cache = MockCache()
        analyzer = MockAnalyzer(importance_score=80)
        notifier = MockNotifier()
        repository = MockRepository()
        pipeline = MessagePipeline(
            cache=cache,
            analyzer=analyzer,
            notifier=notifier,
            repository=repository,
        )

        await pipeline.process(
            SourceMessage(
                source="telegram_public",
                source_chat_id="alpha",
                source_chat_title="@alpha",
                source_message_id=1004,
                raw_text="plain market update",
            )
        )

        self.assertEqual(repository.records[0]["signal_level"], "A")
        self.assertEqual(repository.records[0]["push_status"], "pending")
        self.assertEqual(repository.push_updates, [(1, "sent", None)])
        self.assertEqual(len(notifier.payloads), 1)

    async def test_pipeline_saves_discord_source_platform(self) -> None:
        cache = MockCache()
        analyzer = MockAnalyzer(importance_score=80)
        notifier = MockNotifier()
        repository = MockRepository()
        pipeline = MessagePipeline(
            cache=cache,
            analyzer=analyzer,
            notifier=notifier,
            repository=repository,
        )

        await pipeline.process(
            SourceMessage(
                source="discord",
                source_chat_id="999",
                source_chat_title="server#alpha",
                source_message_id=123456789012345678,
                raw_text="discord alpha update",
                watchlist_category="base",
                watchlist_label="Base Discord",
                watchlist_priority=10,
                metadata={"project": "Base", "ecosystem": "Base", "discord_channel_type": "announcement"},
            )
        )

        self.assertEqual(repository.records[0]["source"], "discord")
        self.assertEqual(repository.records[0]["source_platform"], "discord")
        self.assertEqual(repository.records[0]["source_chat_title"], "server#alpha")
        self.assertEqual(analyzer.last_source_context["source_platform"], "discord")
        self.assertEqual(analyzer.last_source_context["project"], "Base")
        self.assertEqual(analyzer.last_source_context["channel"], "server#alpha")
        self.assertEqual(notifier.payloads[0]["source_platform"], "discord")
        self.assertEqual(notifier.payloads[0]["source_project"], "Base")

    async def test_pipeline_telegram_watchlist_fields_are_empty(self) -> None:
        cache = MockCache()
        analyzer = MockAnalyzer(importance_score=40)
        notifier = MockNotifier()
        repository = MockRepository()
        pipeline = MessagePipeline(
            cache=cache,
            analyzer=analyzer,
            notifier=notifier,
            repository=repository,
        )

        await pipeline.process(
            SourceMessage(
                source="telegram_public",
                source_chat_id="alpha",
                source_chat_title="@alpha",
                source_message_id=1010,
                raw_text="telegram market update",
            )
        )

        self.assertIsNone(repository.records[0]["watchlist_category"])
        self.assertIsNone(repository.records[0]["watchlist_label"])
        self.assertIsNone(repository.records[0]["watchlist_priority"])

    async def test_pipeline_saves_x_watchlist_metadata(self) -> None:
        cache = MockCache()
        analyzer = MockAnalyzer(importance_score=40)
        notifier = MockNotifier()
        repository = MockRepository()
        pipeline = MessagePipeline(
            cache=cache,
            analyzer=analyzer,
            notifier=notifier,
            repository=repository,
        )

        await pipeline.process(
            SourceMessage(
                source="x",
                source_chat_id="http://rsshub:1200/twitter/user/base",
                source_chat_title="Twitter @Base",
                source_message_id="tweet-1011",
                raw_text="x market update",
                watchlist_category="base_core",
                watchlist_label="Base核心生态",
                watchlist_priority=10,
            )
        )

        self.assertEqual(repository.records[0]["watchlist_category"], "base_core")
        self.assertEqual(repository.records[0]["watchlist_label"], "Base核心生态")
        self.assertEqual(repository.records[0]["watchlist_priority"], 10)
        self.assertEqual(analyzer.last_source_context["source_profile"]["key"], "base")
        analysis = json.loads(repository.records[0]["analysis_json"])
        breakdown = analysis["score_breakdown"]
        self.assertEqual(breakdown["source_profile"]["key"], "base")
        self.assertEqual(breakdown["source_score"], 14)
        self.assertEqual(breakdown["context_score"], 10)
        self.assertIn("content_score", breakdown)
        self.assertIn("signal_score", breakdown)

    async def test_pipeline_watch_message_is_pushed_as_observation(self) -> None:
        cache = MockCache()
        analyzer = MockAnalyzer(importance_score=65, decision="watch")
        notifier = MockNotifier()
        repository = MockRepository()
        pipeline = MessagePipeline(
            cache=cache,
            analyzer=analyzer,
            notifier=notifier,
            repository=repository,
        )

        await pipeline.process(
            SourceMessage(
                source="telegram_public",
                source_chat_id="alpha",
                source_chat_title="@alpha",
                source_message_id=1002,
                raw_text="plain market update",
            )
        )

        self.assertEqual(repository.records[0]["signal_level"], "B")
        self.assertEqual(repository.records[0]["push_status"], "pending")
        self.assertNotEqual(repository.records[0]["push_status"], "skipped_watch")
        self.assertEqual(repository.push_updates, [(1, "sent", None)])
        self.assertEqual(notifier.payloads[0]["ai_decision"], "watch")

    async def test_pipeline_invalid_ai_decision_defaults_to_watch(self) -> None:
        cache = MockCache()
        analyzer = MockAnalyzer(importance_score=95, decision="send")
        notifier = MockNotifier()
        repository = MockRepository()
        pipeline = MessagePipeline(
            cache=cache,
            analyzer=analyzer,
            notifier=notifier,
            repository=repository,
        )

        await pipeline.process(
            SourceMessage(
                source="telegram_public",
                source_chat_id="alpha",
                source_chat_title="@alpha",
                source_message_id=10020,
                raw_text="high score but invalid decision",
            )
        )

        self.assertEqual(repository.records[0]["ai_decision"], "watch")
        self.assertEqual(repository.records[0]["push_status"], "pending")
        self.assertNotEqual(repository.records[0]["push_status"], "skipped_watch")
        self.assertEqual(repository.push_updates, [(1, "sent", None)])
        self.assertEqual(notifier.payloads[0]["ai_decision"], "watch")

    async def test_pipeline_a_level_hourly_limit_marks_rate_limited(self) -> None:
        cache = MockCache()
        analyzer = MockAnalyzer(importance_score=80)
        notifier = MockNotifier()
        repository = MockRepository(push_counts={"sent_today_count": 0, "sent_last_hour_count": 5, "s_sent_last_hour_count": 0, "a_sent_last_hour_count": 5})
        pipeline = MessagePipeline(
            cache=cache,
            analyzer=analyzer,
            notifier=notifier,
            repository=repository,
        )

        with patch.object(pipeline_module.settings, "push_a_level_hourly_limit", 5):
            await pipeline.process(
                SourceMessage(
                    source="telegram_public",
                    source_chat_id="alpha",
                    source_chat_title="@alpha",
                    source_message_id=1005,
                    raw_text="plain market update",
                )
            )

        self.assertEqual(repository.records[0]["signal_level"], "A")
        self.assertEqual(repository.push_updates, [(1, "skipped_rate_limited", "hourly_limit_reached")])
        self.assertEqual(notifier.payloads, [])

    async def test_pipeline_daily_limit_marks_rate_limited(self) -> None:
        cache = MockCache()
        analyzer = MockAnalyzer(importance_score=95)
        notifier = MockNotifier()
        repository = MockRepository(push_counts={"sent_today_count": 30, "sent_last_hour_count": 0, "s_sent_last_hour_count": 0, "a_sent_last_hour_count": 0})
        pipeline = MessagePipeline(
            cache=cache,
            analyzer=analyzer,
            notifier=notifier,
            repository=repository,
        )

        with patch.object(pipeline_module.settings, "push_daily_limit", 30):
            await pipeline.process(
                SourceMessage(
                    source="telegram_public",
                    source_chat_id="alpha",
                    source_chat_title="@alpha",
                    source_message_id=1006,
                    raw_text="plain market update",
                )
            )

        self.assertEqual(repository.records[0]["signal_level"], "A")
        self.assertEqual(repository.push_updates, [(1, "skipped_rate_limited", "daily_limit_reached")])
        self.assertEqual(notifier.payloads, [])

    async def test_pipeline_smoke_c_level_message_is_saved_without_push(self) -> None:
        cache = MockCache()
        analyzer = MockAnalyzer(importance_score=40, decision="ignore")
        notifier = MockNotifier()
        repository = MockRepository()
        pipeline = MessagePipeline(
            cache=cache,
            analyzer=analyzer,
            notifier=notifier,
            repository=repository,
        )

        await pipeline.process(
            SourceMessage(
                source="telegram_public",
                source_chat_id="alpha",
                source_chat_title="@alpha",
                source_message_id=1003,
                raw_text="plain market update",
            )
        )

        self.assertEqual(repository.records[0]["signal_level"], "C")
        self.assertEqual(repository.records[0]["push_status"], "skipped_ignore")
        self.assertEqual(repository.push_updates, [])
        self.assertEqual(notifier.payloads, [])


if __name__ == "__main__":
    unittest.main()
