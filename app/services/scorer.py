from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Any

from app.core.logging import get_logger


logger = get_logger(__name__)
RULES_PATH = Path("config/score_rules.yaml")
SIGNAL_RULES_PATH = Path("config/signal_rules.yaml")
LOW_SIGNAL_TYPES = {"macro", "ipo", "market_news", "unknown"}
LOW_SIGNAL_SUPPRESSED_KEYWORDS = {
    "listing",
    "binance",
    "coinbase",
    "mainnet",
    "funding",
    "launch",
    "partnership",
}

FALLBACK_RULES = {
    "keywords": {
        "airdrop": 8,
        "claim": 10,
        "snapshot": 10,
        "points": 6,
        "reward": 6,
        "rewards": 6,
        "incentive": 6,
        "incentives": 6,
        "retroactive": 10,
        "testnet": 5,
        "whitelist": 6,
        "allowlist": 6,
        "campaign": 5,
        "quest": 5,
        "epoch": 4,
        "season": 5,
        "listing": 8,
        "binance": 8,
        "coinbase": 8,
        "mainnet": 6,
        "funding": 8,
        "launch": 4,
        "partnership": 4,
        "hack": 15,
        "exploit": 15,
        "security incident": 15,
        "paused": 10,
        "withdrawal suspended": 12,
    },
    "risk_keywords": {
        "rumor": 10,
        "unconfirmed": 10,
        "传言": 10,
        "未经证实": 10,
        "100x": 15,
        "1000x": 20,
        "gem": 8,
        "ape now": 12,
        "send funds": 20,
        "private key": 30,
        "seed phrase": 30,
        "助记词": 30,
        "私钥": 30,
    },
    "limits": {
        "keyword_bonus_max": 12,
        "risk_penalty_max": 40,
        "final_score_min": 0,
        "final_score_max": 100,
    },
}

FALLBACK_SIGNAL_RULES = {
    "signal_types": {
        "airdrop": {"bonus": 15, "label": "空投机会"},
        "points_program": {"bonus": 12, "label": "积分活动"},
        "claim": {"bonus": 12, "label": "领取/申领"},
        "snapshot": {"bonus": 12, "label": "快照"},
        "testnet": {"bonus": 8, "label": "测试网交互"},
        "wallet_tge": {"bonus": 10, "label": "钱包/TGE机会"},
        "exchange_listing": {"bonus": 6, "label": "交易所上线"},
        "token_unlock": {"bonus": 6, "label": "代币解锁"},
        "funding": {"bonus": 3, "label": "融资"},
        "hack": {"bonus": 10, "label": "安全事件"},
        "exploit": {"bonus": 10, "label": "漏洞攻击"},
        "partnership": {"bonus": 2, "label": "合作"},
        "governance": {"bonus": 1, "label": "治理"},
        "macro": {"bonus": 0, "label": "宏观资讯"},
        "ipo": {"bonus": 0, "label": "IPO资讯"},
        "market_news": {"bonus": 0, "label": "普通市场资讯"},
        "unknown": {"bonus": 0, "label": "未知"},
    },
    "limits": {"signal_bonus_max": 15},
}


@dataclass(frozen=True)
class ScoreRules:
    keywords: dict[str, float]
    risk_keywords: dict[str, float]
    keyword_bonus_max: float
    risk_penalty_max: float
    final_score_min: float
    final_score_max: float


@dataclass(frozen=True)
class SignalTypeRule:
    bonus: float
    label: str


@dataclass(frozen=True)
class SignalRules:
    signal_types: dict[str, SignalTypeRule]
    signal_bonus_max: float


@dataclass
class ScoreResult:
    final_score: float
    content_score: float
    source_score: float
    context_score: float
    signal_score: float
    risk_penalty: float
    keyword_auxiliary_bonus: float
    keyword_bonus: float
    signal_type: str
    signal_label: str
    signal_bonus: float
    matched_keywords: list[str]
    matched_risk_keywords: list[str]
    source_profile: dict | None
    watchlist_category: str | None
    watchlist_priority: int | None

    @property
    def ai_score(self) -> float:
        return self.content_score

    @property
    def source_bonus(self) -> float:
        return self.source_score

    def breakdown(self) -> dict:
        return {
            "content_score": self.content_score,
            "source_score": self.source_score,
            "context_score": self.context_score,
            "signal_score": self.signal_score,
            "risk_penalty": self.risk_penalty,
            "keyword_auxiliary_bonus": self.keyword_auxiliary_bonus,
            "ai_score": self.content_score,
            "keyword_bonus": self.keyword_bonus,
            "source_bonus": self.source_score,
            "signal_type": self.signal_type,
            "signal_label": self.signal_label,
            "signal_bonus": self.signal_bonus,
            "final_score": self.final_score,
            "matched_keywords": self.matched_keywords,
            "matched_risk_keywords": self.matched_risk_keywords,
            "source_profile": self.source_profile,
            "watchlist_category": self.watchlist_category,
            "watchlist_priority": self.watchlist_priority,
            "formula": "content_score + source_score + context_score + signal_score - risk_penalty",
        }


def score_message(
    text: str,
    ai_score: float,
    signal_type: str = "unknown",
    *,
    analysis: dict | None = None,
    source_profile: dict | None = None,
    watchlist_category: str | None = None,
    watchlist_priority: int | None = None,
    rules: ScoreRules | None = None,
    signal_rules: SignalRules | None = None,
) -> ScoreResult:
    active_rules = rules or load_score_rules()
    active_signal_rules = signal_rules or load_signal_rules()
    normalized_signal_type = normalize_signal_type(signal_type, active_signal_rules)
    signal_rule = active_signal_rules.signal_types[normalized_signal_type]
    lower_text = text.lower()

    matched_keywords = _matched_terms(lower_text, active_rules.keywords)
    if normalized_signal_type in LOW_SIGNAL_TYPES:
        matched_keywords = [
            keyword for keyword in matched_keywords if keyword not in LOW_SIGNAL_SUPPRESSED_KEYWORDS
        ]
    matched_risk_keywords = _matched_terms(lower_text, active_rules.risk_keywords)
    legacy_keyword_bonus = min(
        active_rules.keyword_bonus_max,
        sum(active_rules.keywords[keyword] for keyword in matched_keywords),
    )
    keyword_auxiliary_bonus = min(3.0, legacy_keyword_bonus * 0.25)
    keyword_risk_penalty = min(
        active_rules.risk_penalty_max,
        sum(active_rules.risk_keywords[keyword] for keyword in matched_risk_keywords),
    )
    content_score = min(75.0, _score_value(analysis, "content_score", ai_score) + keyword_auxiliary_bonus)
    source_score = max(_source_score(source_profile), _score_value(analysis, "source_score", 0))
    source_score = max(0.0, min(15.0, source_score))
    context_score = max(_context_score(watchlist_category, watchlist_priority), _score_value(analysis, "context_score", 0))
    context_score = max(0.0, min(10.0, context_score))
    raw_signal_score = _score_value(analysis, "signal_score", signal_rule.bonus)
    if raw_signal_score <= 0 and normalized_signal_type != "unknown":
        raw_signal_score = signal_rule.bonus
    signal_score = min(active_signal_rules.signal_bonus_max, raw_signal_score)
    risk_penalty = min(
        active_rules.risk_penalty_max,
        max(_score_value(analysis, "risk_penalty", 0), keyword_risk_penalty),
    )
    raw_score = content_score + source_score + context_score + signal_score - risk_penalty
    final_score = round(max(active_rules.final_score_min, min(active_rules.final_score_max, raw_score)), 2)

    return ScoreResult(
        final_score=final_score,
        content_score=round(content_score, 2),
        source_score=round(source_score, 2),
        context_score=round(context_score, 2),
        signal_score=round(signal_score, 2),
        risk_penalty=round(risk_penalty, 2),
        keyword_auxiliary_bonus=round(keyword_auxiliary_bonus, 2),
        keyword_bonus=round(keyword_auxiliary_bonus, 2),
        signal_type=normalized_signal_type,
        signal_label=signal_rule.label,
        signal_bonus=round(signal_score, 2),
        matched_keywords=matched_keywords,
        matched_risk_keywords=matched_risk_keywords,
        source_profile=source_profile,
        watchlist_category=watchlist_category,
        watchlist_priority=watchlist_priority,
    )


@lru_cache(maxsize=8)
def load_score_rules(path: Path = RULES_PATH) -> ScoreRules:
    try:
        payload = _load_yaml_mapping(path)
        return _normalize_rules(payload)
    except Exception as exc:
        logger.warning("failed to load score rules, using fallback", extra={"score_rules_path": str(path), "error": str(exc)})
        return _normalize_rules(FALLBACK_RULES)


@lru_cache(maxsize=8)
def load_signal_rules(path: Path = SIGNAL_RULES_PATH) -> SignalRules:
    try:
        payload = _load_yaml_mapping(path)
        return _normalize_signal_rules(payload)
    except Exception as exc:
        logger.warning(
            "failed to load signal rules, using fallback",
            extra={"signal_rules_path": str(path), "error": str(exc)},
        )
        return _normalize_signal_rules(FALLBACK_SIGNAL_RULES)


def normalize_signal_type(signal_type: str | None, signal_rules: SignalRules | None = None) -> str:
    active_signal_rules = signal_rules or load_signal_rules()
    normalized = str(signal_type or "unknown").strip().lower()
    return normalized if normalized in active_signal_rules.signal_types else "unknown"


def _score_value(payload: dict | None, key: str, fallback: float) -> float:
    if not isinstance(payload, dict):
        return float(fallback or 0)
    try:
        return float(payload.get(key, fallback) or 0)
    except (TypeError, ValueError):
        return float(fallback or 0)


def _source_score(source_profile: dict | None) -> float:
    if not isinstance(source_profile, dict):
        return 0.0
    try:
        return max(0.0, min(15.0, float(source_profile.get("score") or 0)))
    except (TypeError, ValueError):
        return 0.0


def _context_score(watchlist_category: str | None, watchlist_priority: int | None) -> float:
    try:
        priority_score = float(watchlist_priority or 0)
    except (TypeError, ValueError):
        priority_score = 0.0
    category_bonus = 1.0 if watchlist_category else 0.0
    return max(0.0, min(10.0, priority_score + category_bonus))


def _matched_terms(text: str, rules: dict[str, float]) -> list[str]:
    return [term for term in rules if term.lower() in text]


def _normalize_rules(payload: dict[str, Any]) -> ScoreRules:
    keywords = _number_mapping(payload.get("keywords", {}))
    risk_keywords = _number_mapping(payload.get("risk_keywords", {}))
    limits = _number_mapping(payload.get("limits", {}))

    return ScoreRules(
        keywords=keywords,
        risk_keywords=risk_keywords,
        keyword_bonus_max=limits.get("keyword_bonus_max", 20),
        risk_penalty_max=limits.get("risk_penalty_max", 40),
        final_score_min=limits.get("final_score_min", 0),
        final_score_max=limits.get("final_score_max", 100),
    )


def _normalize_signal_rules(payload: dict[str, Any]) -> SignalRules:
    raw_signal_types = payload.get("signal_types", {})
    limits = _number_mapping(payload.get("limits", {}))
    signal_types: dict[str, SignalTypeRule] = {}

    if isinstance(raw_signal_types, dict):
        for key, value in raw_signal_types.items():
            if not isinstance(value, dict):
                continue
            signal_types[str(key).lower()] = SignalTypeRule(
                bonus=float(value.get("bonus", 0)),
                label=str(value.get("label", key)),
            )

    if "unknown" not in signal_types:
        signal_types["unknown"] = SignalTypeRule(bonus=0, label="未知")

    return SignalRules(
        signal_types=signal_types,
        signal_bonus_max=limits.get("signal_bonus_max", 15),
    )


def _number_mapping(value: Any) -> dict[str, float]:
    if not isinstance(value, dict):
        return {}
    return {str(key).lower(): float(weight) for key, weight in value.items()}


def _load_yaml_mapping(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(path)

    try:
        import yaml

        payload = yaml.safe_load(path.read_text(encoding="utf-8"))
        if not isinstance(payload, dict):
            raise ValueError("score rules YAML root must be a mapping")
        return payload
    except ModuleNotFoundError:
        return _parse_simple_yaml_mapping(path.read_text(encoding="utf-8"))


def _parse_simple_yaml_mapping(content: str) -> dict[str, dict[str, float]]:
    payload: dict[str, Any] = {}
    current_section: str | None = None
    current_nested_key: str | None = None

    for raw_line in content.splitlines():
        line = raw_line.rstrip()
        if not line.strip() or line.lstrip().startswith("#"):
            continue
        if not line.startswith(" ") and line.endswith(":"):
            current_section = line[:-1].strip()
            current_nested_key = None
            payload[current_section] = {}
            continue
        if current_section and line.startswith("  ") and not line.startswith("    ") and line.endswith(":"):
            current_nested_key = line.strip()[:-1]
            payload[current_section][current_nested_key] = {}
            continue
        if current_section and current_nested_key and line.startswith("    ") and ":" in line:
            key, value = line.strip().split(":", 1)
            payload[current_section][current_nested_key][key.strip()] = _parse_scalar(value.strip())
            continue
        if current_section and line.startswith("  ") and ":" in line:
            current_nested_key = None
            key, value = line.strip().rsplit(":", 1)
            payload[current_section][key.strip()] = _parse_scalar(value.strip())

    if not payload:
        raise ValueError("score rules YAML is empty")
    return payload


def _parse_scalar(value: str) -> Any:
    stripped = value.strip()
    if stripped.startswith(("\"", "'")) and stripped.endswith(("\"", "'")):
        return stripped[1:-1]
    try:
        return float(stripped)
    except ValueError:
        return stripped
