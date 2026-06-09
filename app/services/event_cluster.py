from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from hashlib import sha256
import re
from typing import Iterable

from app.core.logging import get_logger
from app.services.duplicates import text_similarity


logger = get_logger(__name__)
EVENT_LOOKBACK_HOURS = 48
EVENT_MATCH_THRESHOLD = 0.68
TITLE_SIMILARITY_THRESHOLD = 0.82
URL_RE = re.compile(r"https?://\S+")
SPACE_RE = re.compile(r"\s+")
PUNCT_RE = re.compile(r"[^\w\u4e00-\u9fff]+", re.UNICODE)
NUMBER_RE = re.compile(r"\d+(?:\.\d+)?\s*(?:万|亿|m|M|k|K|万美元|美元|枚|个|%|％)?")
SYMBOL_RE = re.compile(r"(?<![A-Za-z0-9])[$#]?([A-Za-z][A-Za-z0-9_]{1,24})(?![A-Za-z0-9])")

GENERIC_SYMBOLS = {
    "airdrop",
    "alpha",
    "api",
    "chain",
    "crypto",
    "daily",
    "defi",
    "event",
    "market",
    "news",
    "official",
    "project",
    "protocol",
    "token",
    "update",
    "vault",
    "vaults",
    "web3",
}

KNOWN_PROJECTS = {
    "humanity",
    "humanityprotocol",
    "piggybank",
    "lab",
    "base",
    "coinbase",
    "binance",
    "okx",
    "bybit",
    "kraken",
    "bitstamp",
    "virtuals",
    "bankr",
    "clanker",
    "zora",
    "farcaster",
    "hyperliquid",
    "kalshi",
    "arkham",
    "blackrock",
    "grayscale",
    "cypherpunk",
    "cypherpunks",
    "arthur",
    "hayes",
}

KNOWN_TOKENS = {
    "h",
    "btc",
    "eth",
    "sol",
    "zec",
    "usdt",
    "usdc",
    "bnb",
    "lab",
    "base",
}

GENERIC_ASSET_TOKENS = {
    "btc",
    "eth",
    "sol",
    "usdt",
    "usdc",
    "bnb",
}

KEY_PHRASE_ALIASES = {
    "private_key_leak": ("私钥泄露", "私钥泄漏", "private key leak", "private key leaked"),
    "stolen_funds": ("被盗", "盗取", "黑客", "stolen", "theft", "drain"),
    "token_mint": ("铸造", "增发", "mint", "minted"),
    "selloff": ("抛售", "砸盘", "套现", "selloff", "sell-off", "dump"),
    "price_crash": ("暴跌", "归零", "闪崩", "crash", "plunge"),
    "proxy_admin": ("代理管理员", "管理员权限", "proxy admin"),
    "dex_cex_gap": ("价差", "链上价格", "永续价", "perp price", "price gap"),
    "funding_rate": ("资金费率", "funding rate"),
    "market_maker": ("做市", "market maker", "market making"),
    "insider": ("监守自盗", "自导自演", "提前归集", "insider"),
    "short_loss": ("做空亏", "做空气亏", "空亏", "short loss", "short_loss"),
    "short": ("做空", "short"),
    "loss": ("亏损", "亏", "loss"),
    "close_position": ("平仓", "清仓", "close position", "liquidat"),
    "vault_nav": ("vault", "vaults", "净值", "nav"),
    "exploit": ("漏洞", "攻击", "exploit", "hack"),
    "infinite_mint": ("无限增发", "无限印钞", "增发", "infinite mint"),
    "listing": ("上线", "上币", "listing"),
    "snapshot": ("快照", "snapshot"),
    "claim": ("领取", "申领", "claim"),
    "funding": ("融资", "funding", "raise"),
    "partnership": ("合作", "partnership"),
    "exchange_flow": ("转入", "存入", "转出", "转账", "转至", "存入", "提现", "deposit", "withdraw", "transfer"),
    "buy": ("买入", "增持", "bought", "buy"),
    "sell": ("卖出", "减持", "sold", "sell"),
}

ENTITY_ALIASES = {
    "blackrock": ("贝莱德", "blackrock", "blackrock相关", "贝莱德关联"),
    "coinbase": ("coinbase", "coin base"),
    "binance": ("币安", "binance"),
    "okx": ("欧易", "okx"),
    "bybit": ("bybit",),
    "grayscale": ("灰度", "grayscale"),
}

EXCHANGE_ENTITIES = {"coinbase", "binance", "okx", "bybit", "kraken", "bitstamp"}


@dataclass(frozen=True)
class EventMatch:
    event_id: int
    is_new_event: bool
    event_similarity: float
    event_match_reason: str


@dataclass(frozen=True)
class EventCandidateScore:
    event: object
    similarity: float
    reason: str


@dataclass(frozen=True)
class EventFeatures:
    entities: set[str]
    projects: set[str]
    tokens: set[str]
    numbers: set[str]
    key_phrases: set[str]

    def as_dict(self) -> dict:
        return {
            "entities": sorted(self.entities),
            "projects": sorted(self.projects),
            "tokens": sorted(self.tokens),
            "numbers": sorted(self.numbers),
            "key_phrases": sorted(self.key_phrases),
        }


@dataclass(frozen=True)
class EventMatchDetail:
    event: object
    title_similarity: float
    summary_similarity: float
    raw_text_similarity: float
    entity_overlap: list[str]
    token_overlap: list[str]
    number_overlap: list[str]
    key_phrase_overlap: list[str]
    entity_overlap_score: float
    token_overlap_score: float
    number_overlap_score: float
    time_distance_hours: float | None
    final_match_score: float
    match_threshold: float
    matched: bool
    reason: str

    def as_dict(self) -> dict:
        return {
            "event_id": int(getattr(self.event, "id")),
            "event_title": getattr(self.event, "event_title", None),
            "event_summary": getattr(self.event, "event_summary", None),
            "title_similarity": self.title_similarity,
            "summary_similarity": self.summary_similarity,
            "raw_text_similarity": self.raw_text_similarity,
            "entity_overlap": self.entity_overlap,
            "token_overlap": self.token_overlap,
            "number_overlap": self.number_overlap,
            "key_phrase_overlap": self.key_phrase_overlap,
            "entity_overlap_score": self.entity_overlap_score,
            "token_overlap_score": self.token_overlap_score,
            "number_overlap_score": self.number_overlap_score,
            "time_distance_hours": self.time_distance_hours,
            "final_match_score": self.final_match_score,
            "match_threshold": self.match_threshold,
            "would_match": self.matched,
            "matched": self.matched,
            "reason": self.reason,
        }


class EventClusterer:
    def __init__(self, repository) -> None:
        self.repository = repository

    async def match_or_create(self, event_title: str, event_summary: str, message_text: str, message=None) -> EventMatch:
        normalized_title = normalize_event_title(event_title) or normalize_event_title(event_summary) or "未知事件"
        since = datetime.now(timezone.utc) - timedelta(hours=EVENT_LOOKBACK_HOURS)
        candidates = await self.repository.recent_events(since=since)
        details = rank_event_candidates(
            normalized_title,
            event_summary,
            message_text,
            candidates,
            message_created_at=getattr(message, "created_at", None) if message else None,
        )
        best = details[0] if details else None
        matched = best if best and best.matched else None
        log_event_match_diagnostics(
            message=message,
            event_title=normalized_title,
            event_summary=event_summary,
            message_text=message_text,
            details=details,
            matched=matched,
        )
        if matched:
            return EventMatch(
                event_id=int(getattr(matched.event, "id")),
                is_new_event=False,
                event_similarity=matched.final_match_score,
                event_match_reason=matched.reason,
            )

        event_key = deterministic_event_key(normalized_title, event_summary, message_text)
        event = await self.repository.create_event(
            event_key=event_key,
            event_title=normalized_title,
            event_summary=event_summary,
        )
        was_created = bool(getattr(event, "_was_created", True))
        return EventMatch(
            event_id=int(getattr(event, "id")),
            is_new_event=was_created,
            event_similarity=1.0 if was_created else 0.99,
            event_match_reason="new_event" if was_created else "existing_event_key_conflict",
        )


def best_event_match(
    event_title: str,
    event_summary: str,
    message_text: str,
    candidates: Iterable[object],
) -> EventCandidateScore | None:
    details = rank_event_candidates(event_title, event_summary, message_text, candidates)
    if not details or not details[0].matched:
        return None
    best = details[0]
    return EventCandidateScore(event=best.event, similarity=best.final_match_score, reason=best.reason)


def rank_event_candidates(
    event_title: str,
    event_summary: str,
    message_text: str,
    candidates: Iterable[object],
    message_created_at: datetime | None = None,
    limit: int | None = None,
) -> list[EventMatchDetail]:
    current_text = " ".join(part for part in (event_title, event_summary, message_text) if part)
    current_summary_text = " ".join(part for part in (event_title, event_summary) if part)
    current_features = extract_event_features(current_text)
    details = [
        score_event_candidate(
            event_title,
            event_summary,
            message_text,
            current_summary_text,
            current_features,
            event,
            message_created_at=message_created_at,
        )
        for event in candidates
    ]
    details.sort(key=lambda item: (item.matched, item.final_match_score, item.summary_similarity), reverse=True)
    return details[:limit] if limit else details


def score_event_candidate(
    event_title: str,
    event_summary: str,
    message_text: str,
    current_summary_text: str,
    current_features: EventFeatures,
    event: object,
    message_created_at: datetime | None = None,
) -> EventMatchDetail:
    candidate_title = getattr(event, "event_title", "") or ""
    candidate_summary = getattr(event, "event_summary", "") or ""
    candidate_latest_summary = getattr(event, "latest_summary", "") or ""
    candidate_text = " ".join(part for part in (candidate_title, candidate_summary, candidate_latest_summary) if part)
    candidate_features = extract_event_features(candidate_text)

    entity_overlap = sorted(
        (current_features.entities | current_features.projects | current_features.tokens)
        & (candidate_features.entities | candidate_features.projects | candidate_features.tokens)
    )
    token_overlap = sorted((current_features.key_phrases | current_features.tokens) & (candidate_features.key_phrases | candidate_features.tokens))
    number_overlap = sorted(_number_overlap(current_features.numbers, candidate_features.numbers))
    key_phrase_overlap = sorted(current_features.key_phrases & candidate_features.key_phrases)
    entity_overlap_score = overlap_score(
        current_features.entities | current_features.projects | current_features.tokens,
        candidate_features.entities | candidate_features.projects | candidate_features.tokens,
    )
    token_overlap_score = overlap_score(current_features.key_phrases | current_features.tokens, candidate_features.key_phrases | candidate_features.tokens)
    number_overlap_score = number_overlap_score_fn(current_features.numbers, candidate_features.numbers)
    title_similarity = round(text_similarity(event_title, candidate_title), 4)
    summary_similarity = round(text_similarity(current_summary_text, candidate_text), 4)
    raw_text_similarity = round(text_similarity(message_text, candidate_text), 4)
    time_distance_hours = _time_distance_hours(message_created_at, getattr(event, "last_seen_at", None))
    final_score = round(
        min(
            1.0,
            0.30 * summary_similarity
            + 0.25 * entity_overlap_score
            + 0.20 * token_overlap_score
            + 0.15 * raw_text_similarity
            + 0.10 * number_overlap_score,
        ),
        4,
    )

    strong_match = _strong_match(
        entity_overlap=entity_overlap,
        token_overlap=token_overlap,
        number_overlap=number_overlap,
        key_phrase_overlap=key_phrase_overlap,
        time_distance_hours=time_distance_hours,
    )
    humanity_security_match = _humanity_security_context_match(
        entity_overlap=entity_overlap,
        current_key_phrases=current_features.key_phrases,
        candidate_key_phrases=candidate_features.key_phrases,
        time_distance_hours=time_distance_hours,
    )
    strong_match = strong_match or humanity_security_match
    matched = strong_match or final_score >= EVENT_MATCH_THRESHOLD or title_similarity >= TITLE_SIMILARITY_THRESHOLD
    if humanity_security_match:
        reason = "strong_humanity_security_context_match"
        final_score = max(final_score, 0.92)
    elif strong_match:
        reason = "strong_entity_token_time_match"
        final_score = max(final_score, 0.92)
    elif final_score >= EVENT_MATCH_THRESHOLD:
        reason = "multi_factor_score"
    elif title_similarity >= TITLE_SIMILARITY_THRESHOLD:
        reason = "title_similarity"
        final_score = max(final_score, title_similarity)
    else:
        reason = not_matched_reason(
            entity_overlap=entity_overlap,
            token_overlap=token_overlap,
            number_overlap=number_overlap,
            final_score=final_score,
            time_distance_hours=time_distance_hours,
        )

    return EventMatchDetail(
        event=event,
        title_similarity=title_similarity,
        summary_similarity=summary_similarity,
        raw_text_similarity=raw_text_similarity,
        entity_overlap=entity_overlap,
        token_overlap=token_overlap,
        number_overlap=number_overlap,
        key_phrase_overlap=key_phrase_overlap,
        entity_overlap_score=round(entity_overlap_score, 4),
        token_overlap_score=round(token_overlap_score, 4),
        number_overlap_score=round(number_overlap_score, 4),
        time_distance_hours=round(time_distance_hours, 2) if time_distance_hours is not None else None,
        final_match_score=round(min(final_score, 1.0), 4),
        match_threshold=EVENT_MATCH_THRESHOLD,
        matched=matched,
        reason=reason,
    )


def extract_event_features(text: str) -> EventFeatures:
    normalized = URL_RE.sub(" ", text or "")
    lower = normalized.lower()
    compact = normalize_event_title(normalized).lower()
    symbols = {
        symbol.lower()
        for symbol in SYMBOL_RE.findall(normalized)
        if len(symbol) >= 2 and symbol.lower() not in GENERIC_SYMBOLS
    }
    aliased_entities = _alias_entities(lower, compact)
    symbols.update(aliased_entities)
    projects = {symbol for symbol in symbols if symbol in KNOWN_PROJECTS}
    tokens = {symbol for symbol in symbols if symbol in KNOWN_TOKENS or symbol.isupper()}
    entities = set(projects) | set(tokens)
    for symbol in symbols:
        if len(symbol) >= 3 and symbol not in GENERIC_SYMBOLS:
            entities.add(symbol)
    if _contains_humanity_h_token(normalized):
        entities.update({"humanity", "h"})
        projects.add("humanity")
        tokens.add("h")
    key_phrases = {
        normalized_key
        for normalized_key, aliases in KEY_PHRASE_ALIASES.items()
        if any(alias.lower() in lower or alias.lower() in compact for alias in aliases)
    }
    numbers = {_normalize_number(match.group(0)) for match in NUMBER_RE.finditer(normalized)}
    return EventFeatures(
        entities=entities,
        projects=projects,
        tokens=tokens,
        numbers={number for number in numbers if number},
        key_phrases=key_phrases,
    )


def _alias_entities(lower_text: str, compact_text: str) -> set[str]:
    entities: set[str] = set()
    for normalized_entity, aliases in ENTITY_ALIASES.items():
        if any(alias.lower() in lower_text or alias.lower() in compact_text for alias in aliases):
            entities.add(normalized_entity)
    return entities


def event_features(text: str) -> dict[str, set[str]]:
    features = extract_event_features(text)
    return {
        "entities": features.entities,
        "tokens": features.tokens | features.key_phrases,
        "actions": features.key_phrases,
        "amounts": features.numbers,
        "projects": features.projects,
        "numbers": features.numbers,
        "key_phrases": features.key_phrases,
    }


def normalize_event_title(title: str | None) -> str:
    if not title:
        return ""
    normalized = URL_RE.sub(" ", title).strip()
    normalized = SPACE_RE.sub("", normalized)
    normalized = normalized.rstrip("。.!！")
    return normalized[:120]


def deterministic_event_key(event_title: str, event_summary: str, message_text: str, created_at: datetime | None = None) -> str:
    full_text = " ".join(part for part in (event_title, event_summary, message_text) if part)
    features = extract_event_features(full_text)
    parts = [
        *sorted(features.projects or features.entities)[:4],
        *sorted(features.tokens - features.projects)[:3],
        *sorted(features.key_phrases)[:4],
        *_event_action_signature(full_text, features),
    ]
    if not parts:
        return event_key_for_title(event_title)
    bucket = _time_bucket(created_at)
    return sha256("|".join([*parts, bucket]).encode("utf-8")).hexdigest()


def _event_action_signature(text: str, features: EventFeatures) -> list[str]:
    """Keep deterministic keys from merging different institutional flow actions."""
    lower = text.lower()
    compact = normalize_event_title(text).lower()
    signature: list[str] = []
    if "exchange_flow" in features.key_phrases:
        if any(term in lower or term in compact for term in ("转入", "存入", "转至", "deposit", "transfer to")):
            signature.append("flow_in")
        elif any(term in lower or term in compact for term in ("转出", "提现", "withdraw")):
            signature.append("flow_out")
        else:
            signature.append("flow")
    if "sell" in features.key_phrases:
        signature.append("sell")
    if "buy" in features.key_phrases:
        signature.append("buy")
    if {"buy", "sell"} <= features.key_phrases:
        signature.append("rebalance")
    if signature and features.numbers:
        signature.extend(f"num:{number}" for number in sorted(features.numbers)[:3])
    return signature


def event_key_for_title(title: str) -> str:
    normalized = PUNCT_RE.sub("", title.lower())
    normalized = SPACE_RE.sub("", normalized)
    return sha256(normalized.encode("utf-8")).hexdigest()


def log_event_match_diagnostics(
    message,
    event_title: str,
    event_summary: str,
    message_text: str,
    details: list[EventMatchDetail],
    matched: EventMatchDetail | None,
) -> None:
    message_features = extract_event_features(" ".join(part for part in (event_title, event_summary, message_text) if part))
    base = {
        "message_id": getattr(message, "source_message_id", None) or getattr(message, "id", None),
        "source_channel": getattr(message, "source_chat_title", None) or getattr(message, "source_chat_id", None),
        "message_entities": sorted(message_features.entities),
        "message_numbers": sorted(message_features.numbers),
        "match_threshold": EVENT_MATCH_THRESHOLD,
        "matched": bool(matched),
        "not_matched_reason": None if matched else (details[0].reason if details else "no_candidate_events"),
    }
    if matched:
        logger.info(
            "event cluster matched",
            extra={**base, **_candidate_log_payload(matched)},
        )
        return
    logger.info(
        "event cluster no match",
        extra={
            **base,
            "top_candidates": [_candidate_log_payload(detail) for detail in details[:5]],
        },
    )


def overlap_score(left: set[str], right: set[str]) -> float:
    if not left or not right:
        return 0.0
    return len(left & right) / max(1, min(len(left), len(right)))


def number_overlap_score_fn(left: set[str], right: set[str]) -> float:
    if not left or not right:
        return 0.0
    return len(_number_overlap(left, right)) / max(1, min(len(left), len(right)))


def _number_overlap(left: set[str], right: set[str]) -> set[str]:
    overlap = set(left & right)
    for left_value in left:
        for right_value in right:
            if _numbers_close(left_value, right_value):
                overlap.add(left_value if len(left_value) <= len(right_value) else right_value)
    return overlap


def _numbers_close(left: str, right: str) -> bool:
    if left == right:
        return True
    left_number, left_suffix = _number_parts(left)
    right_number, right_suffix = _number_parts(right)
    if left_number is None or right_number is None:
        return False
    if left_suffix != right_suffix:
        return False
    larger = max(abs(left_number), abs(right_number))
    if larger == 0:
        return True
    return abs(left_number - right_number) / larger <= 0.02


def _number_parts(value: str) -> tuple[float | None, str]:
    suffix = "%" if value.endswith("%") else ""
    raw = value[:-1] if suffix else value
    try:
        return float(raw), suffix
    except ValueError:
        return None, suffix


def not_matched_reason(
    entity_overlap: list[str],
    token_overlap: list[str],
    number_overlap: list[str],
    final_score: float,
    time_distance_hours: float | None,
) -> str:
    if time_distance_hours is not None and time_distance_hours > 48:
        return "time_distance_too_large"
    if not entity_overlap:
        return "no_core_entity_overlap"
    if not token_overlap and not number_overlap:
        return "no_key_token_or_number_overlap"
    return f"score_below_threshold:{round(final_score, 4)}"


def _strong_match(
    entity_overlap: list[str],
    token_overlap: list[str],
    number_overlap: list[str],
    key_phrase_overlap: list[str],
    time_distance_hours: float | None,
) -> bool:
    if time_distance_hours is not None and time_distance_hours > 24:
        return False
    entity_set = set(entity_overlap)
    key_phrase_set = set(key_phrase_overlap)
    important_entities = entity_set - GENERIC_ASSET_TOKENS

    if {"piggybank", "lab"}.issubset(entity_set) and (
        "short_loss" in key_phrase_set
        or {"short", "loss"} <= key_phrase_set
        or "vault_nav" in key_phrase_set
        or "loss" in key_phrase_set
    ):
        return True
    if "zec" in entity_set and {"exploit", "infinite_mint"} & key_phrase_set:
        return True
    if {"humanity", "h"} & entity_set and _has_humanity_security_overlap(key_phrase_set):
        return True
    if "exchange_flow" in key_phrase_set:
        return _institution_exchange_flow_match(entity_set, key_phrase_set, number_overlap)
    if len(important_entities) >= 2 and (key_phrase_set or number_overlap):
        return True
    if len(important_entities) >= 1 and len(key_phrase_set) >= 2:
        return True
    return False


def _institution_exchange_flow_match(entity_set: set[str], key_phrase_set: set[str], number_overlap: list[str]) -> bool:
    if "exchange_flow" not in key_phrase_set:
        return False
    if not number_overlap:
        return False
    has_exchange = bool(entity_set & EXCHANGE_ENTITIES)
    has_asset = bool(entity_set & GENERIC_ASSET_TOKENS)
    non_generic_entities = entity_set - GENERIC_ASSET_TOKENS - EXCHANGE_ENTITIES
    return has_exchange and has_asset and bool(non_generic_entities)


def _contains_humanity_h_token(text: str) -> bool:
    lower = text.lower()
    if "humanity" in lower:
        return True
    return bool(
        re.search(r"(?<![A-Za-z0-9])\$H(?![A-Za-z0-9])", text)
        or re.search(r"(?<![A-Za-z0-9])H(?:代币|鏈上|链上|全网|合约|永续|资金费率)", text)
    )


def _has_humanity_security_overlap(key_phrase_set: set[str]) -> bool:
    security_phrases = {
        "private_key_leak",
        "stolen_funds",
        "token_mint",
        "selloff",
        "price_crash",
        "proxy_admin",
        "dex_cex_gap",
        "funding_rate",
        "market_maker",
        "insider",
        "exploit",
        "hack",
        "infinite_mint",
    }
    return bool(key_phrase_set & security_phrases)


def _humanity_security_context_match(
    entity_overlap: list[str],
    current_key_phrases: set[str],
    candidate_key_phrases: set[str],
    time_distance_hours: float | None,
) -> bool:
    if time_distance_hours is not None and time_distance_hours > 24:
        return False
    if not ({"humanity", "h"} & set(entity_overlap)):
        return False
    return _has_humanity_security_overlap(current_key_phrases) and _has_humanity_security_overlap(candidate_key_phrases)


def _time_distance_hours(left: datetime | None, right: datetime | None) -> float | None:
    if not left or not right:
        return None
    if left.tzinfo is None:
        left = left.replace(tzinfo=timezone.utc)
    if right.tzinfo is None:
        right = right.replace(tzinfo=timezone.utc)
    return abs((left - right).total_seconds()) / 3600


def _time_bucket(created_at: datetime | None = None) -> str:
    value = created_at or datetime.now(timezone.utc)
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.strftime("%Y%m%d")


def _normalize_number(value: str) -> str:
    raw = value.strip().lower().replace("％", "%")
    multiplier = 1.0
    if "亿" in raw:
        multiplier = 100_000_000
    elif "万" in raw:
        multiplier = 10_000
    elif "m" in raw:
        multiplier = 1_000_000
    elif "k" in raw:
        multiplier = 1_000
    numeric_match = re.search(r"\d+(?:\.\d+)?", raw)
    if not numeric_match:
        return ""
    number = float(numeric_match.group(0)) * multiplier
    suffix = "%" if "%" in raw else ""
    if number >= 1_000_000:
        normalized = str(int(round(number / 100_000)) * 100_000)
    elif number >= 10_000:
        normalized = str(int(round(number / 1_000)) * 1_000)
    elif number >= 100:
        normalized = str(int(round(number / 10)) * 10)
    else:
        normalized = str(int(number) if number.is_integer() else round(number, 2))
    return normalized + suffix


def _candidate_log_payload(detail: EventMatchDetail) -> dict:
    payload = detail.as_dict()
    return {
        "candidate_event_id": payload["event_id"],
        "candidate_event_title": payload["event_title"],
        "candidate_event_summary": payload["event_summary"],
        "title_similarity": payload["title_similarity"],
        "summary_similarity": payload["summary_similarity"],
        "raw_text_similarity": payload["raw_text_similarity"],
        "entity_overlap": payload["entity_overlap"],
        "token_overlap": payload["token_overlap"],
        "number_overlap": payload["number_overlap"],
        "time_distance_hours": payload["time_distance_hours"],
        "final_match_score": payload["final_match_score"],
        "match_threshold": payload["match_threshold"],
        "matched": payload["matched"],
        "not_matched_reason": None if payload["matched"] else payload["reason"],
        "reason": payload["reason"],
    }
