from __future__ import annotations

from dataclasses import dataclass
import re
from typing import Iterable


DUPLICATE_SIMILARITY_THRESHOLD = 0.90
STRICT_FULLTEXT_DUPLICATE_THRESHOLD = 0.97
EVENT_SIMILARITY_THRESHOLD = 0.82
SUMMARY_TEXT_DUPLICATE_THRESHOLD = 0.68
URL_RE = re.compile(r"https?://\S+")
SPACE_RE = re.compile(r"\s+")
PUNCT_RE = re.compile(r"[^\w\u4e00-\u9fff]+", re.UNICODE)
NUMBER_RE = re.compile(r"\d+(?:\.\d+)?")
ENTITY_RE = re.compile(r"\b[a-z][a-z0-9_]{4,}\b")

IMPORTANT_TERMS = {
    "btc",
    "eth",
    "sol",
    "zec",
    "etf",
    "coinbase",
    "binance",
    "blackrock",
    "贝莱德",
    "富达",
    "灰度",
    "现货",
    "比特币",
    "以太坊",
    "净流出",
    "流出",
    "流入",
    "转入",
    "存入",
    "提现",
    "卖出",
    "买入",
    "攻击",
    "黑客",
    "漏洞",
    "增发",
    "无限增发",
    "暴跌",
    "清仓",
    "供应",
    "安全",
    "验证",
    "形式化验证",
    "回应",
    "空投",
    "积分",
    "申领",
    "上线",
    "交易对",
    "巨鲸",
    "融资",
    "估值",
    "投资",
    "核聚变",
    "商业化",
    "openai",
    "altman",
    "arthur",
    "hayes",
    "cypherpunk",
    "cypherpunks",
    "mtgox",
    "mt gox",
    "bitstamp",
    "kraken",
    "kalshi",
    "hyperliquid",
    "grayscale",
    "bitget",
    "poolx",
    "saylor",
    "strategy",
    "openai",
    "altman",
}

EVENT_ACTION_TERMS = {
    "攻击",
    "黑客",
    "漏洞",
    "增发",
    "无限增发",
    "暴跌",
    "清仓",
    "供应",
    "安全",
    "验证",
    "形式化验证",
    "回应",
    "上线",
    "融资",
    "投资",
    "流出",
    "流入",
    "转入",
    "存入",
    "提现",
    "卖出",
    "买入",
}

GENERIC_ENTITY_STOPWORDS = {
    "about",
    "after",
    "alpha",
    "chain",
    "crypto",
    "daily",
    "first",
    "market",
    "markets",
    "official",
    "points",
    "protocol",
    "source",
    "token",
    "trading",
    "update",
}

STRONG_ENTITY_TERMS = {
    "blackrock",
    "贝莱德",
    "富达",
    "灰度",
    "grayscale",
    "coinbase",
    "binance",
    "mtgox",
    "mt gox",
    "bitstamp",
    "kraken",
    "kalshi",
    "hyperliquid",
    "bitget",
    "poolx",
    "saylor",
    "strategy",
    "zec",
    "arthur",
    "hayes",
    "cypherpunk",
    "cypherpunks",
}


@dataclass(frozen=True)
class DuplicateMatch:
    possible_duplicate: bool
    duplicate_of_message_id: int | None
    similarity_score: float | None


@dataclass(frozen=True)
class DuplicateBackfillMatch:
    message_id: int
    duplicate_of_message_id: int
    similarity_score: float
    summary: str | None
    duplicate_summary: str | None


@dataclass(frozen=True)
class DuplicateBackfillResult:
    scanned_count: int
    matched_count: int
    threshold: float
    dry_run: bool
    samples: list[DuplicateBackfillMatch]
    matches: list[DuplicateBackfillMatch]


NO_DUPLICATE = DuplicateMatch(
    possible_duplicate=False,
    duplicate_of_message_id=None,
    similarity_score=None,
)


def find_possible_duplicate(
    text: str,
    candidates: Iterable[object],
    summary: str | None = None,
    category: str | None = None,
) -> DuplicateMatch:
    current_event_text = " ".join(part for part in (summary, text) if part)
    current_signature = event_signature(current_event_text)
    best_message_id: int | None = None
    best_score = 0.0

    for candidate in candidates:
        candidate_text = getattr(candidate, "cleaned_text", "") or getattr(candidate, "raw_text", "")
        candidate_summary = getattr(candidate, "summary_zh", None)
        candidate_category = getattr(candidate, "category", None)
        candidate_id = getattr(candidate, "id", None)
        candidate_event_text = " ".join(part for part in (candidate_summary, candidate_text) if part)
        candidate_signature = event_signature(candidate_event_text)
        if candidate_id is not None and current_signature and current_signature == candidate_signature:
            return DuplicateMatch(
                possible_duplicate=True,
                duplicate_of_message_id=int(candidate_id),
                similarity_score=0.995,
            )
        score = duplicate_similarity(
            text,
            candidate_text,
            left_summary=summary,
            right_summary=candidate_summary,
            same_category=bool(category and candidate_category and category == candidate_category),
        )
        if candidate_id is not None and score > best_score:
            best_message_id = int(candidate_id)
            best_score = score

    if best_message_id is None or best_score <= EVENT_SIMILARITY_THRESHOLD:
        return NO_DUPLICATE
    return DuplicateMatch(
        possible_duplicate=True,
        duplicate_of_message_id=best_message_id,
        similarity_score=round(best_score, 4),
    )


def backfill_possible_duplicates(
    messages: Iterable[object],
    threshold: float,
    dry_run: bool,
    sample_limit: int = 20,
) -> DuplicateBackfillResult:
    previous_messages: list[object] = []
    matches: list[DuplicateBackfillMatch] = []

    for message in messages:
        best_previous = None
        best_score = 0.0
        message_text = _message_text(message)

        for previous in previous_messages:
            score = duplicate_similarity(
                message_text,
                _message_text(previous),
                left_summary=getattr(message, "summary_zh", None),
                right_summary=getattr(previous, "summary_zh", None),
                same_category=bool(
                    getattr(message, "category", None)
                    and getattr(previous, "category", None)
                    and getattr(message, "category", None) == getattr(previous, "category", None)
                ),
            )
            if score > best_score:
                best_previous = previous
                best_score = score

        if best_previous is not None and best_score >= threshold:
            matches.append(
                DuplicateBackfillMatch(
                    message_id=int(getattr(message, "id")),
                    duplicate_of_message_id=int(getattr(best_previous, "id")),
                    similarity_score=round(best_score, 4),
                    summary=getattr(message, "summary_zh", None),
                    duplicate_summary=getattr(best_previous, "summary_zh", None),
                )
            )

        previous_messages.append(message)

    return DuplicateBackfillResult(
        scanned_count=len(previous_messages),
        matched_count=len(matches),
        threshold=threshold,
        dry_run=dry_run,
        samples=matches[:sample_limit],
        matches=matches,
    )


def apply_backfill_marks(
    messages: Iterable[object],
    result: DuplicateBackfillResult,
) -> int:
    if result.dry_run:
        return 0

    messages_by_id = {int(getattr(message, "id")): message for message in messages}
    for message in messages_by_id.values():
        setattr(message, "possible_duplicate", False)
        setattr(message, "duplicate_of_message_id", None)
        setattr(message, "similarity_score", None)

    changed_count = 0
    for match in result.matches:
        message = messages_by_id.get(match.message_id)
        if message is None:
            continue
        setattr(message, "possible_duplicate", True)
        setattr(message, "duplicate_of_message_id", match.duplicate_of_message_id)
        setattr(message, "similarity_score", match.similarity_score)
        changed_count += 1
    return changed_count


def text_similarity(left: str, right: str) -> float:
    left_tokens = _fingerprints(left)
    right_tokens = _fingerprints(right)
    if not left_tokens or not right_tokens:
        return 0.0
    if left_tokens == right_tokens:
        return 1.0

    intersection = len(left_tokens & right_tokens)
    union = len(left_tokens | right_tokens)
    return round(intersection / union, 4) if union else 0.0


def duplicate_similarity(
    left: str,
    right: str,
    left_summary: str | None = None,
    right_summary: str | None = None,
    same_category: bool = False,
) -> float:
    full_text_score = text_similarity(left, right)
    if full_text_score >= STRICT_FULLTEXT_DUPLICATE_THRESHOLD:
        return full_text_score

    summary_text_score = 0.0
    if left_summary and right_summary:
        summary_text_score = text_similarity(left_summary, right_summary)

    if left_summary and right_summary:
        left_event_text = left_summary
        right_event_text = right_summary
    else:
        left_event_text = " ".join(part for part in (left_summary, left) if part)
        right_event_text = " ".join(part for part in (right_summary, right) if part)
    event_score = event_similarity(left_event_text, right_event_text)
    strong_signal_bonus = _strong_signal_bonus(left_event_text, right_event_text)
    event_score = min(1.0, event_score + strong_signal_bonus)
    if same_category and event_score >= 0.68:
        event_score += 0.04
    if summary_text_score >= SUMMARY_TEXT_DUPLICATE_THRESHOLD and _has_core_signal_overlap(left_event_text, right_event_text):
        return round(min(1.0, max(event_score, summary_text_score + 0.14)), 4)
    if full_text_score >= 0.66 and _has_core_signal_overlap(left_event_text, right_event_text):
        return round(min(1.0, max(event_score, full_text_score + 0.12)), 4)
    return round(min(event_score, 1.0), 4)


def event_similarity(left: str, right: str) -> float:
    left_tokens = _event_tokens(left)
    right_tokens = _event_tokens(right)
    if not left_tokens or not right_tokens:
        return 0.0
    left_anchor_tokens = {token for token in left_tokens if not token.startswith("num:")}
    right_anchor_tokens = {token for token in right_tokens if not token.startswith("num:")}
    shared_anchor_tokens = left_anchor_tokens & right_anchor_tokens
    shared_anchor_count = len(shared_anchor_tokens)
    shared_strong_entities = shared_anchor_tokens & STRONG_ENTITY_TERMS
    shared_numbers = {
        token for token in left_tokens & right_tokens
        if token.startswith("num:")
    }
    shared_dynamic_entities = {token for token in shared_anchor_tokens if token.startswith("entity:")}
    has_entity_number_match = bool(shared_dynamic_entities and shared_numbers)
    has_anchor_number_match = shared_anchor_count >= 2 and len(shared_numbers) >= 1
    has_entity_action_match = _has_entity_action_match(left_tokens, right_tokens)
    has_strong_text_match = bool(shared_strong_entities) and _char_ngram_similarity(left, right) >= 0.42
    if has_entity_action_match or has_strong_text_match:
        char_score = _char_ngram_similarity(left, right)
        action_overlap = _shared_action_terms(left_tokens, right_tokens)
        entity_overlap = _shared_entity_terms(left_tokens, right_tokens)
        if len(entity_overlap) >= 2 and action_overlap and char_score >= 0.32:
            return round(max(char_score + 0.36, 0.86), 4)
        if entity_overlap and len(action_overlap) >= 2 and char_score >= 0.36:
            return round(max(char_score + 0.34, 0.84), 4)
        if shared_strong_entities and action_overlap:
            return round(max(char_score + 0.34, 0.82), 4)
    if shared_anchor_count < 3 and not has_entity_number_match:
        if not has_anchor_number_match:
            return 0.0
    if not shared_strong_entities and not (shared_anchor_count >= 3 and shared_numbers) and not has_entity_number_match:
        if not (has_anchor_number_match and _char_ngram_similarity(left, right) >= 0.58):
            return 0.0

    char_score = _char_ngram_similarity(left, right)
    if has_anchor_number_match and char_score >= 0.6:
        return round(max(char_score + 0.18, 0.84), 4)
    if has_entity_number_match and char_score >= 0.55:
        return round(max(char_score + 0.16, 0.82), 4)
    if not shared_strong_entities and not (shared_anchor_count >= 3 and shared_numbers) and not has_entity_number_match:
        return 0.0

    intersection = len(left_tokens & right_tokens)
    smaller = min(len(left_tokens), len(right_tokens))
    containment = intersection / smaller if smaller else 0.0
    jaccard = intersection / len(left_tokens | right_tokens)
    return round(max(jaccard, containment * 0.92), 4)


def event_signature(text: str) -> str | None:
    tokens = _event_tokens(text)
    if not tokens:
        return None
    anchors = sorted(token for token in tokens if not token.startswith("num:"))
    numbers = sorted(token for token in tokens if token.startswith("num:"))
    if len(anchors) < 2:
        return None
    if not numbers:
        entities = sorted(_sharedable_entity_terms(tokens))
        actions = sorted(_action_terms(tokens))
        if not entities or len(actions) < 2:
            return None
        return "|".join([*entities[:4], *actions[:4]])
    return "|".join([*anchors[:6], *numbers[:4]])


def _message_text(message: object) -> str:
    return getattr(message, "cleaned_text", "") or getattr(message, "raw_text", "") or ""


def _char_ngram_similarity(left: str, right: str) -> float:
    return text_similarity(left, right)


def _has_core_signal_overlap(left: str, right: str) -> bool:
    left_tokens = _event_tokens(left)
    right_tokens = _event_tokens(right)
    shared_numbers = {token for token in left_tokens & right_tokens if token.startswith("num:")}
    shared_anchors = {
        token for token in left_tokens & right_tokens
        if not token.startswith("num:")
    }
    return bool((len(shared_anchors) >= 2 and shared_numbers) or (shared_anchors & STRONG_ENTITY_TERMS))


def _strong_signal_bonus(left: str, right: str) -> float:
    left_tokens = _event_tokens(left)
    right_tokens = _event_tokens(right)
    shared_numbers = {
        token for token in left_tokens & right_tokens
        if token.startswith("num:")
    }
    shared_strong_entities = {
        token for token in (left_tokens & right_tokens)
        if token in STRONG_ENTITY_TERMS
    }
    shared_other_anchors = {
        token for token in (left_tokens & right_tokens)
        if not token.startswith("num:") and token not in STRONG_ENTITY_TERMS
    }

    bonus = 0.0
    if shared_strong_entities and shared_numbers:
        bonus += 0.12
    elif len(shared_other_anchors) >= 2 and shared_numbers:
        bonus += 0.08
    elif len(shared_numbers) >= 2 and len(shared_other_anchors) >= 1:
        bonus += 0.06
    elif _has_entity_action_match(left_tokens, right_tokens):
        bonus += 0.10
    return bonus


def _fingerprints(text: str) -> set[str]:
    normalized = _normalize(text)
    if not normalized:
        return set()
    if len(normalized) <= 3:
        return {normalized}
    return {normalized[index : index + 3] for index in range(len(normalized) - 2)}


def _normalize(text: str) -> str:
    text = URL_RE.sub(" ", text.lower())
    text = PUNCT_RE.sub(" ", text)
    return SPACE_RE.sub("", text).strip()


def _event_tokens(text: str) -> set[str]:
    normalized = URL_RE.sub(" ", text.lower())
    tokens: set[str] = set()
    compact = _normalize(text)
    for term in IMPORTANT_TERMS:
        if _term_matches(term.lower(), normalized, compact):
            tokens.add(term.lower())
    for number in NUMBER_RE.findall(normalized):
        tokens.add(f"num:{_normalize_number(number)}")
    for entity in ENTITY_RE.findall(normalized):
        if entity not in GENERIC_ENTITY_STOPWORDS:
            tokens.add(f"entity:{entity}")
    return tokens


def _action_terms(tokens: set[str]) -> set[str]:
    return {token for token in tokens if token in EVENT_ACTION_TERMS}


def _sharedable_entity_terms(tokens: set[str]) -> set[str]:
    entities = {token for token in tokens if token.startswith("entity:")}
    entities |= tokens & STRONG_ENTITY_TERMS
    return entities


def _shared_entity_terms(left_tokens: set[str], right_tokens: set[str]) -> set[str]:
    return _sharedable_entity_terms(left_tokens) & _sharedable_entity_terms(right_tokens)


def _shared_action_terms(left_tokens: set[str], right_tokens: set[str]) -> set[str]:
    return _action_terms(left_tokens) & _action_terms(right_tokens)


def _has_entity_action_match(left_tokens: set[str], right_tokens: set[str]) -> bool:
    shared_entities = _shared_entity_terms(left_tokens, right_tokens)
    shared_actions = _shared_action_terms(left_tokens, right_tokens)
    shared_strong_entities = shared_entities & STRONG_ENTITY_TERMS
    if shared_strong_entities and shared_actions:
        return True
    return bool(shared_entities and shared_actions and (len(shared_entities) >= 2 or len(shared_actions) >= 2))


def _normalize_number(value: str) -> str:
    try:
        number = float(value)
    except ValueError:
        return value
    if number >= 1000:
        return str(int(round(number / 100)) * 100)
    if number >= 100:
        return str(int(round(number / 10)) * 10)
    return str(int(number))


def _term_matches(term: str, normalized_text: str, compact_text: str) -> bool:
    if re.fullmatch(r"[a-z0-9]{1,4}", term):
        return re.search(rf"(?<![a-z0-9]){re.escape(term)}(?![a-z0-9])", normalized_text) is not None
    return term in normalized_text or term in compact_text
