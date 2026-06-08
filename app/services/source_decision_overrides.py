from __future__ import annotations

from typing import Any


ASTER_LISTING_PROMO_REASON = (
    "Aster 上币/合约上线绑定交易积分倍数属于 listing 促销，不属于用户要跟踪的独立活动或低成本撸毛机会。"
)


def apply_source_decision_overrides(
    text: str,
    analysis: dict[str, Any],
    *,
    source_profile: dict[str, Any] | None = None,
) -> dict[str, Any]:
    if _is_aster_listing_trading_points_promo(text, source_profile):
        analysis["decision"] = "ignore"
        analysis["confidence"] = max(float(analysis.get("confidence") or 0), 90.0)
        analysis["relevance"] = "low"
        analysis["actionability"] = "none"
        analysis["urgency"] = "low"
        analysis["user_value_summary"] = "Aster 上币或新合约上线的交易积分促销，对当前策略没有跟踪价值。"
        analysis["action_suggestion"] = "none"
        analysis["reason"] = ASTER_LISTING_PROMO_REASON
        analysis["decision_override"] = {
            "applied": True,
            "rule": "aster_listing_trading_points_promo",
            "reason": ASTER_LISTING_PROMO_REASON,
        }
    return analysis


def _is_aster_listing_trading_points_promo(text: str, source_profile: dict[str, Any] | None) -> bool:
    if not source_profile or str(source_profile.get("key") or "").lower() != "aster_dex":
        return False
    lower_text = text.lower()
    listing_terms = (
        "上线",
        "即将上线",
        "launch",
        "listing",
        "listed",
        "go live",
        "live on",
        "perp",
        "perpetual",
        "futures",
        "永续",
        "合约",
        "交易对",
    )
    trading_terms = ("交易", "trade", "trading", "volume")
    reward_terms = ("积分", "points", "xp", "倍", "multiplier", "boost", "rewards", "奖励")
    return (
        any(term in lower_text for term in listing_terms)
        and any(term in lower_text for term in trading_terms)
        and any(term in lower_text for term in reward_terms)
    )
