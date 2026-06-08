from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Any

from app.core.logging import get_logger


logger = get_logger(__name__)
USER_PROFILE_PATH = Path("config/user_profile.yaml")


FALLBACK_USER_PROFILE = {
    "user_goals": [
        "关注 Base 生态空投、积分、Season、Builder 激励",
        "关注低成本可交互机会",
        "关注交易相关信号，如巨鲸、CEX上币、资金流、合约异动",
        "不关心普通宏观新闻、美股IPO、泛AI公司新闻，除非直接影响加密市场或链上机会",
    ],
    "push_if": [
        "用户可以采取明确行动",
        "与 Base 或重点撸毛生态直接相关",
        "存在 claim、snapshot、deadline、testnet、points、season、whitelist",
        "官方/核心人物释放重要暗示",
    ],
    "do_not_push_if": [
        "普通新闻，无行动价值",
        "媒体重复报道",
        "信息过于模糊且来源不重要",
        "纯喊单、100x、gem、无依据 FOMO",
    ],
    "ai_decision_principles": [
        "对融资/合作不要一刀切，要判断是否带来未发币、TGE、空投、生态激励、交易叙事、重点机构背书等后续可跟踪机会。",
        "融资/合作如果短期不能行动但具备后续跟踪价值，应输出 watch，而不是 ignore。",
    ],
    "decision_levels": {
        "push": "强提醒推送，重点关注",
        "watch": "观察提醒，也会推送，用于后续跟踪",
        "ignore": "只入库",
    },
}


@dataclass(frozen=True)
class UserProfile:
    user_goals: tuple[str, ...]
    push_if: tuple[str, ...]
    do_not_push_if: tuple[str, ...]
    ai_decision_principles: tuple[str, ...]
    decision_levels: dict[str, str]

    def as_dict(self) -> dict:
        return {
            "user_goals": list(self.user_goals),
            "push_if": list(self.push_if),
            "do_not_push_if": list(self.do_not_push_if),
            "ai_decision_principles": list(self.ai_decision_principles),
            "decision_levels": dict(self.decision_levels),
        }

    def to_prompt_text(self) -> str:
        sections = [
            ("用户目标", self.user_goals),
            ("应该推送", self.push_if),
            ("不应推送", self.do_not_push_if),
            ("AI决策原则", self.ai_decision_principles),
        ]
        lines: list[str] = []
        for title, items in sections:
            lines.append(f"{title}:")
            lines.extend(f"- {item}" for item in items)
        lines.append("决策等级:")
        lines.extend(f"- {key}: {value}" for key, value in self.decision_levels.items())
        return "\n".join(lines)


@lru_cache(maxsize=4)
def load_user_profile(path: Path = USER_PROFILE_PATH) -> UserProfile:
    try:
        payload = _load_yaml_mapping(path)
    except Exception as exc:
        logger.warning("failed to load user profile, using fallback", extra={"user_profile_path": str(path), "error": str(exc)})
        payload = FALLBACK_USER_PROFILE
    return _normalize_user_profile(payload)


def _normalize_user_profile(payload: dict[str, Any]) -> UserProfile:
    return UserProfile(
        user_goals=tuple(_string_list(payload.get("user_goals"))),
        push_if=tuple(_string_list(payload.get("push_if"))),
        do_not_push_if=tuple(_string_list(payload.get("do_not_push_if"))),
        ai_decision_principles=tuple(_string_list(payload.get("ai_decision_principles"))),
        decision_levels={
            str(key): str(value)
            for key, value in (payload.get("decision_levels") if isinstance(payload.get("decision_levels"), dict) else {}).items()
        }
        or dict(FALLBACK_USER_PROFILE["decision_levels"]),
    )


def _string_list(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    return [str(item).strip() for item in value if str(item).strip()]


def _load_yaml_mapping(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(path)
    import yaml

    payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("user profile YAML root must be a mapping")
    return payload
