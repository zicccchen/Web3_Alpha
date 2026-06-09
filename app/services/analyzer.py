import asyncio
import json
import re

try:
    from anthropic import AsyncAnthropic
except ModuleNotFoundError:
    AsyncAnthropic = None

try:
    from openai import AsyncOpenAI
except ModuleNotFoundError:
    AsyncOpenAI = None

from app.core.config import get_settings
from app.core.logging import get_logger
from app.services.scorer import normalize_signal_type
from app.services.user_profile import load_user_profile


PROMPT = """
你是一个 Web3 Alpha 研究员。请阅读消息并输出 JSON，格式如下：
{
  "event_title": "稳定、简短的事件标题，用于同一事件聚合，例如 ZEC无限增发漏洞事件",
  "summary_zh": "不超过80字的中文总结",
  "category": "请从 Alpha机会 / 上线上币 / 融资合作 / 安全风险 / 治理提案 / 宏观市场 / 其他 中选择",
  "signal_type": "请从 airdrop / points_program / claim / snapshot / testnet / wallet_tge / exchange_listing / token_unlock / funding / hack / exploit / partnership / governance / macro / ipo / market_news / unknown 中选择",
  "content_score": 0-75之间的数字,
  "source_score": 0-15之间的数字,
  "context_score": 0-10之间的数字,
  "signal_score": 0-15之间的数字,
  "risk_penalty": 0-40之间的数字,
  "importance_score": 0-100之间的兼容字段，建议等于 content_score + signal_score - risk_penalty,
  "decision": "push / watch / ignore 三选一",
  "confidence": 0-100之间的数字,
  "user_value_summary": "这条消息对用户的价值",
  "action_suggestion": "用户可以做什么，如果没有则写 none",
  "urgency": "low / medium / high 三选一",
  "relevance": "low / medium / high 三选一",
  "actionability": "none / low / medium / high 四选一",
  "risk_level": "low / medium / high 三选一",
  "reason": "用中文简要说明评分原因"
}
AI Push Decision 规则：
- 推送主逻辑以后以 decision 为准，不以 final_score 为准。
- push：用户需要马上知道，通常有明确行动、强相关、较高确定性、官方/核心来源、交易或交互窗口。
- watch：有一定价值但不急、不够明确、需要观察或等待更多确认。
- ignore：普通新闻、重复报道、泛宏观/泛美股/普通IPO/普通融资、低质量喊单或与用户目标无关。
- 如果没有明确行动且来源不重要，优先 watch 或 ignore。
- 如果内容符合用户画像中的 push_if，且风险不高，优先 push。
评分规则：
- content_score：只评价消息内容本身的重要性、可操作性、确定性和时效性，不看关键词堆砌。
- source_score：结合 source_profile 的可信度和角色；如果来源上下文未提供 source_profile，则给 0。
- context_score：结合 watchlist_category 和 watchlist_priority；核心 watchlist、明确生态上下文可更高。
- signal_score：结合 signal_type 的 Alpha 属性；空投/领取/积分/安全事件高，普通市场资讯低。
- risk_penalty：传言、未证实、诈骗诱导、缺少明确项目/代币、低质量喊单需要扣分。
- 不要因为单纯出现 airdrop、listing、funding 等关键词就给高分；关键词只能作为辅助参考。
event_title 规则：
- 事件标题应优先包含核心项目/代币/机构 + 核心事件动作。
- 同一事件的不同媒体报道应尽量输出相同标题。
- 不要把次要人物动作作为标题，例如 ZEC 漏洞事件不要写成 Arthur Hayes清仓。
- 示例：ZEC无限增发漏洞事件、贝莱德ETF资金流出事件、某项目融资事件。
summary_zh 规则：
- 总结必须优先写清“目标项目/协议/代币/机构 + 事件动作”，例如“Solstice开放空投领取”。
- 如果原文没有明确目标项目/协议/代币/机构，不要臆测；总结开头必须写“未说明具体项目/代币：...”。
- 来源频道、Discord项目名、Telegram频道名只能作为信息来源，不要把它们误当成事件主体。
- 缺少明确项目/协议/代币/机构的空投、融资、发币、积分消息，content_score 最高 35，Final Score 应低于 60，除非存在明确安全风险或重大市场影响。
signal_type 规则：
- 空投、领取、积分、测试网、钱包/TGE 等适合撸毛的机会优先识别为 airdrop / claim / points_program / testnet / wallet_tge。
- 交易所上币、代币解锁、安全事件、融资、治理分别归入对应类型。
- IPO、pre-IPO、普通美股资讯归入 ipo 或 market_news。
- 油价、利率、宏观、普通市场快讯归入 macro 或 market_news。
只输出 JSON，不要输出其他内容。
""".strip()

EVENT_UPDATE_PROMPT = """
你是 Web3 Alpha 事件编辑。请判断一条已归入现有 Event 的后续消息是否值得再次推送。
核心原则：
同一个 event 的后续消息，只有出现“新增事实”时，才允许判断为 major 或 critical。
如果没有新增事实，即使语气更强、标题更夸张、来源不同、媒体重复报道，也只能是 none 或 minor，不能触发二次推送。

新增事实定义：
- 官方确认
- 交易所动作
- 项目方回应
- 新的时间窗口
- 新的 claim/snapshot/deadline
- 新的资金变化
- 新的链上证据
- 新的风险处置方案
- 新的攻击/漏洞细节
- 新的交易机会或操作建议

不是新增事实：
- 同一事实换一种说法
- 另一家媒体重复报道
- 标题更夸张
- 摘要更完整但核心信息没变
- 只是补充背景
- 价格小幅变化但没有新因果
- 来源增加但没有新内容

只输出 JSON：
{
  "upgrade_level": "none|minor|major|critical",
  "has_new_facts": true,
  "new_facts": ["新增事实1"],
  "is_substantive_update": true,
  "should_push_upgrade": true,
  "reason": "中文说明",
  "push_summary": "如果需要再次推送，写一句话摘要；否则写 none"
}

硬规则：
- 如果 has_new_facts=false，upgrade_level 只能是 none 或 minor。
- 如果 new_facts 为空，should_push_upgrade 必须是 false。
- 只有 has_new_facts=true 且 new_facts 非空，才允许 major/critical。
- 只有 major/critical 且 should_push_upgrade=true，才允许二次推送。
- 重复报道、换标题、换来源、补背景，一律 should_push_upgrade=false。
只输出 JSON，不要输出其他内容。
""".strip()


logger = get_logger(__name__)
settings = get_settings()


class AIAnalyzer:
    def __init__(self) -> None:
        self.provider = settings.ai_provider.lower()
        self.openai_client = (
            AsyncOpenAI(api_key=settings.openai_api_key, base_url=settings.openai_base_url)
            if settings.openai_api_key and AsyncOpenAI
            else None
        )
        self.anthropic_client = (
            AsyncAnthropic(api_key=settings.anthropic_api_key) if settings.anthropic_api_key and AsyncAnthropic else None
        )

    async def analyze(self, text: str, source_context: dict | None = None) -> dict:
        analysis_text = build_analysis_input(text, source_context, user_profile=load_user_profile().as_dict())
        last_error: str | None = None
        backoffs = {1: 1, 2: 3}
        for attempt in range(1, 4):
            try:
                if self.provider == "anthropic":
                    return await self._analyze_with_anthropic(analysis_text)
                return await self._analyze_with_openai(analysis_text)
            except Exception as exc:
                last_error = str(exc)
                logger.exception("ai analysis failed", extra={"attempt": attempt})
                if attempt in backoffs:
                    await asyncio.sleep(backoffs[attempt])

        return {
            "event_title": text[:40] or "未知事件",
            "summary_zh": text[:80],
            "category": "其他",
            "signal_type": "unknown",
            "content_score": 50,
            "source_score": 0,
            "context_score": 0,
            "signal_score": 0,
            "risk_penalty": 0,
            "importance_score": 50,
            "decision": "watch",
            "confidence": 0,
            "user_value_summary": "AI 分析失败，建议观察",
            "action_suggestion": "none",
            "urgency": "low",
            "relevance": "low",
            "actionability": "none",
            "risk_level": "medium",
            "reason": "AI 分析失败，已重试并使用降级结果",
            "ai_error": last_error,
        }

    async def analyze_event_update(
        self,
        *,
        event_title: str,
        event_summary: str | None,
        latest_summary: str | None,
        message_summary: str,
        message_text: str,
    ) -> dict:
        payload = (
            f"现有事件标题：{event_title}\n"
            f"现有事件摘要：{event_summary or ''}\n"
            f"事件最新摘要：{latest_summary or ''}\n"
            f"新消息总结：{message_summary}\n"
            f"新消息原文：{message_text}"
        )
        try:
            if self.provider == "anthropic":
                content = await self._event_update_with_anthropic(payload)
            else:
                content = await self._event_update_with_openai(payload)
            return self._parse_event_update_json(content)
        except Exception:
            logger.exception("event update analysis failed")
            return {
                "event_update_level": "minor",
                "upgrade_level": "minor",
                "has_new_facts": False,
                "new_facts": [],
                "is_substantive_update": False,
                "should_push_upgrade": False,
                "reason": "事件升级判断失败，按保守策略不重复推送",
                "push_summary": "none",
            }

    async def _analyze_with_openai(self, text: str) -> dict:
        if not self.openai_client:
            raise ValueError("OPENAI_API_KEY is not configured")
        response = await self.openai_client.chat.completions.create(
            model=settings.openai_model,
            messages=[
                {"role": "system", "content": PROMPT},
                {"role": "user", "content": text},
            ],
            temperature=0.2,
        )
        content = response.choices[0].message.content or ""
        return self._parse_json(content)

    async def _analyze_with_anthropic(self, text: str) -> dict:
        if not self.anthropic_client:
            raise ValueError("ANTHROPIC_API_KEY is not configured")
        response = await self.anthropic_client.messages.create(
            model=settings.anthropic_model,
            max_tokens=400,
            system=PROMPT,
            messages=[{"role": "user", "content": text}],
        )
        content = "".join(block.text for block in response.content if getattr(block, "text", None))
        return self._parse_json(content)

    async def _event_update_with_openai(self, text: str) -> str:
        if not self.openai_client:
            raise ValueError("OPENAI_API_KEY is not configured")
        response = await self.openai_client.chat.completions.create(
            model=settings.openai_model,
            messages=[
                {"role": "system", "content": EVENT_UPDATE_PROMPT},
                {"role": "user", "content": text},
            ],
            temperature=0.1,
        )
        return response.choices[0].message.content or ""

    async def _event_update_with_anthropic(self, text: str) -> str:
        if not self.anthropic_client:
            raise ValueError("ANTHROPIC_API_KEY is not configured")
        response = await self.anthropic_client.messages.create(
            model=settings.anthropic_model,
            max_tokens=300,
            system=EVENT_UPDATE_PROMPT,
            messages=[{"role": "user", "content": text}],
        )
        return "".join(block.text for block in response.content if getattr(block, "text", None))

    def _parse_json(self, content: str) -> dict:
        normalized = content.strip()
        if normalized.startswith("```"):
            normalized = re.sub(r"^```(?:json)?\s*", "", normalized)
            normalized = re.sub(r"\s*```$", "", normalized).strip()
        elif "{" in normalized and "}" in normalized:
            normalized = normalized[normalized.find("{") : normalized.rfind("}") + 1]
        payload = json.loads(normalized)
        importance_score = float(payload.get("importance_score", payload.get("content_score", 50)))
        return {
            "event_title": payload.get("event_title") or payload.get("summary_zh", "")[:40] or "未知事件",
            "summary_zh": payload.get("summary_zh", ""),
            "category": payload.get("category", "其他"),
            "signal_type": normalize_signal_type(payload.get("signal_type")),
            "content_score": float(payload.get("content_score", importance_score)),
            "source_score": float(payload.get("source_score", 0) or 0),
            "context_score": float(payload.get("context_score", 0) or 0),
            "signal_score": float(payload.get("signal_score", 0) or 0),
            "risk_penalty": float(payload.get("risk_penalty", 0) or 0),
            "importance_score": importance_score,
            "decision": normalize_decision(payload.get("decision")),
            "confidence": _safe_float(payload.get("confidence"), 0),
            "user_value_summary": str(payload.get("user_value_summary") or ""),
            "action_suggestion": str(payload.get("action_suggestion") or "none"),
            "urgency": _normalize_choice(payload.get("urgency"), {"low", "medium", "high"}, "low"),
            "relevance": _normalize_choice(payload.get("relevance"), {"low", "medium", "high"}, "low"),
            "actionability": _normalize_choice(payload.get("actionability"), {"none", "low", "medium", "high"}, "none"),
            "risk_level": _normalize_choice(payload.get("risk_level"), {"low", "medium", "high"}, "medium"),
            "reason": payload.get("reason", ""),
        }

    def _parse_event_update_json(self, content: str) -> dict:
        normalized = content.strip()
        if normalized.startswith("```"):
            normalized = re.sub(r"^```(?:json)?\s*", "", normalized)
            normalized = re.sub(r"\s*```$", "", normalized).strip()
        elif "{" in normalized and "}" in normalized:
            normalized = normalized[normalized.find("{") : normalized.rfind("}") + 1]
        payload = json.loads(normalized)
        level = str(payload.get("upgrade_level") or payload.get("event_update_level") or "minor").lower()
        if level not in {"none", "minor", "major", "critical"}:
            level = "minor"
        new_facts = _string_list(payload.get("new_facts"))
        has_new_facts = bool(payload.get("has_new_facts")) and bool(new_facts)
        should_push_upgrade = bool(payload.get("should_push_upgrade"))
        if not has_new_facts and level in {"major", "critical"}:
            level = "minor"
            should_push_upgrade = False
        if not new_facts:
            should_push_upgrade = False
        if level in {"none", "minor"}:
            should_push_upgrade = False
        return {
            "event_update_level": level,
            "upgrade_level": level,
            "decision": "push" if should_push_upgrade and level in {"major", "critical"} else "ignore",
            "has_new_facts": has_new_facts,
            "new_facts": new_facts,
            "is_substantive_update": bool(payload.get("is_substantive_update")) and has_new_facts,
            "should_push_upgrade": should_push_upgrade,
            "reason": payload.get("reason", ""),
            "push_summary": str(payload.get("push_summary") or "none"),
        }


def build_analysis_input(text: str, source_context: dict | None = None, user_profile: dict | None = None) -> str:
    lines: list[str] = []
    if user_profile:
        lines.append("用户画像：")
        for section in ("user_goals", "push_if", "do_not_push_if", "ai_decision_principles"):
            values = user_profile.get(section)
            if isinstance(values, list):
                lines.append(f"{section}:")
                lines.extend(f"- {item}" for item in values)
        decision_levels = user_profile.get("decision_levels")
        if isinstance(decision_levels, dict):
            lines.append("decision_levels:")
            lines.extend(f"- {key}: {value}" for key, value in decision_levels.items())
        lines.append("")
    if not source_context:
        lines.append("消息正文：")
        lines.append(text)
        return "\n".join(lines)
    lines.append("来源上下文（仅用于判断信息来源，不要当成事件主体）：")
    source_profile = source_context.get("source_profile")
    if isinstance(source_profile, dict):
        lines.append("source_profile_context:")
        lines.append(f"- Handle: {source_profile.get('key') or 'unknown'}")
        lines.append(f"- Label: {source_profile.get('label') or source_profile.get('key') or 'unknown'}")
        lines.append(f"- Role: {source_profile.get('role') or 'unknown'}")
        lines.append(f"- Ecosystem: {source_profile.get('ecosystem') or 'unknown'}")
        lines.append(f"- Importance: {source_profile.get('importance', 0)}")
        specialty = source_profile.get("specialty") or []
        if specialty:
            lines.append("- Specialty:")
            lines.extend(f"  - {item}" for item in specialty)
        description = source_profile.get("description")
        if description:
            lines.append(f"- Description: {description}")
        lines.append("- 注意：来源画像必须作为 Push / Watch / Ignore 的判断上下文，但不能只因为 importance 高就自动 push。")
    fields = [
        ("source_platform", "平台"),
        ("project", "来源项目"),
        ("ecosystem", "生态"),
        ("watchlist_category", "Watchlist分类"),
        ("watchlist_priority", "Watchlist优先级"),
        ("channel", "频道"),
        ("channel_id", "频道ID"),
        ("author_name", "作者"),
    ]
    for key, label in fields:
        value = source_context.get(key)
        if value:
            lines.append(f"- {label}: {value}")
    lines.append("")
    lines.append("消息正文：")
    lines.append(text)
    return "\n".join(lines)


def normalize_decision(value) -> str:
    return _normalize_choice(value, {"push", "watch", "ignore"}, "watch")


def _normalize_choice(value, allowed: set[str], default: str) -> str:
    normalized = str(value or "").strip().lower()
    return normalized if normalized in allowed else default


def _safe_float(value, default: float) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _string_list(value) -> list[str]:
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    if isinstance(value, tuple):
        return [str(item).strip() for item in value if str(item).strip()]
    if value in (None, ""):
        return []
    return [str(value).strip()]
