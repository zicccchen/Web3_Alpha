from dataclasses import dataclass
import json

import httpx

from app.core.config import get_settings
from app.core.logging import get_logger


logger = get_logger(__name__)
settings = get_settings()


@dataclass(frozen=True)
class NotificationResult:
    sent: bool
    error: str | None = None


class FeishuNotifier:
    def __init__(self) -> None:
        self.webhook_url = settings.feishu_webhook_url
        self.app_id = settings.feishu_app_id
        self.app_secret = settings.feishu_app_secret
        self.chat_id = settings.feishu_chat_id

    async def notify(self, payload: dict) -> NotificationResult:
        card = build_feedback_card(payload)
        if self.app_id and self.app_secret and self.chat_id:
            return await self._notify_with_app_bot(card)
        return await self._notify_with_webhook(card)

    async def _notify_with_webhook(self, card: dict) -> NotificationResult:
        if not self.webhook_url:
            logger.warning("feishu notifier is not configured, skipping notification")
            return NotificationResult(sent=False, error="feishu notifier is not configured")
        message = build_webhook_message(card)
        try:
            async with httpx.AsyncClient(timeout=15) as client:
                response = await client.post(self.webhook_url, json=message)
                response.raise_for_status()
            return NotificationResult(sent=True)
        except Exception as exc:
            logger.exception("failed to push feishu notification")
            return NotificationResult(sent=False, error=str(exc))

    async def _notify_with_app_bot(self, card: dict) -> NotificationResult:
        try:
            async with httpx.AsyncClient(timeout=15) as client:
                token_response = await client.post(
                    "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal",
                    json={
                        "app_id": self.app_id,
                        "app_secret": self.app_secret,
                    },
                )
                token_response.raise_for_status()
                token_payload = token_response.json()
                tenant_access_token = token_payload.get("tenant_access_token")
                if not tenant_access_token:
                    raise RuntimeError(f"missing tenant_access_token: {token_payload}")

                response = await client.post(
                    "https://open.feishu.cn/open-apis/im/v1/messages",
                    params={"receive_id_type": "chat_id"},
                    headers={"Authorization": f"Bearer {tenant_access_token}"},
                    json=build_app_bot_message(self.chat_id, card),
                )
                response.raise_for_status()
                response_payload = response.json()
                if response_payload.get("code") not in (0, None):
                    raise RuntimeError(f"feishu app bot send failed: {response_payload}")
            return NotificationResult(sent=True)
        except Exception as exc:
            logger.exception("failed to push feishu app bot notification")
            return NotificationResult(sent=False, error=str(exc))


def build_webhook_message(card: dict) -> dict:
    return {
        "msg_type": "interactive",
        "card": card,
    }


def build_app_bot_message(chat_id: str, card: dict) -> dict:
    return {
        "receive_id": chat_id,
        "msg_type": "interactive",
        "content": json.dumps(card, ensure_ascii=False),
    }


def build_feedback_card(payload: dict) -> dict:
    message_id = payload.get("message_id")
    event_id = payload.get("event_id")
    channel = payload.get("source_chat_title") or payload.get("source_chat_id") or "unknown"
    platform = payload.get("source_platform")
    project = payload.get("source_project")
    ecosystem = payload.get("source_ecosystem")
    source_parts = [str(part) for part in (platform, project, ecosystem) if part]
    source_line = " / ".join(source_parts)
    source_elements = []
    if source_line:
        source_elements.append({"tag": "markdown", "content": f"**来源**：{source_line}"})
    decision = _normalized_card_decision(payload.get("ai_decision"))
    decision_label = _decision_label(decision, event_upgrade=bool(payload.get("event_upgrade")))
    action_suggestion = payload.get("action_suggestion") or "none"
    if decision == "watch" and str(action_suggestion).strip().lower() in {"", "none"}:
        action_suggestion = "建议跟踪"
    return {
        "config": {"wide_screen_mode": True},
        "header": {
            "template": "red" if decision == "push" else "orange",
            "title": {"tag": "plain_text", "content": f"Web3 Alpha 告警 {decision_label}"},
        },
        "elements": [
            {"tag": "markdown", "content": f"**{decision_label}**"},
            *source_elements,
            {"tag": "markdown", "content": f"**频道**：{channel}"},
            {"tag": "markdown", "content": f"**决策**：{decision.title()}"},
            {"tag": "markdown", "content": f"**置信度**：{payload.get('ai_confidence', '')}"},
            {"tag": "markdown", "content": f"**相关性**：{payload.get('relevance', '')}"},
            {"tag": "markdown", "content": f"**可行动性**：{payload.get('actionability', '')}"},
            {"tag": "markdown", "content": f"**紧急度**：{payload.get('urgency', '')}"},
            {"tag": "markdown", "content": f"**ID**：event_id={event_id or 'none'} / message_id={message_id or 'none'}"},
            {"tag": "markdown", "content": f"**分类**：{payload.get('category', '')}"},
            {"tag": "markdown", "content": f"**评分**：{payload.get('score', '')}（调试）"},
            {"tag": "markdown", "content": f"**总结**：{payload.get('summary_zh', '')}"},
            {"tag": "markdown", "content": f"**用户价值摘要**：{payload.get('user_value_summary', '')}"},
            {"tag": "markdown", "content": f"**建议动作**：{action_suggestion}"},
            {"tag": "markdown", "content": f"**原因**：{payload.get('reason', '')}"},
            {
                "tag": "action",
                "actions": [
                    _feedback_button("good", "好", message_id, event_id, "primary"),
                    _feedback_button("bad", "差", message_id, event_id, "danger"),
                    _feedback_button("ignore", "忽略", message_id, event_id, "default"),
                ],
            },
        ],
    }


def _normalized_card_decision(decision) -> str:
    decision = str(decision or "push").strip().lower()
    if decision not in {"push", "watch"}:
        return "watch"
    return decision


def _decision_label(decision: str, *, event_upgrade: bool = False) -> str:
    if event_upgrade:
        return "【事件升级｜🚨 Push】" if decision == "push" else "【事件升级｜👀 Watch】"
    return "【🚨 Push｜重点关注】" if decision == "push" else "【👀 Watch｜观察】"


def _feedback_button(action: str, label: str, message_id, event_id, button_type: str) -> dict:
    return {
        "tag": "button",
        "text": {"tag": "plain_text", "content": label},
        "type": button_type,
        "value": {
            "action": action,
            "message_id": message_id,
            "event_id": event_id,
        },
    }
