from functools import lru_cache
from typing import Annotated

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, NoDecode, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    app_name: str = Field(default="web3-alpha-mvp", alias="APP_NAME")
    app_env: str = Field(default="development", alias="APP_ENV")
    log_level: str = Field(default="INFO", alias="LOG_LEVEL")
    api_host: str = Field(default="0.0.0.0", alias="API_HOST")
    api_port: int = Field(default=8000, alias="API_PORT")
    source_collector: str = Field(default="telegram", alias="SOURCE_COLLECTOR")

    database_url: str = Field(alias="DATABASE_URL")
    redis_url: str = Field(alias="REDIS_URL")
    dedup_ttl_seconds: int = Field(default=86400, alias="DEDUP_TTL_SECONDS")

    telegram_api_id: int | None = Field(default=None, alias="TELEGRAM_API_ID")
    telegram_api_hash: str | None = Field(default=None, alias="TELEGRAM_API_HASH")
    telegram_session_name: str = Field(default="web3_alpha", alias="TELEGRAM_SESSION_NAME")
    telegram_source: str = Field(default="api", alias="TELEGRAM_SOURCE")
    telegram_fetch_limit: int = Field(default=100, alias="TELEGRAM_FETCH_LIMIT")
    telegram_poll_interval_seconds: int = Field(default=60, alias="TELEGRAM_POLL_INTERVAL_SECONDS")
    telegram_session_string: str | None = Field(default=None, alias="TELEGRAM_SESSION_STRING")
    telegram_channels: Annotated[list[str], NoDecode] = Field(default_factory=list, alias="TELEGRAM_CHANNELS")
    public_telegram_channels: Annotated[list[str], NoDecode] = Field(
        default_factory=list,
        alias="PUBLIC_TELEGRAM_CHANNELS",
    )
    public_poll_interval_seconds: int = Field(default=60, alias="PUBLIC_POLL_INTERVAL_SECONDS")
    public_fetch_limit: int = Field(default=20, alias="PUBLIC_FETCH_LIMIT")
    discord_bot_token: str | None = Field(default=None, alias="DISCORD_BOT_TOKEN")
    discord_channel_ids: Annotated[list[str], NoDecode] = Field(default_factory=list, alias="DISCORD_CHANNEL_IDS")
    discord_enabled: bool = Field(default=False, alias="DISCORD_ENABLED")
    discord_mode: str = Field(default="session", alias="DISCORD_MODE")
    discord_poll_interval_seconds: int = Field(default=60, alias="DISCORD_POLL_INTERVAL_SECONDS")
    discord_request_timeout_seconds: int = Field(default=15, alias="DISCORD_REQUEST_TIMEOUT_SECONDS")
    discord_session_token: str | None = Field(default=None, alias="DISCORD_SESSION_TOKEN")
    discord_user_agent: str | None = Field(default=None, alias="DISCORD_USER_AGENT")
    x_feed_enabled: bool = Field(default=False, alias="X_FEED_ENABLED")
    x_feed_mode: str = Field(default="rss", alias="X_FEED_MODE")
    x_feed_urls: Annotated[list[str], NoDecode] = Field(default_factory=list, alias="X_FEED_URLS")
    x_feed_base_url: str = Field(default="http://rsshub:1200/twitter/user", alias="X_FEED_BASE_URL")
    x_feed_poll_interval_seconds: int = Field(default=300, alias="X_FEED_POLL_INTERVAL_SECONDS")
    x_feed_request_timeout_seconds: int = Field(default=15, alias="X_FEED_REQUEST_TIMEOUT_SECONDS")

    ai_provider: str = Field(default="openai", alias="AI_PROVIDER")
    openai_api_key: str | None = Field(default=None, alias="OPENAI_API_KEY")
    openai_base_url: str | None = Field(default=None, alias="OPENAI_BASE_URL")
    openai_model: str = Field(default="gpt-4.1-mini", alias="OPENAI_MODEL")
    anthropic_api_key: str | None = Field(default=None, alias="ANTHROPIC_API_KEY")
    anthropic_model: str = Field(default="claude-3-5-sonnet-latest", alias="ANTHROPIC_MODEL")

    feishu_webhook_url: str | None = Field(default=None, alias="FEISHU_WEBHOOK_URL")
    feishu_app_id: str | None = Field(default=None, alias="FEISHU_APP_ID")
    feishu_app_secret: str | None = Field(default=None, alias="FEISHU_APP_SECRET")
    feishu_chat_id: str | None = Field(default=None, alias="FEISHU_CHAT_ID")
    push_score_threshold: float = Field(default=75, alias="PUSH_SCORE_THRESHOLD")
    push_daily_limit: int = Field(default=30, alias="PUSH_DAILY_LIMIT")
    push_a_level_hourly_limit: int = Field(default=5, alias="PUSH_A_LEVEL_HOURLY_LIMIT")
    push_s_level_hourly_limit: int = Field(default=20, alias="PUSH_S_LEVEL_HOURLY_LIMIT")

    @field_validator(
        "telegram_channels",
        "public_telegram_channels",
        "discord_channel_ids",
        "x_feed_urls",
        mode="before",
    )
    @classmethod
    def split_channels(cls, value: str | list[str] | None) -> list[str]:
        if isinstance(value, list):
            return value
        if not value:
            return []
        return [item.strip() for item in value.split(",") if item.strip()]

    @field_validator("telegram_api_id", mode="before")
    @classmethod
    def empty_int_to_none(cls, value: str | int | None) -> int | None:
        if value == "":
            return None
        return value

    @field_validator(
        "telegram_api_hash",
        "telegram_session_string",
        "discord_bot_token",
        "discord_session_token",
        "discord_user_agent",
        "openai_api_key",
        "anthropic_api_key",
        "feishu_webhook_url",
        "feishu_app_id",
        "feishu_app_secret",
        "feishu_chat_id",
        mode="before",
    )
    @classmethod
    def empty_string_to_none(cls, value: str | None) -> str | None:
        if value == "":
            return None
        return value


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
