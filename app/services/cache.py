from redis.asyncio import Redis

from app.core.config import get_settings


settings = get_settings()


class DedupCache:
    def __init__(self) -> None:
        self.client = Redis.from_url(settings.redis_url, encoding="utf-8", decode_responses=True)

    async def seen(self, dedup_key: str) -> bool:
        return await self.client.exists(f"dedup:{dedup_key}") == 1

    async def mark(self, dedup_key: str) -> None:
        await self.client.setex(f"dedup:{dedup_key}", settings.dedup_ttl_seconds, "1")

    async def close(self) -> None:
        await self.client.aclose()
