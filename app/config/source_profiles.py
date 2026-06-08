from __future__ import annotations

import re
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from app.core.logging import get_logger


logger = get_logger(__name__)
SOURCE_PROFILES_PATH = Path("config/source_profiles.yaml")


@dataclass(frozen=True)
class SourceProfile:
    key: str
    label: str
    role: str
    ecosystem: str
    specialty: tuple[str, ...]
    importance: int
    description: str
    score: float = 0.0

    def as_dict(self) -> dict:
        return {
            "key": self.key,
            "label": self.label,
            "role": self.role,
            "ecosystem": self.ecosystem,
            "importance": self.importance,
            "specialty": list(self.specialty),
            "description": self.description,
            # Backward-compatible debug score for the existing scorer path.
            "score": self.score,
        }

    def api_dict(self) -> dict:
        return {
            "label": self.label,
            "role": self.role,
            "ecosystem": self.ecosystem,
            "importance": self.importance,
            "specialty": list(self.specialty),
        }


@lru_cache(maxsize=4)
def load_source_profiles(path: Path = SOURCE_PROFILES_PATH) -> dict[str, SourceProfile]:
    try:
        payload = _load_yaml_mapping(path)
    except Exception as exc:
        logger.warning("failed to load source profiles", extra={"source_profiles_path": str(path), "error": str(exc)})
        return {}

    raw_profiles = payload.get("sources", payload.get("source_profiles", payload))
    if not isinstance(raw_profiles, dict):
        return {}

    profiles: dict[str, SourceProfile] = {}
    for key, value in raw_profiles.items():
        if not isinstance(value, dict):
            continue
        normalized_key = normalize_source_key(str(key))
        if not normalized_key:
            continue
        importance = _safe_int(value.get("importance"), _safe_int(value.get("score"), 0))
        profiles[normalized_key] = SourceProfile(
            key=normalized_key,
            label=str(value.get("label") or key),
            role=str(value.get("role") or "unknown"),
            ecosystem=str(value.get("ecosystem") or "unknown"),
            specialty=tuple(_string_list(value.get("specialty"))),
            importance=importance,
            description=str(value.get("description") or ""),
            score=float(value.get("score", importance) or 0),
        )
    return profiles


def match_source_profile(source_context: dict | None, profiles: dict[str, SourceProfile] | None = None) -> SourceProfile | None:
    active_profiles = profiles if profiles is not None else load_source_profiles()
    default_profile = active_profiles.get("default")
    for candidate in _profile_candidates(source_context or {}):
        normalized = normalize_source_key(candidate)
        if normalized in active_profiles:
            return active_profiles[normalized]
    return default_profile


def normalize_source_key(value: str | None) -> str:
    if not value:
        return ""
    normalized = str(value).strip()
    normalized = normalized.removeprefix("rss:")
    normalized = normalized.removeprefix("@")
    if normalized.lower().startswith("twitter @"):
        normalized = normalized.split("@", 1)[1]
    return re.sub(r"[^a-z0-9_]", "", normalized.lower())


def api_source_profile(profile: dict | None) -> dict | None:
    if not isinstance(profile, dict):
        return None
    return {
        "label": profile.get("label") or profile.get("key"),
        "role": profile.get("role") or "unknown",
        "ecosystem": profile.get("ecosystem") or "unknown",
        "importance": int(profile.get("importance") or profile.get("score") or 0),
        "specialty": _string_list(profile.get("specialty")),
    }


def _profile_candidates(source_context: dict) -> list[str]:
    candidates: list[str] = []
    for key in ("source_handle", "author_name", "project", "channel", "channel_id"):
        value = source_context.get(key)
        if value:
            candidates.append(str(value))
            candidates.append(f"rss:{value}")
            url_account = _account_from_url(str(value))
            if url_account:
                candidates.extend([url_account, f"rss:{url_account}", f"Twitter @{url_account}"])

    channel = str(source_context.get("channel") or "")
    if "@" in channel:
        candidates.append(channel.split("@", 1)[1].split()[0])
    if ":" in channel:
        candidates.extend(part for part in channel.split(":") if part)
    return candidates


def _account_from_url(value: str) -> str | None:
    parsed = urlparse(value)
    if not parsed.path:
        return None
    parts = [part for part in parsed.path.split("/") if part]
    if len(parts) >= 3 and parts[-3:-1] == ["twitter", "user"]:
        return parts[-1]
    if len(parts) >= 2 and parts[-2] == "user":
        return parts[-1]
    return None


def _load_yaml_mapping(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(path)
    import yaml

    payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("source profiles YAML root must be a mapping")
    return payload


def _string_list(value) -> list[str]:
    if isinstance(value, (list, tuple)):
        return [str(item) for item in value if str(item)]
    if value in (None, ""):
        return []
    return [str(value)]


def _safe_int(value, default: int) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default
