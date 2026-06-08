import hashlib
import re
from dataclasses import dataclass


SPACE_RE = re.compile(r"\s+")
URL_RE = re.compile(r"https?://\S+")


@dataclass
class CleanedMessage:
    cleaned_text: str
    dedup_key: str
    language: str


def clean_message(text: str) -> CleanedMessage:
    normalized = URL_RE.sub("", text).strip().lower()
    normalized = SPACE_RE.sub(" ", normalized)
    dedup_key = hashlib.sha256(normalized.encode("utf-8")).hexdigest()
    language = "zh" if re.search(r"[\u4e00-\u9fff]", normalized) else "en"
    return CleanedMessage(cleaned_text=normalized, dedup_key=dedup_key, language=language)
