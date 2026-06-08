import logging


LOG_FORMAT = (
    "%(asctime)s | %(levelname)s | %(name)s | %(message)s | "
    "platform=%(platform)s "
    "channel=%(channel)s source_message_id=%(source_message_id)s dedup_key=%(dedup_key)s "
    "channel_id=%(channel_id)s message_id=%(message_id)s "
    "project=%(project)s ecosystem=%(ecosystem)s channel_name=%(channel_name)s event_id=%(event_id)s "
    "score=%(score)s signal_level=%(signal_level)s push_status=%(push_status)s "
    "possible_duplicate=%(possible_duplicate)s duplicate_of_message_id=%(duplicate_of_message_id)s "
    "similarity_score=%(similarity_score)s "
    "rate_limit_reason=%(rate_limit_reason)s score_breakdown=%(score_breakdown)s"
)


class ContextDefaultsFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        for field in (
            "channel",
            "platform",
            "channel_id",
            "message_id",
            "project",
            "ecosystem",
            "channel_name",
            "event_id",
            "source_message_id",
            "dedup_key",
            "score",
            "signal_level",
            "push_status",
            "possible_duplicate",
            "duplicate_of_message_id",
            "similarity_score",
            "rate_limit_reason",
            "score_breakdown",
        ):
            if not hasattr(record, field):
                setattr(record, field, None)
        return True


def configure_logging(level: str) -> None:
    logging.basicConfig(level=level.upper(), format=LOG_FORMAT)
    for handler in logging.getLogger().handlers:
        handler.addFilter(ContextDefaultsFilter())


def get_logger(name: str) -> logging.Logger:
    return logging.getLogger(name)
