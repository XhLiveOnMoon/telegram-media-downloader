from __future__ import annotations

import re
from dataclasses import dataclass
from urllib.parse import urlparse

_PUBLIC = re.compile(r"^/(?:(?:s)/)?(?P<username>[A-Za-z0-9_]{4,})/(?P<message_id>\d+)/?$")
_PRIVATE = re.compile(r"^/c/(?P<channel_id>\d+)/(?P<message_id>\d+)/?$")


@dataclass(frozen=True, slots=True)
class MessageLink:
    kind: str
    message_id: int
    username: str | None = None
    channel_id: int | None = None


def parse_message_link(value: str) -> MessageLink:
    """Parse public t.me/<name>/<id> and private t.me/c/<id>/<id> links."""
    raw = value.strip().strip("<>")
    if not raw:
        raise ValueError("Please provide a Telegram message link.")
    parsed = urlparse(raw if "://" in raw else f"https://{raw}")
    host = (parsed.hostname or "").lower()
    if host not in {"t.me", "www.t.me", "telegram.me", "www.telegram.me"}:
        raise ValueError("Only t.me message links are supported.")

    path = parsed.path.rstrip("/")
    private = _PRIVATE.fullmatch(path)
    if private:
        return MessageLink(
            kind="private",
            channel_id=int(private.group("channel_id")),
            message_id=int(private.group("message_id")),
        )

    public = _PUBLIC.fullmatch(path)
    if public:
        username = public.group("username")
        if username.lower() in {"joinchat", "share", "addstickers", "proxy", "login"}:
            raise ValueError("This is not a channel or group message link.")
        return MessageLink(
            kind="public", username=username, message_id=int(public.group("message_id"))
        )
    raise ValueError(
        "Unsupported link. Use t.me/channel/message_id or t.me/c/channel_id/message_id."
    )
