from __future__ import annotations

import re
from dataclasses import dataclass
from urllib.parse import urlparse

_PUBLIC = re.compile(r"^/(?:(?:s)/)?(?P<username>[A-Za-z0-9_]{4,})/(?P<message_id>[1-9]\d*)/?$")
_PRIVATE = re.compile(r"^/c/(?P<channel_id>[1-9]\d*)/(?P<message_id>[1-9]\d*)/?$")
_ALLOWED_HOSTS = {"t.me", "www.t.me", "telegram.me", "www.telegram.me"}


@dataclass(frozen=True, slots=True)
class MessageLink:
    kind: str
    message_id: int
    username: str | None = None
    channel_id: int | None = None


def parse_message_link(value: str) -> MessageLink:
    """Parse canonical HTTPS Telegram public and private message links."""
    raw = value.strip().strip("<>")
    if not raw:
        raise ValueError("Please provide a Telegram message link.")

    # Scheme-less Telegram links are accepted as HTTPS, but explicit non-HTTPS
    # schemes, credentials, ports, and fragments are rejected.
    candidate = raw if "://" in raw else f"https://{raw}"
    try:
        parsed = urlparse(candidate)
        port = parsed.port
    except ValueError as exc:
        raise ValueError("Invalid Telegram message link.") from exc

    host = (parsed.hostname or "").lower()
    if (
        parsed.scheme.lower() != "https"
        or host not in _ALLOWED_HOSTS
        or parsed.netloc.lower() != host
        or parsed.username is not None
        or parsed.password is not None
        or port is not None
        or parsed.fragment
    ):
        raise ValueError("Only canonical HTTPS t.me message links are supported.")

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
