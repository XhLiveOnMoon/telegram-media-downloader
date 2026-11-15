import pytest

from app.links import parse_message_link


def test_parse_public_message_link():
    parsed = parse_message_link("https://t.me/example_channel/123")
    assert parsed.kind == "public"
    assert parsed.username == "example_channel"
    assert parsed.message_id == 123


def test_parse_private_message_link():
    parsed = parse_message_link("https://t.me/c/123456789/42")
    assert parsed.kind == "private"
    assert parsed.channel_id == 123456789
    assert parsed.message_id == 42


def test_parse_short_public_link():
    parsed = parse_message_link("t.me/example_channel/7")
    assert parsed.message_id == 7


@pytest.mark.parametrize("value", ["", "https://example.com/a/1", "https://t.me/example_channel"])
def test_reject_invalid_links(value):
    with pytest.raises(ValueError):
        parse_message_link(value)
