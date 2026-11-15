import stat
from pathlib import Path
from types import SimpleNamespace

import pytest
from telethon.tl.types import PeerChannel
from telethon.utils import get_peer_id

from app.downloader import DownloadError, MediaDownloader, TransientDownloadError, media_identity
from app.storage import safe_filename, stable_partial_path


class FakeClient:
    def __init__(self, chunks: list[bytes]):
        self.chunks = chunks
        self.requested_offset: int | None = None

    async def iter_download(self, media, *, offset, request_size, chunk_size):
        self.requested_offset = offset
        for chunk in self.chunks:
            yield chunk


def make_message(size: int = 8192):
    return SimpleNamespace(
        id=42,
        peer_id=PeerChannel(123456),
        media=object(),
        photo=None,
        grouped_id=None,
        document=SimpleNamespace(
            id=9001, size=size, attributes=[SimpleNamespace(file_name="asset.bin")]
        ),
    )


@pytest.mark.asyncio
async def test_private_link_resolves_uncached_channel_from_account_dialogs(tmp_path: Path):
    from types import SimpleNamespace

    from app.links import MessageLink

    channel = SimpleNamespace(id=123456, access_hash=987654321)

    class DialogClient:
        async def get_entity(self, peer):
            raise ValueError("Could not find input entity in cache")

        async def iter_dialogs(self):
            yield SimpleNamespace(entity=SimpleNamespace(id=999, access_hash=111))
            yield SimpleNamespace(entity=channel)

    downloader = MediaDownloader(DialogClient(), tmp_path / "complete", tmp_path / "incomplete")
    entity = await downloader._resolve_entity(
        MessageLink(kind="private", channel_id=123456, message_id=42)
    )

    assert entity is channel


@pytest.mark.asyncio
async def test_private_link_rejects_channel_not_found_in_account_dialogs(tmp_path: Path):
    from types import SimpleNamespace

    from app.downloader import DownloadError
    from app.links import MessageLink

    class DialogClient:
        async def get_entity(self, peer):
            raise ValueError("Could not find input entity in cache")

        async def iter_dialogs(self):
            yield SimpleNamespace(entity=SimpleNamespace(id=999, access_hash=111))

    downloader = MediaDownloader(DialogClient(), tmp_path / "complete", tmp_path / "incomplete")

    with pytest.raises(DownloadError, match="not present in this account"):
        await downloader._resolve_entity(
            MessageLink(kind="private", channel_id=123456, message_id=42)
        )


@pytest.mark.asyncio
async def test_download_resumes_from_aligned_partial_file(tmp_path: Path):
    message = make_message()
    client = FakeClient([b"b" * 4096])
    downloader = MediaDownloader(client, tmp_path / "complete", tmp_path / "incomplete")
    source_key = f"{get_peer_id(message.peer_id)}-{message.id}-{media_identity(message)}"
    partial = stable_partial_path(tmp_path / "incomplete", source_key)
    partial.write_bytes(b"a" * 4096)

    progress = []
    result = await downloader._download_one(
        message,
        progress_callback=lambda downloaded, total, filename: progress.append(
            (downloaded, total, filename)
        ),
    )

    assert progress[-1] == (8192, 8192, "asset.bin")
    assert client.requested_offset == 4096
    assert result.read_bytes() == b"a" * 4096 + b"b" * 4096
    assert stat.S_IMODE(result.stat().st_mode) == 0o644
    assert not partial.exists()


@pytest.mark.asyncio
async def test_download_truncates_unaligned_partial_tail_before_resume(tmp_path: Path):
    message = make_message()
    client = FakeClient([b"b" * 4096])
    downloader = MediaDownloader(client, tmp_path / "complete", tmp_path / "incomplete")
    source_key = f"{get_peer_id(message.peer_id)}-{message.id}-{media_identity(message)}"
    partial = stable_partial_path(tmp_path / "incomplete", source_key)
    partial.write_bytes(b"a" * 5000)

    result = await downloader._download_one(message)

    assert client.requested_offset == 4096
    assert result.read_bytes() == b"a" * 4096 + b"b" * 4096


@pytest.mark.asyncio
async def test_incomplete_download_keeps_partial_file_for_retry(tmp_path: Path):
    message = make_message()
    client = FakeClient([b"b" * 1024])
    downloader = MediaDownloader(
        client, tmp_path / "complete", tmp_path / "incomplete", max_network_retries=0
    )
    source_key = f"{get_peer_id(message.peer_id)}-{message.id}-{media_identity(message)}"
    partial = stable_partial_path(tmp_path / "incomplete", source_key)

    with pytest.raises(TransientDownloadError, match="Partial data was kept"):
        await downloader._download_one(message)

    assert partial.exists()
    assert partial.stat().st_size == 1024


@pytest.mark.asyncio
async def test_transient_network_error_retries_from_partial_offset(tmp_path: Path):
    message = make_message()

    class FlakyClient:
        def __init__(self):
            self.offsets = []

        async def iter_download(self, media, *, offset, request_size, chunk_size):
            self.offsets.append(offset)
            if len(self.offsets) == 1:
                yield b"a" * 4096
                raise ConnectionError("temporary network disconnect")
            yield b"b" * 4096

    client = FlakyClient()
    downloader = MediaDownloader(
        client,
        tmp_path / "complete",
        tmp_path / "incomplete",
        max_network_retries=2,
        retry_base_delay=0.001,
    )

    result = await downloader._download_one(message)

    assert client.offsets == [0, 4096]
    assert result.read_bytes() == b"a" * 4096 + b"b" * 4096


@pytest.mark.asyncio
async def test_expired_file_reference_is_refreshed_before_resuming(tmp_path: Path):
    message = make_message()

    class FileReferenceExpiredError(Exception):
        pass

    class RefreshingClient:
        def __init__(self):
            self.offsets = []
            self.refreshed = make_message()

        async def get_messages(self, entity, ids):
            return self.refreshed

        async def iter_download(self, media, *, offset, request_size, chunk_size):
            self.offsets.append(offset)
            if len(self.offsets) == 1:
                yield b"a" * 4096
                raise FileReferenceExpiredError("stale file reference")
            yield b"b" * 4096

    client = RefreshingClient()
    downloader = MediaDownloader(
        client,
        tmp_path / "complete",
        tmp_path / "incomplete",
        max_network_retries=2,
        retry_base_delay=0.001,
    )

    result = await downloader._download_one(message)

    assert client.offsets == [0, 4096]
    assert result.read_bytes() == b"a" * 4096 + b"b" * 4096


@pytest.mark.asyncio
async def test_auth_expiration_is_not_retried_as_network_error(tmp_path: Path):
    message = make_message()

    class AuthKeyUnregisteredError(Exception):
        pass

    class UnauthorizedClient:
        def __init__(self):
            self.calls = 0

        async def iter_download(self, media, *, offset, request_size, chunk_size):
            self.calls += 1
            raise AuthKeyUnregisteredError("session revoked")
            yield b""

    from app.downloader import AuthorizationRequiredError

    client = UnauthorizedClient()
    downloader = MediaDownloader(
        client,
        tmp_path / "complete",
        tmp_path / "incomplete",
        max_network_retries=5,
        retry_base_delay=0.001,
    )

    with pytest.raises(AuthorizationRequiredError, match="authorization expired"):
        await downloader._download_one(message)

    assert client.calls == 1


@pytest.mark.asyncio
async def test_transient_entity_lookup_failure_is_deferred_to_worker(tmp_path: Path):

    class DisconnectedClient:
        async def get_entity(self, username):
            raise ConnectionError("network is temporarily unavailable")

    downloader = MediaDownloader(
        DisconnectedClient(), tmp_path / "complete", tmp_path / "incomplete"
    )
    from app.links import MessageLink

    with pytest.raises(TransientDownloadError, match="resolving the source message"):
        await downloader.download(MessageLink(kind="public", username="example", message_id=1))


@pytest.mark.asyncio
async def test_local_disk_full_is_not_retried_as_network_failure(tmp_path: Path):
    import errno

    message = make_message()

    class FullDiskClient:
        async def iter_download(self, media, *, offset, request_size, chunk_size):
            raise OSError(errno.ENOSPC, "No space left on device")
            yield b""

    downloader = MediaDownloader(
        FullDiskClient(), tmp_path / "complete", tmp_path / "incomplete",
        max_network_retries=5, retry_base_delay=0.001,
    )

    with pytest.raises(DownloadError, match="Local storage I/O failed"):
        await downloader._download_one(message)


@pytest.mark.asyncio
async def test_file_reference_refresh_does_not_consume_network_retry_budget(tmp_path: Path):
    message = make_message()

    class FileReferenceExpiredError(Exception):
        pass

    class MixedFailureClient:
        def __init__(self):
            self.calls = 0
            self.refreshed = make_message()

        async def get_messages(self, entity, ids):
            return self.refreshed

        async def iter_download(self, media, *, offset, request_size, chunk_size):
            self.calls += 1
            if self.calls == 1:
                raise ConnectionError("temporary disconnect")
            if self.calls == 2:
                raise FileReferenceExpiredError("stale reference")
            yield b"z" * 8192

    client = MixedFailureClient()
    downloader = MediaDownloader(
        client, tmp_path / "complete", tmp_path / "incomplete",
        max_network_retries=1, retry_base_delay=0.001,
    )

    result = await downloader._download_one(message)

    assert client.calls == 3
    assert result.read_bytes() == b"z" * 8192


@pytest.mark.asyncio
async def test_unexpected_programming_error_is_not_retried_as_network_failure(tmp_path: Path):
    message = make_message()

    class BrokenClient:
        def __init__(self):
            self.calls = 0

        async def iter_download(self, media, *, offset, request_size, chunk_size):
            self.calls += 1
            raise ValueError("unexpected client state")
            yield b""

    client = BrokenClient()
    downloader = MediaDownloader(
        client, tmp_path / "complete", tmp_path / "incomplete",
        max_network_retries=5, retry_base_delay=0.001,
    )

    with pytest.raises(DownloadError, match="Non-retryable Telegram download failure"):
        await downloader._download_one(message)

    assert client.calls == 1


@pytest.mark.asyncio
async def test_protected_message_is_rejected_without_downloading(tmp_path: Path):
    from app.links import MessageLink

    message = make_message()
    message.noforwards = True

    class ProtectedClient:
        def __init__(self):
            self.download_calls = 0

        async def get_entity(self, username):
            return object()

        async def get_messages(self, entity, ids):
            return message

        async def iter_download(self, media, *, offset, request_size, chunk_size):
            self.download_calls += 1
            yield b"x" * 8192

    client = ProtectedClient()
    downloader = MediaDownloader(client, tmp_path / "complete", tmp_path / "incomplete")

    with pytest.raises(DownloadError, match="protected"):
        await downloader.download(
            MessageLink(kind="public", username="example", message_id=42)
        )

    assert client.download_calls == 0


@pytest.mark.asyncio
async def test_protected_album_is_rejected_before_any_item_download(tmp_path: Path):
    from app.links import MessageLink

    first = make_message()
    first.id = 41
    first.grouped_id = 7
    second = make_message()
    second.id = 42
    second.grouped_id = 7
    second.noforwards = True

    class ProtectedAlbumClient:
        def __init__(self):
            self.download_calls = 0

        async def get_entity(self, username):
            return object()

        async def get_messages(self, entity, ids):
            if isinstance(ids, int):
                return first
            return [first, second]

        async def iter_download(self, media, *, offset, request_size, chunk_size):
            self.download_calls += 1
            yield b"x" * 8192

    client = ProtectedAlbumClient()
    downloader = MediaDownloader(client, tmp_path / "complete", tmp_path / "incomplete")

    with pytest.raises(DownloadError, match="protected"):
        await downloader.download(
            MessageLink(kind="public", username="example", message_id=42)
        )

    assert client.download_calls == 0


@pytest.mark.asyncio
async def test_existing_completed_file_is_made_readable(tmp_path: Path):
    message = make_message()
    client = FakeClient([])
    downloader = MediaDownloader(client, tmp_path / "complete", tmp_path / "incomplete")
    source_key = f"{get_peer_id(message.peer_id)}-{message.id}-{media_identity(message)}"
    final = tmp_path / "complete" / f"{source_key}-asset.bin"
    final.parent.mkdir(parents=True)
    final.write_bytes(b"x" * 8192)
    final.chmod(0o600)

    result = await downloader._download_one(message)

    assert result == final
    assert stat.S_IMODE(final.stat().st_mode) == 0o644
    assert client.requested_offset is None


@pytest.mark.asyncio
async def test_filename_change_keeps_same_media_partial_resume(tmp_path: Path):
    message = make_message()
    client = FakeClient([b"b" * 4096])
    downloader = MediaDownloader(client, tmp_path / "complete", tmp_path / "incomplete")
    source_key = f"{get_peer_id(message.peer_id)}-{message.id}-{media_identity(message)}"
    partial = stable_partial_path(tmp_path / "incomplete", source_key)
    partial.write_bytes(b"a" * 4096)
    message.document.attributes[0].file_name = "renamed.bin"

    result = await downloader._download_one(message)

    assert client.requested_offset == 4096
    assert result.name.endswith("-renamed.bin")
    assert result.read_bytes() == b"a" * 4096 + b"b" * 4096

@pytest.mark.asyncio
async def test_replaced_media_does_not_reuse_partial_or_completed_file(tmp_path: Path):
    original = make_message()
    replacement = make_message()
    replacement.document.id = 9002
    client = FakeClient([b"n" * 8192])
    downloader = MediaDownloader(client, tmp_path / "complete", tmp_path / "incomplete")
    old_key = f"{get_peer_id(original.peer_id)}-{original.id}-{media_identity(original)}"
    old_partial = stable_partial_path(tmp_path / "incomplete", old_key)
    old_partial.write_bytes(b"o" * 4096)
    old_final = tmp_path / "complete" / f"{old_key}-asset.bin"
    old_final.parent.mkdir(parents=True)
    old_final.write_bytes(b"o" * 8192)

    result = await downloader._download_one(replacement)

    assert client.requested_offset == 0
    assert result != old_final
    assert result.read_bytes() == b"n" * 8192
    assert old_partial.read_bytes() == b"o" * 4096
    assert old_final.read_bytes() == b"o" * 8192


@pytest.mark.asyncio
async def test_file_reference_refresh_rejects_replaced_media(tmp_path: Path):
    message = make_message()

    class FileReferenceExpiredError(Exception):
        pass

    class ReplacingClient:
        async def get_messages(self, entity, ids):
            replacement = make_message()
            replacement.document.id = 9002
            return replacement

        async def iter_download(self, media, *, offset, request_size, chunk_size):
            yield b"x" * 4096
            raise FileReferenceExpiredError("stale reference")

    downloader = MediaDownloader(
        ReplacingClient(), tmp_path / "complete", tmp_path / "incomplete",
        max_network_retries=1, retry_base_delay=0.001,
    )

    with pytest.raises(DownloadError, match="media changed"):
        await downloader._download_one(message, entity=object())


@pytest.mark.asyncio
async def test_refreshed_message_that_becomes_protected_is_rejected(tmp_path: Path):
    message = make_message()
    refreshed = make_message()
    refreshed.noforwards = True

    class FileReferenceExpiredError(Exception):
        pass

    class NewlyProtectedClient:
        def __init__(self):
            self.download_calls = 0

        async def get_messages(self, entity, ids):
            return refreshed

        async def iter_download(self, media, *, offset, request_size, chunk_size):
            self.download_calls += 1
            if self.download_calls == 1:
                yield b"x" * 4096
                raise FileReferenceExpiredError("stale file reference")
            yield b"y" * 4096

    client = NewlyProtectedClient()
    downloader = MediaDownloader(
        client, tmp_path / "complete", tmp_path / "incomplete",
        max_network_retries=1, retry_base_delay=0.001,
    )

    with pytest.raises(DownloadError, match="protected"):
        await downloader._download_one(message, entity=object())

    assert client.download_calls == 1


@pytest.mark.asyncio
async def test_media_without_stable_telegram_id_is_rejected(tmp_path: Path):
    message = make_message()
    message.document.id = None
    downloader = MediaDownloader(FakeClient([]), tmp_path / "complete", tmp_path / "incomplete")

    with pytest.raises(DownloadError, match="no stable"):
        await downloader._download_one(message)


def test_photo_identity_uses_telegram_photo_id():
    message = SimpleNamespace(document=None, photo=SimpleNamespace(id=123456))

    assert media_identity(message) == "photo-123456"


def test_safe_filename_respects_utf8_filesystem_component_limit():
    filename = safe_filename("界" * 100)

    assert len(filename.encode("utf-8")) <= 180


@pytest.mark.asyncio
async def test_auth_key_not_found_is_retried_as_ambiguous_transport_failure(tmp_path: Path):
    message = make_message()

    class AuthKeyNotFound(Exception):
        pass

    class ClientWithUnknownAuthKey:
        async def iter_download(self, media, *, offset, request_size, chunk_size):
            raise AuthKeyNotFound("server temporarily forgot the key")
            yield b""

    from app.downloader import TransientDownloadError

    downloader = MediaDownloader(
        ClientWithUnknownAuthKey(),
        tmp_path / "complete",
        tmp_path / "incomplete",
        max_network_retries=0,
    )
    with pytest.raises(TransientDownloadError, match="AuthKeyNotFound"):
        await downloader._download_one(message)


@pytest.mark.asyncio
@pytest.mark.parametrize("error_name", ["AuthKeyInvalidError", "SessionExpiredError"])
async def test_invalid_or_expired_auth_key_requires_reauthorization(tmp_path: Path, error_name):
    message = make_message()
    error_type = type(error_name, (Exception,), {})

    class UnauthorizedClient:
        async def iter_download(self, media, *, offset, request_size, chunk_size):
            raise error_type("authorization expired")
            yield b""

    from app.downloader import AuthorizationRequiredError

    downloader = MediaDownloader(
        UnauthorizedClient(), tmp_path / "complete", tmp_path / "incomplete"
    )
    with pytest.raises(AuthorizationRequiredError):
        await downloader._download_one(message)


@pytest.mark.asyncio
async def test_progress_storage_failure_aborts_download_and_keeps_partial(tmp_path: Path):
    import sqlite3

    message = make_message()
    client = FakeClient([b"x" * 8192])
    downloader = MediaDownloader(client, tmp_path / "complete", tmp_path / "incomplete")
    source_key = f"{get_peer_id(message.peer_id)}-{message.id}-{media_identity(message)}"
    partial = stable_partial_path(tmp_path / "incomplete", source_key)

    def unavailable_progress(downloaded, total, filename):
        if downloaded:
            raise sqlite3.OperationalError("database is locked")

    with pytest.raises(sqlite3.OperationalError, match="database is locked"):
        await downloader._download_one(message, progress_callback=unavailable_progress)

    assert partial.exists()
    assert partial.read_bytes() == b"x" * 8192
    assert not list((tmp_path / "complete").glob("*"))


@pytest.mark.asyncio
async def test_transient_error_message_is_not_exposed_to_user(tmp_path: Path):
    secret = "opaque-session-or-request-secret"

    class FailingClient:
        async def iter_download(self, media, *, offset, request_size, chunk_size):
            raise ConnectionError(secret)
            yield b""

    downloader = MediaDownloader(
        FailingClient(),
        tmp_path / "complete",
        tmp_path / "incomplete",
        max_network_retries=0,
    )

    with pytest.raises(TransientDownloadError) as caught:
        await downloader._download_one(make_message())

    assert secret not in str(caught.value)
    assert "ConnectionError" in str(caught.value)
