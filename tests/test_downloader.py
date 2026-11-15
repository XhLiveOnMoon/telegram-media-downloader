from pathlib import Path
from types import SimpleNamespace

import pytest
from telethon.tl.types import PeerChannel
from telethon.utils import get_peer_id

from app.downloader import DownloadError, MediaDownloader, TransientDownloadError
from app.storage import stable_partial_path


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
        document=SimpleNamespace(size=size, attributes=[SimpleNamespace(file_name="asset.bin")]),
    )


@pytest.mark.asyncio
async def test_download_resumes_from_aligned_partial_file(tmp_path: Path):
    message = make_message()
    client = FakeClient([b"b" * 4096])
    downloader = MediaDownloader(client, tmp_path / "complete", tmp_path / "incomplete")
    source_key = f"{get_peer_id(message.peer_id)}-{message.id}"
    partial = stable_partial_path(tmp_path / "incomplete", source_key, "asset.bin")
    partial.write_bytes(b"a" * 4096)

    result = await downloader._download_one(message)

    assert client.requested_offset == 4096
    assert result.read_bytes() == b"a" * 4096 + b"b" * 4096
    assert not partial.exists()


@pytest.mark.asyncio
async def test_download_truncates_unaligned_partial_tail_before_resume(tmp_path: Path):
    message = make_message()
    client = FakeClient([b"b" * 4096])
    downloader = MediaDownloader(client, tmp_path / "complete", tmp_path / "incomplete")
    source_key = f"{get_peer_id(message.peer_id)}-{message.id}"
    partial = stable_partial_path(tmp_path / "incomplete", source_key, "asset.bin")
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
    source_key = f"{get_peer_id(message.peer_id)}-{message.id}"
    partial = stable_partial_path(tmp_path / "incomplete", source_key, "asset.bin")

    with pytest.raises(DownloadError, match="Incomplete download"):
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
