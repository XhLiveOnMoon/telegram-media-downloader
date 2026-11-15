from pathlib import Path
from types import SimpleNamespace

import pytest
from telethon.tl.types import PeerChannel
from telethon.utils import get_peer_id

from app.downloader import DownloadError, MediaDownloader
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
    downloader = MediaDownloader(client, tmp_path / "complete", tmp_path / "incomplete")
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
