from __future__ import annotations

import asyncio
import logging
import os
from pathlib import Path

from telethon import TelegramClient
from telethon.errors import FloodWaitError
from telethon.tl.types import PeerChannel
from telethon.utils import get_peer_id

from .links import MessageLink
from .storage import safe_filename, stable_partial_path

logger = logging.getLogger(__name__)


class DownloadError(RuntimeError):
    """A user-facing download failure."""


class MediaDownloader:
    def __init__(
        self,
        client: TelegramClient,
        complete_dir: Path,
        incomplete_dir: Path,
        max_concurrent_downloads: int = 1,
    ) -> None:
        self.client = client
        self.complete_dir = complete_dir
        self.incomplete_dir = incomplete_dir
        self._semaphore = asyncio.Semaphore(max_concurrent_downloads)

    async def download(self, link: MessageLink) -> list[Path]:
        async with self._semaphore:
            entity = await self._resolve_entity(link)
            message = await self.client.get_messages(entity, ids=link.message_id)
            if not message:
                raise DownloadError(
                    "Message not found. It may have been deleted or is inaccessible."
                )
            messages = await self._album_messages(entity, message)
            results: list[Path] = []
            for item in messages:
                if not item or not item.media:
                    continue
                results.append(await self._download_one(item))
            if not results:
                raise DownloadError("The selected message does not contain downloadable media.")
            return results

    async def _resolve_entity(self, link: MessageLink):
        if link.kind == "public" and link.username:
            return await self.client.get_entity(link.username)
        if link.kind == "private" and link.channel_id:
            return await self.client.get_entity(PeerChannel(link.channel_id))
        raise DownloadError("Invalid Telegram message link.")

    async def _album_messages(self, entity, message) -> list:
        if not message.grouped_id:
            return [message]
        low = max(1, message.id - 10)
        high = message.id + 10
        nearby = await self.client.get_messages(entity, ids=list(range(low, high + 1)))
        album = [item for item in nearby if item and item.grouped_id == message.grouped_id]
        return sorted(album, key=lambda item: item.id) or [message]

    async def _download_one(self, message) -> Path:
        document = getattr(message, "document", None)
        original_name = None
        if document:
            for attribute in document.attributes:
                original_name = getattr(attribute, "file_name", None) or original_name
        if not original_name:
            extension = ".jpg" if getattr(message, "photo", None) else ".bin"
            original_name = f"telegram-{message.id}{extension}"
        filename = safe_filename(original_name, f"telegram-{message.id}")
        source_key = f"{get_peer_id(message.peer_id)}-{message.id}"
        partial = stable_partial_path(self.incomplete_dir, source_key, filename)
        final = self.complete_dir / f"{source_key}-{filename}"
        self.complete_dir.mkdir(parents=True, exist_ok=True)
        try:
            size = getattr(document, "size", None) if document else None
            if final.exists() and (not size or final.stat().st_size == size):
                return final
            offset = partial.stat().st_size if partial.exists() else 0
            if size and offset > size:
                partial.unlink(missing_ok=True)
                offset = 0
            # Telegram file offsets are aligned to 4 KiB.
            # Discard an incomplete tail before resuming.
            aligned_offset = offset - (offset % 4096)
            if aligned_offset != offset:
                with partial.open("r+b") as stream:
                    stream.truncate(aligned_offset)
                offset = aligned_offset
            if not (size and offset == size):
                async for chunk in self.client.iter_download(
                    message.media,
                    offset=offset,
                    request_size=512 * 1024,
                    chunk_size=512 * 1024,
                ):
                    with partial.open("ab") as stream:
                        stream.write(chunk)
            if size and partial.stat().st_size != size:
                raise DownloadError(
                    f"Incomplete download ({partial.stat().st_size}/{size} bytes); retry to resume."
                )
            os.replace(partial, final)
            return final
        except FloodWaitError as exc:
            raise DownloadError(f"Telegram rate limit: retry after {exc.seconds} seconds.") from exc
        except DownloadError:
            raise
        except Exception as exc:
            logger.exception("Media download failed for message %s", message.id)
            raise DownloadError(f"Download failed: {type(exc).__name__}. Retry later.") from exc
