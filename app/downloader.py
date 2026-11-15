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
_CHECKPOINT_BYTES = 8 * 1024 * 1024


class DownloadError(RuntimeError):
    """A user-facing download failure."""


class AuthorizationRequiredError(DownloadError):
    """The Telegram user session must be reauthorized before resuming."""


_AUTH_ERROR_NAMES = {
    "AuthKeyUnregisteredError",
    "AuthKeyDuplicatedError",
    "SessionRevokedError",
    "UserDeactivatedError",
    "UnauthorizedError",
}


class MediaDownloader:
    def __init__(
        self,
        client: TelegramClient,
        complete_dir: Path,
        incomplete_dir: Path,
        max_concurrent_downloads: int = 1,
        max_network_retries: int = 5,
        retry_base_delay: float = 2.0,
    ) -> None:
        self.client = client
        self.complete_dir = complete_dir
        self.incomplete_dir = incomplete_dir
        self.max_network_retries = max(0, max_network_retries)
        self.retry_base_delay = max(0.1, retry_base_delay)
        self._semaphore = asyncio.Semaphore(max_concurrent_downloads)
        self._file_locks: dict[str, asyncio.Lock] = {}

    async def download(self, link: MessageLink) -> list[Path]:
        async with self._semaphore:
            try:
                entity = await self._resolve_entity(link)
                message = await self.client.get_messages(entity, ids=link.message_id)
            except Exception as exc:
                if type(exc).__name__ in _AUTH_ERROR_NAMES:
                    raise AuthorizationRequiredError(
                        "Telegram account authorization is required. Run /auth to resume."
                    ) from exc
                raise
            if not message:
                raise DownloadError(
                    "Message not found. It may have been deleted or is inaccessible."
                )
            messages = await self._album_messages(entity, message)
            results: list[Path] = []
            for item in messages:
                if not item or not item.media:
                    continue
                results.append(await self._download_one(item, entity=entity))
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

    @staticmethod
    def _sync_directory(directory: Path) -> None:
        """Persist directory-entry changes such as the final atomic rename."""
        descriptor = os.open(directory, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)

    async def _download_one(self, message, entity=None) -> Path:
        source_key = f"{get_peer_id(message.peer_id)}-{message.id}"
        lock = self._file_locks.setdefault(source_key, asyncio.Lock())
        async with lock:
            return await self._download_one_locked(message, entity, source_key)

    async def _download_one_locked(self, message, entity, source_key: str) -> Path:
        document = getattr(message, "document", None)
        original_name = None
        if document:
            for attribute in document.attributes:
                original_name = getattr(attribute, "file_name", None) or original_name
        if not original_name:
            extension = ".jpg" if getattr(message, "photo", None) else ".bin"
            original_name = f"telegram-{message.id}{extension}"
        filename = safe_filename(original_name, f"telegram-{message.id}")
        partial = stable_partial_path(self.incomplete_dir, source_key, filename)
        final = self.complete_dir / f"{source_key}-{filename}"
        self.complete_dir.mkdir(parents=True, exist_ok=True)
        size = getattr(document, "size", None) if document else None
        if final.exists() and (not size or final.stat().st_size == size):
            return final

        for attempt in range(self.max_network_retries + 1):
            try:
                offset = partial.stat().st_size if partial.exists() else 0
                if size and offset > size:
                    partial.unlink(missing_ok=True)
                    offset = 0
                # Telegram file offsets are aligned to 4 KiB.
                aligned_offset = offset - (offset % 4096)
                if aligned_offset != offset:
                    with partial.open("r+b") as stream:
                        stream.truncate(aligned_offset)
                        stream.flush()
                        os.fsync(stream.fileno())
                    offset = aligned_offset

                if not (size and offset == size):
                    partial.parent.mkdir(parents=True, exist_ok=True)
                    with partial.open("ab") as stream:
                        since_checkpoint = 0
                        try:
                            async for chunk in self.client.iter_download(
                                message.media,
                                offset=offset,
                                request_size=512 * 1024,
                                chunk_size=512 * 1024,
                            ):
                                stream.write(chunk)
                                since_checkpoint += len(chunk)
                                if since_checkpoint >= _CHECKPOINT_BYTES:
                                    stream.flush()
                                    os.fsync(stream.fileno())
                                    since_checkpoint = 0
                        finally:
                            # Flush every yielded byte before retrying or propagating errors.
                            stream.flush()
                            os.fsync(stream.fileno())
                            self._sync_directory(partial.parent)

                actual_size = partial.stat().st_size
                if size and actual_size != size:
                    raise ConnectionError(
                        f"Incomplete download ({actual_size}/{size} bytes); retry to resume."
                    )
                os.replace(partial, final)
                self._sync_directory(self.complete_dir)
                if partial.parent != self.complete_dir:
                    self._sync_directory(partial.parent)
                return final
            except FloodWaitError as exc:
                raise DownloadError(
                    f"Telegram rate limit: retry after {exc.seconds} seconds."
                ) from exc
            except DownloadError:
                raise
            except Exception as exc:
                if type(exc).__name__ in _AUTH_ERROR_NAMES:
                    raise AuthorizationRequiredError(
                        "Telegram account authorization expired during download. "
                        "Run /auth; the partial file has been retained."
                    ) from exc
                if type(exc).__name__ in {
                    "FileReferenceExpiredError",
                    "FileReferenceInvalidError",
                    "FileReferenceEmptyError",
                }:
                    try:
                        refreshed = await self.client.get_messages(
                            entity or message.peer_id, ids=message.id
                        )
                    except Exception as refresh_exc:
                        logger.warning(
                            "Could not refresh Telegram media reference for message %s: %s",
                            message.id,
                            type(refresh_exc).__name__,
                        )
                        refreshed = None
                    if refreshed and refreshed.media:
                        message = refreshed
                        logger.info(
                            "Refreshed expired Telegram media reference for message %s",
                            message.id,
                        )
                        continue
                    raise DownloadError(
                        "Telegram file reference expired and the source message could not "
                        "be refreshed. Check source access and submit the link again."
                    ) from exc

                if attempt >= self.max_network_retries:
                    logger.exception("Media download failed for message %s", message.id)
                    raise DownloadError(
                        f"Network/download failed after {attempt + 1} attempts "
                        f"({type(exc).__name__}: {exc}). Partial data was kept; resend the same "
                        "message link later to resume."
                    ) from exc
                delay = min(self.retry_base_delay * (2**attempt), 30.0)
                logger.warning(
                    "Download interrupted for message %s; retry %s/%s in %.1fs (%s)",
                    message.id,
                    attempt + 1,
                    self.max_network_retries,
                    delay,
                    type(exc).__name__,
                )
                await asyncio.sleep(delay)
        raise DownloadError("Download stopped unexpectedly.")
