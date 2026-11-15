from __future__ import annotations

import asyncio
import errno
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


class TransientDownloadError(DownloadError):
    """A network failure exhausted the local retry window and should be rescheduled."""


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
        max_network_retries: int = 0,
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
            except FloodWaitError:
                raise
            except Exception as exc:
                self._translate_rpc_error(exc, "resolving the source message")
            if not message:
                raise DownloadError(
                    "Message not found. It may have been deleted or is inaccessible."
                )
            try:
                messages = await self._album_messages(entity, message)
            except FloodWaitError:
                raise
            except Exception as exc:
                self._translate_rpc_error(exc, "reading the source album")
            results: list[Path] = []
            for item in messages:
                if not item or not item.media:
                    continue
                results.append(await self._download_one(item, entity=entity))
            if not results:
                raise DownloadError("The selected message does not contain downloadable media.")
            return results

    @staticmethod
    def _translate_rpc_error(exc: Exception, context: str) -> None:
        name = type(exc).__name__
        if name in _AUTH_ERROR_NAMES:
            raise AuthorizationRequiredError(
                "Telegram account authorization is required. Run /auth to resume."
            ) from exc
        if isinstance(exc, (ConnectionError, OSError, asyncio.TimeoutError)) or name in {
            "ServerError",
            "TimedOutError",
            "RpcCallFailError",
            "InterdcCallError",
            "ConnectionNotInitedError",
        }:
            raise TransientDownloadError(
                f"Temporary Telegram network failure while {context}: {name}"
            ) from exc
        raise exc

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

        network_attempt = 0
        reference_refreshes = 0
        while True:
            try:
                offset = partial.stat().st_size if partial.exists() else 0
                if size and offset > size:
                    partial.unlink(missing_ok=True)
                    offset = 0

                # Telegram file offsets must be aligned to 4 KiB.
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
                            # Persist every yielded byte before retrying or propagating errors.
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

            except FloodWaitError:
                # The durable worker owns the global cooldown and retry schedule.
                raise
            except DownloadError:
                raise
            except Exception as exc:
                if isinstance(exc, OSError) and exc.errno in {
                    errno.EACCES, errno.EPERM, errno.ENOSPC, errno.EDQUOT,
                    errno.EIO, errno.EROFS, errno.ENOENT, errno.ENOTDIR,
                }:
                    raise DownloadError(
                        f"Local storage I/O failed ({exc.strerror or type(exc).__name__}). "
                        "Check the download disk and directory permissions."
                    ) from exc
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
                    if reference_refreshes >= 1:
                        raise DownloadError(
                            "Telegram file reference remained invalid after one refresh."
                        ) from exc
                    try:
                        refreshed = await self.client.get_messages(
                            entity or message.peer_id, ids=message.id
                        )
                    except FloodWaitError:
                        raise
                    except Exception as refresh_exc:
                        if type(refresh_exc).__name__ in _AUTH_ERROR_NAMES:
                            raise AuthorizationRequiredError(
                                "Telegram account authorization expired while refreshing media."
                            ) from refresh_exc
                        if isinstance(
                            refresh_exc, (ConnectionError, OSError, asyncio.TimeoutError)
                        ) or type(refresh_exc).__name__ in {
                            "ServerError", "TimedOutError", "RpcCallFailError"
                        }:
                            raise TransientDownloadError(
                                "Network failure while refreshing Telegram file reference."
                            ) from refresh_exc
                        logger.warning(
                            "Could not refresh Telegram media reference for message %s: %s",
                            message.id,
                            type(refresh_exc).__name__,
                        )
                        refreshed = None
                    if not refreshed or not refreshed.media:
                        raise DownloadError(
                            "Telegram file reference expired and the source message could not "
                            "be refreshed. Check source access and submit the link again."
                        ) from exc
                    message = refreshed
                    reference_refreshes += 1
                    logger.info(
                        "Refreshed expired Telegram media reference for message %s",
                        message.id,
                    )
                    continue

                if network_attempt >= self.max_network_retries:
                    logger.warning(
                        "Download failed for message %s after %s network retries: %s",
                        message.id,
                        network_attempt,
                        type(exc).__name__,
                    )
                    raise TransientDownloadError(
                        f"Network/download failed after {network_attempt + 1} attempts "
                        f"({type(exc).__name__}: {exc}). Partial data was kept for resume."
                    ) from exc

                delay = min(self.retry_base_delay * (2**network_attempt), 30.0)
                network_attempt += 1
                logger.warning(
                    "Download interrupted for message %s; retry %s/%s in %.1fs (%s)",
                    message.id,
                    network_attempt,
                    self.max_network_retries,
                    delay,
                    type(exc).__name__,
                )
                await asyncio.sleep(delay)
