from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import sqlite3
import time
from html import escape as html_escape

from aiogram import Bot, Dispatcher, F, Router
from aiogram.filters import Command, CommandObject
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import CallbackQuery, LinkPreviewOptions, Message
from telethon import TelegramClient
from telethon.errors import FloodWaitError, SessionPasswordNeededError

from .config import Settings
from .downloader import (
    AuthorizationRequiredError,
    MediaDownloader,
    TransientDownloadError,
    is_transient_transport_error,
)
from .links import parse_message_link
from .storage import identity_token, prepare_directories, safe_filename, stable_partial_path
from .task_store import DownloadTask, DownloadTaskStore
from .worker import (
    AuthorizationRequired,
    DownloadWorker,
    FloodWait,
    PermanentTaskError,
    RetryableTaskError,
    StorageUnavailable,
)

logger = logging.getLogger(__name__)
router = Router()


_MAX_BOT_API_DOWNLOAD_BYTES = 20 * 1024 * 1024


def direct_media_protection_error(message: Message) -> str | None:
    """Reject Bot API media explicitly marked as protected content."""
    if getattr(message, "has_protected_content", False):
        return "This Telegram message is protected from forwarding/downloading."
    return None


def direct_media_size_error(file_size: int | None) -> str | None:
    """Fail closed when Bot API size metadata cannot prove the file is downloadable."""
    if isinstance(file_size, bool) or not isinstance(file_size, int) or file_size <= 0:
        return (
            "Telegram did not provide a valid file size. Send the original message link "
            "with /download instead."
        )
    if file_size > _MAX_BOT_API_DOWNLOAD_BYTES:
        return (
            "This file exceeds the standard Telegram Bot API download limit (20 MB). "
            "Send the original message link with /download instead."
        )
    return None


async def _delete_sensitive_message(message: Message) -> None:
    try:
        await message.delete()
    except Exception as exc:
        logger.warning("Could not delete sensitive login message: %s", type(exc).__name__)


class AuthFlow(StatesGroup):
    phone = State()
    code = State()
    password = State()


class BotService:
    def __init__(self, settings: Settings, client: TelegramClient) -> None:
        self.settings = settings
        self.client = client
        # The durable worker is the only caller of the downloader.
        self.downloader = MediaDownloader(
            client, settings.complete_dir, settings.incomplete_dir, max_concurrent_downloads=1
        )
        self.tasks = DownloadTaskStore(settings.data_dir / "downloads.sqlite3")
        self.mtproto_lock = asyncio.Lock()
        self.worker: DownloadWorker | None = None
        self.worker_task: asyncio.Task | None = None
        self.pending_phone: str | None = None
        self.pending_code_hash: str | None = None
        self.auth_flow_generation = 0
        self.auth_state_lock = asyncio.Lock()

    async def begin_auth_flow(self, state: FSMContext, expected_generation: int) -> int | None:
        """Start a fresh login flow only if no cancellation superseded the request."""
        async with self.auth_state_lock:
            if self.auth_flow_generation != expected_generation:
                return None
            self.auth_flow_generation += 1
            self.pending_phone = self.pending_code_hash = None
            await state.set_state(AuthFlow.phone)
            return self.auth_flow_generation

    async def transition_auth_flow(
        self,
        generation: int,
        state: FSMContext,
        next_state: State,
        *,
        phone: str | None = None,
        code_hash: str | None = None,
    ) -> bool:
        """Apply an auth FSM transition only while its originating flow is current."""
        async with self.auth_state_lock:
            if self.auth_flow_generation != generation:
                return False
            if phone is not None:
                self.pending_phone = phone
            if code_hash is not None:
                self.pending_code_hash = code_hash
            await state.set_state(next_state)
            return True

    async def clear_auth_flow(
        self, state: FSMContext, generation: int | None = None
    ) -> bool:
        """Invalidate in-flight login handlers and clear the owner FSM state."""
        async with self.auth_state_lock:
            if generation is not None and self.auth_flow_generation != generation:
                return False
            self.auth_flow_generation += 1
            self.pending_phone = self.pending_code_hash = None
            await state.clear()
            return True

    @property
    def active_downloads(self) -> int:
        return int(self.worker is not None and self.worker.active_task_id is not None)

    def is_owner(self, message: Message) -> bool:
        return bool(message.from_user and message.from_user.id == self.settings.owner_telegram_id)

    async def wait_for_mtproto_cooldown(self) -> None:
        while True:
            remaining = self.tasks.cooldown_remaining()
            if self.worker is not None:
                remaining = max(
                    remaining, self.worker.local_cooldown_until - time.time()
                )
            if remaining <= 0:
                return
            await asyncio.sleep(remaining)

    def note_flood_wait(self, seconds: float) -> None:
        if self.worker is not None:
            self.worker.extend_local_cooldown(seconds)
        try:
            self.tasks.set_global_cooldown(seconds)
        except Exception:
            logger.exception("Could not persist Telegram FloodWait cooldown")

    async def is_authorized(self) -> bool:
        try:
            async with self.mtproto_lock:
                await self.wait_for_mtproto_cooldown()
                return await self.client.is_user_authorized()
        except FloodWaitError as exc:
            self.note_flood_wait(exc.seconds)
            raise
        except Exception as exc:
            if type(exc).__name__ in {
                "AuthKeyUnregisteredError",
                "AuthKeyInvalidError",
                "SessionExpiredError",
                "AuthKeyDuplicatedError",
                "SessionRevokedError",
                "UserDeactivatedError",
                "UnauthorizedError",
            }:
                return False
            raise

    async def reconcile_authorization(self) -> tuple[bool, int]:
        """Requeue auth-blocked tasks if the persisted Telegram session is valid."""
        authorized = await self.is_authorized()
        resumed = self.tasks.resume_auth_required() if authorized else 0
        return authorized, resumed

    def start_worker(self, bot: Bot) -> asyncio.Task:
        if self.worker_task is not None:
            raise RuntimeError("Download worker has already been started")
        self.worker = DownloadWorker(self.tasks, lambda task: self.execute_task(task, bot))
        self.worker_task = asyncio.create_task(self.worker.run(), name="download-worker")
        return self.worker_task

    async def execute_task(self, task: DownloadTask, bot: Bot) -> str:
        try:
            if task.task_type == "message-link":
                link = parse_message_link(task.source)

                def progress(downloaded: int, total: int | None, filename: str) -> None:
                    self.tasks.update_progress(task.task_id, downloaded, total, filename)

                async with self.mtproto_lock:
                    await self.wait_for_mtproto_cooldown()
                    paths = await self.downloader.download(
                        link, progress_callback=progress
                    )
                detail = ", ".join(path.name for path in paths)
                await self._notify_after_commit(
                    bot, f"下载任务 #{task.task_id} 完成（{len(paths)} 个文件）：\n{detail}"
                )
                return detail

            if task.task_type == "direct-media":
                payload = json.loads(task.source)
                file_id = payload.get("file_id")
                filename_value = payload.get("filename")
                source_key = payload.get("source_key")
                stored_identity = payload.get("media_identity")
                if not isinstance(file_id, str) or not file_id:
                    raise PermanentTaskError("Direct Bot API task has an invalid file identifier.")
                if not isinstance(filename_value, str):
                    raise PermanentTaskError("Direct Bot API task has an invalid filename.")
                if not isinstance(source_key, str) or not re.fullmatch(
                    r"bot-[0-9]+-[0-9]+", source_key
                ):
                    raise PermanentTaskError("Direct Bot API task has an invalid source identity.")
                if stored_identity is not None and (
                    not isinstance(stored_identity, str)
                    or not re.fullmatch(r"[0-9a-f]{24}", stored_identity)
                ):
                    raise PermanentTaskError("Direct Bot API task has an invalid media identity.")
                filename = safe_filename(filename_value)
                file_size = payload.get("file_size")
                size_error = direct_media_size_error(file_size)
                if size_error is not None:
                    raise PermanentTaskError(
                        f"Direct Bot API task cannot be resumed safely: {size_error}"
                    )
                # Legacy queued tasks lack file_unique_id; file_id is a conservative
                # fallback so they cannot collide with newly submitted media.
                media_identity = payload.get("media_identity") or identity_token(
                    f"bot-api-file-id:{file_id}"
                )
                identity_scoped_key = f"{source_key}-{media_identity}"
                partial = stable_partial_path(
                    self.settings.incomplete_dir, identity_scoped_key
                )
                final = self.settings.complete_dir / f"{identity_scoped_key}-{filename}"
                self.settings.complete_dir.mkdir(parents=True, exist_ok=True)

                if final.exists() and (
                    file_size is None or final.stat().st_size == file_size
                ):
                    self.tasks.update_progress(
                        task.task_id, final.stat().st_size, file_size, filename
                    )
                    os.chmod(final, 0o644)
                    with final.open("rb+") as stream:
                        os.fsync(stream.fileno())
                    self._sync_directory(final.parent)
                    await self._notify_after_commit(
                        bot, f"直传任务 #{task.task_id} 已完成：\n{final.name}"
                    )
                    return final.name

                # Bot API downloads do not expose a byte-offset resume interface.
                # Restart this small (<=20 MB) transfer from byte zero, never append.
                self.tasks.update_progress(task.task_id, 0, file_size, filename)
                partial.unlink(missing_ok=True)
                partial.touch(mode=0o600)
                os.chmod(partial, 0o600)
                await bot.download(file=file_id, destination=partial)
                actual_size = partial.stat().st_size
                if file_size is not None and actual_size != file_size:
                    self.tasks.update_progress(task.task_id, actual_size, file_size, filename)
                    raise RetryableTaskError(
                        f"Bot API file size mismatch ({actual_size}/{file_size} bytes)"
                    )
                self.tasks.update_progress(task.task_id, actual_size, file_size, filename)
                with partial.open("rb+") as stream:
                    os.fsync(stream.fileno())
                os.replace(partial, final)
                os.chmod(final, 0o644)
                # Sync the promoted file through a writable descriptor, then persist
                # both sides of the cross-directory rename before reporting completion.
                with final.open("rb+") as stream:
                    os.fsync(stream.fileno())
                self._sync_directory(final.parent)
                if partial.parent != final.parent:
                    self._sync_directory(partial.parent)
                await self._notify_after_commit(
                    bot, f"直传任务 #{task.task_id} 完成：\n{final.name}"
                )
                return final.name

            raise ValueError(f"Unsupported task type: {task.task_type}")
        except FloodWaitError as exc:
            self.note_flood_wait(exc.seconds)
            await self._notify(
                bot,
                f"Telegram 触发 FloodWait，整个下载队列暂停 {exc.seconds} 秒；到期后自动继续。",
            )
            raise FloodWait(exc.seconds) from exc
        except AuthorizationRequiredError as exc:
            await self._notify(
                bot,
                (
                    f"任务 #{task.task_id} 已暂停：Telegram 用户账号需要重新授权，"
                    "请私聊发送 /auth。"
                ),
            )
            raise AuthorizationRequired(str(exc)) from exc
        except TransientDownloadError as exc:
            if self.worker and task.attempts >= self.worker.max_attempts:
                await self._notify(
                    bot, f"任务 #{task.task_id} 多次网络重试仍失败，已停止自动重试：{exc}"
                )
            raise RetryableTaskError(str(exc)) from exc
        except PermanentTaskError as exc:
            await self._notify(bot, f"下载任务 #{task.task_id} 无法执行：{exc}")
            raise
        except sqlite3.Error as exc:
            raise StorageUnavailable(
                "Local task database is temporarily unavailable; the task will be retried."
            ) from exc
        except RetryableTaskError as exc:
            if self.worker and task.attempts >= self.worker.max_attempts:
                await self._notify(
                    bot, f"任务 #{task.task_id} 多次重试仍失败，已停止自动重试：{exc}"
                )
            raise
        except Exception as exc:
            retry_after = getattr(exc, "retry_after", None)
            if retry_after is not None:
                seconds = getattr(retry_after, "total_seconds", None)
                delay = seconds() if callable(seconds) else float(retry_after)
                self.note_flood_wait(delay)
                await self._notify(
                    bot,
                    f"Telegram 要求等待 {int(delay)} 秒，整个下载队列将在等待结束后继续。",
                )
                raise FloodWait(int(delay)) from exc
            if is_transient_transport_error(exc):
                if self.worker and task.attempts >= self.worker.max_attempts:
                    await self._notify(
                        bot,
                        f"任务 #{task.task_id} 多次网络重试仍失败，已停止自动重试："
                        f"{type(exc).__name__}",
                    )
                raise RetryableTaskError(
                    f"Telegram transport error: {type(exc).__name__}"
                ) from exc
            await self._notify(
                bot,
                f"下载任务 #{task.task_id} 失败：{type(exc).__name__}。可通过 /tasks 查看状态。",
            )
            raise

    async def _notify_after_commit(self, bot: Bot, text: str) -> None:
        """Cancellation after durable file promotion must not mark the task cancelled."""
        try:
            await self._notify(bot, text)
        except asyncio.CancelledError:
            logger.info("Cancellation arrived after file promotion; preserving completed result")

    async def _notify(self, bot: Bot, text: str) -> None:
        try:
            await bot.send_message(self.settings.owner_telegram_id, html_escape(text))
        except Exception as exc:
            retry_after = getattr(exc, "retry_after", None)
            if retry_after is not None:
                seconds = getattr(retry_after, "total_seconds", None)
                delay = seconds() if callable(seconds) else float(retry_after)
                self.note_flood_wait(delay)
            # Notification failure must not turn an already-saved file into a failed task.
            logger.warning("Could not send task notification: %s", type(exc).__name__)

    @staticmethod
    def _sync_directory(directory) -> None:
        descriptor = os.open(directory, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)


service: BotService | None = None


def _format_bytes(value: int) -> str:
    amount = float(value)
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if amount < 1024 or unit == "TiB":
            return f"{amount:.1f} {unit}"
        amount /= 1024
    return f"{amount:.1f} TiB"


def get_service() -> BotService:
    if service is None:
        raise RuntimeError("Bot service is not initialized")
    return service


@router.message(F.chat.type == "private", Command("start"))
async def start(message: Message) -> None:
    svc = get_service()
    if not svc.is_owner(message):
        return
    await message.answer(
        "Telegram Media Downloader is ready.\n\n"
        "/auth — authorize your Telegram account\n"
        "/download &lt;message link&gt; — queue a download\n"
        "/status — show service status\n"
        "/tasks — show recent download tasks\n"
        "/clear — clear terminal download history (confirmation required)\n"
        "/retry &lt;task_id&gt; — retry a failed or cancelled task\n"
        "/cancel [task_id] — cancel a task or the current interaction"
    )


@router.message(F.chat.type == "private", Command("status"))
async def status(message: Message) -> None:
    svc = get_service()
    if not svc.is_owner(message):
        return
    if svc.active_downloads:
        state = "download in progress; authorization check deferred"
    else:
        try:
            authorized = await svc.is_authorized()
            state = "authorized" if authorized else "authorization required"
        except Exception as exc:
            state = f"temporarily unavailable ({type(exc).__name__})"
    cooldown = svc.tasks.cooldown_remaining()
    await message.answer(
        f"Bot: running\nTelegram account: {state}\n"
        f"Active downloads: {svc.active_downloads}\n"
        f"Global Telegram cooldown: {cooldown:.0f}s"
    )


@router.message(F.chat.type == "private", Command("tasks"))
async def tasks(message: Message) -> None:
    svc = get_service()
    if not svc.is_owner(message):
        return
    records = svc.tasks.recent(10)
    if not records:
        await message.answer("No download tasks have been recorded yet.")
        return
    lines = ["Recent download tasks:"]
    for record in records:
        if record.task_type == "direct-media":
            try:
                source = json.loads(record.source).get("filename", "direct media")
            except (json.JSONDecodeError, AttributeError):
                source = "direct media"
        else:
            source = record.source[:100]
        source = html_escape(str(source))
        detail = f" — {html_escape(record.detail[:100])}" if record.detail else ""
        retry = (
            f" retry-in={max(0, record.next_attempt_at - time.time()):.0f}s"
            if record.next_attempt_at is not None
            else ""
        )
        if record.task_type == "direct-media" and record.status == "downloading":
            progress = " progress unavailable during Bot API transfer"
        elif record.total_bytes:
            percent = min(100, record.progress_bytes * 100 // record.total_bytes)
            progress = (
                f" {percent}% ({_format_bytes(record.progress_bytes)}/"
                f"{_format_bytes(record.total_bytes)})"
            )
        elif record.progress_bytes:
            progress = f" {_format_bytes(record.progress_bytes)} downloaded"
        else:
            progress = ""
        if record.current_file:
            progress += f" {html_escape(record.current_file)}"
        lines.append(
            f"#{record.task_id} [{record.status}] {record.task_type} "
            f"attempts={record.attempts}{retry}{progress}: {source}{detail}"
        )
    await message.answer(
        "\n".join(lines),
        link_preview_options=LinkPreviewOptions(is_disabled=True),
    )



@router.message(F.chat.type == "private", Command("clear"))
async def clear_history(message: Message, command: CommandObject) -> None:
    svc = get_service()
    if not svc.is_owner(message):
        return
    if (command.args or "").strip().lower() != "confirm":
        await message.answer(
            "确认清除历史下载记录？\\n"
            "将删除已完成、已失败和已取消的任务记录。\\n"
            "排队中、下载中、等待重试、等待授权及中断待恢复的任务会保留。\\n"
            "不会删除任何已下载或未完成的媒体文件。\\n\\n"
            "确认执行请发送：/clear confirm"
        )
        return
    deleted = svc.tasks.clear_history()
    await message.answer(f"历史下载记录清理完成，共删除 {deleted} 条记录；媒体文件未删除。")


@router.message(F.chat.type == "private", Command("auth"))
async def auth_start(message: Message, state: FSMContext) -> None:
    svc = get_service()
    if not svc.is_owner(message):
        return
    generation = svc.auth_flow_generation
    try:
        authorized = await svc.is_authorized()
    except FloodWaitError as exc:
        await message.answer(
            f"Telegram asks you to wait {exc.seconds} seconds before checking authorization."
        )
        return
    except Exception as exc:
        logger.warning("Authorization check failed: %s", type(exc).__name__)
        if type(exc).__name__ == "AuthKeyNotFound":
            await message.answer(
                "Telegram 服务器暂时无法识别当前 MTProto Session。"
                "这可能是临时服务器问题；请稍后重试。若持续出现，请按 README 中 "
                "“Recovering an unrecognized Telethon session” 的步骤备份并重建 Session，"
                "然后重新发送 /auth。当前不会自动删除或覆盖旧 Session。"
            )
        else:
            await message.answer(
                "暂时无法检查 Telegram 授权状态，可能是网络或本地会话存储暂时不可用。"
                "请稍后重试 /auth；当前不会将账号误判为未授权。"
            )
        return
    if generation != svc.auth_flow_generation:
        return
    if authorized:
        await message.answer("Telegram account is already authorized.")
        return
    if await svc.begin_auth_flow(state, generation) is None:
        return
    await message.answer("Send your phone number in international format, e.g. +1234567890.")


@router.callback_query(F.data == "auth:start")
async def auth_callback(callback: CallbackQuery, state: FSMContext) -> None:
    svc = get_service()
    if not callback.message or callback.message.chat.type != "private":
        await callback.answer(
            "Please authorize through a private chat with the Bot.", show_alert=True
        )
        return
    if not callback.from_user or callback.from_user.id != svc.settings.owner_telegram_id:
        await callback.answer("Not authorized", show_alert=True)
        return
    generation = svc.auth_flow_generation
    try:
        authorized = await svc.is_authorized()
    except FloodWaitError as exc:
        await callback.answer(
            f"Wait {exc.seconds} seconds before retrying authorization.", show_alert=True
        )
        return
    except Exception as exc:
        logger.warning("Authorization callback check failed: %s", type(exc).__name__)
        await callback.answer(
            "暂时无法检查授权状态，请稍后重试。", show_alert=True
        )
        return
    if generation != svc.auth_flow_generation:
        await callback.answer("Authorization flow was cancelled", show_alert=True)
        return
    if authorized:
        await callback.answer("Already authorized")
        await callback.bot.send_message(
            callback.from_user.id, "Telegram account is already authorized."
        )
        return
    if await svc.begin_auth_flow(state, generation) is None:
        await callback.answer("Authorization flow was cancelled", show_alert=True)
        return
    await callback.answer()
    await callback.bot.send_message(
        callback.from_user.id,
        "Send your phone number in international format, e.g. +1234567890.",
    )


async def _authorization_succeeded(
    message: Message, state: FSMContext, svc: BotService, generation: int
) -> None:
    if not await svc.clear_auth_flow(state, generation):
        return
    resumed = svc.tasks.resume_auth_required()
    await message.answer(
        f"Authorization successful. {resumed} authorization-blocked task(s) returned to the queue."
    )


@router.message(AuthFlow.phone, F.chat.type == "private", F.text, ~F.text.startswith("/"))
async def auth_phone(message: Message, state: FSMContext) -> None:
    svc = get_service()
    if not svc.is_owner(message):
        return
    generation = await _current_auth_generation(svc, state, AuthFlow.phone)
    if generation is None:
        return
    phone = (message.text or "").strip()
    await _delete_sensitive_message(message)
    if generation != svc.auth_flow_generation:
        return
    if not phone.startswith("+") or not phone[1:].isdigit():
        await message.answer("Invalid phone number. Use international format, e.g. +1234567890.")
        return
    try:
        async with svc.mtproto_lock:
            await svc.wait_for_mtproto_cooldown()
            if generation != svc.auth_flow_generation:
                return
            sent = await svc.client.send_code_request(phone)
    except FloodWaitError as exc:
        svc.note_flood_wait(exc.seconds)
        await message.answer(
            f"Telegram asks you to wait {exc.seconds} seconds before requesting another login code."
        )
        return
    except sqlite3.Error:
        await message.answer(
            "Local session storage is temporarily unavailable. Retry the same phone number."
        )
        return
    except Exception as exc:
        logger.warning("Telegram code request failed: %s", type(exc).__name__)
        if is_transient_transport_error(exc):
            await message.answer(
                "Temporary Telegram network failure. Send the same phone number again to retry."
            )
            return
        await message.answer(
            "Could not request a login code. Check the number and try /auth again."
        )
        await svc.clear_auth_flow(state, generation)
        return
    if not await svc.transition_auth_flow(
        generation, state, AuthFlow.code, phone=phone, code_hash=sent.phone_code_hash
    ):
        return
    await message.answer("Login code requested. Send the code you received in Telegram.")


@router.message(AuthFlow.code, F.chat.type == "private", F.text, ~F.text.startswith("/"))
async def auth_code(message: Message, state: FSMContext) -> None:
    svc = get_service()
    if not svc.is_owner(message):
        return
    generation = await _current_auth_generation(svc, state, AuthFlow.code)
    if generation is None:
        return
    phone = svc.pending_phone
    code_hash = svc.pending_code_hash
    code = (message.text or "").replace(" ", "").strip()
    await _delete_sensitive_message(message)
    if generation != svc.auth_flow_generation:
        return
    if not code.isdigit() or not phone or not code_hash:
        await message.answer("Invalid or expired login flow. Run /auth again.")
        await svc.clear_auth_flow(state, generation)
        return
    try:
        async with svc.mtproto_lock:
            await svc.wait_for_mtproto_cooldown()
            if generation != svc.auth_flow_generation:
                return
            await svc.client.sign_in(
                phone=phone,
                code=code,
                phone_code_hash=code_hash,
            )
    except SessionPasswordNeededError:
        if await svc.transition_auth_flow(generation, state, AuthFlow.password):
            await message.answer("Two-step verification is enabled. Send your 2FA password.")
        return
    except FloodWaitError as exc:
        svc.note_flood_wait(exc.seconds)
        await message.answer(
            f"Telegram asks you to wait {exc.seconds} seconds before retrying this login step."
        )
        return
    except sqlite3.Error:
        await message.answer(
            "Local session storage is temporarily unavailable. Retry the same login code."
        )
        return
    except Exception as exc:
        if type(exc).__name__ == "PhoneCodeInvalidError":
            await message.answer("Incorrect login code. Try again.")
            return
        logger.warning("Telegram code verification failed: %s", type(exc).__name__)
        if is_transient_transport_error(exc):
            await message.answer(
                "Temporary Telegram network failure. Send the same login code again to retry."
            )
            return
        await message.answer("Code was rejected or expired. Run /auth to request a new code.")
        await svc.clear_auth_flow(state, generation)
        return
    await _authorization_succeeded(message, state, svc, generation)


@router.message(AuthFlow.password, F.chat.type == "private", F.text, ~F.text.startswith("/"))
async def auth_password(message: Message, state: FSMContext) -> None:
    svc = get_service()
    if not svc.is_owner(message):
        return
    generation = await _current_auth_generation(svc, state, AuthFlow.password)
    if generation is None:
        return
    # Telegram 2FA passwords are opaque strings; surrounding spaces are significant.
    password = message.text or ""
    await _delete_sensitive_message(message)
    if generation != svc.auth_flow_generation:
        return
    try:
        async with svc.mtproto_lock:
            await svc.wait_for_mtproto_cooldown()
            if generation != svc.auth_flow_generation:
                return
            await svc.client.sign_in(password=password)
    except FloodWaitError as exc:
        svc.note_flood_wait(exc.seconds)
        await message.answer(
            f"Telegram asks you to wait {exc.seconds} seconds before retrying 2FA."
        )
        return
    except sqlite3.Error:
        await message.answer(
            "Local session storage is temporarily unavailable. Retry the same 2FA password."
        )
        return
    except Exception as exc:
        if type(exc).__name__ == "PasswordHashInvalidError":
            await message.answer("Incorrect 2FA password. Try again.")
            return
        logger.warning("Telegram 2FA verification failed: %s", type(exc).__name__)
        if is_transient_transport_error(exc):
            await message.answer(
                "Temporary Telegram network failure. Send the 2FA password again to retry."
            )
            return
        await message.answer("2FA verification failed. Run /auth to start again.")
        await svc.clear_auth_flow(state, generation)
        return
    await _authorization_succeeded(message, state, svc, generation)


@router.message(F.chat.type == "private", Command("cancel"))
async def cancel(
    message: Message, state: FSMContext, command: CommandObject
) -> None:
    svc = get_service()
    if not svc.is_owner(message):
        return
    if command.args:
        try:
            task_id = int(command.args.strip())
        except ValueError:
            await message.answer("Usage: /cancel &lt;task_id&gt;")
            return
        if svc.worker is not None and svc.worker.cancel_task(task_id):
            await message.answer(f"已请求取消任务 #{task_id}。")
        else:
            await message.answer(
                f"任务 #{task_id} 当前不存在或已进入不可取消状态。"
            )
        return
    await svc.clear_auth_flow(state)
    await message.answer("Current interaction cancelled.")


@router.message(F.chat.type == "private", Command("retry"))
async def retry_task(message: Message, command: CommandObject) -> None:
    svc = get_service()
    if not svc.is_owner(message):
        return
    if not command.args:
        await message.answer("Usage: /retry &lt;task_id&gt;")
        return
    try:
        task_id = int(command.args.strip())
    except ValueError:
        await message.answer("Usage: /retry &lt;task_id&gt;")
        return
    if svc.tasks.requeue(task_id):
        await message.answer(f"任务 #{task_id} 已重新加入下载队列。")
    else:
        await message.answer(
            f"任务 #{task_id} 不是失败或已取消状态，无法手动重试。"
        )


@router.message(F.chat.type == "private", Command("download"))
async def download(message: Message, command: CommandObject) -> None:
    svc = get_service()
    if not svc.is_owner(message):
        return
    if not command.args:
        await message.answer("Usage: /download https://t.me/channel/123")
        return
    try:
        parse_message_link(command.args)
    except ValueError as exc:
        await message.answer(str(exc))
        return
    task_id, created = svc.tasks.enqueue_with_status("message-link", command.args)
    if created:
        response = f"任务 #{task_id} 已加入下载队列，当前采用全局单任务串行下载。"
    else:
        response = f"相同来源的任务 #{task_id} 已存在，未重复创建。"
    await message.answer(response)


@router.message(
    F.chat.type == "private",
    F.document | F.video | F.audio | F.voice | F.photo,
)
async def direct_media(message: Message) -> None:
    """Queue directly sent/forwarded media; all sources share the same worker."""
    svc = get_service()
    if not svc.is_owner(message):
        return
    protection_error = direct_media_protection_error(message)
    if protection_error is not None:
        await message.answer(protection_error)
        return
    media = message.document or message.video or message.audio or message.voice
    if message.photo:
        photo = message.photo[-1]
        file_id = photo.file_id
        file_unique_id = photo.file_unique_id
        filename = f"telegram-photo-{message.message_id}.jpg"
        file_size = photo.file_size
    elif media:
        file_id = media.file_id
        file_unique_id = media.file_unique_id
        filename = getattr(media, "file_name", None) or f"telegram-{message.message_id}.bin"
        file_size = getattr(media, "file_size", None)
    else:
        return
    if not file_unique_id:
        await message.answer(
            "Telegram did not provide a stable file identity; this media cannot be queued safely."
        )
        return
    size_error = direct_media_size_error(file_size)
    if size_error is not None:
        await message.answer(size_error)
        return

    source_key = f"bot-{message.chat.id}-{message.message_id}"
    payload = json.dumps(
        {
            "file_id": file_id,
            "filename": filename,
            "file_size": file_size,
            "source_key": source_key,
            "media_identity": identity_token(
                f"bot-api-file-unique-id:{file_unique_id}"
            ),
        },
        ensure_ascii=False,
    )
    task_id, created = svc.tasks.enqueue_with_status("direct-media", payload)
    if created:
        response = (
            f"直传文件任务 #{task_id} 已加入全局下载队列；当前任务完成后会自动处理。"
        )
    else:
        response = f"相同媒体的任务 #{task_id} 已存在，未重复创建。"
    await message.answer(response)


async def _current_auth_generation(
    svc: BotService, state: FSMContext, expected: State
) -> int | None:
    """Read the FSM state and generation atomically against cancellation."""
    async with svc.auth_state_lock:
        get_state = getattr(state, "get_state", None)
        if get_state is not None and await get_state() != expected.state:
            return None
        return svc.auth_flow_generation


def build_dispatcher(settings: Settings, client: TelegramClient) -> Dispatcher:
    global service
    prepare_directories(settings.complete_dir, settings.incomplete_dir)
    service = BotService(settings, client)
    from aiogram.fsm.storage.memory import MemoryStorage

    dispatcher = Dispatcher(storage=MemoryStorage())
    dispatcher.include_router(router)
    return dispatcher
