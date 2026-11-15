from __future__ import annotations

import logging

from aiogram import Dispatcher, F, Router
from aiogram.filters import Command, CommandObject
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import (
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Message,
)
from telethon import TelegramClient
from telethon.errors import SessionPasswordNeededError

from .config import Settings
from .downloader import DownloadError, MediaDownloader
from .links import parse_message_link
from .storage import prepare_directories, safe_filename, stable_partial_path

logger = logging.getLogger(__name__)
router = Router()


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
        self.downloader = MediaDownloader(
            client,
            settings.complete_dir,
            settings.incomplete_dir,
            settings.max_concurrent_downloads,
        )
        self.active_downloads = 0
        self.pending_phone: str | None = None
        self.pending_code_hash: str | None = None

    def is_owner(self, message: Message) -> bool:
        return bool(message.from_user and message.from_user.id == self.settings.owner_telegram_id)


service: BotService | None = None


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
        "/download <message link> — download media\n"
        "/status — show service status\n"
        "/cancel — cancel the current interaction"
    )


@router.message(F.chat.type == "private", Command("status"))
async def status(message: Message) -> None:
    svc = get_service()
    if not svc.is_owner(message):
        return
    authorized = await svc.client.is_user_authorized()
    state = "authorized" if authorized else "authorization required"
    await message.answer(
        f"Bot: running\nTelegram account: {state}\n"
        f"Active downloads: {svc.active_downloads}"
    )


@router.message(F.chat.type == "private", Command("auth"))
async def auth_start(message: Message, state: FSMContext) -> None:
    svc = get_service()
    if not svc.is_owner(message):
        return
    if await svc.client.is_user_authorized():
        await message.answer("Telegram account is already authorized.")
        return
    await state.set_state(AuthFlow.phone)
    await message.answer("Send your phone number in international format, e.g. +1234567890.")


@router.callback_query(F.data == "auth:start")
async def auth_callback(callback: CallbackQuery, state: FSMContext) -> None:
    svc = get_service()
    if not callback.from_user or callback.from_user.id != svc.settings.owner_telegram_id:
        await callback.answer("Not authorized", show_alert=True)
        return
    if await svc.client.is_user_authorized():
        await callback.answer("Already authorized")
        if callback.message:
            await callback.message.answer("Telegram account is already authorized.")
        return
    await state.set_state(AuthFlow.phone)
    await callback.answer()
    if callback.message:
        await callback.message.answer(
            "Send your phone number in international format, e.g. +1234567890."
        )


@router.message(AuthFlow.phone, F.chat.type == "private")
async def auth_phone(message: Message, state: FSMContext) -> None:
    svc = get_service()
    if not svc.is_owner(message):
        return
    phone = (message.text or "").strip()
    await _delete_sensitive_message(message)
    if not phone.startswith("+") or not phone[1:].isdigit():
        await message.answer("Invalid phone number. Use international format, e.g. +1234567890.")
        return
    try:
        sent = await svc.client.send_code_request(phone)
    except Exception as exc:
        logger.warning("Telegram code request failed: %s", type(exc).__name__)
        await message.answer(
            "Could not request a login code. Check the number and try /auth again."
        )
        await state.clear()
        return
    svc.pending_phone = phone
    svc.pending_code_hash = sent.phone_code_hash
    await state.set_state(AuthFlow.code)
    await message.answer("Login code requested. Send the code you received in Telegram.")


@router.message(AuthFlow.code, F.chat.type == "private")
async def auth_code(message: Message, state: FSMContext) -> None:
    svc = get_service()
    if not svc.is_owner(message):
        return
    code = (message.text or "").replace(" ", "").strip()
    await _delete_sensitive_message(message)
    if not code.isdigit() or not svc.pending_phone or not svc.pending_code_hash:
        await message.answer("Invalid or expired login flow. Run /auth again.")
        await state.clear()
        return
    try:
        await svc.client.sign_in(
            phone=svc.pending_phone,
            code=code,
            phone_code_hash=svc.pending_code_hash,
        )
    except SessionPasswordNeededError:
        await state.set_state(AuthFlow.password)
        await message.answer("Two-step verification is enabled. Send your 2FA password.")
        return
    except Exception as exc:
        logger.warning("Telegram code verification failed: %s", type(exc).__name__)
        await message.answer("Code was rejected or expired. Run /auth to request a new code.")
        svc.pending_phone = svc.pending_code_hash = None
        await state.clear()
        return
    svc.pending_phone = svc.pending_code_hash = None
    await state.clear()
    await message.answer("Authorization successful. You can now use /download.")


@router.message(AuthFlow.password, F.chat.type == "private")
async def auth_password(message: Message, state: FSMContext) -> None:
    svc = get_service()
    if not svc.is_owner(message):
        return
    password = (message.text or "").strip()
    await _delete_sensitive_message(message)
    try:
        await svc.client.sign_in(password=password)
    except Exception as exc:
        logger.warning("Telegram 2FA verification failed: %s", type(exc).__name__)
        await message.answer("2FA verification failed. Run /auth to start again.")
        svc.pending_phone = svc.pending_code_hash = None
        await state.clear()
        return
    svc.pending_phone = svc.pending_code_hash = None
    await state.clear()
    await message.answer("Authorization successful. You can now use /download.")


@router.message(F.chat.type == "private", Command("cancel"))
async def cancel(message: Message, state: FSMContext) -> None:
    svc = get_service()
    if not svc.is_owner(message):
        return
    await state.clear()
    svc.pending_phone = svc.pending_code_hash = None
    await message.answer("Current interaction cancelled.")


@router.message(F.chat.type == "private", Command("download"))
async def download(message: Message, command: CommandObject) -> None:
    svc = get_service()
    if not svc.is_owner(message):
        return
    if not await svc.client.is_user_authorized():
        await message.answer("Telegram account is not authorized. Run /auth first.")
        return
    if not command.args:
        await message.answer("Usage: /download https://t.me/channel/123")
        return
    try:
        link = parse_message_link(command.args)
    except ValueError as exc:
        await message.answer(str(exc))
        return
    progress = await message.answer("Download started…")
    svc.active_downloads += 1
    try:
        paths = await svc.downloader.download(link)
        names = "\n".join(f"• {path.name}" for path in paths)
        await progress.edit_text(f"Download completed ({len(paths)} file(s)):\n{names}")
    except DownloadError as exc:
        await progress.edit_text(f"Download failed: {exc}")
    finally:
        svc.active_downloads -= 1


@router.message(
    F.chat.type == "private",
    F.document | F.video | F.audio | F.voice | F.photo,
)
async def direct_media(message: Message) -> None:
    """Download media sent or forwarded directly to the Bot (Bot API size limit applies)."""
    svc = get_service()
    if not svc.is_owner(message):
        return
    media = message.document or message.video or message.audio or message.voice
    if message.photo:
        photo = message.photo[-1]
        file_id = photo.file_id
        filename = f"telegram-photo-{message.message_id}.jpg"
        file_size = photo.file_size
    elif media:
        file_id = media.file_id
        filename = getattr(media, "file_name", None) or f"telegram-{message.message_id}.bin"
        file_size = getattr(media, "file_size", None)
    else:
        return
    if file_size and file_size > 20 * 1024 * 1024:
        await message.answer(
            "This file exceeds the standard Telegram Bot API download limit (20 MB). "
            "Send the original message link with /download instead."
        )
        return

    source_key = f"bot-{message.chat.id}-{message.message_id}"
    safe_name = safe_filename(filename)
    partial = stable_partial_path(svc.settings.incomplete_dir, source_key, safe_name)
    final = svc.settings.complete_dir / f"{source_key}-{safe_name}"
    progress = await message.answer("Direct media download started…")
    svc.active_downloads += 1
    try:
        if not (final.exists() and (not file_size or final.stat().st_size == file_size)):
            await message.bot.download(file=file_id, destination=partial)
            if file_size and partial.stat().st_size != file_size:
                raise DownloadError(
                    f"Downloaded file size mismatch ({partial.stat().st_size}/{file_size} bytes)."
                )
            final.parent.mkdir(parents=True, exist_ok=True)
            partial.replace(final)
        await progress.edit_text(f"Download completed:\n{final.name}")
    except Exception as exc:
        logger.exception("Direct Bot media download failed")
        await progress.edit_text(f"Direct media download failed: {type(exc).__name__}.")
    finally:
        svc.active_downloads -= 1


def build_dispatcher(settings: Settings, client: TelegramClient) -> Dispatcher:
    global service
    prepare_directories(settings.complete_dir, settings.incomplete_dir)
    service = BotService(settings, client)
    from aiogram.fsm.storage.memory import MemoryStorage

    dispatcher = Dispatcher(storage=MemoryStorage())
    dispatcher.include_router(router)
    return dispatcher
