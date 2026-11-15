from __future__ import annotations

import asyncio
import fcntl
import logging
import os

from aiogram import Bot
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup
from telethon import TelegramClient

from .bot import build_dispatcher, get_service
from .config import Settings


async def monitor_authorization(bot: Bot, owner_id: int) -> None:
    """Notify the owner once when the MTProto session becomes unauthorized."""
    previous: bool | None = None
    auth_key_warning_sent = False
    while True:
        try:
            authorized, resumed = await get_service().reconcile_authorization()
            if resumed:
                await bot.send_message(
                    owner_id,
                    (
                        "Telegram account is authorized; "
                        f"{resumed} paused task(s) were returned to the queue."
                    ),
                )
            if not authorized and previous is not False:
                markup = InlineKeyboardMarkup(
                    inline_keyboard=[
                        [InlineKeyboardButton(text="重新授权", callback_data="auth:start")]
                    ]
                )
                await bot.send_message(
                    owner_id,
                    "⚠️ Telegram 用户账号授权已失效或尚未完成。点击下方按钮重新授权。",
                    reply_markup=markup,
                )
            previous = authorized
            auth_key_warning_sent = False
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            retry_after = getattr(exc, "retry_after", None)
            if retry_after is not None:
                seconds = getattr(retry_after, "total_seconds", None)
                delay = seconds() if callable(seconds) else float(retry_after)
                get_service().note_flood_wait(delay)
            # AuthKeyNotFound is ambiguous: Telegram documents it as potentially
            # temporary or permanent. Warn once, but never erase the session automatically.
            if type(exc).__name__ == "AuthKeyNotFound" and not auth_key_warning_sent:
                await bot.send_message(
                    owner_id,
                    "Telegram 暂时无法识别当前 MTProto Session。这可能是临时服务器问题；"
                    "请稍后重试。如果持续出现，请按项目 README 的 "
                    "“Recovering an unrecognized Telethon session” 步骤备份并重建 Session。"
                    "服务不会自动删除或覆盖旧 Session。",
                )
                auth_key_warning_sent = True
            # A transient RPC/network failure is not proof that authorization expired.
            logging.getLogger(__name__).warning(
                "Authorization monitor check failed: %s", type(exc).__name__
            )
        await asyncio.sleep(900)


async def main() -> None:
    settings = Settings.from_env()
    logging.basicConfig(
        level=getattr(logging, settings.log_level, logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    # Session files, the task database, and the process lock contain account state.
    os.umask(0o077)
    settings.data_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
    settings.data_dir.chmod(0o700)
    lock_file = (settings.data_dir / "service.lock").open("a+")
    try:
        fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError as exc:
        lock_file.close()
        raise RuntimeError(
            "Another Telegram Media Downloader instance is already running."
        ) from exc

    client = TelegramClient(
        str(settings.data_dir / "telegram_user"), settings.api_id, settings.api_hash
    )
    await client.connect()
    bot = Bot(settings.bot_token, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    monitor_task: asyncio.Task | None = None
    worker_task: asyncio.Task | None = None
    try:
        dispatcher = build_dispatcher(settings, client)
        svc = get_service()
        worker_task = svc.start_worker(bot)
        monitor_task = asyncio.create_task(
            monitor_authorization(bot, settings.owner_telegram_id)
        )
        await dispatcher.start_polling(
            bot, allowed_updates=dispatcher.resolve_used_update_types()
        )
    finally:
        for task in (monitor_task, worker_task):
            if task is not None:
                task.cancel()
        await asyncio.gather(
            *(task for task in (monitor_task, worker_task) if task is not None),
            return_exceptions=True,
        )
        await bot.session.close()
        await client.disconnect()
        fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)
        lock_file.close()


if __name__ == "__main__":
    asyncio.run(main())
