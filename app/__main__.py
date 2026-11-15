from __future__ import annotations

import asyncio
import fcntl
import logging

from aiogram import Bot
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup
from telethon import TelegramClient

from .bot import build_dispatcher, get_service
from .config import Settings


async def monitor_authorization(bot: Bot, client: TelegramClient, owner_id: int) -> None:
    """Notify the owner once when the MTProto session becomes unauthorized."""
    previous: bool | None = None
    while True:
        try:
            authorized = await client.is_user_authorized()
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
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            # A transient RPC/network failure is not proof that authorization expired.
            logging.getLogger(__name__).warning(
                "Authorization monitor check failed: %s", type(exc).__name__
            )
        await asyncio.sleep(300)


async def main() -> None:
    settings = Settings.from_env()
    logging.basicConfig(
        level=getattr(logging, settings.log_level, logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    settings.data_dir.mkdir(parents=True, exist_ok=True)
    lock_file = (settings.data_dir / "service.lock").open("a+")
    try:
        fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError as exc:
        lock_file.close()
        raise RuntimeError("Another Telegram Media Downloader instance is already running.") from exc

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
            monitor_authorization(bot, client, settings.owner_telegram_id)
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
