# Telegram Media Downloader

A private, manually triggered Telegram media downloader. A Telegram Bot provides the control interface, while Telethon uses your own Telegram account to read message links and download media. The service does **not** monitor channels or groups.

## Features

- Private Bot commands restricted to one configured Telegram user ID.
- Interactive MTProto login through the Bot: phone number, login code, and optional 2FA password.
- Automatic authorization checks every five minutes, with a private Bot notification and a one-tap reauthorization button when the session is unavailable.
- Public message links (`t.me/<channel>/<message_id>`) and private message links (`t.me/c/<channel_id>/<message_id>`), provided the authorized account can access the source.
- Album-aware message download.
- Directly sent or forwarded Bot media for files up to the standard Bot API download limit (20 MB).
- Link-based downloads keep partial files in the HDD-backed incomplete directory and resume from the last 4 KiB-aligned byte offset. File data is fsynced every 8 MiB and again before finalization.
- Completed files are atomically moved into the complete directory.
- Persistent Telethon session stored in the data volume.
- SQLite task history uses WAL mode and full synchronous commits on the SSD-backed application volume. Message-link tasks interrupted by a process/container restart are replayed automatically after Telegram authorization is available.

## Requirements

- Docker Engine with the Compose plugin.
- A Telegram Bot token from [@BotFather](https://t.me/BotFather).
- Telegram API ID and API Hash from [my.telegram.org](https://my.telegram.org).
- Your numeric Telegram user ID.

## Deployment

The compose file follows this host layout:

```text
/data/docker/telegram-media-downloader/
/data/media/downloads/complete/telegram/
/data/media/downloads/incomplete/telegram/
```

Create the application and download directories, then set ownership to UID/GID 1000 (the same account used by qBittorrent):

```bash
mkdir -p data
mkdir -p /data/media/downloads/complete/telegram
mkdir -p /data/media/downloads/incomplete/telegram
chown -R 1000:1000 data /data/media/downloads/complete/telegram /data/media/downloads/incomplete/telegram
```

Configure secrets:

```bash
cp .env.example .env
chmod 600 .env
nano .env
```

Fill in `BOT_TOKEN`, `API_ID`, `API_HASH`, and `OWNER_TELEGRAM_ID`. Do not commit `.env`, Telegram session files, or real credentials.

Build and start:

```bash
docker compose up -d --build
```

View logs:

```bash
docker compose logs -f
```

Open a private chat with the Bot and send `/start` once so it can send you status and authorization notifications.

## Bot commands

- `/start` — show available commands.
- `/auth` — start or renew the user-account authorization flow.
- `/download <message-link>` — download media from a Telegram message.
- `/status` — show Bot, account authorization, and active download status.
- `/tasks` — show the latest 10 download tasks, including completed, failed, and interrupted tasks.
- `/cancel` — cancel the current authorization conversation.
- Send or forward a media file directly to the Bot for a small-file download.

Only the configured owner ID is allowed to use the Bot. The user account must already have access to the linked channel or group. Protected content, deleted messages, and inaccessible private sources cannot be downloaded.

## Storage layout

```text
/data/media/downloads/
├── complete/
│   ├── qbittorrent/
│   └── telegram/
└── incomplete/
    ├── qbittorrent/
    └── telegram/
```

The service mounts only its own Telegram subdirectories. It does not mount or modify qBittorrent's directories. The SQLite database and Telethon session live under the project data directory (SSD); partial and completed media files live on the HDD.

## Security

- Keep `.env` and `data/telegram_user.session` private; both contain credentials or account access material.
- Restrict access to the project directory and its `data` subdirectory.
- Use the Bot only in a private chat. Login code and 2FA password messages are deleted on a best-effort basis immediately after receipt.
- Do not share logs or session files publicly. The application avoids logging login codes, passwords, API hashes, and Bot tokens.

## Development and tests

```bash
python -m venv .venv
. .venv/bin/activate
pip install -e '.[dev]'
ruff check .
pytest -q
python -m compileall -q app
```

GitHub Actions runs lint, unit tests, and Python bytecode compilation on pushes and pull requests.

## Current limitations

- Directly sent or forwarded media uses the standard Bot API download endpoint and is limited to 20 MB. For larger files, send the original message link with `/download` so Telethon downloads it through your authorized account.
- Link-based downloads automatically retry transient network interruptions and short transfers up to five times with exponential backoff. Partial data is flushed to disk before retry. If all retries are exhausted, the task is marked failed but its partial file is retained; resend the same link to continue. Message-link tasks interrupted by a restart are automatically replayed from the persisted task ledger after the MTProto account is authorized. Direct Bot API media tasks cannot currently be replayed after restart because Bot API file identifiers are not persisted.
- Downloads are serialized by default (`MAX_CONCURRENT_DOWNLOADS=1`) to keep session and disk behavior predictable.
- Authorization prompts are private-chat only. Use `/cancel` to abandon an in-progress login.
