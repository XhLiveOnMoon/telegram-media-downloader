# Telegram Media Downloader

A private, manually triggered Telegram media downloader. A Telegram Bot provides the control interface, while Telethon uses your own Telegram account to read message links and download media. The service does **not** monitor channels or groups.

## Features

- Private Bot commands restricted to one configured Telegram user ID.
- Interactive MTProto login through the Bot: phone number, login code, and optional 2FA password.
- Automatic authorization checks every 15 minutes, with a private Bot notification and a one-tap reauthorization button when the session is unavailable.
- Public message links (`t.me/<channel>/<message_id>`) and private message links (`t.me/c/<channel_id>/<message_id>`), provided the authorized account can access the source.
- Album-aware message download.
- Directly sent or forwarded Bot media for files up to the standard Bot API download limit (20 MB).
- Link-based downloads keep partial files in the HDD-backed incomplete directory and resume from the last 4 KiB-aligned byte offset. Document and photo transfers are checked against Telegram's advertised media size before promotion; truncated files remain partial and retryable. File data is fsynced every 8 MiB and again before finalization.
- Expired Telegram file references are refreshed from the original message before resuming. A refresh is accepted only when the Telegram media object ID is unchanged. Authorization failures pause the task instead of consuming network retries; successful reauthorization triggers recovery.
- Resume partial paths are keyed by source and media identity, not the display filename. Completed-file paths also include the media identity. Telethon uses the stable Document/Photo ID, while Bot API direct media uses `file_unique_id` (the separate `file_id` remains the download handle). Replaced media with the same message ID, filename, and size therefore cannot reuse old partial or completed files; a filename change for the same media can still resume the existing partial. Legacy files without identity-scoped names are left untouched and are not automatically reused or deleted.
- Completed files are atomically moved into the complete directory.
- Persistent Telethon session stored in the data volume.
- SQLite stores a durable FIFO queue, retry schedules, authorization pauses, and a global Telegram cooldown using WAL mode and full synchronous commits on the SSD-backed application volume. All download sources use one worker; only one task is active at a time. Interrupted work is recovered after restart.

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

Create the application and download directories. The Telegram container runs as the dedicated UID/GID 1001, separate from qBittorrent (UID/GID 1000):

```bash
mkdir -p data
chmod 700 data
mkdir -p /data/media/downloads/complete/telegram
mkdir -p /data/media/downloads/incomplete/telegram
chown -R 1001:1001 data /data/media/downloads/complete/telegram /data/media/downloads/incomplete/telegram
chmod 755 /data/media/downloads /data/media/downloads/complete /data/media/downloads/incomplete
```

The container mounts the downloads root once so incomplete-to-complete promotion stays on one mount and can use an atomic rename. It runs as UID 1001; the qBittorrent directories remain owned by UID 1000 and are not writable by this service. The application validates at startup that its complete and incomplete paths are sibling `telegram` directories; it refuses configurations pointing at qBittorrent or other media folders. It only writes under those two Telegram subdirectories. Telegram download directories are mode `755`, completed media files are mode `644` for read access by Jellyfin/other containers, and in-progress partial files remain private until promotion. The application state directory is mode `700`; the process umask keeps the Telethon session and SQLite state files private. `MAX_CONCURRENT_DOWNLOADS` must remain `1`; the setting is validated at startup.

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
- `/tasks` — show the latest 10 download tasks, per-file byte/percentage progress, retry attempts, and retry countdown. MTProto progress is persisted at 8 MiB fsync checkpoints; Bot API direct transfers expose completion progress only because the current download interface has no chunk callback.
- `/retry <task_id>` — requeue a failed or cancelled task.
- `/cancel [task_id]` — cancel a queued/active task; without an ID, cancel the current authorization conversation.
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

The service sees the shared downloads root through one HDD mount so a completed file can be atomically renamed from incomplete to complete. It runs as UID 1001, separate from qBittorrent's UID 1000, and only writes under `complete/telegram` and `incomplete/telegram`. The SQLite database and Telethon session live under the project data directory (SSD); partial and completed media files live on the HDD. At startup, `DATA_DIR` is rejected if it contains symlinks or shares an ancestor/descendant path with the downloads mount.

## Security

- Keep `.env` and `data/telegram_user.session` private; both contain credentials or account access material.
- Restrict access to the project directory and its `data` subdirectory.
- Use the Bot only in a private chat. Login code and 2FA password messages are deleted on a best-effort basis immediately after receipt.
- Do not share logs or session files publicly. The application avoids logging login codes, passwords, API hashes, and Bot tokens.

## Backup and restore

The `data/` directory contains the Telethon session and the durable task database. Stop the service before making a filesystem-level backup so SQLite WAL data is checkpointed:

```bash
set -Eeuo pipefail
umask 077
backup="$HOME/telegram-media-downloader-state-$(date +%F).tar.gz"
# Always attempt to restart the service, including when the archive operation fails.
trap 'docker compose start' EXIT
docker compose stop
tar -czf "$backup" .env data
chmod 600 "$backup"
docker compose start
trap - EXIT
```

Keep the archive private: the Telethon session can authorize access to the Telegram account. Back up the media directories on the data disk separately if the downloaded files must also be recoverable. To restore, stop the service, restore `.env` and `data/`, reapply ownership/permissions (`1001:1001`, `data/` mode `700`, `.env` mode `600`), then start the Compose service.

## Recovering an unrecognized Telethon session

Telethon's `AuthKeyNotFound` error is ambiguous: Telegram may temporarily fail to recognize the key, or the session may need to be recreated. The service deliberately does not delete or replace session files automatically. First wait and retry authorization; only use this recovery procedure if the error persists.

Run these commands from the project directory. They create a private backup, stop the service, preserve the old session files under timestamped names, and start the service with a fresh session:

```bash
set -Eeuo pipefail
umask 077
backup="$HOME/telegram-media-downloader-before-session-reset-$(date +%F-%H%M%S).tar.gz"
# Keep the service stopped while copying SQLite session state; restart on failure.
trap 'docker compose start' EXIT
docker compose stop
tar -czf "$backup" .env data
stamp=$(date +%F-%H%M%S)
for suffix in "" "-journal" "-wal" "-shm"; do
  file="data/telegram_user.session${suffix}"
  if [ -e "$file" ]; then
    mv "$file" "${file}.invalid-${stamp}"
  fi
done
docker compose up -d
trap - EXIT
```

Then send `/auth` to the Bot and complete login. Keep the backup and renamed session files private; they contain account authorization material. Do not run this procedure while the service is running.

## Development and tests

```bash
python -m venv .venv
. .venv/bin/activate
pip install -e '.[dev]'
ruff check .
pytest -q
python -m compileall -q app
```

GitHub Actions runs automatically for pull requests and can also be started manually from the repository Actions tab (`workflow_dispatch`). It runs lint, unit tests, Python bytecode compilation, Compose validation, and image build.

## Current limitations

- Directly sent or forwarded media uses the standard Bot API download endpoint and is limited to 20 MB. The Bot rejects direct media when Telegram omits or reports an invalid file size rather than bypassing the limit check. For larger files or missing size metadata, send the original message link with `/download` so Telethon downloads it through your authorized account.
- Link-based downloads preserve partial data and retry through the persistent worker schedule. If all worker attempts are exhausted, the task is marked failed and its partial file is retained; submit the same link again to continue. Both link and direct-media tasks are recorded in the queue; direct-media retries start from zero because the Bot API does not provide an offset-resume API.
- All link-based and direct Bot API downloads pass through one persistent FIFO worker. Multiple pending tasks are stored in SQLite (there is no configured queue-count cap; practical capacity is bounded by disk/database space), but only one task is active at a time. Each MTProto file uses one sequential asynchronous stream (512 KiB request chunks); there is no multi-threaded per-file download. `MAX_CONCURRENT_DOWNLOADS` is intentionally fixed at 1.
- Telegram FloodWait responses pause the entire worker for at least the server-specified duration. The cooldown and task retry deadline are persisted in SQLite, so a container restart cannot bypass the wait. Transient network failures are retried by the worker using persistent exponential backoff.
- Direct Bot API tasks persist their file identifier and can be replayed after a restart, but the Bot API download endpoint does not expose byte-offset resume; a retried direct-media transfer starts again from byte zero. Link-based Telethon downloads retain their partial files and resume from an aligned offset. Legacy direct-media tasks without a valid size field (or above 20 MB) are failed closed during upgrade rather than replayed without the size guard.
- Active downloads can be cancelled with `/cancel <task_id>`; a link-based partial file is retained and can be resumed with `/retry <task_id>`. Queued tasks can also be cancelled. A direct Bot API transfer restarts from byte zero after retry.
- Authorization prompts are private-chat only. Use `/cancel` without an ID to abandon an in-progress login.
- Unit tests use mocked Telegram clients; CI does not perform real Telegram login or download tests and never requires live account credentials.
