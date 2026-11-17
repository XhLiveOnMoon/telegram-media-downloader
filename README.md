# Telegram Media Downloader

A private, manually triggered Telegram media downloader. The Bot provides the control interface; Telethon uses an authorized Telegram user account to read message links and download media. The service does not monitor channels or groups.

## Features

- Bot access restricted to one configured Telegram user ID.
- Telethon user-account login through the Bot, including login codes and optional 2FA.
- Public message links (`t.me/<channel>/<message_id>`) and private message links (`t.me/c/<channel_id>/<message_id>`), subject to the account's access.
- Album-aware downloads.
- Direct photo, document, video, audio, and voice messages to the Bot; limited by the standard Bot API **20 MB download limit**.
- Link downloads with offset resume, media-size verification, and atomic finalization. Direct Bot API downloads restart from byte zero after interruption.
- Durable SQLite FIFO queue shared by all sources, processed by one worker and recovered after restart.
- Persistent retry scheduling and global Telegram FloodWait cooldown.
- Periodic account-authorization checks with owner notification and reauthorization flow.

## Deployment

Requires Docker Engine and the Docker Compose plugin.

### Directories

The Compose file uses this host layout:

```text
project/
├── compose.yaml
├── .env
└── data/                         # SQLite and Telethon Session (SSD)
/data/media/downloads/
├── complete/telegram/            # completed files (data disk)
└── incomplete/telegram/          # partial files (data disk)
```

Create the directories and set ownership. The container runs as UID/GID `1001:1001`:

```bash
mkdir -p data
chmod 700 data
mkdir -p /data/media/downloads/complete/telegram
mkdir -p /data/media/downloads/incomplete/telegram
chown -R 1001:1001 data /data/media/downloads/complete/telegram /data/media/downloads/incomplete/telegram
chmod 755 /data/media/downloads /data/media/downloads/complete /data/media/downloads/incomplete
```

### Configuration

Create a Bot token with [@BotFather](https://t.me/BotFather), obtain a Telegram API ID and API Hash from [my.telegram.org](https://my.telegram.org), and provide your numeric Telegram user ID as the sole authorized owner.

```bash
cp .env.example .env
chmod 600 .env
```

Edit `.env`:

| Variable | Description |
| --- | --- |
| `BOT_TOKEN` | BotFather token |
| `API_ID` | Telegram API ID |
| `API_HASH` | Telegram API Hash |
| `OWNER_TELEGRAM_ID` | Only Telegram user ID allowed to operate the Bot |
| `LOG_LEVEL` | Optional log level; defaults to `INFO` |
| `MAX_CONCURRENT_DOWNLOADS` | Must be `1`; all sources share one worker |

Start the service:

```bash
docker compose up -d --build
docker compose logs -f
```

Open a private chat with the Bot and send `/start`. Run `/auth` to authorize the Telegram user account before using link-based downloads.

## Bot commands

| Command | Description |
| --- | --- |
| `/start` | Show available commands |
| `/auth` | Authorize or reauthorize the Telegram user account |
| `/download <message-link>` | Queue a message-link download |
| `/status` | Show Bot, account authorization, active-task, and global cooldown status |
| `/tasks` | Show the latest 10 tasks, progress, and retry information |
| `/retry <task_id>` | Requeue a failed or cancelled task |
| `/cancel <task_id>` | Cancel a queued or active task |
| `/cancel` | Cancel the current authorization interaction |

You can also send or forward a photo, document, video, audio, or voice message directly to the Bot to create a direct-media task.

## Download behavior

### Message links

- Supports public and private message links. The authorized user account must be able to access private channels or groups.
- A link to an album downloads its media items.
- Downloads use the Telethon user account and are not subject to the standard Bot API 20 MB download limit.
- Partial files remain in `incomplete/telegram`; retries resume from the last 4 KiB-aligned offset.
- The media size is verified before an atomic move into `complete/telegram`.
- On an expired file reference, the original message is refreshed once. The download continues only if the media object identity is unchanged.

### Direct Bot media

- Handles photos, documents, videos, audio, and voice messages.
- Subject to the standard Bot API 20 MB download limit. Media with missing or invalid size metadata is rejected.
- The Bot API does not expose byte-offset resume; an interrupted transfer restarts from byte zero.
- Direct-media tasks share the same FIFO worker as message-link tasks.

### Queue and retries

- A duplicate source is not enqueued while an equivalent task is non-terminal.
- Only one download runs at a time.
- Recognized transient failures are retried automatically up to 5 attempts with exponential backoff, capped at 5 minutes.
- Telegram FloodWait pauses the entire worker; the cooldown is persisted.
- Tasks requiring account authorization are paused and automatically requeued after authorization is restored.
- Tasks that exhaust automatic retries are marked failed and can be manually requeued with `/retry`.
- Partial Telethon files are retained after cancellation or failure. Direct Bot API retries start from byte zero.

## Storage and permissions

- `data/`: SQLite task database, Telethon Session, and process lock. Keep it on SSD with mode `700`.
- `/data/media/downloads/incomplete/telegram/`: partial downloads.
- `/data/media/downloads/complete/telegram/`: completed downloads.
- The container runs as UID/GID `1001:1001`; completed files use mode `644`, and partial files use mode `600`.
- The service writes only to the two Telegram-specific directories; it does not write to qBittorrent or other media directories.
- A process lock prevents multiple instances from sharing the same application state.
- `DATA_DIR` must be separate from the shared downloads mount. The complete and incomplete directories must be sibling `telegram` directories under the same downloads root.

## Backup and restore

The `data/` directory contains the Telethon Session and durable task database. Stop the container before a filesystem-level backup so SQLite WAL state is included:

```bash
set -Eeuo pipefail
umask 077
backup="$HOME/telegram-media-downloader-state-$(date +%F).tar.gz"
trap 'docker compose start' EXIT
docker compose stop
tar -czf "$backup" .env data
chmod 600 "$backup"
docker compose start
trap - EXIT
```

Media files are stored on the data disk and must be backed up separately. To restore, stop the service, restore `.env` and `data/`, set `data/` to mode `700` and `.env` to mode `600`, verify ownership and permissions for UID/GID `1001:1001`, then start the service.

## Recovering an unrecognized Session

For a persistent `AuthKeyNotFound`, do not immediately delete the Session. Wait and retry authorization first. If the error persists, stop the service, back up its state, and rename the existing Session files so Telethon can create a new Session:

```bash
set -Eeuo pipefail
umask 077
backup="$HOME/telegram-media-downloader-before-session-reset-$(date +%F-%H%M%S).tar.gz"
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

Then send `/auth` to the Bot and sign in again. Backups and old Session files contain account authorization material and must be kept private.

## Development and tests

```bash
python -m venv .venv
. .venv/bin/activate
pip install -e '.[dev]'
ruff check .
pytest -q
python -m compileall -q app
docker compose config
docker compose build
```

GitHub Actions runs lint, unit tests, bytecode compilation, Compose validation, and image build. Tests use mocked Telegram clients; CI does not perform live Telegram login or downloads.

## Security

- Keep `.env`, `data/telegram_user.session`, and state backups private. The Session can authorize access to the Telegram user account.
- Do not share login messages, logs, or state files. The Bot is restricted to the configured owner ID and private chats.

## Limitations

- Only accessible, undeleted messages without content protection can be downloaded.
- Private message links require the authorized account to have access to the source channel or group.
- Direct media sent to the Bot is limited by the standard Bot API 20 MB download limit. For larger files, send the original message link and use Telethon.
- The service does not monitor channels or groups and does not upload files from an LXC directory to Telegram.
- `.env`, `data/telegram_user.session`, and backups contain sensitive credentials or account access material. Never commit or publish them.
