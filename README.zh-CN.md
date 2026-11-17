# Telegram Media Downloader

基于 Telegram Bot + Telethon 的私有媒体下载服务。Bot 负责交互与任务管理；Telethon 使用已授权的 Telegram 用户账号读取消息链接并下载媒体。服务不会主动监控频道或群组。

## 功能

- 仅允许配置的 Telegram 用户 ID 操作 Bot。
- 通过 Bot 完成 Telethon 用户账号登录，支持登录验证码和两步验证（2FA）。
- 支持公开消息链接 `t.me/<channel>/<message_id>`、私有消息链接 `t.me/c/<channel_id>/<message_id>`；账号必须有权访问源消息。
- 按相册识别并下载消息中的全部媒体。
- 支持直接发送或转发照片、文件、视频、音频和语音给 Bot；此方式受标准 Bot API **20 MB 下载限制**。
- 链接下载支持断点续传、文件大小校验和原子完成；直接 Bot API 下载不支持偏移续传。
- SQLite 持久化 FIFO 任务队列；单 worker 串行处理所有下载来源，重启后恢复未完成任务。
- 持久化重试计划和 Telegram FloodWait 全局冷却时间。
- 定期检查用户账号授权状态；失效时通知并提供重新授权入口。

## 部署

要求 Docker Engine 和 Docker Compose 插件。

### 目录

Compose 文件使用以下宿主机目录：

```text
项目目录/
├── compose.yaml
├── .env
└── data/                         # SQLite、Telethon Session（SSD）
/data/media/downloads/
├── complete/telegram/            # 完成文件（数据盘）
└── incomplete/telegram/          # 下载中的文件（数据盘）
```

创建目录并设置权限。容器以 UID/GID `1001:1001` 运行：

```bash
mkdir -p data
chmod 700 data
mkdir -p /data/media/downloads/complete/telegram
mkdir -p /data/media/downloads/incomplete/telegram
chown -R 1001:1001 data /data/media/downloads/complete/telegram /data/media/downloads/incomplete/telegram
chmod 755 /data/media/downloads /data/media/downloads/complete /data/media/downloads/incomplete
```

### 配置

从 [@BotFather](https://t.me/BotFather) 获取 Bot Token；从 [my.telegram.org](https://my.telegram.org) 获取 Telegram API ID 和 API Hash。填写自己的数字 Telegram 用户 ID 作为唯一操作用户。

```bash
cp .env.example .env
chmod 600 .env
```

编辑 `.env`：

| 变量 | 说明 |
| --- | --- |
| `BOT_TOKEN` | BotFather 提供的 Bot Token |
| `API_ID` | Telegram API ID |
| `API_HASH` | Telegram API Hash |
| `OWNER_TELEGRAM_ID` | 唯一允许操作 Bot 的数字用户 ID |
| `LOG_LEVEL` | 可选，日志级别，默认 `INFO` |
| `MAX_CONCURRENT_DOWNLOADS` | 必须为 `1`；所有来源共用单 worker |

启动：

```bash
docker compose up -d --build
docker compose logs -f
```

在 Telegram 私聊 Bot，发送 `/start`。首次使用时执行 `/auth` 完成用户账号授权。

## Bot 命令

| 命令 | 作用 |
| --- | --- |
| `/start` | 显示服务入口 |
| `/auth` | 授权或重新授权 Telegram 用户账号 |
| `/download <message-link>` | 将消息链接加入下载队列 |
| `/status` | 查看 Bot、账号授权、活动任务和全局冷却状态 |
| `/tasks` | 查看最近 10 个任务及进度、重试信息 |
| `/retry <task_id>` | 重新加入已失败或已取消的任务 |
| `/cancel <task_id>` | 取消排队中或正在执行的任务 |
| `/cancel` | 取消当前授权交互 |

也可以直接向 Bot 发送或转发照片、文件、视频、音频或语音，创建直接媒体下载任务。

## 下载行为

### 消息链接

- 支持公开和私有消息链接；私有频道/群组必须已在授权用户账号中可访问。
- 相册链接会下载该相册中的媒体。
- 下载通过 Telethon 用户账号执行，不受标准 Bot API 的 20 MB 下载限制。
- 不完整文件保留在 `incomplete/telegram`，重试时从最后一个 4 KiB 对齐位置继续。
- 下载完成前校验媒体大小，再通过原子重命名移动到 `complete/telegram`。
- 文件引用过期时会尝试从原消息刷新一次；只有媒体对象身份未变化才继续。

### 直接发送给 Bot

- 处理照片、文件、视频、音频和语音。
- 受标准 Bot API 20 MB 下载限制；文件大小缺失或无效时拒绝入队。
- Bot API 不提供按字节偏移续传；中断后重试会从头下载。
- 直接媒体任务与链接任务共用同一 FIFO worker。

### 队列与重试

- 同一来源存在未结束任务时，不会重复创建任务。
- 同一时刻只执行一个下载任务。
- 网络类临时错误最多自动尝试 5 次，采用指数退避，延迟上限 5 分钟。
- Telegram FloodWait 会暂停整个 worker，并持久化冷却时间。
- 用户账号需要重新授权时，任务会暂停；授权恢复后自动重新入队。
- 超过自动重试次数的任务标记为失败；可用 `/retry` 手动重试。
- 取消或失败的 Telethon 部分文件会保留；直接 Bot API 任务重试时从零开始。

## 存储与权限

- `data/`：SQLite 任务数据库、Telethon Session 和进程锁；应放在 SSD，权限为 `700`。
- `/data/media/downloads/incomplete/telegram/`：下载中的部分文件。
- `/data/media/downloads/complete/telegram/`：已完成文件。
- 容器以 UID/GID `1001:1001` 运行；完成文件权限为 `644`，部分文件为 `600`。
- 服务只写入上述两个 Telegram 专用目录，不会写入 qBittorrent 等其他媒体目录。
- `DATA_DIR` 必须与共享下载挂载路径分离；下载目录必须是同一 downloads 根目录下的两个兄弟目录。

## 备份与恢复

`data/` 包含 Telethon Session 和持久化任务数据库。备份数据库前先停止容器，以确保 SQLite WAL 状态完整：

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

媒体文件位于数据盘，需单独备份。恢复时停止服务，恢复 `.env` 和 `data/`，重新设置 `data/` 为 `700`、`.env` 为 `600`，并确保容器用户 `1001:1001` 对目录有正确权限，再启动服务。

## Session 异常恢复

遇到持续的 `AuthKeyNotFound` 时，不要直接删除 Session。先等待并重试授权；若问题持续，先停止服务并备份状态，再将旧 Session 文件改名保留，让 Telethon 创建新 Session：

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

随后向 Bot 发送 `/auth` 重新登录。备份和旧 Session 均包含账号授权材料，必须妥善保管。

## 开发与测试

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

GitHub Actions 会执行 lint、单元测试、字节码编译、Compose 配置校验和镜像构建。测试使用 mock，不会执行真实 Telegram 登录或下载。

## 限制

- 仅支持可访问且未删除、未开启内容保护的消息；受保护内容无法下载。
- 私有消息链接要求授权账号已加入相应频道或群组。
- 直接发送给 Bot 的媒体受标准 Bot API 20 MB 下载限制；超过限制请发送原始消息链接，通过 Telethon 下载。
- 服务不会监控频道/群组，也不提供从 LXC 本地目录向 Telegram 上传文件的功能。
- `.env`、`data/telegram_user.session` 和备份文件均属于敏感凭据，不得提交或公开。
