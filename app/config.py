from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv


@dataclass(frozen=True, slots=True)
class Settings:
    bot_token: str
    api_id: int
    api_hash: str
    owner_telegram_id: int
    data_dir: Path
    complete_dir: Path
    incomplete_dir: Path
    max_concurrent_downloads: int = 1
    log_level: str = "INFO"

    def __post_init__(self) -> None:
        data_dir = Path(os.path.abspath(self.data_dir))
        if data_dir.resolve() != data_dir:
            raise ValueError("DATA_DIR must not contain symlinks.")
        complete = self.complete_dir.resolve()
        incomplete = self.incomplete_dir.resolve()
        valid_layout = (
            complete == self.complete_dir.absolute()
            and incomplete == self.incomplete_dir.absolute()
            and complete.name == "telegram"
            and complete.parent.name == "complete"
            and incomplete.name == "telegram"
            and incomplete.parent.name == "incomplete"
            and complete.parent.parent == incomplete.parent.parent
        )
        if not valid_layout:
            raise ValueError(
                "Download paths must be real sibling Telegram-only directories: "
                "<downloads>/complete/telegram and <downloads>/incomplete/telegram"
            )

    @classmethod
    def from_env(cls) -> Settings:
        load_dotenv()
        required = ("BOT_TOKEN", "API_ID", "API_HASH", "OWNER_TELEGRAM_ID")
        missing = [key for key in required if not os.getenv(key)]
        if missing:
            raise ValueError(f"Missing required environment variables: {', '.join(missing)}")

        api_id = int(os.environ["API_ID"])
        owner_id = int(os.environ["OWNER_TELEGRAM_ID"])
        concurrency = int(os.getenv("MAX_CONCURRENT_DOWNLOADS", "1"))
        if api_id <= 0 or owner_id <= 0:
            raise ValueError("API_ID and OWNER_TELEGRAM_ID must be positive")
        if concurrency != 1:
            raise ValueError(
                "MAX_CONCURRENT_DOWNLOADS must be 1; all download sources share one worker"
            )

        return cls(
            bot_token=os.environ["BOT_TOKEN"],
            api_id=api_id,
            api_hash=os.environ["API_HASH"],
            owner_telegram_id=owner_id,
            data_dir=Path(os.getenv("DATA_DIR", "./data")),
            complete_dir=Path(os.getenv("COMPLETE_DIR", "/downloads/complete/telegram")),
            incomplete_dir=Path(os.getenv("INCOMPLETE_DIR", "/downloads/incomplete/telegram")),
            max_concurrent_downloads=concurrency,
            log_level=os.getenv("LOG_LEVEL", "INFO").upper(),
        )
