from pathlib import Path

import pytest

from app.config import Settings


def set_required_environment(monkeypatch):
    monkeypatch.setenv("BOT_TOKEN", "test-token")
    monkeypatch.setenv("API_ID", "12345")
    monkeypatch.setenv("API_HASH", "test-hash")
    monkeypatch.setenv("OWNER_TELEGRAM_ID", "123456")


def test_global_download_concurrency_defaults_to_one(monkeypatch):
    set_required_environment(monkeypatch)
    monkeypatch.delenv("MAX_CONCURRENT_DOWNLOADS", raising=False)

    settings = Settings.from_env()

    assert settings.max_concurrent_downloads == 1


def test_global_download_concurrency_rejects_parallel_configuration(monkeypatch):
    set_required_environment(monkeypatch)
    monkeypatch.setenv("MAX_CONCURRENT_DOWNLOADS", "2")

    with pytest.raises(ValueError, match="must be 1"):
        Settings.from_env()


def test_download_paths_must_be_telegram_scoped_siblings(tmp_path: Path):
    with pytest.raises(ValueError, match="Telegram-only"):
        Settings(
            bot_token="test-token",
            api_id=12345,
            api_hash="test-hash",
            owner_telegram_id=123456,
            data_dir=tmp_path / "state",
            complete_dir=tmp_path / "downloads" / "complete" / "qbittorrent",
            incomplete_dir=tmp_path / "downloads" / "incomplete" / "telegram",
        )


def test_download_paths_reject_symlink_escape(tmp_path: Path):
    outside_complete = tmp_path / "outside" / "complete" / "telegram"
    outside_complete.mkdir(parents=True)
    complete_link = tmp_path / "downloads" / "complete" / "telegram"
    complete_link.parent.mkdir(parents=True)
    complete_link.symlink_to(outside_complete, target_is_directory=True)

    with pytest.raises(ValueError, match="real sibling"):
        Settings(
            bot_token="test-token",
            api_id=12345,
            api_hash="test-hash",
            owner_telegram_id=123456,
            data_dir=tmp_path / "state",
            complete_dir=complete_link,
            incomplete_dir=tmp_path / "downloads" / "incomplete" / "telegram",
        )


def test_default_download_paths_are_telegram_scoped(monkeypatch):
    set_required_environment(monkeypatch)
    monkeypatch.delenv("MAX_CONCURRENT_DOWNLOADS", raising=False)
    monkeypatch.delenv("COMPLETE_DIR", raising=False)
    monkeypatch.delenv("INCOMPLETE_DIR", raising=False)

    settings = Settings.from_env()

    assert settings.complete_dir == Path("/downloads/complete/telegram")
    assert settings.incomplete_dir == Path("/downloads/incomplete/telegram")



def test_data_directory_rejects_symlink_escape(tmp_path: Path):
    real_data = tmp_path / "real-state"
    real_data.mkdir()
    data_link = tmp_path / "state-link"
    data_link.symlink_to(real_data, target_is_directory=True)

    with pytest.raises(ValueError, match="DATA_DIR must not contain symlinks"):
        Settings(
            bot_token="test-token",
            api_id=12345,
            api_hash="test-hash",
            owner_telegram_id=123456,
            data_dir=data_link,
            complete_dir=tmp_path / "downloads" / "complete" / "telegram",
            incomplete_dir=tmp_path / "downloads" / "incomplete" / "telegram",
        )



def test_data_directory_must_be_outside_download_mount(tmp_path: Path):
    downloads = tmp_path / "downloads"
    with pytest.raises(ValueError, match="outside the shared downloads mount"):
        Settings(
            bot_token="test-token",
            api_id=12345,
            api_hash="test-hash",
            owner_telegram_id=123456,
            data_dir=downloads / "state",
            complete_dir=downloads / "complete" / "telegram",
            incomplete_dir=downloads / "incomplete" / "telegram",
        )
