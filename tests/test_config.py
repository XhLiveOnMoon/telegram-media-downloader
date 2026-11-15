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
