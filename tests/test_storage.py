from pathlib import Path

from app.storage import reserve_incomplete_path, safe_filename, stable_partial_path


def test_safe_filename_removes_path_and_invalid_characters():
    assert safe_filename("../../bad:name?.mp4") == "bad_name_.mp4"


def test_reserve_incomplete_path_avoids_overwrite(tmp_path: Path):
    first = reserve_incomplete_path(tmp_path, "clip.mp4")
    first.touch()
    second = reserve_incomplete_path(tmp_path, "clip.mp4")
    assert first.name == "clip.mp4.part"
    assert second.name == "clip-1.mp4.part"


def test_stable_partial_path_is_reused_only_for_same_source_identity(tmp_path: Path):
    source_key = "channel-1-message-42-document-9001"
    first = stable_partial_path(tmp_path, source_key)
    first.write_bytes(b"partial")

    second = stable_partial_path(tmp_path, source_key)
    different_media = stable_partial_path(
        tmp_path, "channel-1-message-42-document-9002"
    )

    assert first == second
    assert second.read_bytes() == b"partial"
    assert different_media != first
    assert not different_media.exists()
