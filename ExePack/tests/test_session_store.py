from pathlib import Path
import importlib


session_store = importlib.import_module("session_store")


def _configure_temp_store(monkeypatch, tmp_path: Path):
    history_dir = tmp_path / "history_data"
    sessions_file = history_dir / "sessions.json"
    monkeypatch.setattr(session_store, "HISTORY_DIR", str(history_dir))
    monkeypatch.setattr(session_store, "SESSIONS_FILE", str(sessions_file))


def test_get_save_and_reload_session(monkeypatch, tmp_path):
    _configure_temp_store(monkeypatch, tmp_path)

    session = session_store.get_session_for_file("a.csv")
    session["messages"].append({"role": "user", "content": "hello"})
    session_store.save_session_for_file("a.csv", session)

    loaded = session_store.get_session_for_file("a.csv")
    assert loaded["id"] == session["id"]
    assert loaded["messages"][0]["content"] == "hello"


def test_rename_session_file_key_updates_mapping(monkeypatch, tmp_path):
    _configure_temp_store(monkeypatch, tmp_path)

    session = session_store.get_session_for_file("old.csv")
    session_store.save_session_for_file("old.csv", session)

    ok = session_store.rename_session_file_key("old.csv", "new.csv")
    assert ok is True

    data = session_store._load_sessions_raw()
    assert "new.csv" in data["by_file"]
    assert "old.csv" not in data["by_file"]
    assert data["by_id"][session["id"]] == "new.csv"


def test_list_history_files_with_sessions_returns_sorted_csv(monkeypatch, tmp_path):
    _configure_temp_store(monkeypatch, tmp_path)
    history_dir = Path(session_store.HISTORY_DIR)
    history_dir.mkdir(parents=True, exist_ok=True)
    (history_dir / "20260101_a.csv").write_text("", encoding="utf-8")
    (history_dir / "20260102_b.csv").write_text("", encoding="utf-8")
    (history_dir / "note.txt").write_text("x", encoding="utf-8")

    files = session_store.list_history_files_with_sessions()
    assert files == ["20260102_b.csv", "20260101_a.csv"]


def test_load_sessions_raw_handles_corrupted_json(monkeypatch, tmp_path):
    _configure_temp_store(monkeypatch, tmp_path)
    sessions_file = Path(session_store.SESSIONS_FILE)
    sessions_file.parent.mkdir(parents=True, exist_ok=True)
    sessions_file.write_text("{not-json", encoding="utf-8")

    data = session_store._load_sessions_raw()
    assert data == {"by_file": {}, "by_id": {}}


def test_rename_session_file_key_returns_false_for_missing_source(monkeypatch, tmp_path):
    _configure_temp_store(monkeypatch, tmp_path)
    ok = session_store.rename_session_file_key("none.csv", "new.csv")
    assert ok is False
