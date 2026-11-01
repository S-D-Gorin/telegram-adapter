import sqlite3
import threading
from contextlib import closing
from pathlib import Path

import pytest

from app.backup import BackupError, create_backup, main, verify_backup
from app.config import Config


def config_for(data_dir: Path) -> Config:
    return Config(server_api="https://example.test", bot_token="test", data_dir=data_dir, log_level="INFO")


def create_source(path: Path) -> None:
    with closing(sqlite3.connect(path)) as connection:
        connection.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, value TEXT NOT NULL)")
        connection.execute("INSERT INTO items (value) VALUES ('first')")
        connection.commit()


def test_creates_and_verifies_snapshot(tmp_path: Path) -> None:
    source = tmp_path / "adapter.db"
    destination = tmp_path / "snapshot.db"
    create_source(source)

    create_backup(destination, config=config_for(tmp_path))
    original = destination.read_bytes()
    verify_backup(destination)
    assert destination.read_bytes() == original

    with closing(sqlite3.connect(destination)) as connection:
        assert connection.execute("SELECT value FROM items").fetchall() == [("first",)]
    assert not list(tmp_path.glob(".snapshot.db.*.tmp"))
    assert not list(tmp_path.glob("snapshot.db-*"))


def test_backup_allows_wal_writer_to_commit_during_copy(tmp_path: Path) -> None:
    source = tmp_path / "adapter.db"
    destination = tmp_path / "snapshot.db"
    keeper = sqlite3.connect(source)
    assert keeper.execute("PRAGMA journal_mode=WAL").fetchone() == ("wal",)
    keeper.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, payload BLOB NOT NULL)")
    keeper.executemany("INSERT INTO items (payload) VALUES (zeroblob(4096))", [()] * 1024)
    keeper.commit()
    assert Path(f"{source}-wal").is_file()

    start_write = threading.Event()
    wrote = threading.Event()
    errors: list[BaseException] = []

    def writer() -> None:
        try:
            assert start_write.wait(5)
            with closing(sqlite3.connect(source, timeout=2)) as connection:
                connection.execute("INSERT INTO items (payload) VALUES (zeroblob(4096))")
                connection.commit()
            wrote.set()
        except BaseException as error:
            errors.append(error)
            wrote.set()

    thread = threading.Thread(target=writer)
    thread.start()

    def progress(_status: int, remaining: int, _total: int) -> None:
        if remaining > 0 and not start_write.is_set():
            start_write.set()
            assert wrote.wait(5), "writer could not commit while the backup was in progress"

    try:
        create_backup(destination, config=config_for(tmp_path), progress=progress)
    finally:
        start_write.set()
        thread.join(timeout=5)
        keeper.close()

    assert not errors
    assert wrote.is_set()
    verify_backup(destination)
    with closing(sqlite3.connect(destination)) as connection:
        assert connection.execute("SELECT count(*) FROM items").fetchone()[0] in (1024, 1025)


def test_missing_source_does_not_publish_file(tmp_path: Path) -> None:
    destination = tmp_path / "snapshot.db"

    with pytest.raises(BackupError, match="Source database does not exist"):
        create_backup(destination, config=config_for(tmp_path))

    assert not destination.exists()
    assert not list(tmp_path.glob(".snapshot.db.*.tmp"))


def test_corrupt_backup_fails_read_only(tmp_path: Path) -> None:
    destination = tmp_path / "snapshot.db"
    destination.write_bytes(b"not a SQLite database")
    original = destination.read_bytes()

    assert main(["verify", str(destination)]) == 1
    assert destination.read_bytes() == original
    assert not list(tmp_path.glob("snapshot.db-*"))


def test_empty_backup_is_rejected(tmp_path: Path) -> None:
    destination = tmp_path / "snapshot.db"
    destination.touch()

    with pytest.raises(BackupError, match="Not a SQLite database"):
        verify_backup(destination)


def test_copy_error_removes_temporary_file(tmp_path: Path) -> None:
    (tmp_path / "adapter.db").write_bytes(b"not a SQLite database")
    destination = tmp_path / "snapshot.db"

    with pytest.raises(sqlite3.DatabaseError):
        create_backup(destination, config=config_for(tmp_path))
    assert not destination.exists()
    assert not list(tmp_path.glob(".snapshot.db.*.tmp"))


def test_existing_backup_requires_explicit_overwrite(tmp_path: Path) -> None:
    source = tmp_path / "adapter.db"
    destination = tmp_path / "snapshot.db"
    create_source(source)
    destination.write_bytes(b"keep me")

    with pytest.raises(BackupError, match="already exists"):
        create_backup(destination, config=config_for(tmp_path))
    assert destination.read_bytes() == b"keep me"

    create_backup(destination, config=config_for(tmp_path), overwrite=True)
    verify_backup(destination)


def test_source_cannot_be_used_as_destination(tmp_path: Path) -> None:
    source = tmp_path / "adapter.db"
    create_source(source)
    original = source.read_bytes()

    with pytest.raises(BackupError, match="must differ"):
        create_backup(source, config=config_for(tmp_path), overwrite=True)
    assert source.read_bytes() == original


def test_cli_uses_configured_data_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    create_source(tmp_path / "adapter.db")
    destination = tmp_path / "snapshot.db"
    monkeypatch.setenv("DATA_DIR", str(tmp_path))
    monkeypatch.setenv("SERVER_API", "https://example.test")
    monkeypatch.setenv("BOT_TOKEN", "test")

    assert main(["create", str(destination)]) == 0
    assert main(["verify", str(destination)]) == 0
    assert main(["create", str(destination)]) == 1
