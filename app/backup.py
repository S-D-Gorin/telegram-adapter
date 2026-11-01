"""Online SQLite snapshots for external backup tooling."""

from __future__ import annotations

import argparse
import os
import sqlite3
import sys
import tempfile
from collections.abc import Callable
from contextlib import closing
from pathlib import Path

from app.config import Config, ConfigError, load_config


class BackupError(ValueError):
    """A backup could not be created or verified."""


def verify_backup(path: Path) -> None:
    """Check a standalone snapshot without writing to it or creating sidecars."""
    if not path.is_file():
        raise BackupError(f"Backup file does not exist: {path}")
    with path.open("rb") as snapshot:
        if snapshot.read(16) != b"SQLite format 3\x00":
            raise BackupError(f"Not a SQLite database: {path}")
    uri = path.resolve().as_uri() + "?mode=ro&immutable=1"
    with closing(sqlite3.connect(uri, uri=True)) as connection:
        results = [row[0] for row in connection.execute("PRAGMA integrity_check")]
    if results != ["ok"]:
        raise BackupError(f"SQLite integrity_check failed for {path}: {results}")


def create_backup(
    destination: Path,
    *,
    config: Config,
    overwrite: bool = False,
    progress: Callable[[int, int, int], None] | None = None,
) -> None:
    """Copy the configured live database and publish only a verified snapshot."""
    source = config.database_path
    if not source.is_file():
        raise BackupError(f"Source database does not exist: {source}")
    if source.resolve() == destination.resolve():
        raise BackupError("Backup destination must differ from the source database")
    if destination.exists() and not overwrite:
        raise BackupError(f"Backup already exists: {destination} (use --overwrite to replace it)")

    # The temporary database must share a filesystem with the final path.
    descriptor, temporary_name = tempfile.mkstemp(prefix=f".{destination.name}.", suffix=".tmp", dir=destination.parent)
    temporary = Path(temporary_name)
    try:
        os.close(descriptor)
        source_uri = source.resolve().as_uri() + "?mode=ro"
        with closing(sqlite3.connect(source_uri, uri=True)) as source_connection:
            with closing(sqlite3.connect(temporary)) as target_connection:
                source_connection.backup(target_connection, pages=64, progress=progress, sleep=0.25)
        verify_backup(temporary)

        # Flush the verified file before making the name visible to backup tooling.
        with temporary.open("rb+") as snapshot:
            os.fsync(snapshot.fileno())
        if overwrite:
            os.replace(temporary, destination)
        else:
            # Hard-link creation is atomic and fails if the destination appeared
            # during the copy; os.rename would silently overwrite it on POSIX.
            os.link(temporary, destination)
            temporary.unlink()
    finally:
        temporary.unlink(missing_ok=True)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Create or verify an online SQLite backup")
    commands = parser.add_subparsers(dest="command", required=True)
    create = commands.add_parser("create", help="create a verified snapshot of the configured database")
    create.add_argument("file", type=Path)
    create.add_argument("--overwrite", action="store_true", help="replace an existing backup")
    verify = commands.add_parser("verify", help="check a standalone snapshot read-only")
    verify.add_argument("file", type=Path)
    args = parser.parse_args(argv)

    try:
        if args.command == "create":
            create_backup(args.file, config=load_config(), overwrite=args.overwrite)
        else:
            verify_backup(args.file)
    except (OSError, sqlite3.Error, ConfigError, BackupError) as error:
        print(f"Backup {args.command} failed: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
