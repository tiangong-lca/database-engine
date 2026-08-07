#!/usr/bin/env python3
"""Resolve the latest checked-in Supabase migration version."""

from __future__ import annotations

import argparse
import re
import sys
from collections import Counter
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_MIGRATIONS = REPO_ROOT / "supabase" / "migrations"
MIGRATION_NAME = re.compile(r"^(?P<version>\d{14})_.+\.sql$")


def resolve_migration_head(migrations: Path = DEFAULT_MIGRATIONS) -> str:
    paths = sorted(migrations.glob("*.sql"))
    if not paths:
        raise ValueError(f"no SQL migrations found in {migrations}")

    versions: list[str] = []
    invalid: list[str] = []
    for path in paths:
        match = MIGRATION_NAME.fullmatch(path.name)
        if match is None:
            invalid.append(path.name)
        else:
            versions.append(match.group("version"))

    if invalid:
        raise ValueError(
            "invalid migration filenames: " + ", ".join(sorted(invalid))
        )

    duplicates = sorted(
        version for version, count in Counter(versions).items() if count > 1
    )
    if duplicates:
        raise ValueError(
            "duplicate migration versions: " + ", ".join(duplicates)
        )

    return max(versions)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Print the latest checked-in Supabase migration version."
    )
    parser.add_argument(
        "--migrations-dir",
        type=Path,
        default=DEFAULT_MIGRATIONS,
        help="Migration directory (default: supabase/migrations).",
    )
    args = parser.parse_args()

    try:
        print(resolve_migration_head(args.migrations_dir))
    except ValueError as error:
        print(f"ERROR: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
