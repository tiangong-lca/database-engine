from __future__ import annotations

import argparse
from pathlib import Path

from _db_workflow import new_migration


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Generate a migration from a single schema object under "
            "`supabase/model/schemas` or `supabase/workspace/changes`."
        )
    )
    parser.add_argument("--name", required=True)
    parser.add_argument("--source-path", required=True)
    parser.add_argument("--migrations-directory")
    args = parser.parse_args()

    path = new_migration(
        name=args.name,
        source_path=Path(args.source_path),
        migrations_directory=Path(args.migrations_directory) if args.migrations_directory else None,
    )
    print(f"Wrote {path}")


if __name__ == "__main__":
    main()
