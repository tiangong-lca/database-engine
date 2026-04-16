from __future__ import annotations

import argparse
from pathlib import Path

from _db_workflow import add_database_args, export_remote_schema


def main() -> None:
    parser = argparse.ArgumentParser(description="Export the remote schema of a target database.")
    add_database_args(parser)
    parser.add_argument("--schema-file", "--output-file", dest="schema_file")
    args = parser.parse_args()

    path = export_remote_schema(
        environment=args.environment,
        db_url=args.db_url,
        schema_file=Path(args.schema_file) if args.schema_file else None,
        schemas=args.schemas,
    )
    print(f"Wrote {path}")


if __name__ == "__main__":
    main()
