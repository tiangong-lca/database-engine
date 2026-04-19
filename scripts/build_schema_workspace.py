from __future__ import annotations

import argparse

from _db_workflow import add_database_args, build_schema_workspace


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Split the remote database schema dump into a human-readable, editable workspace."
    )
    add_database_args(parser)
    args = parser.parse_args()

    path = build_schema_workspace(
        environment=args.environment,
        db_url=args.db_url,
        schemas=args.schemas,
    )
    print(f"Wrote {path}")


if __name__ == "__main__":
    main()
