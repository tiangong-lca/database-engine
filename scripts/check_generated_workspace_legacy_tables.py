#!/usr/bin/env python3
"""Fail if generated schema workspace still advertises retired job tables."""

from __future__ import annotations

import argparse
from pathlib import Path
import re
import sys


RETIRED_TABLES = (
    "lca_jobs",
    "lca_package_jobs",
    "dataset_review_submit_jobs",
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Check that supabase/workspace generated schema output no longer "
            "contains public lca_jobs, lca_package_jobs, or "
            "dataset_review_submit_jobs table definitions."
        )
    )
    parser.add_argument(
        "--root",
        type=Path,
        default=Path.cwd(),
        help="database-engine repo root (default: current directory)",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    root = args.root.resolve()
    workspace = root / "supabase" / "workspace"
    findings: list[str] = []

    remote_schema = workspace / "remote_schema.sql"
    if remote_schema.exists():
        sql = remote_schema.read_text(encoding="utf-8", errors="replace")
        for table in RETIRED_TABLES:
            pattern = re.compile(
                rf'CREATE\s+TABLE(?:\s+IF\s+NOT\s+EXISTS)?\s+"?public"?\."?{table}"?\b',
                re.IGNORECASE,
            )
            if pattern.search(sql):
                findings.append(f"{remote_schema}: contains public.{table} table definition")

    generated_tables_root = workspace / "schemas" / "public" / "tables"
    for table in RETIRED_TABLES:
        table_dir = generated_tables_root / table
        if table_dir.exists():
            findings.append(f"{table_dir}: retired generated table directory still exists")

    if findings:
        print("Stale generated schema workspace content found:", file=sys.stderr)
        for finding in findings:
            print(f"- {finding}", file=sys.stderr)
        print(
            "\nRefresh with: python scripts/build_schema_workspace.py --environment dev",
            file=sys.stderr,
        )
        return 1

    print("Generated schema workspace does not advertise retired legacy job tables.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
