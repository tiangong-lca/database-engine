#!/usr/bin/env python3
"""Concurrent public/private review checksum parity for Issue #355."""

from __future__ import annotations

import concurrent.futures
import subprocess
from pathlib import Path

from identity_collaboration_target import resolve_target

ROOT = Path(__file__).resolve().parents[1]
QUERY = """
select public.review_scope_checksum_v1(
  jsonb_build_array(jsonb_build_object('item_kind','root','ordinal',g))
) = private.review_scope_checksum_v1(
  jsonb_build_array(jsonb_build_object('item_kind','root','ordinal',g))
)
from generate_series(1,100) g;
"""


def run_one(db_url: str) -> None:
    result = subprocess.run(
        ["psql", db_url, "-XAt", "-v", "ON_ERROR_STOP=1", "-c", QUERY],
        cwd=ROOT, check=True, text=True, stdout=subprocess.PIPE,
    )
    values = result.stdout.splitlines()
    if len(values) != 100 or set(values) != {"t"}:
        raise AssertionError("concurrent checksum adapter parity failed")


def main() -> int:
    db_url, _ = resolve_target()
    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
        futures = [executor.submit(run_one, db_url) for _ in range(8)]
        for future in futures:
            future.result()
    print("PASS 8 concurrent sessions / 800 public-private checksum parity calls")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
