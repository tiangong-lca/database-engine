#!/usr/bin/env python3
"""Fail when the immutable Portal projection-v1 function closure is replaced."""

from __future__ import annotations

import re
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
MIGRATIONS_DIR = REPO_ROOT / "supabase" / "migrations"
ANCHOR_NAME = "20260826060422_portal_candidate_first_search.sql"
MANIFEST_SHA256 = (
    "b5e0aff9abbffcc8d2dacaf559a5d1a8c993c20b647d0c70f0e4fa18eb06d2dc"
)
FUNCTION_IDENTITIES = (
    "private.catalog_portal_projection_payload_v1(text,integer,jsonb)",
    "private.portal_catalog_card_v1(text,integer,jsonb)",
    "private.portal_capabilities_v1(text,integer,jsonb)",
    "private.portal_publication_root_v1(text,jsonb)",
    "private.portal_access_restrictions_open_v1(jsonb)",
    "private.portal_scalar_text_v1(jsonb)",
    "private.portal_localized_text_v1(jsonb)",
    "private.portal_json_items_v1(jsonb)",
    "private.portal_classifications_v1(jsonb)",
    "private.portal_safe_year_v1(text)",
    "private.portal_source_v1(text,jsonb)",
)
CONTROL_FUNCTION_IDENTITIES = (
    "private.portal_catalog_projection_manifest_sha256_v1()",
    "private.assert_portal_catalog_projection_contract_v1()",
    "private.portal_projection_semantic_process_exact_v1(extensions.vector)",
    "private.portal_projection_semantic_flow_exact_v1(extensions.vector)",
)


def sql_without_comments(sql: str) -> str:
    """Remove SQL comments while retaining executable strings and function bodies."""

    without_blocks = re.sub(r"/\*.*?\*/", " ", sql, flags=re.DOTALL)
    return re.sub(r"--[^\n]*", " ", without_blocks)


def mutation_pattern(identity: str) -> re.Pattern[str]:
    function_name = identity.split(".", 1)[1].split("(", 1)[0]
    return re.compile(
        rf"\b(?:"
        rf"create\s+(?:or\s+replace\s+)?function"
        rf"|drop\s+(?:function|routine)(?:\s+if\s+exists)?"
        rf"|alter\s+(?:function|routine)"
        rf")\s+"
        rf'(?:(?:"?private"?)\s*\.\s*)?"?{re.escape(function_name)}"?\s*\(',
        flags=re.IGNORECASE,
    )


def main() -> int:
    anchor = MIGRATIONS_DIR / ANCHOR_NAME
    if not anchor.is_file():
        print(f"missing Portal projection manifest anchor: {anchor}", file=sys.stderr)
        return 1
    if len(FUNCTION_IDENTITIES) != 11 or len(set(FUNCTION_IDENTITIES)) != 11:
        print("Portal projection manifest must contain exactly 11 identities", file=sys.stderr)
        return 1

    anchor_sql = anchor.read_text(encoding="utf-8")
    if MANIFEST_SHA256 not in anchor_sql:
        print("Portal projection manifest digest literal is absent", file=sys.stderr)
        return 1
    missing_identities = [
        identity for identity in FUNCTION_IDENTITIES if identity not in anchor_sql
    ]
    if missing_identities:
        print(
            "Portal projection manifest anchor omits: "
            + ", ".join(missing_identities),
            file=sys.stderr,
        )
        return 1

    violations: list[str] = []
    protected_identities = FUNCTION_IDENTITIES + CONTROL_FUNCTION_IDENTITIES
    patterns = {identity: mutation_pattern(identity) for identity in protected_identities}
    pattern_probes = (
        "create function private.portal_scalar_text_v1(jsonb)",
        "create or replace function private.portal_scalar_text_v1(jsonb)",
        "drop function if exists private.portal_scalar_text_v1(jsonb)",
        "alter routine private.portal_scalar_text_v1(jsonb) owner to postgres",
        "execute 'drop function private.portal_scalar_text_v1(jsonb)'",
    )
    leaf_pattern = patterns["private.portal_scalar_text_v1(jsonb)"]
    if any(not leaf_pattern.search(probe) for probe in pattern_probes):
        print("Portal projection manifest mutation scanner self-test failed", file=sys.stderr)
        return 1
    control_probe = (
        "alter function private.assert_portal_catalog_projection_contract_v1() "
        "security invoker"
    )
    if not patterns[
        "private.assert_portal_catalog_projection_contract_v1()"
    ].search(control_probe):
        print("Portal projection control-function scanner self-test failed", file=sys.stderr)
        return 1
    for migration in sorted(MIGRATIONS_DIR.glob("*.sql")):
        if migration.name <= ANCHOR_NAME:
            continue
        executable_sql = sql_without_comments(migration.read_text(encoding="utf-8"))
        for identity, pattern in patterns.items():
            if pattern.search(executable_sql):
                violations.append(f"{migration.name}: {identity}")

    required_guard_counts = {
        "20260826080345_portal_projection_reconcile.sql": 1,
        "20260826080400_portal_projection_candidate_cutover.sql": 2,
        "20260826080403_portal_projection_facets.sql": 2,
    }
    guard_name = "assert_portal_catalog_projection_contract_v1"
    for migration_name, expected_count in required_guard_counts.items():
        migration = MIGRATIONS_DIR / migration_name
        if not migration.is_file():
            violations.append(f"missing guarded migration: {migration_name}")
            continue
        actual_count = sql_without_comments(
            migration.read_text(encoding="utf-8")
        ).count(guard_name)
        if actual_count != expected_count:
            violations.append(
                f"{migration_name}: expected {expected_count} {guard_name} calls, "
                f"found {actual_count}"
            )

    if violations:
        print(
            "Portal projection-v1 manifest governance failed:\n- "
            + "\n- ".join(violations),
            file=sys.stderr,
        )
        return 1

    print(
        "Portal projection-v1 manifest is immutable: "
        f"{len(FUNCTION_IDENTITIES)} derivation functions and "
        f"{len(CONTROL_FUNCTION_IDENTITIES)} control functions, "
        f"sha256={MANIFEST_SHA256}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
