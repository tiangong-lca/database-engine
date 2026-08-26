#!/usr/bin/env python3
"""Fail when the immutable Portal projection-v1 function closure is replaced."""

from __future__ import annotations

import re
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
MIGRATIONS_DIR = REPO_ROOT / "supabase" / "migrations"
ANCHOR_NAME = "20260826060422_portal_candidate_first_search.sql"
FLOW_ELIGIBILITY_INDEX_NAME = (
    "20260827010000_portal_flow_embedding_eligibility_index.sql"
)
FLOW_ELIGIBILITY_GUARD_NAME = (
    "20260827010003_portal_flow_embedding_eligibility_guard.sql"
)
FACET_ANCHOR_NAME = "20260827020000_portal_facet_projection_expand.sql"
FACET_RECONCILE_NAME = "20260827020005_portal_facet_projection_reconcile.sql"
FACET_CUTOVER_NAME = "20260827020006_portal_facet_projection_cutover.sql"
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
FACET_MANIFEST_SHA256 = (
    "b238e9573ef08a9339062a2fa3092c0776318d13979ec8bf54ffc7a1ba0c7e3a"
)
FACET_FUNCTION_IDENTITIES = (
    "private.portal_catalog_facet_facts_v1(text,jsonb)",
    "private.sync_portal_catalog_facet_row_v1()",
)
FACET_CONTROL_FUNCTION_IDENTITIES = (
    "private.portal_catalog_facet_manifest_sha256_v1()",
    "private.assert_portal_catalog_facet_contract_v1()",
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

    facet_anchor = MIGRATIONS_DIR / FACET_ANCHOR_NAME
    if not facet_anchor.is_file():
        violations.append(f"missing Portal facet manifest anchor: {FACET_ANCHOR_NAME}")
    else:
        facet_anchor_sql = facet_anchor.read_text(encoding="utf-8")
        if FACET_MANIFEST_SHA256 not in facet_anchor_sql:
            violations.append("Portal facet manifest digest literal is absent")
        for identity in FACET_FUNCTION_IDENTITIES + FACET_CONTROL_FUNCTION_IDENTITIES:
            if identity not in facet_anchor_sql:
                violations.append(f"Portal facet manifest anchor omits: {identity}")

    facet_protected = FACET_FUNCTION_IDENTITIES + FACET_CONTROL_FUNCTION_IDENTITIES
    facet_patterns = {
        identity: mutation_pattern(identity) for identity in facet_protected
    }
    facet_probe = (
        "create or replace function "
        "private.portal_catalog_facet_facts_v1(text,jsonb)"
    )
    if not facet_patterns[FACET_FUNCTION_IDENTITIES[0]].search(facet_probe):
        violations.append("Portal facet manifest mutation scanner self-test failed")
    for migration in sorted(MIGRATIONS_DIR.glob("*.sql")):
        if migration.name <= FACET_ANCHOR_NAME:
            continue
        executable_sql = sql_without_comments(migration.read_text(encoding="utf-8"))
        for identity, pattern in facet_patterns.items():
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

    backfill_migrations = sorted(
        MIGRATIONS_DIR.glob("2026082608*_portal_projection_backfill_*.sql")
    )
    if len(backfill_migrations) != 16:
        violations.append(
            "expected exactly 16 Portal projection backfill migrations, "
            f"found {len(backfill_migrations)}"
        )
    backfill_timeout_pattern = re.compile(
        r"\bbegin\s*;\s*"
        r"set\s+local\s+lock_timeout\s*=\s*'5s'\s*;\s*"
        r"set\s+local\s+statement_timeout\s*=\s*'120s'\s*;",
        flags=re.IGNORECASE,
    )
    for migration in backfill_migrations:
        executable_sql = sql_without_comments(migration.read_text(encoding="utf-8"))
        if not backfill_timeout_pattern.search(executable_sql):
            violations.append(
                f"{migration.name}: missing outer 5s lock / 120s statement timeout"
            )

    facet_backfills = sorted(
        MIGRATIONS_DIR.glob("2026082702000[1-4]_portal_facet_projection_backfill_*.sql")
    )
    if len(facet_backfills) != 4:
        violations.append(
            "expected exactly four Portal facet projection backfills, "
            f"found {len(facet_backfills)}"
        )
    expected_facet_ranges = (
        ("00000000-0000-0000-0000-000000000000", "40000000-0000-0000-0000-000000000000"),
        ("40000000-0000-0000-0000-000000000000", "80000000-0000-0000-0000-000000000000"),
        ("80000000-0000-0000-0000-000000000000", "c0000000-0000-0000-0000-000000000000"),
        ("c0000000-0000-0000-0000-000000000000", None),
    )
    for migration, (lower, upper) in zip(facet_backfills, expected_facet_ranges):
        executable_sql = sql_without_comments(
            migration.read_text(encoding="utf-8")
        ).lower()
        required_tokens = (
            "set local lock_timeout = '5s'",
            "set local statement_timeout = '120s'",
            "grant api_internal_executor to postgres",
            "set role api_internal_executor",
            "reset role",
            "revoke api_internal_executor from postgres",
            "assert_portal_catalog_projection_contract_v1",
            "assert_portal_catalog_facet_contract_v1",
            "portal_catalog_facet_facts_v1",
            "on conflict (dataset_kind, id, version) do nothing",
            "where projection.dataset_kind = 'process'",
            "where projection.dataset_kind = 'flow'",
            "where facet.dataset_kind = 'process'",
            "where facet.dataset_kind = 'flow'",
            "facet.state_code is distinct from projection.state_code",
            "facet.modified_at is distinct from projection.modified_at",
            "facet.facet_contract_version is distinct from 1",
            lower,
        )
        missing = [token for token in required_tokens if token not in executable_sql]
        if upper is not None and upper not in executable_sql:
            missing.append(upper)
        if missing:
            violations.append(
                f"{migration.name}: missing facet backfill tokens "
                + ", ".join(missing)
            )
        if re.search(
            r"(?:insert\s+into|update|delete\s+from)\s+"
            r"private[.]portal_catalog_search_rows_v1",
            executable_sql,
        ):
            violations.append(
                f"{migration.name}: must not mutate the immutable parent projection"
            )

    for migration_name, required_tokens in {
        FACET_RECONCILE_NAME: (
            "lock table private.portal_catalog_search_rows_v1",
            "lock table private.portal_catalog_facet_rows_v1",
            "on conflict (dataset_kind, id, version) do nothing",
            "portal facet projection reconciliation failed",
        ),
        FACET_CUTOVER_NAME: (
            "v_query = '' and v_filters = '{}'::jsonb",
            "catalog_portal_facets_empty_v1_impl",
            "catalog_portal_facets_v1_impl",
            "portal_catalog_facet_rows_v1",
            "set work_mem = '32mb'",
            "portal facet cutover contract drifted",
        ),
    }.items():
        migration = MIGRATIONS_DIR / migration_name
        if not migration.is_file():
            violations.append(f"missing Portal facet migration: {migration_name}")
            continue
        executable_sql = sql_without_comments(
            migration.read_text(encoding="utf-8")
        ).lower()
        missing = [token for token in required_tokens if token not in executable_sql]
        if missing:
            violations.append(
                f"{migration_name}: missing facet rollout tokens "
                + ", ".join(missing)
            )

    eligibility_index = MIGRATIONS_DIR / FLOW_ELIGIBILITY_INDEX_NAME
    if not eligibility_index.is_file():
        violations.append(
            f"missing Flow embedding eligibility migration: {FLOW_ELIGIBILITY_INDEX_NAME}"
        )
    else:
        eligibility_sql = sql_without_comments(
            eligibility_index.read_text(encoding="utf-8")
        )
        eligibility_pattern = re.compile(
            r"\s*create\s+index\s+concurrently\s+"
            r"flows_portal_embedding_eligible_v1_idx\s+"
            r"on\s+public[.]flows\s+using\s+btree\s*"
            r"[(]\s*state_code\s*[)]\s*"
            r"where\s+state_code\s+in\s*[(]\s*100\s*,\s*200\s*[)]\s*"
            r"and\s+embedding_ft\s+is\s+not\s+null\s*;\s*",
            flags=re.IGNORECASE,
        )
        if not eligibility_pattern.fullmatch(eligibility_sql):
            violations.append(
                f"{FLOW_ELIGIBILITY_INDEX_NAME}: must be one exact concurrent "
                "partial btree statement without IF NOT EXISTS"
            )

    eligibility_guard = MIGRATIONS_DIR / FLOW_ELIGIBILITY_GUARD_NAME
    if not eligibility_guard.is_file():
        violations.append(
            f"missing Flow embedding eligibility guard: {FLOW_ELIGIBILITY_GUARD_NAME}"
        )
    else:
        guard_sql = sql_without_comments(
            eligibility_guard.read_text(encoding="utf-8")
        ).lower()
        required_guard_tokens = (
            "set local lock_timeout = '5s'",
            "set local statement_timeout = '8s'",
            "flows_portal_embedding_eligible_v1_idx",
            "index_catalog.indisvalid",
            "index_catalog.indisready",
            "index_catalog.indislive",
            "index_catalog.indnkeyatts = 1",
            "index_catalog.indnatts = 1",
            "first_key.attname = 'state_code'",
            "first_opclass_namespace.nspname = 'pg_catalog'",
            "first_opclass.opcname = 'int4_ops'",
            "first_opclass.opcintype = 'pg_catalog.int4'::pg_catalog.regtype",
            "((state_code=any(array[100,200]))and(embedding_ftisnotnull))",
            "portal flow embedding eligibility index drifted",
        )
        missing_guard_tokens = [
            token for token in required_guard_tokens if token not in guard_sql
        ]
        if missing_guard_tokens:
            violations.append(
                f"{FLOW_ELIGIBILITY_GUARD_NAME}: missing guard tokens "
                + ", ".join(missing_guard_tokens)
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
        f"sha256={MANIFEST_SHA256}; facet closure "
        f"{len(FACET_FUNCTION_IDENTITIES)} derivation functions and "
        f"{len(FACET_CONTROL_FUNCTION_IDENTITIES)} controls, "
        f"sha256={FACET_MANIFEST_SHA256}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
