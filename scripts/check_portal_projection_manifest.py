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
CARD_CONTEXT_ANCHOR_NAME = "20260827021441_portal_card_context_decorator.sql"
FLOW_GEOGRAPHY_SEARCH_NAME = (
    "20260827134100_optimize_portal_flow_geography_search.sql"
)
SITEMAP_LATEST_PROJECTION_NAME = (
    "20260827134101_portal_sitemap_latest_projection.sql"
)
SITEMAP_SHARD_CONTRACT_NAME = (
    "20260827134102_portal_sitemap_shard_contract.sql"
)
SITEMAP_REPAIR_NAME = "20260827134103_portal_sitemap_concurrency_repair.sql"
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
CARD_CONTEXT_MANIFEST_SHA256 = (
    "e0516d5f3a641d26221a5c44b92a2e7a87cab125e9145e8141074d9bc2af39fa"
)
CARD_CONTEXT_FUNCTION_IDENTITIES = (
    "private.portal_card_context_v1(text,integer,jsonb)",
    "private.portal_decorate_card_context_v1(jsonb)",
    "private.portal_process_reference_product_v1(jsonb)",
    "private.portal_process_functional_unit_v1(integer,jsonb)",
    "private.portal_exchange_support_v1(integer,jsonb,jsonb)",
    "private.portal_reference_flowproperty_v1(jsonb)",
    "private.portal_localized_text_v1(jsonb)",
    "private.portal_scalar_text_v1(jsonb)",
    "private.portal_json_items_v1(jsonb)",
    "private.portal_source_v1(text,jsonb)",
    "private.portal_capabilities_v1(text,integer,jsonb)",
    "private.portal_canonical_decimal_v1(text)",
    "private.portal_flow_kind_v1(text)",
    "private.portal_support_capabilities_v1(text,integer)",
    "private.portal_classifications_v1(jsonb)",
    "private.portal_publication_root_v1(text,jsonb)",
    "private.portal_access_restrictions_open_v1(jsonb)",
)
CARD_CONTEXT_CONTROL_FUNCTION_IDENTITIES = (
    "private.portal_card_context_manifest_sha256_v1()",
    "private.assert_portal_card_context_contract_v1()",
)
SITEMAP_SHARD_FUNCTION_IDENTITIES = (
    "private.sync_portal_sitemap_latest_row_v1()",
    "private.sync_portal_sitemap_latest_delete_v1()",
    "private.assert_portal_sitemap_projection_v1()",
    "api.portal_sitemap_manifest_v1()",
    "api.portal_sitemap_shard_v1(text)",
)


def sql_without_comments(sql: str) -> str:
    """Remove SQL comments while retaining executable strings and function bodies."""

    without_blocks = re.sub(r"/\*.*?\*/", " ", sql, flags=re.DOTALL)
    return re.sub(r"--[^\n]*", " ", without_blocks)


def mutation_pattern(identity: str) -> re.Pattern[str]:
    schema_name = identity.split(".", 1)[0]
    function_name = identity.split(".", 1)[1].split("(", 1)[0]
    return re.compile(
        rf"\b(?:"
        rf"create\s+(?:or\s+replace\s+)?function"
        rf"|drop\s+(?:function|routine)(?:\s+if\s+exists)?"
        rf"|alter\s+(?:function|routine)"
        rf")\s+"
        rf'(?:(?:"?{re.escape(schema_name)}"?)\s*\.\s*)?'
        rf'"?{re.escape(function_name)}"?\s*\(',
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

    context_anchor = MIGRATIONS_DIR / CARD_CONTEXT_ANCHOR_NAME
    if not context_anchor.is_file():
        violations.append(
            f"missing Portal card-context migration: {CARD_CONTEXT_ANCHOR_NAME}"
        )
    else:
        context_sql = sql_without_comments(
            context_anchor.read_text(encoding="utf-8")
        )
        context_sql_lower = context_sql.lower()
        if CARD_CONTEXT_MANIFEST_SHA256 not in context_sql:
            violations.append("Portal card-context manifest digest literal is absent")
        for identity in (
            CARD_CONTEXT_FUNCTION_IDENTITIES
            + CARD_CONTEXT_CONTROL_FUNCTION_IDENTITIES
        ):
            if identity not in context_sql:
                violations.append(
                    f"Portal card-context manifest anchor omits: {identity}"
                )
        required_context_tokens = (
            "begin;",
            "set local lock_timeout = '5s'",
            "set local statement_timeout = '120s'",
            "portal_process_functional_unit_v1(100, p_json)",
            "portal_decorate_card_context_v1",
            "jsonb_array_elements(p_page -> 'items')",
            "join public.processes as source",
            "join public.flows as source",
            "v_expected > (",
            "then 50",
            "else 20",
            "portal_card_context_writer_before",
            "private.assert_portal_card_context_contract_v1()",
            "commit;",
        )
        missing = [
            token for token in required_context_tokens if token not in context_sql_lower
        ]
        if missing:
            violations.append(
                f"{CARD_CONTEXT_ANCHOR_NAME}: missing selected-row tokens "
                + ", ".join(missing)
            )
        for wrapper in (
            "api.portal_search_processes_v1",
            "api.portal_search_flows_v1",
            "api.portal_hybrid_search_v1",
        ):
            wrapper_pattern = re.compile(
                rf"create\s+or\s+replace\s+function\s+{re.escape(wrapper)}\s*\(",
                flags=re.IGNORECASE,
            )
            if len(wrapper_pattern.findall(context_sql)) != 1:
                violations.append(
                    f"{CARD_CONTEXT_ANCHOR_NAME}: expected one {wrapper} replacement"
                )
        if re.search(
            r"\b(?:create\s+(?:unlogged\s+)?table|create\s+(?:unique\s+)?index|"
            r"create\s+trigger|alter\s+table)\b",
            context_sql,
            flags=re.IGNORECASE,
        ):
            violations.append(
                f"{CARD_CONTEXT_ANCHOR_NAME}: selected-row decorator must not add "
                "a table, index, trigger, or source-table rewrite"
            )

    context_protected = (
        CARD_CONTEXT_FUNCTION_IDENTITIES
        + CARD_CONTEXT_CONTROL_FUNCTION_IDENTITIES
    )
    context_patterns = {
        identity: mutation_pattern(identity) for identity in context_protected
    }
    for migration in sorted(MIGRATIONS_DIR.glob("*.sql")):
        if migration.name <= CARD_CONTEXT_ANCHOR_NAME:
            continue
        executable_sql = sql_without_comments(migration.read_text(encoding="utf-8"))
        for identity, pattern in context_patterns.items():
            if pattern.search(executable_sql):
                violations.append(f"{migration.name}: {identity}")

    flow_geography_search = MIGRATIONS_DIR / FLOW_GEOGRAPHY_SEARCH_NAME
    if not flow_geography_search.is_file():
        violations.append(
            f"missing Portal Flow geography Search migration: "
            f"{FLOW_GEOGRAPHY_SEARCH_NAME}"
        )
    else:
        geography_sql = sql_without_comments(
            flow_geography_search.read_text(encoding="utf-8")
        )
        geography_sql_lower = geography_sql.lower()
        required_geography_tokens = (
            "3fdf121819227c2885a23233ab406291c8310902cf413e3eca96f1f0809bb7f4",
            "cf1e7a540a9f0fe4370de7588160b7720eb97b94f641d6b3d019ab9780586936",
            "portal_latest_facts as materialized",
            "private.portal_catalog_facet_rows_v1",
            "facet.facet_geography",
            "portal_ordered_keys as materialized",
            "limit p_limit + 1",
            "join private.portal_catalog_search_rows_v1",
            "private.assert_portal_catalog_projection_contract_v1()",
            "private.assert_portal_catalog_facet_contract_v1()",
        )
        missing = [
            token
            for token in required_geography_tokens
            if token not in geography_sql_lower
        ]
        if missing:
            violations.append(
                f"{FLOW_GEOGRAPHY_SEARCH_NAME}: missing narrow Search tokens "
                + ", ".join(missing)
            )
        if len(
            re.findall(
                r"create\s+or\s+replace\s+function\s+"
                r'"?private"?\s*[.]\s*"?catalog_portal_search_v1_impl"?\s*[(]',
                geography_sql,
                flags=re.IGNORECASE,
            )
        ) != 1:
            violations.append(
                f"{FLOW_GEOGRAPHY_SEARCH_NAME}: expected one Search kernel replacement"
            )
        if re.search(
            r"\b(?:create\s+(?:unlogged\s+)?table|create\s+(?:unique\s+)?index|"
            r"create\s+trigger|alter\s+table)\b",
            geography_sql,
            flags=re.IGNORECASE,
        ):
            violations.append(
                f"{FLOW_GEOGRAPHY_SEARCH_NAME}: query-only repair must not add "
                "a table, index, trigger, or writer-table rewrite"
            )

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
            "select count(*)",
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

    sitemap_latest = MIGRATIONS_DIR / SITEMAP_LATEST_PROJECTION_NAME
    if not sitemap_latest.is_file():
        violations.append(
            "missing Portal sitemap latest projection: "
            f"{SITEMAP_LATEST_PROJECTION_NAME}"
        )
    else:
        sitemap_latest_sql = sql_without_comments(
            sitemap_latest.read_text(encoding="utf-8")
        ).lower()
        required_latest_tokens = (
            "create table private.portal_sitemap_latest_rows_v1",
            "primary key (dataset_kind, id)",
            "shard_no smallint not null",
            "check (shard_no between 0 and 63)",
            "create index portal_sitemap_latest_shard_v1_idx",
            "include (",
            "create function private.sync_portal_sitemap_latest_row_v1()",
            "create function private.sync_portal_sitemap_latest_delete_v1()",
            "security definer",
            "portal_sitemap_latest_sync_v1",
            "portal_sitemap_latest_delete_v1",
            "before delete",
            "for update",
            "pg_catalog.pg_advisory_xact_lock(",
            "pg_catalog.hashtextextended(",
            "on conflict (dataset_kind, id) do update",
            "excluded.version > portal_sitemap_latest_rows_v1.version",
            "pg_catalog.md5(",
            ") / 4",
            "select distinct on (facet.dataset_kind, facet.id)",
            "portal sitemap latest projection reconciliation failed",
        )
        missing_latest_tokens = [
            token
            for token in required_latest_tokens
            if token not in sitemap_latest_sql
        ]
        if missing_latest_tokens:
            violations.append(
                f"{SITEMAP_LATEST_PROJECTION_NAME}: missing latest projection tokens "
                + ", ".join(missing_latest_tokens)
            )
        if sitemap_latest_sql.count("pg_catalog.pg_advisory_xact_lock(") != 2:
            violations.append(
                f"{SITEMAP_LATEST_PROJECTION_NAME}: both writer helpers must acquire the identity advisory fence"
            )
        if "create index concurrently" in sitemap_latest_sql:
            violations.append(
                f"{SITEMAP_LATEST_PROJECTION_NAME}: the new index must be created "
                "on the empty latest table before its set-based backfill"
            )
        if "foreign key" in sitemap_latest_sql:
            violations.append(
                f"{SITEMAP_LATEST_PROJECTION_NAME}: exact-version FK would deadlock concurrent version deletes"
            )
        if "portal_sitemap_entries_v1" in sitemap_latest_sql:
            violations.append(
                f"{SITEMAP_LATEST_PROJECTION_NAME}: expand must not change a public RPC"
            )

    sitemap_contract = MIGRATIONS_DIR / SITEMAP_SHARD_CONTRACT_NAME
    if not sitemap_contract.is_file():
        violations.append(
            f"missing Portal sitemap shard contract: {SITEMAP_SHARD_CONTRACT_NAME}"
        )
    else:
        sitemap_contract_sql = sql_without_comments(
            sitemap_contract.read_text(encoding="utf-8")
        )
        sitemap_contract_lower = sitemap_contract_sql.lower()
        required_contract_tokens = (
            "api.portal_sitemap_manifest_v1()",
            "api.portal_sitemap_shard_v1(text)",
            "private.assert_portal_sitemap_projection_v1()",
            "generate_series(0, 63)",
            "'maxitems', 4096",
            "limit 4097",
            "jsonb_array_length(v_items) > 4096",
            "portal.public-sitemap-manifest.v1",
            "portal.public-sitemap-shard.v1",
            "03dd37bd0871c220fcd94cb2dec203ed",
            "45503a8c8455b9ae9e69bc15d150d97f",
            "4278224e16a7f1932d0f3debbc245b2b",
            "portal_sitemap_latest_rows_v1",
            "projection.shard_no = v_bucket",
            "v_cursor is distinct from pg_catalog.jsonb_build_object",
        )
        missing_contract_tokens = [
            token
            for token in required_contract_tokens
            if token not in sitemap_contract_lower
        ]
        if missing_contract_tokens:
            violations.append(
                f"{SITEMAP_SHARD_CONTRACT_NAME}: missing bounded shard tokens "
                + ", ".join(missing_contract_tokens)
            )
        if re.search(
            r"\b(?:create\s+(?:unlogged\s+)?table|create\s+trigger|alter\s+table)\b",
            sitemap_contract_sql,
            flags=re.IGNORECASE,
        ):
            violations.append(
                f"{SITEMAP_SHARD_CONTRACT_NAME}: sitemap shards must not add "
                "a writer table, trigger, or table rewrite"
            )
        if re.search(
            r"create\s+(?:or\s+replace\s+)?function\s+"
            r"api[.]portal_sitemap_entries_v1\s*[(]",
            sitemap_contract_sql,
            flags=re.IGNORECASE,
        ):
            violations.append(
                f"{SITEMAP_SHARD_CONTRACT_NAME}: retained sitemap v1 must stay unchanged"
            )

    sitemap_repair = MIGRATIONS_DIR / SITEMAP_REPAIR_NAME
    if not sitemap_repair.is_file():
        violations.append(
            f"missing Portal sitemap forward repair: {SITEMAP_REPAIR_NAME}"
        )
    else:
        sitemap_repair_sql = sql_without_comments(
            sitemap_repair.read_text(encoding="utf-8")
        ).lower()
        required_repair_tokens = (
            "create or replace function private.sync_portal_sitemap_latest_row_v1()",
            "create or replace function private.sync_portal_sitemap_latest_delete_v1()",
            "drop trigger if exists portal_sitemap_latest_sync_v1",
            "create trigger portal_sitemap_latest_sync_v1",
            "create trigger portal_sitemap_latest_delete_v1",
            "before delete",
            "for update",
            "pg_catalog.pg_advisory_xact_lock(",
            "pg_catalog.hashtextextended(",
            "create or replace function private.assert_portal_sitemap_projection_v1()",
            "create or replace function api.portal_sitemap_shard_v1",
            "45503a8c8455b9ae9e69bc15d150d97f",
            "4278224e16a7f1932d0f3debbc245b2b",
            "v_cursor is distinct from pg_catalog.jsonb_build_object",
            "on conflict (dataset_kind, id) do update",
            "portal sitemap latest concurrency repair did not converge",
        )
        missing_repair_tokens = [
            token
            for token in required_repair_tokens
            if token not in sitemap_repair_sql
        ]
        if missing_repair_tokens:
            violations.append(
                f"{SITEMAP_REPAIR_NAME}: missing forward repair tokens "
                + ", ".join(missing_repair_tokens)
            )
        if sitemap_repair_sql.count("pg_catalog.pg_advisory_xact_lock(") != 2:
            violations.append(
                f"{SITEMAP_REPAIR_NAME}: both repaired writer helpers must acquire the identity advisory fence"
            )
        if "foreign key" in sitemap_repair_sql:
            violations.append(
                f"{SITEMAP_REPAIR_NAME}: forward repair must preserve the FK-free latest table"
            )
        for identity in (
            "private.sync_portal_sitemap_latest_row_v1()",
            "private.sync_portal_sitemap_latest_delete_v1()",
            "private.assert_portal_sitemap_projection_v1()",
            "api.portal_sitemap_shard_v1(text)",
        ):
            function_name = identity.split(".", 1)[1].split("(", 1)[0]
            if sitemap_repair_sql.count(function_name) == 0:
                violations.append(
                    f"{SITEMAP_REPAIR_NAME}: repair omits {identity}"
                )

    sitemap_patterns = {
        identity: mutation_pattern(identity)
        for identity in SITEMAP_SHARD_FUNCTION_IDENTITIES
    }
    for migration in sorted(MIGRATIONS_DIR.glob("*.sql")):
        if migration.name <= SITEMAP_REPAIR_NAME:
            continue
        executable_sql = sql_without_comments(migration.read_text(encoding="utf-8"))
        for identity, pattern in sitemap_patterns.items():
            if pattern.search(executable_sql):
                violations.append(f"{migration.name}: {identity}")

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
        f"sha256={FACET_MANIFEST_SHA256}; selected-row context closure "
        f"{len(CARD_CONTEXT_FUNCTION_IDENTITIES)} derivation functions and "
        f"{len(CARD_CONTEXT_CONTROL_FUNCTION_IDENTITIES)} controls, "
        f"sha256={CARD_CONTEXT_MANIFEST_SHA256}; Flow geography Search "
        "repair remains query-only; sitemap shards remain fixed at 64 with "
        "an advisory-fenced latest-only projection/index, an AFTER INSERT/UPDATE "
        "direct-upsert trigger, a serialized BEFORE DELETE row-lock/fallback "
        "trigger, the 134103 forward repair, and a 4096-item fail-closed read cap"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
