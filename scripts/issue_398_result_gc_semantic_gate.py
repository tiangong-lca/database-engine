#!/usr/bin/env python3
"""Migration-specific semantic gate for the disabled-by-default result GC contract.

The final reviewed Git-blob and normalized PostgreSQL-AST receipts intentionally
remain unset until Issue #398's migration, pgTAP contract, and concurrency proof
are frozen together.  The structural profile below is fail closed on its own;
the final receipts add byte and whole-AST binding rather than replacing it.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections import Counter
from typing import Any, Iterable

from pglast import parser
from pglast.parser import ParseError


RESULT_GC_CLASSIFICATION = "additive-disabled-result-gc-contract-reviewed"
RESULT_GC_MIGRATION_PATH = (
    "supabase/migrations/20260802201933_issue_398_result_gc_contract.sql"
)
REVIEWED_GIT_BLOB: str | None = "6b2f41c028925c9f540f124887e576351b572e48"
REVIEWED_AST_SHA256: str | None = (
    "59e2b433b08017b16c75778168f1c7b524bca1b91d9e2deea5bcadf08d166e49"
)
PARSER_VERSION = 180004

EXECUTOR = "lca_result_gc_executor"
WORKER = "lca_worker_runtime"
CREATED_TABLES = {
    "lca_result_gc_control",
    "lca_result_gc_operations",
    "lca_result_gc_finalize_context",
    "lca_result_gc_attest_context",
}
INTERNAL_FUNCTIONS = {
    "lca_result_gc_error",
    "lca_result_gc_caller_allowed",
    "lca_result_gc_ineligibility_reason",
    "lca_result_gc_prepare_identity",
    "lca_result_gc_guard_result_write",
    "lca_result_gc_assert_reference_allowed",
    "lca_result_gc_guard_cache_reference",
    "lca_result_gc_guard_latest_reference",
    "lca_result_gc_guard_package_reference",
}
WORKER_FUNCTIONS = {
    "worker_lca_result_gc_attest_v1",
    "worker_lca_result_gc_preview_v1",
    "worker_lca_result_gc_claim_v1",
    "worker_lca_result_gc_renew_v1",
    "worker_lca_result_gc_fence_v1",
    "worker_lca_result_gc_finalize_v1",
    "worker_lca_result_gc_fail_v1",
}
ALL_FUNCTIONS = INTERNAL_FUNCTIONS | WORKER_FUNCTIONS
INVOKER_FUNCTIONS = {
    "lca_result_gc_error",
    "lca_result_gc_caller_allowed",
    "lca_result_gc_ineligibility_reason",
}
SQL_FUNCTIONS = INVOKER_FUNCTIONS

POLICIES = {
    "lca_result_gc_control_executor_all": ("private", "lca_result_gc_control", "all", True),
    "lca_result_gc_operations_executor_all": ("private", "lca_result_gc_operations", "all", True),
    "lca_result_gc_finalize_context_executor_all": ("private", "lca_result_gc_finalize_context", "all", True),
    "lca_result_gc_attest_context_executor_all": ("private", "lca_result_gc_attest_context", "all", True),
    "lca_results_gc_executor_select": ("public", "lca_results", "select", False),
    "lca_results_gc_executor_delete": ("public", "lca_results", "delete", False),
    "lca_results_gc_executor_update": ("public", "lca_results", "update", True),
    "lca_result_cache_gc_executor_select": ("public", "lca_result_cache", "select", False),
    "lca_latest_all_unit_results_gc_executor_select": (
        "public",
        "lca_latest_all_unit_results",
        "select",
        False,
    ),
    "lcia_result_packages_gc_executor_select": (
        "public",
        "lcia_result_packages",
        "select",
        False,
    ),
    "worker_jobs_result_gc_executor_select": ("private", "worker_jobs", "select", False),
}

TRIGGERS = {
    "lca_results_gc_prepare_identity": (
        ("public", "lca_results"),
        "lca_result_gc_prepare_identity",
        20,
        (
            "id",
            "job_id",
            "snapshot_id",
            "worker_job_id",
            "artifact_url",
            "artifact_sha256",
            "artifact_byte_size",
            "artifact_format",
            "created_at",
            "retention_partition_key",
        ),
    ),
    "lca_results_gc_write_fence": (
        ("public", "lca_results"),
        "lca_result_gc_guard_result_write",
        24,
        (),
    ),
    "lca_result_cache_gc_reference_fence": (
        ("public", "lca_result_cache"),
        "lca_result_gc_guard_cache_reference",
        20,
        ("result_id", "status"),
    ),
    "lca_latest_all_unit_results_gc_reference_fence": (
        ("public", "lca_latest_all_unit_results"),
        "lca_result_gc_guard_latest_reference",
        20,
        ("result_id",),
    ),
    "lcia_result_packages_gc_reference_fence": (
        ("public", "lcia_result_packages"),
        "lca_result_gc_guard_package_reference",
        20,
        ("result_id", "latest_all_unit_result_id"),
    ),
}

INDEXES = {
    "lca_results_gc_locator_uidx": (
        ("public", "lca_results"),
        True,
        ("artifact_url",),
        {"artifact_url", "retention_partition_key"},
    ),
    "lca_results_gc_partition_created_idx": (
        ("public", "lca_results"),
        False,
        ("retention_partition_key", "created_at", "id"),
        {"retention_partition_key"},
    ),
    "lca_result_gc_operations_active_target_uidx": (
        ("private", "lca_result_gc_operations"),
        True,
        ("target_result_id",),
        {"state"},
    ),
    "lca_result_gc_operations_claim_queue_idx": (
        ("private", "lca_result_gc_operations"),
        False,
        ("state", "lease_expires_at", "claimed_at", "operation_id"),
        {"state"},
    ),
}

TABLE_COLUMNS = {
    "lca_result_gc_control": {
        "singleton",
        "claims_enabled",
        "enabled_at",
        "enabled_by",
        "reason",
        "updated_at",
    },
    "lca_result_gc_operations": {
        "operation_id",
        "target_result_id",
        "live_result_id",
        "state",
        "generation",
        "claim_token",
        "claimed_by",
        "lease_expires_at",
        "retention_partition_key",
        "artifact_url",
        "artifact_sha256",
        "artifact_byte_size",
        "artifact_format",
        "claimed_at",
        "fenced_at",
        "finalized_at",
        "object_outcome",
        "last_error_code",
        "error_count",
        "updated_at",
    },
    "lca_result_gc_finalize_context": {
        "backend_pid",
        "transaction_id",
        "operation_id",
        "claim_token",
        "created_at",
    },
    "lca_result_gc_attest_context": {
        "backend_pid",
        "transaction_id",
        "result_id",
    },
}
TABLE_NAMED_CHECKS = {
    "lca_result_gc_control": {"lca_result_gc_control_enable_metadata_chk"},
    "lca_result_gc_operations": {
        "lca_result_gc_operations_state_chk",
        "lca_result_gc_operations_generation_chk",
        "lca_result_gc_operations_worker_chk",
        "lca_result_gc_operations_locator_chk",
        "lca_result_gc_operations_phase_chk",
    },
    "lca_result_gc_finalize_context": set(),
    "lca_result_gc_attest_context": set(),
}
TABLE_REQUIRED_STRINGS = {
    "lca_result_gc_control": {"issue_398_disabled_by_default"},
    "lca_result_gc_operations": {
        "claimed",
        "deleting",
        "finalizing",
        "finalized",
        "ineligible",
        "deleted",
        "missing",
        "^[0-9a-f]{64}$",
    },
    "lca_result_gc_finalize_context": set(),
    "lca_result_gc_attest_context": set(),
}

FUNCTION_REQUIRED_TOKENS = {
    "worker_lca_result_gc_attest_v1": {
        "lca_result_gc_caller_allowed",
        "lca_result_gc_attest_context",
        "retention_partition_key",
    },
    "worker_lca_result_gc_preview_v1": {
        "lca_result_gc_ineligibility_reason",
        "claims_enabled",
        "lca_results",
    },
    "worker_lca_result_gc_claim_v1": {
        "claims_enabled",
        "for update skip locked",
        "lca_result_gc_operations",
        "claim_token",
    },
    "worker_lca_result_gc_renew_v1": {
        "claim_token",
        "generation",
        "lease_expires_at",
    },
    "worker_lca_result_gc_fence_v1": {
        "claim_token",
        "generation",
        "deleting",
    },
    "worker_lca_result_gc_finalize_v1": {
        "lca_result_gc_finalize_context",
        "claim_token",
        "deleted",
        "missing",
    },
    "worker_lca_result_gc_fail_v1": {
        "claim_token",
        "last_error_code",
        "error_count",
    },
}

EXPECTED_ROOT_COUNTS = Counter(
    {
        "TransactionStmt": 2,
        "VariableSetStmt": 2,
        "DoStmt": 3,
        "CreateSchemaStmt": 1,
        "GrantStmt": 11,
        "AlterTableStmt": 10,
        "CommentStmt": 12,
        "IndexStmt": 4,
        "CreateStmt": 4,
        "InsertStmt": 1,
        "CreatePolicyStmt": 11,
        "CreateFunctionStmt": 16,
        "CreateTrigStmt": 5,
        "GrantRoleStmt": 2,
        "AlterOwnerStmt": 16,
        "NotifyStmt": 1,
    }
)


class ResultGcSemanticError(ValueError):
    pass


def _string_list(value: object) -> list[str]:
    if not isinstance(value, list):
        return []
    return [
        item.get("String", {}).get("sval")
        for item in value
        if isinstance(item, dict) and item.get("String", {}).get("sval") is not None
    ]


def _role_names(value: object) -> list[str]:
    if not isinstance(value, list):
        return []
    result = []
    for item in value:
        role = item.get("RoleSpec", {}) if isinstance(item, dict) else {}
        result.append(role.get("rolename") or role.get("roletype"))
    return result


def _range_identity(value: object) -> tuple[str | None, str | None]:
    row = value if isinstance(value, dict) else {}
    return row.get("schemaname"), row.get("relname")


def _walk(value: object) -> Iterable[tuple[str, dict[str, Any]]]:
    if isinstance(value, dict):
        for key, child in value.items():
            if key[:1].isupper() and isinstance(child, dict):
                yield key, child
            yield from _walk(child)
    elif isinstance(value, list):
        for child in value:
            yield from _walk(child)


def _scalar_strings(value: object) -> Iterable[str]:
    if isinstance(value, dict):
        if isinstance(value.get("sval"), str):
            yield value["sval"]
        for child in value.values():
            yield from _scalar_strings(child)
    elif isinstance(value, list):
        for child in value:
            yield from _scalar_strings(child)


def _without_locations(value: object) -> object:
    if isinstance(value, dict):
        return {
            key: _without_locations(child)
            for key, child in value.items()
            if key not in {"location", "stmt_location", "stmt_len"}
        }
    if isinstance(value, list):
        return [_without_locations(child) for child in value]
    return value


def _canonical(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode("utf-8")


def git_blob_oid(sql: str) -> str:
    payload = sql.encode("utf-8")
    return hashlib.sha1(
        b"blob " + str(len(payload)).encode("ascii") + b"\0" + payload
    ).hexdigest()


def _parse(sql: str) -> tuple[list[dict[str, Any]], list[str]]:
    if "\x00" in sql:
        return [], ["result-gc:nul-byte"]
    try:
        document = json.loads(parser.parse_sql_json(sql))
    except (ParseError, UnicodeDecodeError) as error:
        return [], [f"result-gc:parse-error:{type(error).__name__}"]
    if document.get("version") != PARSER_VERSION:
        return [], ["result-gc:parser-version-differs"]
    return [row["stmt"] for row in document.get("stmts", [])], []


def normalized_ast_sha256(sql: str) -> str:
    statements, errors = _parse(sql)
    if errors:
        raise ResultGcSemanticError(errors[0])
    return hashlib.sha256(_canonical(_without_locations(statements))).hexdigest()


def _function_options(root: dict[str, Any]) -> dict[str, object]:
    return {
        option.get("defname"): option.get("arg")
        for item in root.get("options", [])
        if (option := item.get("DefElem", {})).get("defname")
    }


def _function_body(options: dict[str, object]) -> str:
    values = list(_scalar_strings(options.get("as", {})))
    return values[0] if values else ""


def _without_sql_comments(value: str) -> str:
    """Remove PL/pgSQL comments before enforcing textual semantic anchors."""

    without_blocks = re.sub(r"/\*.*?\*/", " ", value, flags=re.DOTALL)
    return re.sub(r"--[^\n]*", " ", without_blocks)


def _function_search_path_is_safe(options: dict[str, object]) -> bool:
    setting = options.get("set", {})
    node = setting.get("VariableSetStmt", {}) if isinstance(setting, dict) else {}
    return node.get("name") == "search_path" and list(
        _scalar_strings(node.get("args", []))
    ) == ["pg_catalog", "pg_temp"]


def _object_names(root: dict[str, Any]) -> list[tuple[str, ...]]:
    names: list[tuple[str, ...]] = []
    for item in root.get("objects", []):
        if "RangeVar" in item:
            names.append(tuple(part for part in _range_identity(item["RangeVar"]) if part))
        elif "ObjectWithArgs" in item:
            names.append(tuple(_string_list(item["ObjectWithArgs"].get("objname", []))))
        elif "List" in item:
            names.append(tuple(_string_list(item["List"].get("items", []))))
        elif "String" in item:
            names.append((item["String"]["sval"],))
    return names


def _privileges(root: dict[str, Any]) -> list[tuple[str | None, tuple[str, ...]]]:
    result = []
    for item in root.get("privileges", []):
        privilege = item.get("AccessPriv", {})
        columns = tuple(_string_list(privilege.get("cols", [])))
        result.append((privilege.get("priv_name"), columns))
    return result


def _check_timeout(statement: dict[str, Any], violations: list[str]) -> None:
    root = statement["VariableSetStmt"]
    expected = {"lock_timeout": "5s", "statement_timeout": "2min"}
    if expected.get(root.get("name")) != next(iter(_scalar_strings(root.get("args", []))), None):
        violations.append("result-gc:timeout-setting-differs")


def _check_do_statements(rows: list[dict[str, Any]], violations: list[str]) -> None:
    bodies = []
    for row in rows:
        values = [
            item.get("DefElem", {}).get("arg", {})
            for item in row["DoStmt"].get("args", [])
            if item.get("DefElem", {}).get("defname") == "as"
        ]
        bodies.append(next(iter(_scalar_strings(values[0])), "") if len(values) == 1 else "")
    if len(bodies) != 3:
        violations.append("result-gc:do-count-differs")
    checked_bodies = [_without_sql_comments(body) for body in bodies]
    if any(
        re.search(r"\b(?:execute|format|dblink|http|net\.)\b", body, re.I)
        for body in checked_bodies
    ):
        violations.append("result-gc:do-dynamic-or-network-execution")
    preflight = next((body for body in checked_bodies if "supabase_migrations.schema_migrations" in body), "")
    role_body = next((body for body in checked_bodies if "create role lca_worker_runtime" in body.lower()), "")
    role_postflight = next(
        (
            body
            for body in checked_bodies
            if "role owner transfer did not restore the exact creator-edge baseline" in body.lower()
        ),
        "",
    )
    required_preflight = {
        "20260802190427",
        "public.lca_results",
        "public.lca_result_cache",
        "public.lca_latest_all_unit_results",
        "public.lcia_result_packages",
        "private.worker_jobs",
        "retention_partition_key",
        "pg_attribute",
    }
    if not preflight or any(token not in preflight for token in required_preflight):
        violations.append("result-gc:preflight-shape-differs")
    required_role = {
        "create role lca_worker_runtime",
        "create role lca_result_gc_executor",
        "nologin inherit nosuperuser nocreatedb nocreaterole nobypassrls",
        "rolsuper",
        "rolcreaterole",
        "rolcreatedb",
        "rolcanlogin",
        "rolbypassrls",
        "rolinherit",
        "pg_auth_members",
        "admin_option",
        "pg_has_role",
        "member =",
        "roleid =",
        "member_role.rolname in",
        "granted_role.rolname in",
        "'anon'",
        "'authenticated'",
        "'service_role'",
        "'api_internal_executor'",
        "v_runtime_existed",
        "v_executor_existed",
        "if not v_runtime_existed then",
        "if not v_executor_existed then",
        "grantor",
        "supabase_admin",
        "inherit_option",
        "set_option",
        "v_creator_edge_count <> 2",
        "two exact non-inheritable creator admin edges",
    }
    lowered = role_body.lower()
    if not role_body or any(token not in lowered for token in required_role):
        violations.append("result-gc:role-preflight-or-membership-shape-differs")
    required_postflight = {
        "pg_auth_members",
        "member_role.rolname = 'postgres'",
        "granted_role.rolname in",
        "auth_members.grantor = 'supabase_admin'::regrole",
        "auth_members.admin_option",
        "not auth_members.inherit_option",
        "not auth_members.set_option",
        "v_creator_edge_count <> 2",
        "v_unsafe_edge_count <> 0",
        "role owner transfer did not restore the exact creator-edge baseline",
    }
    lowered_postflight = role_postflight.lower()
    if not role_postflight or any(
        token not in lowered_postflight for token in required_postflight
    ):
        violations.append("result-gc:role-postflight-shape-differs")


def _check_alter_tables(rows: list[dict[str, Any]], violations: list[str]) -> None:
    seen_rls: Counter[tuple[str, str]] = Counter()
    saw_column = saw_constraint = False
    for statement in rows:
        root = statement["AlterTableStmt"]
        identity = _range_identity(root.get("relation", {}))
        commands = [item.get("AlterTableCmd", {}) for item in root.get("cmds", [])]
        if len(commands) != 1:
            violations.append("result-gc:alter-table-command-count-differs")
            continue
        command = commands[0]
        subtype = command.get("subtype")
        if identity == ("public", "lca_results") and subtype == "AT_AddColumn":
            column = command.get("def", {}).get("ColumnDef", {})
            if (
                column.get("colname") != "retention_partition_key"
                or column.get("raw_default") is not None
                or column.get("cooked_default") is not None
                or column.get("identity") is not None
                or column.get("generated") is not None
                or bool(column.get("constraints"))
            ):
                violations.append("result-gc:retention-column-shape-differs")
            saw_column = True
        elif identity == ("public", "lca_results") and subtype == "AT_AddConstraint":
            constraint = command.get("def", {}).get("Constraint", {})
            calls = [kind for kind, _ in _walk(constraint) if kind == "FuncCall"]
            strings = set(_scalar_strings(constraint))
            if (
                constraint.get("contype") != "CONSTR_CHECK"
                or constraint.get("conname") != "lca_results_retention_partition_key_chk"
                or constraint.get("skip_validation") is not True
                or calls
                or not {"retention_partition_key", "^[0-9a-f]{64}$"} <= strings
            ):
                violations.append("result-gc:retention-check-shape-differs")
            saw_constraint = True
        elif identity[0] == "private" and identity[1] in CREATED_TABLES and subtype in {
            "AT_EnableRowSecurity",
            "AT_ForceRowSecurity",
        }:
            seen_rls[(identity[1], subtype)] += 1
        else:
            violations.append("result-gc:unexpected-alter-table")
    expected_rls = Counter(
        (name, subtype)
        for name in CREATED_TABLES
        for subtype in ("AT_EnableRowSecurity", "AT_ForceRowSecurity")
    )
    if seen_rls != expected_rls:
        violations.append("result-gc:private-table-rls-shape-differs")
    if not saw_column or not saw_constraint:
        violations.append("result-gc:retention-expand-shape-incomplete")


def _column_ref_name(value: object) -> str | None:
    if not isinstance(value, dict):
        return None
    node = value.get("IndexElem", value)
    return node.get("name") if isinstance(node, dict) else None


def _check_indexes(rows: list[dict[str, Any]], violations: list[str]) -> None:
    seen = set()
    for statement in rows:
        root = statement["IndexStmt"]
        name = root.get("idxname")
        expected = INDEXES.get(name)
        if expected is None:
            violations.append("result-gc:unexpected-index")
            continue
        identity, unique, columns, predicate_columns = expected
        actual_columns = tuple(_column_ref_name(item) for item in root.get("indexParams", []))
        predicate = root.get("whereClause")
        referenced = {
            strings[-1]
            for kind, node in _walk(predicate)
            if kind == "ColumnRef" and (strings := _string_list(node.get("fields", [])))
        }
        dangerous = {
            kind for kind, _ in _walk(predicate) if kind in {"FuncCall", "SubLink", "SQLValueFunction"}
        }
        if (
            _range_identity(root.get("relation", {})) != identity
            or bool(root.get("unique")) != unique
            or actual_columns != columns
            or any(column is None for column in actual_columns)
            or referenced != predicate_columns
            or dangerous
            or predicate is None
        ):
            violations.append(f"result-gc:index-shape-differs:{name}")
        seen.add(name)
    if seen != set(INDEXES):
        violations.append("result-gc:index-set-differs")


def _check_created_tables(rows: list[dict[str, Any]], violations: list[str]) -> None:
    seen = set()
    for statement in rows:
        root = statement["CreateStmt"]
        schema, name = _range_identity(root.get("relation", {}))
        columns = {
            node.get("colname")
            for kind, node in _walk(root.get("tableElts", []))
            if kind == "ColumnDef"
        }
        named_checks = {
            node.get("conname")
            for kind, node in _walk(root.get("tableElts", []))
            if kind == "Constraint"
            and node.get("contype") == "CONSTR_CHECK"
            and node.get("conname")
        }
        strings = set(_scalar_strings(root.get("tableElts", [])))
        if (
            schema != "private"
            or name not in TABLE_COLUMNS
            or columns != TABLE_COLUMNS.get(name)
            or named_checks != TABLE_NAMED_CHECKS.get(name)
            or not TABLE_REQUIRED_STRINGS.get(name, set()) <= strings
        ):
            violations.append(f"result-gc:created-table-shape-differs:{name}")
        seen.add(name)
    if seen != CREATED_TABLES:
        violations.append("result-gc:created-table-set-differs")


def _check_control_insert(rows: list[dict[str, Any]], violations: list[str]) -> None:
    if len(rows) != 1:
        violations.append("result-gc:control-insert-count-differs")
        return
    root = rows[0]["InsertStmt"]
    strings = list(_scalar_strings(root))
    values = root.get("selectStmt", {}).get("SelectStmt", {}).get("valuesLists", [])
    items = values[0].get("List", {}).get("items", []) if len(values) == 1 else []
    booleans = [
        item.get("A_Const", {}).get("boolval", {}).get("boolval", False)
        for item in items[:2]
    ]
    columns = [item.get("ResTarget", {}).get("name") for item in root.get("cols", [])]
    if (
        _range_identity(root.get("relation", {})) != ("private", "lca_result_gc_control")
        or columns != ["singleton", "claims_enabled", "reason"]
        or booleans != [True, False]
        or "issue_398_disabled_by_default" not in strings
    ):
        violations.append("result-gc:control-not-disabled-by-default")


def _check_policies(rows: list[dict[str, Any]], violations: list[str]) -> None:
    seen = set()
    for statement in rows:
        root = statement["CreatePolicyStmt"]
        name = root.get("policy_name")
        expected = POLICIES.get(name)
        roles = _role_names(root.get("roles", []))
        if expected is None:
            violations.append("result-gc:unexpected-policy")
            continue
        schema, table, command, needs_check = expected
        if (
            _range_identity(root.get("table", {})) != (schema, table)
            or root.get("cmd_name") != command
            or roles != [EXECUTOR]
            or root.get("qual", {}).get("A_Const", {}).get("boolval", {}).get("boolval")
            is not True
            or (root.get("with_check") is not None) != needs_check
            or (
                needs_check
                and root.get("with_check", {})
                .get("A_Const", {})
                .get("boolval", {})
                .get("boolval")
                is not True
            )
        ):
            violations.append(f"result-gc:policy-shape-differs:{name}")
        seen.add(name)
    if seen != set(POLICIES):
        violations.append("result-gc:policy-set-differs")


def _check_functions(rows: list[dict[str, Any]], violations: list[str]) -> None:
    seen = set()
    forbidden_body = re.compile(r"\b(?:execute|dblink|http|net\.)\b", re.I)
    for statement in rows:
        root = statement["CreateFunctionStmt"]
        name_parts = _string_list(root.get("funcname", []))
        name = name_parts[-1] if len(name_parts) == 2 else None
        options = _function_options(root)
        language = next(iter(_scalar_strings(options.get("language", {}))), None)
        security = options.get("security", {}).get("Boolean", {}).get("boolval")
        body = _function_body(options)
        checked_body = _without_sql_comments(body)
        expected_security = name not in INVOKER_FUNCTIONS
        expected_language = "sql" if name in SQL_FUNCTIONS else "plpgsql"
        if (
            name_parts[:1] != ["private"]
            or name not in ALL_FUNCTIONS
            or root.get("replace") is True
            or language != expected_language
            or security is not expected_security
            or not _function_search_path_is_safe(options)
            or not body
            or forbidden_body.search(checked_body)
        ):
            violations.append(f"result-gc:function-envelope-differs:{name}")
        for token in FUNCTION_REQUIRED_TOKENS.get(name, set()):
            if token not in checked_body.lower():
                violations.append(f"result-gc:function-state-token-missing:{name}:{token}")
        seen.add(name)
    if seen != ALL_FUNCTIONS:
        violations.append("result-gc:function-set-differs")


def _check_triggers(rows: list[dict[str, Any]], violations: list[str]) -> None:
    seen = set()
    for statement in rows:
        root = statement["CreateTrigStmt"]
        name = root.get("trigname")
        expected = TRIGGERS.get(name)
        if expected is None:
            violations.append("result-gc:unexpected-trigger")
            continue
        target, function, events, columns = expected
        if (
            _range_identity(root.get("relation", {})) != target
            or _string_list(root.get("funcname", [])) != ["private", function]
            or root.get("timing") != 2
            or root.get("events") != events
            or root.get("row") is not True
            or tuple(_string_list(root.get("columns", []))) != columns
            or root.get("whenClause") is not None
            or root.get("transitionRels")
        ):
            violations.append(f"result-gc:trigger-shape-differs:{name}")
        seen.add(name)
    if seen != set(TRIGGERS):
        violations.append("result-gc:trigger-set-differs")


def _check_role_grants(rows: list[dict[str, Any]], violations: list[str]) -> None:
    observed = []
    for statement in rows:
        root = statement["GrantRoleStmt"]
        granted = [item.get("AccessPriv", {}).get("priv_name") for item in root.get("granted_roles", [])]
        grantees = _role_names(root.get("grantee_roles", []))
        observed.append((bool(root.get("is_grant")), tuple(granted), tuple(grantees)))
        admin_option = any(
            option.get("DefElem", {}).get("defname") == "admin"
            and option.get("DefElem", {}).get("arg", {}).get("Boolean", {}).get("boolval")
            for option in root.get("opt", [])
        )
        if admin_option or root.get("grantor") is not None or root.get("behavior") != "DROP_RESTRICT":
            violations.append("result-gc:role-grant-admin-or-grantor")
    if observed != [
        (True, (EXECUTOR,), ("postgres",)),
        (False, (EXECUTOR,), ("postgres",)),
    ]:
        violations.append("result-gc:temporary-role-membership-shape-differs")


def _check_owners(rows: list[dict[str, Any]], violations: list[str]) -> None:
    seen = set()
    for statement in rows:
        root = statement["AlterOwnerStmt"]
        owner = root.get("newowner", {}).get("rolename")
        object_node = root.get("object", {}).get("ObjectWithArgs", {})
        name_parts = _string_list(object_node.get("objname", []))
        name = name_parts[-1] if len(name_parts) == 2 else None
        if (
            root.get("objectType") != "OBJECT_FUNCTION"
            or name_parts[:1] != ["private"]
            or name not in ALL_FUNCTIONS
            or owner != EXECUTOR
        ):
            violations.append(f"result-gc:owner-shape-differs:{name}")
        seen.add(name)
    if seen != ALL_FUNCTIONS:
        violations.append("result-gc:owner-set-differs")


def _check_grants(rows: list[dict[str, Any]], violations: list[str]) -> None:
    for statement in rows:
        root = statement["GrantStmt"]
        is_grant = bool(root.get("is_grant"))
        roles = _role_names(root.get("grantees", []))
        objects = _object_names(root)
        privileges = _privileges(root)
        objtype = root.get("objtype")
        browser_roles = {"ROLESPEC_PUBLIC", "anon", "authenticated", "service_role", "api_internal_executor"}
        if is_grant and browser_roles & set(roles):
            violations.append("result-gc:grant-to-browser-or-service-role")
        allowed = False
        if (
            objtype == "OBJECT_SCHEMA"
            and objects == [("private",)]
            and roles == [EXECUTOR, WORKER]
            and privileges == [("usage", ())]
        ):
            allowed = is_grant
        elif (
            objtype == "OBJECT_SCHEMA"
            and objects == [("extensions",)]
            and roles == [EXECUTOR]
            and privileges == [("usage", ())]
        ):
            allowed = is_grant
        elif (
            objtype == "OBJECT_SCHEMA"
            and objects == [("private",)]
            and roles == [EXECUTOR]
            and privileges == [("create", ())]
        ):
            allowed = True
        elif objtype == "OBJECT_TABLE" and set(objects) == {("private", name) for name in CREATED_TABLES}:
            if is_grant:
                allowed = roles == [EXECUTOR] and {
                    name for name, _ in privileges
                } == {"select", "insert", "update", "delete"}
            else:
                allowed = set(roles) == browser_roles | {WORKER, EXECUTOR}
        elif objtype == "OBJECT_TABLE" and objects == [("public", "lca_results")]:
            if is_grant and roles == [EXECUTOR]:
                allowed = privileges in [[("select", ()), ("delete", ())], [("update", ("retention_partition_key",))]]
        elif objtype == "OBJECT_TABLE" and set(objects) == {
            ("public", "lca_result_cache"),
            ("public", "lca_latest_all_unit_results"),
            ("public", "lcia_result_packages"),
            ("private", "worker_jobs"),
        }:
            allowed = is_grant and roles == [EXECUTOR] and privileges == [("select", ())]
        elif objtype == "OBJECT_FUNCTION" and set(objects) == {("private", name) for name in ALL_FUNCTIONS}:
            allowed = (
                not is_grant
                and set(roles) == browser_roles | {WORKER}
                and privileges == []
            )
        elif objtype == "OBJECT_FUNCTION" and set(objects) == {("private", name) for name in WORKER_FUNCTIONS}:
            allowed = is_grant and roles == [WORKER] and privileges == [("execute", ())]
        if not allowed:
            violations.append("result-gc:grant-shape-differs")


def semantic_violations(sql: str) -> list[str]:
    statements, parse_errors = _parse(sql)
    if parse_errors:
        return parse_errors
    grouped: dict[str, list[dict[str, Any]]] = {}
    for statement in statements:
        grouped.setdefault(next(iter(statement)), []).append(statement)
    violations: list[str] = []
    counts = Counter({key: len(value) for key, value in grouped.items()})
    if counts != EXPECTED_ROOT_COUNTS:
        violations.append("result-gc:statement-set-differs")
    transactions = [row["TransactionStmt"].get("kind") for row in grouped.get("TransactionStmt", [])]
    if transactions != ["TRANS_STMT_BEGIN", "TRANS_STMT_COMMIT"]:
        violations.append("result-gc:transaction-boundary-differs")
    for row in grouped.get("VariableSetStmt", []):
        _check_timeout(row, violations)
    _check_do_statements(grouped.get("DoStmt", []), violations)
    schemas = [row["CreateSchemaStmt"].get("schemaname") for row in grouped.get("CreateSchemaStmt", [])]
    if schemas != ["private"]:
        violations.append("result-gc:schema-create-differs")
    _check_alter_tables(grouped.get("AlterTableStmt", []), violations)
    _check_indexes(grouped.get("IndexStmt", []), violations)
    _check_created_tables(grouped.get("CreateStmt", []), violations)
    _check_control_insert(grouped.get("InsertStmt", []), violations)
    _check_policies(grouped.get("CreatePolicyStmt", []), violations)
    _check_functions(grouped.get("CreateFunctionStmt", []), violations)
    _check_triggers(grouped.get("CreateTrigStmt", []), violations)
    _check_role_grants(grouped.get("GrantRoleStmt", []), violations)
    _check_owners(grouped.get("AlterOwnerStmt", []), violations)
    _check_grants(grouped.get("GrantStmt", []), violations)
    notify = grouped.get("NotifyStmt", [])
    if len(notify) != 1 or notify[0]["NotifyStmt"] != {
        "conditionname": "pgrst",
        "payload": "reload schema",
    }:
        violations.append("result-gc:notify-shape-differs")
    return sorted(set(violations))


def reviewed_result_gc_migration_violations(
    *, path: str, git_blob: str, sql: str
) -> list[str]:
    if REVIEWED_GIT_BLOB is None or REVIEWED_AST_SHA256 is None:
        return ["result-gc:review-receipt-not-configured"]
    violations = []
    if path != RESULT_GC_MIGRATION_PATH:
        violations.append("result-gc:path-differs")
    if git_blob != REVIEWED_GIT_BLOB or git_blob_oid(sql) != REVIEWED_GIT_BLOB:
        violations.append("result-gc:git-blob-differs")
    violations.extend(semantic_violations(sql))
    try:
        digest = normalized_ast_sha256(sql)
    except ResultGcSemanticError as error:
        violations.append(str(error))
    else:
        if digest != REVIEWED_AST_SHA256:
            violations.append("result-gc:normalized-ast-differs")
    return sorted(set(violations))


__all__ = [
    "RESULT_GC_CLASSIFICATION",
    "RESULT_GC_MIGRATION_PATH",
    "REVIEWED_GIT_BLOB",
    "REVIEWED_AST_SHA256",
    "git_blob_oid",
    "normalized_ast_sha256",
    "reviewed_result_gc_migration_violations",
    "semantic_violations",
]
