#!/usr/bin/env python3
"""Exact semantic qualification for the Issue #323 review-progress migration.

The Issue #390 pre-DDL gate remains fail closed for every generic hard-deny
signal.  This module qualifies one immutable migration only when its path,
Git blob, normalized PostgreSQL AST, statement set, role boundary, ACLs,
function envelope, and procedural safety anchors all match the reviewed
least-privilege design.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections import Counter
from collections.abc import Iterable
from typing import Any

from pglast import parser
from pglast.parser import ParseError


REVIEW_PROGRESS_CLASSIFICATION = "review-progress-least-privilege-reviewed"
REVIEW_PROGRESS_MIGRATION_PATH = (
    "supabase/migrations/20260802170000_reference_review_name_fallback.sql"
)
REVIEWED_GIT_BLOB = "675507ec7b04880ac0a9938e1dfa36242c0bb6e2"
REVIEWED_AST_SHA256 = (
    "8f95f3d6e399b32fac44db539760d87c75a042755dd2e8cb521306c12e0c1945"
)
PARSER_VERSION = 180004
EXECUTOR = "review_progress_executor"
RPC = "qry_root_review_reference_progress_v2"

EXPECTED_ROOT_SEQUENCE = [
    "DoStmt",
    "DoStmt",
    "GrantRoleStmt",
    "GrantStmt",
    "GrantStmt",
    "GrantStmt",
    "GrantStmt",
    "GrantStmt",
    "GrantStmt",
    "GrantStmt",
    "DropStmt",
    "CreatePolicyStmt",
    "DropStmt",
    "CreatePolicyStmt",
    "CreateFunctionStmt",
    "GrantStmt",
    "GrantStmt",
    "CommentStmt",
    "GrantStmt",
    "AlterOwnerStmt",
    "GrantStmt",
    "GrantRoleStmt",
]

EXPECTED_PARAMETERS = [
    ("p_root_review_id", "FUNC_PARAM_DEFAULT", "pg_catalog.uuid"),
    ("reference_review_id", "FUNC_PARAM_TABLE", "pg_catalog.uuid"),
    ("target_table", "FUNC_PARAM_TABLE", "pg_catalog.text"),
    ("data_id", "FUNC_PARAM_TABLE", "pg_catalog.uuid"),
    ("data_version", "FUNC_PARAM_TABLE", "pg_catalog.text"),
    ("data_name", "FUNC_PARAM_TABLE", "pg_catalog.jsonb"),
    ("submitted_revision_checksum", "FUNC_PARAM_TABLE", "pg_catalog.text"),
    ("state_code", "FUNC_PARAM_TABLE", "pg_catalog.int4"),
    ("reviewer_count", "FUNC_PARAM_TABLE", "pg_catalog.int4"),
    ("completed_reviewer_count", "FUNC_PARAM_TABLE", "pg_catalog.int4"),
    ("actor_comment_state_code", "FUNC_PARAM_TABLE", "pg_catalog.int4"),
    ("actor_comment_modified_at", "FUNC_PARAM_TABLE", "pg_catalog.timestamptz"),
]

HELPER_IDENTITIES = {
    "public.cmd_review_is_review_admin(pg_catalog.uuid)",
    "public.cmd_review_is_review_member(pg_catalog.uuid)",
    "public.policy_review_can_read(pg_catalog.uuid,pg_catalog.uuid)",
    "public.cmd_review_get_dataset_row(pg_catalog.text,pg_catalog.uuid,pg_catalog.text,pg_catalog.bool)",
    "public.cmd_review_get_dataset_name(pg_catalog.text,pg_catalog.jsonb)",
}
RPC_IDENTITY = f"public.{RPC}(pg_catalog.uuid)"


class ReviewProgressSemanticError(ValueError):
    pass


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


def _string_list(value: object) -> list[str]:
    if not isinstance(value, list):
        return []
    result = []
    for item in value:
        if not isinstance(item, dict) or "String" not in item:
            return []
        result.append(item["String"].get("sval"))
    return [value for value in result if isinstance(value, str)]


def _role_name(value: object) -> str | None:
    row = value.get("RoleSpec", {}) if isinstance(value, dict) else {}
    if row.get("roletype") == "ROLESPEC_PUBLIC":
        return "PUBLIC"
    if row.get("roletype") == "ROLESPEC_CURRENT_USER":
        return "CURRENT_USER"
    return row.get("rolename")


def _type_name(value: object) -> str:
    row = value.get("TypeName", value) if isinstance(value, dict) else {}
    return ".".join(_string_list(row.get("names", [])))


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
        return [], ["review-progress:nul-byte"]
    try:
        document = json.loads(parser.parse_sql_json(sql))
    except (ParseError, UnicodeDecodeError) as error:
        return [], [f"review-progress:parse-error:{type(error).__name__}"]
    if document.get("version") != PARSER_VERSION:
        return [], ["review-progress:parser-version-differs"]
    return [row["stmt"] for row in document.get("stmts", [])], []


def normalized_ast_sha256(sql: str) -> str:
    statements, errors = _parse(sql)
    if errors:
        raise ReviewProgressSemanticError(errors[0])
    return hashlib.sha256(_canonical(_without_locations(statements))).hexdigest()


def _without_sql_comments(value: str) -> str:
    without_blocks = re.sub(r"/\*.*?\*/", " ", value, flags=re.DOTALL)
    return re.sub(r"--[^\n]*", " ", without_blocks)


def _do_body(statement: dict[str, Any]) -> str:
    values = [
        item.get("DefElem", {}).get("arg", {})
        for item in statement["DoStmt"].get("args", [])
        if item.get("DefElem", {}).get("defname") == "as"
    ]
    return next(iter(_scalar_strings(values[0])), "") if len(values) == 1 else ""


def _check_do_statements(
    statements: list[dict[str, Any]], violations: list[str]
) -> None:
    if len(statements) != 2:
        violations.append("review-progress:do-count-differs")
        return
    bodies = [_without_sql_comments(_do_body(row)).lower() for row in statements]
    if any(re.search(r"\b(?:execute|format|dblink|http|net\.)\b", body) for body in bodies):
        violations.append("review-progress:do-dynamic-or-network-execution")
    create_body, contract_body = bodies
    create_required = {
        "if not exists",
        "from pg_catalog.pg_roles",
        "rolname = 'review_progress_executor'",
        "create role review_progress_executor",
        "nologin noinherit nosuperuser nocreatedb nocreaterole nobypassrls",
    }
    if any(token not in create_body for token in create_required):
        violations.append("review-progress:role-create-contract-differs")
    contract_required = {
        "v_role pg_catalog.pg_roles%rowtype",
        "into strict v_role",
        "from pg_catalog.pg_roles",
        "v_role.rolcanlogin",
        "v_role.rolinherit",
        "v_role.rolsuper",
        "v_role.rolcreatedb",
        "v_role.rolcreaterole",
        "v_role.rolreplication",
        "v_role.rolbypassrls",
        "review_progress_executor has unsafe role attributes",
    }
    if any(token not in contract_body for token in contract_required):
        violations.append("review-progress:role-attribute-contract-differs")


def _object_identity(value: dict[str, Any]) -> str:
    if "String" in value:
        return value["String"].get("sval", "")
    if "RangeVar" in value:
        row = value["RangeVar"]
        return ".".join(
            part for part in (row.get("schemaname"), row.get("relname")) if part
        )
    if "ObjectWithArgs" in value:
        row = value["ObjectWithArgs"]
        name = ".".join(_string_list(row.get("objname", [])))
        args = ",".join(_type_name(item) for item in row.get("objargs", []))
        return f"{name}({args})"
    return ""


def _grant_shape(statement: dict[str, Any]) -> tuple[object, ...]:
    root = statement["GrantStmt"]
    objects = tuple(sorted(_object_identity(item) for item in root.get("objects", [])))
    privileges = tuple(
        sorted(
            (
                item.get("AccessPriv", {}).get("priv_name") or "all",
                tuple(_string_list(item.get("AccessPriv", {}).get("cols", []))),
            )
            for item in root.get("privileges", [])
        )
    )
    roles = tuple(sorted(filter(None, (_role_name(item) for item in root.get("grantees", [])))))
    return bool(root.get("is_grant")), root.get("objtype"), objects, privileges, roles


def _expected_grants() -> Counter[tuple[object, ...]]:
    execute = (("execute", ()),)
    expected = Counter(
        {
            (True, "OBJECT_SCHEMA", ("public",), (("usage", ()),), (EXECUTOR,)): 1,
            (
                True,
                "OBJECT_TABLE",
                ("public.comments", "public.reviews"),
                (("select", ()),),
                (EXECUTOR,),
            ): 1,
            (
                False,
                "OBJECT_FUNCTION",
                (RPC_IDENTITY,),
                (),
                ("PUBLIC", "anon"),
            ): 1,
            (
                True,
                "OBJECT_FUNCTION",
                (RPC_IDENTITY,),
                execute,
                ("authenticated", "service_role"),
            ): 1,
            (True, "OBJECT_SCHEMA", ("public",), (("create", ()),), (EXECUTOR,)): 1,
            (False, "OBJECT_SCHEMA", ("public",), (("create", ()),), (EXECUTOR,)): 1,
        }
    )
    for identity in HELPER_IDENTITIES:
        expected[(True, "OBJECT_FUNCTION", (identity,), execute, (EXECUTOR,))] += 1
    return expected


def _check_grants(statements: list[dict[str, Any]], violations: list[str]) -> None:
    observed = Counter(_grant_shape(row) for row in statements)
    if observed != _expected_grants():
        violations.append("review-progress:acl-shape-differs")


def _check_role_grants(
    statements: list[dict[str, Any]], violations: list[str]
) -> None:
    observed = []
    for statement in statements:
        root = statement["GrantRoleStmt"]
        granted = tuple(
            item.get("AccessPriv", {}).get("priv_name")
            for item in root.get("granted_roles", [])
        )
        grantees = tuple(
            filter(None, (_role_name(item) for item in root.get("grantee_roles", [])))
        )
        observed.append(
            (
                bool(root.get("is_grant")),
                granted,
                grantees,
                _role_name({"RoleSpec": root.get("grantor", {})})
                if root.get("grantor")
                else None,
            )
        )
    if observed != [
        (True, (EXECUTOR,), ("postgres",), None),
        (False, (EXECUTOR,), ("postgres",), "CURRENT_USER"),
    ]:
        violations.append("review-progress:temporary-role-membership-shape-differs")


def _check_policy_statements(
    drops: list[dict[str, Any]],
    policies: list[dict[str, Any]],
    violations: list[str],
) -> None:
    drop_tables = []
    for statement in drops:
        root = statement["DropStmt"]
        strings = set(_scalar_strings(root.get("objects", [])))
        table = next((name for name in ("reviews", "comments") if name in strings), None)
        if (
            root.get("removeType") != "OBJECT_POLICY"
            or not root.get("missing_ok")
            or "public" not in strings
            or "review_progress_executor_select" not in strings
            or table is None
        ):
            violations.append("review-progress:policy-drop-shape-differs")
        else:
            drop_tables.append(table)
    if sorted(drop_tables) != ["comments", "reviews"]:
        violations.append("review-progress:policy-drop-set-differs")

    observed = []
    for statement in policies:
        root = statement["CreatePolicyStmt"]
        table = root.get("table", {})
        qual = root.get("qual", {}).get("A_Const", {}).get("boolval", {}).get("boolval")
        observed.append(
            (
                root.get("policy_name"),
                table.get("schemaname"),
                table.get("relname"),
                root.get("cmd_name"),
                bool(root.get("permissive")),
                tuple(filter(None, (_role_name(item) for item in root.get("roles", [])))),
                qual,
            )
        )
    expected = [
        ("review_progress_executor_select", "public", "reviews", "select", True, (EXECUTOR,), True),
        ("review_progress_executor_select", "public", "comments", "select", True, (EXECUTOR,), True),
    ]
    if observed != expected:
        violations.append("review-progress:policy-shape-differs")


def _function_options(root: dict[str, Any]) -> dict[str, object]:
    return {
        option.get("defname"): option.get("arg")
        for item in root.get("options", [])
        if (option := item.get("DefElem", {})).get("defname")
    }


def _check_function(statement: dict[str, Any], violations: list[str]) -> None:
    root = statement["CreateFunctionStmt"]
    if not root.get("replace") or _string_list(root.get("funcname", [])) != ["public", RPC]:
        violations.append("review-progress:function-identity-differs")
    parameters = []
    for item in root.get("parameters", []):
        row = item.get("FunctionParameter", {})
        parameters.append((row.get("name"), row.get("mode"), _type_name(row.get("argType", {}))))
    if parameters != EXPECTED_PARAMETERS:
        violations.append("review-progress:function-signature-differs")

    options = _function_options(root)
    language = next(iter(_scalar_strings(options.get("language", {}))), None)
    volatility = next(iter(_scalar_strings(options.get("volatility", {}))), None)
    security = options.get("security", {}).get("Boolean", {}).get("boolval") if isinstance(options.get("security"), dict) else None
    setting = options.get("set", {}).get("VariableSetStmt", {}) if isinstance(options.get("set"), dict) else {}
    search_path = list(_scalar_strings(setting.get("args", [])))
    if (
        language != "plpgsql"
        or volatility != "stable"
        or security is not True
        or setting.get("name") != "search_path"
        or search_path != ["pg_catalog", "pg_temp"]
    ):
        violations.append("review-progress:function-envelope-differs")

    bodies = list(_scalar_strings(options.get("as", {})))
    if len(bodies) != 1:
        violations.append("review-progress:function-body-count-differs")
        return
    body = _without_sql_comments(bodies[0]).lower()
    if re.search(r"\b(?:execute|format|dblink|http|net\.)\b", body):
        violations.append("review-progress:function-dynamic-or-network-execution")
    required = {
        "request.jwt.claim.sub",
        "request.jwt.claims",
        "public.cmd_review_is_review_admin(v_actor)",
        "public.cmd_review_is_review_member(v_actor)",
        "errcode = '42501'",
        "message = 'review_role_required'",
        "public.cmd_review_get_dataset_row(",
        "public.cmd_review_get_dataset_name(",
        "public.policy_review_can_read(reference_review.id, v_actor)",
        "from public.reviews as root_review",
        "join public.reviews as reference_review",
        "from public.comments as completed_comment",
        "from public.comments as comment_row",
        "actor_comment.state_code <> -2",
        "root_review.current_reference_review_ids",
    }
    if any(token not in body for token in required):
        violations.append("review-progress:function-body-contract-differs")
    public_references = set(re.findall(r"\bpublic\.([a-z_][a-z0-9_]*)", body))
    allowed_references = {
        "reviews",
        "comments",
        "cmd_review_is_review_admin",
        "cmd_review_is_review_member",
        "cmd_review_get_dataset_row",
        "cmd_review_get_dataset_name",
        "policy_review_can_read",
    }
    if public_references != allowed_references:
        violations.append("review-progress:function-public-reference-set-differs")


def _check_owner(statement: dict[str, Any], violations: list[str]) -> None:
    root = statement["AlterOwnerStmt"]
    identity = _object_identity(root.get("object", {}))
    owner = _role_name({"RoleSpec": root.get("newowner", {})})
    if root.get("objectType") != "OBJECT_FUNCTION" or identity != RPC_IDENTITY or owner != EXECUTOR:
        violations.append("review-progress:owner-shape-differs")


def semantic_violations(sql: str) -> list[str]:
    statements, errors = _parse(sql)
    if errors:
        return errors
    roots = [next(iter(statement)) for statement in statements]
    violations: list[str] = []
    if roots != EXPECTED_ROOT_SEQUENCE:
        violations.append("review-progress:statement-sequence-differs")
    grouped: dict[str, list[dict[str, Any]]] = {}
    for statement in statements:
        grouped.setdefault(next(iter(statement)), []).append(statement)
    if Counter(roots) != Counter(EXPECTED_ROOT_SEQUENCE):
        violations.append("review-progress:statement-set-differs")
    _check_do_statements(grouped.get("DoStmt", []), violations)
    _check_role_grants(grouped.get("GrantRoleStmt", []), violations)
    _check_grants(grouped.get("GrantStmt", []), violations)
    _check_policy_statements(
        grouped.get("DropStmt", []), grouped.get("CreatePolicyStmt", []), violations
    )
    functions = grouped.get("CreateFunctionStmt", [])
    if len(functions) != 1:
        violations.append("review-progress:function-count-differs")
    else:
        _check_function(functions[0], violations)
    owners = grouped.get("AlterOwnerStmt", [])
    if len(owners) != 1:
        violations.append("review-progress:owner-count-differs")
    else:
        _check_owner(owners[0], violations)
    comments = grouped.get("CommentStmt", [])
    comment_root = comments[0].get("CommentStmt", {}) if len(comments) == 1 else {}
    comment_text = comment_root.get("comment", "")
    if (
        len(comments) != 1
        or comment_root.get("objtype") != "OBJECT_FUNCTION"
        or _object_identity(comment_root.get("object", {})) != RPC_IDENTITY
        or EXECUTOR not in comment_text
        or "legacy empty names" not in comment_text
    ):
        violations.append("review-progress:comment-shape-differs")
    return sorted(set(violations))


def reviewed_review_progress_migration_violations(
    *, path: str, git_blob: str, sql: str
) -> list[str]:
    violations = []
    if path != REVIEW_PROGRESS_MIGRATION_PATH:
        violations.append("review-progress:path-differs")
    if git_blob != REVIEWED_GIT_BLOB or git_blob_oid(sql) != REVIEWED_GIT_BLOB:
        violations.append("review-progress:git-blob-differs")
    violations.extend(semantic_violations(sql))
    try:
        digest = normalized_ast_sha256(sql)
    except ReviewProgressSemanticError as error:
        violations.append(str(error))
    else:
        if digest != REVIEWED_AST_SHA256:
            violations.append("review-progress:normalized-ast-differs")
    return sorted(set(violations))


__all__ = [
    "REVIEW_PROGRESS_CLASSIFICATION",
    "REVIEW_PROGRESS_MIGRATION_PATH",
    "REVIEWED_AST_SHA256",
    "REVIEWED_GIT_BLOB",
    "git_blob_oid",
    "normalized_ast_sha256",
    "reviewed_review_progress_migration_violations",
    "semantic_violations",
]
