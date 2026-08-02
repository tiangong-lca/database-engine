#!/usr/bin/env python3
"""Exact semantic qualification for the Issue #323 notification migration.

This migration preserves an existing SECURITY DEFINER command and changes only
the legacy-notification uniqueness predicate.  The qualifier proves that the
pre-existing function envelope, authorization checks, relation set, audit
write, and ACL/owner state are not widened while binding the exact source and
normalized AST.
"""

from __future__ import annotations

import re
from collections import Counter
from collections.abc import Iterable
from typing import Any

import json
from pglast import parser
from pglast.parser import ParseError

from scripts.issue_323_review_progress_semantic_gate import (
    git_blob_oid,
    normalized_ast_sha256,
)


REVIEW_NOTIFICATION_CLASSIFICATION = (
    "review-notification-event-identity-semantic-reviewed"
)
REVIEW_NOTIFICATION_MIGRATION_PATH = (
    "supabase/migrations/20260803093000_review_notification_event_identity.sql"
)
REVIEWED_GIT_BLOB = "0a5e5f16b1818d64e28198752a0d594650736167"
REVIEWED_AST_SHA256 = (
    "3895d8684c749b83ea9a6ceb69f81848bb13ae44a4bbf39f6a53cb557eec9e4a"
)
PARSER_VERSION = 180004
INDEX = "notifications_recipient_sender_type_dataset_uq"
FUNCTION = "cmd_notification_send_validation_issue"

EXPECTED_PARAMETERS = [
    ("p_recipient_user_id", "uuid", False),
    ("p_dataset_type", "text", False),
    ("p_dataset_id", "uuid", False),
    ("p_dataset_version", "text", False),
    ("p_link", "text", True),
    ("p_issue_codes", "text[]", True),
    ("p_tab_names", "text[]", True),
    ("p_issue_count", "pg_catalog.int4", True),
    ("p_audit", "jsonb", True),
]
EXPECTED_INDEX_COLUMNS = (
    "recipient_user_id",
    "sender_user_id",
    "type",
    "dataset_type",
    "dataset_id",
    "dataset_version",
)


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
        text = item["String"].get("sval")
        if isinstance(text, str):
            result.append(text)
    return result


def _type_name(value: object) -> str:
    row = value.get("TypeName", value) if isinstance(value, dict) else {}
    name = ".".join(_string_list(row.get("names", [])))
    bounds = row.get("arrayBounds", [])
    return name + "[]" * len(bounds)


def _without_sql_comments(value: str) -> str:
    without_blocks = re.sub(r"/\*.*?\*/", " ", value, flags=re.DOTALL)
    return re.sub(r"--[^\n]*", " ", without_blocks)


def _parse(sql: str) -> tuple[list[dict[str, Any]], list[str]]:
    if "\x00" in sql:
        return [], ["review-notification:nul-byte"]
    try:
        document = json.loads(parser.parse_sql_json(sql))
    except (ParseError, UnicodeDecodeError) as error:
        return [], [f"review-notification:parse-error:{type(error).__name__}"]
    if document.get("version") != PARSER_VERSION:
        return [], ["review-notification:parser-version-differs"]
    return [row["stmt"] for row in document.get("stmts", [])], []


def _check_drop(statement: dict[str, Any], violations: list[str]) -> None:
    root = statement["DropStmt"]
    objects = [
        tuple(_string_list(item.get("List", {}).get("items", [])))
        for item in root.get("objects", [])
    ]
    if (
        root.get("removeType") != "OBJECT_INDEX"
        or not root.get("missing_ok")
        or objects != [("public", INDEX)]
        or root.get("behavior") != "DROP_RESTRICT"
    ):
        violations.append("review-notification:index-drop-shape-differs")


def _check_index(statement: dict[str, Any], violations: list[str]) -> None:
    root = statement["IndexStmt"]
    relation = root.get("relation", {})
    columns = tuple(
        item.get("IndexElem", {}).get("name")
        for item in root.get("indexParams", [])
    )
    if (
        root.get("idxname") != INDEX
        or not root.get("unique")
        or root.get("concurrent")
        or root.get("accessMethod") not in {None, "btree"}
        or (relation.get("schemaname"), relation.get("relname"))
        != ("public", "notifications")
        or columns != EXPECTED_INDEX_COLUMNS
        or root.get("indexIncludingParams")
        or root.get("options")
        or root.get("tableSpace")
    ):
        violations.append("review-notification:index-shape-differs")
    predicate = root.get("whereClause", {})
    strings = list(_scalar_strings(predicate))
    null_tests = [node for kind, node in _walk(predicate) if kind == "NullTest"]
    nullifs = [
        node
        for kind, node in _walk(predicate)
        if kind == "A_Expr" and node.get("kind") == "AEXPR_NULLIF"
    ]
    if (
        "json" not in strings
        or "event_key" not in strings
        or len(nullifs) != 1
        or len(null_tests) != 1
        or null_tests[0].get("nulltesttype") != "IS_NULL"
    ):
        violations.append("review-notification:index-predicate-differs")


def _function_options(root: dict[str, Any]) -> dict[str, object]:
    return {
        option.get("defname"): option.get("arg")
        for item in root.get("options", [])
        if (option := item.get("DefElem", {})).get("defname")
    }


def _check_function(statement: dict[str, Any], violations: list[str]) -> None:
    root = statement["CreateFunctionStmt"]
    if not root.get("replace") or _string_list(root.get("funcname", [])) != [
        "public",
        FUNCTION,
    ]:
        violations.append("review-notification:function-identity-differs")
    parameters = []
    for item in root.get("parameters", []):
        row = item.get("FunctionParameter", {})
        parameters.append(
            (
                row.get("name"),
                _type_name(row.get("argType", {})),
                row.get("defexpr") is not None,
            )
        )
    if parameters != EXPECTED_PARAMETERS:
        violations.append("review-notification:function-signature-differs")
    if _type_name(root.get("returnType", {})) != "jsonb":
        violations.append("review-notification:function-return-type-differs")

    options = _function_options(root)
    language = next(iter(_scalar_strings(options.get("language", {}))), None)
    security = (
        options.get("security", {}).get("Boolean", {}).get("boolval")
        if isinstance(options.get("security"), dict)
        else None
    )
    setting = (
        options.get("set", {}).get("VariableSetStmt", {})
        if isinstance(options.get("set"), dict)
        else {}
    )
    if (
        language != "plpgsql"
        or security is not True
        or setting.get("name") != "search_path"
        or list(_scalar_strings(setting.get("args", [])))
        != ["public", "pg_temp"]
        or set(options) != {"language", "security", "set", "as"}
    ):
        violations.append("review-notification:function-envelope-differs")

    bodies = list(_scalar_strings(options.get("as", {})))
    if len(bodies) != 1:
        violations.append("review-notification:function-body-count-differs")
        return
    body = _without_sql_comments(bodies[0]).lower()
    if re.search(r"\b(?:execute|format|dblink|http|net\.)\b", body):
        violations.append("review-notification:function-dynamic-or-network-execution")
    required = {
        "v_actor uuid := auth.uid()",
        "code', 'auth_required'",
        "code', 'recipient_required'",
        "code', 'notification_self_target'",
        "code', 'dataset_type_required'",
        "code', 'dataset_id_required'",
        "code', 'dataset_version_required'",
        "code', 'recipient_not_found'",
        "public.cmd_review_ref_type_to_table(v_dataset_type)",
        "public.cmd_review_get_dataset_row(",
        "code', 'recipient_not_target_owner'",
        "public.cmd_notification_normalize_text_array(p_issue_codes)",
        "public.cmd_notification_normalize_text_array(p_tab_names)",
        "insert into public.notifications",
        "where nullif(json->>'event_key', '') is null",
        "do update",
        "insert into public.command_audit_log",
        "'cmd_notification_send_validation_issue'",
        "return jsonb_build_object(\n    'ok', true",
    }
    if any(token not in body for token in required):
        violations.append("review-notification:function-body-contract-differs")
    public_references = set(re.findall(r"\bpublic\.([a-z_][a-z0-9_]*)", body))
    expected_references = {
        "users",
        "notifications",
        "command_audit_log",
        "cmd_review_ref_type_to_table",
        "cmd_review_get_dataset_row",
        "cmd_notification_normalize_text_array",
    }
    if public_references != expected_references:
        violations.append("review-notification:function-public-reference-set-differs")


def semantic_violations(sql: str) -> list[str]:
    statements, errors = _parse(sql)
    if errors:
        return errors
    roots = [next(iter(statement)) for statement in statements]
    violations: list[str] = []
    expected = ["DropStmt", "IndexStmt", "CreateFunctionStmt"]
    if roots != expected:
        violations.append("review-notification:statement-sequence-differs")
    if Counter(roots) != Counter(expected):
        violations.append("review-notification:statement-set-differs")
    grouped = {next(iter(statement)): statement for statement in statements}
    if "DropStmt" in grouped:
        _check_drop(grouped["DropStmt"], violations)
    if "IndexStmt" in grouped:
        _check_index(grouped["IndexStmt"], violations)
    if "CreateFunctionStmt" in grouped:
        _check_function(grouped["CreateFunctionStmt"], violations)
    return sorted(set(violations))


def reviewed_review_notification_migration_violations(
    *, path: str, git_blob: str, sql: str
) -> list[str]:
    violations = []
    if path != REVIEW_NOTIFICATION_MIGRATION_PATH:
        violations.append("review-notification:path-differs")
    if git_blob != REVIEWED_GIT_BLOB or git_blob_oid(sql) != REVIEWED_GIT_BLOB:
        violations.append("review-notification:git-blob-differs")
    violations.extend(semantic_violations(sql))
    try:
        digest = normalized_ast_sha256(sql)
    except ValueError as error:
        violations.append(str(error))
    else:
        if digest != REVIEWED_AST_SHA256:
            violations.append("review-notification:normalized-ast-differs")
    return sorted(set(violations))


__all__ = [
    "REVIEW_NOTIFICATION_CLASSIFICATION",
    "REVIEW_NOTIFICATION_MIGRATION_PATH",
    "REVIEWED_AST_SHA256",
    "REVIEWED_GIT_BLOB",
    "reviewed_review_notification_migration_violations",
    "semantic_violations",
]
