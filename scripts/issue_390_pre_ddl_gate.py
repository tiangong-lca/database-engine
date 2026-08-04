#!/usr/bin/env python3
"""PostgreSQL-AST gate for Issue #390 target-aware pre-DDL migrations."""

from __future__ import annotations

import json
import hashlib
from collections.abc import Iterator

import pglast
from pglast import parser
from pglast.parser import ParseError

from scripts.issue_398_result_gc_semantic_gate import (
    RESULT_GC_CLASSIFICATION,
    RESULT_GC_FK_INDEX_CLASSIFICATION,
    RESULT_GC_FK_INDEX_MIGRATION_PATH,
    reviewed_result_gc_fk_index_migration_violations,
    reviewed_result_gc_migration_violations,
)


TARGET_RELATION_NAMES = {
    "lca_factorization_registry",
    "lca_latest_all_unit_results",
    "lca_result_cache",
    "lca_results",
}
TARGET_ROUTINE_NAMES = {
    "lca_read_job_projection",
    "lca_read_latest_single_solve_result",
    "lca_read_result_projection",
}
TARGET_INDEX_NAMES = {
    "lca_factorization_registry_pkey",
    "lca_factorization_registry_prepared_worker_job_idx",
    "lca_factorization_registry_scope_snapshot_backend_opts_uk",
    "lca_factorization_registry_snapshot_status_idx",
    "lca_factorization_registry_status_lease_idx",
    "lca_latest_all_unit_results_computed_idx",
    "lca_latest_all_unit_results_pkey",
    "lca_latest_all_unit_results_result_idx",
    "lca_latest_all_unit_results_snapshot_uk",
    "lca_latest_all_unit_results_worker_job_idx",
    "lca_result_cache_job_uidx",
    "lca_result_cache_last_accessed_idx",
    "lca_result_cache_lookup_idx",
    "lca_result_cache_pkey",
    "lca_result_cache_result_uidx",
    "lca_result_cache_scope_snapshot_request_key_uk",
    "lca_result_cache_snapshot_idx",
    "lca_result_cache_worker_job_idx",
    "lca_results_created_desc_idx",
    "lca_results_expires_at_idx",
    "lca_results_job_idx",
    "lca_results_pkey",
    "lca_results_snapshot_created_idx",
    "lca_results_worker_job_idx",
}
PROTECTED_SCHEMAS = {None, "public", "private"}
PROTECTED_RUNTIME_ROLES = {
    "anon",
    "authenticated",
    "authenticator",
    "service_role",
    "api_internal_executor",
    "postgres",
    "supabase_admin",
}
PGLAST_VERSION = "v8.4"
PGLAST_PARSER_VERSION = 180004
DOCUMENT_EVIDENCE_MIGRATION_PATH = (
    "supabase/migrations/20260803163000_issue_407_document_validation_evidence_expand.sql"
)
DOCUMENT_EVIDENCE_CLASSIFICATION = (
    "additive-document-evidence-private-contract-reviewed"
)
DOCUMENT_EVIDENCE_PHYSICAL_MIGRATION_PATH = (
    "supabase/migrations/20260804100000_issue_407_document_validation_evidence_physical_expand.sql"
)
DOCUMENT_EVIDENCE_PHYSICAL_CLASSIFICATION = (
    "physical-document-evidence-private-expand-reviewed"
)
DOCUMENT_EVIDENCE_PHYSICAL_GIT_BLOB = "534f7ddb13ff52a62e949e8f098859af038c5d4c"
SNAPSHOT_GC_PHYSICAL_MIGRATION_PATH = (
    "supabase/migrations/20260804123000_issue_414_snapshot_gc_audit_physical_expand.sql"
)
SNAPSHOT_GC_PHYSICAL_CLASSIFICATION = (
    "physical-snapshot-gc-audit-private-expand-reviewed"
)
SNAPSHOT_GC_PHYSICAL_GIT_BLOB = "12a6580738149ebd5f447b3f9182b474c9a0bc72"
SNAPSHOT_GC_PHYSICAL_STATEMENT_HASHES = (
    "d7b5cd90d8b335c0624d2ed8b1b9268b43bd61b7a562b39409daa1e5447aaedd",
    "d45617b322938a6ce4dc9832b21d34973d4562ba8b2a43711701444b8c90b5aa",
    "d488d15520ad89482b91371dc23d90b49cb236f288de8861dbd4b1f88cebe3b0",
    "5bae3cb4539071fca97055f2352e05fc2012b2c93abcfefc8c66b0f4bf7cfde3",
    "e31d97977eb78bd11e53408137d1703d72f9358dcc3d7729211be38740e4f6fb",
    "4bd4cf6ea84a8646d5e2590d43ea73ef61446b23b3faae823976ba2356b6a071",
    "a8dfd3d94c2d2f056be061fb2ddd7cdcde0e598b09ad8c88605d73e228dc497a",
    "e400b2efd3cc3b3c91970c0aa7b2c3029a2862319dcbdfad2c716e106e952ee6",
    "54654a77f72be59e918e5c2acef429c6e9f620d01666ce600fe5e255647c224d",
    "5d9f9edbc2801566e6d0b4ca5983e6268d70e101cc505cde2db0b975dabe7a6c",
    "61a952104095fd6b9a610be0a771e27498fa64e0016ce888cad3de8d2b9b519b",
    "7db7c9672f2ef4bc9b858b6037df6b5ea938922f06a22a6f8ede88afc1485446",
    "6875dd7965ec8ee27243a850a4990ff73c4e1a330a77e0b8ecf905ed3ea3144a",
    "a5fe618ab778a2a8cf99fe2f41f2b1b83af03a6cf97e87ed9a19e6e7e6fed579",
    "b22cf0fe608240d73544d5ccd4d12ebe5d68409abf6733535e5efe02b2d9d04f",
    "89349443f1e7389d91399cf5ba5384582bfcac403e0aa3bfa6aa22d8edf1ab75",
    "2c93d92c2bb8cb47a18f992fbca194feb2fb312836f67580e7b383617cd93084",
    "6b743226fe1a30c7de2899bb5eec4090036fb34213a9ea1065b4d2f9d3e53d3c",
    "af61663bd88763b0a147cb38f85a063ccf826ad4a8232327aed16ec73ce8004d",
    "fa90a9af1bf22ea0a99017a5667ab6dc5479c7b77099ce8c480836b18d17f924",
    "8a4a54181ba22332cff1117bfde1b4218b4943d84bc6b8459825e5b41375a00c",
    "cfef1b14ee698fe0b51e127411c64bdf8f8f3604de27b3afea26379746e2cd93",
    "bfbe5b1e0dca5c245a849e9fd1285c5e762b04769a061b644b01deca758b792b",
    "0471d918789b3718efc61cba3590c5a2bf73ce141693fb4be7e4851dd7e9b9be",
    "45d14dd72966377560b7d293faf6dd64c8e7741494c579a900eb2a1097c34c62",
    "61d101426cf2c96428662df328ee3e5d02e1cd5f82f063b3dbb1457aac8b13a9",
    "c570ef7e698606f104237a1dfc8055b26142fecf93ce0025e097900e3f3d9c73",
)
SAFE_FACADE_BUILTINS = {
    "array_agg",
    "clock_timestamp",
    "coalesce",
    "count",
    "greatest",
    "gen_random_uuid",
    "json_agg",
    "json_build_array",
    "json_build_object",
    "jsonb_agg",
    "jsonb_build_array",
    "jsonb_build_object",
    "jsonb_strip_nulls",
    "jsonb_typeof",
    "least",
    "length",
    "lower",
    "max",
    "min",
    "now",
    "nullif",
    "row_to_json",
    "sum",
    "to_json",
    "to_jsonb",
    "upper",
}
SERVICE_GRANTEE_ALLOWLIST = {
    "postgres",
    "service_role",
    "api_internal_executor",
    "supabase_admin",
}
OWNER_ROLE_ALLOWLIST = {"postgres", "supabase_admin"}
INTERNAL_SCHEMAS = {"private", "util", "archive"}
EXPOSED_SCHEMAS = {"api", "public"}
PROTECTED_BOUNDARY_SCHEMAS = {"api", "private", "public"}
PROTECTED_DEPENDENCY_RELATIONS = {
    ("private", "worker_jobs"),
    ("public", "worker_jobs"),
}
PROTECTED_DEPENDENCY_ROUTINES = {
    ("private", "worker_job_payload"),
    ("public", "lca_legacy_job_type"),
    ("public", "worker_job_payload"),
}
SAFE_BUILTIN_TYPES = {
    "bool",
    "bytea",
    "date",
    "float4",
    "float8",
    "inet",
    "int2",
    "int4",
    "int8",
    "interval",
    "json",
    "jsonb",
    "numeric",
    "oid",
    "record",
    "text",
    "time",
    "timestamp",
    "timestamptz",
    "timetz",
    "uuid",
    "varchar",
    "void",
}
SAFE_BUILTIN_OPERATORS = {
    "!=", "!~", "!~*", "!~~", "!~~*", "#>", "#>>", "%", "&", "&&",
    "*", "+", "-", "-|-", "/", "<", "<#>", "<->", "<@", "<=", "<>",
    "=", ">", ">=", ">>", "?", "?&", "?|", "@>", "@?", "^", "|", "||",
    "~", "~*", "~~", "~~*", "->", "->>", "@@",
}
STATIC_STATEMENT_TYPES = {
    "AlterDatabaseSetStmt",
    "AlterDefaultPrivilegesStmt",
    "AlterFunctionStmt",
    "AlterObjectDependsStmt",
    "AlterObjectSchemaStmt",
    "AlterOwnerStmt",
    "AlterPolicyStmt",
    "AlterRoleSetStmt",
    "AlterRoleStmt",
    "AlterSeqStmt",
    "AlterTableStmt",
    "AlterTypeStmt",
    "CommentStmt",
    "CompositeTypeStmt",
    "CreateDomainStmt",
    "CreateEnumStmt",
    "CreateFunctionStmt",
    "CreatePolicyStmt",
    "CreateRangeStmt",
    "CreateRoleStmt",
    "CreateSchemaStmt",
    "CreateSeqStmt",
    "CreateStmt",
    "CreateTableAsStmt",
    "CreateTypeStmt",
    "DeleteStmt",
    "DropOwnedStmt",
    "DropRoleStmt",
    "DropStmt",
    "GrantRoleStmt",
    "GrantStmt",
    "IndexStmt",
    "InsertStmt",
    "ReassignOwnedStmt",
    "RenameStmt",
    "SelectStmt",
    "TransactionStmt",
    "TruncateStmt",
    "UpdateStmt",
    "VariableSetStmt",
    "ViewStmt",
}


def _walk_ast(value: object) -> Iterator[tuple[str, dict]]:
    if isinstance(value, dict):
        for key, child in value.items():
            if key[:1].isupper() and isinstance(child, dict):
                yield key, child
            yield from _walk_ast(child)
    elif isinstance(value, list):
        for child in value:
            yield from _walk_ast(child)


def _string_list(value: object) -> list[str]:
    if not isinstance(value, list):
        return []
    result = []
    for item in value:
        if not isinstance(item, dict) or "String" not in item:
            return []
        result.append(item["String"]["sval"])
    return result


def _nested_string_sequences(value: object) -> Iterator[list[str]]:
    if isinstance(value, list):
        strings = _string_list(value)
        if strings:
            yield strings
        for child in value:
            yield from _nested_string_sequences(child)
    elif isinstance(value, dict):
        if "List" in value:
            strings = _string_list(value["List"].get("items", []))
            if strings:
                yield strings
        for child in value.values():
            yield from _nested_string_sequences(child)


def _relation_name_sequences(value: object) -> Iterator[list[str]]:
    """Yield relation names from both wrapped and inlined pglast RangeVars."""

    if isinstance(value, dict):
        name = value.get("relname")
        if isinstance(name, str):
            schema = value.get("schemaname")
            yield [part for part in (schema, name) if isinstance(part, str)]
        for child in value.values():
            yield from _relation_name_sequences(child)
    elif isinstance(value, list):
        for child in value:
            yield from _relation_name_sequences(child)


def _type_name_sequences(value: object) -> Iterator[list[str]]:
    """Yield type names from both wrapped and inlined pglast TypeName nodes."""

    if isinstance(value, dict):
        if "names" in value and "typemod" in value:
            name = _string_list(value.get("names", []))
            if name:
                yield name
        for child in value.values():
            yield from _type_name_sequences(child)
    elif isinstance(value, list):
        for child in value:
            yield from _type_name_sequences(child)


def _role_names(grantees: object) -> set[str]:
    roles = set()
    if not isinstance(grantees, list):
        return roles
    for item in grantees:
        role = item.get("RoleSpec", {}) if isinstance(item, dict) else {}
        if role.get("roletype") == "ROLESPEC_PUBLIC":
            roles.add("PUBLIC")
        elif role.get("rolename"):
            roles.add(role["rolename"])
    return roles


def _all_role_names(value: object) -> set[str]:
    roles = set()
    if isinstance(value, dict):
        if value.get("roletype") == "ROLESPEC_PUBLIC":
            roles.add("PUBLIC")
        elif value.get("rolename"):
            roles.add(value["rolename"])
        for child in value.values():
            roles.update(_all_role_names(child))
    elif isinstance(value, list):
        for child in value:
            roles.update(_all_role_names(child))
    return roles


def _privilege_names(statement: dict) -> set[str]:
    privileges = set()
    for value in statement.get("privileges", []):
        access = value.get("AccessPriv", {}) if isinstance(value, dict) else {}
        privileges.add(access.get("priv_name") or "ALL")
    return privileges or {"ALL"}


def _granted_role_names(statement: dict) -> set[str]:
    return {
        node.get("priv_name")
        for node_type, node in _walk_ast(statement.get("granted_roles", []))
        if node_type == "AccessPriv" and node.get("priv_name")
    }


def _name_is_protected(parts: list[str], names: set[str]) -> bool:
    if not parts or parts[-1] not in names:
        return False
    schema = parts[-2] if len(parts) > 1 else None
    return schema in PROTECTED_SCHEMAS


def _name_is_api_lca_facade(parts: list[str]) -> bool:
    return (
        len(parts) == 2
        and parts[0] == "api"
        and (parts[1].startswith("lca_") or parts[1].startswith("cmd_lca_"))
    )


def _protected_references(statement: dict) -> list[str]:
    references = set()
    for parts in _relation_name_sequences(statement):
        if len(parts) == 2 and tuple(parts) in PROTECTED_DEPENDENCY_RELATIONS:
            references.add(f"dependency-relation:{'.'.join(parts)}")
        if _name_is_api_lca_facade(parts):
            references.add(f"api-facade:{'.'.join(parts)}")
        if _name_is_protected(parts, TARGET_RELATION_NAMES):
            schema = parts[-2] if len(parts) > 1 else "unqualified"
            references.add(f"relation:{schema}.{parts[-1]}")
        if _name_is_protected(parts, TARGET_INDEX_NAMES):
            schema = parts[-2] if len(parts) > 1 else "unqualified"
            references.add(f"index:{schema}.{parts[-1]}")
    for node_type, node in _walk_ast(statement):
        if node_type in {"CreateFunctionStmt", "FuncCall"}:
            parts = _string_list(node.get("funcname", []))
            if len(parts) == 2 and tuple(parts) in PROTECTED_DEPENDENCY_ROUTINES:
                references.add(f"dependency-routine:{'.'.join(parts)}")
            if _name_is_protected(parts, TARGET_ROUTINE_NAMES):
                references.add(f"routine:{'.'.join(parts)}")
            if _name_is_api_lca_facade(parts):
                references.add(f"api-facade:{'.'.join(parts)}")
        if node_type == "ObjectWithArgs":
            parts = _string_list(node.get("objname", []))
            if len(parts) == 2 and tuple(parts) in PROTECTED_DEPENDENCY_ROUTINES:
                references.add(f"dependency-routine:{'.'.join(parts)}")
            if _name_is_protected(parts, TARGET_ROUTINE_NAMES):
                references.add(f"routine:{'.'.join(parts)}")
            if _name_is_api_lca_facade(parts):
                references.add(f"api-facade:{'.'.join(parts)}")
    for parts in _nested_string_sequences(statement):
        if len(parts) == 2 and tuple(parts) in PROTECTED_DEPENDENCY_RELATIONS:
            references.add(f"dependency-relation:{'.'.join(parts)}")
        if len(parts) == 2 and tuple(parts) in PROTECTED_DEPENDENCY_ROUTINES:
            references.add(f"dependency-routine:{'.'.join(parts)}")
        if _name_is_protected(parts, TARGET_RELATION_NAMES):
            references.add(f"relation:{'.'.join(parts)}")
        if (
            len(parts) >= 2
            and parts[0] in PROTECTED_SCHEMAS
            and parts[1] in TARGET_RELATION_NAMES
        ):
            references.add(f"relation:{parts[0]}.{parts[1]}")
        if len(parts) >= 2 and parts[0] in TARGET_RELATION_NAMES:
            references.add(f"relation:unqualified.{parts[0]}")
        if _name_is_protected(parts, TARGET_INDEX_NAMES):
            references.add(f"index:{'.'.join(parts)}")
        if _name_is_protected(parts, TARGET_ROUTINE_NAMES):
            references.add(f"routine:{'.'.join(parts)}")
        if _name_is_api_lca_facade(parts):
            references.add(f"api-facade:{'.'.join(parts)}")
    return sorted(references)


def _function_name(statement: dict) -> list[str]:
    return _string_list(statement.get("funcname", []))


def _function_options(statement: dict) -> dict[str, object]:
    result = {}
    for item in statement.get("options", []):
        option = item.get("DefElem", {})
        if option.get("defname"):
            result[option["defname"]] = option.get("arg")
    return result


def _function_option_values(statement: dict, name: str) -> list[object]:
    return [
        option.get("arg")
        for item in statement.get("options", [])
        if (option := item.get("DefElem", {})).get("defname") == name
    ]


def _function_body_strings(statement: dict) -> list[str]:
    option = _function_options(statement).get("as", {})
    items = option.get("List", {}).get("items", []) if isinstance(option, dict) else []
    return [
        item["String"]["sval"]
        for item in items
        if isinstance(item, dict) and "String" in item
    ]


def _function_is_invoker_with_empty_search_path(statement: dict) -> bool:
    security_values = _function_option_values(statement, "security")
    set_values = _function_option_values(statement, "set")
    if len(security_values) != 1 or len(set_values) != 1:
        return False
    security = security_values[0].get("Boolean", {})
    if security.get("boolval", True):
        return False
    setting = set_values[0].get("VariableSetStmt", {})
    if setting.get("name") != "search_path":
        return False
    args = setting.get("args", [])
    return len(args) == 1 and args[0].get("A_Const", {}).get("sval", {}).get(
        "sval"
    ) == ""


def _type_identity(value: object) -> tuple[tuple[str, ...], int]:
    node = value.get("TypeName", value) if isinstance(value, dict) else {}
    names = tuple(_string_list(node.get("names", [])))
    arrays = node.get("arrayBounds", [])
    return names, len(arrays) if isinstance(arrays, list) else 0


def _type_name_is_safe(name: list[str]) -> bool:
    return (len(name) == 1 and name[0] in SAFE_BUILTIN_TYPES) or (
        len(name) > 1 and name[-2] == "pg_catalog"
    )


def _created_function_identity(statement: dict) -> tuple | None:
    name = tuple(_function_name(statement))
    if len(name) != 2:
        return None
    arguments = []
    for item in statement.get("parameters", []):
        parameter = item.get("FunctionParameter", {})
        if parameter.get("mode") in {"FUNC_PARAM_OUT", "FUNC_PARAM_TABLE"}:
            continue
        arguments.append(_type_identity(parameter.get("argType", {})))
    return name + (tuple(arguments),)


def _granted_function_identities(statement: dict) -> list[tuple]:
    identities = []
    for value in statement.get("objects", []):
        obj = value.get("ObjectWithArgs", {})
        name = tuple(_string_list(obj.get("objname", [])))
        if len(name) != 2:
            continue
        identities.append(name + (tuple(_type_identity(arg) for arg in obj.get("objargs", [])),))
    return identities


def _identity_text(identity: tuple) -> str:
    schema, name, arguments = identity
    rendered = []
    for parts, array_count in arguments:
        rendered.append(".".join(parts) + "[]" * array_count)
    return f"{schema}.{name}({','.join(rendered)})"


def _grant_object_names(statement: dict) -> list[list[str]]:
    names = []
    for value in statement.get("objects", []):
        if "RangeVar" in value:
            relation = value["RangeVar"]
            names.append(
                [
                    part
                    for part in (relation.get("schemaname"), relation.get("relname"))
                    if part
                ]
            )
        elif "ObjectWithArgs" in value:
            names.append(_string_list(value["ObjectWithArgs"].get("objname", [])))
        elif "String" in value:
            names.append([value["String"]["sval"]])
        elif "List" in value:
            names.append(_string_list(value["List"].get("items", [])))
    return [name for name in names if name]


def _parse_sql(sql: str) -> tuple[list[dict], list[str]]:
    if "\x00" in sql:
        return [], ["hard-deny:nul-byte"]
    try:
        document = json.loads(parser.parse_sql_json(sql))
    except (ParseError, UnicodeDecodeError) as exc:
        return [], [f"hard-deny:postgres-parse-error:{type(exc).__name__}"]
    if document.get("version") != PGLAST_PARSER_VERSION:
        return [], ["hard-deny:unexpected-parser-version"]
    return [row["stmt"] for row in document.get("stmts", [])], []


def _function_call_is_safe(parts: list[str]) -> bool:
    if len(parts) == 2 and parts[0] == "public" and parts[1] in TARGET_ROUTINE_NAMES:
        return True
    return (
        len(parts) == 1 and parts[0] in SAFE_FACADE_BUILTINS
    ) or (
        len(parts) == 2
        and parts[0] == "pg_catalog"
        and parts[1] in SAFE_FACADE_BUILTINS
    )


def _variable_set_nodes(value: object) -> Iterator[dict]:
    if isinstance(value, dict):
        if (
            isinstance(value.get("kind"), str)
            and value["kind"].startswith("VAR_")
            and isinstance(value.get("name"), str)
        ):
            yield value
        for child in value.values():
            yield from _variable_set_nodes(child)
    elif isinstance(value, list):
        for child in value:
            yield from _variable_set_nodes(child)


def _scalar_strings(value: object) -> Iterator[str]:
    if isinstance(value, dict):
        if isinstance(value.get("sval"), str):
            yield value["sval"]
        for child in value.values():
            yield from _scalar_strings(child)
    elif isinstance(value, list):
        for child in value:
            yield from _scalar_strings(child)


def _contains_scalar(value: object, expected: object) -> bool:
    if value == expected:
        return True
    if isinstance(value, dict):
        return any(_contains_scalar(child, expected) for child in value.values())
    if isinstance(value, list):
        return any(_contains_scalar(child, expected) for child in value)
    return False


def _facade_body_ast_violations(statement: dict) -> list[str]:
    violations = []
    violations.extend(_facade_relation_scope_violations(statement))
    for name in _type_name_sequences(statement):
        if len(name) > 1 and name[-2] != "pg_catalog":
            violations.append(
                "facade:function-body-type-forbidden:" + ".".join(name)
            )
    for node_type, node in _walk_ast(statement):
        if node_type == "FuncCall":
            called = _string_list(node.get("funcname", []))
            if not _function_call_is_safe(called):
                violations.append(
                    "facade:function-body-call-forbidden:" + ".".join(called)
                )
        elif node_type == "CollateClause":
            name = _string_list(node.get("collname", []))
            if len(name) > 1 and name[-2] != "pg_catalog":
                violations.append(
                    "facade:function-body-collation-forbidden:" + ".".join(name)
                )
        elif node_type == "A_Expr":
            name = _string_list(node.get("name", []))
            if len(name) > 1 and name[-2] != "pg_catalog":
                violations.append(
                    "facade:function-body-operator-forbidden:" + ".".join(name)
                )
    return violations


def _direct_with_clause(statement: dict) -> dict:
    with_clause = statement.get("withClause", {})
    return with_clause.get("WithClause", with_clause)


def _facade_relation_scope_violations(
    value: object, inherited_ctes: frozenset[str] = frozenset()
) -> list[str]:
    """Validate RangeVars with lexical, non-leaking PostgreSQL CTE scope."""

    violations = []
    if isinstance(value, list):
        for child in value:
            violations.extend(
                _facade_relation_scope_violations(child, inherited_ctes)
            )
        return violations
    if not isinstance(value, dict):
        return violations

    statement_wrapper = next(
        (
            (node_type, node)
            for node_type, node in value.items()
            if node_type.endswith("Stmt") and isinstance(node, dict)
        ),
        None,
    )
    if statement_wrapper is not None:
        _, node = statement_wrapper
        with_clause = _direct_with_clause(node)
        if with_clause.get("recursive"):
            violations.append("facade:function-body-recursive-cte-forbidden")
        visible_ctes = inherited_ctes
        for item in with_clause.get("ctes", []):
            cte = item.get("CommonTableExpr", {}) if isinstance(item, dict) else {}
            violations.extend(
                _facade_relation_scope_violations(
                    cte.get("ctequery", {}), visible_ctes
                )
            )
            if isinstance(cte.get("ctename"), str):
                visible_ctes = visible_ctes | {cte["ctename"]}
        for key, child in node.items():
            if key != "withClause":
                violations.extend(
                    _facade_relation_scope_violations(child, visible_ctes)
                )
        return violations

    if "RangeVar" in value and isinstance(value["RangeVar"], dict):
        relation = value["RangeVar"]
        parts = [
            part
            for part in (relation.get("schemaname"), relation.get("relname"))
            if isinstance(part, str)
        ]
        if not (
            (len(parts) == 1 and parts[0] in inherited_ctes)
            or (
                len(parts) == 2
                and parts[0] == "public"
                and parts[1] in TARGET_RELATION_NAMES
            )
        ):
            violations.append(
                "facade:function-body-relation-forbidden:" + ".".join(parts)
            )
        return violations

    if isinstance(value.get("relname"), str):
        parts = [
            part
            for part in (value.get("schemaname"), value.get("relname"))
            if isinstance(part, str)
        ]
        if not (
            (len(parts) == 1 and parts[0] in inherited_ctes)
            or (
                len(parts) == 2
                and parts[0] == "public"
                and parts[1] in TARGET_RELATION_NAMES
            )
        ):
            violations.append(
                "facade:function-body-relation-forbidden:" + ".".join(parts)
            )
        return violations

    for child in value.values():
        violations.extend(
            _facade_relation_scope_violations(child, inherited_ctes)
        )
    return violations


def _api_internal_reference_violations(value: object) -> list[str]:
    violations = []
    for parts in _relation_name_sequences(value):
        if len(parts) > 1 and parts[-2] in INTERNAL_SCHEMAS:
            violations.append("hard-deny:api-internal-relation-reference")
    for name in _type_name_sequences(value):
        if len(name) > 1 and name[-2] in INTERNAL_SCHEMAS:
            violations.append("hard-deny:api-internal-type-reference")
    for node_type, node in _walk_ast(value):
        if node_type == "FuncCall":
            name = _string_list(node.get("funcname", []))
            if len(name) > 1 and name[-2] in INTERNAL_SCHEMAS:
                violations.append("hard-deny:api-internal-function-reference")
    return violations


def _api_signature_type_is_safe(name: list[str]) -> bool:
    return (
        len(name) == 2
        and name[0] == "pg_catalog"
        and name[1] in SAFE_BUILTIN_TYPES
    )


def _view_is_security_invoker(statement: dict) -> bool:
    values = [
        option.get("arg", {}).get("String", {}).get("sval")
        for item in statement.get("options", [])
        if (option := item.get("DefElem", {})).get("defname")
        == "security_invoker"
    ]
    return values == ["true"]


def _schema_names(statement: dict) -> set[str]:
    return {
        node.get("sval")
        for node_type, node in _walk_ast(statement)
        if node_type == "String" and node.get("sval")
    }


def _statement_signals(statement: dict, *, top_level: bool = True) -> list[str]:
    root_type, root = next(iter(statement.items()))
    protected = _protected_references(statement)
    signals = [f"protected-identifier:{value}" for value in protected]

    if root_type not in STATIC_STATEMENT_TYPES:
        signals.append(f"hard-deny:unclassified-statement:{root_type}")
    if _contains_scalar(root, "DROP_CASCADE"):
        signals.append("hard-deny:cascade-drop")
    if top_level and root_type in {
        "CreateTableAsStmt",
        "DeleteStmt",
        "IndexStmt",
        "InsertStmt",
        "SelectStmt",
        "TruncateStmt",
        "UpdateStmt",
    }:
        signals.append(f"hard-deny:data-executing-statement:{root_type}")

    if root_type in {
        "DoStmt",
        "CallStmt",
        "CreateEventTrigStmt",
        "RuleStmt",
        "CreateTrigStmt",
        "ExecuteStmt",
        "LoadStmt",
        "CreateExtensionStmt",
        "AlterExtensionStmt",
        "AlterExtensionContentsStmt",
        "CreatePLangStmt",
        "CreateTransformStmt",
        "CreateAmStmt",
    }:
        signals.append(f"hard-deny:opaque-{root_type}")
    for node_type, node in _walk_ast(statement):
        if node_type == "FuncCall" and not _function_call_is_safe(
            _string_list(node.get("funcname", []))
        ):
            signals.append("hard-deny:opaque-function-call")
        elif node_type == "A_Expr":
            operator = _string_list(node.get("name", []))
            if (
                len(operator) == 1
                and operator[0] not in SAFE_BUILTIN_OPERATORS
            ) or (len(operator) > 1 and operator[-2] != "pg_catalog"):
                signals.append("hard-deny:custom-operator-execution")
        elif node_type == "TypeCast":
            types = list(_type_name_sequences(node))
            if any(not _type_name_is_safe(name) for name in types):
                signals.append("hard-deny:custom-type-cast-execution")
        elif node_type == "CollateClause":
            collation = _string_list(node.get("collname", []))
            if len(collation) > 1 and collation[-2] != "pg_catalog":
                signals.append("hard-deny:custom-collation-execution")
    if root_type == "CopyStmt" and root.get("is_program"):
        signals.append("hard-deny:copy-program")
    if root_type == "AlterDatabaseSetStmt" and root.get("setstmt", {}).get(
        "kind"
    ) == "VAR_RESET_ALL":
        signals.append("hard-deny:database-reset-all")
    if root_type == "AlterTableStmt":
        index_constraints = {
            "CONSTR_EXCLUSION",
            "CONSTR_PRIMARY",
            "CONSTR_UNIQUE",
        }
        if any(
            node.get("contype") in index_constraints
            for node_type, node in _walk_ast(root)
            if node_type == "Constraint"
        ):
            signals.append("hard-deny:alter-table-index-build")
        for node_type, node in _walk_ast(root):
            if node_type != "AlterTableCmd":
                continue
            if node.get("subtype") == "AT_ChangeOwner":
                owners = _all_role_names(node.get("newowner", {}))
                if len(owners) != 1 or not owners <= OWNER_ROLE_ALLOWLIST:
                    signals.append("hard-deny:unapproved-object-owner")
            if node.get("subtype") == "AT_AddColumn":
                column = node.get("def", {}).get("ColumnDef", {})
                if (
                    column.get("raw_default") is not None
                    or column.get("cooked_default") is not None
                    or column.get("identity") is not None
                    or column.get("generated") is not None
                    or bool(column.get("constraints"))
                ):
                    signals.append("hard-deny:alter-table-add-column-rewrite")
            if node.get("subtype") == "AT_AddConstraint":
                constraint = node.get("def", {}).get("Constraint", {})
                is_non_scanning_constraint = (
                    constraint.get("contype")
                    in {"CONSTR_CHECK", "CONSTR_FOREIGN"}
                    and constraint.get("skip_validation") is True
                )
                if not is_non_scanning_constraint:
                    signals.append(
                        "hard-deny:alter-table-data-validating-operation:"
                        "AT_AddConstraint"
                    )
            if node.get("subtype") in {
                "AT_AlterColumnType",
                "AT_AttachPartition",
                "AT_SetNotNull",
                "AT_ValidateConstraint",
            }:
                signals.append(
                    "hard-deny:alter-table-data-validating-operation:"
                    + str(node.get("subtype"))
                )
            if node.get("subtype") in {
                "AT_SetAccessMethod",
                "AT_SetLogged",
                "AT_SetTableSpace",
                "AT_SetUnLogged",
            }:
                signals.append("hard-deny:alter-table-storage-rewrite")
            if node.get("subtype") in {
                "AT_DisableRule",
                "AT_DisableTrig",
                "AT_EnableAlwaysRule",
                "AT_EnableAlwaysTrig",
                "AT_EnableReplicaRule",
                "AT_EnableReplicaTrig",
                "AT_EnableRule",
                "AT_EnableTrig",
            }:
                signals.append("hard-deny:alter-table-opaque-rule-trigger-state")
    if root_type == "CreateStmt":
        if any(
            not _type_name_is_safe(name) for name in _type_name_sequences(root)
        ):
            signals.append("hard-deny:create-table-custom-type")
        if root.get("accessMethod") not in {None, "heap"}:
            signals.append("hard-deny:create-table-custom-access-method")
        if any(
            node.get("contype") == "CONSTR_EXCLUSION"
            for node_type, node in _walk_ast(root)
            if node_type == "Constraint"
        ):
            signals.append("hard-deny:create-table-exclusion-index")
        if any(
            node.get("options", 0) & 64
            for node_type, node in _walk_ast(root)
            if node_type == "TableLikeClause"
        ):
            signals.append("hard-deny:create-table-like-index-copy")
        if root.get("partbound") is not None:
            signals.append("hard-deny:create-table-partition-validation")
    for setting in _variable_set_nodes(statement):
        setting_name = setting.get("name")
        if setting_name in {"role", "session_authorization"}:
            signals.append("hard-deny:migration-identity-switch")
        if setting_name == "default_table_access_method":
            signals.append("hard-deny:default-table-access-method-change")
        if setting.get("name") == "search_path":
            search_path = {
                token.strip()
                for value in _scalar_strings(setting.get("args", []))
                for token in value.split(",")
                if token.strip()
            }
            if not search_path <= {"pg_catalog"}:
                signals.append("hard-deny:untrusted-search-path")
        if setting_name == "pgrst.db_schemas":
            exposed = [
                token.strip()
                for value in _scalar_strings(setting.get("args", []))
                for token in value.split(",")
                if token.strip()
            ]
            if exposed != ["api", "public", "graphql_public"]:
                signals.append("hard-deny:change-required-postgrest-schemas")
        if setting_name not in {
            "pgrst.db_schemas",
            "pgrst.db_extra_search_path",
        }:
            continue
        tokens = {
            token.strip()
            for value in _scalar_strings(setting.get("args", []))
            for token in value.split(",")
        }
        if tokens & INTERNAL_SCHEMAS:
            signals.append("hard-deny:expose-internal-through-postgrest")

    if root_type == "RenameStmt" and root.get("renameType") == "OBJECT_SCHEMA":
        if {root.get("subname"), root.get("newname")} & PROTECTED_BOUNDARY_SCHEMAS:
            signals.append("hard-deny:protected-boundary-schema-rename")
    if root_type == "RenameStmt" and root.get("renameType") == "OBJECT_ROLE":
        if {root.get("subname"), root.get("newname")} & PROTECTED_RUNTIME_ROLES:
            signals.append("hard-deny:protected-runtime-role-rename")
    if root_type == "RenameStmt" and root.get("renameType") in {
        "OBJECT_FUNCTION",
        "OBJECT_PROCEDURE",
        "OBJECT_ROUTINE",
    }:
        object_with_args = root.get("object", {}).get("ObjectWithArgs", {})
        old_name = _string_list(object_with_args.get("objname", []))
        new_name = str(root.get("newname", ""))
        if (
            len(old_name) == 2
            and old_name[0] == "api"
            and (new_name.startswith("lca_") or new_name.startswith("cmd_lca_"))
        ):
            signals.append("hard-deny:rename-into-protected-api-facade")
        if new_name in TARGET_ROUTINE_NAMES and (
            len(old_name) == 1
            or (len(old_name) == 2 and old_name[0] in PROTECTED_SCHEMAS)
        ):
            signals.append("hard-deny:rename-into-target-routine")
        if len(old_name) == 2 and (
            old_name[0], new_name
        ) in PROTECTED_DEPENDENCY_ROUTINES:
            signals.append("hard-deny:rename-into-protected-dependency-routine")
    if root_type == "RenameStmt" and root.get("renameType") in {
        "OBJECT_TABLE",
        "OBJECT_VIEW",
        "OBJECT_MATVIEW",
        "OBJECT_SEQUENCE",
    }:
        relation = root.get("relation", {})
        schema = relation.get("schemaname")
        new_name = str(root.get("newname", ""))
        if new_name in TARGET_RELATION_NAMES and schema in PROTECTED_SCHEMAS:
            signals.append("hard-deny:rename-into-target-relation")
        if (schema, new_name) in PROTECTED_DEPENDENCY_RELATIONS:
            signals.append("hard-deny:rename-into-protected-dependency-relation")
        if schema == "api" and new_name.startswith(("lca_", "cmd_lca_")):
            signals.append("hard-deny:rename-relation-into-protected-api-facade")
    if root_type == "RenameStmt" and root.get("renameType") == "OBJECT_INDEX":
        relation = root.get("relation", {})
        if (
            relation.get("schemaname") in PROTECTED_SCHEMAS
            and root.get("newname") in TARGET_INDEX_NAMES
        ):
            signals.append("hard-deny:rename-into-target-index")
    if root_type == "AlterObjectSchemaStmt" and root.get("objectType") in {
        "OBJECT_FUNCTION",
        "OBJECT_PROCEDURE",
        "OBJECT_ROUTINE",
    }:
        object_with_args = root.get("object", {}).get("ObjectWithArgs", {})
        old_name = _string_list(object_with_args.get("objname", []))
        if root.get("newschema") == "api" and old_name and (
            old_name[-1].startswith("lca_")
            or old_name[-1].startswith("cmd_lca_")
        ):
            signals.append("hard-deny:move-into-protected-api-facade")
    if root_type == "AlterObjectSchemaStmt" and root.get("newschema") in EXPOSED_SCHEMAS:
        signals.append("hard-deny:move-object-into-exposed-schema")
    if root_type == "DropStmt" and root.get("removeType") == "OBJECT_SCHEMA":
        if _schema_names(statement) & PROTECTED_BOUNDARY_SCHEMAS:
            signals.append("hard-deny:drop-protected-boundary-schema")
    if root_type == "CreateSchemaStmt" and root.get(
        "schemaname"
    ) in PROTECTED_BOUNDARY_SCHEMAS:
        signals.append("hard-deny:create-protected-boundary-schema")
    if root_type == "CreateSchemaStmt" and root.get("authrole") is not None:
        owners = _all_role_names(root.get("authrole", {}))
        if len(owners) != 1 or not owners <= OWNER_ROLE_ALLOWLIST:
            signals.append("hard-deny:unapproved-object-owner")

    if root_type in {
        "AlterObjectSchemaStmt",
        "AlterTableStmt",
        "AlterPolicyStmt",
        "CreatePolicyStmt",
        "RenameStmt",
        "DropStmt",
    } and protected:
        signals.append(f"hard-deny:target-{root_type}")

    if root_type == "CreateStmt":
        relation = root.get("relation", {})
        if relation.get("relname") in TARGET_RELATION_NAMES and relation.get(
            "schemaname"
        ) in PROTECTED_SCHEMAS:
            signals.append("hard-deny:create-target-relation")
    if root_type == "ViewStmt":
        view = root.get("view", {})
        if view.get("relname") in TARGET_RELATION_NAMES and view.get(
            "schemaname"
        ) in PROTECTED_SCHEMAS:
            signals.append("hard-deny:replace-target-with-view")
        if view.get("schemaname") in EXPOSED_SCHEMAS:
            if not _view_is_security_invoker(root):
                signals.append("hard-deny:exposed-view-must-be-security-invoker")
            signals.extend(_api_internal_reference_violations(root.get("query", {})))
        if view.get("schemaname") == "api":
            if view.get("relname", "").startswith(("lca_", "cmd_lca_")):
                signals.append("hard-deny:create-protected-api-facade-view")
        if tuple(
            part
            for part in (view.get("schemaname"), view.get("relname"))
            if isinstance(part, str)
        ) in PROTECTED_DEPENDENCY_RELATIONS:
            signals.append("hard-deny:replace-protected-dependency-relation")

    if root_type == "CreateFunctionStmt":
        name = _function_name(root)
        if len(name) == 2 and tuple(name) in PROTECTED_DEPENDENCY_ROUTINES:
            signals.append("hard-deny:replace-protected-dependency-routine")
        if _name_is_protected(name, TARGET_ROUTINE_NAMES):
            signals.append("hard-deny:create-or-replace-target-routine")
        body = "\n".join(_function_body_strings(root))
        options = _function_options(root)
        language = options.get("language", {}).get("String", {}).get("sval")
        if len(name) == 2 and name[0] in EXPOSED_SCHEMAS:
            security = options.get("security", {}).get("Boolean", {})
            if security.get("boolval") is True:
                signals.append("hard-deny:exposed-security-definer-function")
            if any(
                not _api_signature_type_is_safe(type_name)
                for type_name in _type_name_sequences(root)
            ):
                signals.append("hard-deny:exposed-function-signature-type")
        if body and language == "sql":
            nested, errors = _parse_sql(body)
            signals.extend(errors)
            for nested_statement in nested:
                signals.extend(_statement_signals(nested_statement, top_level=False))
                if len(name) == 2 and name[0] in EXPOSED_SCHEMAS:
                    signals.extend(
                        _api_internal_reference_violations(nested_statement)
                    )
        elif body:
            signals.append("hard-deny:opaque-procedural-function-body")

    if root_type == "GrantRoleStmt":
        membership_roles = _role_names(root.get("grantee_roles", [])) | _granted_role_names(root)
        if membership_roles & PROTECTED_RUNTIME_ROLES:
            signals.append("hard-deny:protected-runtime-role-membership-change")
    if root_type in {"AlterRoleStmt", "DropRoleStmt", "CreateRoleStmt"}:
        roles = _all_role_names(root)
        if root_type == "CreateRoleStmt" and root.get("role"):
            roles.add(root["role"])
        if roles & PROTECTED_RUNTIME_ROLES:
            signals.append("hard-deny:protected-runtime-role-definition-change")
    if root_type == "AlterRoleSetStmt":
        if _all_role_names(root.get("role", {})) & PROTECTED_RUNTIME_ROLES:
            signals.append("hard-deny:protected-runtime-role-setting-change")
    if root_type in {"DropOwnedStmt", "ReassignOwnedStmt"}:
        signals.append("hard-deny:broad-owned-object-change")
    if root_type == "GrantStmt":
        roles = _role_names(root.get("grantees", []))
        objects = _grant_object_names(root)
        internal_object = any(name[0] in INTERNAL_SCHEMAS for name in objects)
        target_object = any(
            _name_is_protected(name, TARGET_RELATION_NAMES | TARGET_ROUTINE_NAMES)
            for name in objects
        )
        all_public_targets = (
            root.get("targtype") == "ACL_TARGET_ALL_IN_SCHEMA"
            and ["public"] in objects
            and root.get("objtype")
            in {
                "OBJECT_TABLE",
                "OBJECT_FUNCTION",
                "OBJECT_PROCEDURE",
                "OBJECT_ROUTINE",
            }
        )
        all_api_functions = (
            root.get("targtype") == "ACL_TARGET_ALL_IN_SCHEMA"
            and ["api"] in objects
            and root.get("objtype")
            in {"OBJECT_FUNCTION", "OBJECT_PROCEDURE", "OBJECT_ROUTINE"}
        )
        all_api_relations = (
            root.get("targtype") == "ACL_TARGET_ALL_IN_SCHEMA"
            and ["api"] in objects
            and root.get("objtype") == "OBJECT_TABLE"
        )
        api_schema_create = (
            root.get("objtype") == "OBJECT_SCHEMA"
            and ["api"] in objects
            and bool(_privilege_names(root) & {"create", "ALL"})
        )
        if root.get("is_grant") and (
            internal_object
            or target_object
            or all_public_targets
            or all_api_functions
            or all_api_relations
            or api_schema_create
        ) and not roles <= SERVICE_GRANTEE_ALLOWLIST:
            signals.append("hard-deny:non-service-private-or-target-grant")
        protected_service_revoke = (
            target_object
            or all_public_targets
            or all_api_functions
            or (
                root.get("objtype") == "OBJECT_SCHEMA"
                and any(name in [["public"], ["private"], ["api"]] for name in objects)
                and bool(_privilege_names(root) & {"usage", "ALL"})
            )
        )
        if (
            not root.get("is_grant")
            and protected_service_revoke
            and bool(roles & {"service_role", "api_internal_executor"})
        ):
            signals.append("hard-deny:remove-protected-service-access")
        if (
            not root.get("is_grant")
            and all_api_functions
            and bool(roles & {"service_role", "api_internal_executor"})
        ):
            signals.append("hard-deny:remove-api-facade-service-access")
        if (
            not root.get("is_grant")
            and root.get("objtype") == "OBJECT_SCHEMA"
            and ["api"] in objects
            and bool(_privilege_names(root) & {"usage", "ALL"})
            and bool(roles & {"service_role", "api_internal_executor"})
        ):
            signals.append("hard-deny:remove-api-schema-service-access")
        if not root.get("is_grant") and roles & {"PUBLIC", "authenticated"}:
            historical_relation = ["public", "lca_results"] in objects
            all_public_tables = (
                root.get("targtype") == "ACL_TARGET_ALL_IN_SCHEMA"
                and root.get("objtype") == "OBJECT_TABLE"
                and ["public"] in objects
            )
            public_schema_usage = (
                root.get("objtype") == "OBJECT_SCHEMA"
                and ["public"] in objects
                and bool(_privilege_names(root) & {"usage", "ALL"})
            )
            if historical_relation or all_public_tables or public_schema_usage:
                signals.append("hard-deny:remove-historical-result-access")
    if root_type == "AlterDefaultPrivilegesStmt":
        schemas = {
            value
            for item in root.get("options", [])
            if (option := item.get("DefElem", {})).get("defname") == "schemas"
            for value in _scalar_strings(option.get("arg", {}))
        }
        action = root.get("action", {})
        roles = _role_names(action.get("grantees", []))
        if (
            action.get("is_grant")
            and (not schemas or bool(schemas & INTERNAL_SCHEMAS))
            and not roles <= SERVICE_GRANTEE_ALLOWLIST
        ):
            signals.append("hard-deny:browser-global-or-internal-default-grant")
        if (
            action.get("is_grant")
            and action.get("objtype")
            in {"OBJECT_FUNCTION", "OBJECT_PROCEDURE", "OBJECT_ROUTINE"}
            and not roles <= SERVICE_GRANTEE_ALLOWLIST
        ):
            signals.append("hard-deny:non-service-function-default-grant")

    if root_type == "AlterOwnerStmt" and root.get("objectType") == "OBJECT_SCHEMA":
        if _schema_names(statement) & PROTECTED_BOUNDARY_SCHEMAS:
            signals.append("hard-deny:protected-boundary-schema-owner-change")
    if root_type == "AlterOwnerStmt":
        owners = _all_role_names(root.get("newowner", {}))
        if len(owners) != 1 or not owners <= OWNER_ROLE_ALLOWLIST:
            signals.append("hard-deny:unapproved-object-owner")

    strings = _schema_names(statement)
    if any(value.startswith("pgrst.db_") for value in strings) and "private" in strings:
        signals.append("hard-deny:expose-private-through-postgrest")
    for node_type, node in _walk_ast(statement):
        if node_type in {"UpdateStmt", "InsertStmt", "DeleteStmt"}:
            relation = node.get("relation", {})
            if relation.get("schemaname") in {None, "pg_catalog"} and relation.get(
                "relname"
            ).startswith("pg_"):
                signals.append("hard-deny:catalog-write")
    return sorted(set(signals))


def pre_ddl_sql_signals(sql: str) -> list[str]:
    statements, errors = _parse_sql(sql)
    signals = list(errors)
    for statement in statements:
        signals.extend(_statement_signals(statement))
    return sorted(set(signals))


RECONCILE_REPLACEMENT_IDENTITY = (
    "api",
    "cmd_lca_reconcile_result_cache_v1",
    (
        (("pg_catalog", "uuid"), 0),
        (("pg_catalog", "uuid"), 0),
    ),
)


def _facade_review_violations(
    sql: str, *, replacement_identity: tuple | None = None
) -> list[str]:
    statements, errors = _parse_sql(sql)
    if errors:
        return errors
    created: set[tuple] = set()
    acl_state: dict[tuple, dict[str, bool]] = {}
    explicitly_revoked: dict[tuple, set[str]] = {}
    violations = []
    for statement in statements:
        root_type, root = next(iter(statement.items()))
        hard = [
            signal
            for signal in _statement_signals(statement)
            if signal.startswith("hard-deny:")
        ]
        violations.extend(hard)
        if root_type == "CreateFunctionStmt":
            name = _function_name(root)
            if not _name_is_api_lca_facade(name):
                violations.append("facade:create-function-outside-api")
                continue
            identity = _created_function_identity(root)
            if identity is None:
                violations.append("facade:invalid-function-identity")
                continue
            if identity in created:
                violations.append("facade:duplicate-function-identity")
            created.add(identity)
            acl_state[identity] = {
                # CREATE grants EXECUTE to PUBLIC by default. A replacement,
                # however, preserves the existing ACL, so model every governed
                # runtime grantee as privileged until this exact migration
                # explicitly revokes it.
                role: replacement_identity is not None or role == "PUBLIC"
                for role in (
                    "PUBLIC",
                    "anon",
                    "authenticated",
                    "service_role",
                    "api_internal_executor",
                )
            }
            explicitly_revoked[identity] = set()
            if root.get("replace") and replacement_identity is None:
                violations.append("facade:create-or-replace-forbidden")
            if replacement_identity is not None and (
                not root.get("replace") or identity != replacement_identity
            ):
                violations.append("facade:replacement-identity-forbidden")
            if not _function_is_invoker_with_empty_search_path(root):
                violations.append("facade:function-must-be-invoker-empty-search-path")
            options = _function_options(root)
            language = options.get("language", {}).get("String", {}).get("sval")
            if language != "sql":
                violations.append("facade:function-language-must-be-sql")
            allowed_options = {
                "language",
                "security",
                "set",
                "as",
                "volatility",
                "strict",
                "parallel",
                "cost",
                "rows",
            }
            option_names = {
                item.get("DefElem", {}).get("defname")
                for item in root.get("options", [])
            }
            if not option_names <= allowed_options:
                violations.append("facade:function-option-forbidden")
            if len(_function_option_values(root, "language")) != 1:
                violations.append("facade:function-must-have-one-language")
            if len(_function_option_values(root, "as")) != 1:
                violations.append("facade:function-must-have-one-body-option")
            for item in root.get("parameters", []):
                if item.get("FunctionParameter", {}).get("defexpr") is not None:
                    violations.append("facade:function-parameter-default-forbidden")
            for type_name in _type_name_sequences(root):
                if not _api_signature_type_is_safe(type_name):
                    violations.append(
                        "facade:function-signature-type-forbidden:"
                        + ".".join(type_name)
                    )
            bodies = _function_body_strings(root)
            if len(bodies) != 1:
                violations.append("facade:function-must-have-one-sql-body")
            for body in bodies:
                nested, nested_errors = _parse_sql(body)
                violations.extend(nested_errors)
                if not nested:
                    violations.append("facade:function-body-must-not-be-empty")
                body_has_target = False
                for nested_statement in nested:
                    nested_type = next(iter(nested_statement))
                    if nested_type not in {
                        "SelectStmt",
                        "InsertStmt",
                        "UpdateStmt",
                        "DeleteStmt",
                    }:
                        violations.append(
                            f"facade:function-body-statement-forbidden:{nested_type}"
                        )
                    if _protected_references(nested_statement):
                        body_has_target = True
                    violations.extend(
                        _facade_body_ast_violations(nested_statement)
                    )
                if not body_has_target:
                    violations.append("facade:function-body-missing-target-reference")
        elif root_type == "GrantStmt":
            roles = _role_names(root.get("grantees", []))
            identities = _granted_function_identities(root)
            if (
                root.get("objtype") != "OBJECT_FUNCTION"
                or not identities
                or len(identities) != len(root.get("objects", []))
            ):
                violations.append("facade:grant-target-must-be-api-function")
                continue
            if any(identity not in created for identity in identities):
                violations.append("facade:grant-target-must-be-created-in-batch")
                continue
            privileges = {value.lower() for value in _privilege_names(root)}
            if not privileges <= {"execute", "all"}:
                violations.append("facade:grant-privilege-must-be-execute")
                continue
            if root.get("grant_option"):
                violations.append("facade:grant-option-change-forbidden")
                continue
            if root.get("is_grant") and not roles <= {"service_role"}:
                violations.append("facade:grant-role-not-service-only")
            for identity in identities:
                for role in roles:
                    acl_state[identity][role] = bool(root.get("is_grant"))
                    if not root.get("is_grant"):
                        explicitly_revoked[identity].add(role)
        elif root_type != "CommentStmt":
            violations.append(f"facade:statement-forbidden:{root_type}")
    for identity in created:
        identity_text = _identity_text(identity)
        if any(
            acl_state[identity][role]
            for role in (
                "PUBLIC",
                "anon",
                "authenticated",
                "api_internal_executor",
            )
        ):
            violations.append(f"facade:missing-browser-revoke:{identity_text}")
        if not acl_state[identity]["service_role"]:
            violations.append(f"facade:missing-service-grant:{identity_text}")
        if replacement_identity is not None and not {
            "PUBLIC",
            "anon",
            "authenticated",
            "service_role",
            "api_internal_executor",
        } <= explicitly_revoked[identity]:
            violations.append(f"facade:missing-exact-acl-reset:{identity_text}")
    if not created:
        violations.append("facade:no-api-function-created")
    return sorted(set(violations))


def reviewed_document_evidence_migration_violations(sql: str) -> list[str]:
    """Exact PostgreSQL-AST admission for Issue #407 Phase A only."""
    statements, errors = _parse_sql(sql)
    if errors:
        return errors
    violations: list[str] = []
    expected_types = (
        "TransactionStmt", "VariableSetStmt", "VariableSetStmt", "DoStmt",
        "CreateFunctionStmt", "CreateFunctionStmt", "GrantStmt", "GrantStmt",
        "CreateFunctionStmt", "CreateFunctionStmt", "GrantStmt", "GrantStmt",
        "CommentStmt", "CommentStmt", "CommentStmt", "CommentStmt", "DoStmt",
        "TransactionStmt",
    )
    observed_types = tuple(next(iter(statement)) for statement in statements)
    if observed_types != expected_types:
        violations.append("document-evidence:exact-statement-sequence-required")
        return violations

    first_transaction = statements[0]["TransactionStmt"]
    last_transaction = statements[-1]["TransactionStmt"]
    if first_transaction.get("kind") != "TRANS_STMT_BEGIN" or last_transaction.get("kind") != "TRANS_STMT_COMMIT":
        violations.append("document-evidence:exact-transaction-boundary-required")
    for index, expected_name, expected_value in (
        (1, "lock_timeout", "5s"),
        (2, "statement_timeout", "2min"),
    ):
        setting = statements[index]["VariableSetStmt"]
        values = [
            item.get("A_Const", {}).get("sval", {}).get("sval")
            for item in setting.get("args", [])
        ]
        if (
            setting.get("kind") != "VAR_SET_VALUE"
            or setting.get("name") != expected_name
            or not setting.get("is_local")
            or values != [expected_value]
        ):
            violations.append(f"document-evidence:exact-setting-required:{expected_name}")

    do_hashes = (
        "44ce800be93888992afb17fa6f7e4fcda90cbfcd02334524e01f4f03f00e598c",
        "e5278b5d8cc6b6fd2cbf2608e95f4d217706550236e505579fd2ef62bf6da768",
    )
    function_hashes = {
        ("private", "svc_lcia_document_validation_evidence_lookup"):
            "ff66e2691246c21c6597cd2a15b44ae8f6d1fedc97159218b05121a3015f79d0",
        ("private", "svc_lcia_document_validation_evidence_record"):
            "5afa390b4f167924705db7202fcf5e618ba2be3d53e170a9a46ac64b0b408a2e",
        ("public", "svc_lcia_document_validation_evidence_lookup"):
            "248620282e8a5733b408948cc504bfdb828863c6682fdf1ceaf5de8ca499e74f",
        ("public", "svc_lcia_document_validation_evidence_record"):
            "a6b9e162382584182c7fee9c7d916f396105a8565793041a8745f1b0830acf79",
    }
    expected_identities = {
        ("private", "svc_lcia_document_validation_evidence_lookup", ((('pg_catalog', 'jsonb'), 0),)),
        ("private", "svc_lcia_document_validation_evidence_record", ((('pg_catalog', 'jsonb'), 0), (('pg_catalog', 'uuid'), 0))),
        ("public", "svc_lcia_document_validation_evidence_lookup", ((('pg_catalog', 'jsonb'), 0),)),
        ("public", "svc_lcia_document_validation_evidence_record", ((('pg_catalog', 'jsonb'), 0), (('pg_catalog', 'uuid'), 0))),
    }
    observed_identities: set[tuple] = set()
    observed_do_hashes: list[str] = []
    grant_shapes: list[tuple] = []
    comments: dict[tuple, str | None] = {}
    for statement in statements:
        root_type, root = next(iter(statement.items()))
        if root_type == "DoStmt":
            bodies = [
                item.get("DefElem", {}).get("arg", {}).get("String", {}).get("sval")
                for item in root.get("args", [])
                if item.get("DefElem", {}).get("defname") == "as"
            ]
            if len(bodies) != 1 or bodies[0] is None:
                violations.append("document-evidence:exact-do-body-required")
            else:
                observed_do_hashes.append(hashlib.sha256(bodies[0].encode()).hexdigest())
        elif root_type == "CreateFunctionStmt":
            identity = _created_function_identity(root)
            if identity is None:
                violations.append("document-evidence:invalid-function-identity")
                continue
            observed_identities.add(identity)
            name = tuple(_function_name(root))
            bodies = _function_body_strings(root)
            if not root.get("replace") or len(bodies) != 1:
                violations.append("document-evidence:exact-function-replacement-required")
            elif hashlib.sha256(bodies[0].encode()).hexdigest() != function_hashes.get(name):
                violations.append(f"document-evidence:function-body-drift:{'.'.join(name)}")
            options = _function_options(root)
            if set(options) != {"language", "security", "set", "as"}:
                violations.append(f"document-evidence:function-options-drift:{'.'.join(name)}")
            language = options.get("language", {}).get("String", {}).get("sval")
            security = options.get("security", {}).get("Boolean", {}).get("boolval")
            setting = options.get("set", {}).get("VariableSetStmt", {})
            search_path = [
                item.get("A_Const", {}).get("sval", {}).get("sval")
                for item in setting.get("args", [])
            ]
            if language != "plpgsql" or security is not True or setting.get("name") != "search_path" or search_path != ["pg_catalog", "pg_temp"]:
                violations.append(f"document-evidence:function-security-drift:{'.'.join(name)}")
            if _type_identity(root.get("returnType", {})) != (("pg_catalog", "jsonb"), 0):
                violations.append(f"document-evidence:function-return-drift:{'.'.join(name)}")
            parameters = [item.get("FunctionParameter", {}) for item in root.get("parameters", [])]
            expected_parameters = (
                [("p_cache_keys", (("pg_catalog", "jsonb"), 0), False)]
                if name[1].endswith("lookup")
                else [
                    ("p_records", (("pg_catalog", "jsonb"), 0), False),
                    ("p_source_worker_job_id", (("pg_catalog", "uuid"), 0), True),
                ]
            )
            observed_parameters = [
                (
                    parameter.get("name"),
                    _type_identity(parameter.get("argType", {})),
                    parameter.get("defexpr", {}).get("A_Const", {}).get("isnull") is True,
                )
                for parameter in parameters
            ]
            if observed_parameters != expected_parameters:
                violations.append(f"document-evidence:function-parameters-drift:{'.'.join(name)}")
        elif root_type == "GrantStmt":
            grant_shapes.append((
                bool(root.get("is_grant")),
                tuple(sorted(_role_names(root.get("grantees", [])))),
                tuple(sorted(_granted_function_identities(root))),
                tuple(sorted(value.lower() for value in _privilege_names(root))),
                root.get("objtype"),
                bool(root.get("grant_option")),
                root.get("behavior"),
                root.get("targtype"),
            ))
        elif root_type == "CommentStmt":
            comment = root.get("object", {}).get("ObjectWithArgs", {})
            identity = tuple(_string_list(comment.get("objname", []))) + (
                tuple(_type_identity(arg) for arg in comment.get("objargs", [])),
            )
            comments[identity] = root.get("comment")

    if observed_identities != expected_identities:
        violations.append("document-evidence:exact-four-functions-required")
    if tuple(observed_do_hashes) != do_hashes:
        violations.append("document-evidence:preflight-postflight-body-drift")
    expected_comments = {
        identity: (
            "Issue #407 Phase A canonical Worker lookup. Direct EXECUTE is restricted to lca_worker_runtime; the public relation remains physical until Contract."
            if identity[0] == "private" and identity[1].endswith("lookup")
            else "Issue #407 Phase A canonical Worker idempotent record command. Direct EXECUTE is restricted to lca_worker_runtime; no relation ACL is granted."
            if identity[0] == "private"
            else "Issue #407 Phase A compatibility wrapper with caller-category-only LOG telemetry; remove only after attributed consumer-zero evidence."
        )
        for identity in expected_identities
    }
    if comments != expected_comments:
        violations.append("document-evidence:exact-four-comments-required")
    private_ids = tuple(sorted(identity for identity in expected_identities if identity[0] == "private"))
    public_ids = tuple(sorted(identity for identity in expected_identities if identity[0] == "public"))
    expected_grants = [
        (False, ("PUBLIC", "anon", "api_internal_executor", "authenticated", "lca_worker_runtime", "service_role"), private_ids, ("all",), "OBJECT_FUNCTION", False, "DROP_RESTRICT", "ACL_TARGET_OBJECT"),
        (True, ("lca_worker_runtime",), private_ids, ("execute",), "OBJECT_FUNCTION", False, "DROP_RESTRICT", "ACL_TARGET_OBJECT"),
        (False, ("PUBLIC", "anon", "api_internal_executor", "authenticated", "lca_worker_runtime", "service_role"), public_ids, ("all",), "OBJECT_FUNCTION", False, "DROP_RESTRICT", "ACL_TARGET_OBJECT"),
        (True, ("api_internal_executor", "service_role"), public_ids, ("execute",), "OBJECT_FUNCTION", False, "DROP_RESTRICT", "ACL_TARGET_OBJECT"),
    ]
    if grant_shapes != expected_grants:
        violations.append("document-evidence:exact-function-acl-statements-required")
    return sorted(set(violations))


def reviewed_document_evidence_physical_migration_violations(
    *, path: str, git_blob: str, sql: str
) -> list[str]:
    """Exact PostgreSQL-AST admission for Issue #407 Phase B only."""
    if (
        path != DOCUMENT_EVIDENCE_PHYSICAL_MIGRATION_PATH
        or git_blob != DOCUMENT_EVIDENCE_PHYSICAL_GIT_BLOB
    ):
        return ["document-evidence-physical:exact-blob-required"]
    statements, errors = _parse_sql(sql)
    if errors:
        return errors
    expected_types = (
        "TransactionStmt", "VariableSetStmt", "VariableSetStmt", "DoStmt",
        "DoStmt", "CreateTableAsStmt", "DoStmt", "ViewStmt",
        "AlterTableStmt", "GrantStmt", "GrantStmt", "GrantStmt",
        "CommentStmt", "CommentStmt", "DoStmt", "CommentStmt",
        "CommentStmt", "DoStmt", "NotifyStmt", "TransactionStmt",
    )
    observed_types = tuple(next(iter(statement)) for statement in statements)
    violations: list[str] = []
    if observed_types != expected_types:
        violations.append(
            "document-evidence-physical:exact-statement-sequence-required"
        )
        return violations
    if (
        statements[0]["TransactionStmt"].get("kind") != "TRANS_STMT_BEGIN"
        or statements[-1]["TransactionStmt"].get("kind") != "TRANS_STMT_COMMIT"
    ):
        violations.append(
            "document-evidence-physical:exact-transaction-boundary-required"
        )
    normalized = " ".join(sql.lower().split())
    lock = normalized.find(
        "lock table public.lcia_document_validation_evidence in access exclusive mode"
    )
    snapshot = normalized.find(
        "create temporary table issue_407_phase_b_relation_before"
    )
    move = normalized.find(
        "alter table public.lcia_document_validation_evidence set schema private"
    )
    if min(lock, snapshot, move) < 0 or not lock < snapshot < move:
        violations.append(
            "document-evidence-physical:lock-snapshot-move-order-required"
        )
    if (
        "create or replace view public.lcia_document_validation_evidence "
        "with (security_invoker = true)" not in normalized
    ):
        violations.append(
            "document-evidence-physical:security-invoker-view-required"
        )
    for forbidden in (
        "drop table public.lcia_document_validation_evidence",
        "create table private.lcia_document_validation_evidence",
        "insert into private.lcia_document_validation_evidence select",
    ):
        if forbidden in normalized:
            violations.append(
                "document-evidence-physical:copy-or-drop-forbidden"
            )
    return sorted(set(violations))


def reviewed_snapshot_gc_physical_migration_violations(
    *, path: str, git_blob: str, sql: str
) -> list[str]:
    """Exact PostgreSQL-AST admission for Issue #414 physical Expand only."""
    if (
        path != SNAPSHOT_GC_PHYSICAL_MIGRATION_PATH
        or git_blob != SNAPSHOT_GC_PHYSICAL_GIT_BLOB
    ):
        return ["snapshot-gc-physical:exact-blob-required"]

    statements, errors = _parse_sql(sql)
    if errors:
        return errors

    expected_types = (
        "TransactionStmt", "VariableSetStmt", "VariableSetStmt",
        "CreateFunctionStmt", "DoStmt", "DoStmt", "DoStmt", "CreateStmt",
        "DoStmt", "DoStmt", "GrantStmt", "GrantStmt", "DoStmt",
        "ViewStmt", "ViewStmt",
        "AlterTableStmt", "AlterTableStmt", "GrantStmt", "GrantStmt",
        "GrantStmt", "CommentStmt", "CommentStmt", "CommentStmt",
        "CommentStmt", "DoStmt", "NotifyStmt", "TransactionStmt",
    )
    observed_types = tuple(next(iter(statement)) for statement in statements)
    violations: list[str] = []
    if observed_types != expected_types:
        return ["snapshot-gc-physical:exact-statement-sequence-required"]

    observed_hashes = tuple(
        hashlib.sha256(
            json.dumps(
                statement, sort_keys=True, separators=(",", ":")
            ).encode()
        ).hexdigest()
        for statement in statements
    )
    if observed_hashes != SNAPSHOT_GC_PHYSICAL_STATEMENT_HASHES:
        violations.append("snapshot-gc-physical:statement-semantic-drift")

    if (
        statements[0]["TransactionStmt"].get("kind") != "TRANS_STMT_BEGIN"
        or statements[-1]["TransactionStmt"].get("kind")
        != "TRANS_STMT_COMMIT"
    ):
        violations.append(
            "snapshot-gc-physical:exact-transaction-boundary-required"
        )

    for index, expected_name, expected_value in (
        (1, "lock_timeout", "5s"),
        (2, "statement_timeout", "2min"),
    ):
        setting = statements[index]["VariableSetStmt"]
        values = [
            item.get("A_Const", {}).get("sval", {}).get("sval")
            for item in setting.get("args", [])
        ]
        if (
            setting.get("kind") != "VAR_SET_VALUE"
            or setting.get("name") != expected_name
            or not setting.get("is_local")
            or values != [expected_value]
        ):
            violations.append(
                f"snapshot-gc-physical:exact-setting-required:{expected_name}"
            )

    normalized = " ".join(sql.lower().split())
    lock = normalized.find(
        "lock table public.lca_snapshot_gc_runs, "
        "public.lca_snapshot_gc_run_items in access exclusive mode"
    )
    snapshot = normalized.find("create temporary table issue_414_before")
    move_items = normalized.find(
        "alter table public.lca_snapshot_gc_run_items set schema private"
    )
    move_runs = normalized.find(
        "alter table public.lca_snapshot_gc_runs set schema private"
    )
    if (
        min(lock, snapshot, move_items, move_runs) < 0
        or not lock < snapshot < move_items < move_runs
    ):
        violations.append(
            "snapshot-gc-physical:lock-snapshot-move-order-required"
        )

    for relation in ("lca_snapshot_gc_runs", "lca_snapshot_gc_run_items"):
        if (
            f"create or replace view public.{relation} "
            "with (security_invoker = true)" not in normalized
        ):
            violations.append(
                f"snapshot-gc-physical:security-invoker-view-required:{relation}"
            )
        for forbidden in (
            f"drop table public.{relation}",
            f"drop table private.{relation}",
            f"create table private.{relation}",
            f"insert into private.{relation} select",
            f"copy private.{relation}",
            f"copy public.{relation}",
        ):
            if forbidden in normalized:
                violations.append("snapshot-gc-physical:copy-or-drop-forbidden")

    forbidden_top_level = {
        "CallStmt", "CopyStmt", "CreateRoleStmt", "DeleteStmt", "DropStmt",
        "InsertStmt", "SelectStmt", "TruncateStmt", "UpdateStmt",
    }
    if any(statement_type in forbidden_top_level for statement_type in observed_types):
        violations.append("snapshot-gc-physical:forbidden-top-level-operation")

    created_functions = [
        _created_function_identity(statement["CreateFunctionStmt"])
        for statement in statements
        if "CreateFunctionStmt" in statement
    ]
    expected_function = (
        "pg_temp", "issue_414_relation_fingerprint",
        ((("pg_catalog", "regclass"), 0),),
    )
    if created_functions != [expected_function]:
        violations.append("snapshot-gc-physical:extra-routine-forbidden")

    for statement in statements:
        root_type, root = next(iter(statement.items()))
        if root_type in {"CreateRoleStmt", "AlterRoleStmt", "DropRoleStmt"}:
            violations.append("snapshot-gc-physical:role-ddl-forbidden")
        if root_type == "GrantRoleStmt":
            violations.append("snapshot-gc-physical:role-membership-forbidden")
        if root_type == "DoStmt":
            bodies = [
                item.get("DefElem", {}).get("arg", {}).get("String", {}).get("sval")
                for item in root.get("args", [])
                if item.get("DefElem", {}).get("defname") == "as"
            ]
            if len(bodies) != 1 or bodies[0] is None:
                violations.append("snapshot-gc-physical:exact-do-body-required")

    return sorted(set(violations))


def pre_ddl_migration_violations(
    *, path: str, git_blob: str, sql: str, allowlist: list[dict[str, str]]
) -> list[str]:
    signals = pre_ddl_sql_signals(sql)
    matching = [
        row
        for row in allowlist
        if row.get("path") == path and row.get("gitBlob") == git_blob
    ]
    if path == DOCUMENT_EVIDENCE_MIGRATION_PATH:
        if (
            len(matching) != 1
            or matching[0].get("classification")
            != DOCUMENT_EVIDENCE_CLASSIFICATION
        ):
            return ["document-evidence:exact-allowlist-entry-required"]
        return reviewed_document_evidence_migration_violations(sql)
    if path == DOCUMENT_EVIDENCE_PHYSICAL_MIGRATION_PATH:
        if (
            len(matching) != 1
            or matching[0].get("classification")
            != DOCUMENT_EVIDENCE_PHYSICAL_CLASSIFICATION
        ):
            return ["document-evidence-physical:exact-allowlist-entry-required"]
        return reviewed_document_evidence_physical_migration_violations(
            path=path, git_blob=git_blob, sql=sql
        )
    if path == SNAPSHOT_GC_PHYSICAL_MIGRATION_PATH:
        if (
            len(matching) != 1
            or matching[0].get("classification")
            != SNAPSHOT_GC_PHYSICAL_CLASSIFICATION
        ):
            return ["snapshot-gc-physical:exact-allowlist-entry-required"]
        return reviewed_snapshot_gc_physical_migration_violations(
            path=path, git_blob=git_blob, sql=sql
        )
    if path == RESULT_GC_FK_INDEX_MIGRATION_PATH:
        if (
            len(matching) != 1
            or matching[0].get("classification")
            != RESULT_GC_FK_INDEX_CLASSIFICATION
        ):
            return ["result-gc-fk-index:exact-allowlist-entry-required"]
        return reviewed_result_gc_fk_index_migration_violations(
            path=path,
            git_blob=git_blob,
            sql=sql,
        )
    if not signals:
        return []
    if (
        len(matching) == 1
        and matching[0].get("classification") == RESULT_GC_CLASSIFICATION
    ):
        return reviewed_result_gc_migration_violations(
            path=path,
            git_blob=git_blob,
            sql=sql,
        )
    hard = [signal for signal in signals if signal.startswith("hard-deny:")]
    if hard:
        return hard
    if len(matching) != 1:
        return signals
    classification = matching[0].get("classification")
    if classification == "additive-api-service-only-reviewed":
        return _facade_review_violations(sql)
    if classification == "reconcile-v1-service-only-replacement-reviewed":
        return _facade_review_violations(
            sql, replacement_identity=RECONCILE_REPLACEMENT_IDENTITY
        )
    return signals


__all__ = [
    "PGLAST_PARSER_VERSION",
    "PGLAST_VERSION",
    "pre_ddl_migration_violations",
    "pre_ddl_sql_signals",
    "reviewed_snapshot_gc_physical_migration_violations",
]


if pglast.__version__ != PGLAST_VERSION:  # pragma: no cover - import-time guard
    raise RuntimeError(
        f"Issue #390 gate requires pglast {PGLAST_VERSION}, got {pglast.__version__}"
    )
