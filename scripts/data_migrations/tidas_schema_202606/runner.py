from __future__ import annotations

import argparse
import collections
import copy
from dataclasses import dataclass
import hashlib
import json
import re
import sys
from pathlib import Path
from typing import Any, Iterable

try:
    import psycopg
    from psycopg.rows import dict_row
except Exception:  # pragma: no cover - only needed for pure unit tests
    psycopg = None
    dict_row = None

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts._db_workflow import resolve_db_url


VERSION_RE = re.compile(r"^\d{2}\.\d{2}(\.\d{3})?$")
INTEGER_RE = re.compile(r"^-?(0|[1-9]\d*)$")
REAL_RE = re.compile(r"^[+-]?(\d+(\.\d*)?|\.\d+)([Ee][+-]?\d+)?$")
UUID_RE = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$", re.IGNORECASE)

TABLE_ROOTS = {
    "flows": "flowDataSet",
    "processes": "processDataSet",
    "flowproperties": "flowPropertyDataSet",
    "unitgroups": "unitGroupDataSet",
    "contacts": "contactDataSet",
    "sources": "sourceDataSet",
    "lciamethods": "LCIAMethodDataSet",
    "lifecyclemodels": "lifeCycleModelDataSet",
}
ROOT_ALIASES = {
    "flows": ("f:flowDataSet",),
    "flowproperties": ("fp:flowPropertyDataSet",),
}
DEFAULT_TABLES = (
    "flowproperties",
    "unitgroups",
    "contacts",
    "sources",
    "lciamethods",
    "lifecyclemodels",
    "flows",
    "processes",
)

TYPE_ENUM = {
    "source data set",
    "process data set",
    "flow data set",
    "flow property data set",
    "unit group data set",
    "contact data set",
    "LCIA method data set",
    "other external file",
}
DATASET_TYPE_BY_TABLE = {
    "flows": "flow data set",
    "processes": "process data set",
    "flowproperties": "flow property data set",
    "unitgroups": "unit group data set",
    "contacts": "contact data set",
    "sources": "source data set",
    "lciamethods": "LCIA method data set",
}
TABLE_BY_DATASET_TYPE = {
    "source data set": "sources",
    "process data set": "processes",
    "flow data set": "flows",
    "flow property data set": "flowproperties",
    "unit group data set": "unitgroups",
    "contact data set": "contacts",
    "LCIA method data set": "lciamethods",
}
APPROVED_INITIAL_DATASET_VERSION = "01.00.000"
SELF_DATASET_TYPE = "__self__"
APPROVED_REFERENCE_TYPE_BY_FIELD = {
    "referenceToComplianceSystem": "source data set",
    "referenceToDataSetFormat": "source data set",
    "referenceToPersonOrEntityEnteringTheData": "contact data set",
    "referenceToPersonOrEntityEnteringTheDataSet": "contact data set",
    "referenceToPersonOrEntityGeneratingTheDataSet": "contact data set",
    "referenceToOwnershipOfDataSet": "contact data set",
    "referenceToCommissioner": "contact data set",
    "referenceToDataSetUseApproval": "contact data set",
    "referenceToNameOfReviewerAndInstitution": "contact data set",
    "referenceToReferenceUnitGroup": "unit group data set",
    "referenceToPrecedingDataSetVersion": SELF_DATASET_TYPE,
    "referenceToFlowPropertyDataSet": "flow property data set",
}

DEFAULT_TYPE_ALIASES = {
    "source dataset": "source data set",
    "source data set": "source data set",
    "process dataset": "process data set",
    "process data set": "process data set",
    "flow dataset": "flow data set",
    "flow data set": "flow data set",
    "flowproperty data set": "flow property data set",
    "flowproperties data set": "flow property data set",
    "flow property dataset": "flow property data set",
    "flow property data set": "flow property data set",
    "unitgroup data set": "unit group data set",
    "unit group dataset": "unit group data set",
    "unit group data set": "unit group data set",
    "contact": "contact data set",
    "contact dataset": "contact data set",
    "contact data set": "contact data set",
    "lcia method data": "LCIA method data set",
    "lcia method dataset": "LCIA method data set",
    "lcia method data set": "LCIA method data set",
    "other external file": "other external file",
}

LCIA_SCOPE_VALUES = {
    "Substance properties, physical and chemical",
    "Substance properties, biological",
    "Model for Transport and Fate",
    "Model for Exposure",
    "Model for Effect",
    "Model for Damage",
    "Characterisation factors",
    "Application of model",
    "Normalisation",
    "Weighting",
    "Documentation",
}
LCIA_METHOD_VALUES = {
    "Recollection / Validation of data",
    "Recalculation",
    "Cross-check with other source",
    "Cross-check with other LCIA method(ology)",
    "Expert judgement",
}

LOCATION_FIXES = {"CN-HK": "HK", "CN-MO": "MO", "CN-TW": "TW"}
SQL_VERSION_RE = r"^[0-9]{2}\.[0-9]{2}(\.[0-9]{3})?$"


@dataclass(frozen=True)
class LciaReviewMap:
    scope: dict[str, str]
    method: dict[str, str]


@dataclass
class MigrationResult:
    document: dict[str, Any]
    status: str
    changes: list[dict[str, Any]]
    issues: list[dict[str, Any]]


VersionIndex = dict[str, dict[str, set[str]]]


def normalize_version(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    stripped = value.strip()
    if VERSION_RE.fullmatch(stripped):
        return stripped
    if re.fullmatch(r"v?\d+", stripped, flags=re.IGNORECASE):
        number = int(re.findall(r"\d+", stripped)[0])
        return f"{number:02d}.00.000"
    if re.fullmatch(r"v?\d+\.\d+", stripped, flags=re.IGNORECASE):
        major, minor = [int(part) for part in re.findall(r"\d+", stripped)[:2]]
        return f"{major:02d}.{minor:02d}.000"
    if re.fullmatch(r"v?\d+\.\d+\.\d+", stripped, flags=re.IGNORECASE):
        major, minor, patch = [int(part) for part in re.findall(r"\d+", stripped)[:3]]
        return f"{major:02d}.{minor:02d}.{patch:03d}"
    if stripped.count(",") == 1:
        return normalize_version(stripped.replace(",", ".", 1))
    return None


def _json_hash(document: Any) -> str:
    payload = json.dumps(document, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.md5(payload.encode("utf-8")).hexdigest()


def _dataset_version(table: str, document: dict[str, Any]) -> str | None:
    return (
        document.get(TABLE_ROOTS[table], {})
        .get("administrativeInformation", {})
        .get("publicationAndOwnership", {})
        .get("common:dataSetVersion")
    )


def _path(parent: str, key: str | int) -> str:
    if isinstance(key, int):
        return f"{parent}[{key}]"
    escaped = key.replace('"', '\\"')
    return f'{parent}."{escaped}"'


def _path_field_name(path: str) -> str | None:
    matches = re.findall(r'"((?:[^"\\]|\\.)*)"', path)
    if not matches:
        return None
    return matches[-1].replace('\\"', '"')


def _approved_reference_type(table: str, path: str) -> str | None:
    field_name = _path_field_name(path)
    if not field_name:
        return None
    target = APPROVED_REFERENCE_TYPE_BY_FIELD.get(field_name.removeprefix("common:"))
    if target == SELF_DATASET_TYPE:
        return DATASET_TYPE_BY_TABLE.get(table)
    return target


def _iter_nodes(node: Any, path: str = "$") -> Iterable[tuple[Any, str]]:
    yield node, path
    if isinstance(node, dict):
        for key, value in node.items():
            yield from _iter_nodes(value, _path(path, key))
    elif isinstance(node, list):
        for index, value in enumerate(node):
            yield from _iter_nodes(value, _path(path, index))


def _merged_type_aliases(type_aliases: dict[str, str] | None) -> dict[str, str]:
    merged = dict(DEFAULT_TYPE_ALIASES)
    for key, value in (type_aliases or {}).items():
        merged[key.strip().lower()] = value
    return merged


def _record_change(
    changes: list[dict[str, Any]],
    *,
    rule: str,
    path: str,
    old: Any,
    new: Any,
) -> None:
    changes.append({"rule": rule, "path": path, "old": old, "new": new})


def _record_issue(
    issues: list[dict[str, Any]],
    *,
    rule: str,
    path: str,
    value: Any,
    message: str,
) -> None:
    issues.append({"rule": rule, "path": path, "value": value, "message": message})


def _version_collision(
    *,
    version_index: VersionIndex | None,
    table: str,
    row_id: str | None,
    current_version: str | None,
    target_version: str,
) -> bool:
    if not version_index or not row_id:
        return False
    known_versions = version_index.get(table, {}).get(row_id, set())
    return target_version in known_versions and target_version != (current_version or "").strip()


def _migrate_root_key(
    *,
    table: str,
    document: dict[str, Any],
    changes: list[dict[str, Any]],
    issues: list[dict[str, Any]],
) -> None:
    root_key = TABLE_ROOTS[table]
    if root_key in document:
        return
    aliases = [alias for alias in ROOT_ALIASES.get(table, ()) if alias in document]
    if len(aliases) != 1:
        return
    alias = aliases[0]
    root = document[alias]
    if not isinstance(root, dict):
        _record_issue(
            issues,
            rule="A9_dataset_root_key",
            path=f'$."{alias}"',
            value=root,
            message="prefixed dataset root is not an object",
        )
        return
    document[root_key] = document.pop(alias)
    _record_change(
        changes,
        rule="A9_dataset_root_key",
        path="$",
        old=alias,
        new=root_key,
    )


def _migrate_dataset_version(
    *,
    table: str,
    document: dict[str, Any],
    current_version: str | None,
    row_id: str | None,
    version_index: VersionIndex | None,
    changes: list[dict[str, Any]],
    issues: list[dict[str, Any]],
) -> None:
    root_key = TABLE_ROOTS[table]
    root = document.get(root_key)
    if not isinstance(root, dict):
        _record_issue(
            issues,
            rule="A2_dataset_version",
            path=f'$."{root_key}"',
            value=root,
            message="dataset root is missing or not an object",
        )
        return
    administrative = root.setdefault("administrativeInformation", {})
    if not isinstance(administrative, dict):
        _record_issue(
            issues,
            rule="A2_dataset_version",
            path=f'$."{root_key}"."administrativeInformation"',
            value=administrative,
            message="administrativeInformation is not an object",
        )
        return
    publication = administrative.setdefault("publicationAndOwnership", {})
    if not isinstance(publication, dict):
        _record_issue(
            issues,
            rule="A2_dataset_version",
            path=f'$."{root_key}"."administrativeInformation"."publicationAndOwnership"',
            value=publication,
            message="publicationAndOwnership is not an object",
        )
        return
    path = f'$."{root_key}"."administrativeInformation"."publicationAndOwnership"."common:dataSetVersion"'
    if "common:dataSetVersion" not in publication:
        if current_version and VERSION_RE.fullmatch(current_version.strip()):
            publication["common:dataSetVersion"] = current_version.strip()
            _record_change(
                changes,
                rule="A2_dataset_version",
                path=path,
                old=None,
                new=current_version.strip(),
            )
        elif (
            current_version is not None
            and not current_version.strip()
            and row_id
            and not _version_collision(
                version_index=version_index,
                table=table,
                row_id=row_id,
                current_version=current_version,
                target_version=APPROVED_INITIAL_DATASET_VERSION,
            )
        ):
            publication["common:dataSetVersion"] = APPROVED_INITIAL_DATASET_VERSION
            _record_change(
                changes,
                rule="A2_dataset_version",
                path=path,
                old=None,
                new=APPROVED_INITIAL_DATASET_VERSION,
            )
        else:
            _record_issue(
                issues,
                rule="A2_dataset_version",
                path=path,
                value=None,
                message="missing common:dataSetVersion and no canonical table version fallback",
            )
        return

    value = publication["common:dataSetVersion"]
    normalized = normalize_version(value)
    if normalized is None:
        if (
            isinstance(value, str)
            and not value.strip()
            and current_version is not None
            and not current_version.strip()
            and row_id
            and not _version_collision(
                version_index=version_index,
                table=table,
                row_id=row_id,
                current_version=current_version,
                target_version=APPROVED_INITIAL_DATASET_VERSION,
            )
        ):
            publication["common:dataSetVersion"] = APPROVED_INITIAL_DATASET_VERSION
            _record_change(
                changes,
                rule="A2_dataset_version",
                path=path,
                old=value,
                new=APPROVED_INITIAL_DATASET_VERSION,
            )
            return
        _record_issue(
            issues,
            rule="A2_dataset_version",
            path=path,
            value=value,
            message="dataset version cannot be normalized safely",
        )
    elif normalized != value:
        if _version_collision(
            version_index=version_index,
            table=table,
            row_id=row_id,
            current_version=current_version,
            target_version=normalized,
        ):
            _record_issue(
                issues,
                rule="A2_dataset_version",
                path=path,
                value=value,
                message="dataset version normalization would collide with an existing row version",
            )
            return
        publication["common:dataSetVersion"] = normalized
        _record_change(
            changes,
            rule="A2_dataset_version",
            path=path,
            old=value,
            new=normalized,
        )


def _migrate_global_reference(
    *,
    table: str,
    node: dict[str, Any],
    path: str,
    type_aliases: dict[str, str],
    version_index: VersionIndex | None,
    changes: list[dict[str, Any]],
    issues: list[dict[str, Any]],
) -> None:
    value = node.get("@type")
    if isinstance(value, str):
        if value not in TYPE_ENUM:
            normalized_type = _approved_reference_type(table, path)
            if normalized_type is None:
                normalized_type = type_aliases.get(value.strip().lower())
            if normalized_type in TYPE_ENUM:
                node["@type"] = normalized_type
                _record_change(
                    changes,
                    rule="A1_reference_type",
                    path=_path(path, "@type"),
                    old=value,
                    new=normalized_type,
                )
            else:
                _record_issue(
                    issues,
                    rule="A1_reference_type",
                    path=_path(path, "@type"),
                    value=value,
                    message="reference @type is not in the TIDAS 2026-06 enum and has no approved alias",
                )
    version = node.get("@version")
    if isinstance(version, str):
        normalized_version = normalize_version(version)
        if normalized_version is None:
            resolved_version = _resolve_reference_version(node, version_index)
            if resolved_version is not None:
                node["@version"] = resolved_version
                _record_change(
                    changes,
                    rule="A2_reference_version",
                    path=_path(path, "@version"),
                    old=version,
                    new=resolved_version,
                )
                return
            _record_issue(
                issues,
                rule="A2_reference_version",
                path=_path(path, "@version"),
                value=version,
                message="reference @version cannot be normalized safely",
            )
        elif normalized_version != version:
            node["@version"] = normalized_version
            _record_change(
                changes,
                rule="A2_reference_version",
                path=_path(path, "@version"),
                old=version,
                new=normalized_version,
            )


def _resolve_reference_version(
    node: dict[str, Any],
    version_index: VersionIndex | None,
) -> str | None:
    if not version_index:
        return None
    ref_type = node.get("@type")
    ref_id = node.get("@refObjectId")
    if not isinstance(ref_type, str) or not isinstance(ref_id, str):
        return None
    target_table = TABLE_BY_DATASET_TYPE.get(ref_type)
    if target_table is None:
        return None
    versions = version_index.get(target_table, {}).get(ref_id)
    if not versions:
        return None
    canonical_versions = [version for version in versions if VERSION_RE.fullmatch(version)]
    if not canonical_versions:
        return None
    return max(canonical_versions, key=_version_sort_key)


def _version_sort_key(version: str) -> tuple[int, int, int]:
    major, minor, *rest = [int(part) for part in version.split(".")]
    patch = rest[0] if rest else -1
    return major, minor, patch


def _migrate_lcia_review_values(
    *,
    node: dict[str, Any],
    path: str,
    review_map: LciaReviewMap,
    changes: list[dict[str, Any]],
    issues: list[dict[str, Any]],
) -> None:
    for key, allowed, mapping, rule in (
        ("common:scope", LCIA_SCOPE_VALUES, review_map.scope, "A4_lcia_review_scope"),
        ("common:method", LCIA_METHOD_VALUES, review_map.method, "A4_lcia_review_method"),
    ):
        value = node.get(key)
        values = value if isinstance(value, list) else [value] if isinstance(value, dict) else []
        for index, item in enumerate(values):
            item_path = _path(path, key)
            if isinstance(value, list):
                item_path = _path(item_path, index)
            if not isinstance(item, dict) or "@name" not in item:
                continue
            name = item["@name"]
            if name in allowed:
                continue
            mapped = mapping.get(name)
            if mapped in allowed:
                item["@name"] = mapped
                _record_change(
                    changes,
                    rule=rule,
                    path=_path(item_path, "@name"),
                    old=name,
                    new=mapped,
                )
            else:
                _record_issue(
                    issues,
                    rule=rule,
                    path=_path(item_path, "@name"),
                    value=name,
                    message="LCIA review value has no approved mapping",
                )


def _migrate_lcia_booleans_and_factor_keys(
    *,
    node: dict[str, Any],
    path: str,
    changes: list[dict[str, Any]],
    issues: list[dict[str, Any]],
) -> None:
    for key in ("normalisation", "weighting"):
        value = node.get(key)
        if value in ("true", "false"):
            new_value = value == "true"
            node[key] = new_value
            _record_change(
                changes,
                rule="A6_lcia_boolean",
                path=_path(path, key),
                old=value,
                new=new_value,
            )
    if "uncertaintyType" in node:
        old_value = node["uncertaintyType"]
        if "uncertaintyDistributionType" in node:
            _record_issue(
                issues,
                rule="A5_lcia_uncertainty_key",
                path=_path(path, "uncertaintyType"),
                value=old_value,
                message="both uncertaintyType and uncertaintyDistributionType are present",
            )
        else:
            node["uncertaintyDistributionType"] = node.pop("uncertaintyType")
            _record_change(
                changes,
                rule="A5_lcia_uncertainty_key",
                path=_path(path, "uncertaintyType"),
                old=old_value,
                new={"uncertaintyDistributionType": old_value},
            )


def _migrate_location(
    *,
    node: dict[str, Any],
    path: str,
    changes: list[dict[str, Any]],
) -> None:
    value = node.get("@location")
    if value in LOCATION_FIXES:
        node["@location"] = LOCATION_FIXES[value]
        _record_change(
            changes,
            rule="A7_location_code",
            path=_path(path, "@location"),
            old=value,
            new=LOCATION_FIXES[value],
        )


def _migrate_lifecycle_fields(
    *,
    node: dict[str, Any],
    path: str,
    unresolved_lifecycle_version: str | None,
    changes: list[dict[str, Any]],
    issues: list[dict[str, Any]],
) -> None:
    if "referenceToReferenceProcess" in node:
        value = node["referenceToReferenceProcess"]
        if isinstance(value, str):
            if INTEGER_RE.fullmatch(value):
                node["referenceToReferenceProcess"] = int(value)
                _record_change(
                    changes,
                    rule="B1_lifecycle_reference_process",
                    path=_path(path, "referenceToReferenceProcess"),
                    old=value,
                    new=int(value),
                )
            else:
                _record_issue(
                    issues,
                    rule="B1_lifecycle_reference_process",
                    path=_path(path, "referenceToReferenceProcess"),
                    value=value,
                    message="referenceToReferenceProcess cannot be converted without losing lexical identity",
                )

    for key in ("outputExchange", "downstreamProcess"):
        value = node.get(key)
        items = value if isinstance(value, list) else [value] if isinstance(value, dict) else []
        for index, item in enumerate(items):
            item_path = _path(path, key)
            if isinstance(value, list):
                item_path = _path(item_path, index)
            if not isinstance(item, dict) or "@version" in item:
                continue
            if unresolved_lifecycle_version:
                item["@version"] = unresolved_lifecycle_version
                _record_change(
                    changes,
                    rule="A8_lifecycle_reference_version",
                    path=_path(item_path, "@version"),
                    old=None,
                    new=unresolved_lifecycle_version,
                )
            else:
                _record_issue(
                    issues,
                    rule="A8_lifecycle_reference_version",
                    path=_path(item_path, "@version"),
                    value=None,
                    message="missing lifecycle reference @version and no fallback was configured",
                )


def migrate_document(
    table: str,
    document: dict[str, Any],
    *,
    type_aliases: dict[str, str] | None,
    lcia_review_map: LciaReviewMap,
    unresolved_lifecycle_version: str | None,
    current_version: str | None = None,
    row_id: str | None = None,
    version_index: VersionIndex | None = None,
) -> MigrationResult:
    if table not in TABLE_ROOTS:
        raise ValueError(f"unsupported table: {table}")
    changes: list[dict[str, Any]] = []
    issues: list[dict[str, Any]] = []
    aliases = _merged_type_aliases(type_aliases)

    if document == {}:
        _record_change(
            changes,
            rule="D1_empty_json_delete",
            path="$",
            old={},
            new=None,
        )
        return MigrationResult(document=document, status="delete_planned", changes=changes, issues=issues)

    _migrate_root_key(
        table=table,
        document=document,
        changes=changes,
        issues=issues,
    )

    _migrate_dataset_version(
        table=table,
        document=document,
        current_version=current_version,
        row_id=row_id,
        version_index=version_index,
        changes=changes,
        issues=issues,
    )

    for node, path in list(_iter_nodes(document)):
        if not isinstance(node, dict):
            continue
        if "@type" in node and "@refObjectId" in node:
            _migrate_global_reference(
                table=table,
                node=node,
                path=path,
                type_aliases=aliases,
                version_index=version_index,
                changes=changes,
                issues=issues,
            )
        _migrate_location(node=node, path=path, changes=changes)
        if table == "lciamethods":
            _migrate_lcia_review_values(
                node=node,
                path=path,
                review_map=lcia_review_map,
                changes=changes,
                issues=issues,
            )
            _migrate_lcia_booleans_and_factor_keys(
                node=node,
                path=path,
                changes=changes,
                issues=issues,
            )
        if table == "lifecyclemodels":
            _migrate_lifecycle_fields(
                node=node,
                path=path,
                unresolved_lifecycle_version=unresolved_lifecycle_version,
                changes=changes,
                issues=issues,
            )

    if issues:
        status = "manual_required"
    elif changes:
        status = "planned"
    else:
        status = "clean"
    return MigrationResult(document=document, status=status, changes=changes, issues=issues)


def validate_required_rules(table: str, document: dict[str, Any]) -> list[dict[str, Any]]:
    issues: list[dict[str, Any]] = []
    root_key = TABLE_ROOTS[table]
    doc_version = (
        document.get(root_key, {})
        .get("administrativeInformation", {})
        .get("publicationAndOwnership", {})
        .get("common:dataSetVersion")
    )
    if not isinstance(doc_version, str) or not VERSION_RE.fullmatch(doc_version):
        _record_issue(
            issues,
            rule="A2_dataset_version",
            path=f'$."{root_key}"."administrativeInformation"."publicationAndOwnership"."common:dataSetVersion"',
            value=doc_version,
            message="dataset version is missing or noncanonical",
        )
    for node, path in _iter_nodes(document):
        if not isinstance(node, dict):
            continue
        if "@type" in node and "@refObjectId" in node:
            value = node.get("@type")
            if value not in TYPE_ENUM:
                _record_issue(
                    issues,
                    rule="A1_reference_type",
                    path=_path(path, "@type"),
                    value=value,
                    message="reference @type is not canonical",
                )
            version = node.get("@version")
            if isinstance(version, str) and not VERSION_RE.fullmatch(version):
                _record_issue(
                    issues,
                    rule="A2_reference_version",
                    path=_path(path, "@version"),
                    value=version,
                    message="reference @version is not canonical",
                )
        if node.get("@location") in LOCATION_FIXES:
            _record_issue(
                issues,
                rule="A7_location_code",
                path=_path(path, "@location"),
                value=node.get("@location"),
                message="legacy CN-* location code remains",
            )
        if table == "lciamethods":
            if "uncertaintyType" in node:
                _record_issue(
                    issues,
                    rule="A5_lcia_uncertainty_key",
                    path=_path(path, "uncertaintyType"),
                    value=node["uncertaintyType"],
                    message="legacy uncertaintyType key remains",
                )
            for key in ("normalisation", "weighting"):
                if node.get(key) in ("true", "false"):
                    _record_issue(
                        issues,
                        rule="A6_lcia_boolean",
                        path=_path(path, key),
                        value=node.get(key),
                        message="LCIA boolean remains a string",
                    )
        if table == "lifecyclemodels":
            if "referenceToReferenceProcess" in node and isinstance(
                node["referenceToReferenceProcess"], str
            ):
                _record_issue(
                    issues,
                    rule="B1_lifecycle_reference_process",
                    path=_path(path, "referenceToReferenceProcess"),
                    value=node["referenceToReferenceProcess"],
                    message="referenceToReferenceProcess remains a string",
                )
            for key in ("outputExchange", "downstreamProcess"):
                value = node.get(key)
                items = value if isinstance(value, list) else [value] if isinstance(value, dict) else []
                for index, item in enumerate(items):
                    if isinstance(item, dict) and "@version" not in item:
                        item_path = _path(path, key)
                        if isinstance(value, list):
                            item_path = _path(item_path, index)
                        _record_issue(
                            issues,
                            rule="A8_lifecycle_reference_version",
                            path=_path(item_path, "@version"),
                            value=None,
                            message="lifecycle reference @version missing",
                        )
    return issues


def _parse_yamlish_mapping(path: str | None) -> dict[str, Any]:
    if not path:
        return {}
    try:
        import yaml
    except Exception as exc:
        raise RuntimeError("pyyaml is required when using mapping files") from exc
    with open(path, "r", encoding="utf-8") as handle:
        return yaml.safe_load(handle) or {}


def load_type_aliases(path: str | None) -> dict[str, str]:
    data = _parse_yamlish_mapping(path)
    aliases = data.get("aliases", data)
    return {str(key): str(value) for key, value in aliases.items()} if aliases else {}


def load_lcia_review_map(path: str | None) -> LciaReviewMap:
    data = _parse_yamlish_mapping(path)
    return LciaReviewMap(
        scope={str(key): str(value) for key, value in (data.get("scope") or {}).items()},
        method={str(key): str(value) for key, value in (data.get("method") or {}).items()},
    )


def parse_tables(value: str | None) -> list[str]:
    if not value:
        return list(DEFAULT_TABLES)
    tables = [item.strip() for item in value.split(",") if item.strip()]
    unsupported = [table for table in tables if table not in TABLE_ROOTS]
    if unsupported:
        raise ValueError(f"unsupported table(s): {', '.join(unsupported)}")
    return tables


def connect(db_url: str):
    if psycopg is None:
        raise RuntimeError("psycopg is required for database commands")
    return psycopg.connect(db_url, row_factory=dict_row, prepare_threshold=None)


def _sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _jsonpath_literal(value: str) -> str:
    return _sql_literal(value)


def build_candidate_predicate(table: str) -> str:
    root_key = TABLE_ROOTS[table]
    version_path = (
        f"{{{root_key},administrativeInformation,publicationAndOwnership,common:dataSetVersion}}"
    )
    dataset_version = (
        f"(t.json #>> '{version_path}') is null "
        f"or not ((t.json #>> '{version_path}') ~ {_sql_literal(SQL_VERSION_RE)})"
    )
    allowed_types = " || ".join(
        f'@."@type" == "{value}"' for value in sorted(TYPE_ENUM)
    )
    invalid_ref_type = (
        '$.** ? (exists(@."@refObjectId") && exists(@."@type") && '
        f"!({allowed_types}))"
    )
    bad_ref_version = (
        '$.** ? (exists(@."@refObjectId") && exists(@."@type") && exists(@."@version") && '
        f'!(@."@version" like_regex "{SQL_VERSION_RE}"))'
    )
    legacy_location = (
        '$.** ? (exists(@."@location") && '
        '(@."@location" == "CN-HK" || @."@location" == "CN-MO" || @."@location" == "CN-TW"))'
    )

    predicates = [
        f"({dataset_version})",
        f"jsonb_path_exists(t.json, {_jsonpath_literal(invalid_ref_type)}::jsonpath)",
        f"jsonb_path_exists(t.json, {_jsonpath_literal(bad_ref_version)}::jsonpath)",
        f"jsonb_path_exists(t.json, {_jsonpath_literal(legacy_location)}::jsonpath)",
    ]

    if table == "lifecyclemodels":
        # This table is small enough to scan fully, and missing nested @version
        # markers are easier to assess safely in Python than in jsonpath.
        predicates.append("true")

    if table == "lciamethods":
        lcia_legacy = (
            '$.** ? (exists(@."uncertaintyType") || '
            '@."normalisation" == "true" || @."normalisation" == "false" || '
            '@."weighting" == "true" || @."weighting" == "false")'
        )
        predicates.append(
            f"jsonb_path_exists(t.json, {_jsonpath_literal(lcia_legacy)}::jsonpath)"
        )

    return " or ".join(f"({predicate})" for predicate in predicates)


def fetch_rows(
    conn,
    table: str,
    *,
    after_id: str | None = None,
    after_version: str | None = None,
    limit: int = 1000,
    all_rows: bool = False,
) -> list[dict[str, Any]]:
    candidate_predicate = "true" if all_rows else build_candidate_predicate(table)
    with conn.cursor() as cur:
        if after_id is None:
            cur.execute(
                f"""
                select id::text as id,
                       trim(version::text) as version,
                       t.xmin::text as row_xmin,
                       json
                from public.{table} as t
                where t.json is not null
                  and ({candidate_predicate})
                order by t.id, t.version
                limit %s
                """,
                (limit,),
            )
        else:
            cur.execute(
                f"""
                select id::text as id,
                       trim(version::text) as version,
                       t.xmin::text as row_xmin,
                       json
                from public.{table} as t
                where t.json is not null
                  and ({candidate_predicate})
                  and (t.id, t.version) > (%s::uuid, %s::character(9))
                order by t.id, t.version
                limit %s
                """,
                (after_id, after_version, limit),
            )
        return list(cur.fetchall())


def collect_version_lookup_ids(
    *,
    table: str,
    rows: Iterable[dict[str, Any]],
    type_aliases: dict[str, str],
) -> dict[str, set[str]]:
    lookup_ids: dict[str, set[str]] = {table: set()}
    for row in rows:
        row_id = row.get("id")
        if isinstance(row_id, str):
            lookup_ids.setdefault(table, set()).add(row_id)
        document = row.get("json")
        if not isinstance(document, dict):
            continue
        for node, path in _iter_nodes(document):
            if not isinstance(node, dict):
                continue
            ref_id = node.get("@refObjectId")
            if not isinstance(ref_id, str) or not UUID_RE.fullmatch(ref_id):
                continue
            version = node.get("@version")
            if not isinstance(version, str) or normalize_version(version) is not None:
                continue
            ref_type = node.get("@type")
            target_type = ref_type if ref_type in TYPE_ENUM else None
            if target_type is None:
                target_type = _approved_reference_type(table, path)
            if target_type is None and isinstance(ref_type, str):
                target_type = type_aliases.get(ref_type.strip().lower())
            target_table = TABLE_BY_DATASET_TYPE.get(target_type or "")
            if target_table:
                lookup_ids.setdefault(target_table, set()).add(ref_id)
    return {name: ids for name, ids in lookup_ids.items() if ids}


def fetch_version_index(conn, lookup_ids: dict[str, set[str]]) -> VersionIndex:
    version_index: VersionIndex = {}
    for table, ids in lookup_ids.items():
        if not ids:
            continue
        table_versions: dict[str, set[str]] = {}
        with conn.cursor() as cur:
            cur.execute(
                f"""
                select id::text as id,
                       trim(version::text) as version
                from public.{table}
                where id = any(%s::uuid[])
                order by id, version
                """,
                (list(ids),),
            )
            for row in cur.fetchall():
                table_versions.setdefault(row["id"], set()).add(row["version"])
        version_index[table] = table_versions
    return version_index


def iter_rows(
    conn, table: str, *, page_size: int, all_rows: bool = False
) -> Iterable[list[dict[str, Any]]]:
    after_id: str | None = None
    after_version: str | None = None
    while True:
        rows = fetch_rows(
            conn,
            table,
            after_id=after_id,
            after_version=after_version,
            limit=page_size,
            all_rows=all_rows,
        )
        if not rows:
            return
        yield rows
        after_id = rows[-1]["id"]
        after_version = rows[-1]["version"]


def build_row_event(
    *,
    run_id: str,
    phase: str,
    table: str,
    row: dict[str, Any],
    result: MigrationResult,
    include_hashes: bool,
    compact: bool,
) -> dict[str, Any]:
    old_doc = row["json"]
    new_version = _dataset_version(table, result.document)
    event = {
        "runId": run_id,
        "phase": phase,
        "table": table,
        "id": row["id"],
        "oldVersion": row["version"],
        "newVersion": new_version,
        "status": result.status,
    }
    if compact:
        event["changeCount"] = len(result.changes)
        event["issueCount"] = len(result.issues)
        event["changeRules"] = dict(
            sorted(collections.Counter(change["rule"] for change in result.changes).items())
        )
        event["issueRules"] = dict(
            sorted(collections.Counter(issue["rule"] for issue in result.issues).items())
        )
    else:
        event["changes"] = result.changes
        event["issues"] = result.issues
    if include_hashes:
        event["oldHash"] = _json_hash(old_doc)
        event["newHash"] = _json_hash(result.document)
    return event


def open_output(path: str | None):
    if not path:
        return sys.stdout, False
    output_path = Path(path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    return output_path.open("w", encoding="utf-8"), True


def write_event(handle, event: dict[str, Any]) -> None:
    handle.write(json.dumps(event, ensure_ascii=False, sort_keys=True) + "\n")
    handle.flush()


def iter_plan_results(
    args, phase: str
) -> Iterable[tuple[str, dict[str, Any], MigrationResult]]:
    db_url = resolve_db_url(args.environment, args.database_url)
    type_aliases = load_type_aliases(args.type_map)
    lcia_review_map = load_lcia_review_map(args.lcia_review_map)
    with connect(db_url) as conn:
        with conn.cursor() as cur:
            cur.execute("set statement_timeout = '5min'")
        for table in parse_tables(args.tables):
            pages = 0
            rows_seen = 0
            print(
                json.dumps({"phase": phase, "table": table, "event": "table_start"}, sort_keys=True),
                file=sys.stderr,
                flush=True,
            )
            for page in iter_rows(conn, table, page_size=args.page_size, all_rows=args.all_rows):
                pages += 1
                rows_seen += len(page)
                version_index = fetch_version_index(
                    conn,
                    collect_version_lookup_ids(
                        table=table,
                        rows=page,
                        type_aliases=_merged_type_aliases(type_aliases),
                    ),
                )
                for row in page:
                    document = copy.deepcopy(row["json"])
                    result = migrate_document(
                        table,
                        document,
                        type_aliases=type_aliases,
                        lcia_review_map=lcia_review_map,
                        unresolved_lifecycle_version=args.unresolved_lifecycle_version,
                        current_version=row["version"],
                        row_id=row["id"],
                        version_index=version_index,
                    )
                    yield table, row, result
                if args.progress_every_pages and pages % args.progress_every_pages == 0:
                    print(
                        json.dumps(
                            {
                                "phase": phase,
                                "table": table,
                                "event": "progress",
                                "pages": pages,
                                "rows": rows_seen,
                            },
                            sort_keys=True,
                        ),
                        file=sys.stderr,
                        flush=True,
                    )
            print(
                json.dumps(
                    {
                        "phase": phase,
                        "table": table,
                        "event": "table_done",
                        "pages": pages,
                        "rows": rows_seen,
                    },
                    sort_keys=True,
                ),
                file=sys.stderr,
                flush=True,
            )


def cmd_scan(args) -> int:
    # scan intentionally uses the same planner but does not write.
    return cmd_plan(args, phase="scan")


def cmd_plan(args, phase: str = "plan") -> int:
    handle, should_close = open_output(args.out)
    try:
        counts: dict[str, int] = {}
        for table, row, result in iter_plan_results(args, phase):
            counts[result.status] = counts.get(result.status, 0) + 1
            if result.status != "clean" or args.emit_clean:
                event = build_row_event(
                    run_id=args.run_id,
                    phase=phase,
                    table=table,
                    row=row,
                    result=result,
                    include_hashes=args.event_hashes,
                    compact=args.compact_events,
                )
                write_event(handle, event)
        print(json.dumps({"phase": phase, "counts": counts}, sort_keys=True), file=sys.stderr)
    finally:
        if should_close:
            handle.close()
    return 0


def _chunks(items: list[Any], size: int) -> Iterable[list[Any]]:
    for index in range(0, len(items), size):
        yield items[index : index + size]


def cmd_apply(args) -> int:
    db_url = resolve_db_url(args.environment, args.database_url)
    type_aliases = load_type_aliases(args.type_map)
    lcia_review_map = load_lcia_review_map(args.lcia_review_map)
    handle, should_close = open_output(args.out)
    counts: dict[str, int] = {}
    try:
        with connect(db_url) as conn:
            conn.autocommit = False
            with conn.cursor() as cur:
                cur.execute("set statement_timeout = '5min'")
            for table in parse_tables(args.tables):
                pages = 0
                rows_seen = 0
                print(
                    json.dumps(
                        {
                            "phase": "apply",
                            "dryRun": args.dry_run,
                            "table": table,
                            "event": "table_start",
                        },
                        sort_keys=True,
                    ),
                    file=sys.stderr,
                    flush=True,
                )
                for page in iter_rows(conn, table, page_size=args.page_size, all_rows=args.all_rows):
                    pages += 1
                    rows_seen += len(page)
                    version_index = fetch_version_index(
                        conn,
                        collect_version_lookup_ids(
                            table=table,
                            rows=page,
                            type_aliases=_merged_type_aliases(type_aliases),
                        ),
                    )
                    planned: list[tuple[dict[str, Any], MigrationResult]] = []
                    for row in page:
                        result = migrate_document(
                            table,
                            copy.deepcopy(row["json"]),
                            type_aliases=type_aliases,
                            lcia_review_map=lcia_review_map,
                            unresolved_lifecycle_version=args.unresolved_lifecycle_version,
                            current_version=row["version"],
                            row_id=row["id"],
                            version_index=version_index,
                        )
                        if result.status not in ("planned", "delete_planned"):
                            counts[result.status] = counts.get(result.status, 0) + 1
                            if result.status != "clean" or args.emit_clean:
                                event = build_row_event(
                                    run_id=args.run_id,
                                    phase="apply",
                                    table=table,
                                    row=row,
                                    result=result,
                                    include_hashes=args.event_hashes,
                                    compact=args.compact_events,
                                )
                                write_event(handle, event)
                            continue
                        planned.append((row, result))

                    for batch in _chunks(planned, args.batch_size):
                        try:
                            with conn.transaction():
                                with conn.cursor() as cur:
                                    cur.execute("set local statement_timeout = '30s'")
                                    cur.execute("set local lock_timeout = '3s'")
                                    if args.suppress_user_triggers:
                                        cur.execute("set local session_replication_role = replica")
                                    for row, result in batch:
                                        event = build_row_event(
                                            run_id=args.run_id,
                                            phase="apply",
                                            table=table,
                                            row=row,
                                            result=result,
                                            include_hashes=args.event_hashes,
                                            compact=args.compact_events,
                                        )
                                        if result.status == "delete_planned":
                                            cur.execute(
                                                f"""
                                                delete from public.{table}
                                                where id = %s::uuid
                                                  and version = %s::character(9)
                                                  and xmin = %s::xid
                                                returning id::text as deleted_id
                                                """,
                                                (row["id"], row["version"], row["row_xmin"]),
                                            )
                                            updated = cur.fetchall()
                                            if len(updated) != 1:
                                                raise RuntimeError(
                                                    f"expected one row delete for {table} {row['id']} {row['version']}, got {len(updated)}"
                                                )
                                            event["newVersion"] = None
                                            event["status"] = "rolled_back" if args.dry_run else "deleted"
                                            counts[event["status"]] = counts.get(event["status"], 0) + 1
                                            write_event(handle, event)
                                            continue

                                        payload = json.dumps(result.document, ensure_ascii=False, separators=(",", ":"))
                                        if args.suppress_user_triggers:
                                            new_row_version = _dataset_version(table, result.document) or row["version"]
                                            cur.execute(
                                                f"""
                                                update public.{table}
                                                set json_ordered = %s::json,
                                                    json = %s::jsonb,
                                                    version = %s::character(9),
                                                    modified_at = now()
                                                where id = %s::uuid
                                                  and version = %s::character(9)
                                                  and xmin = %s::xid
                                                returning trim(version::text) as new_version
                                                """,
                                                (
                                                    payload,
                                                    payload,
                                                    new_row_version,
                                                    row["id"],
                                                    row["version"],
                                                    row["row_xmin"],
                                                ),
                                            )
                                        else:
                                            cur.execute(
                                                f"""
                                                update public.{table}
                                                set json_ordered = %s::json
                                                where id = %s::uuid
                                                  and version = %s::character(9)
                                                  and xmin = %s::xid
                                                returning trim(version::text) as new_version
                                                """,
                                                (payload, row["id"], row["version"], row["row_xmin"]),
                                            )
                                        updated = cur.fetchall()
                                        if len(updated) != 1:
                                            raise RuntimeError(
                                                f"expected one row update for {table} {row['id']} {row['version']}, got {len(updated)}"
                                            )
                                        event["newVersion"] = updated[0]["new_version"]
                                        event["status"] = "rolled_back" if args.dry_run else "written"
                                        counts[event["status"]] = counts.get(event["status"], 0) + 1
                                        write_event(handle, event)
                                if args.dry_run:
                                    raise _DryRunRollback()
                        except _DryRunRollback:
                            conn.rollback()
                        except Exception as exc:
                            conn.rollback()
                            for row, result in batch:
                                event = build_row_event(
                                    run_id=args.run_id,
                                    phase="apply",
                                    table=table,
                                    row=row,
                                    result=result,
                                    include_hashes=args.event_hashes,
                                    compact=args.compact_events,
                                )
                                event["status"] = "write_conflict"
                                event["error"] = str(exc)
                                counts["write_conflict"] = counts.get("write_conflict", 0) + 1
                                write_event(handle, event)
                            if not args.continue_on_error:
                                print(json.dumps({"phase": "apply", "counts": counts}, sort_keys=True), file=sys.stderr)
                                return 1
                    if args.progress_every_pages and pages % args.progress_every_pages == 0:
                        print(
                            json.dumps(
                                {
                                    "phase": "apply",
                                    "dryRun": args.dry_run,
                                    "table": table,
                                    "event": "progress",
                                    "pages": pages,
                                    "rows": rows_seen,
                                    "counts": counts,
                                },
                                sort_keys=True,
                            ),
                            file=sys.stderr,
                            flush=True,
                        )
                print(
                    json.dumps(
                        {
                            "phase": "apply",
                            "dryRun": args.dry_run,
                            "table": table,
                            "event": "table_done",
                            "pages": pages,
                            "rows": rows_seen,
                            "counts": counts,
                        },
                        sort_keys=True,
                    ),
                    file=sys.stderr,
                    flush=True,
                )
        print(json.dumps({"phase": "apply", "dryRun": args.dry_run, "counts": counts}, sort_keys=True), file=sys.stderr)
        return 0
    finally:
        if should_close:
            handle.close()


class _DryRunRollback(Exception):
    pass


def cmd_validate(args) -> int:
    db_url = resolve_db_url(args.environment, args.database_url)
    handle, should_close = open_output(args.out)
    counts: dict[str, int] = {}
    try:
        with connect(db_url) as conn:
            for table in parse_tables(args.tables):
                for page in iter_rows(conn, table, page_size=args.page_size, all_rows=args.all_rows):
                    for row in page:
                        issues = validate_required_rules(table, row["json"])
                        status = "validation_failed" if issues else "clean"
                        counts[status] = counts.get(status, 0) + 1
                        if issues or args.emit_clean:
                            write_event(
                                handle,
                                {
                                    "runId": args.run_id,
                                    "phase": "validate",
                                    "table": table,
                                    "id": row["id"],
                                    "version": row["version"],
                                    "status": status,
                                    "issues": issues,
                                },
                            )
        print(json.dumps({"phase": "validate", "counts": counts}, sort_keys=True), file=sys.stderr)
        return 1 if counts.get("validation_failed") else 0
    finally:
        if should_close:
            handle.close()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="TIDAS schema 2026-06 database data migration runner")
    subparsers = parser.add_subparsers(dest="command", required=True)

    def add_common(subparser: argparse.ArgumentParser) -> None:
        subparser.add_argument("--database-url")
        subparser.add_argument("--environment", default="dev", choices=("dev", "main", "local"))
        subparser.add_argument("--run-id", required=True)
        subparser.add_argument("--tables")
        subparser.add_argument("--type-map")
        subparser.add_argument("--lcia-review-map")
        subparser.add_argument("--unresolved-lifecycle-version", default="00.00.001")
        subparser.add_argument("--out")
        subparser.add_argument("--emit-clean", action="store_true")
        subparser.add_argument("--page-size", type=int, default=1000)
        subparser.add_argument("--progress-every-pages", type=int, default=50)
        subparser.add_argument("--all-rows", action="store_true")
        subparser.add_argument("--event-hashes", action="store_true")
        subparser.add_argument("--compact-events", action="store_true")

    scan = subparsers.add_parser("scan")
    add_common(scan)
    scan.set_defaults(func=cmd_scan)

    plan = subparsers.add_parser("plan")
    add_common(plan)
    plan.add_argument("--dry-run", action="store_true")
    plan.set_defaults(func=cmd_plan)

    apply = subparsers.add_parser("apply")
    add_common(apply)
    apply.add_argument("--batch-size", type=int, default=50)
    apply.add_argument("--dry-run", action="store_true")
    apply.add_argument("--continue-on-error", action="store_true")
    apply.add_argument("--suppress-user-triggers", action="store_true")
    apply.set_defaults(func=cmd_apply)

    validate = subparsers.add_parser("validate")
    add_common(validate)
    validate.set_defaults(func=cmd_validate)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
