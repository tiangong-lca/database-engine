#!/usr/bin/env python3
"""Build and verify the complete public-object inventory and dependency closure."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import re
import subprocess
from collections import defaultdict
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
CONTRACT_DIR = ROOT / "supabase/tests/contracts"
LEDGER = CONTRACT_DIR / "public_object_target_ledger.tsv"
CONSUMERS = CONTRACT_DIR / "public_object_consumers.json"
OUT = CONTRACT_DIR / "public_object_inventory.json"
SHA = CONTRACT_DIR / "public_object_inventory.sha256"
GENESIS_OUT = CONTRACT_DIR / "public_object_inventory.genesis.json"
GENESIS_SHA = CONTRACT_DIR / "public_object_inventory.genesis.sha256"

SOURCE = {
    # Workspace/report lineage. These identify the original #338 evidence context.
    "baseline": "tiangong-lca/workspace#533",
    "workspaceBaselineSha": "520b7af67240beb0f08419ab432a018d93542170",
    "workspacePinnedDatabaseSha": "1516ad7bb3f74734095756e741f00f60e93b79b3",
    # The only migration/catalog input for current artifact regeneration.
    "databaseSchemaSha": "05d1387cc073da8161db782db978a77431ff8b3f",
    # Historical #345 branch lineage; never use these as the schema reset target.
    "databaseBaseSha": "157ef7bb4e844edb26525dfb89f4fde188ee0cef",
    "databaseInventorySha": "86203c9190b11f12109a7fdd3f310ff47a47c9e5",
    "databaseMergeBaseSha": "907f7b6a47b98c401d98184a8b7452aaaa429bbf",
    "previousArtifactSha256": "250d91d0df9edcf2a187b635b35829c4bdba93cfc73d330b9c30320479838adf",
}
SOURCE_SHA_FIELDS = (
    "workspaceBaselineSha",
    "workspacePinnedDatabaseSha",
    "databaseSchemaSha",
    "databaseBaseSha",
    "databaseInventorySha",
    "databaseMergeBaseSha",
)

CORE_TABLES = {
    "contacts", "flowproperties", "flows", "ilcd", "lciamethods",
    "lifecyclemodels", "processes", "sources", "unitgroups",
}
VALID_TARGETS = {"public", "api", "private", "util", "archive", "retire"}
TARGET_NORMALIZATION = {"private_or_retire": "private", "util_or_retire": "util"}

CATALOG_SQL = r"""
with
relations as (
  select c.oid, n.nspname as schema_name, c.relname as object_name,
    case c.relkind when 'r' then 'table' when 'p' then 'partitioned_table'
      when 'v' then 'view' when 'm' then 'materialized_view' end as object_type,
    format('%I.%I', n.nspname, c.relname) as object_key,
    pg_get_userbyid(c.relowner) as owner_role, c.relrowsecurity as rls_enabled,
    c.relforcerowsecurity as force_rls,
    coalesce(c.reloptions, '{}'::text[]) as options
  from pg_class c join pg_namespace n on n.oid = c.relnamespace
  where n.nspname = 'public' and c.relkind in ('r','p','v','m')
), routines as (
  select p.oid, n.nspname as schema_name, p.proname as object_name,
    case p.prokind when 'p' then 'procedure' else 'function' end as object_type,
    format('%I.%I(%s)', n.nspname, p.proname,
      pg_get_function_identity_arguments(p.oid)) as object_key,
    pg_get_userbyid(p.proowner) as owner_role, p.prosecdef as security_definer,
    p.proleakproof as leakproof, p.provolatile as volatility,
    coalesce(p.proconfig, '{}'::text[]) as config,
    pg_get_function_result(p.oid) as result_type,
    pg_get_functiondef(p.oid) as definition
  from pg_proc p join pg_namespace n on n.oid = p.pronamespace
  where n.nspname = 'public' and p.prokind in ('f','p')
), relation_acl as (
  select r.object_key, coalesce(grantee.rolname, 'PUBLIC') as grantee,
    x.privilege_type, x.is_grantable
  from relations r join pg_class c on c.oid = r.oid
  cross join lateral aclexplode(coalesce(c.relacl, acldefault('r', c.relowner))) x
  left join pg_roles grantee on grantee.oid = x.grantee
), routine_acl as (
  select r.object_key, coalesce(grantee.rolname, 'PUBLIC') as grantee,
    x.privilege_type, x.is_grantable
  from routines r join pg_proc p on p.oid = r.oid
  cross join lateral aclexplode(coalesce(p.proacl, acldefault('f', p.proowner))) x
  left join pg_roles grantee on grantee.oid = x.grantee
), policies as (
  select format('%I.%I', schemaname, tablename) as object_key, policyname,
    permissive, roles, cmd, qual, with_check
  from pg_policies where schemaname = 'public'
), foreign_keys as (
  select format('%I.%I', sn.nspname, sc.relname) as source_key,
    format('%I.%I', tn.nspname, tc.relname) as target_key,
    con.conname as dependency_name, pg_get_constraintdef(con.oid, true) as definition
  from pg_constraint con
  join pg_class sc on sc.oid=con.conrelid join pg_namespace sn on sn.oid=sc.relnamespace
  join pg_class tc on tc.oid=con.confrelid join pg_namespace tn on tn.oid=tc.relnamespace
  where con.contype='f' and (sn.nspname='public' or tn.nspname='public')
), triggers as (
  select format('%I.%I', n.nspname, c.relname) as source_key,
    format('%I.%I(%s)', pn.nspname, p.proname,
      pg_get_function_identity_arguments(p.oid)) as target_key,
    t.tgname as dependency_name, pg_get_triggerdef(t.oid, true) as definition
  from pg_trigger t join pg_class c on c.oid=t.tgrelid
  join pg_namespace n on n.oid=c.relnamespace join pg_proc p on p.oid=t.tgfoid
  join pg_namespace pn on pn.oid=p.pronamespace
  where not t.tgisinternal and (n.nspname='public' or pn.nspname='public')
), rewrites as (
  select distinct format('%I.%I', vn.nspname, vc.relname) as source_key,
    format('%I.%I', rn.nspname, rc.relname) as target_key,
    rw.rulename as dependency_name
  from pg_rewrite rw join pg_class vc on vc.oid=rw.ev_class
  join pg_namespace vn on vn.oid=vc.relnamespace
  join pg_depend d on d.classid='pg_rewrite'::regclass and d.objid=rw.oid
  join pg_class rc on d.refclassid='pg_class'::regclass and rc.oid=d.refobjid
  join pg_namespace rn on rn.oid=rc.relnamespace
  where vc.relkind in ('v','m') and (vn.nspname='public' or rn.nspname='public')
    and vc.oid <> rc.oid
), policy_dependencies as (
  select distinct format('%I.%I', tn.nspname, tc.relname) as source_key,
    case when d.refclassid='pg_class'::regclass then format('%I.%I', rn.nspname, rc.relname)
      else format('%I.%I(%s)', pn.nspname, pp.proname,
        pg_get_function_identity_arguments(pp.oid)) end as target_key,
    pol.polname as dependency_name
  from pg_policy pol join pg_class tc on tc.oid=pol.polrelid
  join pg_namespace tn on tn.oid=tc.relnamespace
  join pg_depend d on d.classid='pg_policy'::regclass and d.objid=pol.oid
  left join pg_class rc on d.refclassid='pg_class'::regclass and rc.oid=d.refobjid
  left join pg_namespace rn on rn.oid=rc.relnamespace
  left join pg_proc pp on d.refclassid='pg_proc'::regclass and pp.oid=d.refobjid
  left join pg_namespace pn on pn.oid=pp.pronamespace
  where tn.nspname='public' and d.refclassid in ('pg_class'::regclass,'pg_proc'::regclass)
    and d.refobjid <> tc.oid
), signature_dependencies as (
  select distinct r.object_key as source_key,
    format('%I.%I', tn.nspname, tc.relname) as target_key,
    'routine-signature-composite'::text as dependency_name
  from routines r join pg_proc p on p.oid=r.oid
  cross join lateral unnest(coalesce(p.proallargtypes, p.proargtypes::oid[])) arg(type_oid)
  join pg_type typ on typ.oid=arg.type_oid
  join pg_class tc on tc.reltype=typ.oid join pg_namespace tn on tn.oid=tc.relnamespace
  where tn.nspname='public'
), default_privileges as (
  select pg_get_userbyid(d.defaclrole) as owner_role, coalesce(n.nspname, '*') schema_name,
    d.defaclobjtype as object_type, coalesce(grantee.rolname, 'PUBLIC') grantee,
    x.privilege_type, x.is_grantable
  from pg_default_acl d left join pg_namespace n on n.oid=d.defaclnamespace
  cross join lateral aclexplode(d.defaclacl) x
  left join pg_roles grantee on grantee.oid=x.grantee
  where n.nspname='public'
), schema_acl as (
  select n.nspname schema_name, coalesce(grantee.rolname, 'PUBLIC') grantee,
    x.privilege_type, x.is_grantable
  from pg_namespace n
  cross join lateral aclexplode(coalesce(n.nspacl, acldefault('n', n.nspowner))) x
  left join pg_roles grantee on grantee.oid=x.grantee where n.nspname='public'
), publications as (
  select p.pubname, format('%I.%I', n.nspname, c.relname) object_key
  from pg_publication p join pg_publication_rel pr on pr.prpubid=p.oid
  join pg_class c on c.oid=pr.prrelid join pg_namespace n on n.oid=c.relnamespace
  where n.nspname='public'
)
select jsonb_build_object(
  'relations',(select coalesce(jsonb_agg(to_jsonb(x)-'oid' order by object_key),'[]') from relations x),
  'routines',(select coalesce(jsonb_agg(to_jsonb(x)-'oid' order by object_key),'[]') from routines x),
  'relationAcl',(select coalesce(jsonb_agg(to_jsonb(x) order by object_key,grantee,privilege_type),'[]') from relation_acl x),
  'routineAcl',(select coalesce(jsonb_agg(to_jsonb(x) order by object_key,grantee,privilege_type),'[]') from routine_acl x),
  'policies',(select coalesce(jsonb_agg(to_jsonb(x) order by object_key,policyname),'[]') from policies x),
  'foreignKeys',(select coalesce(jsonb_agg(to_jsonb(x) order by source_key,target_key,dependency_name),'[]') from foreign_keys x),
  'triggers',(select coalesce(jsonb_agg(to_jsonb(x) order by source_key,target_key,dependency_name),'[]') from triggers x),
  'rewrites',(select coalesce(jsonb_agg(to_jsonb(x) order by source_key,target_key,dependency_name),'[]') from rewrites x),
  'policyDependencies',(select coalesce(jsonb_agg(to_jsonb(x) order by source_key,target_key,dependency_name),'[]') from policy_dependencies x),
  'signatureDependencies',(select coalesce(jsonb_agg(to_jsonb(x) order by source_key,target_key),'[]') from signature_dependencies x),
  'defaultPrivileges',(select coalesce(jsonb_agg(to_jsonb(x) order by owner_role,schema_name,object_type,grantee,privilege_type),'[]') from default_privileges x),
  'schemaAcl',(select coalesce(jsonb_agg(to_jsonb(x) order by grantee,privilege_type),'[]') from schema_acl x),
  'publications',(select coalesce(jsonb_agg(to_jsonb(x) order by pubname,object_key),'[]') from publications x)
)::text;
"""


def canonical(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n"


def git_output(repo: Path, *args: str) -> str:
    result = subprocess.run(
        ["git", "-C", str(repo), *args], check=False, text=True,
        stdout=subprocess.PIPE, stderr=subprocess.PIPE,
    )
    if result.returncode != 0:
        raise ValueError(f"immutable provenance commit is missing or unreachable: {args[-1]}")
    return result.stdout.strip()


def validate_source(
    source: dict[str, Any], *, database_repo: Path = ROOT,
    workspace_repo: Path | None = None, require_expected: bool = True,
) -> None:
    if require_expected and source != SOURCE:
        raise ValueError("inventory source metadata differs from the reviewed immutable inputs")
    if set(source) != set(SOURCE):
        raise ValueError("inventory source metadata fields are incomplete or unreviewed")
    for field in SOURCE_SHA_FIELDS:
        if not isinstance(source.get(field), str) or not re.fullmatch(r"[0-9a-f]{40}", source[field]):
            raise ValueError(f"inventory source {field} must be an immutable full SHA")
    if not re.fullmatch(r"[0-9a-f]{64}", str(source.get("previousArtifactSha256", ""))):
        raise ValueError("inventory source previousArtifactSha256 must be an exact SHA-256")

    for field in SOURCE_SHA_FIELDS:
        if field != "workspaceBaselineSha":
            git_output(database_repo, "cat-file", "-e", f"{source[field]}^{{commit}}")
    actual_merge_base = git_output(
        database_repo, "merge-base", source["databaseInventorySha"], source["databaseBaseSha"],
    )
    if actual_merge_base != source["databaseMergeBaseSha"]:
        raise ValueError("databaseMergeBaseSha is not the replayed merge-base of the immutable inventory/base SHAs")
    try:
        git_output(
            database_repo, "merge-base", "--is-ancestor",
            source["databaseInventorySha"], source["databaseSchemaSha"],
        )
    except ValueError:
        raise ValueError(
            "databaseSchemaSha must descend from the immutable inventory lineage"
        ) from None

    if workspace_repo is not None:
        git_output(workspace_repo, "cat-file", "-e", f"{source['workspaceBaselineSha']}^{{commit}}")
        gitlink = git_output(
            workspace_repo, "ls-tree", source["workspaceBaselineSha"], "database-engine",
        )
        match = re.fullmatch(r"160000 commit ([0-9a-f]{40})\tdatabase-engine", gitlink)
        if not match or match.group(1) != source["workspacePinnedDatabaseSha"]:
            raise ValueError("workspace baseline does not pin workspacePinnedDatabaseSha")


def verify_committed_artifacts(out_path: Path = OUT, sha_path: Path = SHA) -> dict[str, Any]:
    if not out_path.exists() or not sha_path.exists():
        raise ValueError("committed inventory JSON/hash artifact is missing")
    raw = out_path.read_bytes()
    try:
        contract = json.loads(raw)
    except json.JSONDecodeError:
        raise ValueError("committed inventory JSON is invalid") from None
    if raw != canonical(contract).encode("utf-8"):
        raise ValueError("committed inventory JSON is not canonical byte-for-byte")
    recorded = sha_path.read_text(encoding="utf-8")
    actual = hashlib.sha256(raw).hexdigest() + "\n"
    if recorded != actual:
        raise ValueError("committed inventory SHA-256 does not match the JSON bytes")
    validate_source(contract.get("source", {}))
    return contract


def database_url() -> str:
    if value := os.environ.get("DATABASE_URL"):
        return value
    result = subprocess.run(
        ["supabase", "status", "--output", "json"], cwd=ROOT, check=True,
        text=True, stdout=subprocess.PIPE,
    )
    return json.loads(result.stdout)["DB_URL"]


def load_catalog() -> dict[str, Any]:
    result = subprocess.run(
        ["psql", database_url(), "-XAt", "-v", "ON_ERROR_STOP=1", "-c", CATALOG_SQL],
        cwd=ROOT, check=True, text=True, stdout=subprocess.PIPE,
    )
    return json.loads(result.stdout)


def load_ledger() -> dict[str, dict[str, str]]:
    with LEDGER.open(encoding="utf-8", newline="") as stream:
        rows = list(csv.DictReader(stream, delimiter="\t"))
    by_key = {row["object_key"]: row for row in rows}
    if len(rows) != len(by_key):
        raise ValueError("baseline ledger contains duplicate object_key values")
    return by_key


def migration_batch(row: dict[str, str], target: str) -> str:
    name = row["object_name"]
    if target == "public":
        return "00-retain-public-core"
    if row["object_type"] == "view":
        return f"20-{target}-views"
    prefixes = (
        ("identity_center_", "30-identity"), ("worker_", "31-worker-control-plane"),
        ("lcia_", "32-lcia"), ("lca_", "33-lca"),
        (("review", "comment", "role", "team", "user", "notification"), "34-collaboration"),
        (("dataset_", "cmd_dataset", "qry_dataset"), "35-dataset-workflow"),
    )
    for prefix, batch in prefixes:
        if isinstance(prefix, tuple) and name.startswith(prefix) or isinstance(prefix, str) and name.startswith(prefix):
            return batch
    return f"39-{target}-remaining"


def target_for(row: dict[str, str]) -> str:
    target = TARGET_NORMALIZATION.get(row["candidate_target"], row["candidate_target"])
    if row["object_type"] == "table" and row["object_name"] in CORE_TABLES:
        target = "public"
    if target not in VALID_TARGETS:
        raise ValueError(f"{row['object_key']}: invalid target {target!r}")
    return target


def body_dependencies(catalog: dict[str, Any]) -> list[dict[str, str]]:
    objects = catalog["relations"] + catalog["routines"]
    names: dict[str, list[str]] = defaultdict(list)
    for item in objects:
        names[item["object_name"]].append(item["object_key"])
    edges: set[tuple[str, str, str, str]] = set()
    for routine in catalog["routines"]:
        definition = routine.pop("definition")
        lower = definition.lower()
        dynamic = bool(re.search(r"\bexecute\b|\bformat\s*\(|\bto_reg(class|procedure)\s*\(", lower))
        routine["dynamicSql"] = dynamic
        routine["definitionSha256"] = hashlib.sha256(definition.encode()).hexdigest()
        for name, target_keys in names.items():
            patterns = (
                rf"(?<![a-z0-9_])public\s*\.\s*{re.escape(name.lower())}(?![a-z0-9_])",
                rf"['\"]public\.{re.escape(name.lower())}['\"]\s*::\s*reg(class|procedure)",
                rf"\bto_reg(class|procedure)\s*\(\s*['\"]public\.{re.escape(name.lower())}",
            )
            evidence = next((p for p in patterns if re.search(p, lower)), None)
            if evidence:
                edge_type = "routine-body-dynamic" if dynamic else "routine-body-static"
                for target in target_keys:
                    if target != routine["object_key"]:
                        edges.add((routine["object_key"], target, edge_type, evidence))
    return [
        {"source_key": s, "target_key": t, "dependency_name": k, "evidence": e}
        for s, t, k, e in sorted(edges)
    ]


def load_consumers() -> dict[str, Any]:
    data = json.loads(CONSUMERS.read_text(encoding="utf-8"))
    for repo in data["repositories"]:
        if not re.fullmatch(r"[0-9a-f]{40}", repo["commitSha"]):
            raise ValueError(f"consumer {repo['repo']} lacks an exact commit SHA")
    return data


def dependency_plan(
    keys: set[str], edges: list[dict[str, str]], objects: list[dict[str, Any]],
) -> dict[str, Any]:
    """Return SCC-aware dependency-first Expand and reverse Contract orders."""
    graph: dict[str, set[str]] = {key: set() for key in keys}
    for edge in edges:
        if edge["source_key"] in keys and edge["target_key"] in keys:
            graph[edge["source_key"]].add(edge["target_key"])
    index = 0
    indexes: dict[str, int] = {}
    low: dict[str, int] = {}
    stack: list[str] = []
    on_stack: set[str] = set()
    components: list[list[str]] = []

    def visit(node: str) -> None:
        nonlocal index
        indexes[node] = low[node] = index
        index += 1
        stack.append(node)
        on_stack.add(node)
        for target in sorted(graph[node]):
            if target not in indexes:
                visit(target)
                low[node] = min(low[node], low[target])
            elif target in on_stack:
                low[node] = min(low[node], indexes[target])
        if low[node] == indexes[node]:
            component = []
            while True:
                member = stack.pop()
                on_stack.remove(member)
                component.append(member)
                if member == node:
                    break
            components.append(sorted(component))

    for key in sorted(keys):
        if key not in indexes:
            visit(key)
    component_of = {member: idx for idx, component in enumerate(components) for member in component}
    component_dependencies: dict[int, set[int]] = {idx: set() for idx in range(len(components))}
    for source, targets in graph.items():
        for target in targets:
            if component_of[source] != component_of[target]:
                component_dependencies[component_of[source]].add(component_of[target])
    pending = set(component_dependencies)
    ordered: list[int] = []
    while pending:
        ready = sorted(idx for idx in pending if not (component_dependencies[idx] & pending))
        if not ready:
            raise ValueError("SCC condensation unexpectedly contains a cycle")
        ordered.extend(ready)
        pending.difference_update(ready)
    object_by_key = {item["objectKey"]: item for item in objects}
    groups = [
        {
            "group": position + 1,
            "objects": components[idx],
            "migrationBatches": sorted({object_by_key[key]["migrationBatch"] for key in components[idx]}),
            "cyclic": len(components[idx]) > 1 or any(key in graph[key] for key in components[idx]),
        }
        for position, idx in enumerate(ordered)
    ]
    contract_groups = [
        {**group, "group": position + 1}
        for position, group in enumerate(reversed(groups))
    ]
    return {"expandDependencyGroups": groups, "contractDependencyGroups": contract_groups}


def scan_consumers(workspace_root: Path) -> None:
    """Refresh exact-SHA static evidence without checking out consumer commits."""
    data = load_consumers()
    ledger = load_ledger()
    names: dict[str, list[str]] = defaultdict(list)
    for key, row in ledger.items():
        names[row["object_name"]].append(key)
    grep_pattern = "(" + "|".join(sorted(map(re.escape, names), key=len, reverse=True)) + ")"
    evidence: dict[str, list[dict[str, str]]] = defaultdict(list)
    needle = re.compile(
        r"(?<![A-Za-z0-9_])(" + "|".join(sorted(map(re.escape, names), key=len, reverse=True)) + r")(?![A-Za-z0-9_])"
    )
    for repo in data["repositories"]:
        repo_path = workspace_root / repo["path"]
        sha = repo["commitSha"]
        subprocess.run(["git", "-C", str(repo_path), "cat-file", "-e", f"{sha}^{{commit}}"], check=True)
        result = subprocess.run(
            ["git", "-C", str(repo_path), "grep", "-n", "-I", "-E", "-e", grep_pattern,
             sha, "--", ":!docker/volumes/db/init/data.sql"],
            check=False, text=True, stdout=subprocess.PIPE,
        )
        if result.returncode not in (0, 1):
            raise subprocess.CalledProcessError(result.returncode, result.args)
        for line in result.stdout.splitlines():
            parsed = re.match(r"^[^:]+:(.*?):([0-9]+):(.*)$", line)
            if not parsed:
                continue
            path, line_no, source = parsed.groups()
            for match in needle.finditer(source):
                name = match.group(1)
                strong = bool(re.search(
                    rf"public\s*\.\s*{re.escape(name)}|(?:from|rpc)\s*\(\s*['\"]{re.escape(name)}['\"]|"
                    rf"to_regclass\s*\(\s*['\"]public\.{re.escape(name)}|\b(?:from|join|update|into|table)\s+(?:public\.)?{re.escape(name)}\b",
                    source, re.IGNORECASE,
                ))
                if not strong:
                    continue
                for key in names[name]:
                    evidence[key].append({
                        "repo": repo["repo"], "commitSha": sha, "path": path,
                        "line": line_no, "evidenceSha256": hashlib.sha256(source.strip().encode()).hexdigest(),
                        "matchResolution": "exact-object-name" if len(names[name]) == 1 else "object-name-all-overloads",
                        "overloadCount": len(names[name]),
                    })
    data["objects"] = {
        key: sorted({canonical(item).strip(): item for item in items}.values(), key=lambda x: (x["repo"], x["path"], int(x["line"])))
        for key, items in sorted(evidence.items())
    }
    CONSUMERS.write_text(canonical(data), encoding="utf-8")
    print(json.dumps({"objectsWithStaticEvidence": len(data["objects"]), "repositoryCount": len(data["repositories"])}, sort_keys=True))


def merge_contract(catalog: dict[str, Any]) -> dict[str, Any]:
    ledger = load_ledger()
    consumers = load_consumers()
    live = {item["object_key"]: item for item in catalog["relations"] + catalog["routines"]}
    missing_live = sorted(set(ledger) - set(live))
    missing_mapping = sorted(set(live) - set(ledger))
    all_dependencies: list[dict[str, str]] = []
    for section, kind in (
        ("foreignKeys", "foreign-key"), ("triggers", "trigger-function"),
        ("rewrites", "view-rewrite"), ("policyDependencies", "policy"),
        ("signatureDependencies", "routine-signature-composite"),
    ):
        for edge in catalog[section]:
            all_dependencies.append({**edge, "kind": kind})
    for edge in body_dependencies(catalog):
        all_dependencies.append({
            **{key: value for key, value in edge.items() if key != "dependency_name"},
            "kind": edge["dependency_name"],
        })
    all_dependencies.sort(key=lambda x: (x["source_key"], x["target_key"], x["kind"], x.get("dependency_name", "")))
    outgoing: dict[str, list[dict[str, str]]] = defaultdict(list)
    incoming: dict[str, list[dict[str, str]]] = defaultdict(list)
    for edge in all_dependencies:
        outgoing[edge["source_key"]].append(edge)
        incoming[edge["target_key"]].append(edge)
    evidence_by_object = consumers.get("objects", {})
    dependency_graph: dict[str, list[tuple[str, str]]] = defaultdict(list)
    for edge in all_dependencies:
        if edge["kind"] != "routine-body-dynamic":
            dependency_graph[edge["source_key"]].append((edge["target_key"], edge["kind"]))
    transitive_evidence: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for root_key, root_evidence in evidence_by_object.items():
        exact_roots = [item for item in root_evidence if item["matchResolution"] == "exact-object-name"]
        if not exact_roots:
            continue
        queue: list[tuple[str, list[str], list[str]]] = [(root_key, [root_key], [])]
        visited = {root_key}
        while queue:
            source, path, kinds = queue.pop(0)
            for target, kind in dependency_graph.get(source, []):
                if target in visited or target not in live:
                    continue
                visited.add(target)
                target_path = path + [target]
                target_kinds = kinds + [kind]
                transitive_evidence[target].append({
                    "rootObjectKey": root_key,
                    "consumerRepositories": sorted({item["repo"] for item in exact_roots}),
                    "consumerCommitShas": sorted({item["commitSha"] for item in exact_roots}),
                    "dependencyPath": target_path,
                    "dependencyKinds": target_kinds,
                })
                queue.append((target, target_path, target_kinds))
    objects: list[dict[str, Any]] = []
    residue: dict[str, list[Any]] = {
        "missingFromLiveCatalog": missing_live,
        "unmappedLiveObjects": missing_mapping,
        "objectsWithoutConsumerClosure": [],
        "dynamicSqlReviewRequired": [],
        "retirementBlocked": [],
    }
    for key in sorted(set(live) & set(ledger)):
        row, actual = ledger[key], live[key]
        target = target_for(row)
        object_evidence = evidence_by_object.get(key, [])
        exact_evidence = object_evidence and all(item["matchResolution"] == "exact-object-name" for item in object_evidence)
        transitive = sorted(
            transitive_evidence.get(key, []),
            key=lambda item: (item["rootObjectKey"], len(item["dependencyPath"]), item["dependencyPath"]),
        )
        closure = (
            "exact-static-evidence" if exact_evidence else
            "transitive-static-evidence" if transitive else
            "owner-runtime-confirmation-required"
        )
        blockers = []
        if closure == "owner-runtime-confirmation-required" and target != "public":
            blockers.append("Exact-signature static evidence, runtime telemetry, and owner sign-off are required before Contract.")
            residue["objectsWithoutConsumerClosure"].append(key)
        if actual.get("dynamicSql"):
            residue["dynamicSqlReviewRequired"].append(key)
            blockers.append("Dynamic SQL/regclass body requires an exact runtime and owner review before Contract.")
        if target == "retire" and closure != "static-zero-owner-confirmed":
            residue["retirementBlocked"].append(key)
            blockers.append("Retirement is forbidden until static, runtime, and owner evidence all prove zero consumers.")
        intended_consumers = {
            "public": ["anon", "authenticated", "service_role"],
            "api": ["anon-or-authenticated-explicit-grant", "service_role-as-required"],
            "private": ["service_role-or-dedicated-direct-db-role"],
            "util": ["operator-role"], "archive": ["operator-role"], "retire": [],
        }[target]
        objects.append({
            "objectKey": key, "objectType": row["object_type"], "currentSchema": "public",
            "objectName": row["object_name"], "ownerRole": actual["owner_role"],
            "targetSchema": target, "decision": "retain" if target == "public" else "move",
            "decisionBasis": row["candidate_basis"], "ownerRepo": "tiangong-lca/database-engine",
            "sourceOfTruth": "supabase/migrations/**",
            "migrationBatch": migration_batch(row, target),
            "testGate": "catalog-acl-rls-dependency-and-consumer-residue",
            "intendedConsumers": intended_consumers,
            "roleContract": "explicit-object-grants-plus-schema-usage-plus-rls-when-relational",
            "consumerClosure": closure, "consumerEvidence": object_evidence,
            "transitiveConsumerEvidence": transitive, "blockers": blockers,
            "catalog": actual, "dependencies": outgoing[key], "dependents": incoming[key],
        })
    counts = defaultdict(int)
    for obj in objects:
        counts[obj["objectType"]] += 1
    return {
        "schemaVersion": "database.public-object-inventory-closure.v1",
        "source": dict(SOURCE),
        "counts": dict(sorted(counts.items())), "objects": objects,
        "dependencies": all_dependencies,
        "migrationPlan": dependency_plan(set(live) & set(ledger), all_dependencies, objects),
        "security": {
            "relationAcl": catalog["relationAcl"], "routineAcl": catalog["routineAcl"],
            "policies": catalog["policies"], "defaultPrivileges": catalog["defaultPrivileges"],
            "schemaAcl": catalog["schemaAcl"], "publications": catalog["publications"],
        },
        "consumerEvidence": {"repositories": consumers["repositories"]},
        "residue": residue,
        "contractReady": not any(residue.values()),
    }


def validate(contract: dict[str, Any]) -> None:
    errors = []
    if contract.get("schemaVersion") != "database.public-object-inventory-closure.v1":
        errors.append("unexpected inventory schemaVersion")
    try:
        validate_source(contract.get("source", {}))
    except ValueError as error:
        errors.append(str(error))
    keys = [item["objectKey"] for item in contract["objects"]]
    if len(keys) != len(set(keys)):
        errors.append("duplicate object keys")
    for item in contract["objects"]:
        for field in ("targetSchema", "ownerRepo", "sourceOfTruth", "migrationBatch", "testGate", "consumerClosure"):
            if not item.get(field):
                errors.append(f"{item['objectKey']}: missing {field}")
        if item["targetSchema"] not in VALID_TARGETS:
            errors.append(f"{item['objectKey']}: invalid target")
        if item["targetSchema"] == "retire" and not item["blockers"]:
            errors.append(f"{item['objectKey']}: retirement lacks explicit evidence/blocker")
    if contract["residue"]["missingFromLiveCatalog"] or contract["residue"]["unmappedLiveObjects"]:
        errors.append("live catalog and mapping ledger differ")
    # Workspace #533 froze 391 objects. Database PR #337 then added two
    # service-only Expand adapters; Issue #338 deliberately closes over that
    # exact merged dev head instead of silently retaining the stale count.
    expected = {"table": 56, "view": 5, "function": 332}
    if contract["counts"] != expected:
        errors.append(f"expected baseline counts {expected}, got {contract['counts']}")
    if len(contract["objects"]) != 393:
        errors.append(f"expected 393 objects, got {len(contract['objects'])}")
    # Issue #354 replaces one public-to-public canonical view edge with five
    # explicit public compatibility-wrapper edges to non-public targets.
    if len(contract["dependencies"]) != 1123:
        errors.append(f"expected 1123 dependency edges, got {len(contract['dependencies'])}")
    if len(contract["residue"]["objectsWithoutConsumerClosure"]) != 206:
        errors.append("expected 206 owner/consumer residue entries")
    if len(contract["residue"]["dynamicSqlReviewRequired"]) != 35:
        errors.append("expected 35 dynamic SQL review entries")
    if contract.get("contractReady") is not False:
        errors.append("pre-Contract inventory must remain contractReady=false")
    if errors:
        raise ValueError("inventory closure validation failed:\n" + "\n".join(errors))


def write_or_check(write: bool) -> str:
    if not write:
        try:
            verify_committed_artifacts()
        except ValueError as error:
            raise SystemExit(f"public inventory committed artifact invalid: {error}") from None
    contract = merge_contract(load_catalog())
    validate(contract)
    payload = canonical(contract)
    digest = hashlib.sha256(payload.encode()).hexdigest() + "\n"
    if write:
        OUT.write_text(payload, encoding="utf-8")
        SHA.write_text(digest, encoding="utf-8")
    elif not OUT.exists() or OUT.read_text(encoding="utf-8") != payload or SHA.read_text(encoding="utf-8") != digest:
        raise SystemExit("public inventory contract drift detected; review and run --write")
    print(json.dumps({
        "sha256": digest.strip(), "objectCount": len(contract["objects"]),
        "dependencyCount": len(contract["dependencies"]),
        "residue": {key: len(value) for key, value in contract["residue"].items()},
        "contractReady": contract["contractReady"],
    }, sort_keys=True))
    return digest.strip()


def check_frozen_baseline() -> str:
    """Verify the current exact-head contract without querying a database.

    The immutable Issue #338 input remains at ``*.genesis.*`` for lineage
    consumers.  Issue #405 advances the unqualified artifact names to v2.
    """
    try:
        current = json.loads(OUT.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        current = {}
    if current.get("schemaVersion") == "database.public-object-inventory-closure.v2":
        import public_inventory_exact_head as exact_head

        result = exact_head.verify()
        print(json.dumps(result, sort_keys=True))
        return result["inventorySha256"]
    try:
        contract = verify_committed_artifacts()
        validate(contract)
    except ValueError as error:
        raise SystemExit(f"public inventory committed artifact invalid: {error}") from None
    digest = SHA.read_text(encoding="utf-8").strip()
    print(json.dumps({
        "sha256": digest,
        "objectCount": len(contract["objects"]),
        "dependencyCount": len(contract["dependencies"]),
        "frozenBaseline": True,
        "contractReady": contract["contractReady"],
    }, sort_keys=True))
    return digest


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    action = parser.add_mutually_exclusive_group(required=True)
    action.add_argument("--write", action="store_true", help="write the reviewed deterministic contract")
    action.add_argument("--check", action="store_true", help="verify immutable v1 baseline bytes and provenance")
    action.add_argument(
        "--check-live-baseline", action="store_true",
        help="compare an exact baseline-schema local catalog to the frozen v1 contract",
    )
    action.add_argument("--scan-consumers", metavar="WORKSPACE_ROOT", help="refresh exact-SHA static evidence")
    action.add_argument(
        "--verify-provenance", metavar="WORKSPACE_ROOT",
        help="verify immutable database SHAs and the exact workspace database gitlink",
    )
    args = parser.parse_args()
    if args.scan_consumers:
        scan_consumers(Path(args.scan_consumers).resolve())
        return 0
    if args.verify_provenance:
        contract = verify_committed_artifacts(GENESIS_OUT, GENESIS_SHA)
        validate_source(
            contract["source"], workspace_repo=Path(args.verify_provenance).resolve(),
        )
        print(json.dumps(contract["source"], sort_keys=True))
        return 0
    if args.write:
        raise SystemExit(
            "Issue #405 retired the unqualified v1 writer; use "
            "public_inventory_exact_head.py --refresh --db-url <explicit-loopback-url>"
        )
    if args.check:
        check_frozen_baseline()
    else:
        write_or_check(args.write)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
