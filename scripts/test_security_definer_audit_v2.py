#!/usr/bin/env python3
"""Offline fail-closed tests for the cross-schema privileged routine ledger."""

from __future__ import annotations

import copy
import json
import sys
import tempfile
import time
import unittest
import urllib.parse
from pathlib import Path
from unittest import mock

import jsonschema

sys.path.insert(0, str(Path(__file__).resolve().parent))
import security_definer_audit_v2 as audit


def database_uri(
    authority_host: str, *, user: str = "u", credential: str = "p",
    path: str = "/db", query: str = "", fragment: str = "",
) -> str:
    authority = "@".join((":".join((user, credential)), authority_host))
    return urllib.parse.urlunsplit(("postgresql", authority, path, query, fragment))


def schema_and_name(object_key: str) -> tuple[str, str]:
    qualified = object_key.split("(", 1)[0]
    return tuple(qualified.split(".", 1))  # type: ignore[return-value]


class SecurityDefinerAuditV2Test(unittest.TestCase):
    def setUp(self) -> None:
        real_git_index_mode = audit.git_index_mode
        patcher = mock.patch.object(
            audit, "git_index_mode",
            side_effect=lambda value: (
                "100644" if (
                    Path(value).name.startswith("test-")
                    or value == str(audit.OUT.relative_to(audit.ROOT))
                ) else real_git_index_mode(value)
            ),
        )
        patcher.start()
        self.addCleanup(patcher.stop)
        self.inventory, self.inventory_hash = audit.read_hashed_json(audit.INVENTORY, audit.INVENTORY_SHA)
        self.baseline, self.baseline_hash = audit.read_hashed_json(
            audit.BASELINE_AUDIT, audit.BASELINE_AUDIT_SHA,
        )
        self.lineage, self.lineage_hash = audit.read_hashed_json(audit.LINEAGE, audit.LINEAGE_SHA)
        self.committed, _ = audit.read_hashed_json(audit.OUT, audit.SHA)
        self.fixture, _ = audit.read_hashed_json(
            audit.CONTRACT_DIR / "security_definer_transition_fixture.v1.json",
            audit.CONTRACT_DIR / "security_definer_transition_fixture.v1.sha256",
        )
        audit.validate_transition_fixture(
            self.fixture,
            audit.TRANSITION_BASELINE_LINEAGE_SHA.read_text(encoding="utf-8").strip(),
        )
        self.catalog = self.synthetic_catalog(self.committed)

    def write_test_receipt(self, name: str, value: dict) -> tuple[Path, str, str]:
        path = audit.CONTRACT_DIR / f"test-{name}.json"
        path.write_text(audit.canonical(value), encoding="utf-8")
        return path, str(path.relative_to(audit.ROOT)), audit.sha256_bytes(path.read_bytes())

    @staticmethod
    def synthetic_catalog(committed: dict) -> dict[str, dict]:
        result = {}
        for item in committed["routines"]:
            for endpoint in [item["canonical"], *item["compatibilityAliases"]]:
                matrix = endpoint["roleMatrix"]
                direct = sorted({
                    source for role in matrix for source in role["directGrantSources"]
                })
                named = [
                    {
                        "role": role["role"],
                        "schemaUsage": role["effectiveSchemaUsage"],
                        "execute": role["effectiveExecute"],
                        "postgrestAuthenticatorCanSetRole": role.get(
                            "postgrestAuthenticatorCanSetRole",
                            role["role"] in {"anon", "authenticated", "service_role"},
                        ),
                    }
                    for role in matrix if role["role"] != "PUBLIC"
                ]
                _, name = schema_and_name(endpoint["currentObjectKey"])
                result[endpoint["currentObjectKey"]] = {
                    "objectKey": endpoint["currentObjectKey"],
                    "schema": endpoint["currentSchema"],
                    "name": name,
                    "routineKind": endpoint["routineKind"],
                    "securityDefiner": endpoint["securityDefiner"],
                    "ownerRole": endpoint["ownerRole"],
                    "databaseOwnerRole": endpoint.get("databaseOwnerRole", "postgres"),
                    "language": "plpgsql",
                    "volatility": "v",
                    "strict": False,
                    "parallel": "u",
                    "resultType": "jsonb",
                    "returnTypeName": endpoint.get("postgrestShape", {}).get("returnTypeName", "jsonb"),
                    "returnTypeKind": endpoint.get("postgrestShape", {}).get("returnTypeKind", "b"),
                    "argumentNames": endpoint.get("postgrestShape", {}).get("inputArgumentNames", []),
                    "argumentModes": endpoint.get("postgrestShape", {}).get("argumentModes", []),
                    "inputArgumentNames": endpoint.get("postgrestShape", {}).get("inputArgumentNames", []),
                    "inputArgumentTypes": endpoint.get("postgrestShape", {}).get("inputArgumentTypes", []),
                    "inputArgumentTypeKinds": endpoint.get("postgrestShape", {}).get(
                        "inputArgumentTypeKinds",
                        ["b"] * len(endpoint.get("postgrestShape", {}).get("inputArgumentTypes", [])),
                    ),
                    "inputArgumentRequired": endpoint.get("postgrestShape", {}).get(
                        "inputArgumentRequired",
                        [True] * len(endpoint.get("postgrestShape", {}).get("inputArgumentTypes", [])),
                    ),
                    "outputArgumentCount": endpoint.get("postgrestShape", {}).get("outputArgumentCount", 0),
                    # Synthetic catalogs default to the post-Contract shape. Tests
                    # that exercise residue replace this with the legacy setting.
                    "config": ["search_path=public, pg_temp"],
                    "definition": f"definition:{endpoint['currentObjectKey']}",
                    "body": f"body:{endpoint['currentObjectKey']}",
                    "directExecuteGrants": direct,
                    "effectiveCallerMatrix": [{
                        **caller,
                        "postgrestAuthenticatorCanSetRole": caller.get(
                            "postgrestAuthenticatorCanSetRole", False,
                        ),
                    } for caller in endpoint.get("effectiveCallerMatrix", [
                        {
                            "role": role["role"],
                            "canLogin": role["role"] in {
                                "anon", "authenticated", "service_role", "postgres",
                            },
                            "schemaUsage": role["effectiveSchemaUsage"],
                            "execute": role["effectiveExecute"],
                            "postgrestAuthenticatorCanSetRole": role.get(
                                "postgrestAuthenticatorCanSetRole",
                                role["role"] in {"anon", "authenticated", "service_role"},
                            ),
                        }
                        for role in matrix
                        if role["role"] != "PUBLIC" and role["effectiveCallable"]
                    ])],
                    "publicSchemaUsage": next(
                        role["effectiveSchemaUsage"] for role in matrix if role["role"] == "PUBLIC"
                    ),
                    "namedRoleMatrix": named,
                    "schemaTrust": [
                        {
                            "schema": schema,
                            "ownerRole": "postgres",
                            "ownerTrusted": True,
                            "publicCreate": False,
                            "roleCreate": {
                                "anon": False, "authenticated": False,
                                "service_role": False, "api_internal_executor": False,
                            },
                            "nonOwnerCallableCreateRoles": [],
                        }
                        for schema in ("api", "archive", "extensions", "pg_catalog", "private", "public", "util")
                    ],
                }
        return result

    def transition(self) -> tuple[dict, dict]:
        lineage = copy.deepcopy(self.lineage)
        catalog = copy.deepcopy(self.catalog)
        by_original = {row["originalObjectKey"]: row for row in lineage["lineages"]}
        for move in self.fixture["moves"]:
            mapping = by_original[move["originalObjectKey"]]
            old = catalog.pop(mapping["canonicalObjectKey"])
            canonical = copy.deepcopy(old)
            canonical["objectKey"] = move["canonicalObjectKey"]
            canonical["schema"], canonical["name"] = schema_and_name(move["canonicalObjectKey"])
            canonical["definition"] = f"transition-definition:{move['canonicalObjectKey']}"
            canonical["body"] = f"transition-body:{move['canonicalObjectKey']}"
            mapping["canonicalObjectKey"] = move["canonicalObjectKey"]
            mapping["compatibilityAliases"] = move["compatibilityAliases"]
            catalog[canonical["objectKey"]] = canonical
            for alias_key in move["compatibilityAliases"]:
                alias = copy.deepcopy(old)
                alias["objectKey"] = alias_key
                alias["schema"], alias["name"] = schema_and_name(alias_key)
                alias["securityDefiner"] = False
                alias["definition"] = f"alias-definition:{alias_key}"
                alias["body"] = f"alias-body:{alias_key}"
                catalog[alias_key] = alias
        return lineage, catalog

    def test_current_transition_covers_public_private_and_util_once(self) -> None:
        self.assertEqual(self.committed["summary"]["lineageCount"], 333)
        self.assertEqual(self.committed["summary"]["originalPublicLineageCount"], 241)
        self.assertEqual(self.committed["summary"]["genesisNativeNonPublicLineageCount"], 74)
        self.assertEqual(self.committed["summary"]["transitionNativeLineageCount"], 18)
        self.assertEqual(
            self.committed["summary"]["globalPrivilegedBySchema"],
            {"api": 0, "archive": 0, "private": 64, "public": 233, "util": 36},
        )
        self.assertEqual(self.committed["summary"]["unregisteredPrivilegedEndpointCount"], 0)

    def test_issue323_registration_preserves_birth_definition_before_hardening(self) -> None:
        expected_birth_hashes = {
            "public.qry_review_get_admin_root_queue_items_v2(p_status text, p_page integer, p_page_size integer, p_sort_by text, p_sort_order text)":
                "7267786d450407a0c85d2dac189caf55280bedfc7a1313fa710f12b51e136017",
            "public.qry_review_get_member_root_queue_items_v2(p_status text, p_page integer, p_page_size integer, p_sort_by text, p_sort_order text)":
                "2fb6bc9b281de9c950e5d9ae0247de4dc7af9bc173182e40062171c256b2d463",
            "public.qry_root_review_reference_progress_v2(p_root_review_id uuid)":
                "11f1b3d83755e2ab0c460e7451a8b516482722d0097b667b906cd054f422cb57",
        }
        audit_by_key = {
            row["canonical"]["currentObjectKey"]: row
            for row in self.committed["routines"]
            if row["origin"] == "transition-native"
        }
        registered = [
            row for row in self.lineage["lineages"]
            if row["migrationBatch"] == "issue-323-root-grouped-review-queue"
        ]
        self.assertEqual(len(registered), 3)
        for row in registered:
            object_key = row["originalObjectKey"]
            receipt = json.loads(audit.read_reviewed_contract_bytes(
                row["birthTransition"]["receiptPath"],
            ))
            self.assertEqual(row["originalDefinitionSha256"], expected_birth_hashes[object_key])
            self.assertEqual(receipt["original"]["definitionSha256"], expected_birth_hashes[object_key])
            self.assertNotEqual(
                row["originalDefinitionSha256"],
                audit_by_key[object_key]["canonical"]["definitionSha256"],
            )
            self.assertTrue(audit_by_key[object_key]["canonical"]["safeSearchPath"])

    def test_catalog_input_argument_names_use_one_based_ordinality_not_oid_array_bounds(self) -> None:
        self.assertIn("with ordinality g(type_oid, ordinality)", audit.CATALOG_QUERY)
        self.assertIn("p.proargnames[g.ordinality]", audit.CATALOG_QUERY)
        self.assertNotIn("generate_subscripts(coalesce(p.proallargtypes", audit.CATALOG_QUERY)
        self.assertIn("'inputArgumentRequired'", audit.CATALOG_QUERY)

    def test_database_connection_is_strict_loopback_and_keeps_password_out_of_argv(self) -> None:
        connection = audit.parse_loopback_connection(
            database_uri(
                "127.0.0.1:57322", user="audit_user",
                credential="s3cr%25et", path="/postgres",
            ),
        )
        self.assertEqual(connection.host, "127.0.0.1")
        self.assertEqual(connection.port, 57322)
        self.assertEqual(connection.user, "audit_user")
        self.assertEqual(connection.database, "postgres")
        self.assertNotIn("s3cr", " ".join(connection.command("-qAtX")))
        self.assertEqual(connection.environment()["PGPASSWORD"], "s3cr%et")
        self.assertNotIn("DATABASE_URL", connection.environment())

        rejected = (
            database_uri("example.com:5432"),
            database_uri("localhost"),
            database_uri("localhost:5432", query="host=/tmp"),
            database_uri("localhost:5432", fragment="fragment"),
            database_uri("localhost,127.0.0.1:5432"),
            database_uri("%2Ftmp:5432"),
            database_uri("localhost:5432", path="/db/other"),
            database_uri("localhost:5432", credential="p%"),
        )
        for value in rejected:
            with self.subTest(value=value), self.assertRaises(ValueError):
                audit.parse_loopback_connection(value)

    def test_catalog_subprocess_never_receives_database_secret_in_argv(self) -> None:
        completed = mock.Mock(stdout="[]\n")
        with mock.patch.object(audit.subprocess, "run", return_value=completed) as run:
            self.assertEqual(audit.load_catalog(
                database_uri(
                    "localhost:57322", user="postgres",
                    credential="do-not-leak", path="/postgres",
                ),
            ), {})
        command = run.call_args.args[0]
        environment = run.call_args.kwargs["env"]
        self.assertNotIn("do-not-leak", " ".join(command))
        self.assertNotIn("DATABASE_URL", environment)
        self.assertEqual(environment["PGPASSWORD"], "do-not-leak")

    def test_reviewed_contract_paths_reject_aliases_untracked_and_symlinks(self) -> None:
        for value in (
            "/supabase/tests/contracts/file.json",
            "supabase/tests/contracts/../contracts/file.json",
            "supabase/tests/contracts/./file.json",
            "supabase//tests/contracts/file.json",
            "supabase\\tests\\contracts\\file.json",
            "scripts/file.json",
        ):
            with self.subTest(value=value), self.assertRaises(ValueError):
                audit.canonical_contract_path(value)

        with mock.patch.object(audit, "git_index_mode", side_effect=ValueError("untracked")):
            with self.assertRaisesRegex(ValueError, "untracked"):
                audit.read_reviewed_contract_bytes(
                    str(audit.BASELINE_AUDIT.relative_to(audit.ROOT)),
                )

        target = audit.CONTRACT_DIR / "test-no-follow-target.json"
        link = audit.CONTRACT_DIR / "test-no-follow-link.json"
        target.write_text("{}\n", encoding="utf-8")
        link.symlink_to(target.name)
        try:
            with self.assertRaisesRegex(ValueError, "without following links"):
                audit.read_reviewed_contract_bytes(str(link.relative_to(audit.ROOT)))
        finally:
            link.unlink(missing_ok=True)
            target.unlink(missing_ok=True)

    def test_committed_lineage_and_audit_validate_against_json_schemas(self) -> None:
        lineage_schema = json.loads(
            (audit.CONTRACT_DIR / "privileged_routine_lineage.schema.json").read_text(encoding="utf-8")
        )
        audit_schema = json.loads(
            (audit.CONTRACT_DIR / "security_definer_audit_v2.schema.json").read_text(encoding="utf-8")
        )
        jsonschema.Draft202012Validator(lineage_schema).validate(self.lineage)
        jsonschema.Draft202012Validator(audit_schema).validate(self.committed)

    def test_contract_schemas_reject_invalid_lifecycle_paths_and_missing_evidence(self) -> None:
        lineage_schema = json.loads(
            (audit.CONTRACT_DIR / "privileged_routine_lineage.schema.json").read_text(encoding="utf-8")
        )
        audit_schema = json.loads(
            (audit.CONTRACT_DIR / "security_definer_audit_v2.schema.json").read_text(encoding="utf-8")
        )
        invalid_lineages = []
        active_without_canonical = copy.deepcopy(self.lineage)
        active_without_canonical["lineages"][0]["canonicalObjectKey"] = None
        invalid_lineages.append(active_without_canonical)
        retired_without_receipt = copy.deepcopy(self.lineage)
        retired_without_receipt["lineages"][0].update({
            "lifecycle": "retired", "canonicalObjectKey": None, "retirement": None,
        })
        invalid_lineages.append(retired_without_receipt)
        transition_native_without_birth = copy.deepcopy(self.lineage)
        transition_native_without_birth["lineages"][0]["origin"] = "transition-native"
        invalid_lineages.append(transition_native_without_birth)
        invalid_path = copy.deepcopy(self.lineage)
        invalid_path["source"]["completedTransitions"] = [{
            **invalid_path["source"]["currentTransition"],
            "predecessorAuditPath": "supabase/tests/contracts/../escape.json",
            "producedAuditV2Sha256": "a" * 64,
            "producedAuditV2Path": "supabase/tests/contracts/a.json",
            "receiptPath": "supabase/tests/contracts/r.json",
            "receiptSha256": "b" * 64,
        }]
        invalid_lineages.append(invalid_path)
        for value in invalid_lineages:
            with self.assertRaises(jsonschema.ValidationError):
                jsonschema.Draft202012Validator(lineage_schema).validate(value)

        missing_postgrest_evidence = copy.deepcopy(self.committed)
        del missing_postgrest_evidence["routines"][0]["canonical"]["postgrestShape"]["modelVersion"]
        with self.assertRaises(jsonschema.ValidationError):
            jsonschema.Draft202012Validator(audit_schema).validate(missing_postgrest_evidence)
        extra_role_evidence = copy.deepcopy(self.committed)
        extra_role_evidence["routines"][0]["canonical"]["roleMatrix"][0]["unexpected"] = True
        with self.assertRaises(jsonschema.ValidationError):
            jsonschema.Draft202012Validator(audit_schema).validate(extra_role_evidence)

    def test_original_lineage_key_is_schema_independent_and_stable(self) -> None:
        transitioned, _ = self.transition()
        before = {row["originalObjectKey"]: row["lineageKey"] for row in self.lineage["lineages"]}
        after = {row["originalObjectKey"]: row["lineageKey"] for row in transitioned["lineages"]}
        self.assertEqual(before, after)
        self.assertTrue(all(
            key == (
                audit.transition_lineage_key(original, row["birthTransition"])
                if row["birthTransition"] is not None
                else audit.lineage_key(original)
            )
            for row in self.lineage["lineages"]
            for original, key in [(row["originalObjectKey"], row["lineageKey"])]
        ))

    def test_unrelated_current_inventory_hash_cannot_rekey_frozen_or_registered_lineages(self) -> None:
        before = [row["lineageKey"] for row in self.lineage["lineages"]]
        simulated_current_inventory_hash = "f" * 64
        self.assertNotEqual(simulated_current_inventory_hash, audit.BASELINE_PROVENANCE["inventorySha256"])
        after = [
            audit.transition_lineage_key(row["originalObjectKey"], row["birthTransition"])
            if row["birthTransition"] is not None
            else audit.lineage_key(row["originalObjectKey"])
            for row in self.lineage["lineages"]
        ]
        self.assertEqual(before, after)

    def test_issue356_transition_fixture_preserves_315_privileged_lineages(self) -> None:
        lineage, catalog = self.transition()
        transitioned = audit.build_audit(
            lineage, audit.sha256_text(audit.canonical(lineage)), self.baseline, catalog,
            audit.exposed_schemas(),
        )
        expected = self.fixture["expected"]
        for field, value in expected.items():
            if field == "unsafeSearchPathEndpointCount":
                # The synthetic catalog intentionally omits live plpgsql-check
                # residue details; the two-stack test proves this field live.
                self.assertEqual(value, self.committed["summary"][field], field)
                continue
            if field == "compatibilityEndpointCount":
                # The reviewed current lineage also contains the two #355
                # invoker aliases layered after the immutable #356 fixture.
                self.assertEqual(
                    transitioned["summary"][field], self.committed["summary"][field], field,
                )
                continue
            if field in {"lineageCount", "globalPrivilegedEndpointCount"}:
                self.assertEqual(transitioned["summary"][field], value + 18, field)
                continue
            if field in {"canonicalPrivilegedBySchema", "globalPrivilegedBySchema"}:
                registered = dict(value)
                registered["public"] += 3
                registered["private"] += 15
                self.assertEqual(transitioned["summary"][field], registered, field)
                continue
            self.assertEqual(transitioned["summary"][field], value, field)
        self.assertEqual(len(self.fixture["moves"]), 12)
        self.assertEqual(sum(len(row["compatibilityAliases"]) for row in self.fixture["moves"]), 23)

    def test_issue355_invoker_adapters_are_aliases_while_canonical_stays_public(self) -> None:
        lineage = copy.deepcopy(self.lineage)
        catalog = copy.deepcopy(self.catalog)
        names = {"review_append_scope_snapshot_v1", "review_validate_scope_history_v1"}
        selected = []
        for mapping in lineage["lineages"]:
            _, name = schema_and_name(mapping["originalObjectKey"])
            if name not in names:
                continue
            alias_key = "private." + mapping["originalObjectKey"].split(".", 1)[1]
            alias = copy.deepcopy(catalog[mapping["canonicalObjectKey"]])
            alias["objectKey"] = alias_key
            alias["schema"] = "private"
            alias["securityDefiner"] = False
            alias["definition"] = f"issue355-alias:{alias_key}"
            alias["body"] = f"issue355-alias-body:{alias_key}"
            catalog[alias_key] = alias
            mapping["compatibilityAliases"] = [alias_key]
            selected.append(mapping)
        self.assertEqual(len(selected), 2)
        observed = audit.build_audit(
            lineage, audit.sha256_text(audit.canonical(lineage)), self.baseline, catalog,
            audit.exposed_schemas(),
        )
        self.assertEqual(observed["summary"]["globalPrivilegedEndpointCount"], 333)
        self.assertEqual(
            observed["summary"]["compatibilityEndpointCount"],
            self.committed["summary"]["compatibilityEndpointCount"],
        )
        self.assertEqual(observed["summary"]["privilegedCompatibilityEndpointCount"], 0)
        self.assertTrue(all(row["canonicalObjectKey"].startswith("public.") for row in selected))

    def test_missing_canonical_fails_closed(self) -> None:
        catalog = copy.deepcopy(self.catalog)
        catalog.pop(self.lineage["lineages"][0]["canonicalObjectKey"])
        with self.assertRaisesRegex(ValueError, "canonical privileged endpoint missing"):
            audit.build_audit(self.lineage, self.lineage_hash, self.baseline, catalog, audit.exposed_schemas())

    def test_duplicate_canonical_and_alias_claims_fail_closed(self) -> None:
        duplicate = copy.deepcopy(self.lineage)
        duplicate["lineages"][1]["canonicalObjectKey"] = duplicate["lineages"][0]["canonicalObjectKey"]
        with self.assertRaisesRegex(ValueError, "duplicate canonical endpoints"):
            audit.validate_lineage(duplicate, self.inventory, self.inventory_hash, self.baseline_hash)
        overlap = copy.deepcopy(self.lineage)
        overlap["lineages"][0]["compatibilityAliases"] = [overlap["lineages"][1]["canonicalObjectKey"]]
        with self.assertRaisesRegex(ValueError, "sets overlap"):
            audit.validate_lineage(overlap, self.inventory, self.inventory_hash, self.baseline_hash)

    def test_invoker_wrapper_cannot_substitute_for_canonical(self) -> None:
        lineage, catalog = self.transition()
        moved = next(row for row in lineage["lineages"] if row["compatibilityAliases"])
        moved["canonicalObjectKey"] = moved["compatibilityAliases"][0]
        moved["compatibilityAliases"] = moved["compatibilityAliases"][1:]
        with self.assertRaisesRegex(ValueError, "not SECURITY DEFINER"):
            audit.build_audit(
                lineage, audit.sha256_text(audit.canonical(lineage)), self.baseline, catalog,
                audit.exposed_schemas(),
            )

    def test_unregistered_privileged_endpoint_fails_closed(self) -> None:
        catalog = copy.deepcopy(self.catalog)
        extra = copy.deepcopy(next(iter(catalog.values())))
        extra["objectKey"] = "private.unregistered_privileged()"
        extra["schema"] = "private"
        extra["name"] = "unregistered_privileged"
        catalog[extra["objectKey"]] = extra
        with self.assertRaisesRegex(ValueError, "unregistered governed"):
            audit.build_audit(self.lineage, self.lineage_hash, self.baseline, catalog, audit.exposed_schemas())

    def test_reviewed_transition_native_registration_is_extensible(self) -> None:
        lineage = copy.deepcopy(self.lineage)
        catalog = copy.deepcopy(self.catalog)
        original_key = "private.transition_native_example(p_value text)"
        original = copy.deepcopy(next(iter(catalog.values())))
        original.update({
            "objectKey": original_key, "schema": "private", "name": "transition_native_example",
            "routineKind": "function", "definition": "transition native definition",
            "body": "transition native body", "securityDefiner": True,
        })
        transition = dict(lineage["source"]["currentTransition"])
        row = {
            "lineageKey": audit.transition_lineage_key(original_key, transition),
            "origin": "transition-native", "originalObjectKey": original_key,
            "originalSchema": "private", "originalDefinitionSha256": audit.sha256_text(original["definition"]),
            "originalRoutineKind": "function", "targetSchema": "private",
            "migrationBatch": transition["batch"], "lifecycle": "active",
            "birthTransition": None, "retirement": None, "canonicalObjectKey": original_key,
            "compatibilityAliases": [],
        }
        receipt = {
            "schemaVersion": "database.privileged-routine-registration-receipt.v1",
            "lineageKey": row["lineageKey"], "transition": transition,
            "original": {"objectKey": original_key, "schema": "private",
                         "definitionSha256": row["originalDefinitionSha256"], "routineKind": "function"},
            "targetSchema": "private", "migrationBatch": transition["batch"],
        }
        path, receipt_path, receipt_sha = self.write_test_receipt("registration", receipt)
        row["birthTransition"] = {**transition, "receiptPath": receipt_path, "receiptSha256": receipt_sha}
        lineage["lineages"].append(row)
        catalog[original_key] = original
        try:
            audit.validate_lineage(lineage, self.inventory, self.inventory_hash, self.baseline_hash)
            observed = audit.build_audit(
                lineage, audit.sha256_text(audit.canonical(lineage)), self.baseline, catalog,
                audit.exposed_schemas(),
            )
            self.assertEqual(observed["summary"]["lineageCount"], 334)
            self.assertEqual(observed["summary"]["transitionNativeLineageCount"], 19)
            without_receipt = copy.deepcopy(lineage)
            without_receipt["lineages"][-1]["birthTransition"]["receiptPath"] = "missing.json"
            with self.assertRaisesRegex(ValueError, "registration receipt path"):
                audit.validate_lineage(without_receipt, self.inventory, self.inventory_hash, self.baseline_hash)
        finally:
            path.unlink(missing_ok=True)

    def test_retirement_is_derived_from_predecessor_and_rejects_alias_residue(self) -> None:
        predecessor_lineage = copy.deepcopy(self.lineage)
        predecessor_catalog = copy.deepcopy(self.catalog)
        mapping = next(row for row in predecessor_lineage["lineages"]
                       if row["canonicalObjectKey"].startswith("public."))
        canonical_key = mapping["canonicalObjectKey"]
        alias_key = "private.test_retirement_alias()"
        alias = copy.deepcopy(predecessor_catalog[canonical_key])
        alias.update({"objectKey": alias_key, "schema": "private", "name": "test_retirement_alias",
                      "securityDefiner": False, "definition": "retirement alias", "body": "retirement alias"})
        predecessor_catalog[alias_key] = alias
        mapping["compatibilityAliases"] = [alias_key]
        predecessor = audit.build_audit(
            predecessor_lineage, audit.sha256_text(audit.canonical(predecessor_lineage)),
            self.baseline, predecessor_catalog, audit.exposed_schemas(),
        )
        predecessor_path, predecessor_rel, predecessor_sha = self.write_test_receipt(
            "retirement-predecessor-audit", predecessor,
        )
        completed_transition = dict(predecessor_lineage["source"]["currentTransition"])
        prior_completed = predecessor_lineage["source"]["completedTransitions"]
        completed = {**completed_transition, "producedAuditV2Sha256": predecessor_sha,
                     "producedAuditV2Path": predecessor_rel,
                     "predecessorAuditPath": prior_completed[-1]["producedAuditV2Path"]}
        completed_receipt = {
            "schemaVersion": "database.security-definer-transition-receipt.v1",
            "transition": completed_transition, "producedAuditV2Sha256": predecessor_sha,
            "producedAuditV2Path": predecessor_rel,
            "predecessorAuditPath": completed["predecessorAuditPath"],
        }
        completed_path, completed_rel, completed_sha = self.write_test_receipt(
            "retirement-completed-transition", completed_receipt,
        )
        completed.update({"receiptPath": completed_rel, "receiptSha256": completed_sha})
        current = {"sequence": completed_transition["sequence"] + 1,
                   "batch": "issue-358-retirement", "databaseSchemaSha": "a" * 40,
                   "predecessorArtifactSha256": predecessor_sha}
        lineage = copy.deepcopy(predecessor_lineage)
        lineage["source"]["completedTransitions"] = [*prior_completed, completed]
        lineage["source"]["currentTransition"] = current
        retired = next(row for row in lineage["lineages"] if row["lineageKey"] == mapping["lineageKey"])
        endpoints = [canonical_key, alias_key]
        retirement_receipt = {
            "schemaVersion": "database.privileged-routine-retirement-receipt.v1",
            "lineageKey": retired["lineageKey"], "transition": current,
            "consumerZeroEvidenceSha256": "b" * 64, "ownerEvidenceSha256": "c" * 64,
            "retiredEndpointKeys": endpoints, "predecessorAuditPath": predecessor_rel,
        }
        retire_path, retire_rel, retire_sha = self.write_test_receipt("retirement", retirement_receipt)
        retired.update({"lifecycle": "retired", "canonicalObjectKey": None,
                        "compatibilityAliases": [], "retirement": {
                            **current, "receiptPath": retire_rel, "receiptSha256": retire_sha,
                            "consumerZeroEvidenceSha256": "b" * 64, "ownerEvidenceSha256": "c" * 64,
                            "retiredEndpointKeys": endpoints, "predecessorAuditPath": predecessor_rel,
                        }})
        catalog = copy.deepcopy(predecessor_catalog)
        catalog.pop(canonical_key)
        catalog.pop(alias_key)
        try:
            with mock.patch.object(
                    audit, "EXPECTED_COMPLETED_TRANSITIONS",
                    tuple([*prior_completed, completed])), \
                    mock.patch.object(audit, "EXPECTED_CURRENT_TRANSITION", current):
                audit.validate_lineage(lineage, self.inventory, self.inventory_hash, self.baseline_hash)
                observed = audit.build_audit(
                    lineage, audit.sha256_text(audit.canonical(lineage)), self.baseline, catalog,
                    audit.exposed_schemas(),
                )
                self.assertEqual(observed["summary"]["retiredLineageCount"], 1)
                residue = copy.deepcopy(catalog)
                residue[alias_key] = alias
                with self.assertRaisesRegex(ValueError, "retired lineage endpoints remain"):
                    audit.build_audit(lineage, "x", self.baseline, residue, audit.exposed_schemas())
                self_reported = copy.deepcopy(lineage)
                self_reported["lineages"][lineage["lineages"].index(retired)]["retirement"]["retiredEndpointKeys"] = [canonical_key]
                with self.assertRaisesRegex(ValueError, "endpoint closure differs|receipt retiredEndpointKeys"):
                    audit.validate_lineage(self_reported, self.inventory, self.inventory_hash, self.baseline_hash)
                for predecessor_value in (None, str(audit.BASELINE_AUDIT.relative_to(audit.ROOT))):
                    mismatched = copy.deepcopy(lineage)
                    retirement = mismatched["lineages"][lineage["lineages"].index(retired)]["retirement"]
                    if predecessor_value is None:
                        retirement.pop("predecessorAuditPath")
                    else:
                        retirement["predecessorAuditPath"] = predecessor_value
                    with self.subTest(predecessor_value=predecessor_value), \
                            self.assertRaisesRegex(ValueError, "predecessor audit path differs"):
                        audit.validate_lineage(
                            mismatched, self.inventory, self.inventory_hash, self.baseline_hash,
                        )
        finally:
            for path in (predecessor_path, completed_path, retire_path):
                path.unlink(missing_ok=True)

    def test_definition_role_and_schema_usage_drift_fail_closed(self) -> None:
        key = self.lineage["lineages"][0]["canonicalObjectKey"]
        pristine = audit.build_audit(
            self.lineage, self.lineage_hash, self.baseline, self.catalog, audit.exposed_schemas(),
        )
        audit.validate_audit(
            pristine, self.lineage, self.lineage_hash, self.baseline, self.catalog,
            audit.exposed_schemas(),
        )
        for field, value in (
            ("definition", "changed definition"),
            ("publicSchemaUsage", not self.catalog[key]["publicSchemaUsage"]),
        ):
            catalog = copy.deepcopy(self.catalog)
            catalog[key][field] = value
            with self.subTest(field=field), self.assertRaisesRegex(ValueError, "exact lineage/catalog"):
                audit.validate_audit(
                    pristine, self.lineage, self.lineage_hash, self.baseline, catalog,
                    audit.exposed_schemas(),
                )
        catalog = copy.deepcopy(self.catalog)
        catalog[key]["namedRoleMatrix"][0]["execute"] = not catalog[key]["namedRoleMatrix"][0]["execute"]
        with self.assertRaisesRegex(ValueError, "exact lineage/catalog"):
            audit.validate_audit(
                pristine, self.lineage, self.lineage_hash, self.baseline, catalog,
                audit.exposed_schemas(),
            )

    def test_custom_and_inherited_effective_callers_are_audited(self) -> None:
        key = next(row["canonicalObjectKey"] for row in self.lineage["lineages"]
                   if row["canonicalObjectKey"].startswith("public."))
        before = audit.observed_endpoint(
            self.catalog[key], audit.exposed_schemas(), "canonical", self.catalog,
        )
        changed = copy.deepcopy(self.catalog[key])
        changed["directExecuteGrants"] = sorted(
            [*changed["directExecuteGrants"], "custom_executor_group"]
        )
        changed["effectiveCallerMatrix"].extend([
            {
                "role": "custom_executor_group", "canLogin": False,
                "schemaUsage": True, "execute": True,
                "postgrestAuthenticatorCanSetRole": True,
            },
            {
                "role": "custom_executor_login", "canLogin": True,
                "schemaUsage": True, "execute": True,
                "postgrestAuthenticatorCanSetRole": False,
            },
        ])
        changed["effectiveCallerMatrix"].sort(key=lambda row: row["role"])
        changed_catalog = {**self.catalog, key: changed}
        after = audit.observed_endpoint(
            changed, audit.exposed_schemas(), "canonical", changed_catalog,
        )
        self.assertNotEqual(before, after)
        self.assertIn("custom_executor_group", after["directExecuteGrants"])
        self.assertEqual(
            [row["role"] for row in after["effectiveCallerMatrix"] if row["role"].startswith("custom_")],
            ["custom_executor_group", "custom_executor_login"],
        )

    def test_predecessor_schema_and_hash_tamper_fail_closed(self) -> None:
        for field in ("databaseSchemaSha", "predecessorArtifactSha256"):
            tampered = copy.deepcopy(self.lineage)
            tampered["source"]["currentTransition"][field] = "0" * 64 if field.endswith("Sha256") else "0" * 40
            with self.subTest(field=field), self.assertRaisesRegex(ValueError, "transition provenance"):
                audit.validate_lineage(tampered, self.inventory, self.inventory_hash, self.baseline_hash)
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "artifact.json"
            digest = Path(directory) / "artifact.sha256"
            path.write_text(audit.canonical(self.lineage), encoding="utf-8")
            digest.write_text("0" * 64 + "\n", encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "hash does not match"):
                audit.read_hashed_json(path, digest)

    def test_reviewed_transition_can_append_exact_predecessor_chain(self) -> None:
        advanced = copy.deepcopy(self.lineage)
        produced = audit.SHA.read_text(encoding="utf-8").strip()
        completed = dict(advanced["source"]["currentTransition"])
        completed["producedAuditV2Sha256"] = produced
        completed["producedAuditV2Path"] = str(audit.OUT.relative_to(audit.ROOT))
        prior_completed = advanced["source"]["completedTransitions"]
        completed["predecessorAuditPath"] = prior_completed[-1]["producedAuditV2Path"]
        receipt = {
            "schemaVersion": "database.security-definer-transition-receipt.v1",
            "transition": dict(advanced["source"]["currentTransition"]),
            "producedAuditV2Sha256": produced,
            "producedAuditV2Path": completed["producedAuditV2Path"],
            "predecessorAuditPath": completed["predecessorAuditPath"],
        }
        path = audit.CONTRACT_DIR / "test-completed-transition-receipt.json"
        path.write_text(audit.canonical(receipt), encoding="utf-8")
        completed["receiptPath"] = str(path.relative_to(audit.ROOT))
        completed["receiptSha256"] = audit.sha256_bytes(path.read_bytes())
        current = {
            "sequence": completed["sequence"] + 1,
            "batch": "issue-358-contract",
            "databaseSchemaSha": "a" * 40,
            "predecessorArtifactSha256": produced,
        }
        advanced["source"]["completedTransitions"] = [*prior_completed, completed]
        advanced["source"]["currentTransition"] = current
        try:
            with mock.patch.object(
                    audit, "EXPECTED_COMPLETED_TRANSITIONS",
                    tuple([*prior_completed, completed])), \
                    mock.patch.object(audit, "EXPECTED_CURRENT_TRANSITION", current):
                audit.validate_lineage(advanced, self.inventory, self.inventory_hash, self.baseline_hash)
                schema = json.loads((audit.CONTRACT_DIR / "privileged_routine_lineage.schema.json").read_text())
                jsonschema.Draft202012Validator(schema).validate(advanced)
        finally:
            path.unlink(missing_ok=True)

    def test_transition_advance_plan_settles_current_and_opens_exact_next_source(self) -> None:
        produced = audit.SHA.read_text(encoding="utf-8").strip()
        with self.assertRaisesRegex(ValueError, "live-verified committed audit SHA"):
            audit.transition_advance_plan(
                self.lineage, "0" * 64, batch="issue-356-worker-control-plane",
                database_schema_sha="a" * 40,
            )
        plan = audit.transition_advance_plan(
            self.lineage, produced, batch="issue-358-contract",
            database_schema_sha="a" * 40,
        )
        completed = plan["completedTransition"]
        current = plan["currentTransition"]
        self.assertEqual(completed["sequence"], 4)
        self.assertEqual(completed["predecessorAuditPath"],
                         self.lineage["source"]["completedTransitions"][-1]["producedAuditV2Path"])
        self.assertEqual(completed["producedAuditV2Sha256"], produced)
        self.assertNotEqual(completed["producedAuditV2Path"],
                            "supabase/tests/contracts/security_definer_audit_v2.json")
        self.assertEqual(current, {
            "sequence": 5, "batch": "issue-358-contract",
            "databaseSchemaSha": "a" * 40, "predecessorArtifactSha256": produced,
        })
        self.assertEqual(plan["reviewedCodeConstants"]["EXPECTED_CURRENT_TRANSITION"], current)

    def test_data_api_callable_is_limited_to_transport_roles(self) -> None:
        for item in self.committed["routines"]:
            for endpoint in [item["canonical"], *item["compatibilityAliases"]]:
                for role in endpoint["roleMatrix"]:
                    if role["role"] in {"PUBLIC", "api_internal_executor", "postgres"}:
                        self.assertFalse(role["dataApiTransportRole"])
                        self.assertFalse(role["effectiveDataApiCallable"])

    def test_missing_fixed_search_path_fails_closed(self) -> None:
        catalog = copy.deepcopy(self.catalog)
        key = next(row["canonicalObjectKey"] for row in self.lineage["lineages"]
                   if row["canonicalObjectKey"].startswith("public."))
        catalog[key]["config"] = []
        with self.assertRaisesRegex(ValueError, "unsafe search_path"):
            audit.build_audit(self.lineage, self.lineage_hash, self.baseline, catalog, audit.exposed_schemas())

    def test_attacker_schema_and_misordered_pg_temp_fail_closed(self) -> None:
        key = self.lineage["lineages"][0]["canonicalObjectKey"]
        for setting in ("search_path=attacker, pg_temp", "search_path=pg_temp, public", "search_path=$user, public"):
            catalog = copy.deepcopy(self.catalog)
            catalog[key]["config"] = [setting]
            with self.subTest(setting=setting), self.assertRaisesRegex(ValueError, "unsafe search_path"):
                audit.build_audit(
                    self.lineage, self.lineage_hash, self.baseline, catalog, audit.exposed_schemas(),
                )

    def test_search_path_rejects_duplicate_nonowner_create_and_unproved_legacy_paths(self) -> None:
        key = self.lineage["lineages"][0]["canonicalObjectKey"]
        duplicate = copy.deepcopy(self.catalog)
        duplicate[key]["config"] = ["search_path=public, public"]
        with self.assertRaisesRegex(ValueError, "unsafe search_path"):
            audit.build_audit(self.lineage, self.lineage_hash, self.baseline, duplicate, audit.exposed_schemas())
        writable = copy.deepcopy(self.catalog)
        writable[key]["config"] = ["search_path=public"]
        trust = next(item for item in writable[key]["schemaTrust"] if item["schema"] == "public")
        trust["roleCreate"]["authenticated"] = True
        trust["nonOwnerCallableCreateRoles"] = ["authenticated"]
        with self.assertRaisesRegex(ValueError, "unsafe search_path"):
            audit.build_audit(self.lineage, self.lineage_hash, self.baseline, writable, audit.exposed_schemas())
        untrusted_owner = copy.deepcopy(self.catalog)
        untrusted_owner[key]["config"] = ["search_path=public"]
        owner_trust = next(item for item in untrusted_owner[key]["schemaTrust"] if item["schema"] == "public")
        owner_trust.update({"ownerRole": "custom_owner", "ownerTrusted": False})
        with self.assertRaisesRegex(ValueError, "unsafe search_path"):
            audit.build_audit(
                self.lineage, self.lineage_hash, self.baseline, untrusted_owner, audit.exposed_schemas(),
            )
        for setting in ('search_path=""', "search_path=public"):
            legacy = copy.deepcopy(self.catalog)
            legacy[key]["config"] = [setting]
            with self.subTest(setting=setting), self.assertRaisesRegex(ValueError, "unsafe search_path"):
                audit.build_audit(
                    self.lineage, self.lineage_hash, self.baseline, legacy, audit.exposed_schemas(),
                )

    def test_unchanged_legacy_definition_can_be_grandfathered_only_with_static_proof(self) -> None:
        row = copy.deepcopy(next(iter(self.catalog.values())))
        row["config"] = ['search_path=""']
        row["body"] = "select public.teams.id from public.teams"
        row["staticDependencies"] = [
            {"type": "RELATION", "schema": "public", "name": "teams", "params": ""},
        ]
        endpoint = audit.endpoint_properties(row, grandfathered_definition=True)
        self.assertTrue(endpoint["safeSearchPath"])
        self.assertEqual(endpoint["searchPathDisposition"], "grandfathered-static-proof")
        self.assertTrue(endpoint["grandfatheredSearchPathResidue"])
        self.assertEqual(endpoint["searchPathProof"]["unqualifiedDependencies"], [])

        row["body"] = "select teams.id from teams"
        endpoint = audit.endpoint_properties(row, grandfathered_definition=True)
        self.assertFalse(endpoint["safeSearchPath"])
        self.assertIn("unqualified", endpoint["unsafeSearchPathReason"])

    def test_dynamic_sql_without_literal_qualification_proof_fails_closed(self) -> None:
        row = copy.deepcopy(next(iter(self.catalog.values())))
        row["config"] = ["search_path=public"]
        row["body"] = "begin execute v_sql; end"
        endpoint = audit.endpoint_properties(row, grandfathered_definition=True)
        self.assertFalse(endpoint["safeSearchPath"])
        self.assertIn("unproven-dynamic-sql", endpoint["unsafeSearchPathReason"])

        row["body"] = "begin execute format('select * from %I', p_table); end"
        endpoint = audit.endpoint_properties(row, grandfathered_definition=True)
        self.assertFalse(endpoint["safeSearchPath"])
        self.assertFalse(endpoint["searchPathProof"]["dynamicSqlTemplates"][0]["provenQualified"])

        row["body"] = "begin execute format('select * from %I.%I', p_schema, p_table); end"
        endpoint = audit.endpoint_properties(row, grandfathered_definition=True)
        self.assertFalse(endpoint["safeSearchPath"])
        self.assertFalse(endpoint["searchPathProof"]["dynamicSqlTemplates"][0]["provenQualified"])

        row["body"] = "begin execute format('select * from public.%I', p_table); end"
        endpoint = audit.endpoint_properties(row, grandfathered_definition=True)
        self.assertTrue(endpoint["safeSearchPath"])
        self.assertTrue(endpoint["searchPathProof"]["dynamicSqlTemplates"][0]["provenQualified"])

    def test_runtime_regclass_regtype_and_sequence_lookups_require_trusted_qualification(self) -> None:
        row = copy.deepcopy(next(iter(self.catalog.values())))
        row["config"] = ['search_path=""']
        for expression in (
            "to_regclass('target')", "'target'::regclass", "nextval('target')",
            "currval('target')", "setval('target', 1)", "to_regtype('target')",
            "to_regclass(p_target)",
        ):
            row["body"] = f"select {expression}"
            endpoint = audit.endpoint_properties(row, grandfathered_definition=True)
            with self.subTest(expression=expression):
                self.assertFalse(endpoint["safeSearchPath"])
                self.assertIn("runtime-relation-or-type-lookup", endpoint["unsafeSearchPathReason"])
        for expression in (
            "to_regclass('public.target')", "'public.target'::regclass",
            "nextval('private.target')", "to_regtype('util.target')",
        ):
            row["body"] = f"select {expression}"
            endpoint = audit.endpoint_properties(row, grandfathered_definition=True)
            with self.subTest(expression=expression):
                self.assertTrue(endpoint["safeSearchPath"])
                self.assertEqual(endpoint["searchPathProof"]["unqualifiedRuntimeLookupTokens"], [])

    def test_new_privileged_routine_cannot_create_grandfathered_residue(self) -> None:
        lineage = copy.deepcopy(self.lineage)
        catalog = copy.deepcopy(self.catalog)
        mapping = lineage["lineages"][0]
        mapping["origin"] = "transition-native"
        mapping["birthTransition"] = {
            "sequence": 1, "batch": "issue-358-contract", "databaseSchemaSha": "a" * 40,
        }
        mapping["originalDefinitionSha256"] = audit.sha256_text(catalog[mapping["canonicalObjectKey"]]["definition"])
        catalog[mapping["canonicalObjectKey"]]["config"] = ['search_path=""']
        with self.assertRaisesRegex(ValueError, "unsafe search_path"):
            audit.build_audit(
                lineage, audit.sha256_text(audit.canonical(lineage)), self.baseline, catalog,
                audit.exposed_schemas(),
            )

    def test_issue358_can_require_zero_grandfathered_search_path_residue(self) -> None:
        with self.assertRaisesRegex(ValueError, "requires grandfatheredSearchPathResidueCount=0"):
            audit.require_zero_search_path_residue(self.committed)
        contract = copy.deepcopy(self.committed)
        contract["summary"].update({
            "grandfatheredSearchPathResidueCount": 0,
            "unsafeSearchPathEndpointCount": 0,
            "searchPathContractReady": True,
        })
        audit.require_zero_search_path_residue(contract)

    def test_procedure_is_never_reported_as_postgrest_callable(self) -> None:
        catalog = copy.deepcopy(self.catalog)
        key = next(row["canonicalObjectKey"] for row in self.lineage["lineages"]
                   if row["canonicalObjectKey"].startswith("public."))
        catalog[key]["routineKind"] = "procedure"
        observed = audit.build_audit(
            self.lineage, self.lineage_hash, self.baseline, catalog, audit.exposed_schemas(),
        )
        endpoint = next(row["canonical"] for row in observed["routines"] if row["canonical"]["currentObjectKey"] == key)
        self.assertTrue(endpoint["currentSchema"] in audit.exposed_schemas())
        self.assertTrue(any(role["effectiveCallable"] for role in endpoint["roleMatrix"]))
        self.assertTrue(all(not role["effectiveDataApiCallable"] for role in endpoint["roleMatrix"]))

    def test_postgrest_schema_cache_and_direct_invocation_are_distinct(self) -> None:
        key = next(row["canonicalObjectKey"] for row in self.lineage["lineages"]
                   if row["canonicalObjectKey"].startswith("public."))
        expected = {
            "trigger": (False, False, False),
            "event_trigger": (True, False, False),
            "internal": (True, False, False),
            "cstring": (True, True, True),
            "record": (True, True, True),
        }
        for result_type, (cached, direct, callable_shape) in expected.items():
            catalog = copy.deepcopy(self.catalog)
            catalog[key]["returnTypeName"] = result_type
            catalog[key]["resultType"] = result_type
            catalog[key]["outputArgumentCount"] = 0
            observed = audit.build_audit(
                self.lineage, self.lineage_hash, self.baseline, catalog, audit.exposed_schemas(),
            )
            endpoint = next(row["canonical"] for row in observed["routines"]
                            if row["canonical"]["currentObjectKey"] == key)
            with self.subTest(result_type=result_type):
                shape = endpoint["postgrestShape"]
                self.assertEqual(shape["schemaCacheEligible"], cached)
                self.assertEqual(shape["directInvocationSupported"], direct)
                self.assertEqual(shape["eligible"], callable_shape)
                if result_type == "event_trigger":
                    self.assertTrue(any(
                        role["effectiveDataApiEndpoint"] for role in endpoint["roleMatrix"]
                        if role["dataApiTransportRole"] and role["effectiveCallable"]
                    ))
                    self.assertTrue(all(
                        not role["effectiveDataApiCallable"] for role in endpoint["roleMatrix"]
                    ))

        catalog = copy.deepcopy(self.catalog)
        for pseudo_type in ("internal", "trigger", "event_trigger", "record", "cstring"):
            candidate = copy.deepcopy(catalog[key])
            candidate["inputArgumentTypes"] = [pseudo_type]
            candidate["inputArgumentTypeKinds"] = ["p"]
            candidate["inputArgumentNames"] = ["value"]
            candidate["inputArgumentRequired"] = [True]
            shape = audit.postgrest_shape(candidate, {candidate["objectKey"]: candidate})
            with self.subTest(pseudo_type=pseudo_type):
                self.assertTrue(shape["schemaCacheEligible"])
                self.assertFalse(shape["directInvocationSupported"])
                self.assertEqual(
                    shape["directInvocationReason"],
                    "pseudo-type-input-is-not-postgrest-materializable",
                )

    def test_data_api_transport_requires_authenticator_set_role_membership(self) -> None:
        row = copy.deepcopy(next(item for item in self.catalog.values() if item["schema"] == "public"))
        anon = next(item for item in row["namedRoleMatrix"] if item["role"] == "anon")
        anon.update({
            "schemaUsage": True,
            "execute": True,
            "postgrestAuthenticatorCanSetRole": False,
        })
        shape = audit.postgrest_shape(row, {row["objectKey"]: row})
        matrix = audit.role_matrix(row, ["public"], shape)
        anon_result = next(item for item in matrix if item["role"] == "anon")
        self.assertTrue(anon_result["effectiveCallable"])
        self.assertFalse(anon_result["dataApiTransportRole"])
        self.assertFalse(anon_result["effectiveDataApiCallable"])

    def test_data_api_schema_order_is_preserved_as_route_semantics(self) -> None:
        first = audit.build_audit(
            self.lineage, self.lineage_hash, self.baseline, self.catalog,
            ["api", "public", "graphql_public"],
        )
        second = audit.build_audit(
            self.lineage, self.lineage_hash, self.baseline, self.catalog,
            ["public", "api", "graphql_public"],
        )
        self.assertEqual(first["exposedSchemas"][0], "api")
        self.assertEqual(second["exposedSchemas"][0], "public")
        self.assertNotEqual(first, second)

    def test_overloads_and_composite_arguments_keep_exact_transport_identity(self) -> None:
        composite = self.fixture["moves"][0]
        self.assertIn("worker_job_artifacts", composite["originalObjectKey"])
        self.assertNotEqual(composite["originalObjectKey"], composite["canonicalObjectKey"])
        template = copy.deepcopy(next(row for row in self.catalog.values() if row["schema"] == "public"))
        first = copy.deepcopy(template)
        second = copy.deepcopy(template)
        first.update({"objectKey": "public.test_overload(p_value text)", "name": "test_overload",
                      "routineKind": "function", "inputArgumentNames": ["p_value"],
                      "inputArgumentTypes": ["text"], "inputArgumentTypeKinds": ["b"],
                      "inputArgumentRequired": [True]})
        second.update({"objectKey": "public.test_overload(p_value uuid)", "name": "test_overload",
                       "routineKind": "function", "inputArgumentNames": ["p_value"],
                       "inputArgumentTypes": ["uuid"], "inputArgumentTypeKinds": ["b"],
                       "inputArgumentRequired": [True]})
        self.assertNotEqual(first["objectKey"], second["objectKey"])
        overload_catalog = {first["objectKey"]: first, second["objectKey"]: second}
        for endpoint in (first, second):
            shape = audit.postgrest_shape(endpoint, overload_catalog)
            matrix = audit.role_matrix(endpoint, {"public"}, shape)
            self.assertTrue(all(item["postgrestRoutineKind"] for item in matrix))
            self.assertFalse(shape["eligible"])
            self.assertEqual(shape["reason"], "all-named-request-shapes-are-ambiguous")
            self.assertTrue(shape["hasAmbiguousRequestShape"])
            self.assertEqual(shape["ambiguousRequestKeySetExamples"], [["p_value"]])
        second["inputArgumentNames"] = ["p_id"]
        self.assertTrue(audit.postgrest_shape(first, overload_catalog)["eligible"])
        self.assertTrue(audit.postgrest_shape(second, overload_catalog)["eligible"])

    def test_postgrest_optional_overload_and_raw_body_resolution(self) -> None:
        template = copy.deepcopy(next(row for row in self.catalog.values() if row["schema"] == "public"))
        short = copy.deepcopy(template)
        long = copy.deepcopy(template)
        short.update({
            "objectKey": "public.optional_overload(a integer)", "name": "optional_overload",
            "inputArgumentNames": ["a"], "inputArgumentTypes": ["integer"],
            "inputArgumentTypeKinds": ["b"],
            "inputArgumentRequired": [True],
        })
        long.update({
            "objectKey": "public.optional_overload(a integer, b integer)", "name": "optional_overload",
            "inputArgumentNames": ["a", "b"], "inputArgumentTypes": ["integer", "integer"],
            "inputArgumentTypeKinds": ["b", "b"],
            "inputArgumentRequired": [True, False],
        })
        catalog = {short["objectKey"]: short, long["objectKey"]: long}
        short_shape = audit.postgrest_shape(short, catalog)
        long_shape = audit.postgrest_shape(long, catalog)
        self.assertFalse(short_shape["eligible"])
        self.assertEqual(short_shape["ambiguousRequestKeySetExamples"], [["a"]])
        self.assertTrue(long_shape["eligible"])
        self.assertEqual(long_shape["unambiguousRequestKeySet"], ["a", "b"])
        self.assertTrue(long_shape["hasAmbiguousRequestShape"])
        self.assertEqual(long_shape["ambiguousRequestKeySetExamples"], [["a"]])

        for pg_type, media_type in (
            ("bytea", "application/octet-stream"), ("json", "application/json"),
            ("jsonb", "application/json"), ("text", "text/plain"), ("xml", "text/xml"),
        ):
            raw = copy.deepcopy(template)
            raw.update({
                "objectKey": f"public.raw_body({pg_type})", "name": f"raw_body_{pg_type}",
                "inputArgumentNames": [""], "inputArgumentTypes": [pg_type],
                "inputArgumentTypeKinds": ["b"],
                "inputArgumentRequired": [True],
            })
            shape = audit.postgrest_shape(raw, {raw["objectKey"]: raw})
            with self.subTest(pg_type=pg_type):
                self.assertTrue(shape["schemaCacheEligible"])
                self.assertTrue(shape["eligible"])
                self.assertEqual(shape["rawBodyMediaTypes"], [media_type])

        raw_json = copy.deepcopy(template)
        raw_jsonb = copy.deepcopy(template)
        raw_json.update({
            "objectKey": "public.raw_collision(json)", "name": "raw_collision",
            "inputArgumentNames": [""], "inputArgumentTypes": ["json"],
            "inputArgumentTypeKinds": ["b"],
            "inputArgumentRequired": [True],
        })
        raw_jsonb.update({
            "objectKey": "public.raw_collision(jsonb)", "name": "raw_collision",
            "inputArgumentNames": [""], "inputArgumentTypes": ["jsonb"],
            "inputArgumentTypeKinds": ["b"],
            "inputArgumentRequired": [True],
        })
        raw_catalog = {row["objectKey"]: row for row in (raw_json, raw_jsonb)}
        for row in raw_catalog.values():
            shape = audit.postgrest_shape(row, raw_catalog)
            self.assertFalse(shape["requestResolvable"])
            self.assertEqual(
                shape["requestResolutionReason"],
                "ambiguous-single-unnamed-argument-fallback",
            )

        zero = copy.deepcopy(template)
        optional = copy.deepcopy(template)
        zero.update({
            "objectKey": "public.empty_collision()", "name": "empty_collision",
            "inputArgumentNames": [], "inputArgumentTypes": [], "inputArgumentTypeKinds": [],
            "inputArgumentRequired": [],
        })
        optional.update({
            "objectKey": "public.empty_collision(value integer)", "name": "empty_collision",
            "inputArgumentNames": ["value"], "inputArgumentTypes": ["integer"],
            "inputArgumentTypeKinds": ["b"],
            "inputArgumentRequired": [False],
        })
        empty_catalog = {row["objectKey"]: row for row in (zero, optional)}
        self.assertFalse(audit.postgrest_shape(zero, empty_catalog)["requestResolvable"])
        optional_shape = audit.postgrest_shape(optional, empty_catalog)
        self.assertTrue(optional_shape["requestResolvable"])
        self.assertEqual(optional_shape["unambiguousRequestKeySet"], ["value"])

    def test_postgrest_fully_shadowed_twenty_optional_args_is_bounded(self) -> None:
        template = copy.deepcopy(next(row for row in self.catalog.values() if row["schema"] == "public"))
        names = [f"p_{index:02d}" for index in range(20)]
        first = copy.deepcopy(template)
        second = copy.deepcopy(template)
        for index, row in enumerate((first, second)):
            row.update({
                "objectKey": f"public.large_collision_{index}({', '.join(['integer'] * 20)})",
                "name": "large_collision",
                "inputArgumentNames": names,
                "inputArgumentTypes": ["integer"] * 20,
                "inputArgumentTypeKinds": ["b"] * 20,
                "inputArgumentRequired": [False] * 20,
            })
        catalog = {row["objectKey"]: row for row in (first, second)}
        started = time.perf_counter()
        shape = audit.postgrest_shape(first, catalog)
        elapsed = time.perf_counter() - started
        self.assertLess(elapsed, 0.5)
        self.assertFalse(shape["requestResolvable"])
        self.assertTrue(shape["hasAmbiguousRequestShape"])
        self.assertEqual(shape["ambiguousRequestKeySetExamples"], [[]])

    def test_in_memory_endpoint_mapping_roundtrip_preserves_lineage_identity(self) -> None:
        transitioned, _ = self.transition()
        restored = copy.deepcopy(transitioned)
        baseline_by_original = {row["originalObjectKey"]: row for row in self.lineage["lineages"]}
        for row in restored["lineages"]:
            baseline = baseline_by_original[row["originalObjectKey"]]
            row["canonicalObjectKey"] = baseline["canonicalObjectKey"]
            row["compatibilityAliases"] = baseline["compatibilityAliases"]
        self.assertEqual(restored, self.lineage)

    def test_canonical_serialization_is_deterministic_in_memory(self) -> None:
        first = audit.canonical(copy.deepcopy(self.committed)).encode("utf-8")
        second = audit.canonical(copy.deepcopy(self.committed)).encode("utf-8")
        self.assertEqual(first, second)
        self.assertEqual(audit.sha256_bytes(first), audit.sha256_bytes(second))

    def test_canonical_runner_checks_v1_and_v2(self) -> None:
        runner = (audit.ROOT / "scripts/run_database_contract.py").read_text(encoding="utf-8")
        self.assertIn('"scripts/security_definer_audit.py", "--check"', runner)
        self.assertIn('"scripts/security_definer_audit_v2.py", "--check"', runner)
        self.assertIn('"scripts.test_security_definer_audit_v2_postgrest_conformance"', runner)
        self.assertIn('"scripts/test_security_definer_audit_v2_transition_integration.py"', runner)
        harness = (audit.ROOT / "scripts/test_security_definer_audit_v2_transition_integration.py").read_text()
        self.assertNotIn("skip", harness.lower())
        self.assertIn("audit.catalog_proof_query()", harness)
        for required in ("--migration", "--rollback", "--expected-migration-sha256",
                         "--expected-rollback-sha256", "--expected-base"):
            self.assertIn(required, harness)


if __name__ == "__main__":
    unittest.main()
