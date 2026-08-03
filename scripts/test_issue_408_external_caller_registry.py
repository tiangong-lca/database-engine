from __future__ import annotations

import copy
import hashlib
import json
import os
import tempfile
import unittest
from pathlib import Path

from scripts.issue_408_external_caller_registry import (
    DEFAULT_REGISTRY,
    DEFAULT_SCHEMA,
    EXPECTED_CALLER_IDS,
    RegistryError,
    WORKER_ROLE,
    _validate_shape,
    repository_roots,
    validate_registry_files,
    validate_source_readback,
)


class ExternalCallerRegistryTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.document = json.loads(DEFAULT_REGISTRY.read_text(encoding="utf-8"))
        cls.schema = json.loads(DEFAULT_SCHEMA.read_text(encoding="utf-8"))
        workspace_root = os.environ.get("ISSUE408_WORKSPACE_ROOT")
        cls.roots = repository_roots(Path(workspace_root)) if workspace_root else None

    def caller(self, document: dict[str, object], caller_id: str) -> dict[str, object]:
        return next(
            row
            for row in document["callers"]  # type: ignore[index]
            if row["id"] == caller_id
        )

    def validate_temporary(self, document: dict[str, object], *, digest: str | None = None) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            registry = root / "registry.json"
            schema = root / "schema.json"
            sidecar = root / "registry.sha256"
            registry_bytes = (json.dumps(document, indent=2) + "\n").encode("utf-8")
            registry.write_bytes(registry_bytes)
            schema.write_text(json.dumps(self.schema), encoding="utf-8")
            sidecar.write_text(
                (digest or hashlib.sha256(registry_bytes).hexdigest()) + "\n",
                encoding="ascii",
            )
            validate_registry_files(
                registry,
                schema,
                sidecar,
                repo_roots=self.roots,
                require_source_readback=self.roots is not None,
            )

    def test_canonical_registry_shape_hash_and_optional_exact_sources_pass(self) -> None:
        validated = validate_registry_files(
            repo_roots=self.roots,
            require_source_readback=self.roots is not None,
        )
        self.assertEqual(
            {row["id"] for row in validated["callers"]}, EXPECTED_CALLER_IDS
        )
        self.assertTrue(
            all(row["includedInWorkerManifest"] is False for row in validated["callers"])
        )

    def test_non_worker_entry_cannot_be_included_in_worker_manifest(self) -> None:
        document = copy.deepcopy(self.document)
        self.caller(document, "release-actor-public-cli")[
            "includedInWorkerManifest"
        ] = True
        with self.assertRaisesRegex(RegistryError, "included in Worker manifest"):
            _validate_shape(document)

    def test_non_worker_entry_cannot_reuse_runtime_role_or_capability_owner(self) -> None:
        for field in ("runtimeRole", "capabilityOwner", "identityOwner"):
            with self.subTest(field=field):
                document = copy.deepcopy(self.document)
                self.caller(document, "utilities-service-operator-runtime")[field] = WORKER_ROLE
                with self.assertRaisesRegex(RegistryError, "reuses lca_worker_runtime"):
                    _validate_shape(document)

    def test_worker_role_reference_is_limited_to_operator_and_test_evidence(self) -> None:
        document = copy.deepcopy(self.document)
        self.caller(document, "edge-postgrest-service-runtime")[
            "observedCapabilityReferences"
        ] = [WORKER_ROLE]
        with self.assertRaisesRegex(RegistryError, "deployment/test evidence"):
            _validate_shape(document)

    def test_missing_or_unclassified_dynamic_selector_disposition_fails(self) -> None:
        missing = copy.deepcopy(self.document)
        del self.caller(missing, "edge-direct-postgres-embedding-runtime")[
            "dynamicSelectorDisposition"
        ]
        with self.assertRaisesRegex(RegistryError, "keys are not exact"):
            _validate_shape(missing)

        unclassified = copy.deepcopy(self.document)
        self.caller(unclassified, "edge-direct-postgres-embedding-runtime")[
            "dynamicSelectorDisposition"
        ]["status"] = "pending"  # type: ignore[index]
        with self.assertRaisesRegex(RegistryError, "remains unclassified"):
            _validate_shape(unclassified)

    def test_missing_required_caller_fails_closed(self) -> None:
        document = copy.deepcopy(self.document)
        document["callers"] = document["callers"][:-1]  # type: ignore[index]
        with self.assertRaisesRegex(RegistryError, "caller census differs"):
            _validate_shape(document)

    def test_registry_hash_drift_fails(self) -> None:
        with self.assertRaisesRegex(RegistryError, "registry sha256 differs"):
            self.validate_temporary(copy.deepcopy(self.document), digest="0" * 64)

    def test_source_commit_path_and_blob_drift_fail(self) -> None:
        if self.roots is None:
            for caller in self.document["callers"]:
                for source in caller["provenance"]:
                    self.assertRegex(source["commit"], r"^[0-9a-f]{40}$")
                    self.assertRegex(source["gitBlob"], r"^[0-9a-f]{40}$")
            return
        cases = (
            ("commit", "0" * 40, "git readback failed"),
            ("path", "README-does-not-exist.md", "source path drifted"),
            ("gitBlob", "0" * 40, "source blob drifted"),
        )
        for field, value, message in cases:
            with self.subTest(field=field):
                document = copy.deepcopy(self.document)
                source = self.caller(document, "release-actor-public-cli")["provenance"][0]  # type: ignore[index]
                source[field] = value
                _validate_shape(document)
                with self.assertRaisesRegex(RegistryError, message):
                    validate_source_readback(document, self.roots)

    def test_source_repository_mapping_is_mandatory(self) -> None:
        with self.assertRaisesRegex(RegistryError, "mapping is missing"):
            validate_source_readback(self.document, {})


if __name__ == "__main__":
    unittest.main()
