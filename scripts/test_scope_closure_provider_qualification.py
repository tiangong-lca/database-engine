from __future__ import annotations

import os
from pathlib import Path
import sys
import unittest
from unittest.mock import patch


sys.path.insert(0, str(Path(__file__).resolve().parent))
import scope_closure_provider_qualification as qualification


RUN_ID = "12345678-1234-4123-8123-123456789abc"
SHA = "1" * 40


class QualificationTests(unittest.TestCase):
    def test_owner_result_is_deterministic_and_exact(self) -> None:
        evidence = {"descriptors": {"count": 1501}}
        with patch.object(qualification, "_component_sha", return_value=SHA):
            first = qualification._owner_result(
                owner="database", run_id=RUN_ID, assertions=1, evidence=evidence
            )
            second = qualification._owner_result(
                owner="database", run_id=RUN_ID, assertions=1, evidence=evidence
            )
        self.assertEqual(first, second)
        self.assertEqual(
            set(first),
            {
                "schemaVersion",
                "runId",
                "owner",
                "component",
                "componentSha",
                "targetClass",
                "productionMutation",
                "assertions",
                "evidence",
            },
        )
        self.assertFalse(first["productionMutation"])

    def test_database_and_storage_leaf_partitions_do_not_overlap(self) -> None:
        database = {
            "descriptors": {"count": 1},
            "publication": {"retryPassed": True},
            "lifecycle": {"detailGcPassed": True, "remainingDetailRows": 0},
        }
        storage = {
            "storage": {"objectCount": 1},
            "download": {
                "signedHeadPassed": True,
                "signedRangePassed": True,
                "hashVerified": True,
            },
            "lifecycle": {
                "expiryRejected": True,
                "objectGcPassed": True,
                "retryIdempotencyPassed": True,
                "remainingObjects": 0,
            },
        }
        leaves: set[str] = set()
        for section, values in database.items():
            leaves.update(f"{section}.{key}" for key in values)
        storage_leaves = {
            f"{section}.{key}" for section, values in storage.items() for key in values
        }
        self.assertTrue(leaves.isdisjoint(storage_leaves))

    def test_production_fingerprint_fails_closed_without_echo(self) -> None:
        environment = {
            "QUALIFICATION_NON_PRODUCTION_CONFIRMATION": (
                "I_CONFIRM_ISOLATED_NON_PRODUCTION_TARGETS"
            ),
            "QUALIFICATION_DATABASE_URL": (
                "postgresql://127.0.0.1:5432/postgres"
                "?application_name=qgzvkongdjqiiamzbbts"
            ),
            "QUALIFICATION_SUPABASE_URL": "http://127.0.0.1:54321",
            "QUALIFICATION_SUPABASE_SERVICE_ROLE_KEY": "local-only",
        }
        with patch.dict(os.environ, environment, clear=True):
            with self.assertRaisesRegex(
                qualification.QualificationError, "production fingerprint"
            ) as raised:
                qualification._validate_common_environment("database")
        self.assertNotIn("qgzvkongdjqiiamzbbts", str(raised.exception))

    def test_storage_rejects_ambiguous_remote_target(self) -> None:
        environment = {
            "QUALIFICATION_NON_PRODUCTION_CONFIRMATION": (
                "I_CONFIRM_ISOLATED_NON_PRODUCTION_TARGETS"
            ),
            "QUALIFICATION_DATABASE_URL": "postgresql://127.0.0.1:5432/postgres",
            "QUALIFICATION_S3_ENDPOINT": "https://storage.example.test",
            "QUALIFICATION_S3_ACCESS_KEY_ID": "local-access",
            "QUALIFICATION_S3_SECRET_ACCESS_KEY": "local-secret",
            "QUALIFICATION_S3_BUCKET": "qualification-test",
        }
        with patch.dict(os.environ, environment, clear=True):
            with self.assertRaisesRegex(
                qualification.QualificationError, "allowlist is invalid"
            ):
                qualification._validate_common_environment("storage")

    def test_storage_accepts_positively_identified_non_production_target(self) -> None:
        database_url = "postgresql://db.example.test/postgres?sslmode=verify-full"
        s3_endpoint = "https://storage.example.test"
        bucket = "qualification-test"
        fingerprints = (
            qualification._target_fingerprint("database", database_url),
            qualification._target_fingerprint("s3", s3_endpoint, bucket=bucket),
        )
        environment = {
            "QUALIFICATION_NON_PRODUCTION_CONFIRMATION": (
                "I_CONFIRM_ISOLATED_NON_PRODUCTION_TARGETS"
            ),
            "QUALIFICATION_DATABASE_URL": database_url,
            "QUALIFICATION_S3_ENDPOINT": s3_endpoint,
            "QUALIFICATION_S3_ACCESS_KEY_ID": "local-access",
            "QUALIFICATION_S3_SECRET_ACCESS_KEY": "local-secret",
            "QUALIFICATION_S3_BUCKET": bucket,
            "QUALIFICATION_VERIFIED_NON_PRODUCTION_FINGERPRINTS": ",".join(
                fingerprints
            ),
        }
        with patch.dict(os.environ, environment, clear=True):
            qualification._validate_common_environment("storage")

    def test_retry_is_bounded(self) -> None:
        attempts: list[int] = []

        def operation(attempt: int) -> bool:
            attempts.append(attempt)
            if attempt < 2:
                raise qualification.QualificationError("transient")
            return True

        self.assertTrue(qualification._retry(operation, attempts=3))
        self.assertEqual(attempts, [0, 1, 2])

    def test_actual_pre_put_guard_blocks_operation(self) -> None:
        calls: list[str] = []
        self.assertEqual(
            qualification._bounded_put(10, lambda: calls.append("allowed") or "ok"),
            "ok",
        )
        with self.assertRaisesRegex(qualification.QualificationError, "pre-PUT"):
            qualification._bounded_put(
                qualification.MAX_OBJECT_BYTES + 1,
                lambda: calls.append("forbidden"),
            )
        self.assertEqual(calls, ["allowed"])

    def test_scale_metrics_derive_count_and_bytes(self) -> None:
        output = (
            '# issue-316 scale metrics: [{"descriptorCount":596,'
            '"descriptorBytes":12000,"rowCount":596,"batchSize":500},'
            '{"descriptorCount":1501,"descriptorBytes":30000,'
            '"rowCount":1501,"batchSize":400}]\n'
        )
        metrics = qualification._scale_metrics(output)
        self.assertEqual(metrics[1501]["descriptorBytes"], 30000)

    def test_sensitive_evidence_is_rejected(self) -> None:
        with self.assertRaisesRegex(qualification.QualificationError, "sensitive"):
            qualification._reject_sensitive({"signedUrl": "redacted"})


if __name__ == "__main__":
    unittest.main()
