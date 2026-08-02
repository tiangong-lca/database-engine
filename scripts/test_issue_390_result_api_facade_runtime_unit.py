#!/usr/bin/env python3
"""Offline target-binding tests for the Issue #390 runtime qualification."""

from __future__ import annotations

import os
import unittest
from unittest import mock

from scripts.identity_collaboration_target import TargetContext
from scripts import test_issue_390_result_api_facade_runtime as runtime


def context(*, port: int, identity: str = "system-a:postgres") -> TargetContext:
    return TargetContext(
        database_url=f"postgresql://postgres:x@127.0.0.1:{port}/postgres",
        rest_url=f"http://127.0.0.1:{port - 1}/rest/v1",
        anon_key=f"anon-{port}",
        service_role_key=f"service-{port}",
        jwt_secret=f"jwt-{port}",
        workdir=f"/tmp/stack-{port}",
        identity=identity,
    )


def injected_environment(target: TargetContext) -> dict[str, str]:
    values = {
        variable: str(getattr(target, field))
        for field, variable in runtime.INJECTED_TARGET_ENV.items()
    }
    if target.workdir is not None:
        values["SUPABASE_WORKDIR"] = target.workdir
    return values


class Issue390RuntimeTargetTest(unittest.TestCase):
    def test_injected_target_cannot_use_default_stack_credentials(self) -> None:
        target_a = context(port=61002)
        stack_b = context(port=62002, identity="system-b:postgres")
        with mock.patch.dict(os.environ, injected_environment(target_a), clear=True), \
             mock.patch.object(runtime, "resolve_target", return_value=stack_b):
            with self.assertRaisesRegex(RuntimeError, "differs from selected"):
                runtime.runtime_context()

    def test_complete_injected_target_is_reused_exactly(self) -> None:
        target = context(port=61002)
        with mock.patch.dict(os.environ, injected_environment(target), clear=True), \
             mock.patch.object(runtime, "resolve_target", return_value=target), \
             mock.patch.object(runtime, "apply_target_environment") as apply:
            self.assertEqual(runtime.runtime_context(), target)
        apply.assert_not_called()

    def test_partial_injected_target_fails_closed(self) -> None:
        with mock.patch.dict(os.environ, {
            "DATABASE_URL": "postgresql://postgres:x@127.0.0.1:61002/postgres",
            runtime.VERIFIED_ENV: "system-a:postgres",
        }, clear=True):
            with self.assertRaisesRegex(RuntimeError, "partial canonical target"):
                runtime.runtime_context()

    def test_marker_missing_resolves_and_rebinds_safe_local_target(self) -> None:
        target = context(port=61002)
        with mock.patch.dict(os.environ, {
            "DATABASE_URL": target.database_url,
            "SUPABASE_REST_URL": "http://127.0.0.1:62001/rest/v1",
        }, clear=True), mock.patch.object(
            runtime, "resolve_target", return_value=target,
        ), mock.patch.object(runtime, "apply_target_environment") as apply:
            self.assertEqual(runtime.runtime_context(), target)
        apply.assert_called_once_with(target)

    def test_verified_workdir_mismatch_fails_closed(self) -> None:
        target = context(port=61002)
        values = injected_environment(target)
        values["SUPABASE_WORKDIR"] = "/tmp/wrong-stack"
        with mock.patch.dict(os.environ, values, clear=True), mock.patch.object(
            runtime, "resolve_target", return_value=target,
        ):
            with self.assertRaisesRegex(RuntimeError, "SUPABASE_WORKDIR differs"):
                runtime.runtime_context()

    def test_non_loopback_injected_target_is_rejected_before_resolution(self) -> None:
        target = context(port=61002)
        values = injected_environment(target)
        values["DATABASE_URL"] = "postgresql://postgres:x@db.example.com:5432/postgres"
        with mock.patch.dict(os.environ, values, clear=True), \
             mock.patch.object(runtime, "resolve_target") as resolve:
            with self.assertRaisesRegex(RuntimeError, "refusing non-loopback"):
                runtime.runtime_context()
        resolve.assert_not_called()


if __name__ == "__main__":
    unittest.main()
