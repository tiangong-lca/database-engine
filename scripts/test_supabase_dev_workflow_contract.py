#!/usr/bin/env python3
"""Fail closed if persistent-Dev verification becomes a second deployer."""

from __future__ import annotations

import re
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
WORKFLOW = REPO_ROOT / ".github" / "workflows" / "supabase-dev.yml"
MIGRATIONS = REPO_ROOT / "supabase" / "migrations"


def main() -> int:
    text = WORKFLOW.read_text(encoding="utf-8")
    lowered = text.lower()

    forbidden = {
        "database password": "supabase_db_password",
        "manual project link": "supabase link",
        "second migration deployer": "supabase db push",
        "second config deployer": "supabase config push",
        "Management API mutation": "--request patch",
    }
    failures = [label for label, token in forbidden.items() if token in lowered]

    migration_versions = sorted(path.name.split("_", 1)[0] for path in MIGRATIONS.glob("*.sql"))
    expected_head = migration_versions[-1]
    match = re.search(r'EXPECTED_MIGRATION_HEAD:\s*["\']?(\d{14})', text)
    if match is None or match.group(1) != expected_head:
        failures.append(
            f"expected migration head must equal latest migration {expected_head}"
        )

    required = (
        "pull_request:",
        "supabase-dev-verification-${{ github.ref }}",
        "cancel-in-progress: true",
        "github.event_name == 'push' && github.ref == 'refs/heads/dev'",
        "python scripts/test_supabase_dev_workflow_contract.py",
        "python scripts/build_schema_workspace.py --environment local",
        "git diff --exit-code -- supabase/workspace",
        "git status --porcelain --untracked-files=all -- supabase/workspace",
        "::add-mask::",
        "svc_schema_contract_status",
        "SUPABASE_ACCESS_TOKEN",
        "SUPABASE_DEV_PROJECT_ID",
        "supabase db reset --no-seed",
        '"public", "api", "graphql_public"',
        '"public", "api", "extensions"',
        "Accept-Profile: private",
        "Content-Profile: public",
    )
    failures.extend(f"missing {token}" for token in required if token not in text)

    if failures:
        for failure in failures:
            print(f"FAIL: {failure}")
        return 1

    print(
        "PASS: Supabase Dev workflow is read-only and waits for exact head "
        f"{expected_head}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
