#!/usr/bin/env python3
"""Fail closed if persistent-Dev deployment exceeds the database boundary."""

from __future__ import annotations

import re
from pathlib import Path

from resolve_migration_head import resolve_migration_head


REPO_ROOT = Path(__file__).resolve().parents[1]
WORKFLOW = REPO_ROOT / ".github" / "workflows" / "supabase-dev.yml"
MIGRATIONS = REPO_ROOT / "supabase" / "migrations"


def main() -> int:
    text = WORKFLOW.read_text(encoding="utf-8")
    lowered = text.lower()

    forbidden = {
        "Edge Functions command": "supabase functions",
        "project configuration deploy": "supabase config push",
    }
    failures = [label for label, token in forbidden.items() if token in lowered]

    expected_head = resolve_migration_head(MIGRATIONS)
    if re.search(r'EXPECTED_MIGRATION_HEAD:\s*["\']?\d{14}', text):
        failures.append("migration head must not be pinned manually")

    hosted_workflow = text.split("  deploy-and-verify:", 1)[-1]
    hosted_required = (
        "uses: actions/checkout@v7",
        "needs: local-contract",
        "SUPABASE_DB_PASSWORD: ${{ secrets.SUPABASE_DEV_DB_PASSWORD }}",
        'supabase link --project-ref "$SUPABASE_PROJECT_ID"',
        "supabase db push --include-all",
        "id: migration_head",
        "python scripts/resolve_migration_head.py",
        "EXPECTED_MIGRATION_HEAD: ${{ steps.migration_head.outputs.head }}",
    )
    failures.extend(
        f"hosted verification missing {token}"
        for token in hosted_required
        if token not in hosted_workflow
    )

    if lowered.count("supabase db push --include-all") != 1:
        failures.append("workflow must contain exactly one database-only db push")

    exact_postgrest_patch = (
        '{"db_schema":"public,api,graphql_public",'
        '"db_extra_search_path":"public,api,extensions","max_rows":1000}'
    )
    if text.count("--request PATCH") != 1:
        failures.append("workflow must contain exactly one targeted PostgREST PATCH")
    if exact_postgrest_patch not in text:
        failures.append("PostgREST PATCH must contain only the exact runtime contract")

    deployment_order = (
        hosted_workflow.find('supabase link --project-ref "$SUPABASE_PROJECT_ID"'),
        hosted_workflow.find("supabase db push --include-all"),
        hosted_workflow.find("- name: Apply exact PostgREST runtime contract"),
        hosted_workflow.find("id: migration_head"),
        hosted_workflow.find("- name: Verify exact deployed migration head"),
        hosted_workflow.find("- name: Verify hosted PostgREST boundary"),
    )
    if -1 not in deployment_order and deployment_order != tuple(sorted(deployment_order)):
        failures.append(
            "link, migration push, PostgREST runtime PATCH, and hosted probes are out of order"
        )

    required = (
        "pull_request:",
        "supabase-dev-deployment-${{ github.ref }}",
        "cancel-in-progress: true",
        "github.event_name == 'push' && github.ref == 'refs/heads/dev'",
        "python scripts/test_resolve_migration_head.py",
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
        '"max_rows":1000',
        "and .max_rows == 1000",
        "Accept-Profile: private",
        "Content-Profile: public",
    )
    failures.extend(f"missing {token}" for token in required if token not in text)

    if failures:
        for failure in failures:
            print(f"FAIL: {failure}")
        return 1

    print(
        "PASS: Supabase Dev workflow deploys migrations, applies only the exact "
        "PostgREST runtime contract, and verifies exact head "
        f"{expected_head}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
