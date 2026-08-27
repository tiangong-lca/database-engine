#!/usr/bin/env python3
"""Fail closed if persistent-Dev deployment exceeds the database boundary."""

from __future__ import annotations

import re
from pathlib import Path

from resolve_migration_head import resolve_migration_head


REPO_ROOT = Path(__file__).resolve().parents[1]
WORKFLOW = REPO_ROOT / ".github" / "workflows" / "supabase-dev.yml"
MIGRATIONS = REPO_ROOT / "supabase" / "migrations"
API_CONTRACT_TEST = (
    REPO_ROOT / "supabase" / "tests" / "20260806_api_contract_closure.sql"
)


def job_sections(text: str) -> dict[str, str]:
    """Return exact top-level job bodies without treating step mappings as jobs."""

    matches = list(re.finditer(r"(?m)^  ([a-zA-Z0-9_-]+):\n", text))
    return {
        match.group(1): text[
            match.start() : matches[index + 1].start()
            if index + 1 < len(matches)
            else len(text)
        ]
        for index, match in enumerate(matches)
    }


def management_api_mutations(text: str) -> list[tuple[str, str]]:
    """Classify curl calls to Management API without parsing unrelated REST probes."""

    calls: list[tuple[str, str]] = []
    for url_match in re.finditer(r'"(https://api[.]supabase[.]com/[^"\n]+)"', text):
        curl_start = text.rfind("curl ", 0, url_match.start())
        if curl_start < 0:
            continue
        curl_block = text[curl_start : url_match.end()]
        methods = re.findall(r"--request\s+([A-Z]+)", curl_block)
        method = methods[-1] if methods else "GET"
        if method in {"PATCH", "POST", "PUT", "DELETE"}:
            calls.append((method, url_match.group(1)))
    return calls


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

    api_contract_text = API_CONTRACT_TEST.read_text(encoding="utf-8")
    api_contract_heads = re.findall(
        r"api[.]svc_schema_contract_status[(][)]\s*->>\s*'migrationHead'\s*,"
        r"\s*'(\d{14})'::text",
        api_contract_text,
    )
    if api_contract_heads != [expected_head]:
        failures.append(
            "API closure must assert the exact checkout migration head "
            f"{expected_head}, found {api_contract_heads or 'none'}"
        )

    jobs = job_sections(text)
    hosted_workflow = jobs.get("deploy-and-verify", "")
    preview_workflow = jobs.get("preview-runtime-contract", "")
    if not hosted_workflow:
        failures.append("persistent-Dev job is missing")
    if not preview_workflow:
        failures.append("Preview runtime job is missing")

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
    if hosted_workflow.count("--request PATCH") != 1:
        failures.append("persistent-Dev job must contain exactly one PostgREST PATCH")
    if hosted_workflow.count(exact_postgrest_patch) != 1:
        failures.append("persistent-Dev PATCH must contain only the exact runtime contract")
    if preview_workflow.count("--request PATCH") != 1:
        failures.append("Preview job must contain exactly one PostgREST PATCH")
    if preview_workflow.count(exact_postgrest_patch) != 1:
        failures.append("Preview PATCH must contain only the exact runtime contract")
    if text.count("--request PATCH") != 2:
        failures.append("workflow must contain exactly two targeted PostgREST PATCH calls")
    expected_management_mutations = {
        (
            "PATCH",
            "https://api.supabase.com/v1/projects/$SUPABASE_PROJECT_ID/postgrest",
        ),
        (
            "PATCH",
            "https://api.supabase.com/v1/projects/$PREVIEW_PROJECT_REF/postgrest",
        ),
    }
    actual_management_mutations = management_api_mutations(text)
    if len(actual_management_mutations) != 2 or set(actual_management_mutations) != expected_management_mutations:
        failures.append(
            "Management API mutation surface must be exactly one persistent-Dev and one Preview PostgREST PATCH"
        )

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

    preview_required = (
        "github.event_name == 'pull_request'",
        "github.event.pull_request.head.repo.full_name == github.repository",
        "SUPABASE_ACCESS_TOKEN: ${{ secrets.SUPABASE_ACCESS_TOKEN }}",
        "SUPABASE_MAIN_PROJECT_ID: ${{ vars.SUPABASE_MAIN_PROJECT_ID }}",
        "PREVIEW_GIT_BRANCH: ${{ github.event.pull_request.head.ref }}",
        "PREVIEW_HEAD_SHA: ${{ github.event.pull_request.head.sha }}",
        "PREVIEW_PR_NUMBER: ${{ github.event.pull_request.number }}",
        "steps.preview_authority.outputs.available == 'true'",
        "Supabase Preview runtime verification requires SUPABASE_ACCESS_TOKEN, SUPABASE_MAIN_PROJECT_ID, and SUPABASE_DEV_PROJECT_ID",
        "exit 1",
        "uses: supabase/setup-cli@v2",
        "version: 2.98.0",
        "Wait for exact Supabase Preview check",
        "check_name=Supabase%20Preview&filter=latest",
        '.name == "Supabase Preview"',
        '.head_sha == $head',
        ".app.id == 330661",
        '.app.slug == "supabase"',
        '.app.owner.login == "supabase"',
        '.conclusion == "success"',
        'capture("^https://supabase[.]com/dashboard/project/(?<ref>[a-z]{20})$").ref',
        'EXPECTED_PREVIEW_PROJECT_REF: ${{ steps.supabase_preview_check.outputs.ref }}',
        "supabase branches list",
        '--project-ref "$SUPABASE_MAIN_PROJECT_ID"',
        "--output json",
        '--arg branch "$PREVIEW_GIT_BRANCH"',
        '--arg parent "$SUPABASE_MAIN_PROJECT_ID"',
        '--argjson pr_number "$PREVIEW_PR_NUMBER"',
        ".git_branch == $branch",
        ".pr_number == $pr_number",
        ".parent_project_ref == $parent",
        ".is_default == false",
        ".persistent == false",
        '(.project_ref | test("^[a-z]{20}$"))',
        "if length == 1 then .[0].project_ref",
        '[[ "$preview_project_ref" == "$EXPECTED_PREVIEW_PROJECT_REF" ]]',
        '[[ "$preview_project_ref" != "$SUPABASE_MAIN_PROJECT_ID" ]]',
        '[[ "$preview_project_ref" != "$SUPABASE_DEV_PROJECT_ID" ]]',
        "Read back exact Preview PostgREST runtime contract",
        '"https://api.supabase.com/v1/projects/$PREVIEW_PROJECT_REF/api-keys"',
        "(.disabled // false) == false",
        '.type == "publishable"',
        '.name == "anon"',
        '.type == "legacy"',
        '(.api_key | type == "string")',
        'test("^sb_publishable_[A-Za-z0-9_-]+$")',
        'test("^eyJ[A-Za-z0-9_-]+[.][A-Za-z0-9_-]+[.][A-Za-z0-9_-]+$")',
        "unset api_keys SUPABASE_ACCESS_TOKEN",
        "PREVIEW_PUBLIC_API_KEY=%s",
        "id: preview_api_key",
        'Content-Profile: api',
        'portal_hybrid_search_v1',
        'portal.public-hybrid-candidate-page.v1.schema.json',
        "sanitized_error_code=",
        'test("^(PGRST[0-9]{3}|[0-9A-Z]{5})$")',
        "with sanitized code $sanitized_error_code",
        'assert_opaque_error 404 PGRST202',
        'assert_opaque_error 406 PGRST106',
        'p_actor_id p_team_id p_state_codes p_data_source',
        'Verify anonymous Preview sitemap shard boundary',
        'portal_sitemap_manifest_v1',
        'portal_sitemap_shard_v1',
        'portal.public-sitemap-manifest.v1.schema.json',
        'portal.public-sitemap-shard.v1.schema.json',
        'probe_deadline=$((SECONDS + 300))',
        '--connect-timeout 5',
        '--max-time "$request_timeout"',
        "steps.preview_api_key.outcome == 'success'",
        'always()',
        '(.shards | length) == 64',
        'all(.shards[]; .maxItems == 4096)',
        '.code == "22023"',
    )
    failures.extend(
        f"Preview verification missing {token}"
        for token in preview_required
        if token not in preview_workflow
    )
    if preview_workflow.count("probe_deadline=$((SECONDS + 300))") != 2:
        failures.append(
            "Hybrid and sitemap Preview readiness probes must each use one exact 300-second deadline"
        )
    if preview_workflow.count('--max-time "$request_timeout"') != 2:
        failures.append(
            "Hybrid and sitemap readiness curl calls must be bounded by their remaining deadline"
        )
    for app_identity_token in (
        ".app.id == 330661",
        '.app.slug == "supabase"',
        '.app.owner.login == "supabase"',
    ):
        if preview_workflow.count(app_identity_token) < 3:
            failures.append(
                "Preview exact-count, success, and failure checks must all bind "
                f"the official Supabase App with {app_identity_token}"
            )

    preview_forbidden = (
        "supabase link",
        "supabase db push",
        "supabase config push",
        "supabase functions",
        "SUPABASE_DB_PASSWORD",
        "supabase branches get",
        "supabase projects api-keys",
        "?reveal=true",
    )
    failures.extend(
        f"Preview verification must not contain {token}"
        for token in preview_forbidden
        if token.lower() in preview_workflow.lower()
    )

    preview_order = (
        preview_workflow.find("- name: Wait for exact Supabase Preview check"),
        preview_workflow.find("- name: Resolve exact Preview project"),
        preview_workflow.find("- name: Apply exact Preview PostgREST runtime contract"),
        preview_workflow.find("- name: Read back exact Preview PostgREST runtime contract"),
        preview_workflow.find("- name: Verify anonymous Preview Hybrid boundary"),
        preview_workflow.find("- name: Verify anonymous Preview sitemap shard boundary"),
    )
    if -1 not in preview_order and preview_order != tuple(sorted(preview_order)):
        failures.append(
            "Preview check, branch resolution, PATCH, readback, and anonymous probes are out of order"
        )

    preview_probe_marker = "- name: Verify anonymous Preview Hybrid boundary"
    preview_key_marker = "- name: Read enabled Preview public API key"
    preview_key_step = preview_workflow.split(preview_key_marker, 1)[-1].split(
        preview_probe_marker, 1
    )[0]
    preview_probe = preview_workflow.split(preview_probe_marker, 1)[-1]
    preview_job_header = preview_workflow.split("    steps:", 1)[0]
    if "secrets.SUPABASE_ACCESS_TOKEN" in preview_job_header:
        failures.append("Supabase access token must be scoped only to steps that require it")
    if "available=false" in preview_workflow or "verification skipped" in preview_workflow:
        failures.append(
            "same-repository Preview verification must fail closed when authority is missing"
        )
    for forbidden_probe_credential in (
        "Authorization:",
        "Cookie:",
        "SERVICE_ROLE",
        "service_role",
        "SUPABASE_ACCESS_TOKEN",
        "api-keys",
    ):
        if forbidden_probe_credential in preview_probe:
            failures.append(
                "anonymous Preview probes must not contain "
                f"{forbidden_probe_credential}"
            )
    for forbidden_probe_output in (
        'cat "$response_file"',
        'echo "$(<"$response_file")"',
        'echo "$(cat "$response_file")"',
    ):
        if forbidden_probe_output in preview_probe:
            failures.append(
                "anonymous Preview diagnostics must not print the raw "
                f"response body via {forbidden_probe_output}"
            )
    if preview_key_step.count(
        '"https://api.supabase.com/v1/projects/$PREVIEW_PROJECT_REF/api-keys"'
    ) != 1:
        failures.append("Preview probe must read API keys exactly once without reveal")
    if preview_key_step.count("(.disabled // false) == false") != 2:
        failures.append("Preview key selector must reject disabled publishable and anon keys")
    if preview_key_step.count('(.api_key | type == "string")') != 2:
        failures.append("Preview key selector must require concrete string public keys")
    if preview_key_step.find('.type == "publishable"') > preview_key_step.find('.name == "anon"'):
        failures.append("Preview key selector must prefer publishable over legacy anon")
    key_step_order = (
        preview_key_step.find('echo "::add-mask::$public_api_key"'),
        preview_key_step.find("PREVIEW_PUBLIC_API_KEY=%s"),
        preview_key_step.find("unset api_keys SUPABASE_ACCESS_TOKEN"),
    )
    if -1 in key_step_order or key_step_order != tuple(sorted(key_step_order)):
        failures.append("Preview public key must be masked, exported, then raw authority cleared")
    for forbidden_key_output in ('echo "$api_keys"', "?reveal=true"):
        if forbidden_key_output in preview_key_step:
            failures.append(
                f"Preview API-key read must not retain or print {forbidden_key_output}"
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
        "SUPABASE_MAIN_PROJECT_ID",
        "supabase db reset --no-seed",
        "supabase test db supabase/tests/20260827_portal_sitemap_shards_v1.sql",
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
        "PASS: Supabase Dev workflow separately gates persistent Dev and exact "
        "PR Preview runtime contracts, limits Management API mutation to two "
        "three-field PostgREST PATCH calls, and verifies exact head "
        f"{expected_head}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
