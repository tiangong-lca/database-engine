---
title: database-engine Repo Validation Guide
docType: guide
scope: repo
status: active
authoritative: false
owner: database-engine
language: en
whenToUse:
  - when a database-engine change is ready for local validation
  - when deciding the minimum proof required for migration, config, workflow, script, or docs changes
  - when writing PR validation notes for database-engine work
whenToUpdate:
  - when the repo gains a new canonical validation command or wrapper
  - when change categories require different minimum checks
  - when schema-workspace tooling or branch operations change
checkPaths:
  - docs/agents/repo-validation.md
  - .docpact/config.yaml
  - supabase/config.toml
  - supabase/migrations/**
  - supabase/tests/**
  - supabase/seed.sql
  - supabase/seeds/**
  - scripts/**
  - .github/workflows/supabase-dev.yml
  - .github/workflows/ai-doc-lint.yml
  - .githooks/pre-push
  - scripts/docpact
  - scripts/docpact-gate.sh
  - scripts/install-git-hooks.sh
lastReviewedAt: 2026-07-24
lastReviewedCommit: d748dbb2e43763e5ca217c61ea4cb136977ef0bc
lastReviewedNote: "Reviewed issue #287 after the production candidate hash backfill exceeded the platform statement timeout; first-release scope-closure changes also require bounded production-volume migration proof."
related:
  - ../../AGENTS.md
  - ../../.docpact/config.yaml
  - ./repo-architecture.md
  - ./supabase-branching.md
---

## Validation Order

1. identify the change type
2. run the minimum proof for that change type
3. add stronger proof only when the risk actually increases
4. record exact commands, SQL files, and environments in the PR

## Default Baseline

Unless the change is doc-only repo-maintenance work, the baseline local commands are:

```bash
supabase start
supabase db reset
supabase migration list
```

## Proof Matrix

| Change type | Minimum local proof | Stronger proof when risk is higher | Notes |
| --- | --- | --- | --- |
| `supabase/migrations/**` | `supabase db reset` succeeds | run the relevant SQL assertions under `supabase/tests/**`; inspect affected workspace objects if the migration was authored from workspace files | Record which migration and which SQL test files were exercised. |
| guarded atomic owner-draft FP/UG alias primitive | `supabase db reset`; run `supabase/tests/20260711_guarded_dataset_alias_batch.sql` and the legacy `supabase/tests/20260404_dataset_command_rpcs.sql` | prove both the replay-capable whole-plan function and per-dimension executor have no API grants; exercise them only through postgres-owned test wrappers; retain the exact `dataset-alias-plan.v1` envelope, ordered time plus length-time batches, 25-row/20-exchange and 27-row/39-exchange scopes, public/foreign rejection, exact factors, live closure, hashes, and all-or-nothing audit proof | This is an internal mutation primitive after protected cutover. It never publishes or changes `state_code`; direct authenticated and service-role execution remains revoked. |
| protected one-shot owner-draft FP/UG execution | `supabase db reset`; run `supabase/tests/20260715_protected_dataset_alias_execution.sql`, `supabase/tests/20260711_guarded_dataset_alias_batch.sql`, both derivative rebuild suites, and the rollback-only statement-timeout fault assertion under `supabase/tests/preview/**` | on the exact hosted Preview ref, run `node supabase/tests/preview/protected_alias_rest_e2e.mjs --expected-preview-ref <ref>` and prove real authenticated success, atomic business rollback, concurrent duplicate admission, lost response, HTTP timeout, status-only recovery, exact 52 rows/59 exchanges/55 audits plus 23-flow + 27-process child admission, 309 unrelated-exchange preservation, and zero actor-scoped fixture residue; also retain the exact authenticated preflight/gate/admit/read ACLs and service-only executor, trusted server context, canonical plan/freeze/approval binding, immutable live-gate hashes, 180-second window, owner/state fences, zero legacy RPC grants, six unchanged support snapshots, and all-terminal causal aggregate proof | Hosted fixtures must hard-fail outside the exact disposable Preview, use separate Auth actors/UUID namespaces, emit redacted evidence, and clean up only their own rows, ledgers, queue items, failures, audits, and users. If a disposable Preview lacks `project_url` and `project_secret_key`, the fixture may create only two exact scenario-scoped Vault values after ref/URL binding, must refuse every pre-existing same-name value, and must delete them by exact name, description, and decrypted value; cleanup failure revokes but retains the actor identity for recovery. Pure BAFU owner-draft data may use the released tool directly on production only after a fresh production freeze and exact human approval; Preview/Dev validate shared capability and never replay BAFU data. No retry, approval reuse, publication, `state_code` change, raw SQL fallback, broad queue cleanup, or client-supplied desired PostgreSQL hash is allowed. |
| guarded owner-draft flow/process derivative rebuild | `supabase db reset`; run `supabase/tests/20260714_guarded_dataset_derivative_rebuild.sql` and `supabase/tests/20260715_guarded_dataset_derivative_rebuild_batch.sql` plus nearby webhook, embedding queue, and dataset-command regressions | retain the single-process v1 ACL/envelope/error contract; prove flow/process snapshot, fence, staging, quarantine, dispatch, proposal, commit, and terminal parity; prove the internal admission/read helpers have empty search paths and no anon/authenticated/service-role grants; reject 0, 51, duplicate, foreign, public, non-draft, incomplete, or desired-hash-drift targets before any child/audit/quarantine effect; atomically bind 1..50 children to supplied frozen baselines and actual post-write primary snapshots; and require exact 50/50, 23-flow + 27-process live primary, committed proposal, fresh vector, terminal audit, queue/failure drain, and completed-snapshot proof before aggregate completion | The compatible public v1 admission remains one process. Protected alias execution alone may invoke the private batch primitive in its mutation transaction. The primitive has a five-second lock wait, takes stable table/id/version row locks rather than a broad table write lock, validates every target before effects, never reports replay, and never retries. Nonterminal drain/failure phases retain each target fence. `completed`, `stale`, or `failed` may release it only after request-specific work and pre-existing worker windows are drained. |
| guarded Step 3 owner-draft public-flow identity rewrite | `supabase db reset`; run `supabase/tests/20260716_guarded_dataset_flow_identity_mapping.sql`, `supabase/tests/20260716_guarded_dataset_flow_identity_mapping_v2_contract.sql`, and the two guarded derivative rebuild suites; run both `protected_flow_identity_actor_lifecycle_contract.mjs` and `protected_flow_identity_residue_readback_contract.mjs` with `node --test`, then run the transport contract; before requesting Hosted execution approval, run the exact read-only transport path with `node supabase/tests/preview/protected_flow_identity_rest_e2e.mjs --transport-preflight-only --expected-preview-ref <ref>`; only after that proof passes may an outer-frozen one-shot Hosted E2E run with `node supabase/tests/preview/protected_flow_identity_rest_e2e.mjs --expected-preview-ref <ref> --scenario-namespace <fie2e-hosted-24hex> --request-id <uuid> --private-temp-dir <absolute-0700-empty-dir> --recovery-ipc` | prove authenticated-only preflight/process/read/finalize/recovery/lookup plus zero-write-cancel ACLs; exact actor/environment/policy/artifact and actor-wide request/text/identity hash-domain non-reuse; prove each disposable actor uses outer-frozen deterministic selectors, is registered before create, verifies an acknowledged create by GET-by-ID plus exact metadata, durably checkpoints its exact ID/selectors through outer-owned IPC before sign-in or fixture mutation, recovers a lost/malformed create only by one exact filtered email/fixture/request/namespace/role match, is globally logged out and hard admin-deleted, and is absent only after GET-404 plus a fresh empty exact filtered census; require cleanup to hold the derivative coordinator lock and accept only pre-external-dispatch exact children; require a transaction-external, read-only 39-surface zero-residue census covering Auth, capture children, scope/ledger/permit/invocation rows, derivative request/proposal/permit rows, live and archived queues, failure tables, pg_net, audits, primary/support fixtures, global exact-name Vault rows, fault hooks, and fixture DB sessions; one active wrapper invocation, memory-only raw permit custody, persisted token hash/generation only, atomic success rotation, old-permit rejection, and no permit consumption on malformed/authorization failure; prove the public process/finalize surface is three-argument only, exact replay/read/lookup never returns a permit, lost-preflight-response lookup is exact and read-only, and continuation uses a fresh human-approved recovery bound to observed scope state/whole proof that supersedes the old permit; retain 1-based scope/process/rewrite ordinals with 0-based exchange indexes; source owner-draft and exact non-owner public Elementary target/support guards; quantitative reference FP/UG/reference-unit identity; server-side reconstruction that changes only five reference fields; exact collision and pending/blocker occurrence ledgers; process-atomic audit plus one-target derivative admission; cancel success only for an untouched sealed scope with exact receipt/operation/plan/scope proof and zero ledger/audit/derivative/permit progress; read-only cancel replay plus foreign/anonymous/post-primary rejection; zero approved-source residue; dynamic 1..50 causal derivative proof; failed/stale child output with exact current primary/snapshot hashes; distinct derivative-only compensation-plan lineage; and terminal finalization only after every primary/audit/protected/derivative closure and completed wrapper invocation/generation proof passes. The hosted proof additionally binds the exact non-production ref, runs an authenticated read-only database transport proof through the same selected URL, exact native Supabase CLI, exact regular nested Docker executable, and exact cached `pg_prove` image ID/RepoDigest/platform; missing or mismatched images fail before database transport and are never pulled automatically. Every DB file sets and asserts its exact application name in-connection. The proof binds the child environment and network path before any disposable actor is created, then uses real owner/foreign authenticated JWTs across capture/read/process/finalize fences, proves live-drift fail-closed, full post-primary rollback, a true two-process same-permit race with exactly one winning first mutation, immediately captures recovery selectors after each committed process, rejects a stale permit, completes the second process, and performs one guarded finalize with two exact audits/derivative children before zero-residue cleanup | Do not copy the Step 2 180-second token, three live gates, pg_net one-shot dispatch, or fixed 50/23/27 reader. Credential/ref/hash checks and a connector-side SQL query are not substitutes for the exact runner transport proof; direct and pooler URL evidence are not interchangeable. The offline actor-lifecycle and residue contracts exercise failure/recovery and readback rendering/parsing without Hosted authority; they do not replace or authorize the full Hosted run or its independent Auth/SQL zero-residue execution, which remain deferred until the exact head, outer recovery envelope, and Preview authority are frozen. The read-only transport mode creates zero actors and writes zero primary rows. The full runner repeats the same proof before actor creation, so an operator-network failure cannot leave disposable identities. The outer wrapper creates and owns the exact private temp directory, durably fsyncs every secret-free checkpoint before returning the exact IPC ACK, owns recovery after signal, crash, ambiguous create, or derivative fast-path refusal, independently proves descendant quiescence plus temp removal, and must independently repeat Auth and SQL residue readback without ever relaunching the mutation runner. If inner cleanup cannot globally log out an actor, it retains that actor and performs no hard DELETE. Outer recovery may issue exactly one service-role password reset to a fresh memory-only value, sign in only the exact actor ID, require acknowledged global logout, then hard-delete and prove GET-404 plus a fresh empty census; an ambiguous reset, failed/wrong-ID sign-in, or ambiguous/failed logout retains the actor, performs no DELETE, and cannot be retried under the same authority. The original actor password, JWTs, service key, database URL, and raw permit may never enter checkpoint, journal, evidence, or durable environment files. The Step 3 scope is durable and process-atomic. A scope read resumes from durable evidence; the exact scope lookup resolves a lost preflight response, while exact replay is proof-only and no read/replay mints or discloses a permit. Wrapper loss, ambiguous responses, deterministic domain rejection, and derivative readiness after exit require a separate fresh exact recovery approval; there is no automatic retry or approval reuse. Cancellation is an abandon path only before any primary evidence and never rolls back or releases a partially written scope. A terminal derivative failure remains `derivatives_pending`, never auto-retries or replays primary, and requires a new single-target plan/freeze/exact approval before its request-ID causal proof is eligible. Source/public/support rows, `state_code`, publication, deduplication, merging, and summing are outside this capability. The hosted fixture is one disposable 305-source/two-process universe, creates the two exact branch-local Vault values only when absent, never invokes a revoked RPC as `anon`, keeps raw permits in memory/stdin only, stores rendered credential-bearing SQL only in mode-0600 files inside the outer-owned private temporary directory whose inner and outer removal proofs are terminal, and removes only namespace-bound rows/users. Live BAFU plans/freezes remain post-Step-2/post-dependency artifacts and are never generated by database tests. |
| review-submit gate / job coordinator schema or RPCs | `supabase db reset`; run `supabase/tests/20260404_review_submit_rpc.sql`, `supabase/tests/20260529_review_submit_jobs.sql`, and the relevant `supabase/tests/*review_submit_gate*.sql` file | include owner-access, service-role result recording, worker job result mapping, stale checksum, and `cmd_review_submit` rejection/acceptance assertions | Database proof covers persisted gate runs, `worker_jobs` coordinator links, and final submit assertions. Worker report heuristics and Edge orchestration need separate repo proof. |
| `worker_jobs` lifecycle schema or RPCs | `supabase db reset`; run `supabase/tests/20260531_worker_jobs_foundation.sql` | add any job-family-specific coordinator SQL tests affected by the change | Prove claim/reclaim, lease-token fencing, idempotency, concurrency keys, status transitions, and RLS/direct-access boundaries. |
| legacy lifecycle cleanup after `worker_jobs` cutover | `supabase db reset`; run `supabase/tests/20260531_worker_jobs_foundation.sql`, `supabase/tests/20260531_worker_jobs_legacy_lifecycle_cleanup.sql`, and `supabase/tests/20260602_worker_legacy_table_retirement_audit.sql` | verify production queue drain/cutover and archive availability separately before applying the cleanup migration to `main` | The cleanup disables legacy pgmq delivery surfaces, archives `public.lca_jobs`, `public.lca_package_jobs`, and `public.dataset_review_submit_jobs` into `archive.worker_legacy_job_table_rows`, and physically retires those legacy tables with `DROP TABLE ... RESTRICT`. |
| worker-produced domain artifact/state contracts after legacy table retirement | `supabase db reset`; run `supabase/tests/20260603_worker_domain_artifact_contracts.sql`; run `python scripts/check_generated_workspace_legacy_tables.py` after refreshing generated workspace output | inspect `public.worker_domain_traceability_violations` on the target remote after deployment | Prove retained `lca_package_*`, LCA result/cache, and review-submit domain rows are documented as domain state, new post-cutover rows remain traceable to `worker_jobs`, and package retention has both dry-run and apply helpers. |
| canonical LCI/LCIA release control plane | `supabase db reset`; run `supabase/tests/20260716_lci_lcia_release_control_plane.sql` and `supabase/tests/20260623_data_product_publication_mvp.sql` | also run `supabase/tests/20260603_worker_domain_artifact_contracts.sql`, `supabase/tests/20260608_missing_fk_support_indexes.sql`, and local `supabase db lint --level warning`; use a disposable Preview for authenticated Edge/REST transport proof | Prove API/publishable key without user session is 401; ordinary session is 403; manager prepare/approve/publish/readback succeeds only for exact hashes; service can finalize four artifacts but cannot write tables, approve, or publish; retries are idempotent; every source Process has one self-mapped Unit Process plus one LifecycleModel and Result Process; generated datasets stay out of authoring tables; public Process projections expose identities but no object locator; published artifacts are pinned and immutable; unpublish removes current projection without deleting artifacts. |
| data-product scope-closure snapshot source or preflight RPC | `supabase db reset`; run `supabase/tests/20260724_initial_candidate_scope_closure_e2e.sql`, `supabase/tests/20260722_scope_closure_release_binding_e2e.sql`, and `supabase/tests/20260722_data_product_scope_closure_and_task_feed.sql` | run `supabase migration list` and `supabase db lint --level warning`; when a migration hashes or rewrites the candidate universe, prove its production-volume statement fits the platform timeout or declares a bounded session override and restores the default immediately afterward; after production deployment, submit one real zero-release completeness check and inspect Worker completion plus the downloadable report | Prove zero-release subset/global requests, exact versions, canonical LCIA locator mapping, Worker-compatible canonical hashes, deterministic snapshot/scan reuse, explicit current-membership rejection, unchanged formal-release binding, ACLs, and task-center projection. Preview data volume alone is not production-scale backfill evidence. |
| `supabase/tests/**` only | run the relevant SQL assertion files against a reset local DB | add a nearby migration or policy smoke check if the new assertions expose a gap | This repo stores PGTAP-style SQL assertions, not a single canonical runner wrapper. |
| `supabase/seed.sql` or `supabase/seeds/dev.sql` | `supabase db reset` succeeds with expected seed behavior | rerun targeted SQL assertions that depend on the seeded rows; for shared-seed changes, confirm the hosted Preview seed stage completes | Keep shared seed and dev-only seed expectations separate. A shared seed with no data must still contain an executable no-op statement; a comments-only batch is not deployment-safe. |
| `supabase/config.toml` | `supabase start` and `supabase db reset` still work locally | verify the changed branch-binding or auth assumption against `docs/agents/supabase-branching.md` | Config changes can affect preview, persistent dev, and local behavior together. |
| `.github/workflows/supabase-dev.yml` | inspect YAML changes and confirm referenced secrets and vars still exist in docs | verify the intended deploy path in a PR note because the real push occurs only on Git `dev`; for `main -> dev` reconciliation, prove an older-timestamped committed migration is installed with `--include-all` without reapplying recorded history | Local dry-run for GitHub-hosted execution is limited; document the expected remote proof. |
| `scripts/**` | run the touched script with `--help` when possible, or execute the narrowest safe non-destructive path | if a script changes generated workspace behavior, refresh the workspace in a safe environment and inspect git diff | Avoid remote-destructive script runs unless the task explicitly requires them. |
| `supabase/workspace/**` | prove whether the touched file is generated or stable | if stable manual overlay files changed, explain how they feed migration generation | Generated files alone are not sufficient evidence of a durable schema change. |
| repo docs only | `scripts/docpact lint --root . --files "<csv>" --mode enforce` | `scripts/docpact validate-config --root . --strict` when `.docpact/config.yaml` changes | Refresh review metadata even when prose stays unchanged. |

## SQL And Offline Node Contract Notes

The repo stores SQL assertions and narrow offline Node contracts under `supabase/tests/**`.

Facts that matter:

- database semantics use PGTAP-style SQL assertions
- offline Node contracts may mock transport to prove runner control flow such as deterministic selector binding, exact filtered actor recovery, global logout, hard DELETE acknowledgement, GET-404 plus empty-census absence confirmation, and fail-closed 39-surface residue rendering/parsing
- offline contracts do not replace exact-head Hosted execution or independent post-run Auth/SQL readback
- no single checked-in wrapper defines the only valid invocation
- PR validation notes should therefore record the exact command or SQL runner used

If you add or change SQL assertions:

1. reset the local DB first
2. run the relevant assertion files against that reset state
3. record the exact invocation in the PR

If you add or change an offline Node contract, run its exact file with `node --test` and record that command separately from any deferred Hosted proof.

## Schema Workspace Tooling Checks

If the task touches `scripts/**` or `supabase/workspace/**`, first decide whether you changed:

- stable documentation or overlay paths
- generated inspection output
- helper scripts that transform one into the other

Useful low-risk checks:

```bash
python scripts/build_schema_workspace.py --help
python scripts/copy_workspace_file_to_changes.py --help
python scripts/new_migration.py --help
python scripts/export_remote_schema.py --help
python scripts/check_generated_workspace_legacy_tables.py --help
```

If you actually refresh the workspace, make the validation note explicit about which generated paths changed and whether the change is intended to be committed.

## Preview, Persistent Dev, and Root Integration

For branch-oriented changes:

- preview-branch proof usually happens on the repo PR
- persistent remote `dev` proof happens after merge into Git `dev`
- production `main` proof happens after `dev -> main` promote and should confirm the Supabase GitHub integration applied migrations automatically
- root workspace proof happens later in `lca-workspace`

Do not collapse those phases into one validation note.

## Minimum PR Validation Note

Every PR note for this repo should state:

1. exact commands run
2. exact SQL assertion files or migration paths exercised
3. whether any proof is deferred to preview branch, persistent `dev`, production `main`, or root integration

## Local Docpact Push Gate

Install the versioned local hook once per checkout:

```bash
./scripts/install-git-hooks.sh
```

The `pre-push` hook runs `scripts/docpact-gate.sh`, which delegates CLI lookup to `scripts/docpact` and performs strict config validation plus enforced lint before the push leaves the machine. The wrapper checks `DOCPACT_BIN`, Cargo install locations, Homebrew install locations, and then `PATH`, so local agent shells should not fail only because bare `docpact` is unavailable. The default comparison base is `origin/dev` for routine branches and `origin/main` for promote or hotfix branches. Override it for unusual stacks with `DOCPACT_BASE_REF=<ref>` or `scripts/docpact-gate.sh --base <ref>`. The gate writes its detailed report to a temporary file so normal pushes do not create `.docpact/runs/` artifacts.
