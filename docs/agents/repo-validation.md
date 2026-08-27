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
  - when validating a Portal public DTO, RPC, capability, Hybrid search, Exchange, or LCIA projection
whenToUpdate:
  - when the repo gains a new canonical validation command or wrapper
  - when change categories require different minimum checks
  - when schema-workspace tooling or branch operations change
checkPaths:
  - docs/agents/repo-validation.md
  - .docpact/config.yaml
  - contracts/portal/**
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
lastReviewedAt: 2026-08-27
lastReviewedCommit: 712558e
lastReviewedNote: "Reviewed for Issue #539: 273-migration reset, 13-module generation, fixed 64-shard parity/capacity, bounded latest-sync cost, and anonymous Preview transport gates."
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

There is no checked-in monolithic database-contract runner. Run the relevant
pgTAP files explicitly after a clean reset, and run the change-specific shell
or Node upgrade/transport harnesses named in the proof matrix. Workflows that
need Git provenance still check out full history (`fetch-depth: 0`).

## Proof Matrix

| Change type | Minimum local proof | Stronger proof when risk is higher | Notes |
| --- | --- | --- | --- |
| `supabase/migrations/**` | `supabase db reset` succeeds | run the relevant SQL assertions under `supabase/tests/**`; inspect affected workspace objects if the migration was authored from workspace files | Record which migration and which SQL test files were exercised. |
| Auth identity/profile mirror Trigger or registered-user lookup | `supabase db reset --no-seed`; run the dedicated Auth profile synchronization pgTAP file plus the affected API contract and nearby identity-field regressions | rehearse a populated upgrade from the previous migration head with both an Auth-only identity and an existing private profile; prove backfill, metadata reconciliation, contact preservation, helper rename, and exactly one governed Trigger | `auth.users` remains authoritative. Prove insert/update/delete mirroring, an Auth-only lookup by normalized email, review-admin-only authorization, fixed empty helper `search_path`, no direct helper execution by application roles, and preserved RPC ACLs. The deferred Trigger must still synchronize before commit while remaining compatible with explicit same-transaction profile writes. |
| full application Schema cutover | `supabase db reset --no-seed`; run the `20260805`/`20260806` contract pgTAP files plus `20260807_data_product_consumer_facades.sql`; `scripts/test_full_schema_cutover_upgrade.sh` | restart local Supabase after config changes; prove explicit `public` core-table REST access, explicit `api` RPC access, rejection of `private`, and absence of the old `public` RPC route; on hosted Preview/persistent `dev`, also prove profile-less core access resolves to `public` after exact ordered Management API readback; repeat the populated upgrade against a production-shaped disposable copy before hosted deployment | Require the exact nine-table public allowlist, zero public routines/views/sequences, complete target placement, OID and exact-row preservation, trigger/policy/constraint parity, denial of internal Data API exposure, exact equality between external API grants and `private.api_capability_grants`, the exact authenticated `private.roles`/`private.reviews` read-only exception required by core-table RLS, bounded Data Product projections, stable TIDAS DTO/error behavior, a service-only boolean lifecycle review-admin predicate, and real traversal through each constrained façade family. When production has already recorded a later main hotfix, the populated upgrade must reproduce that ledger order, prove any older-version bridge is independently service-valid, and prove the same bridge is a strict no-op after cutover. Local Supabase may normalize custom schemas ahead of `public`, so local profile-less behavior is not hosted default-profile proof. Do not add compatibility views, duplicate tables, or dual-write paths. |
| Root/Reference Review schema, commands, or queue projections | `supabase db reset --no-seed`; run `supabase/tests/20260731_root_reference_review_v2.sql`, `20260802_root_grouped_review_queue.sql`, `20260809_rejected_reference_resubmission.sql`, `20260809_review_metadata_scope_basis.sql`, `20260812_reference_approval_scope_isolation.sql`, and `20260806_api_contract_closure.sql` | compare all six Admin/Member tabs on a production-shaped disposable copy; capture a bounded `EXPLAIN (ANALYZE, BUFFERS)` for representative queue cardinalities before hosted cutover | Review submission has no numerical-stability or upstream-completeness Gate. Prove seven-table state transitions, parallel active versions for the same table/id with exact version/checksum isolation, active/approved checksum reuse, unchanged rejected-revision re-entry, current JSON/submitted-Comment derivation, stale candidate rejection, Reviewer metadata draft/submission addition and revocation, isolated impacted-root lookup and Reference approval when unrelated Roots are incomplete, one-row-per-Root-or-Reference flat pagination/counts, and exact external grants. The v3 queues must apply `all`/model-process/other display modes and exact target-table filters before totals, ordering, and pagination; combined filters use intersection semantics, page size defaults to 50, and unsupported values fail closed. `private.reviews` must not contain scope snapshots or Root/Reference ID arrays; business `reviews[].id` contains Root candidates only; no rebind or history replay contract remains. Comment draft saves (`state_code = 0`) must persist only the editable draft and must not create Reference Reviews or append candidate Root hints; formal Comment submission must create or reuse Reference Reviews and append candidate Root hints for unpublished targets. The retained grouped v2 compatibility queues and child relationship query continue deriving only direct or candidate-hinted Roots; the v3 main queues never derive parent membership. |
| process/flow hybrid-search query shape, HNSW settings, or vector indexes | `supabase db reset`; run `supabase/tests/20260727_hybrid_search_staging_plan_governance.sql`, `supabase/tests/20260528_semantic_hybrid_candidate_helpers.sql`, and `supabase/tests/20260528_hybrid_semantic_candidate_limit_hotfix.sql` | after the migration reaches persistent `dev`, run the exact read-only `supabase/tests/benchmarks/20260727_hybrid_search_staging_explain.sql` profile against ref `submidrhbtknjxfympna`; compare process, broad-flow, Product-flow, and full-RPC timings/buffers with Issue #292; rerun performance and security advisors | Use the benchmark's checked-in Edge route lexical parameter profile with its redacted real staging vectors; never synthesize lexical terms from the first Markdown tokens because shared headings create an artificial near-whole-table match. Do not use a seed-only Preview as production-cardinality proof, expose raw text/UUID/vector values, or run production `EXPLAIN ANALYZE`. Empty JSON filters must fold away under a custom plan, filtered HNSW must retain strict-order iterative scans, and semantic candidate growth remains one 10x bound with a 200-row floor. |
| Contact, FlowProperty, Source, or UnitGroup Semantic/Hybrid pipeline, RPC, or HNSW index | `supabase db reset`; run `supabase/tests/20260727_foundation_dataset_semantic_hybrid_search.sql`, `supabase/tests/20260514_embedding_derivative_trigger_scope.sql`, `supabase/tests/20260727_hybrid_search_staging_plan_governance.sql`, `supabase/tests/20260528_semantic_hybrid_candidate_helpers.sql`, and `supabase/tests/20260528_hybrid_semantic_candidate_limit_hotfix.sql` | on the exact Preview branch, run bounded cursor backfill pages and drain both extraction and embedding queues; prove zero terminal failures and full eligible-row derivative counts; smoke `tg`/`co` explicit-team scope, `my` explicit-state scope, `te` explicit-readable-team plus state scope, and missing/foreign-team fail-closed behavior; after merge to persistent `dev`, capture redacted real-parameter `EXPLAIN (ANALYZE, BUFFERS)` for Semantic and Hybrid queries across all four datasets, compare timings/buffers, then rerun performance and security advisors | Never do a synchronous whole-table migration rewrite. Backfill is service-only, capped at 500 rows, idempotent, and uses compact queue payloads. A blank-version row is ineligible because it has no stable worker identity: count it as a data-quality exception and never guess a version or enqueue it. Helpers require exact table/RPC allowlists, `tg`/`co`/`my`/`te` visibility, PGroonga escaping, custom plans, and strict-order HNSW scans; derivative writes must not change authored `modified_at`. Team Data requires an explicit team readable by the authenticated actor. A small table may correctly choose a sequential scan. Do not run production `EXPLAIN ANALYZE` or retain raw vectors, search text, IDs, or private row values in evidence. |
| Database A `search_text` expand, canonical Search RPCs, or bounded replay enqueue | `supabase db reset --no-seed`; run `scripts/test_search_text_array_upgrade.sh`, `supabase/tests/20260810_search_text_expand_and_canonical_search_rpcs.sql`, `supabase/tests/20260810_search_text_array_and_review_guard.sql`, `supabase/tests/20260731_flow_identity_derivative_fence.sql`, `supabase/tests/20260729_extracted_md_lexical_search_contract_v2.sql`, `supabase/tests/20260613_hybrid_search_query_terms_escape.sql`, and `supabase/tests/20260806_api_contract_closure.sql`; run `python scripts/build_schema_workspace.py --environment local --schemas public api private util archive` and `python scripts/build_database_types.py --environment local`, then require `git diff --exit-code -- supabase/workspace` | on the exact Preview branch, confirm the migration applies and the generated-workspace/type-drift CI gate is green; record any service-role ACL change in the Issue and PR | Database A keeps all seven `search_text` columns nullable `text[]` values with no default. The production upgrade requires the additive scalar columns to remain empty and dependency-free, then replaces them without rewriting the seven table heaps; it fails before changing any column when that contract has drifted. The later backfill writes the complete projection. State 20/100 rows permit only `extracted_md`, `search_text`, `embedding_ft`, and `embedding_ft_at` outside the review-controlled command context, while derived updates do not advance authored `modified_at`; existing anon/authenticated table-write revocations remain the boundary. Flow treats the same four fields as guard-neutral, and the canonical RPCs have exact no-overload signatures with manifest-matched ACLs. Formal hybrid filters are `jsonb`; formal `page_size` and `page_current` are `integer`; text/bigint conversion stays at the existing private boundary. Database A provides only bounded enqueue: historical backfill is the workspace#565-coordinated Release 1 operational gate after Edge double-write deployment and is never run by this migration. Database B owns the index/source switch, Database C old-index cleanup, and Database D old-RPC cleanup. |
| Database B `search_text` lexical source switch or PGroonga cutover gate | `supabase db reset`; run `supabase/tests/20260811_search_text_source_switch.sql` plus the adjacent Search/Hybrid regressions; verify all seven new indexes are valid, ready, live, single-column, explicitly opclassed, and configured for TokenBigram/NormalizerAuto; prove the array-boundary and newline counterexamples, multilingual and UUID terms, grant-free gate behavior, and a selective 20,000-row `EXPLAIN (ANALYZE, BUFFERS)` that names the new index; regenerate `supabase/workspace` and `database.types.ts` from the exact local state and prove deterministic no-drift | before any persistent-Dev migration application, deploy Edge array writes, prove 100% eligible-row non-NULL coverage and zero terminal derivative failures, and run remote catalog plus read-only smoke checks; only then may the source switch be considered for cutover | The migration changes only the lexical source from `extracted_md` to `search_text` and preserves permissions, visibility, filters, ordering, pagination, ranking, UUID fast paths, wrappers, and old extracted_md indexes/RPCs. The gate distinguishes an empty fresh database (allowed) from an existing database with any missing projection (SQLSTATE `55006`, blocked). Do not claim performance from `enable_seqscan = off` or a tiny table, and do not use array position as ranking weight. |
| Process Search actor-draft or Hybrid/Search lexical contract | `supabase db reset`; run `supabase/tests/20260730_extracted_text_and_embedding_flag_retirement.sql`, `supabase/tests/20260613_hybrid_search_query_terms_escape.sql`, `supabase/tests/20260727_foundation_dataset_semantic_hybrid_search.sql`, and `supabase/tests/20260729_extracted_md_lexical_search_contract_v2.sql` plus the nearby semantic/HNSW and guarded-rebuild regressions | on the exact Preview branch, smoke all seven v2 Hybrid RPCs and `search_processes_latest_v2`; for `owner_draft_only`, cover actor-owned state-zero rows with null and non-null team/review workflow metadata and prove non-owner/non-zero rows remain excluded | `owner_draft_only` is an actor-plus-state calculation filter: `team_id` and `review_id` are workflow metadata, not ownership gates. Search uses the current `search_text` PGroonga source. A PGroonga index may still appear as `unused_index` with `idx_scan = 0` after use; require a direct `EXPLAIN (ANALYZE, BUFFERS)` naming the index before classifying or dropping it. |
| embedding queue selector or per-scope backpressure | `supabase db reset`; run `supabase/tests/20260728_embedding_queue_selector_plan_governance.sql` and `supabase/tests/20260525_embedding_backpressure_and_deferred_queue.sql` | on persistent `dev`, capture a redacted real-queue `EXPLAIN (ANALYZE, BUFFERS)` before and after the append-only migration; prove the visible queue is scanned once, policy is resolved once per distinct scope, active counts are aggregated once per scope, terminal failures stay zero, and the queue drains under the checked-in policy defaults | Never correlate a full active-queue count with every visible job. Preserve retry ordering, stale-job cleanup, per-scope admission, and the default `max_in_flight` values. Any temporary staging-only policy ramp must be bounded, monitored for HTTP/queue failures, and restored before handoff; do not use higher concurrency to hide selector cost. |
| Performance Advisor index or table-bloat governance | `supabase db reset`; run `supabase/tests/20260608_missing_fk_support_indexes.sql` and any affected domain regression | run `supabase db lint --level warning`; compare the relevant Performance Advisor notices on Preview and persistent `dev`; for physical compaction, follow the hooks maintenance-window procedure below | An unused-index notice alone is not permission to drop HNSW, GIN, PGroonga, FK-supporting, or other workload-sensitive indexes. `VACUUM FULL` is never part of the automatic migration path. |
| guarded atomic owner-draft FP/UG alias primitive | `supabase db reset`; run `supabase/tests/20260711_guarded_dataset_alias_batch.sql` and the legacy `supabase/tests/20260404_dataset_command_rpcs.sql` | prove both the replay-capable whole-plan function and per-dimension executor have no API grants; exercise them only through postgres-owned test wrappers; retain the exact `dataset-alias-plan.v1` envelope, ordered time plus length-time batches, 25-row/20-exchange and 27-row/39-exchange scopes, public/foreign rejection, exact factors, live closure, hashes, and all-or-nothing audit proof | This is an internal mutation primitive after protected cutover. It never publishes or changes `state_code`; direct authenticated and service-role execution remains revoked. |
| protected one-shot owner-draft FP/UG execution | `supabase db reset`; run `supabase/tests/20260715_protected_dataset_alias_execution.sql`, `supabase/tests/20260711_guarded_dataset_alias_batch.sql`, both derivative rebuild suites, and the rollback-only statement-timeout fault assertion under `supabase/tests/preview/**` | on the exact hosted Preview ref, run `node supabase/tests/preview/protected_alias_rest_e2e.mjs --expected-preview-ref <ref>` and prove real authenticated success, atomic business rollback, concurrent duplicate admission, lost response, HTTP timeout, status-only recovery, exact 52 rows/59 exchanges/55 audits plus 23-flow + 27-process child admission, 309 unrelated-exchange preservation, and zero actor-scoped fixture residue; also retain the exact authenticated preflight/gate/admit/read ACLs and service-only executor, trusted server context, canonical plan/freeze/approval binding, immutable live-gate hashes, 180-second window, owner/state fences, zero legacy RPC grants, six unchanged support snapshots, and all-terminal causal aggregate proof | Hosted fixtures must hard-fail outside the exact disposable Preview, use separate Auth actors/UUID namespaces, emit redacted evidence, and clean up only their own rows, ledgers, queue items, failures, audits, and users. If a disposable Preview lacks `project_url` and `project_secret_key`, the fixture may create only two exact scenario-scoped Vault values after ref/URL binding, must refuse every pre-existing same-name value, and must delete them by exact name, description, and decrypted value; cleanup failure revokes but retains the actor identity for recovery. Pure BAFU owner-draft data may use the released tool directly on production only after a fresh production freeze and exact human approval; Preview/Dev validate shared capability and never replay BAFU data. No retry, approval reuse, publication, `state_code` change, raw SQL fallback, broad queue cleanup, or client-supplied desired PostgreSQL hash is allowed. |
| guarded owner-draft flow/process derivative rebuild | `supabase db reset`; run `supabase/tests/20260714_guarded_dataset_derivative_rebuild.sql` and `supabase/tests/20260715_guarded_dataset_derivative_rebuild_batch.sql` plus nearby webhook, embedding queue, and dataset-command regressions | retain the single-process v1 ACL/envelope/error contract; prove flow/process snapshot, fence, staging, quarantine, dispatch, proposal, commit, and terminal parity; prove the internal admission/read helpers have empty search paths and no anon/authenticated/service-role grants; reject 0, 51, duplicate, foreign, public, non-draft, incomplete, or desired-hash-drift targets before any child/audit/quarantine effect; atomically bind 1..50 children to supplied frozen baselines and actual post-write primary snapshots; and require exact 50/50, 23-flow + 27-process live primary, committed proposal, fresh vector, terminal audit, queue/failure drain, and completed-snapshot proof before aggregate completion | The compatible public v1 admission remains one process. Protected alias execution alone may invoke the private batch primitive in its mutation transaction. The primitive has a five-second lock wait, takes stable table/id/version row locks rather than a broad table write lock, validates every target before effects, never reports replay, and never retries. Nonterminal drain/failure phases retain each target fence. `completed`, `stale`, or `failed` may release it only after request-specific work and pre-existing worker windows are drained. |
| guarded Step 3 owner-draft public-flow identity rewrite | `supabase db reset`; run `supabase/tests/20260716_guarded_dataset_flow_identity_mapping.sql`, `supabase/tests/20260716_guarded_dataset_flow_identity_mapping_v2_contract.sql`, and the two guarded derivative rebuild suites; run both `protected_flow_identity_actor_lifecycle_contract.mjs` and `protected_flow_identity_residue_readback_contract.mjs` with `node --test`, then run the transport contract; before requesting Hosted execution approval, run the exact read-only transport path with `node supabase/tests/preview/protected_flow_identity_rest_e2e.mjs --transport-preflight-only --expected-preview-ref <ref>`; only after that proof passes may an outer-frozen one-shot Hosted E2E run with `node supabase/tests/preview/protected_flow_identity_rest_e2e.mjs --expected-preview-ref <ref> --scenario-namespace <fie2e-hosted-24hex> --request-id <uuid> --private-temp-dir <absolute-0700-empty-dir> --recovery-ipc` | prove authenticated-only preflight/process/read/finalize/recovery/lookup plus zero-write-cancel ACLs; exact actor/environment/policy/artifact and actor-wide request/text/identity hash-domain non-reuse; prove each disposable actor uses outer-frozen deterministic selectors, is registered before create, verifies an acknowledged create by GET-by-ID plus exact metadata, durably checkpoints its exact ID/selectors through outer-owned IPC before sign-in or fixture mutation, recovers a lost/malformed create only by one exact filtered email/fixture/request/namespace/role match, is globally logged out and hard admin-deleted, and is absent only after GET-404 plus a fresh empty exact filtered census; require cleanup to hold the derivative coordinator lock and accept only pre-external-dispatch exact children; require a transaction-external, read-only 39-surface zero-residue census covering Auth, capture children, scope/ledger/permit/invocation rows, derivative request/proposal/permit rows, live and archived queues, failure tables, pg_net, audits, primary/support fixtures, global exact-name Vault rows, fault hooks, and fixture DB sessions; one active wrapper invocation, memory-only raw permit custody, persisted token hash/generation only, atomic success rotation, old-permit rejection, and no permit consumption on malformed/authorization failure; prove the public process/finalize surface is three-argument only, exact replay/read/lookup never returns a permit, lost-preflight-response lookup is exact and read-only, and continuation uses a fresh human-approved recovery bound to observed scope state/whole proof that supersedes the old permit; retain 1-based scope/process/rewrite ordinals with 0-based exchange indexes; source owner-draft and exact non-owner public Elementary target/support guards; quantitative reference FP/UG/reference-unit identity; server-side reconstruction that changes only five reference fields; exact collision and pending/blocker occurrence ledgers; process-atomic audit plus one-target derivative admission; cancel success only for an untouched sealed scope with exact receipt/operation/plan/scope proof and zero ledger/audit/derivative/permit progress; read-only cancel replay plus foreign/anonymous/post-primary rejection; zero approved-source residue; dynamic 1..50 causal derivative proof; failed/stale child output with exact current primary/snapshot hashes; distinct derivative-only compensation-plan lineage; and terminal finalization only after every primary/audit/protected/derivative closure and completed wrapper invocation/generation proof passes. The hosted proof additionally binds the exact non-production ref, runs an authenticated read-only database transport proof through the same selected URL, exact native Supabase CLI, exact regular nested Docker executable, and exact cached `pg_prove` image ID/RepoDigest/platform; missing or mismatched images fail before database transport and are never pulled automatically. Every DB file sets and asserts its exact application name in-connection. The proof binds the child environment and network path before any disposable actor is created, then uses real owner/foreign authenticated JWTs across capture/read/process/finalize fences, proves live-drift fail-closed, full post-primary rollback, a true two-process same-permit race with exactly one winning first mutation, immediately captures recovery selectors after each committed process, rejects a stale permit, completes the second process, and performs one guarded finalize with two exact audits/derivative children before zero-residue cleanup | Do not copy the Step 2 180-second token, three live gates, pg_net one-shot dispatch, or fixed 50/23/27 reader. Credential/ref/hash checks and a connector-side SQL query are not substitutes for the exact runner transport proof; direct and pooler URL evidence are not interchangeable. The offline actor-lifecycle and residue contracts exercise failure/recovery and readback rendering/parsing without Hosted authority; they do not replace or authorize the full Hosted run or its independent Auth/SQL zero-residue execution, which remain deferred until the exact head, outer recovery envelope, and Preview authority are frozen. The read-only transport mode creates zero actors and writes zero primary rows. The full runner repeats the same proof before actor creation, so an operator-network failure cannot leave disposable identities. The outer wrapper creates and owns the exact private temp directory, durably fsyncs every secret-free checkpoint before returning the exact IPC ACK, owns recovery after signal, crash, ambiguous create, or derivative fast-path refusal, independently proves descendant quiescence plus temp removal, and must independently repeat Auth and SQL residue readback without ever relaunching the mutation runner. If inner cleanup cannot globally log out an actor, it retains that actor and performs no hard DELETE. Outer recovery may issue exactly one service-role password reset to a fresh memory-only value, sign in only the exact actor ID, require acknowledged global logout, then hard-delete and prove GET-404 plus a fresh empty census; an ambiguous reset, failed/wrong-ID sign-in, or ambiguous/failed logout retains the actor, performs no DELETE, and cannot be retried under the same authority. The original actor password, JWTs, service key, database URL, and raw permit may never enter checkpoint, journal, evidence, or durable environment files. The Step 3 scope is durable and process-atomic. A scope read resumes from durable evidence; the exact scope lookup resolves a lost preflight response, while exact replay is proof-only and no read/replay mints or discloses a permit. Wrapper loss, ambiguous responses, deterministic domain rejection, and derivative readiness after exit require a separate fresh exact recovery approval; there is no automatic retry or approval reuse. Cancellation is an abandon path only before any primary evidence and never rolls back or releases a partially written scope. A terminal derivative failure remains `derivatives_pending`, never auto-retries or replays primary, and requires a new single-target plan/freeze/exact approval before its request-ID causal proof is eligible. Source/public/support rows, `state_code`, publication, deduplication, merging, and summing are outside this capability. The hosted fixture is one disposable 305-source/two-process universe, creates the two exact branch-local Vault values only when absent, never invokes a revoked RPC as `anon`, keeps raw permits in memory/stdin only, stores rendered credential-bearing SQL only in mode-0600 files inside the outer-owned private temporary directory whose inner and outer removal proofs are terminal, and removes only namespace-bound rows/users. Live BAFU plans/freezes remain post-Step-2/post-dependency artifacts and are never generated by database tests. |
| approve or direct-publish lifecycle closure | `supabase db reset`; run `supabase/tests/20260728_review_publication_path_roles.sql`, `supabase/tests/20260405_review_workflow_rpcs.sql`, and `supabase/tests/20260404_dataset_command_rpcs.sql` | inspect the affected function ACLs and fixed search paths; retain lock-order evidence for Process/LifecycleModel composition changes; run `supabase db lint --level warning` and distinguish existing extension/dynamic-temp-table findings from new warnings | Prove Lineage is read-only; RequiredSupport approve/publish linkage remains active; ILCD `processInstance` is authoritative for source Process dependencies; `public.processes.model_id` plus exact version is authoritative for generated model-result membership; stale, missing, or malformed `json_tg` does not change review closure; private and missing composition dependencies share one safe public error shape; unknown Process/Flow paths fail closed; and approve/direct-publish operations write no dataset, review, comment, or audit state after a failed assertion. These whole-graph assertions do not run during `cmd_review_submit`. A same-id/version paired row is a dependency only when it exists. |
| review submission or Review Admin quality-diagnostic schema/RPCs | `supabase db reset --no-seed`; run `supabase/tests/20260404_review_submit_rpc.sql`, `20260813_review_quality_diagnostic.sql`, `20260819_retire_review_submit_coordinator.sql`, and `20260806_api_contract_closure.sql` | include owner/state/transaction admission, exact role partition, diagnostic active-run reuse, fresh post-terminal execution, service-role claim/result recording, report readback, review-state isolation, and retired coordinator surface assertions | `api.cmd_review_submit(text, uuid, text, jsonb)` is the only review-submission command and never enqueues or waits for a Worker quality result. Only Review Admin may manually start and read the combined completeness/numerical diagnostic. `clear`, `findings`, `not_evaluable`, and execution failure are informational and must not block assignment, approval, or rejection. Legacy request, Gate, and Worker rows remain audit history only; their RPCs, triggers, cron, and production runners are retired. Worker diagnostics and Edge orchestration require separate repository proof. |
| `worker_jobs` lifecycle schema or RPCs | `supabase db reset`; run `supabase/tests/20260531_worker_jobs_foundation.sql` | add any job-family-specific coordinator SQL tests affected by the change | Prove claim/reclaim, lease-token fencing, idempotency, concurrency keys, status transitions, and RLS/direct-access boundaries. A recorded `completed` result must atomically normalize both the job and terminal event to progress `1`; blocked and failed jobs retain their actual progress. |
| AI worker queue or suggestion service facade | `supabase db reset`; run `supabase/tests/20260825_ai_worker_contract.sql` and `supabase/tests/20260531_worker_jobs_foundation.sql`; run `supabase db lint --level warning` | on disposable Preview/Dev, call enqueue/read through Edge with two real authenticated actors, then run the matching Worker against `worker_queue=ai` and prove claim/heartbeat/complete-partial-failed readback without Process/Flow row mutation | Preserve service-only ACLs, exact requester scoping, v1 payload/result schemas, active-request idempotency, the absolute 16 MiB DB cap, lease fencing, hidden payload/internal diagnostics, and the generic `ai` queue identity. Provider calls and AI result quality belong to Worker/Edge proof, not SQL tests. |
| legacy lifecycle cleanup after `worker_jobs` cutover | `supabase db reset`; run `supabase/tests/20260531_worker_jobs_foundation.sql`, `supabase/tests/20260531_worker_jobs_legacy_lifecycle_cleanup.sql`, and `supabase/tests/20260602_worker_legacy_table_retirement_audit.sql` | verify production queue drain/cutover and archive availability separately before applying the cleanup migration to `main` | The cleanup disables legacy pgmq delivery surfaces, archives `public.lca_jobs`, `public.lca_package_jobs`, and `public.dataset_review_submit_jobs` into `archive.worker_legacy_job_table_rows`, and physically retires those legacy tables with `DROP TABLE ... RESTRICT`. |
| worker-produced domain artifact/state contracts after legacy table retirement | `supabase db reset`; run `supabase/tests/20260603_worker_domain_artifact_contracts.sql`; run `python scripts/check_generated_workspace_legacy_tables.py` after refreshing generated workspace output | inspect `util.worker_domain_traceability_violations` on the target remote after deployment | Prove retained `lca_package_*`, LCA result/cache, and legacy review-submit diagnostic rows are documented as domain or audit history rather than lifecycle authority, new post-cutover rows remain traceable to `private.worker_jobs`, and package retention has both dry-run and apply helpers. |
| TIDAS package export request-cache or artifact read lifecycle | `supabase db reset`; run `supabase/tests/20260806_lca_package_capability_facades.sql`, `supabase/tests/20260810_issue_448_package_ownership_and_attempt_diagnostics.sql`, and `supabase/tests/20260810_issue_455_mutable_export_terminal_refresh.sql` | on the exact Preview/Dev revision, create a `current_user` package, mutate the actor-owned dataset, submit again, and verify fresh package/Worker identities plus current ZIP contents | Preserve advisory-lock and active-job idempotency for mutable scopes while refusing terminal artifact reuse; retain terminal cache hits for the same exact versioned `selected_roots`. Owner authorization stays canonical-job/cache based, and current diagnostics remain distinct from event history. |
| canonical LCI/LCIA release control plane | `supabase db reset`; run `supabase/tests/20260716_lci_lcia_release_control_plane.sql` and `supabase/tests/20260623_data_product_publication_mvp.sql` | also run `supabase/tests/20260603_worker_domain_artifact_contracts.sql`, `supabase/tests/20260608_missing_fk_support_indexes.sql`, and local `supabase db lint --level warning`; use a disposable Preview for authenticated Edge/REST transport proof | Prove API/publishable key without user session is 401; ordinary session is 403; manager prepare/approve/publish/readback succeeds only for exact hashes; service can finalize four artifacts but cannot write tables, approve, or publish; retries are idempotent; Calculation Bundle projection accepts legacy zero-download packages but requires new semantic sets to contain exactly five unique role/group/filename/media-type bindings with valid hash/size/count refs and returns them separately as `productDownloads`; every source Process has one self-mapped Unit Process plus one LifecycleModel and Result Process; generated datasets stay out of authoring tables; public Process projections expose identities but no object locator; published artifacts are pinned and immutable; unpublish removes current projection without deleting artifacts. |
| Portal public catalog, capability, Hybrid search, Exchange, or LCIA numeric projection | on a uniquely named isolated local project, complete a blank `supabase db reset`; run every `supabase/tests/*portal_*.sql` file plus `20260806_api_contract_closure.sql`, `20260623_data_product_publication_mvp.sql`, and `20260716_lci_lcia_release_control_plane.sql`; compile every `contracts/portal/*.schema.json` in strict Draft 2020-12 mode; run `python3 scripts/build_portal_contract_types.py` twice and `python3 scripts/build_portal_contract_types.py --check`; run the immutable projection-manifest static check and `scripts/test_portal_projection_upgrade_recovery.sh`; filter `supabase db lint --level warning` to the Portal surface and require zero findings; regenerate the five-schema workspace and public/api TypeScript types twice with no drift | run the exact clean local `release`, `sparse-zero`, and `sparse-199` benchmark profiles, including Process/Flow empty and filtered Facets plans, zero spill on the narrow empty path, 12-term Hybrid, broad/selective filters, `name_asc`, cursor page two, raw-ANN branch evidence, full-cardinality direct exact-helper plans finishing within 5 seconds, formal ANN-plus-exact phases within 6 seconds, parseable shared buffers below 750,000 total/250,000 read blocks, p95 and sub-8-second limits; on exact Preview, probe the explicit `api` profile with a publishable key and no user JWT, including the exact Portal Hybrid PostgREST body and strict response schema; run a real matching Worker V3 job through begin/batch/status/seal/package-ready, simulate reclaim with a new job lease and job-level package readback, then run the Release package-publish/finalize/visibility-readback workflow; after persistent Dev merge, capture bounded read-only `EXPLAIN (ANALYZE, BUFFERS)` plus security/performance advisors for representative production-cardinality catalog, Facets, Hybrid Process/Flow, exchange, and LCIA modes before adding indexes | Prove fixed 100/200 scope, 0/20 opacity, forged actor/team/state/data_source rejection, exact-version identity, one stable keyset order/cursor, recursive internal/locator/credential absence, fail-closed license capabilities, canonical decimal strings, exact Exchange support chains, 500-record/1-MiB lease-fenced projection batches, positional Process/Method axes, and dense grid/hash reconciliation. Portal JSON Schemas remain the only authored TypeScript DTO source; generated modules are exact-pinned, committed output and CI must reject missing, changed, or unexpected files. Portal Search/Facets/Hybrid must bind every row to the immutable v1 projection registry and live transitive manifest. Empty/unfiltered Facets additionally bind the literal narrow-facet manifest, preserve exact JSON equality with the retained card implementation, and use the narrow path only for normalized `query=''` plus `filters={}`; every query or filter remains on the unchanged card fallback. Any current v1 helper drift fails closed, and future card semantics require a shadow v2 expand/backfill/reconcile/index/cutover rather than in-place rebuild. The supported safety model excludes out-of-band privileged/dynamic DDL: if change/write/restore may have occurred, current digest parity is not historical row provenance and v1 must remain operationally disabled until shadow-v2 recovery. Hybrid must additionally prove one to twelve normalized bounded unique terms, one exact finite 1024-dimensional embedding, strict normalized filters, fixed 1..20 limit, exact latest-visible identity with zero false positives, filled-pool ANN recall@20/@200 at least 0.95, exact underfill parity, deterministic `portal-hybrid-rank-v1` threshold/RRF/tie-breaks over the retrieved pool, actual-only lexical/semantic evidence including the zero-query-vector lexical-only case, a normalized 0..1 score, a 512-KiB response cap, authoritative LCIA decoration, and byte/owner/config/ACL-stable legacy raw Hybrid routines without calling them. Exact helpers must remain API-internal, single-worker, JIT-off, hash-join-on, nested/merge-join-off, and bounded by a 32-MB per-node workspace; caller settings restore through function proconfig. Package readiness must validate authoritative job/result/latest refs, packageResultHash, projection hashes, and deterministic default Impact before mutation. A deliberately late post-insert validation failure must roll back the package, trigger side effects, Worker resultRef, and temporary request-v2 schema switch before returning stable `projection_package_binding_invalid`; exact committed replay alone returns the strict locator-free receipt with `reused=true`, creates no duplicate, and conflicting replay stays `package_conflict`. A reclaimed Worker must recover the same receipt through the current V3 job lease while resolving the manifest-bound old prepared projection without requiring its old lease; no package returns stable `projection_package_not_found`, and package/result/projection drift remains conflict with zero package/projection mutation. Package publish prepare and command plus projection prepare and finalize must invoke the same authoritative binding validator, keep their retained implementations private/owner-only, and create no publication, pin, or projection binding when authoritative result evidence drifts. Also prove exact `publishPlanHash` confirmation and response-loss retry, immutable package/projection/publication binding, and manager readback parity with anonymous `isPubliclyVisible`. Search, Detail, Versions, and Hybrid must reuse that same current finalized non-revoked predicate: exact open projected Processes expose `lciaVisible=true`, Detail adds locator-free publication/Method context, while Flow, state 200, unrelated, unfinalized, unpublished, or revoked evidence remains false/null. Preserve item order/cursor, prove revocation synchronously and within 60 seconds, and measure the bounded 50-item decorator path before any index decision. Also prove supersede/unpublish/revoke fences, unavailable-not-zero semantics, exact manifest/ACL parity, NOLOGIN/NOBYPASSRLS executor, and byte/owner/config/ACL-stable legacy functions. Raw tables gain no anon policy and private artifacts remain private. Never reset or stop the shared `project_id=database-engine` stack for isolated proof. |
| data-product scope-closure snapshot source or preflight RPC | `supabase db reset`; run `supabase/tests/20260801_scope_closure_cutoff_main_hotfix.sql`, `supabase/tests/20260724_initial_candidate_scope_closure_e2e.sql`, `supabase/tests/20260722_scope_closure_release_binding_e2e.sql`, and `supabase/tests/20260722_data_product_scope_closure_and_task_feed.sql` | run `supabase migration list` and `supabase db lint --level warning`; when a migration hashes or rewrites the candidate universe, prove its production-volume statement fits the platform timeout or declares a bounded session override and restores the default immediately afterward; for an interactive timeout exception, assert the exact RPC `pg_proc.proconfig`, confirm role-level query budgets are unchanged, and run a rollback-only production-volume request; after production deployment, submit one real zero-release completeness check and inspect Worker completion plus the downloadable report | Prove zero-release subset/global requests, exact versions, canonical LCIA locator mapping, Worker-compatible canonical hashes, deterministic snapshot/scan reuse, explicit current-membership rejection, unchanged formal-release binding, ACLs, and task-center projection. A complete blocked source must reuse and finalize without a source snapshot even though its execution owns a preallocated snapshot UUID; passed reuse must retain exact snapshot equality. A scanner revision change must produce a distinct request fingerprint and scan execution. Boundary normalization must prove omitted, `closed`, `open`, and `cutoff` inputs all freeze canonical `cutoff` and produce one canonical requested-scope hash/fingerprint input, while unknown values fail. Preview data volume alone is not production-scale backfill evidence. |
| scope-closure artifact publication, certificate expiry, download, or GC lifecycle | `scripts/test_scope_closure_base_to_head_upgrade.sh`; `supabase db reset`; run `scripts/test_scope_closure_staged_write_set_v2_fixture.sh`, `node supabase/tests/20260730_scope_closure_staged_write_set_v2_rest_contract.mjs`, `supabase/tests/20260730_scope_closure_staged_write_set_v2.sql`, `supabase/tests/20260729_scope_closure_publication_staging_and_gc_control.sql`, `supabase/tests/20260729_scope_closure_owner_artifact_projection.sql`, `supabase/tests/20260729_scope_closure_artifact_retention.sql`, `supabase/tests/20260722_scope_closure_release_binding_e2e.sql`, `supabase/tests/20260722_data_product_scope_closure_and_task_feed.sql`, `supabase/tests/20260531_worker_jobs_foundation.sql`, and `supabase/tests/20260608_missing_fk_support_indexes.sql` | run `supabase migration list` and `supabase db lint --level warning`; on the exact Preview branch, confirm the additive migration appears in remote history, prove both public selectors and the temporary one-argument XLSX overload through actor RPC transport, then exercise DB-first staging and Worker GC RPC transport when the matching Worker consumer is available | Prove the shared Worker/database fixture has one exact canonical digest and request/response shape. Exercise 0/1/500/501/596/1500+ descriptors, multiple batch sizes, duplicate/reordered/missing/out-of-range ordinals, wrong count/digest, role/locator conflict, stale/foreign fences, exact and conflicting replay, ambiguous-response status, concurrent register/seal, rollback, post-seal mutation, and one-shot compatibility. For 596 and at least 1500 descriptors, record request count, batch size, wall time, maximum registration statement time, row count, and seal time; no request may contain more than 500 descriptors. Also prove a populated canonical-base fixture upgrades atomically; fresh publication resolves bundle clientKey to manifest UUID in one finalize; reused publication uploads only the current XLSX and preserves source manifest/bundle lineage; crash recovery sees every locator and no partial ready set. A passed reused certificate must complete Calculation Bundle request admission, after-lease Worker binding, and package-ready publication with a target-owned report and exact direct-source Bundle/Snapshot; foreign, forged, recursive, stale, revoked, expired, or mismatched evidence remains fail-closed. Prove the owner check returns exactly two fixed-order locator-free summaries and exact selector DTO/opacity semantics, plus certificate deadlines, non-mutating preview, renewable `SKIP LOCKED` leases with old-token rejection, fresh detail-cleanup reclaim without object re-delete, repeated completion, and compact audit residue. SQL coordinates object I/O but never performs it. |
| Flow identity actor-fence trigger boundary | `supabase db reset`; run `supabase/tests/20260731_flow_identity_derivative_fence.sql`, both `20260716_guarded_dataset_flow_identity_mapping*.sql` suites, and the two guarded derivative rebuild suites | also run the nearby JSON/webhook regression and local security/performance advisors | Prove a separately held owner advisory lock does not block updates limited to `extracted_md`, `embedding_ft`, or `embedding_ft_at`; every other current or future Flow-column change plus every INSERT/DELETE remains fail-closed behind the existing v2 fence. |
| `supabase/tests/**` only | run the relevant SQL assertion files against a reset local DB | add a nearby migration or policy smoke check if the new assertions expose a gap | This repo stores PGTAP-style SQL assertions, not a single canonical runner wrapper. |
| `supabase/seed.sql` or `supabase/seeds/dev.sql` | `supabase db reset` succeeds with expected seed behavior | rerun targeted SQL assertions that depend on the seeded rows; for shared-seed changes, confirm the hosted Preview seed stage completes | Keep shared seed and dev-only seed expectations separate. A shared seed with no data must still contain an executable no-op statement; a comments-only batch is not deployment-safe. |
| `supabase/config.toml` | `supabase start` and `supabase db reset` still work locally | verify the changed branch-binding or auth assumption against `docs/agents/supabase-branching.md`; for exposed-schema changes, prove the Supabase GitHub integration applies the configuration or record the explicit operator gate, then read back exact ordered hosted PostgREST schema/search-path lists | Config changes affect preview, persistent dev, and local behavior together; local CLI order is not hosted default-profile proof. |
| `.github/workflows/supabase-dev.yml` | inspect and parse YAML; run `python scripts/test_resolve_migration_head.py` and `python scripts/test_supabase_dev_workflow_contract.py`; regenerate `supabase/workspace/database.types.ts` from the full local migration state; run `python3 scripts/build_portal_contract_types.py --check`; assert the persistent-Dev job alone links the configured Dev project, runs exactly one `supabase db push --include-all`, derives rather than pins its migration head, and performs exactly one three-field PostgREST PATCH; separately assert the pull-request-only Preview job performs exactly one identical PATCH and contains no database link/push, Functions/config deployment, database password, or persistent-Dev/production target | on an exact same-repository PR head, require nonempty access token, main-parent ref, and persistent-Dev ref; prove the Preview job accepts exactly one successful check named `Supabase Preview` on that head from official Supabase App id `330661`, slug/owner `supabase`, captures the expected 20-character ref from the exact dashboard `details_url`, and independently resolves exactly one pinned-CLI `branches list --output json` row whose Git branch, PR number, parent ref, `is_default=false`, and `persistent=false` all match; require equality and unconditional inequality with both main and Dev. Read back the exact PostgREST fields. In a dedicated credential step, perform exactly one no-reveal Management API key GET, require raw `disabled=false`, exact public key type and shape, prefer publishable over legacy anon, mask/export only that public key, then clear raw JSON and PAT. The following explicit-`api` Hybrid step must contain no PAT, `Authorization`, `Cookie`, or service credential; validate its response against the strict checked-in schema, require forged actor/team/state/data-source parameters to return opaque 404/PGRST202, reject `private` with 406/PGRST106 and the same RPC under `public` with 404/PGRST202; also retain persistent-Dev exact-head, configuration readback, default `public`, explicit `api`, rejected `private`, and retired `public` probes | The same workflow contains two isolated remote jobs. Fork PRs skip before authority; a same-repository PR missing any of the three authority inputs fails closed. Same-name checks from any other App cannot authorize mutation. Branch resolution reads only BranchResponse metadata, not branch credentials. The Preview job may repair only its disposable branch after the exact Supabase integration check and never deploys schema or Functions. The push-only job remains the sole persistent-Dev migration deployer. Across both jobs, the only Management API mutations are their two exact PostgREST PATCH calls. |
| `scripts/**` | run the touched script with `--help` when possible, or execute the narrowest safe non-destructive path | if a script changes generated workspace behavior, refresh the workspace in a safe environment and inspect git diff | Avoid remote-destructive script runs unless the task explicitly requires them. |
| `supabase/workspace/**` | prove whether the touched file is generated or stable | if stable manual overlay files changed, explain how they feed migration generation | Generated files alone are not sufficient evidence of a durable schema change. |
| repo docs only | `scripts/docpact lint --root . --files "<csv>" --mode enforce` | `scripts/docpact validate-config --root . --strict` when `.docpact/config.yaml` changes | Refresh review metadata even when prose stays unchanged. |

The Issue #532 Search/Hybrid card context is not a stored v1 card/document
semantic replacement. It must run only after the existing ordering and 50/20
limit, use exact source keys, share one independently manifest-guarded
decorator, and preserve the #531 projection/index/Facet/writer graph
byte-for-byte. Run `supabase/tests/20260827_portal_card_context_v1.sql`, require
Search and Hybrid schemas/types to share the exact exhaustive context, and add
Process/Flow Search-50 plus Hybrid-20 context p95/buffer samples to each named
Portal performance profile. Search remains <= 2 seconds; Hybrid remains <= 6
seconds p95 and < 8 seconds per call. Preview must validate the strict response
schemas with a publishable key. Any later change to the stored v1 card/document
still requires the shadow-v2 rollout described above.

The named benchmark fixture must exercise the complete expensive path rather
than context shape alone: every Process card carries a real quantitative
reference Exchange to an exact public Flow, complete FlowProperty/UnitGroup
support, technology, source/database identity, and dataset-authored review
status; every Flow card resolves the same public FlowProperty and source
identity while retaining explicit Process-only nulls. Require exactly 50/20
items for the four dedicated wrapper labels, capture full
`EXPLAIN (ANALYZE, BUFFERS)` for each wrapper, record any temp-buffer evidence,
keep each plan below 750,000 total and 250,000 read shared blocks, and apply the same
2-second Search / 6-second Hybrid budgets to both plans and 20-sample p95.
The Flow empty-query `geography=cn` Search-50 path must have its own full plan
artifact because it is the representative filtered worst case.
Its timing label must return exactly 50 complete cards, and its repair must
prove latest-version and relevance-cursor equality against the synchronized
facet child, fail closed on live Facet-manifest drift, preserve the general path
for every other filter, and add no index or writer work.

When the anonymous PR Preview probe fails, CI may log only the HTTP status and
a response-shape-validated PostgREST or SQLSTATE code. It must not print the
raw response body, `message`, `details`, `hint`, request payload, or public key.
An `unclassified` code preserves that boundary when the response is malformed
or carries unexpected fields.

For the narrow Portal facet projection, run both
`scripts/test_portal_projection_upgrade_recovery.sh` and
`scripts/test_portal_facet_projection_populated_upgrade.sh`. The latter must
exercise 126,246 pre-existing parent cards, retain 2x per-statement timeout
headroom with each shard file below 120 seconds, and prove a sub-five-second
successful reconcile plus exact key coverage, sampled facts, and DTO counts.

For the Portal Flow embedding-eligibility hotfix, sparse-zero and sparse-199
profiles must naturally name `flows_portal_embedding_eligible_v1_idx` for the
exact `state_code IN (100,200) AND embedding_ft IS NOT NULL LIMIT 200` universe
probe, expose parseable buffers/execution, and show no Flow heap Seq Scan or
temp/disk spill. The release profile records the natural full-vector plan and
keeps existing writer/ANN/exact budgets. After persistent Dev deploy, a
zero-embedding Flow probe plus at least five anonymous Hybrid samples must show
p95 <= 6 seconds, every call < 8 seconds, and no `portal hybrid unavailable`;
record index validity/size, database size before/after when available, and both
advisor classes. Process remains index-free unless separate hosted evidence
justifies shared write amplification.

For the Portal catalog summary, run
`supabase/tests/benchmarks/20260827_portal_catalog_summary_cardinality.sql`
only against a uniquely named isolated local project with
`benchmark_target=local`, exactly 17,299 Process rows, 108,947 Flow rows, and
20 samples per profile. `normal`, `tail-evidence`, and
`no-cas-classification-evidence` must each keep p95 at or below 250 ms and the
response below 16 KiB. The CAS and classification plans must naturally name
`portal_catalog_summary_eligibility_v1_idx`, with no sequential card scan or
temp spill under the façade's 32-MB workspace. Record the index bytes and build
time. The benchmark's separate temporary writer probe must compare 50
four-update samples before and after only the combined eligibility index; it
must not drop, rebuild, truncate, or rewrite the real Portal projection while
collecting incremental writer evidence.

For the bounded Portal sitemap shards, run
`supabase/tests/20260827_portal_sitemap_shards_v1.sql` and the existing
`20260825_portal_public_catalog_v1.sql` after a blank reset. The manifest must
return exactly 64 byte-stable, unique opaque cursors with a fixed 4,096-item
limit. Calling every descriptor must produce a globally disjoint union equal
to the retained latest-visible sitemap set; the legacy façade remains
byte-identical. A 4,097th latest identity must still commit to the synchronized
projection while only that shard read fails closed, and removing it must
restore the page immediately.

Run `supabase/tests/benchmarks/20260827_portal_sitemap_shards_cardinality.sql`
only on a uniquely named isolated local project with exactly 17,299 Process
rows, 108,947 Flow rows, and 20 samples. Require all 126,246 identities exactly
once, every shard at or below 4,096, manifest p95 at or below 250 ms, largest
shard p95 at or below 2 seconds, each call below the function's four-second
timeout, response JSON below 2 MiB, and a conservative rendered-XML estimate
below 6 MiB. The largest-shard plan must naturally name
`portal_sitemap_latest_shard_v1_idx`, scan no full latest-projection heap, and
spill no temp data. Record the covering-index build time/bytes plus 20-sample
writer p95 before/after both the physical index and the real facet-to-latest
sync trigger. Preview then uses only the enabled publishable or
legacy anon `apikey`, polls no longer than 300 seconds, strictly validates both
new JSON Schemas, and passes one manifest cursor back byte-for-byte; Portal #12
owns final Next/CDN XML and five-minute visibility proof.

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

## Supabase Functions Hooks Compaction

`util.purge_supabase_functions_hooks` enforces logical retention, but ordinary
deletes and autovacuum do not return the table files to the operating system.
Treat physical compaction as an explicit production maintenance action.

Before the maintenance window:

1. confirm a current backup or point-in-time recovery window
2. run the retention preview and bounded purge until `eligible_rows` is zero
3. capture row count and heap, index, and total bytes with the query below
4. confirm no long-running session or queued lock is using the hooks relation
5. stop if the application cannot tolerate an `ACCESS EXCLUSIVE` lock

```sql
select *
from util.preview_supabase_functions_hooks_retention(
  interval '14 days',
  pg_catalog.now()
);

select
  count(*) as row_count,
  pg_catalog.pg_relation_size('supabase_functions.hooks') as heap_bytes,
  pg_catalog.pg_indexes_size('supabase_functions.hooks') as index_bytes,
  pg_catalog.pg_total_relation_size('supabase_functions.hooks') as total_bytes
from supabase_functions.hooks;

select
  activity.pid,
  activity.usename,
  activity.state,
  activity.wait_event_type,
  activity.wait_event,
  lock.mode,
  lock.granted,
  activity.query_start
from pg_catalog.pg_locks as lock
join pg_catalog.pg_stat_activity as activity
  on activity.pid = lock.pid
where lock.relation = 'supabase_functions.hooks'::regclass
order by lock.granted, activity.query_start;
```

Run compaction from a direct database session during the approved window. The
timeouts make lock contention fail closed instead of queueing indefinitely:

```sql
set lock_timeout = '5s';
set statement_timeout = '15min';
vacuum (full, analyze) supabase_functions.hooks;
reset statement_timeout;
reset lock_timeout;
```

If the lock timeout fires, reschedule; do not increase it while traffic is
active. After success, rerun the storage query and the retention SQL test,
record before/after bytes in the delivery Issue, and verify webhook execution.
`VACUUM FULL` cannot be rolled back, so do not repeat it merely because the
remaining live rows still require disk space.

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

The canonical refresh target remains remote `dev`. For an exact local migration-state reconstruction, omit `--db-url` so the helper uses the Supabase CLI local connection path:

```bash
python scripts/build_schema_workspace.py --environment local
```

For a schema-changing PR, an exact-local reconstruction may be committed as a review snapshot after a blank migration rebuild, targeted contract tests, and a second deterministic regeneration show no drift. Record those proofs in the PR. After merge, require the database-only Dev deployment to reach the exact head, hosted catalog checks, and a remote-Dev refresh comparison; commit any resulting drift as a follow-up.

## Preview, Persistent Dev, and Root Integration

For branch-oriented changes:

- preview-branch proof usually happens on the repo PR
- persistent remote `dev` proof happens after merge into Git `dev`: first record the database workflow result, then the Edge-owned Function deployment and runtime validation
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
