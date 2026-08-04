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
  - supabase/operator/**
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
lastReviewedAt: 2026-08-02
lastReviewedCommit: 1ff6d2775bed146379d50dc91eaf43c7915dca0f
lastReviewedNote: "Reviewed for Issue #323: local fresh-stack reset plus the Root/Reference Review v2 and root-grouped queue pgTAP suites remain the minimum proof; hosted Preview and persistent-dev proof stay deferred to deployment."
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

The checked-in canonical entry point is:

```bash
python scripts/run_database_contract.py --suite canonical-local
```

It classifies every test asset, excludes benchmark/fixture/Preview/upgrade
harnesses from pgTAP, fails on unexpected lint errors even when the Supabase CLI
would exit zero, and checks catalog plus generated-workspace drift. Worker
control-plane changes additionally run `--suite worker-control-plane`,
`python scripts/test_worker_control_plane_upgrade.py`, and—after a clean reset—
`python scripts/test_worker_control_plane_data_api.py` to prove the final
PostgREST cache exposes the `public` service RPCs while anonymous calls fail
closed. The probe sends explicit `public` profile headers so the reviewed
`api`-first exposed-schema order cannot silently change its target schema.

The canonical green suite currently runs 56 top-level files. Nineteen stale
top-level files are visible, reasoned exclusions—not passes—and are tracked for
repair or explicit retirement in
`tiangong-lca/database-engine#336`. A green canonical result must not be
reported as those 19 files passing.

Every workflow that invokes the canonical runner must check out full Git
history (`fetch-depth: 0`). The immutable public-inventory provenance gate
verifies its recorded source commit with Git; a shallow checkout is an invalid
validation environment and must fail before any hosted deployment job starts.

## Proof Matrix

| Change type | Minimum local proof | Stronger proof when risk is higher | Notes |
| --- | --- | --- | --- |
| `supabase/migrations/**` | `supabase db reset` succeeds | run the relevant SQL assertions under `supabase/tests/**`; inspect affected workspace objects if the migration was authored from workspace files | Record which migration and which SQL test files were exercised. |
| schema/view boundary Expand or Contract | `supabase db reset`; `supabase test db supabase/tests/20260801_schema_boundary_view_compat.sql --local`; `python scripts/schema_boundary_phase.py`; `python scripts/test_schema_boundary_data_api.py`; `python scripts/test_schema_boundary_rollback.py` | run the global production-equivalent harness at its one-million-row qualification scale; refresh the generated workspace from the exact intended hosted target and inspect all five application schemas | Expand preserves canonical view OIDs, uses explicit-column `security_invoker` compatibility wrappers, and keeps `contractReady=false`. Contract is a separately reviewed phase. Never use `CASCADE`, `SELECT *`, or expose `private`, `util`, or `archive` through PostgREST. |
| Root/Reference Review v2 schema, commands, queue projections, or legacy migration | `supabase db reset`; `supabase test db --local supabase/tests/20260731_root_reference_review_v2.sql` | rehearse `root_reference_review_v2_local.sh backup`, restore verification, second-local-copy verification, dry-run, one-row apply, and reconciliation against a disposable production-shaped copy before any hosted cutover | Process alone keeps the numeric Gate. Prove the seven-table state and immutability transitions, exact checksum reuse, append-only scope history, owner-only notifications, advisory Reviewer outcomes, Admin final decisions, and preservation of legacy `review_id`/`reviews` projections. Never use the backup runner against production until its local restore markers represent real checks. |
| process/flow hybrid-search query shape, HNSW settings, or vector indexes | `supabase db reset`; run `supabase/tests/20260727_hybrid_search_staging_plan_governance.sql`, `supabase/tests/20260528_semantic_hybrid_candidate_helpers.sql`, and `supabase/tests/20260528_hybrid_semantic_candidate_limit_hotfix.sql` | after the migration reaches persistent `dev`, run the exact read-only `supabase/tests/benchmarks/20260727_hybrid_search_staging_explain.sql` profile against ref `fotofiyqnuyvgtotswie`; compare process, broad-flow, Product-flow, and full-RPC timings/buffers with Issue #292; rerun performance and security advisors | Use the benchmark's checked-in Edge route lexical parameter profile with its redacted real staging vectors; never synthesize lexical terms from the first Markdown tokens because shared headings create an artificial near-whole-table match. Do not use a seed-only Preview as production-cardinality proof, expose raw text/UUID/vector values, or run production `EXPLAIN ANALYZE`. Empty JSON filters must fold away under a custom plan, filtered HNSW must retain strict-order iterative scans, and semantic candidate growth remains one 10x bound with a 200-row floor. |
| Contact, FlowProperty, Source, or UnitGroup Semantic/Hybrid pipeline, RPC, or HNSW index | `supabase db reset`; run `supabase/tests/20260727_foundation_dataset_semantic_hybrid_search.sql`, `supabase/tests/20260514_embedding_derivative_trigger_scope.sql`, `supabase/tests/20260727_hybrid_search_staging_plan_governance.sql`, `supabase/tests/20260528_semantic_hybrid_candidate_helpers.sql`, and `supabase/tests/20260528_hybrid_semantic_candidate_limit_hotfix.sql` | on the exact Preview branch, run bounded cursor backfill pages and drain both extraction and embedding queues; prove zero terminal failures and full eligible-row derivative counts; smoke `tg`/`co` explicit-team scope, `my` explicit-state scope, `te` explicit-readable-team plus state scope, and missing/foreign-team fail-closed behavior; after merge to persistent `dev`, capture redacted real-parameter `EXPLAIN (ANALYZE, BUFFERS)` for Semantic and Hybrid queries across all four datasets, compare timings/buffers, then rerun performance and security advisors | Never do a synchronous whole-table migration rewrite. Backfill is service-only, capped at 500 rows, idempotent, and uses compact queue payloads. A blank-version row is ineligible because it has no stable worker identity: count it as a data-quality exception and never guess a version or enqueue it. Helpers require exact table/RPC allowlists, `tg`/`co`/`my`/`te` visibility, PGroonga escaping, custom plans, and strict-order HNSW scans; derivative writes must not change authored `modified_at`. Team Data requires an explicit team readable by the authenticated actor. A small table may correctly choose a sequential scan. Do not run production `EXPLAIN ANALYZE` or retain raw vectors, search text, IDs, or private row values in evidence. |
| `extracted_text` / legacy embedding-flag Contract retirement or Hybrid/Search v2 lexical contract | `supabase db reset`; run `supabase/tests/20260730_extracted_text_and_embedding_flag_retirement.sql`, `supabase/tests/20260613_hybrid_search_query_terms_escape.sql`, `supabase/tests/20260727_foundation_dataset_semantic_hybrid_search.sql`, and `supabase/tests/20260729_extracted_md_lexical_search_contract_v2.sql` plus the nearby semantic/HNSW and guarded-rebuild regressions | before applying the Contract, prove zero nonterminal guarded derivative requests and zero non-FT pending/PGMQ jobs; on the exact Preview branch, smoke all seven v2 Hybrid RPCs and `search_processes_latest_v2`, including strict owner-only draft scope; after merge to persistent `dev`, use the read-only benchmark's redacted real vectors and Edge route parameter profile, then compare advisors before promotion | Expand must already have built all seven `extracted_md` PGroonga indexes, switched consumers to v2, and completed burn-in. Contract drops legacy objects with `RESTRICT`, removes orphaned rule-based projection helpers, and must retain 21 `extracted_md`/`embedding_ft`/`embedding_ft_at` columns, seven lexical PGroonga indexes, and at least seven HNSW indexes. A PGroonga index may still appear as `unused_index` with `idx_scan = 0` after use; require a direct `EXPLAIN (ANALYZE, BUFFERS)` naming the index before classifying or dropping it. |
| embedding queue selector or per-scope backpressure | `supabase db reset`; run `supabase/tests/20260728_embedding_queue_selector_plan_governance.sql` and `supabase/tests/20260525_embedding_backpressure_and_deferred_queue.sql` | on persistent `dev`, capture a redacted real-queue `EXPLAIN (ANALYZE, BUFFERS)` before and after the append-only migration; prove the visible queue is scanned once, policy is resolved once per distinct scope, active counts are aggregated once per scope, terminal failures stay zero, and the queue drains under the checked-in policy defaults | Never correlate a full active-queue count with every visible job. Preserve retry ordering, stale-job cleanup, per-scope admission, and the default `max_in_flight` values. Any temporary staging-only policy ramp must be bounded, monitored for HTTP/queue failures, and restored before handoff; do not use higher concurrency to hide selector cost. |
| Performance Advisor index or table-bloat governance | `supabase db reset`; run `supabase/tests/20260608_missing_fk_support_indexes.sql` and any affected domain regression | run `supabase db lint --level warning`; compare the relevant Performance Advisor notices on Preview and persistent `dev`; for physical compaction, follow the hooks maintenance-window procedure below | An unused-index notice alone is not permission to drop HNSW, GIN, PGroonga, FK-supporting, or other workload-sensitive indexes. `VACUUM FULL` is never part of the automatic migration path. |
| guarded atomic owner-draft FP/UG alias primitive | `supabase db reset`; run `supabase/tests/20260711_guarded_dataset_alias_batch.sql` and the legacy `supabase/tests/20260404_dataset_command_rpcs.sql` | prove both the replay-capable whole-plan function and per-dimension executor have no API grants; exercise them only through postgres-owned test wrappers; retain the exact `dataset-alias-plan.v1` envelope, ordered time plus length-time batches, 25-row/20-exchange and 27-row/39-exchange scopes, public/foreign rejection, exact factors, live closure, hashes, and all-or-nothing audit proof | This is an internal mutation primitive after protected cutover. It never publishes or changes `state_code`; direct authenticated and service-role execution remains revoked. |
| protected one-shot owner-draft FP/UG execution | `supabase db reset`; run `supabase/tests/20260715_protected_dataset_alias_execution.sql`, `supabase/tests/20260711_guarded_dataset_alias_batch.sql`, both derivative rebuild suites, and the rollback-only statement-timeout fault assertion under `supabase/tests/preview/**` | on the exact hosted Preview ref, run `node supabase/tests/preview/protected_alias_rest_e2e.mjs --expected-preview-ref <ref>` and prove real authenticated success, atomic business rollback, concurrent duplicate admission, lost response, HTTP timeout, status-only recovery, exact 52 rows/59 exchanges/55 audits plus 23-flow + 27-process child admission, 309 unrelated-exchange preservation, and zero actor-scoped fixture residue; also retain the exact authenticated preflight/gate/admit/read ACLs and service-only executor, trusted server context, canonical plan/freeze/approval binding, immutable live-gate hashes, 180-second window, owner/state fences, zero legacy RPC grants, six unchanged support snapshots, and all-terminal causal aggregate proof | Hosted fixtures must hard-fail outside the exact disposable Preview, use separate Auth actors/UUID namespaces, emit redacted evidence, and clean up only their own rows, ledgers, queue items, failures, audits, and users. If a disposable Preview lacks `project_url` and `project_secret_key`, the fixture may create only two exact scenario-scoped Vault values after ref/URL binding, must refuse every pre-existing same-name value, and must delete them by exact name, description, and decrypted value; cleanup failure revokes but retains the actor identity for recovery. Pure BAFU owner-draft data may use the released tool directly on production only after a fresh production freeze and exact human approval; Preview/Dev validate shared capability and never replay BAFU data. No retry, approval reuse, publication, `state_code` change, raw SQL fallback, broad queue cleanup, or client-supplied desired PostgreSQL hash is allowed. |
| guarded owner-draft flow/process derivative rebuild | `supabase db reset`; run `supabase/tests/20260714_guarded_dataset_derivative_rebuild.sql` and `supabase/tests/20260715_guarded_dataset_derivative_rebuild_batch.sql` plus nearby webhook, embedding queue, and dataset-command regressions | retain the single-process v1 ACL/envelope/error contract; prove flow/process snapshot, fence, staging, quarantine, dispatch, proposal, commit, and terminal parity; prove the internal admission/read helpers have empty search paths and no anon/authenticated/service-role grants; reject 0, 51, duplicate, foreign, public, non-draft, incomplete, or desired-hash-drift targets before any child/audit/quarantine effect; atomically bind 1..50 children to supplied frozen baselines and actual post-write primary snapshots; and require exact 50/50, 23-flow + 27-process live primary, committed proposal, fresh vector, terminal audit, queue/failure drain, and completed-snapshot proof before aggregate completion | The compatible public v1 admission remains one process. Protected alias execution alone may invoke the private batch primitive in its mutation transaction. The primitive has a five-second lock wait, takes stable table/id/version row locks rather than a broad table write lock, validates every target before effects, never reports replay, and never retries. Nonterminal drain/failure phases retain each target fence. `completed`, `stale`, or `failed` may release it only after request-specific work and pre-existing worker windows are drained. |
| guarded Step 3 owner-draft public-flow identity rewrite | `supabase db reset`; run `supabase/tests/20260716_guarded_dataset_flow_identity_mapping.sql`, `supabase/tests/20260716_guarded_dataset_flow_identity_mapping_v2_contract.sql`, and the two guarded derivative rebuild suites; run both `protected_flow_identity_actor_lifecycle_contract.mjs` and `protected_flow_identity_residue_readback_contract.mjs` with `node --test`, then run the transport contract; before requesting Hosted execution approval, run the exact read-only transport path with `node supabase/tests/preview/protected_flow_identity_rest_e2e.mjs --transport-preflight-only --expected-preview-ref <ref>`; only after that proof passes may an outer-frozen one-shot Hosted E2E run with `node supabase/tests/preview/protected_flow_identity_rest_e2e.mjs --expected-preview-ref <ref> --scenario-namespace <fie2e-hosted-24hex> --request-id <uuid> --private-temp-dir <absolute-0700-empty-dir> --recovery-ipc` | prove authenticated-only preflight/process/read/finalize/recovery/lookup plus zero-write-cancel ACLs; exact actor/environment/policy/artifact and actor-wide request/text/identity hash-domain non-reuse; prove each disposable actor uses outer-frozen deterministic selectors, is registered before create, verifies an acknowledged create by GET-by-ID plus exact metadata, durably checkpoints its exact ID/selectors through outer-owned IPC before sign-in or fixture mutation, recovers a lost/malformed create only by one exact filtered email/fixture/request/namespace/role match, is globally logged out and hard admin-deleted, and is absent only after GET-404 plus a fresh empty exact filtered census; require cleanup to hold the derivative coordinator lock and accept only pre-external-dispatch exact children; require a transaction-external, read-only 39-surface zero-residue census covering Auth, capture children, scope/ledger/permit/invocation rows, derivative request/proposal/permit rows, live and archived queues, failure tables, pg_net, audits, primary/support fixtures, global exact-name Vault rows, fault hooks, and fixture DB sessions; one active wrapper invocation, memory-only raw permit custody, persisted token hash/generation only, atomic success rotation, old-permit rejection, and no permit consumption on malformed/authorization failure; prove the public process/finalize surface is three-argument only, exact replay/read/lookup never returns a permit, lost-preflight-response lookup is exact and read-only, and continuation uses a fresh human-approved recovery bound to observed scope state/whole proof that supersedes the old permit; retain 1-based scope/process/rewrite ordinals with 0-based exchange indexes; source owner-draft and exact non-owner public Elementary target/support guards; quantitative reference FP/UG/reference-unit identity; server-side reconstruction that changes only five reference fields; exact collision and pending/blocker occurrence ledgers; process-atomic audit plus one-target derivative admission; cancel success only for an untouched sealed scope with exact receipt/operation/plan/scope proof and zero ledger/audit/derivative/permit progress; read-only cancel replay plus foreign/anonymous/post-primary rejection; zero approved-source residue; dynamic 1..50 causal derivative proof; failed/stale child output with exact current primary/snapshot hashes; distinct derivative-only compensation-plan lineage; and terminal finalization only after every primary/audit/protected/derivative closure and completed wrapper invocation/generation proof passes. The hosted proof additionally binds the exact non-production ref, runs an authenticated read-only database transport proof through the same selected URL, exact native Supabase CLI, exact regular nested Docker executable, and exact cached `pg_prove` image ID/RepoDigest/platform; missing or mismatched images fail before database transport and are never pulled automatically. Every DB file sets and asserts its exact application name in-connection. The proof binds the child environment and network path before any disposable actor is created, then uses real owner/foreign authenticated JWTs across capture/read/process/finalize fences, proves live-drift fail-closed, full post-primary rollback, a true two-process same-permit race with exactly one winning first mutation, immediately captures recovery selectors after each committed process, rejects a stale permit, completes the second process, and performs one guarded finalize with two exact audits/derivative children before zero-residue cleanup | Do not copy the Step 2 180-second token, three live gates, pg_net one-shot dispatch, or fixed 50/23/27 reader. Credential/ref/hash checks and a connector-side SQL query are not substitutes for the exact runner transport proof; direct and pooler URL evidence are not interchangeable. The offline actor-lifecycle and residue contracts exercise failure/recovery and readback rendering/parsing without Hosted authority; they do not replace or authorize the full Hosted run or its independent Auth/SQL zero-residue execution, which remain deferred until the exact head, outer recovery envelope, and Preview authority are frozen. The read-only transport mode creates zero actors and writes zero primary rows. The full runner repeats the same proof before actor creation, so an operator-network failure cannot leave disposable identities. The outer wrapper creates and owns the exact private temp directory, durably fsyncs every secret-free checkpoint before returning the exact IPC ACK, owns recovery after signal, crash, ambiguous create, or derivative fast-path refusal, independently proves descendant quiescence plus temp removal, and must independently repeat Auth and SQL residue readback without ever relaunching the mutation runner. If inner cleanup cannot globally log out an actor, it retains that actor and performs no hard DELETE. Outer recovery may issue exactly one service-role password reset to a fresh memory-only value, sign in only the exact actor ID, require acknowledged global logout, then hard-delete and prove GET-404 plus a fresh empty census; an ambiguous reset, failed/wrong-ID sign-in, or ambiguous/failed logout retains the actor, performs no DELETE, and cannot be retried under the same authority. The original actor password, JWTs, service key, database URL, and raw permit may never enter checkpoint, journal, evidence, or durable environment files. The Step 3 scope is durable and process-atomic. A scope read resumes from durable evidence; the exact scope lookup resolves a lost preflight response, while exact replay is proof-only and no read/replay mints or discloses a permit. Wrapper loss, ambiguous responses, deterministic domain rejection, and derivative readiness after exit require a separate fresh exact recovery approval; there is no automatic retry or approval reuse. Cancellation is an abandon path only before any primary evidence and never rolls back or releases a partially written scope. A terminal derivative failure remains `derivatives_pending`, never auto-retries or replays primary, and requires a new single-target plan/freeze/exact approval before its request-ID causal proof is eligible. Source/public/support rows, `state_code`, publication, deduplication, merging, and summing are outside this capability. The hosted fixture is one disposable 305-source/two-process universe, creates the two exact branch-local Vault values only when absent, never invokes a revoked RPC as `anon`, keeps raw permits in memory/stdin only, stores rendered credential-bearing SQL only in mode-0600 files inside the outer-owned private temporary directory whose inner and outer removal proofs are terminal, and removes only namespace-bound rows/users. Live BAFU plans/freezes remain post-Step-2/post-dependency artifacts and are never generated by database tests. |
| review-submit, approve, or direct-publish lifecycle closure | `supabase db reset`; run `supabase/tests/20260728_review_publication_path_roles.sql`, `supabase/tests/20260404_review_submit_rpc.sql`, `supabase/tests/20260405_review_workflow_rpcs.sql`, and `supabase/tests/20260404_dataset_command_rpcs.sql` | inspect the affected function ACLs and fixed search paths; retain lock-order evidence for Process/LifecycleModel composition changes; run `supabase db lint --level warning` and distinguish existing extension/dynamic-temp-table findings from new warnings | Prove Lineage is read-only; existing RequiredSupport submit/approve linkage remains active; `json_tg.submodels` and ILCD `processInstance` identify the same exact Process versions; private and missing composition dependencies share one safe public error shape; unknown Process/Flow paths fail closed; and submit, approve, and direct publish write no dataset, review, comment, or audit state after a failed assertion. A same-id/version paired row is a dependency only when it exists. |
| review-submit gate / job coordinator schema or RPCs | `supabase db reset`; run `supabase/tests/20260404_review_submit_rpc.sql`, `supabase/tests/20260529_review_submit_jobs.sql`, and the relevant `supabase/tests/*review_submit_gate*.sql` file | include owner-access, service-role result recording, worker job result mapping, stale checksum, and `cmd_review_submit` rejection/acceptance assertions | Database proof covers persisted gate runs, `worker_jobs` coordinator links, and final submit assertions. Worker report heuristics and Edge orchestration need separate repo proof. |
| `worker_jobs` lifecycle schema or RPCs | `supabase db reset`; run `supabase/tests/20260531_worker_jobs_foundation.sql` | add any job-family-specific coordinator SQL tests affected by the change | Prove claim/reclaim, lease-token fencing, idempotency, concurrency keys, status transitions, and RLS/direct-access boundaries. |
| exposed-schema/default-privilege/API-role ACL convergence | `supabase db reset`; `supabase test db --local supabase/tests/20260801_security_acl_expand.sql`; `python scripts/test_security_acl_upgrade.py`; `python -m unittest scripts/test_hosted_security_acl.py scripts/test_apply_postgrest_config.py` | on the exact hosted Preview or persistent-dev migration head, reconcile configuration only through `scripts/apply_postgrest_config.py`; then run the read-only `scripts/hosted_security_acl.py` with SQL, Management API, and REST credentials; retain its secret-free evidence; rerun Security Advisor | Keep `public` compatibility during Expand. Require the exact `api,public,graphql_public` hosted exposed set in reviewed order; negative `private/util/archive` profiles; opaque publishable keys only in `apikey`; no direct API-role internal schema/routine grants; production-compatible ACLs for the ten drifted LCA relations; zero API-role `MAINTAIN`; closed `postgres` and `supabase_admin` defaults. Private search facades use a non-login, non-BYPASSRLS executor that inherits authenticated transport prerequisites. The two lifecycle bundle RPCs remain compatibility blockers until consumer migration permits Contract. The `supabase_admin` operator SQL requires an authorized owner session and remains a blocker until readback passes. |
| legacy lifecycle cleanup after `worker_jobs` cutover | `supabase db reset`; run `supabase/tests/20260531_worker_jobs_foundation.sql`, `supabase/tests/20260531_worker_jobs_legacy_lifecycle_cleanup.sql`, and `supabase/tests/20260602_worker_legacy_table_retirement_audit.sql` | verify production queue drain/cutover and archive availability separately before applying the cleanup migration to `main` | The cleanup disables legacy pgmq delivery surfaces, archives `public.lca_jobs`, `public.lca_package_jobs`, and `public.dataset_review_submit_jobs` into `archive.worker_legacy_job_table_rows`, and physically retires those legacy tables with `DROP TABLE ... RESTRICT`. |
| worker-produced domain artifact/state contracts after legacy table retirement | `supabase db reset`; run `supabase/tests/20260603_worker_domain_artifact_contracts.sql`; run `python scripts/check_generated_workspace_legacy_tables.py` after refreshing generated workspace output | inspect `public.worker_domain_traceability_violations` on the target remote after deployment | Prove retained `lca_package_*`, LCA result/cache, and review-submit domain rows are documented as domain state, new post-cutover rows remain traceable to `worker_jobs`, and package retention has both dry-run and apply helpers. |
| canonical LCI/LCIA release control plane | `supabase db reset`; run `supabase/tests/20260716_lci_lcia_release_control_plane.sql` and `supabase/tests/20260623_data_product_publication_mvp.sql` | also run `supabase/tests/20260603_worker_domain_artifact_contracts.sql`, `supabase/tests/20260608_missing_fk_support_indexes.sql`, and local `supabase db lint --level warning`; use a disposable Preview for authenticated Edge/REST transport proof | Prove API/publishable key without user session is 401; ordinary session is 403; manager prepare/approve/publish/readback succeeds only for exact hashes; service can finalize four artifacts but cannot write tables, approve, or publish; retries are idempotent; every source Process has one self-mapped Unit Process plus one LifecycleModel and Result Process; generated datasets stay out of authoring tables; public Process projections expose identities but no object locator; published artifacts are pinned and immutable; unpublish removes current projection without deleting artifacts. |
| data-product scope-closure snapshot source or preflight RPC | `supabase db reset`; run `supabase/tests/20260724_initial_candidate_scope_closure_e2e.sql`, `supabase/tests/20260722_scope_closure_release_binding_e2e.sql`, and `supabase/tests/20260722_data_product_scope_closure_and_task_feed.sql` | run `supabase migration list` and `supabase db lint --level warning`; when a migration hashes or rewrites the candidate universe, prove its production-volume statement fits the platform timeout or declares a bounded session override and restores the default immediately afterward; after production deployment, submit one real zero-release completeness check and inspect Worker completion plus the downloadable report | Prove zero-release subset/global requests, exact versions, canonical LCIA locator mapping, Worker-compatible canonical hashes, deterministic snapshot/scan reuse, explicit current-membership rejection, unchanged formal-release binding, ACLs, and task-center projection. Preview data volume alone is not production-scale backfill evidence. |
| scope-closure artifact publication, certificate expiry, download, or GC lifecycle | `scripts/test_scope_closure_base_to_head_upgrade.sh`; `supabase db reset`; run `scripts/test_scope_closure_staged_write_set_v2_fixture.sh`, `node supabase/tests/20260730_scope_closure_staged_write_set_v2_rest_contract.mjs`, `supabase/tests/20260730_scope_closure_staged_write_set_v2.sql`, `supabase/tests/20260729_scope_closure_publication_staging_and_gc_control.sql`, `supabase/tests/20260729_scope_closure_owner_artifact_projection.sql`, `supabase/tests/20260729_scope_closure_artifact_retention.sql`, `supabase/tests/20260722_scope_closure_release_binding_e2e.sql`, `supabase/tests/20260722_data_product_scope_closure_and_task_feed.sql`, `supabase/tests/20260531_worker_jobs_foundation.sql`, and `supabase/tests/20260608_missing_fk_support_indexes.sql` | run `supabase migration list` and `supabase db lint --level warning`; on the exact Preview branch, confirm the additive migration appears in remote history, prove both public selectors and the temporary one-argument XLSX overload through actor RPC transport, then exercise DB-first staging and Worker GC RPC transport when the matching Worker consumer is available | Prove the shared Worker/database fixture has one exact canonical digest and request/response shape. Exercise 0/1/500/501/596/1500+ descriptors, multiple batch sizes, duplicate/reordered/missing/out-of-range ordinals, wrong count/digest, role/locator conflict, stale/foreign fences, exact and conflicting replay, ambiguous-response status, concurrent register/seal, rollback, post-seal mutation, and one-shot compatibility. For 596 and at least 1500 descriptors, record request count, batch size, wall time, maximum registration statement time, row count, and seal time; no request may contain more than 500 descriptors. Also prove a populated canonical-base fixture upgrades atomically; fresh publication resolves bundle clientKey to manifest UUID in one finalize; reused publication uploads only the current XLSX and preserves source manifest/bundle lineage; crash recovery sees every locator and no partial ready set. Prove the owner check returns exactly two fixed-order locator-free summaries and exact selector DTO/opacity semantics, plus certificate deadlines, non-mutating preview, renewable `SKIP LOCKED` leases with old-token rejection, fresh detail-cleanup reclaim without object re-delete, repeated completion, and compact audit residue. SQL coordinates object I/O but never performs it. |
| Flow identity actor-fence trigger boundary | `supabase db reset`; run `supabase/tests/20260731_flow_identity_derivative_fence.sql`, both `20260716_guarded_dataset_flow_identity_mapping*.sql` suites, and the two guarded derivative rebuild suites | also run the nearby JSON/webhook regression and local security/performance advisors | Prove a separately held owner advisory lock does not block updates limited to `extracted_md`, `embedding_ft`, or `embedding_ft_at`; every other current or future Flow-column change plus every INSERT/DELETE remains fail-closed behind the existing v2 fence. |
| `supabase/tests/**` only | run the relevant SQL assertion files against a reset local DB | add a nearby migration or policy smoke check if the new assertions expose a gap | This repo stores PGTAP-style SQL assertions, not a single canonical runner wrapper. |
| `supabase/seed.sql` or `supabase/seeds/dev.sql` | `supabase db reset` succeeds with expected seed behavior | rerun targeted SQL assertions that depend on the seeded rows; for shared-seed changes, confirm the hosted Preview seed stage completes | Keep shared seed and dev-only seed expectations separate. A shared seed with no data must still contain an executable no-op statement; a comments-only batch is not deployment-safe. |
| `supabase/config.toml` | `supabase start` and `supabase db reset` still work locally | verify the changed branch-binding or auth assumption against `docs/agents/supabase-branching.md` | Config changes can affect preview, persistent dev, and local behavior together. |
| Data API exposed-schema or `api` DTO/RPC boundary | deploy in two stages: first `supabase db reset` and `supabase/tests/20260801_api_private_boundary_poc.sql`; only after that exact migration is hosted, change exposed schemas and run `node --test supabase/tests/20260801_api_private_boundary_config_contract.mjs` plus the read-only `20260801_api_private_boundary_rest_contract.mjs` with local URL/keys | after the schema-only stage succeeds on persistent `dev`, deploy configuration separately; on that exact ref rerun the REST contract with `EXPECTED_PROJECT_REF` and prove `api` succeeds, `private`/`util`/`archive` return PGRST106, anonymous service RPC calls fail, authenticated write and service/direct-SQL paths succeed, and schema-cache reload plus generated types see the intended contract | Supabase applies `Configure` before `Migrate`; never expose a schema in the same deployment that first creates it. Legacy JWT keys may be bearer tokens, but new opaque `sb_publishable_`/`sb_secret_` keys must not be sent as bearer values. If a disposable hosted secret key is required, create it with a unique name, delete its exact ID in an EXIT cleanup, and prove zero same-name residue. `api` views are `security_invoker`; every object has explicit grants; private schemas stay out of exposed schemas and `extra_search_path`; Realtime remains on explicitly published physical tables rather than API views. |
| `.github/workflows/supabase-dev.yml` or persistent-dev PostgREST config gate | parse workflow YAML; run `python -m unittest scripts/test_apply_postgrest_config.py`; run `scripts/apply_postgrest_config.py --check` against the exact non-production ref | after merge to Git `dev`, prove the workflow is serialized, migration push precedes config apply, PATCH contains only drifted allowlisted fields, exact GET readback matches, repeated execution is a no-op, failed readback restores and verifies the prior allowlisted snapshot, and the REST profile matrix still passes | The gate may PATCH only `db_schema`, `db_extra_search_path`, and `max_rows`; it must never emit the access token or remote response body. Do not replace it with unconditional `supabase config push`, which can apply unrelated Auth/Storage/Realtime drift. |
| global schema boundary or populated-upgrade qualification | on an isolated local stack and clean exact commit, run `python scripts/test_production_equivalent_upgrade.py --evidence-out <new-absolute-path-outside-worktree>` with the contract's one-million-row default; run `python -m unittest scripts/test_production_equivalent_upgrade_unit.py` for offline output safety | inspect the emitted redacted mode-0600 manifest; compare both fixture loads, upgrade seconds, and migration WAL to the checked budgets; retain exact commit, migration and rehearsal-cleanup SHA-256, base/rehearsed-head/actual-head row and catalog hashes, per-migration fault results, lock-timeout recovery, compatible-reader proof, and role matrix in the PR | The harness is destructive only to the selected local stack and must never use `--linked` or a hosted URL. Both PostgreSQL URL schemes are redacted from command and exception output. Evidence uses exclusive/no-follow creation, refuses existing targets or symlinks, and is fsynced before success. Its fixture spans identity, review, notification, audit, active/leased/retry/failed Worker jobs, artifacts, package/cache, release, and closure. Row content hashes normalize only generic `created_at`, `updated_at`, `modified_at`, and migration-evidence `captured_at` values that resets or migration replays regenerate; row counts, primary keys, and all other fields remain exact. Every pending migration must roll back cleanly against its immediate pre-migration state under injected failure, then rehearse successfully; before reset, the contract-pinned Issue #339 operator rollback removes its database-global rehearsal role, after which the runner deterministically reloads the populated base for the real CLI roll-forward. An incompatible lock must fail within five seconds; every base physical relation must preserve its row/PK/content oracle while newly introduced evidence relations and constraints, ACL/RLS, policies, triggers, and publication membership must match the rehearsed head exactly; retry must preserve that complete head and emit exactly zero WAL bytes. Update the base/head contract deliberately when migration scope advances. |
| `scripts/**` | run the touched script with `--help` when possible, or execute the narrowest safe non-destructive path | if a script changes generated workspace behavior, refresh the workspace in a safe environment and inspect git diff | Avoid remote-destructive script runs unless the task explicitly requires them. |
| public object inventory/dependency closure | reset an isolated stack from exact `source.databaseSchemaSha`; run `python -m unittest scripts/test_public_inventory_closure.py`, `supabase test db supabase/tests/20260801_public_inventory_dependency_closure.sql --local`, `python scripts/public_inventory_closure.py --verify-provenance <workspace-root>`, and `python scripts/public_inventory_closure.py --check` | prove two clean-clone regenerations against that same schema SHA are byte-identical; refresh exact-SHA consumer evidence with `--scan-consumers <workspace-root>` only when a pinned consumer commit changes; review every emitted dynamic-SQL and owner/runtime residue entry before Contract | The gate separates immutable workspace and historical review lineage from `databaseSchemaSha`, the sole migration/catalog replay input, and covers canonical JSON/hash bytes, tables, views, routines, ACL/RLS/default privileges, dependency edges, migration order, and consumer evidence. Moving remotes and the current worktree are not provenance inputs. The previous #338 hash remains lineage only. Unknown consumers remain explicit blockers and `contractReady=false` remains mandatory until independent residue closes. |
| SECURITY DEFINER owner/runtime and cross-schema lineage audit | run artifact-only `python scripts/security_definer_audit.py --check`; only on the exact genesis stack run v1 `--check-live-baseline`; on the current exact-SHA stack run `python scripts/security_definer_audit_v2.py --check`, the v2 offline and transition-runner unittest modules, `ISSUE333_DATABASE_URL=<loopback-url> python -m unittest scripts/test_security_definer_audit_v2_postgrest_conformance.py`, and `supabase test db supabase/tests/20260801_security_definer_owner_runtime_audit.sql --local` | validate artifact hashes and immutable transition receipts, exercise the source-bound #356 transition fixture, compare v14.7 OpenAPI/default-profile behavior to the audit, and inspect all critical/high inferred-risk signatures plus every endpoint role matrix before #358 | v1 remains the immutable 241-public-routine genesis baseline and is not a current-state claim. v2 is the live gate: it preserves genesis identity, requires receipt-bound transition-native births and retirements, claims every current SECURITY DEFINER endpoint in the five governed schemas exactly once as an active canonical, and permits only SECURITY INVOKER aliases. search_path trust, schema USAGE, effective EXECUTE, ordered exposed schemas, `authenticator` SET-role reachability, PostgREST cache/request/direct-invocation shape, definition/body/property hashes, predecessor bytes, canonical Git regular-file/no-follow receipt paths, and exact schema provenance fail closed. Issue #352 remains blocked and `contractReady=false` is mandatory. |
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

Do not commit a local reconstruction unless its applied migration versions match the intended hosted target and targeted hosted catalog checks rule out relevant schema drift. Record both proofs in the PR.

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
