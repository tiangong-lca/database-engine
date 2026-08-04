---
title: database-engine Repo Architecture Notes
docType: guide
scope: repo
status: active
authoritative: false
owner: database-engine
language: en
whenToUse:
  - when you need a compact mental model before editing SQL, config, or schema-workspace tooling
  - when deciding whether a path is stable source of truth or generated inspection output
  - when the task mentions workspace-based migration authoring, remote schema export, or branch-specific database behavior
whenToUpdate:
  - when major repo paths or authoring workflows change
  - when the generated schema workspace contract changes
  - when new hotspot areas make the current map misleading
checkPaths:
  - docs/agents/repo-architecture.md
  - .docpact/config.yaml
  - supabase/config.toml
  - supabase/migrations/**
  - supabase/tests/**
  - supabase/workspace/**
  - scripts/**
  - .github/workflows/supabase-dev.yml
  - .githooks/pre-push
  - scripts/docpact
  - scripts/docpact-gate.sh
  - scripts/install-git-hooks.sh
lastReviewedAt: 2026-08-02
lastReviewedCommit: 1ff6d2775bed146379d50dc91eaf43c7915dca0f
lastReviewedNote: "Reviewed for Issue #323: the additive root-grouped queue RPCs stay within the existing Root/Reference Review v2 database boundary and do not change the repository shape or cross-repo ownership."
related:
  - ../../AGENTS.md
  - ../../.docpact/config.yaml
  - ./repo-validation.md
  - ./supabase-branching.md
---

## Repo Shape

This repo is organized around one checked-in Supabase project plus a generated schema-inspection workspace.

## Stable Path Map

| Path group | Role |
| --- | --- |
| `supabase/config.toml` | shared local baseline plus branch-specific remote bindings |
| `supabase/migrations/**` | authoritative migration history and durable schema changes |
| `supabase/seed.sql` | shared seed data; when no rows are needed, retain an executable no-op statement instead of a comments-only file so hosted Preview seeding has a valid SQL batch |
| `supabase/seeds/dev.sql` | persistent dev-only seed overlay |
| `supabase/tests/**` | PGTAP-style database assertions plus narrow offline Node contracts for test-runner control flow |
| `supabase/tests/benchmarks/**` | explicit operator-run performance profiles; read each profile's environment guard because some are local/Preview-only while hybrid-search evidence is pinned to persistent staging |
| `supabase/tests/preview/**` | exact-ref-bound disposable Hosted Preview mutation fixtures, cleanup, rollback-only fault assertions, and offline transport/lifecycle contracts; test-only and excluded from migrations, seeds, Dev data rehearsal, and production execution |
| `.env.supabase.dev.local.example`, `.env.supabase.main.local.example` | operator branch-binding templates |
| `scripts/**` | export, refresh, change-copy, and migration-generation helpers |
| `.github/workflows/supabase-dev.yml` | serialized automation for pushing committed migrations to persistent remote `dev`, then reconciling and readback-verifying only the reviewed PostgREST fields |
| `supabase/workspace/changes/**` | manual overlay area used when generating migrations from workspace files |
| `supabase/workspace/remote_schema.sql` | generated full raw dump from the remote database |
| `supabase/workspace/global/**` | generated split-out global objects rebuilt on workspace refresh |
| `supabase/workspace/schemas/**` | generated human-browsable split schema objects rebuilt on workspace refresh |

## Schema Boundary Model

- `public` retains the nine core entity tables and existing compatibility objects during Expand.
- `api` is the explicit versioned PostgREST DTO/RPC layer. Views use `security_invoker`, routines have fixed search paths, and grants are per object. Its schema and objects are deployed before a later configuration change exposes it.
- `private` owns internal runtime and control-plane objects and is never an exposed Data API schema.
- `util` and `archive` retain operations/history responsibilities and are never exposed Data API schemas.
- Realtime publishes explicitly selected physical tables; `api` views are not a Realtime source.

Physical moves happen only after consumers use the `api` or `private` contract and the Contract gate proves zero compatibility callers. Expand views and wrappers must preserve one physical source of truth.

The stable public-boundary inventory lives under
`supabase/tests/contracts/public_object_*`. It imports the workspace #533
baseline, reconciles the #337 delta with the current `dev` head, and is regenerated
from a live local catalog by `scripts/public_inventory_closure.py`. Generated
dependency and consumer evidence belongs in that contract surface, not in
`supabase/workspace/**`.

## Branch Model In Practice

`database-engine` is an M2 repo:

- Git `dev` is the daily integration trunk
- Git `main` is the promoted release line
- PR branches map to Supabase preview branches
- `.github/workflows/supabase-dev.yml` pushes every committed migration missing from remote history to the persistent remote `dev` branch on Git `dev`, including an older-timestamped migration introduced by a governed `main -> dev` backmerge
- the production Supabase project is migrated automatically by the Supabase GitHub integration when Git `main` advances

This means branch behavior is part of the repo architecture, not just delivery process.

## Test Proof Layers

SQL assertions own database semantics and ACL regressions. Offline Node contracts own runner-only control flow, including outer-frozen request/namespace selectors, deterministic role emails, an outer-created exact empty mode-0700 private temp directory, fsync-before-ACK secret-free recovery checkpoints, exact filtered metadata recovery, global logout, hard DELETE followed by GET-404 plus a fresh empty filtered census, in-connection application-name binding, and fail-closed rendering/parsing of the 39-surface read-only residue proof. The inner runner may not begin actor sign-in or fixture mutation until the outer process has durably acknowledged the exact actor/selectors checkpoint. Cleanup shares the derivative coordinator advisory lock and is allowed only before either exact child crosses external dispatch; otherwise it fails into separately authorized durable recovery. A missing or ambiguous global logout always retains the actor and forbids hard DELETE. Those offline contracts use no Hosted database authority and do not replace the later exact-head Hosted mutation proof or independent Auth/SQL readback execution.

The global populated-upgrade harness is a separate local-only proof layer. Its
checked contract pins a reviewed migration base and exact head, one-million-row
representative package-evidence scale, fixture surfaces, expected boundary
objects, and time/WAL budgets. The runner creates only synthetic deterministic
data, hashes rather than exports physical rows, injects a terminal error into
every pending transaction, exercises incompatible and compatible lock holders,
then verifies roll-forward and no-op retry against row/primary-key/content
oracles plus constraint, ACL/RLS, policy, trigger, and publication invariants.
It is not a production snapshot, hosted deployment, or authorization to mutate
a linked project; advancing the migration head requires explicit contract
review.

## Current Hotspot Themes

The current migration and test history clusters around these themes:

1. access control and policy hardening
2. review workflow command/query RPCs
3. dataset lifecycle, protected one-shot private owner-draft FP/UG alias rewrites, durable process-atomic Step 3 public-flow identity rewrites, guarded flow/process derivative rebuild coordination with dynamic 1..50 and retained fixed 23+27 closure proofs, and publish/delete flows
4. notification and membership query boundaries
5. lifecycle bundle cleanup, embedding compatibility, and measured process/flow plus foundation-dataset Semantic/Hybrid HNSW plan governance
6. remote schema reconciliation and preview-branch validation
7. review-submit gate persistence, Process-only Gate enforcement, Root/Reference Review v2 range history and shared exact-reference reviews, `worker_jobs` queue state, final submit-review assertions, and retired legacy job-table archives
8. worker-produced domain artifact/state contracts for retained `lca_package_*`, LCA result/cache/projection, and review-submit report/coordinator tables
9. canonical LCI/LCIA release runs, exact dataset-version indexes, immutable four-package artifact refs, durable approval, publication, and readback
10. Performance Advisor evidence, usable foreign-key support indexes, exact duplicate removal, and lock-aware managed-schema bloat maintenance

If the task touches one of those areas, expect both schema truth and regression assertions to matter.

## Hybrid Search Plan Boundary

The process/flow hybrid RPCs deliberately separate text-fusion expansion from
semantic candidate expansion. Public hybrid functions pass the unexpanded
semantic match count; private semantic helpers apply one 10x scan bound with a
200-row floor. Empty residual JSON filters must be foldable under custom plans
so they do not suppress HNSW. Filtered pgvector 0.8 HNSW paths use strict-order
iterative scans because filtering happens after approximate index traversal.

The global process/flow HNSW indexes remain necessary for owner/team and broad
visibility paths. A smaller process partial HNSW index covers the measured
public `state_code = 100` path; large flow-specific duplicate indexes require
new staging evidence before they are added. Production-cardinality proof lives
in the read-only benchmark profile and Issues #292/#310. The benchmark combines
the checked-in Edge route lexical parameter profile with redacted real staging
vectors; it must not derive lexical terms from shared Markdown document-prefix
tokens or retain raw embeddings, UUIDs, or user query text.

All seven dataset families use deterministic `extracted_md` as the single
lexical document and as the source for backpressured 1024-dimensional
`embedding_ft` jobs. PGroonga indexes cover `extracted_md` for Process, Flow,
LifecycleModel, Contact, FlowProperty, Source, and UnitGroup search. Hybrid v2
RPCs expose one `lexical_weight`; the old two-weight signatures remain only as
an historical Expand-phase compatibility surface and do not represent two
lexical branches. The Contract migration retires those signatures after all
consumers have moved to v2. On the hosted branch, standard `idx_scan` statistics and the
Performance Advisor may continue to report a PGroonga index as unused after a
query plan has used it; a direct `EXPLAIN (ANALYZE, BUFFERS)` naming the index is
the required cross-check before any retention decision. After Edge and Next
consumer cutover, the fail-closed Contract migration removes the legacy RPC
signatures, seven `extracted_text` columns/triggers/indexes, the rule-based text
projection/backfill helpers, three `embedding_flag` columns, three legacy
`embedding_at` columns, and the obsolete embedding-input/generation routines.
It refuses to run while a guarded derivative rebuild or non-FT embedding job is
active and uses `RESTRICT` for dependency-sensitive drops. `extracted_md`,
`embedding_ft`, `embedding_ft_at`, their seven PGroonga indexes, and their HNSW
indexes remain the supported derivative contract.

Contacts, FlowProperties, Sources, and UnitGroups otherwise follow the same
durable search-derivative shape as the established Process, Flow, and
LifecycleModel surfaces: compact extraction jobs persist `extracted_md`, and
backpressured embedding jobs persist `embedding_ft`. Their obsolete
1536-dimensional `embedding` columns and empty legacy HNSW indexes are retired
instead of being treated as a usable pipeline.

Historical repair is an explicit service-role cursor operation with a maximum
500-row page. It resumes either missing Markdown or missing embeddings, is
idempotent against live extraction and embedding queues, and never performs a
synchronous whole-table rewrite inside a migration. Extraction and embedding
writes are derived state: their trigger paths must not advance the authored
`modified_at` timestamp. A historical row without a nonblank version has no
stable worker identity and is ineligible for backfill; retain it as an explicit
data-quality exception instead of guessing a version or enqueueing a terminally
invalid job.

Embedding queue dispatch must take one visible-job snapshot per invocation,
resolve policy once for each distinct table/column scope, and aggregate active
queue counts once per scope before ranking admissible work. A correlated active
count that rescans the queue for every visible job is prohibited: a large
backfill turns that shape into quadratic queue work before any external request
is dispatched. Selector optimization must preserve the existing per-scope
backpressure, retry, ordering, and stale-job contracts; it is not permission to
raise the checked-in policy defaults.

The four public Semantic and Hybrid RPC families share exact-regclass
allowlisted private helpers. Those helpers preserve owner/team/public
visibility (`tg`/`co`/`my`/`te`): `tg`/`co` may be narrowed to an explicit
team, `my` may be narrowed to an explicit state, and `te` requires one explicit
team that the authenticated actor can read and may also be narrowed by state.
They force custom plans so empty filters fold away, use strict-order iterative
HNSW scans, escape PGroonga terms, and fuse text and semantic ranks with RRF.
Local seed cardinality is not index-plan evidence;
after the migration reaches persistent `dev`, real redacted parameters must be
measured with read-only `EXPLAIN (ANALYZE, BUFFERS)` there before adding partial
or duplicate indexes.

## Worker Jobs And Domain State

`worker_jobs` is the canonical lifecycle and queue-control table for work that cannot be safely carried by Edge Function request/response execution.

The first private-boundary rollout is additive: `private.worker_jobs`,
`private.worker_job_events`, `private.worker_job_artifacts`, and
`private.worker_job_kinds` are security-invoker views over the one physical
public source during Expand. Public RPC signatures and composite types remain
stable; there is no dual write. A physical schema move is a later Contract step
gated by exact consumer SHAs, zero compatibility calls, burn-in, rollback-window
closure, and the checked residue report. `public.worker_job_domain_refs` remains
a public cross-domain projection rather than Worker control-plane storage.
The bounded concurrency snapshot uses the partial composite index
`worker_jobs_job_kind_concurrency_created_idx` on job kind, concurrency key,
and descending creation/id order, so the equality prefix is an index condition
and the twenty-row result needs no filtered sort.

Retained domain tables such as `lca_package_artifacts`, `lca_package_export_items`, `lca_package_request_cache`, `lca_results`, `lca_result_cache`, `lca_latest_all_unit_results`, `lca_network_snapshots`, `dataset_review_submit_requests`, and `dataset_review_submit_gate_runs` are not replacement job tables. They store worker-produced artifacts, caches, projections, reports, or coordinator domain state. Post-cutover rows should be traceable back to `worker_jobs` through the appropriate worker job reference columns, except for explicitly documented exceptions such as snapshot identity rows that are traced through downstream worker-linked records.

Use `public.worker_domain_traceability_cutoffs` and `public.worker_domain_traceability_violations` for DB-side audit checks when validating that new worker-produced domain rows remain traceable.

## LCI/LCIA Release Control Plane

`lca_release_runs` is the durable release state machine; `lca_release_dataset_versions`, `lca_release_artifacts`, `lca_release_approvals`, and `lca_release_publications` are immutable/indexed release facts. The dataset index binds every generated identity to its exact source Process and requires exactly one Unit Process, LifecycleModel, and Result Process per source identity; the Unit Process mapping must point to itself. Generated LifecycleModel and Result Process documents are referenced from canonical object artifacts and never inserted into editable `lifecyclemodels` or `processes` authoring rows.

Authenticated prepare, approve, publish, readback, and unpublish commands re-check `auth.uid()` against the live `data_product_manager` platform role. The separate service-only finalize command binds four uploaded package refs to the exact prepared plan and validated release manifest, but service identity has neither direct table writes nor approval/publication function grants. Public and private read/download projections remain RPC-owned so Edge can issue signed downloads without exposing database mutation capability.

## Scope-Closure Snapshot Sources

The data-product completeness check can run before the first formal `lca_release_publications` row exists. When a current release exists, normalization and the immutable `lcia.scope-closure-data-snapshot.v2` manifest remain bound to its exact `lca_release_dataset_versions`. Before the first release, the database instead freezes every exact `state_code 100..199` Process, Flow, FlowProperty, UnitGroup, Source, Contact, and LifecycleModel document readable by the deployed closure Worker, plus the exact reviewed 25-method LCIA allowlist (whose production authoring rows intentionally remain `state_code=0`). Global Process roots select the latest eligible version per UUID, while the frozen support universe retains all exact eligible versions so explicit transitive references remain resolvable.

Candidate snapshots compute `canonicalContentHash` with the same recursively key-sorted, compact JSON algorithm used by the Worker and normalize the reviewed LCIA method/artifact-locator alias before freezing identities. Because the initial production universe exceeds 120,000 exact rows, migration backfill pays the canonical-hash cost once into a private cache; eight table triggers then refresh only changed identities, while interactive requests aggregate the cached manifest inside the authenticated role timeout. This one-time production-volume administrative statement must declare a bounded session `statement_timeout` above the platform's ordinary two-minute cap and restore the default immediately after the backfill; a small Preview dataset is not sufficient evidence for that bound. `candidateData.sourceKind=candidate-public-state` is the authoritative source discriminator. The zero-UUID `currentPublicRelease` object is only a deterministic compatibility projection required by the deployed Worker v2 schema and must never be treated as publication evidence. `current-membership-required-v1` continues to require a real current release; the default frozen-artifact policy may consume a candidate snapshot.

## Scope-Closure Evidence Retention

`worker_job_artifacts` is the authoritative lifecycle surface for the private scope-closure report, complete machine result, and closure bundle. A database trigger assigns their immutable artifact roles and a retention deadline no later than seven days after creation. Ready evidence must have a private object locator, media type, size, and checksum; lifecycle transitions are one-way from `ready` to `expired` to `deleted`.

Service-only publication write sets preallocate every artifact UUID and register every possible object locator before the first upload. The versioned v2 protocol first creates an upload-ineligible `registration_open` header bound to the closure check, Worker job and current lease generation, request UUID, expected descriptor count, canonical descriptor-set digest, required primary roles, expiry, and write-token fence. The Worker then submits strict 1-based descriptors in idempotent batches of at most 500. Locator-free status/readback exposes counts, batch ranges, and—only after seal—the authoritative `clientKey -> artifactId` map without returning owner identity, object locators, raw lease authority, or service credentials.

One atomic seal locks the header and verifies exact contiguous cardinality, canonical digest, primary roles, unique locators, metadata, and current fences before changing `registration_open` to upload-eligible `staging`; missing, reordered, conflicting, stale, or post-seal input cannot create a partial ready set. Current-lease finalize still publishes every artifact and the closure projection in one transaction. Fresh publication requires one report, one bundle, one manifest, and any partitions; bundle staging metadata names the manifest by `completeMachineResultClientKey`, and finalize atomically replaces that field with `completeMachineResultArtifactId`. Reused publication is explicitly bound to a completed source check, accepts exactly one new XLSX report, and finalize changes only the target report binding while preserving the source manifest and bundle. Abandoned `registration_open` and failed or expired `staging` rows enter the same fenced reconciliation path, so process death cannot leave DB-invisible uploaded objects or a partially ready set. The original at-most-500-item one-shot RPCs remain a compatibility adapter during expand/migrate; removal requires a separate explicit contraction audit after deployed consumers migrate.

Every new valid closure certificate links all three evidence roles. Its `valid_until` is the earliest evidence deadline, and both authenticated build admission and service-side build binding re-check current lifecycle state and expiry. The owner-scoped check read returns exactly two locator-free public artifact summaries in fixed order: `closure_report_xlsx`, then `closure_issue_manifest`. Each summary always has the public role and synthesized `pending`, `ready`, `expired`, `deleted`, or `failed` state, plus semantic filename/format/media and nullable size/checksum/expiry fields; it never exposes an artifact ID, bucket, object path, storage path, or service detail. A missing link is `pending` only while the check is queued or running and otherwise `failed`; a malformed or unready linked artifact is `failed`.

The actor-bound download projection requires one strict public selector: `closure_report_xlsx` maps only to the linked report artifact, while `closure_issue_manifest` maps only to the linked complete-machine-result artifact. It maps database-owned coarse roles at the boundary and returns the shared 11-field role/state/filename/format/media/integrity/expiry/locator descriptor. Cross-actor results remain opaque 404; an owner whose selected artifact expired receives the stable `410 closure_report_expired` contract. During the expand-migrate-contract rollout, the temporary one-argument overload forwards exactly to the two-argument XLSX selector; remove it only in a separate cleanup Issue after every Edge consumer has migrated.

Storage object deletion remains outside SQL. The service-only preview is stable and non-mutating. GC claims use `FOR UPDATE SKIP LOCKED`, return `leaseExpiresAt`, and may be renewed only by the current unexpired fenced token; expiry permits handoff and rejects the old token. Completion accepts retryable failure and treats repeated or missing-object completion idempotently. The first successful completion tombstones the locator and moves any remaining bounded detail cleanup into DB-owned `gc_cleanup_state=pending`. A later claim returns `gcPhase=detail_cleanup` and `objectDeleteRequired=false` with a fresh fenced token, so a new Worker process can finish without attempting Storage deletion again. Final completion retains artifact checksum/size plus a compact closure summary, counts, and content hash for audit.

## Generated Workspace Workflow

The generated schema workspace exists to inspect and transform remote schema objects without hand-maintaining the generated tree.

Use it like this:

1. refresh the generated workspace from the target remote database
2. inspect `remote_schema.sql`, `global/**`, or `schemas/**`
3. copy the object you want to modify into `supabase/workspace/changes/**`
4. generate a migration from the stable overlay file or write the migration directly

Do not leave durable manual edits only inside generated paths.

Remote `dev` is the canonical refresh target. `--environment local` is a validation and recovery path that uses the Supabase CLI-native local connection; locally reconstructed output must not be represented as hosted truth without matching migration-version and targeted hosted catalog evidence.

## Script Responsibilities

| Script | Job |
| --- | --- |
| `scripts/export_remote_schema.py` | export the target remote schema into `supabase/workspace/remote_schema.sql` |
| `scripts/build_schema_workspace.py` | rebuild the generated workspace tree from the exported schema |
| `scripts/copy_workspace_file_to_changes.py` | copy generated workspace files into the stable `changes/**` overlay |
| `scripts/new_migration.py` | generate a migration file from a supported overlay object path |
| `scripts/_db_workflow.py` | shared internal module for the helpers above |

## Cross-Repo Boundaries

This repo owns database truth, but not every runtime consequence:

- `database-engine` owns persisted review-submit gate runs, `worker_jobs` lifecycle schema/RPCs, review-submit job coordinator state, access checks, idempotent gate lookup, result recording, legacy lifecycle cutover cleanup, retired legacy job-table archives under `archive.worker_legacy_job_table_rows`, and the final submit-review assertion
- `database-engine` owns authoring-row review/publication lifecycle closure. It classifies references by JSON path and lifecycle role, keeps Lineage read-only, preserves RequiredSupport submit/approve linkage, requires `json_tg.submodels` and ILCD `processInstance` to agree on exact composition versions, and applies transaction-final locks and assertions before submit, approve, or direct-publish writes. A same-id/version Process/LifecycleModel pair participates only when both rows exist. Private and missing composition dependencies use the same non-disclosing error envelope; Worker numeric/source closure semantics remain outside this database role matrix.
- `database-engine` owns durable LCI/LCIA release facts and final authorization: exact plan/artifact hashes, manager approval/publication, service-only artifact finalization, immutable pinning, and readback; it does not materialize TIDAS/ILCD bytes or place generated datasets in authoring tables
- `database-engine` owns the protected one-shot owner-draft FP/UG alias execution contract. Authenticated callers may only run the guarded preflight, three ordered live gates, one admission, and read-only status polling; a nonce-bound service executor alone can invoke the private replay-capable whole-plan and per-dimension primitives. The sealed `dataset-alias-plan.v1` request keeps time followed by length-time, one plan hash and operation ID, `target_visibility=owner_draft`, 52 distinct action rows, 59 exchange mutations, 55 immutable alias audits, and atomic admission of all 23-flow plus 27-process derivative children. Preflight and execution independently enforce actor-owned `state_code=0`, unchanged support, embedded identity, canonical exchange hashes, no public/foreign/non-draft parent, exact closure, stable row locks, table-specific allowed paths, and exact factors; indexed `json_ordered` subtrees provide candidates, while legacy `json` is never evidence. A timeout or any primary, audit, or derivative-admission mismatch rolls back every business effect, and the sealed approval permits no redispatch or replay. Production owner-draft data execution is allowed only against a freshly frozen production state with exact human approval; Preview/Dev validate the toolchain rather than replaying that production mutation. Status polling defers the full 50-target causal proof until terminal evidence is available and returns an explicit read-only conflict if the parent ledger changes while evidence is assembled.
- `database-engine` owns the guarded Step 3 public-flow identity rewrite contract. Preflight seals exact source/public/support guards, compatibility policy/evidence, ordered process templates, five-field rewrite locators, collision rows, derivative baselines, and exact pending/blocker occurrences. Initial and recovery approval artifacts are actor-wide non-reusable across request/text/identity hash domains. Each fresh preflight creates exactly one wrapper invocation and returns one memory-only rotating permit; the database persists only its generation and token hash, every successful process or finalize rotates it atomically, and exact preflight replay returns no permit. The public process/finalize RPCs require this authorization as their third argument. Scope read remains read-only status/resume evidence, while an exact read-only scope lookup resolves a lost preflight response without minting or disclosing a permit. If the wrapper loses its permit or exits after an ambiguous/domain-rejected call, continuation requires a fresh exact human-approved recovery artifact bound to observed scope state and whole-scope proof; recovery supersedes the old invocation and permit and never constitutes automatic retry. Each authenticated process call acquires the scope advisory lock, revalidates the next owner-draft process and every used mapping, reconstructs the desired JSON from live data, changes only `@refObjectId`, `@type`, `@uri`, `@version`, and `common:shortDescription`, records one unique audit, and admits one protected derivative child in the same transaction. An authenticated cancel request is actor/receipt/operation/plan/scope-proof bound and may release active fences only for an untouched `sealed` scope whose ledger, primary audits, derivative references, and mutation permits all prove zero writes; exact replay is read-only, while any post-primary scope is immutable to cancel. A terminal failed/stale derivative exposes the exact current single-target snapshot for a distinct derivative-only plan/freeze/approval; it never replays primary or auto-admits compensation. Finalize may consume only the newest exact approved-compensation request and retains active fences until all desired primaries, zero approved-source residue, unchanged source/public/support and protected occurrences, dynamic causal derivative proofs, and the completed final wrapper invocation/generation proof are current. The CLI/foundry own semantic review, canonical approval artifacts, raw in-memory permit custody, live plan/freeze/approval, and process-schema evidence; this repo never turns a historical oracle into execution authority.
- `tiangong-lca-worker` owns numeric-stability checks and the calculator report payload semantics
- `tiangong-lca-next` owns frontend env selection and app-side Supabase clients
- `tiangong-lca-edge-functions` owns Edge Function runtime orchestration, worker invocation, API response shape, deterministic Markdown generation for the four foundation datasets, and the exact table/column embedding allowlist
- `lca-workspace` owns root delivery completion after a child PR merges

If a task changes both schema and app behavior, the SQL truth still starts here.

## Common Misreads

- generated workspace files are not the durable schema source of truth
- GitHub default branch does not define the daily trunk
- a merged child PR does not finish workspace delivery
- `public.lca_jobs`, `public.lca_package_jobs`, and `public.dataset_review_submit_jobs` are not active or retained task surfaces after the `worker_jobs` cutover; use `worker_jobs`, retained domain result/artifact tables, and the archive table instead
- `lca_package_*`, LCA result/cache, and review-submit gate/coordinator tables are retained domain state, not leftover legacy job tables; clean them through domain retention contracts instead of dropping them as lifecycle tables

## Local Docpact Push Gate

This repository has a versioned local `pre-push` hook under `.githooks/pre-push` that delegates to `scripts/docpact-gate.sh`. The gate resolves the CLI through `scripts/docpact`, so local agent shells do not need bare `docpact` on `PATH`. The hook is a local developer guard for docpact config validation and enforced doc-governance linting; ordinary PRs and pushes rely on the local gate; `.github/workflows/ai-doc-lint.yml` is manual-dispatch fallback for remote reproduction.
