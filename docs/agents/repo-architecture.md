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
  - when adding or consuming the anonymous Portal public DTO boundary
whenToUpdate:
  - when major repo paths or authoring workflows change
  - when the generated schema workspace contract changes
  - when new hotspot areas make the current map misleading
checkPaths:
  - docs/agents/repo-architecture.md
  - .docpact/config.yaml
  - contracts/portal/**
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
lastReviewedAt: 2026-08-28
lastReviewedCommit: 26f583b
lastReviewedNote: "Reviewed for Issue #543 non-broad classification plus history-unique CAS examples over a summary-isolated exact index; Search semantics and writer ownership remain unchanged."
related:
  - ../../AGENTS.md
  - ../../.docpact/config.yaml
  - ./repo-validation.md
  - ./supabase-branching.md
---

## Repo Shape

This repo is organized around one checked-in Supabase project plus a generated schema-inspection workspace.

## Schema Boundaries

For LifecycleModel review and bundle operations, authoritative composition comes from ILCD `processInstance` references and exact-version `public.processes.model_id` ownership. `lifecyclemodels.json_tg` is persisted for frontend reconstruction only and must not define review closure, approval targets, publication admission, or deletion membership.

The application database uses five durable schemas with deliberately different
responsibilities:

- `public` contains only the nine stable domain entity tables: `processes`,
  `flows`, `contacts`, `sources`, `unitgroups`, `flowproperties`,
  `lciamethods`, `lifecyclemodels`, and `ilcd`
- `api` contains Data API RPCs and API-facing projections
- `private` contains internal application state and implementation routines
- `util` contains operational queues, controls, diagnostics, and maintenance helpers
- `archive` contains retained historical or retired data

PostgREST exposes `public` and `api`. The hosted deployment contract enforces
the ordered list `public,api,graphql_public`, so a profile-less request remains
on the core-entity surface. Data API consumers must still select their schema
explicitly: `public` for entity tables and `api` for RPCs. This avoids relying
on local CLI normalization, which may place a custom schema before `public`.
`private`, `util`, and `archive` are not exposed. `authenticated` has only the
`private.roles` and `private.reviews` reads needed to evaluate the preserved
public core-table RLS policies; browser roles receive no other internal
relation, routine, or write capability.

Every external `EXECUTE` grant on an `api` routine is an exact-signature entry
in `private.api_capability_grants`. That table records the owning capability ID
and admitted caller roles; migrations first remove inherited grants and then
rebuild the external ACL from this closed manifest. New or overloaded RPCs are
therefore denied until their exact signature is deliberately classified.

## Portal Public Read Boundary

`contracts/portal/**` owns the exhaustive versioned JSON Schemas for the anonymous Portal DTOs. `contracts/portal/generated/*.d.ts` is committed, deterministic output from those schemas; it is never a parallel handwritten contract. The matching `api.portal_*_v1` functions are additive façades: they fix visibility to public states 100/200, use stable keyset cursors, return canonical decimal strings, and recursively exclude actor/team/review fields, embeddings, credentials, private artifact locators, and database error detail. They do not replace or change legacy Search, raw-table RLS, Data Product, or Release consumers.

Portal capability policy is fail closed. State 200 and unknown, missing, exclusive, or conflicting license evidence remain metadata-only. Exchange values require an exact public Process/Flow/FlowProperty/UnitGroup support chain plus an explicitly open capability. LCIA values come only from typed Process/Impact/Value rows staged by an exact V3 Worker job under its active lease. Database independently binds the ordered certificate Process/Method axes, dense Cartesian grid, canonical decimals, int32be UTF-8 record/relation/content hashes, source artifact hashes, and the package's exact `portalProjectionId`/`portalProjectionContentHash`. Batches are capped at 500 records and 1 MiB serialized UTF-8 JSON; terminal rows are immutable and raw tables remain private.

Process Search, exact Detail, and Versions do not infer LCIA availability from state or metadata alone. A constrained decorator resolves the exact Process against the same current, finalized, non-revoked projection predicate used by the anonymous numeric reader. Search and Versions change only `capabilities.lciaVisible` while preserving strict item order and cursor; Detail also returns the locator-free publication/package/version/time and distinct sorted Method identities already allowed by `portal.public-dataset.v1`. Flow, state 200, unrelated, unprepared, unpublished, or revoked evidence remains `false`/`null`. One narrow executor-owned boolean definer reuses the existing license capability dependency graph without granting those helpers or raw tables to browser roles or postgres.

Package readiness remains a service-only V3 wrapper over the frozen V1/V2 insert helper. It parses the job contract and verifies authoritative result/latest ownership, strict artifact refs, packageResultHash/result/projection SHA equality, and deterministic default Impact before the compatibility schema switch or insert. The legacy call plus all post-insert checks execute inside a PL/pgSQL subtransaction: an invalid returned identity or any unexpected binding failure raises an internal condition that rolls back the new package, Worker resultRef, trigger effects, and temporary request-v2 schema mutation before the wrapper converts it to stable `projection_package_binding_invalid`. If the legacy insert instead reports `package_conflict` after an ambiguous response, the wrapper reads only the same build-job/package-version row and exact equality returns the same locator-free receipt with `reused=true`; any difference remains conflict, and the legacy helper bytes are unchanged. A process-restart readback uses the reclaimed job's current lease and the existing V3 job/closure predicate, then resolves the committed manifest's projection ID/content hash back to the same prepared projection. It reuses the same full binding validator, including authoritative result/latest rows and deterministic default Impact selection, but deliberately never requires the prior projection lease; missing packages return one stable 404 and drift returns conflict without package/projection mutation. Package publication is a separate authenticated-manager plan/command pair. Its prepare and command wrappers, plus projection prepare and finalize, invoke the same authoritative package binding validator before advancement; their retained V3 implementations are private owner-only functions, so an authoritative result drift cannot create a publication, pin the result, or finalize a projection binding. The read-only prepare result freezes the V3 package, projection axes/counts/hashes, current eligible Process-set hash, selected display default, and current-publication predecessor into `publishPlanHash`; the command locks publication state, recomputes and requires that exact hash, then records it for response-loss reconciliation. Projection finalization independently recomputes typed evidence and binds the exact current package publication. Anonymous visibility uses the same full predicate returned by manager readback as `isPubliclyVisible`; supersede, unpublish, or projection revoke makes the public RPC return unavailable rather than a synthetic zero. The public RPC is bounded and locator-free, while Calculation Bundle/result artifact storage remains private.

For Exchange support types whose TIDAS schemas do not carry a Process-style license field, `portal-capability-policy.v1` treats exact state-100 Flow, FlowProperty, and UnitGroup rows as the explicit support capability only after the containing Process passes the full-free license policy. State 200 never supplies numeric support. Public search matches only the projected allowlist, never `search_text`, `extracted_md`, URI, or stripped raw fields. Facet values use the same bounded normalization as filters and return at most 100 values per group with `hasMore` evidence.

The Portal executor is NOLOGIN/NOBYPASSRLS and receives only the minimum object privileges required by the façades. External wrapper ACLs are revoked from `PUBLIC` and classified by exact signature in `private.api_capability_grants`; raw core tables receive no new anon policy.

Flow semantic sparse-cardinality detection uses one narrow source-side partial
B-tree on `state_code` for state-100/200 rows whose `embedding_ft` is non-null.
It exists only to prove an exact 0..199 embedding universe without scanning the
wide Flow heap. Its low-selectivity key avoids a covering id/version exact-join
path. It contains no vector payload, does not replace the source HNSW ranking
index, does not change projection semantics, and has no speculative Process
counterpart because hosted Process evidence already meets budget.

Empty-query, empty-filter Portal Facets aggregate from an independent narrow
`private.portal_catalog_facet_rows_v1` projection keyed to the immutable
public-safe card row. Its own literal two-function manifest binds the exact five
emitted facet facts, while a parent trigger and `ON DELETE CASCADE` maintain the
child transactionally. Four UUID-quarter inserts and a short parent-first
reconcile fence populate existing rows without updating the wide card heap or
its PGroonga indexes. Only normalized `query = ''` and `filters = {}` dispatch
to the 32-MB bounded `GROUPING SETS` helper; every query or filter, including
classification and year ranges, retains the established card implementation.
The API signature, DTO, value ordering, 100-value cap, `hasMore`, owner, ACL,
and error contract are unchanged.

The anonymous homepage summary is a separate bounded read façade over those
same synchronized projections. Exact latest-visible counts and
`latestModifiedAt` come from the narrow facet rows. UUID, CAS, and
classification examples join one materialized latest-identity set back to the
exact public-safe card; their ordered scans use one combined partial B-tree
whose keys contain only dataset kind, identity, version, timestamp, and state.
The index predicate observes whether a card has regex-shaped Flow CAS or a
nonempty classification array, but neither the card nor any example value is
stored or included in the index. The façade revalidates CAS check digits,
classification bounds, labels, and exact latest identity before returning a
value. It adds no source-table scan, new projection table, existing-index
change, or trigger/writer branch.

The bounded sitemap surface stores one locator-free row for every public facet
version in `private.portal_sitemap_rows_v1`. Its primary key is
`(dataset_kind,id,version)`, and the same exact key references
`private.portal_catalog_facet_rows_v1` with `ON UPDATE RESTRICT` and
`ON DELETE CASCADE`. One `AFTER INSERT OR UPDATE` facet trigger performs an
exact-key upsert; DELETE maintenance belongs only to the FK cascade. Each
identity version receives the same stable six-bit bucket derived from
`MD5(dataset_kind || ':' || id)`; MD5 is only a distribution primitive, never
an authorization or integrity decision.

The history-covering B-tree is ordered by
`(shard_no,contract_version,dataset_kind,id,version DESC,modified_at DESC)`.
The shard reader filters one bucket and contract version, then uses
`DISTINCT ON (dataset_kind,id)` in that index order to select the current
version of each identity. Output remains fail-closed at 4,096 identities,
2 MiB, and the four-second function timeout, but physical scan work grows with
the number of retained public versions. The representative single-version
fixture contains 126,246 rows, has a largest shard of 2,066 identities, and
measured about 11 ms p95. The history-density probe expands 2,048 identities
to 64 versions each, or 131,072 exact rows; its natural plan must use the
history index as an index-only path, contain no `Sort` or `Incremental Sort`,
spill no temporary data, and finish below four seconds.

The manifest still emits exactly 64 ordered opaque shard cursors without
scanning or counting catalog rows. A shard cursor is accepted only when its
input bytes equal a fresh encoding of the exact expected four-field object, so
JSONB-equivalent numeric scales such as `1.0` or `64.0` fail with `22023`.
Capacity never constrains facet or sitemap-child writes: an overfull shard alone
becomes unavailable until a separately versioned reshard. No counter row,
source-table index, existing card/facet semantics, or retained
`portal_sitemap_entries_v1` façade changes.

Migration `20260827134103_portal_sitemap_concurrency_repair.sql` is the atomic
forward path for an earlier PR Preview winner-table shape. It locks the sole
facet writer, creates and fully backfills the exact-version shadow child,
rebinds the assertion and shard reader, and drops the obsolete winner table and
helpers in one transaction. Fresh databases already create the final child and
take the repair's no-drift path.

Edge consumers must obtain Data Product publication, package, and worker
metadata through bounded `api.svc_data_product_*` projections rather than
reading private relations. TIDAS package reads and import admission likewise
remain service-only capability contracts; their DTO and terminal error codes
are part of the consumer-facing façade and must be regression-tested.
LifecycleModel bundle authorization uses a service-only boolean review-admin
predicate, preserving service orchestration without returning membership rows.

Schema cutovers must preserve object identity and database dependencies with
`ALTER ... SET SCHEMA`; they must not rebuild tables, triggers, policies,
constraints, or functions merely to change namespaces. Stored function source
and fixed search paths must be rewritten in the same migration whenever they
refer to moved objects. The small set of security-invoker API search facades
that needs private RLS helpers runs through the constrained
`api_internal_executor` role, which cannot log in or bypass RLS.

## Auth Identity And Profile Boundary

`auth.users` is the authoritative registry for Supabase Auth identities.
`private.users` is an internal application-profile mirror keyed by the same
user ID; it carries mirrored `raw_user_meta_data` plus application-owned fields
such as `contact`. API lookups for registered users must therefore drive from
`auth.users` and may left-join `private.users` for optional profile data. They
must not treat a missing profile mirror as proof that the Auth identity is
absent.

A governed, deferred constraint trigger on `auth.users` mirrors inserts and
metadata updates into `private.users` and removes the profile after an Auth
identity is deleted. Forward migrations must also reconcile historical Auth
rows while preserving application-owned profile fields. The trigger helper is
`SECURITY DEFINER`, uses an empty fixed `search_path`, and is not directly
executable by application roles.

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
| `scripts/**` | export, refresh, change-copy, migration-generation, and workflow-contract helpers; `resolve_migration_head.py` is the single parser used to derive the current checkout's exact migration head for persistent-Dev verification |
| `.github/workflows/supabase-dev.yml` | local-contract rebuild, exact-check-gated PR Preview PostgREST/anonymous Hybrid verification, database-only persistent-Dev migration deployment with `db push --include-all`, and exact hosted verification |
| `supabase/workspace/changes/**` | manual overlay area used when generating migrations from workspace files |
| `supabase/workspace/remote_schema.sql` | generated full raw dump from the remote database |
| `supabase/workspace/global/**` | generated split-out global objects rebuilt on workspace refresh |
| `supabase/workspace/schemas/**` | generated human-browsable split schema objects rebuilt on workspace refresh |
| `supabase/workspace/database.types.ts` | generated TypeScript contract for the exposed `public` and `api` Data API schemas; CI rejects drift from the full local migration state |
| `contracts/portal/generated/*.d.ts` | generated TypeScript DTO modules from every exhaustive Portal JSON Schema; CI regenerates with exact-pinned `json-schema-to-typescript` and rejects drift |

## Branch Model In Practice

`database-engine` is an M2 repo:

- Git `dev` is the daily integration trunk
- Git `main` is the promoted release line
- PR branches map to Supabase preview branches
- the pull-request-only Preview job skips forks before authority, fails closed when a same-repository PR lacks the access token, main-parent ref, or persistent-Dev ref, requires exactly one successful check named `Supabase Preview` on the PR head from official Supabase App id `330661` and slug/owner `supabase`, captures the expected ref from its exact dashboard URL, and independently resolves one matching non-default/non-persistent BranchResponse from pinned CLI `branches list --output json`; equality plus main/Dev inequality gates the three-field PostgREST PATCH/readback. A dedicated Management API key step uses raw `disabled` state and exact public-key shape, clears its PAT/raw JSON after masked public-key export, and leaves the following anonymous Portal Hybrid step credential-free except for `apikey`; the job has no migration, Function, persistent-Dev, or production mutation path
- `.github/workflows/supabase-dev.yml` is the sole migration deployer for Git `dev`; it gates the remote job on the local contract, issues exactly one `db push --include-all`, applies one exact Management API PATCH limited to the three PostgREST runtime fields checked into `supabase/config.toml`, and verifies the exact hosted result without Functions deployment, broad config push, or any other Management API mutation
- after the database deployment succeeds, persistent-Dev Functions are deployed and validated through `tiangong-lca-edge-functions`; this repo contains no Function runtime source or Function deploy command
- the production Supabase project is migrated automatically by the Supabase GitHub integration when Git `main` advances

This means branch behavior is part of the repo architecture, not just delivery process.

## Test Proof Layers

SQL assertions own database semantics and ACL regressions. Offline Node contracts
own runner control flow and cleanup behavior; they do not replace exact-head
Hosted proof or independent Auth/SQL readback. Hosted mutation fixtures must be
bound to the intended disposable Preview and clean up only their own actors and
effects. The exact proof required for each capability lives in
`docs/agents/repo-validation.md`.

The PR Preview transport probe keeps failure evidence deliberately narrow: it
may report the HTTP status and a strict shape-validated PostgREST or SQLSTATE
code, but never the raw response body, error prose, request payload, or public
API key. This preserves enough evidence to distinguish schema-cache failures
from database exceptions without widening the credential or data boundary.

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

All seven dataset families use deterministic `extracted_md` as the source for
backpressured 1024-dimensional `embedding_ft` jobs. Database A retains the
legacy scalar lexical document and its seven PGroonga indexes. Database B adds
one explicit `search_text text[]` PGroonga index per dataset table and switches
formal lexical candidates plus the hybrid lexical branch to `search_text`; the
legacy `extracted_md` indexes remain until the separately tracked Database C
cleanup. Hybrid v2 RPCs expose one `lexical_weight`; the old two-weight
signatures remain only as an historical Expand-phase compatibility surface and
do not represent two lexical branches. The Contract migration retires those
signatures after all consumers have moved to v2. On the hosted branch, standard
`idx_scan` statistics and the Performance Advisor may continue to report a
PGroonga index as unused after a query plan has used it; a direct
`EXPLAIN (ANALYZE, BUFFERS)` naming the index is the required cross-check before
any retention decision. After Edge and Next consumer cutover, the fail-closed
Contract migration removes the legacy RPC signatures, seven `extracted_text`
columns/triggers/indexes, the rule-based text projection/backfill helpers, three
`embedding_flag` columns, three legacy `embedding_at` columns, and the obsolete
embedding-input/generation routines. It refuses to run while a guarded
derivative rebuild or non-FT embedding job is active and uses `RESTRICT` for
dependency-sensitive drops. `extracted_md`, `search_text`, `embedding_ft`,
`embedding_ft_at`, the seven `extracted_md` indexes, the seven `search_text`
indexes, and the HNSW indexes remain supported derivative surfaces until their
separately gated cleanup.

The Release 1 `search_text` projection is nullable `text[]` on all seven
dataset tables. Production verification established that the additive scalar
columns were empty, so the forward migration replaces each empty column with a
`text[]` catalog entry instead of rewriting the seven table heaps. It fails
before changing any column if a value, dependency, or column contract has
drifted; the later service/Edge backfill writes the full normalized projection.
Database B's source-switch migration is fail-closed for
existing rows with incomplete coverage and permits an empty new database; the
persistent-Dev and hosted cutover gate still requires Edge array deployment,
100% eligible-row backfill, and zero terminal failures. State 20/100 rows keep
authored content, review metadata, and `modified_at` immutable outside the
existing review-controlled command context; only `extracted_md`, `search_text`,
`embedding_ft`, and `embedding_ft_at` are derivative-write exceptions.

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

The four Data API Semantic and Hybrid RPC families share exact-regclass
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

The anonymous Portal Hybrid facade is a separate additive boundary, not a new
grant or parameter preset over those actor/team-aware raw Hybrid families.
`api.portal_hybrid_search_v1` accepts only Process or Flow, one to twelve
normalized model-generated terms, one exact finite 1024-dimensional embedding,
the fixed public-card filters, and a limit of 1..20. Search, Facets, and Hybrid
read one synchronized private card/document projection maintained from exact
Process/Flow content writes. Two projection PGroonga indexes serve lexical
candidates; semantic retrieval reuses the existing full source HNSW indexes and
never duplicates vectors or HNSW storage in the projection.

The `portal-hybrid-rank-v1` contract binds the latest visible state-100/200
identity exactly and excludes every false-positive identity. A filled semantic
pool uses bounded filtered source-HNSW with recall@20 and recall@200 of at least
0.95; raw ANN underfill takes an exact-distance fallback. Threshold, weights,
RRF constant, and score/id/version tie-breaks are deterministic for the actual
candidate pool. ACL-closed Process/Flow exact helpers own the full fallback:
they use one 32-MB-per-node workspace, hash the latest projection keys against
one source scan, force Hash Join while disabling nested/merge joins, JIT, and parallel workers, and
restore all caller GUCs through function configuration. The release benchmark invokes those exact
helpers directly at full vector cardinality, while raw ANN counts separately
record the naturally selected runtime branch. A second formal ANN plan mirrors
the production 5,000-candidate scan and 200-candidate rerank; its time and
buffers are added to the direct exact plan. Production-cardinality release,
zero-vector, and 199-vector profiles retain the 6-second p95, sub-8-second
per-call, direct-exact 5-second, combined ANN-plus-exact 6-second, zero-spill,
and reviewed shared-buffer ceilings; the Edge admission
gate bounds concurrency before R2 is enabled.

Projection rows bind an immutable v1 derivation-contract version. Its registry
stores a committed SHA-256 over the exact transitive card/document helper
closure, and each Portal read checks the live manifest once before using the
projection. The v1 helpers and registry row are immutable: any semantic change
requires a new helper closure, shadow projection, bounded backfill, concurrent
indexes, short reconcile fence, and atomic read cutover. Updating a digest or
rebuilding v1 rows in place is not a valid migration path.

Search and Hybrid card context is a bounded composition layer rather than a
stored-card mutation. After the existing candidate path fixes ordering and
applies its 50-item Search or 20-item Hybrid limit, one shared helper resolves
each exact `kind/id/version` source row and reuses only the public-dataset
allowlist helpers for reference product/property, complete functional unit,
technology, source, and dataset-authored review status. An independent live
manifest protects that helper/decorator closure. It creates no second
full-cardinality projection, index, policy, source trigger, or writer work;
missing source identity or helper drift fails the wrapper closed. A future
change to stored v1 card/document semantics still requires shadow-v2 rollout.

The representative benchmark exercises this composition with complete
evidence, not placeholder nulls: exact Process→Flow and shared
Flow→FlowProperty→UnitGroup support, public source/database identity,
technology, and review status are all present. Dedicated Search-50 and
Hybrid-20 labels record 20-sample p95 plus full wrapper
`EXPLAIN (ANALYZE, BUFFERS)` under the existing time, shared-buffer, and
recorded temp-buffer evidence. Existing zero-spill gates remain scoped to the
narrow empty/exact phases that own them. These fixtures remain rollback-only test data and add no
runtime relation, index, trigger, or writer path.

The measured empty-query, geography-only Flow Search has one deliberately
narrow query fast path. It reads the already synchronized facet child through
the existing latest-key B-tree, applies exact geography/cursor/order/limit on
those narrow rows, and joins at most 51 exact parent cards. It first validates
the independent live Facet manifest on every matching request. It does not add a
geography index—the representative filter matches the full Flow universe, so
such an index would add writer/storage cost without selectivity—and all other
Search filters continue through the general exhaustive card-facts path.

The catalog summary is a bounded consumer of the same synchronized projections,
not a separate search index. UUID and CAS examples keep their exact evidence
rules. A classification example is optional, requires a trimmed public code of
at least four characters, and prefers Process evidence before Flow evidence so
the homepage does not advertise a one-character near-universe query. The
representative benchmark resolves that emitted example dynamically and executes
it through the matching public Search wrapper for 20 samples under the existing
2-second p95 and 8-second hard timeout. Missing eligible evidence omits the
example; it never authorizes a placeholder, private fallback, timeout increase,
new writer path, or unmeasured index.

This contract assumes privileged DDL is delivered only through the governed
migration and CI path. The live digest proves current definitions, not the
historical derivation of a row after out-of-band change/write/restore. Direct or
dynamic privileged DDL is outside the supported safety model. If it may have
overlapped a source write or backfill, restoring old bytes does not make v1
trusted again; reads stay disabled operationally and recovery uses a shadow v2
rebuild/cutover.

The facade never invokes or relaxes the legacy raw Hybrid or semantic helpers
and exposes no actor, team, state, data-source, cursor, model, weight, threshold,
raw document, embedding, or locator control. Representative-cardinality plans,
advisors, and the complete one-to-twelve-term/filter/sort/cursor input matrix
remain promotion gates.

## Worker Jobs And Domain State

`worker_jobs` is the canonical lifecycle and queue-control table for work that cannot be safely carried by Edge Function request/response execution.

The reusable AI runtime uses `worker_queue=ai`; `ai.tidas_suggestion` is only its first versioned job kind. `api.svc_ai_tidas_suggestion_enqueue` validates the exact Process/Flow v1 envelope, computes the canonical request hash, and reuses only identical active requests. `api.svc_ai_tidas_suggestion_read` requires the original requester and exposes the non-internal Worker projection. Both facades are service-only: Edge owns user authentication, while authenticated clients never enqueue or read `private.worker_jobs` directly. AI results remain advisory and create no Process/Flow domain mutation.

Claim must remain non-blocking under concurrent recovery: expired max-attempt rows are selected in bounded `FOR UPDATE SKIP LOCKED` batches before they are marked failed, while claimable queued/stale or expired-retry rows use their own skip-locked candidate set. Terminal result recording is lease-fenced; an exact repeat with the same lease token, status, and normalized result content is an idempotent acknowledgement, while any conflicting replay remains rejected. This permits a Worker to retry an ambiguous database/transport failure without leaving completed compute stranded in `running`.

Review submission itself never enqueues or waits for Worker computation. A Review Admin may manually start a global `review.quality_diagnostic` job that evaluates completeness and numerical quality together. The report is read-only and informational: findings, inability to evaluate, and execution failure do not mutate review state or block assignment, approval, or rejection. Review Members cannot start or read this administrative report, and the job does not enter the ordinary task-center feed.

Retained domain tables such as `lca_package_artifacts`, `lca_package_export_items`, `lca_package_request_cache`, `lca_results`, `lca_result_cache`, `lca_latest_all_unit_results`, and `lca_network_snapshots` are not replacement job tables. They store worker-produced artifacts, caches, projections, reports, or coordinator domain state. `dataset_review_submit_requests` and `dataset_review_submit_gate_runs` are legacy compatibility/audit history only; they no longer authorize or reject review submission. The package request cache deduplicates active work for mutable scopes (`current_user`, `open_data`, and `current_user_and_open_data`), but a new intent after completion must advance to a fresh Worker/package job; only `selected_roots`, whose exact root IDs and versions are request content, retains terminal artifact reuse. Post-cutover rows should be traceable back to `worker_jobs` through the appropriate worker job reference columns, except for explicitly documented exceptions such as snapshot identity rows that are traced through downstream worker-linked records.

Use `private.worker_domain_traceability_cutoffs` and
`util.worker_domain_traceability_violations` for DB-side audit checks when
validating that new worker-produced domain rows remain traceable.

## LCI/LCIA Release Control Plane

`lca_release_runs` is the durable release state machine; `lca_release_dataset_versions`, `lca_release_artifacts`, `lca_release_approvals`, and `lca_release_publications` are immutable/indexed release facts. The dataset index binds every generated identity to its exact source Process and requires exactly one Unit Process, LifecycleModel, and Result Process per source identity; the Unit Process mapping must point to itself. Generated LifecycleModel and Result Process documents are referenced from canonical object artifacts and never inserted into editable `lifecyclemodels` or `processes` authoring rows.

Authenticated prepare, approve, publish, readback, and unpublish commands re-check `auth.uid()` against the live `data_product_manager` platform role. The separate service-only finalize command binds four uploaded package refs to the exact prepared plan and validated release manifest, but service identity has neither direct table writes nor approval/publication function grants. Public and private read/download projections remain RPC-owned so Edge can issue signed downloads without exposing database mutation capability.

The manager-authorized Calculation Bundle projection reads the immutable bundle reference already persisted in the result package. Legacy packages may have no semantic downloads. New packages must carry exactly five unique `tiangong.calculation-download.v1` roles: LCIA XLSX/CSV, LCI Parquet/CSV, and the whole-bundle audit ZIP. Database validates each fixed role/group/filename/media-type tuple plus hash, size, count, and object reference, removes those locators from the returned bundle object, and exposes them only as a separate `productDownloads` projection for Edge signing. Canonical manifest shards remain bundle evidence and are not duplicated into database rows.

## Scope-Closure Snapshot Sources

The data-product completeness check can run before the first formal `lca_release_publications` row exists. When a current release exists, normalization and the immutable `lcia.scope-closure-data-snapshot.v2` manifest remain bound to its exact `lca_release_dataset_versions`. Before the first release, the database instead freezes every exact `state_code 100..199` Process, Flow, FlowProperty, UnitGroup, Source, Contact, and LifecycleModel document readable by the deployed closure Worker, plus the exact reviewed 25-method LCIA allowlist (whose production authoring rows intentionally remain `state_code=0`). Global Process roots select the latest eligible version per UUID, while the frozen support universe retains all exact eligible versions so explicit transitive references remain resolvable.

Candidate snapshots compute `canonicalContentHash` with the same recursively key-sorted, compact JSON algorithm used by the Worker and normalize the reviewed LCIA method/artifact-locator alias before freezing identities. Because the initial production universe exceeds 120,000 exact rows, migration backfill pays the canonical-hash cost once into a private cache; eight table triggers then refresh only changed identities, while interactive requests aggregate the cached manifest inside the authenticated role timeout. This one-time production-volume administrative statement must declare a bounded session `statement_timeout` above the platform's ordinary two-minute cap and restore the default immediately after the backfill; a small Preview dataset is not sufficient evidence for that bound. `candidateData.sourceKind=candidate-public-state` is the authoritative source discriminator. The zero-UUID `currentPublicRelease` object is only a deterministic compatibility projection required by the deployed Worker v2 schema and must never be treated as publication evidence. `current-membership-required-v1` continues to require a real current release; the default frozen-artifact policy may consume a candidate snapshot.

Certificate-grade Scope Closure normalization canonicalizes omitted and supported legacy `closed/open/cutoff` technosphere boundary inputs to `cutoff` before requested-scope, policy, snapshot, and request fingerprints are computed. Unknown values remain invalid. The production-main hotfix targets the pre-cutover `public` normalizer and is intentionally a no-op after that function has moved to `private`; the dev-line migration owns the equivalent post-cutover definition, and the matching Worker contract accepts only the canonical cutoff input. Historical frozen requests and artifacts are not rewritten.

A passed reused Scope Closure certificate owns a new target report while retaining the direct source check's immutable numerical snapshot and Closure Bundle. Calculation admission, Worker binding, and package publication validate that single-hop source through `reused_from_check_id`; they never relabel or copy the source Bundle. The target and source must retain exact scope, policy, data-snapshot, numerical-snapshot, Bundle identity, and checksum parity. Independently, `worker_jobs.status = completed` implies canonical `progress = 1`; blocked and failed terminal states retain their actual partial progress.

The current internal scanner cache identity is `scope-closure-validator-scanner.v1+cutoff-readiness-r4`; changing it creates a distinct request fingerprint and scan execution without changing any public V1 DTO or artifact schema. The r2 revision separates exact-identity Process lineage traversal from numerical provider-universe enforcement; r3 excludes Source `referenceToDigitalFile` attachment locators from dataset-reference scanning; r4 prevents reuse of an r3 completed blocked scan after Worker changes certificate-grade medium singular risk from blocker to warning. The database does not interpret readiness severity. Historical requests retain their recorded revision for in-flight compatibility. A completed `blocked` scan has complete reusable administrative evidence but deliberately no numerical snapshot, even though its scan-execution row retains a preallocated `numerical_snapshot_id`. Reuse therefore compares that ID with the source check snapshot only for `passed` certificate evidence; all immutable scope, policy, data-snapshot, completion, and source-status fences still apply to both terminal outcomes.

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

Remote `dev` is the canonical refresh target. Refresh all application schemas
with `--schemas public api private util archive`. `--environment local` is a
validation and recovery path that uses the Supabase CLI-native local
connection; locally reconstructed output must not be represented as hosted
truth without matching migration-version and targeted hosted catalog evidence.

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

- `database-engine` owns persisted database state, API façades, authorization, transaction boundaries, queue/control-plane state, audit facts, and final database assertions
- protected mutation workflows are authoritative here only for database admission, fencing, atomic effects, and readback; semantic planning, approval-artifact construction, and client-side authority custody remain with their owning consumers
- durable release and scope-closure facts live here, while generated TIDAS/ILCD bytes and Storage object operations remain outside SQL
- `tiangong-lca-worker` owns the combined completeness/numerical diagnostic and its versioned report payload semantics
- `tiangong-lca-next` owns frontend env selection and app-side Supabase clients
- `tiangong-lca-edge-functions` owns Review Admin-only diagnostic orchestration, Worker invocation, API response shape, deterministic Markdown generation for the four foundation datasets, and the exact table/column embedding allowlist
- `lca-workspace` owns root delivery completion after a child PR merges

If a task changes both schema and app behavior, the SQL truth still starts here.

## Common Misreads

- generated workspace files are not the durable schema source of truth
- GitHub default branch does not define the daily trunk
- a merged child PR does not finish workspace delivery
- `public.lca_jobs`, `public.lca_package_jobs`, and `public.dataset_review_submit_jobs` are not active or retained task surfaces after the `worker_jobs` cutover; use `worker_jobs`, retained domain result/artifact tables, and the archive table instead
- `lca_package_*` and LCA result/cache tables are retained domain state; review-submit Gate/coordinator tables are legacy compatibility/audit history and have no submission authority

## Local Docpact Push Gate

This repository has a versioned local `pre-push` hook under `.githooks/pre-push` that delegates to `scripts/docpact-gate.sh`. The gate resolves the CLI through `scripts/docpact`, so local agent shells do not need bare `docpact` on `PATH`. The hook is a local developer guard for docpact config validation and enforced doc-governance linting; ordinary PRs and pushes rely on the local gate; `.github/workflows/ai-doc-lint.yml` is manual-dispatch fallback for remote reproduction.
