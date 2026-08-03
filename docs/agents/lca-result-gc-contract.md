---
title: LCA Result GC Database Contract
docType: contract
scope: repo
status: active
authoritative: true
owner: database-engine
language: en
whenToUse:
  - when implementing or reviewing lca.result_gc
  - when changing lca_results artifact identity or retention behavior
  - when qualifying result GC concurrency, ACL, or recovery semantics
whenToUpdate:
  - when the versioned result GC routines change
  - when the result artifact locator or partition identity changes
  - when rollout control or recovery semantics change
checkPaths:
  - docs/agents/lca-result-gc-contract.md
  - supabase/migrations/20260802201933_issue_398_result_gc_contract.sql
  - supabase/migrations/20260803090000_issue_398_result_gc_fk_indexes.sql
  - supabase/tests/20260802_issue_398_result_gc_contract.sql
  - scripts/test_issue_398_result_gc_runtime.py
lastReviewedAt: 2026-08-04
lastReviewedCommit: e1d467ce3d16ad2d09fed080a4a05e71736ca52e
lastReviewedNote: "Reviewed for Issue #407 Phase A exact-head governance: document-validation routines do not change result-GC runtime, deletion authority, lineage, or Contract gates."
related:
  - ../../AGENTS.md
  - ./repo-validation.md
  - ./repo-architecture.md
---

## Safety boundary

Result GC is an opt-in state machine, not a direct retention DELETE. Existing
rows remain legacy rows because `retention_partition_key` is nullable and
ordinary INSERT or UPDATE never populates it. A result becomes eligible only
after `private.worker_lca_result_gc_attest_v1(uuid)` verifies all of the
following facts:

- a canonical locator contains the exact `/results/<result UUID>/` path segment
- artifact SHA-256, byte size, and format are complete
- expiry, snapshot, Worker job, requester, and request hash are present
- the derived 64-hex partition hash can be frozen immutably

Once attested, the locator, partition identity, artifact identity, source job,
legacy job ID, result UUID, snapshot, and creation clock cannot change. An attested row cannot be deleted by
ordinary table DML, even when no claim is active.

The Worker commit reviewed for Issue #398 still generates
`<prefix>/snapshots/<snapshot UUID>/jobs/<job UUID>/<suffix>.<extension>` and
does not preallocate a result UUID. Those current writes must therefore remain
NULL/ineligible. Worker Issue #202 must first preallocate the result UUID and
generate the case-sensitive `/results/<result UUID>/` segment before it may call
attestation or before operators may enable claims. Endpoint, bucket, prefix, and
scheme are runtime configuration, so the database intentionally does not
hard-code them. Query strings, fragments, case-drifted `Results`, and a UUID at
the end of an unrelated path do not satisfy attestation.

## State machine

The Worker must use one current per-item token through this sequence:

1. `worker_lca_result_gc_preview_v1` audits without mutation.
2. `worker_lca_result_gc_claim_v1` returns independently fenced items.
3. `worker_lca_result_gc_fence_v1` rechecks all references and commits
   `claimed -> deleting`.
4. Only after that transaction commits may the Worker delete the frozen object.
5. `worker_lca_result_gc_finalize_v1` performs an exact compare-and-delete and
   persists the terminal audit row.
6. `worker_lca_result_gc_fail_v1` records failures. A deleting failure remains
   fenced and must be retried; it is never released as fresh work.

The database never performs an object-store or network request. `deleted` and
`missing` are equivalent successful object outcomes because object deletion is
idempotent. Same-token finalize replay is also idempotent.

Expired `claimed` and `deleting` leases are taken over by rotating the claim
token and incrementing the generation. An expired `deleting` operation resumes
at object deletion and never returns to a pre-delete phase. Stale tokens cannot
renew, fence, fail, or finalize.

The transaction-internal `finalizing` state and private finalize capability
exist only so the `ON DELETE SET NULL` audit FK can transition safely. The
capability is keyed by backend, transaction, operation, and token; callers have
no table privileges and cannot forge it with a session setting.

## Concurrency and references

Partition advisory locks serialize simultaneous fences in a deterministic
order. Active cache, latest-result, and package references lock the matching
operation before deciding whether a write is allowed. Therefore either the
reference commits first and the fence marks the item ineligible, or the fence
commits first and the reference is rejected. Concurrent result updates fail
closed without waiting into a reverse-lock deadlock.

The newest attested result in each partition is always ineligible. Pinned,
unexpired, shared-locator, legacy, incomplete, actively referenced, and newest
rows remain present.

## Roles and rollout

Only a real login that is a member of `lca_worker_runtime` can execute the seven
versioned routines. Those routines run as the dedicated NOLOGIN,
NOBYPASSRLS `lca_result_gc_executor`. Neither `service_role` nor the Worker role
has direct coordinator-table access.

Claims are disabled after migration. Enabling requires a separately reviewed
operator transaction that updates the singleton control row through the
executor role and records `enabled_at`, `enabled_by`, and a non-empty reason.
Attestation and preview may run while claims are disabled so rollout readiness
can be audited without deleting anything.

## Required local proof

Use an isolated disposable Supabase stack with a unique local project ID and
explicit non-default ports. The following commands assume the qualification
copy has `project_id = "database-engine-398-qualification"`, API port `60321`,
and database port `60322`; do not run them from the normal checkout or against
the repository's default 5532x stack:

```bash
issue398_workdir=/absolute/path/to/database-engine-398-qualification
supabase db reset --workdir "$issue398_workdir"
supabase test db \
  --workdir "$issue398_workdir" --local \
  "$issue398_workdir/supabase/tests/20260802_issue_398_result_gc_contract.sql"
read -r -s PGPASSWORD
export PGPASSWORD
issue398_database_url=postgresql://postgres@127.0.0.1:60322/postgres
python3 "$issue398_workdir/scripts/test_issue_398_result_gc_runtime.py" \
  --db-url "$issue398_database_url" \
  --confirm-isolated-destructive-test
unset PGPASSWORD
```

The Python test intentionally requires loopback plus an explicit destructive
confirmation. It proves real-login ACLs and the double-fence,
update-versus-fence, and reference-versus-fence races with separate database
sessions. It must never target a linked, hosted, persistent, or production
database.

The Issue #390 physical-move qualification v1 plan is separate from this GC
runtime proof. Its optional baseline capture is read-only and non-authorizing;
it does not enable claims, prove Worker locator adoption, or satisfy the GC
joint-qualification dependency. Physical DDL execution remains blocked until a
reviewed successor binds exact candidate artifacts and every pre-DDL gate is
independently complete.
