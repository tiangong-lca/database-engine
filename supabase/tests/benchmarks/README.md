# SQL performance profiles

These profiles are explicit, operator-run evidence. They are not pgTAP tests,
seed files, migrations, or production maintenance scripts.

## Hybrid-search staging profile

Issues #292 and #310 use a read-only profile against the persistent staging branch. It
selects deterministic real `state_code = 100` process and Product-flow rows,
keeps their text and 1024-dimensional embeddings inside SQL, and emits only
SHA-256 sample identifiers plus `EXPLAIN (ANALYZE, BUFFERS, WAL, SETTINGS)`
plans. The client and Edge Function defaults are fixed at threshold `0.5`,
match count `20`, lexical/semantic weights `0.5/0.5`, RRF `10`, and page
`10/1`.

Run only after independently confirming that the connection is the persistent
staging ref `submidrhbtknjxfympna` and the issue #292 migration is installed:

```bash
psql "$DATABASE_ENGINE_292_STAGING_URL" \
  -v benchmark_target=staging \
  -v expected_project_ref=submidrhbtknjxfympna \
  -v explain_output=/tmp/database-engine-292-hybrid-search-explain.log \
  -f supabase/tests/benchmarks/20260727_hybrid_search_staging_explain.sql
```

Never point this command at production. The profile starts a read-only
transaction and does not expose raw query text, UUIDs, or embeddings. Its
pre-change staging evidence was:

- full process RPC: 5.52 seconds, 35,001 shared hits and 11,515 reads
- full Product-flow RPC: 10.72 seconds, 76,275 shared hits and 29,281 reads
- broad flow semantic path: 9.06 seconds and a 103,259-row sequential scan
- Product-flow semantic path: 7.23 seconds and 13,036 type-index rows scanned
- process semantic path: 95.7 milliseconds warm and all 2,045 public vectors
  distance-sorted

The profile must be rerun on persistent staging after deployment. Compare its
five named plans with the baseline recorded in Issue #292; do not treat a
Preview branch with seed-only cardinality as equivalent evidence.

## Guarded alias production-cardinality profile

Run only against a reset local project or a disposable PR Preview branch after
the issue #254 migrations are installed:

```bash
psql 'postgresql://postgres:postgres@127.0.0.1:54322/postgres' \
  -v benchmark_target=local \
  -v explain_output=/tmp/database-engine-254-guarded-alias-explain.json \
  -f supabase/tests/benchmarks/20260715_guarded_alias_production_cardinality.sql
```

For a disposable Preview, replace the connection string and pass
`-v benchmark_target=preview`. Never point this command at persistent `dev` or
production.

The profile reads the existing physical cardinality, then inserts only enough
transaction-scoped rows to meet these production lower bounds:

- 483 flow properties
- 132,185 flows and 132,259 `flowProperty` nodes
- 42,369 processes and 837,020 exchange nodes

Its fixed UUID range yields exactly 23 flow candidates, 23 support-parent flow
candidates, 27 process candidates, and 59 matching exchange nodes. It captures
the support-parent, flow-closure, and process-closure scans with `EXPLAIN
(ANALYZE, BUFFERS, WAL, SETTINGS, FORMAT JSON)`. The profile requires the issue
#254 GIN indexes for candidate discovery and candidate-driven exact rechecks of
23 `flows_pkey`, 23 support-parent `flows_pkey`, and 27 `processes_pkey` rows. A
sequential live-table relookup or a live-table scan feeding a full-table Hash
input fails the profile; Hash Join remains enabled. The profile writes compact
machine-readable plans to `explain_output`, rolls back every fixture row and
temporary index option, and refreshes table statistics after rollback.
