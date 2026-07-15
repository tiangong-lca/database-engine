# SQL performance profiles

These profiles are explicit, operator-run evidence. They are not pgTAP tests,
seed files, migrations, or production maintenance scripts.

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
