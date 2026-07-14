# database-engine

`database-engine` is the Supabase database governance repository for the TianGong LCA workspace.

## AI Entry Docs

For AI-facing routing and current repo facts, start here:

- `AGENTS.md`
- `.docpact/config.yaml`
- `docs/agents/repo-validation.md`
- `docs/agents/repo-architecture.md`
- `docs/agents/supabase-branching.md`

It owns the checked-in database source of truth:

- `supabase/config.toml`
- `supabase/migrations/*.sql`
- `supabase/seed.sql`
- `supabase/seeds/*`
- `supabase/tests/*.sql`
- `.env.supabase.dev.local.example`
- `.env.supabase.main.local.example`
- Supabase branching and operations docs
- the GitHub Actions flow that pushes committed migrations to the persistent Supabase `dev` branch
- the production Supabase GitHub integration contract that applies Git `main` migrations automatically

It does **not** own:

- frontend runtime env files such as `.env` or `.env.development`
- app-side Supabase clients
- Edge Function runtime code
- ad-hoc manual dashboard changes as a long-term workflow

Those stay in consumer repos such as:

- `tiangong-lca-next` for frontend envs and app integration
- `tiangong-lca-edge-functions` for Edge Function runtime code

## Branch model

- GitHub default branch: `main`
- Daily trunk: `dev`
- Routine PR target: `dev`
- Promotion path: `dev -> main`

## Quick start

1. Start from the latest `dev`.
2. Create a feature branch from `dev`.
3. Use local Supabase for schema authoring and validation.
4. Commit migrations, seeds, tests, and config changes together.
5. Open the PR into `dev`.
6. Validate the Supabase preview branch created for the PR.
7. After merge, validate the persistent `dev` branch.
8. Promote `dev` to `main` when ready to release, then validate the production Supabase auto migration.

## Maintenance env files

This repository keeps the operator-side Supabase branch-binding templates:

- `.env.supabase.dev.local.example`
- `.env.supabase.main.local.example`

Copy them to `.env.supabase.dev.local` or `.env.supabase.main.local` for local-only credentials.
Those real `.local` files are ignored by Git because they may contain remote database passwords.
Frontend runtime env files stay in `tiangong-lca-next`.

## Embedding queue operations

Bulk imports can defer embedding queue fan-out through `util.embedding_queue_policy`.

Minimal operator sequence:

1. Set the target policy to `deferred`, for example `public / flows / embedding_ft / embedding_ft`.
2. Run the bulk import.
3. Confirm pending work in `util.pending_embedding_jobs`.
4. Enqueue backfill in bounded batches with `select util.enqueue_pending_embeddings(<limit>, 'public', 'flows', 'embedding_ft', 'embedding_ft');`.
5. Return the policy to `normal` after the queue is draining safely.

Use `paused` only as a stopgap when no new backfill should be enqueued. Jobs that exceed retry policy are recorded in `util.embedding_job_failures`.

## Guarded process derivative rebuilds

`cmd_dataset_derivative_rebuild_snapshot`, `cmd_dataset_derivative_rebuild_plan_guarded`, and `cmd_dataset_derivative_rebuild_read` are the complete authenticated V1 surface. They accept exactly one current-owner `state_code=0` process and rebuild only `extracted_md` plus `embedding_ft`.

Admission is asynchronous and reports `queued`, never completion. While active, the private coordinator freezes primary writes, quarantines both queued PGMQ work and already-claimed `pg_net` batches, and keeps the fence for at least 420 seconds around any possibly in-flight hosted Edge invocation. An already-claimed HTTP batch that mixes the target with unrelated rows is canceled as one worker batch; unrelated PGMQ messages are not deleted and retry after their visibility timeout, but may be delayed. Markdown remains private staging input until its request-correlated vector is ready; freshness is validated before the final Markdown/vector pair is exposed in one database update, while failure retains the prior pair. Admission takes a short `SHARE ROW EXCLUSIVE` lock on `processes` to serialize pre-fence writers, with a five-second lock timeout. Waiting requests are fairly rotated and one request-scoped coordinator error cannot roll back the rest of the batch.

Operators must use the owner read RPC to distinguish pending, completed, and fully drained failure states. Admission and terminal outcomes each append correlated audit records. Raw queue writes, worker helpers, and private coordinator calls are not supported client paths.

## Docs

- AI entrypoint: `AGENTS.md`
- Machine-readable governance: `.docpact/config.yaml`
- Validation guide: `docs/agents/repo-validation.md`
- Architecture notes: `docs/agents/repo-architecture.md`
- English: `docs/agents/supabase-branching.md`
- Chinese: `docs/agents/supabase-branching_CN.md`
