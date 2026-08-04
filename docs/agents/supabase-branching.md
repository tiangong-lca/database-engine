---
title: Supabase Branching
docType: guide
scope: repo
status: active
authoritative: false
owner: database-engine
language: en
whenToUse:
  - when changing Supabase branch bindings, preview behavior, or persistent dev automation
  - when confirming the branch-specific database workflow for this repo
whenToUpdate:
  - when branch bindings, Vault secret rules, or the persistent dev deployment path change
  - when branch-operation guidance diverges from repo contract or validation guidance
checkPaths:
  - docs/agents/supabase-branching.md
  - AGENTS.md
  - .docpact/config.yaml
  - supabase/config.toml
  - .github/workflows/supabase-dev.yml
  - .env.supabase.dev.local.example
  - .env.supabase.main.local.example
lastReviewedAt: 2026-08-01
lastReviewedCommit: b32072d5a38509c4a25d866692958f0ced1303cf
lastReviewedNote: "Reviewed Issue #346: persistent dev explicitly serializes migration and allowlisted PostgREST config while production remains Supabase-integration owned."
related:
  - ../../AGENTS.md
  - ../../.docpact/config.yaml
  - ./repo-validation.md
  - ./repo-architecture.md
  - ./supabase-branching_CN.md
---

# Supabase Branching

`database-engine` is the single Supabase source-of-truth repository for the TianGong LCA workspace.

This repository owns:

- `supabase/config.toml`
- `supabase/migrations/*.sql`
- `supabase/seed.sql`
- `supabase/seeds/*`
- `supabase/tests/*.sql`
- `.env.supabase.dev.local.example`
- `.env.supabase.main.local.example`
- `.github/workflows/supabase-dev.yml`
- branching and operations documentation for database delivery

This repository does **not** own:

- frontend runtime env files such as `.env` or `.env.development`
- app-side Supabase client code
- Edge Function runtime code

Those stay in consumer repositories such as `tiangong-lca-next` and `tiangong-lca-edge-functions`.

## Branch contract

- Git `main` -> production baseline migrated automatically by the Supabase GitHub integration
- Git `dev` -> persistent Supabase `dev` branch migrated by `.github/workflows/supabase-dev.yml`
- PR / feature branches -> preview branches created by the Supabase GitHub integration

Rules:

- GitHub default branch remains `main` as a platform exception.
- Daily trunk is Git `dev`.
- Routine feature and fix branches start from `dev` and PR back into `dev`.
- `dev -> main` is the promotion path.
- Do not infer the working trunk from GitHub default-branch UI alone.

When review changes an already-applied PR migration, add a later migration that reapplies the authoritative final schema/functions for the existing Preview branch. Still retain and run a real populated canonical-base-to-head upgrade test, because the additive Preview repair does not by itself prove first-time production upgrade safety.

## Repository contract

- Keep one shared `supabase/` directory in Git.
- Treat committed files in `supabase/migrations/` as the schema source of truth for production, `dev`, and preview branches.
- Keep branch-specific overrides in `[remotes.<branch>]` inside `supabase/config.toml`.
- Do not create a separate `supabase/` directory per Git branch.
- Keep `.github/workflows/supabase-dev.yml` as the only GitHub Actions flow that mutates the persistent Supabase `dev` branch. It serializes deployments, runs `supabase db push` first, then calls the repository-owned allowlisted PostgREST config gate.
- Do not add a checked-in GitHub Actions production deploy for Git `main`; the production project is migrated by the Supabase GitHub integration bound to this repository.
- Do not author normal schema changes by editing the remote database first and reconstructing migrations later.
- Keep Data API schemas configuration-as-code: expose `api`, `public`, and `graphql_public`; never add `private`, `util`, or `archive` to exposed schemas or `extra_search_path`.
- Supabase's GitHub deployment DAG applies `Configure` before `Migrate`. Therefore, first deploy and verify a new schema and its API objects with the existing exposure configuration; only a later commit/PR may expose that already-hosted schema. Never introduce a schema and expose it in the same deployment.
- `scripts/apply_postgrest_config.py` is the persistent-dev exception to the generic Supabase integration path. It resolves exactly one `[remotes.*].project_id`, allows only `db_schema`, `db_extra_search_path`, and `max_rows`, PATCHes only drifted fields, and GET-verifies the result. It must not read, patch, or log `jwt_secret` or unrelated service configuration.
- Do not use an unconditional `supabase config push` in CI. It reconciles the whole local/remote config surface and can overwrite unrelated Auth, Storage, or Realtime drift.

## Files to maintain

- `supabase/config.toml`: shared baseline plus `[remotes.dev]`
- `.github/workflows/supabase-dev.yml`: serializes and pushes committed migrations, then allowlisted PostgREST configuration, to persistent Supabase `dev`
- `scripts/apply_postgrest_config.py`: fail-closed persistent-dev PostgREST diff/apply/readback gate
- `supabase/migrations/*.sql`: committed migration history
- `supabase/seed.sql`: shared seed data
- `supabase/seeds/dev.sql`: optional persistent-dev-only seed data
- `supabase/tests/*.sql`: database assertions and safety checks
- `.env.supabase.dev.local.example`: template for the persistent `dev` branch binding
- `.env.supabase.main.local.example`: template for the `main` branch binding
- `docs/agents/supabase-branching.md`: English branching workflow
- `docs/agents/supabase-branching_CN.md`: Chinese branching workflow

Frontend consumer-repo env files are intentionally **not** maintained here.

## Operator env files

Keep the branch-binding templates at the repository root:

- `.env.supabase.dev.local.example`
- `.env.supabase.main.local.example`

Usage rules:

- Copy them to `.env.supabase.dev.local` or `.env.supabase.main.local` for local-only secrets.
- Do not commit the real `.local` files.
- Use them for operator workflows that need `SUPABASE_PROJECT_REF` or `SUPABASE_DB_URL` for the persistent `dev` or `main` branches.
- Frontend `.env` or `.env.development` files still belong in consumer repos such as `tiangong-lca-next`.

## GitHub integration and secrets

Supabase GitHub integration for the production project must point to:

- repository: `tiangong-lca/database-engine`
- relative path: `supabase`

This integration applies committed migrations to the production project automatically
when Git `main` advances. Absence of a checked-in GitHub Actions workflow for
`main` does not mean production migration is manual-only.

Repository configuration expected by `.github/workflows/supabase-dev.yml`:

- variable `SUPABASE_DEV_PROJECT_ID`
- secret `SUPABASE_ACCESS_TOKEN`
- secret `SUPABASE_DEV_DB_PASSWORD`

## PR to Supabase migration path

Committed migration files do not affect any remote database until one of the
deployment paths below runs.

Normal PR path:

1. A feature branch includes new files under `supabase/migrations/`.
2. The PR targets Git `dev`.
3. Supabase GitHub integration creates or updates the PR preview branch from
   the checked-in `supabase/` directory.
4. The preview branch is PR-scoped proof only; it is not the persistent
   Supabase `dev` branch.
5. After the PR merges, the resulting push to Git `dev` triggers
   `.github/workflows/supabase-dev.yml`.
6. The workflow links to `SUPABASE_DEV_PROJECT_ID` and runs `supabase db push --include-all`.
7. Only after migrations succeed, the workflow runs `scripts/apply_postgrest_config.py --apply` against the same exact ref.
8. The gate computes only the three allowlisted PostgREST fields, PATCHes drift, and GET-readbacks the result; a mismatch fails the workflow.

The Supabase GitHub integration continues to own production `main`. Management API action history for this repository contains `main` integration runs but no `dev` integration run, so `[remotes.dev]` is a binding contract for the repository workflow, not an independent persistent-dev deployment mechanism.

An existing Preview branch applies newly added migration files on later PR
pushes. Editing a migration already recorded in that Preview's migration
history does not reapply it. Ship an additive follow-up migration for forward
changes; use explicit Preview branch reprovision only when the intent is to
discard and rebuild that disposable branch state.

`--include-all` means every committed migration absent from remote history is
eligible for application. It is required when a governed `main -> dev`
backmerge introduces a migration whose timestamp precedes newer migrations
already recorded on persistent `dev`; migrations already present in remote
history are still skipped.

Promote path:

1. A `dev -> main` promote PR merges into Git `main`.
2. The production project's Supabase GitHub integration reads the checked-in
   `supabase/` directory from Git `main`.
3. Pending checked-in migrations are applied automatically to the production
   project.
4. Operators validate production migration state and application behavior after
   the promote merge.

This repository currently has no checked-in `workflow_dispatch` production
deploy for Supabase. That is intentional: Git `main` is handled by the Supabase
GitHub integration. An operator can still run `supabase link` and
`supabase db push` locally as an explicit fallback or recovery path, but that
manual action must be recorded in validation or incident notes.

## Vault secret contract

Database-side functions or triggers that call Edge Functions must read branch-specific Vault secrets.

Current standard names:

- `project_url`
- `project_secret_key`
- `project_x_key` only for the legacy `generate_flow_embedding()` compatibility path

Rules:

- Never hardcode branch URLs or service keys in SQL, migrations, or dumped baseline files.
- Treat the values as branch-specific. `main`, persistent `dev`, and any preview branch that needs webhook execution must each have the required secrets.
- If a branch is recreated or relinked, re-check the Vault entries before testing webhook-driven flows.

## Hosted Data API security operator gate

During the schema-boundary Expand phase, hosted PostgREST must expose exactly
`api,public,graphql_public` in the reviewed order. Keeping `public` is an explicit compatibility
decision; `private`, `util`, and `archive` are never exposed. The repository
config and representative `api` objects are owned by their implementation
change, while Issue #339 supplies the independent Management API readback and
negative REST gate in the read-only `scripts/hosted_security_acl.py`. Configuration
mutation remains exclusively behind `scripts/apply_postgrest_config.py` and its
allowlisted diff, readback, and rollback gate.

Public search wrappers that require `private` helpers run through the
non-login, non-BYPASSRLS `api_internal_executor`, which inherits authenticated
transport prerequisites while retaining its own RLS-bound identity; browser roles
receive no direct `private` USAGE or EXECUTE. Two lifecycle bundle RPCs remain
authenticated compatibility contracts. Expand records that fact instead of
silently breaking callers; Contract removes them after consumer-zero evidence.

Migrations close default privileges owned by `postgres`. Supabase's internal
`supabase_admin` defaults cannot be altered by the migration owner; an
authorized owner session must run
`supabase/operator/issue_339_supabase_admin_default_privileges.sql`, then the
hosted gate must report `hostedOperatorReady=true`. Until both exposed-schema
and owner-default readbacks pass, Preview/dev/production security proof is
blocked. Never treat a local config value or SQL catalog check alone as hosted
Data API exposure evidence.

## Default workflow

### Routine schema change

1. Sync local Git `dev`.
2. Create a feature branch from `dev`.
3. Start local Supabase.
4. Make schema changes locally.
5. Create a migration with `supabase migration new <name>` or `supabase db diff -f <name>`.
6. Validate with `supabase db reset` and the relevant SQL tests.
7. Commit migrations, seeds, tests, and config together.
8. Open the PR into Git `dev`.
9. Let Supabase create or update the preview branch for that PR.
10. After merge, validate the persistent remote `dev` branch.
11. Promote `dev` to `main` when ready to release.
12. Validate that the production Supabase project was migrated automatically by
    the Supabase GitHub integration.

### Persistent `dev` branch deployment

- Pushes to Git `dev` trigger `.github/workflows/supabase-dev.yml`.
- That workflow links to the persistent Supabase `dev` branch and runs `supabase db push --include-all` so governed backmerges can apply every committed migration missing from remote history, including older-timestamped entries.
- Do not add a second automation path that pushes the same target.

### Production `main` deployment

- Pushes to Git `main` are handled by the production project's Supabase GitHub integration.
- The integration watches repository `tiangong-lca/database-engine` with relative path `supabase`.
- Checked-in pending migrations are applied automatically to the production project when `main` advances.
- Do not treat the missing checked-in GitHub Actions workflow for `main` as a manual-deploy requirement.
- Use local `supabase db push` only as an explicit fallback or recovery path, and record that action.

### Hotfix flow

1. Branch from Git `main`.
2. Fix the issue.
3. Merge back to `main`.
4. Back-merge `main` into `dev`.
5. Keep migration history aligned across both long-lived branches.

## Consumer repo boundaries

Use `database-engine` for:

- schema, policy, SQL function, trigger, seed, and config changes
- preview / persistent branch behavior
- database-side tests and migration recovery

Use consumer repos for:

- frontend env selection and app-side Supabase clients
- Edge Function runtime implementation
- app-level validation against `dev`, preview, or `main`

If a task changes both database schema and application behavior, the database change still starts here.

## Recovery rules

- If local and remote migration histories diverge, inspect them with `supabase migration list` before changing anything else.
- Use `supabase db pull` only to baseline an existing remote schema or to capture remote-only drift back into Git.
- If a branch reaches `MIGRATIONS_FAILED`, fix the migration in Git and prefer recreating the failed branch over hand-editing remote state.
- If remote history metadata is wrong, use `supabase migration repair` deliberately and then re-verify the result.

## Local commands

Use the Supabase CLI in this repository.

- `supabase start`
- `supabase db diff -f <name>`
- `supabase migration new <name>`
- `supabase db reset`
- `supabase migration list`
- `supabase link --project-ref <ref>`
- `supabase db push`
