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
lastReviewedAt: 2026-08-08
lastReviewedCommit: 1d1d153edb92aa01dd5fb7717441b16bedc4a96b
lastReviewedNote: "Updated for Issue #422: added the mandatory Edge-owned post-database redeploy and readback gate for persistent Dev."
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
- Git `dev` -> persistent Supabase `dev` branch migrated and verified by `.github/workflows/supabase-dev.yml`
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
- Keep `.github/workflows/supabase-dev.yml` as the sole persistent-`dev` migration deployer. It may run `supabase link` and exactly one `supabase db push --include-all`, but must not deploy/delete Edge Functions or push project configuration.
- The target state is to disable persistent-Dev Function synchronization from the Supabase native Git binding. Until the platform binding can enforce that boundary, apply the mandatory post-database Edge Functions gate below after every Git `dev` database deployment.
- Do not add a checked-in GitHub Actions production deploy for Git `main`; the production project is migrated by the Supabase GitHub integration bound to this repository.
- Do not author normal schema changes by editing the remote database first and reconstructing migrations later.

### Edge Function ownership verification

- A successful `Supabase Preview` check proves that the native branch workflow ran; it does not by itself prove that deployed Edge Function content changed.
- `tiangong-lca-edge-functions` remains the source of truth and deployer for Edge Function runtime code. This repository must not add or deploy Edge Function sources.
- To determine whether a database-native run changed the persistent Dev Functions, capture the same sorted inventory of Edge-repo-owned function slugs and their hosted content hashes immediately before and after the run, then compare the deterministic inventory digest.
- An unchanged digest means the owned Function content was preserved. Treat a changed digest, owned-function inventory, `verify_jwt` setting, or active state as an ownership-boundary failure that requires investigation.

### Post-database Edge Functions gate

This gate is mandatory after every database-engine push to Git `dev` while the
Supabase native Git binding can still replace persistent-Dev Functions. A
successful database workflow is necessary, but persistent Dev is not
operationally complete until this gate passes.

1. Wait for `.github/workflows/supabase-dev.yml` to finish successfully, including its exact migration-head, hosted PostgREST, and Data API readbacks.
2. Select the exact reviewed `tiangong-lca-edge-functions` Git SHA intended for persistent Dev. Do not infer Function source from a database-engine commit or a hosted bundle.
3. From that Edge checkout, use the published Function inventory and the official `npm run deploy:dev -- <function-names...>` entrypoint documented in `tiangong-lca-edge-functions/README.md`. That command is implemented by `scripts/deploy-function.cjs`; do not reconstruct its CLI flags here, omit the explicit names, or use `--prune`.
4. Read back the hosted inventory and prove that every expected Edge-owned Function exists, is `ACTIVE`, and has the `verify_jwt` setting required by the Edge deployment contract. Prove that no unexpected foreign bundle source remains and that remote-only legacy Functions outside the reviewed inventory were unchanged.
5. Run representative authentication and invalid-payload probes against the restored Dev Functions, and record the exact Edge SHA, inventory, deployment result, and readback evidence on the delivery Issue or PR.

A pre/post deterministic content digest is useful for detecting whether the
native run changed hosted Functions. It is not a requirement that a fresh
redeploy reproduce an older bundle digest: rebundling or toolchain changes can
change the hosted hash without changing the reviewed source. After redeploy,
the authoritative proof is the exact Edge source SHA plus inventory, active
state, auth-setting, foreign-source, legacy-preservation, and smoke readbacks.

This sequencing rule does not transfer Edge runtime ownership into
`database-engine`. The Edge repository remains the command and source owner;
this document owns only the database-to-Edge completion gate.

## Files to maintain

- `supabase/config.toml`: shared baseline plus `[remotes.dev]`
- `.github/workflows/supabase-dev.yml`: rebuilds the local contract, deploys committed migrations to persistent `dev`, and verifies the exact hosted result
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
5. Confirm whether the Supabase native Git binding can still synchronize
   persistent-Dev Functions; when it can, plan the post-database Edge gate.
6. After merge, `.github/workflows/supabase-dev.yml` performs a blank local
   rebuild, links the configured persistent Dev project, and runs
   `supabase db push --include-all` after the local contract passes.
7. The workflow derives the expected head from the checked-out migration
   directory and waits until a service-only readback reports that exact head;
   it never carries a manually pinned head.
8. The workflow reads `public,api,graphql_public` and
   `public,api,extensions` through the Management API and probes the hosted
   Data API boundary. After `db push`, these checks are read-only.
9. While the native Git binding can still replace Functions, complete the
   post-database Edge Functions gate before declaring persistent Dev healthy.

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
4. If `supabase/config.toml` changed, an operator runs
   `supabase config push --project-ref <production-project-ref> --yes` after the
   migration is present, then verifies the PostgREST configuration through the
   Management API.
5. Operators validate production migration state and application behavior after
   the promote merge.

For a schema-boundary cutover, validation on Preview and persistent `dev` must
cover profile-less core entity access through the hosted default `public`
profile, explicit `public` entity access, explicit `api` RPC access, rejection
of `private`, and absence of the former `public` RPC route. Data API consumers
must select `public` for entity tables and `api` for RPCs rather than relying on
local CLI schema ordering. A short maintenance window may be used for the
production migration, but all consumer changes must already be validated
against persistent `dev` before the `dev -> main` promote.

This repository currently has no checked-in `workflow_dispatch` production
deploy for Supabase. That is intentional: Git `main` is handled by the Supabase
GitHub integration. An operator can still run `supabase link` and
`supabase db push --include-all` locally as an explicit fallback or recovery
path, but that manual action must be recorded in validation or incident notes.

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
10. After merge, validate the persistent remote `dev` database and complete the post-database Edge Functions gate when required.
11. Promote `dev` to `main` when ready to release.
12. Validate that the production Supabase project was migrated automatically by
    the Supabase GitHub integration.

### Persistent `dev` branch deployment

- Pushes to Git `dev` are deployed by `.github/workflows/supabase-dev.yml`.
- The workflow first rebuilds and verifies the complete migration history
  locally. The hosted job depends on that success, links the configured Dev
  project, and runs exactly one `supabase db push --include-all`.
- It then derives the expected head from its checkout and fails unless the
  exact-head readback, Management API readback, and REST profile probes match
  the checked-in contract.
- The workflow owns database migrations only. It must not run `supabase
  functions deploy`, `supabase functions delete`, or `supabase config push`.
- The target state is for the native Git binding not to synchronize persistent-Dev Functions. While it still can, the Edge-owned post-database gate is mandatory after each deployment; do not treat database workflow success alone as a healthy Dev release.

### Production `main` deployment

- Pushes to Git `main` are handled by the production project's Supabase GitHub integration.
- The integration watches repository `tiangong-lca/database-engine` with relative path `supabase`.
- Checked-in pending migrations are applied automatically to the production project when `main` advances.
- Project configuration is not assumed to follow the migration automatically;
  when `supabase/config.toml` changes, push it explicitly to the production
  project and verify the hosted PostgREST settings.
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
