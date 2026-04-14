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

- Git `main` -> production baseline
- Git `dev` -> persistent Supabase `dev` branch
- PR / feature branches -> preview branches created by the Supabase GitHub integration

Rules:

- GitHub default branch remains `main` as a platform exception.
- Daily trunk is Git `dev`.
- Routine feature and fix branches start from `dev` and PR back into `dev`.
- `dev -> main` is the promotion path.
- Do not infer the working trunk from GitHub default-branch UI alone.

## Repository contract

- Keep one shared `supabase/` directory in Git.
- Treat committed files in `supabase/migrations/` as the schema source of truth for production, `dev`, and preview branches.
- Keep branch-specific overrides in `[remotes.<branch>]` inside `supabase/config.toml`.
- Do not create a separate `supabase/` directory per Git branch.
- Keep `.github/workflows/supabase-dev.yml` as the only GitHub Actions flow in this repo that runs `supabase db push` for the persistent Supabase `dev` branch.
- Do not author normal schema changes by editing the remote database first and reconstructing migrations later.

## Files to maintain

- `supabase/config.toml`: shared baseline plus `[remotes.dev]`
- `.github/workflows/supabase-dev.yml`: pushes committed migrations to the persistent Supabase `dev` branch on Git `dev`
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

Repository configuration expected by `.github/workflows/supabase-dev.yml`:

- variable `SUPABASE_DEV_PROJECT_ID`
- secret `SUPABASE_ACCESS_TOKEN`
- secret `SUPABASE_DEV_DB_PASSWORD`

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
10. After merge, validate the persistent remote `dev` branch.
11. Promote `dev` to `main` when ready to release.

### Persistent `dev` branch deployment

- Pushes to Git `dev` trigger `.github/workflows/supabase-dev.yml`.
- That workflow links to the persistent Supabase `dev` branch and runs `supabase db push`.
- Do not add a second automation path that pushes the same target.

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
