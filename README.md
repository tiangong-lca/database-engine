# database-engine

`database-engine` is the Supabase database governance repository for the TianGong LCA workspace.

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
8. Promote `dev` to `main` when ready to release.

## Maintenance env files

This repository keeps the operator-side Supabase branch-binding templates:

- `.env.supabase.dev.local.example`
- `.env.supabase.main.local.example`

Copy them to `.env.supabase.dev.local` or `.env.supabase.main.local` for local-only credentials.
Those real `.local` files are ignored by Git because they may contain remote database passwords.
Frontend runtime env files stay in `tiangong-lca-next`.

## Docs

- English: `docs/agents/supabase-branching.md`
- Chinese: `docs/agents/supabase-branching_CN.md`
