# TIDAS Schema 2026-06 Data Migration Runner

This directory contains the dated data remediation runner used for the TIDAS schema 2026-06 JSON migration.

## Scope

The runner can scan, plan, apply, and validate JSON changes for these dataset tables:

- `flowproperties`
- `unitgroups`
- `contacts`
- `sources`
- `lciamethods`
- `lifecyclemodels`
- `flows`
- `processes`

It handles path-aware `@type` normalization, dataset and reference version normalization, lifecycle reference repair, location-code cleanup, root-key alias normalization, and empty JSON row deletion when explicitly planned.

## Safety Rules

- Run `plan` before `apply`.
- Use `--dry-run` for command validation and preflight counts.
- Write audit output under `_artifacts/`; that directory is ignored by Git.
- Do not commit raw production audit exports or row-level investigation artifacts.
- Use `--suppress-user-triggers` only when the migration is intentionally bypassing user-facing table triggers for controlled operator repair.

## Commands

Plan without writing rows:

```bash
python scripts/data_migrations/tidas_schema_202606/runner.py plan \
  --environment dev \
  --run-id tidas-schema-202606-dev-plan \
  --out _artifacts/tidas-schema-202606/dev-plan.jsonl \
  --dry-run
```

Apply a small batch with row-level audit output:

```bash
python scripts/data_migrations/tidas_schema_202606/runner.py apply \
  --environment dev \
  --run-id tidas-schema-202606-dev-apply \
  --out _artifacts/tidas-schema-202606/dev-apply.jsonl \
  --batch-size 50 \
  --suppress-user-triggers
```

Validate remaining required-rule violations:

```bash
python scripts/data_migrations/tidas_schema_202606/runner.py validate \
  --environment dev \
  --run-id tidas-schema-202606-dev-validate \
  --out _artifacts/tidas-schema-202606/dev-validate.jsonl
```

Use `--database-url` to target an explicit connection string instead of resolving it from the environment profile.

## Local Validation

Run the unit tests after changing the runner:

```bash
python -m unittest tests/data_migrations/test_tidas_schema_202606.py
```

Run a syntax check for the runner and tests:

```bash
python -m py_compile scripts/data_migrations/tidas_schema_202606/runner.py tests/data_migrations/test_tidas_schema_202606.py
```
