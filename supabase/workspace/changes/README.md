# Workspace Changes

Use this directory for manual SQL edits that should survive `python scripts/build_schema_workspace.py --environment dev`.

Guidelines:

- Copy files or directories from `schemas/` into `changes/` before editing them.
- Keep the same relative structure as the generated `schemas/` tree when possible.
- Use `python scripts/copy_workspace_file_to_changes.py --source-path ...` to copy a file or directory while preserving its relative path.
- Use `python scripts/copy_workspace_file_to_changes.py --git-changes` to copy every uncommitted file currently detected under `schemas/`.
- Generate migrations from files in `changes/` with `python scripts/new_migration.py --source-path ...` only for currently supported object types:
  - `functions/<name>/definition.sql`
  - `views/<name>/definition.sql`
  - `materialized_views/<name>/definition.sql`
  - `tables/<table>/policies/<name>.sql`
  - `tables/<table>/triggers/<name>.sql`
- Files such as `table.sql`, indexes, sequences, and schema-level SQL can be copied into `changes/`, but they are not valid `new_migration.py` inputs today.
- Do not treat `schemas/` as a stable edit location because it is regenerated on refresh.
