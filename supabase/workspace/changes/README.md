# Workspace Changes

Use this directory for manual SQL edits that should survive `python scripts/build_schema_workspace.py --environment dev`.

Quick guidance:

- Copy files or directories from `../schemas/` into `changes/` before editing them.
- Keep the same relative structure as the generated `schemas/` tree when possible.
- Do not treat `../schemas/` as a stable edit location because it is regenerated on refresh.
- Generate migrations from files in `changes/` with `python scripts/new_migration.py --source-path ...`.

See also:

- `../README.md` for workspace structure, refresh behavior, and overwrite warnings
- `../../scripts/README.md` for script usage, `--git-changes`, and current `new_migration.py` limitations
