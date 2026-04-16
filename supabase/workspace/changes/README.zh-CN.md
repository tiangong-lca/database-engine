# Workspace Changes

这个目录用于放置手工修改的 SQL 文件，并且这些修改在执行 `python scripts/build_schema_workspace.py --environment dev` 后仍会保留。

使用约定：

- 修改前先将文件或目录从 `schemas/` 复制到 `changes/`。
- 尽量保持与生成目录 `schemas/` 相同的相对路径结构。
- 可使用 `python scripts/copy_workspace_file_to_changes.py --source-path ...` 复制文件或目录，并保留相对路径。
- 可使用 `python scripts/copy_workspace_file_to_changes.py --git-changes` 自动复制当前 Git 检测到的 `schemas/` 下所有未提交文件。
- 只有当文件属于当前脚本支持的对象类型时，才可通过 `python scripts/new_migration.py --source-path ...` 从 `changes/` 下生成 migration：
  - `functions/<name>/definition.sql`
  - `views/<name>/definition.sql`
  - `materialized_views/<name>/definition.sql`
  - `tables/<table>/policies/<name>.sql`
  - `tables/<table>/triggers/<name>.sql`
- `table.sql`、索引、sequence、schema 级 SQL 以及其他 workspace 文件，当前都不能直接作为 `new_migration.py` 的输入。
- 不要把 `schemas/` 当作稳定的编辑目录，因为刷新 workspace 时它会被重新生成。
