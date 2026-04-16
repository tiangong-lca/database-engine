# 远程 Schema 工作区

这个目录用于保存 `dev` 数据库的最新远程 schema 导出，以及基于该导出拆分出来的可读工作区。

## 生成内容

执行 `python scripts/build_schema_workspace.py --environment dev` 时，会生成或刷新以下路径：

- `remote_schema.sql`
- `global/`
- `schemas/`

## 刷新行为

每次刷新 `supabase/workspace` 时，会执行以下操作：

- `remote_schema.sql` 会被最新导出的 dump 覆盖。
- `global/` 会被删除并按最新 dump 重新生成。
- `schemas/` 会被删除并按最新 dump 重新生成。
- `README.md` 会被保留。
- `changes/` 会被保留，适合作为手工修改且需要跨刷新保留的目录。
- 其他根目录文件当前也会保留，但建议只把 `README.md`、文档类文件和 `changes/` 当作稳定的人工维护位置。

## 重要注意事项

- 任何写在 `remote_schema.sql`、`global/` 或 `schemas/` 里的手工修改，下一次刷新时都会丢失。
- 这些路径下如果存在尚未提交到 Git 的改动，在刷新时也可能被覆盖或删除。
- 执行刷新命令前，先检查 `git status`，把需要保留的内容提交或暂存。
- 这个工作区应该被视为远程数据库的生成视图，而不是手工维护 schema 变更的真相源。

## 建议用法

- `remote_schema.sql` 适合查看完整原始导出。
- `global/` 和 `schemas/` 适合按对象结构浏览和审查。
- 需要修改的对象应先复制到 `changes/`，并尽量保持与 `schemas/` 相同的相对目录结构。
- 只有当源文件路径属于当前脚本支持的对象类型时，才应从 `changes/` 或 `supabase/model/schemas/` 生成 migration：
  - `functions/<name>/definition.sql`
  - `views/<name>/definition.sql`
  - `materialized_views/<name>/definition.sql`
  - `tables/<table>/policies/<name>.sql`
  - `tables/<table>/triggers/<name>.sql`
- `table.sql`、索引、sequence、schema 文件以及其他 workspace 文件，目前都不能直接作为 `new_migration.py` 的输入。
- 需要长期保留的说明、备注或操作约定，应写在 `README.md` 或其他根目录文档文件中，不要写进生成目录。

## 刷新命令

```bash
python scripts/build_schema_workspace.py --environment dev
```
