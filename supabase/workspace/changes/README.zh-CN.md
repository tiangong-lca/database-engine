# Workspace Changes

这个目录用于放置手工修改的 SQL 文件，并且这些修改在执行 `python scripts/build_schema_workspace.py --environment dev` 后仍会保留。

简要说明：

- 修改前先将文件或目录从 `../schemas/` 复制到 `changes/`。
- 尽量保持与生成目录 `schemas/` 相同的相对路径结构。
- 不要把 `../schemas/` 当作稳定的编辑目录，因为刷新 workspace 时它会被重新生成。
- 通过 `python scripts/new_migration.py --source-path ...` 从 `changes/` 下的文件生成 migration。

更多说明：

- `../README.md`：workspace 结构、刷新行为、覆盖风险
- `../../scripts/README.zh-CN.md`：脚本用法、`--git-changes` 以及 `new_migration.py` 当前限制
