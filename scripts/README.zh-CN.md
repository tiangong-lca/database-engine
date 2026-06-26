---
title: Scripts
docType: guide
scope: repo
status: active
authoritative: false
owner: database-engine
language: zh-CN
whenToUse:
  - 当任务涉及 schema-workspace 辅助脚本时
  - 当你需要确认 workspace 刷新或 migration 生成的支持命令面时
whenToUpdate:
  - 当脚本入口、支持的 source path 形态或 workspace 刷新行为变化时
checkPaths:
  - scripts/README.zh-CN.md
  - scripts/**
  - supabase/workspace/**
  - docs/agents/repo-architecture.md
  - .githooks/pre-push
  - scripts/docpact
  - scripts/docpact-gate.sh
  - scripts/install-git-hooks.sh
lastReviewedAt: 2026-06-03
lastReviewedCommit: b6c351231116fd0890d2e2d8186f6ec2e455fea0
related:
  - ../AGENTS.md
  - ../.docpact/config.yaml
  - ../docs/agents/repo-architecture.md
  - ../docs/agents/repo-validation.md
  - README.md
---

# Scripts

这个目录包含用于远程 schema 导出、workspace 刷新、修改复制和 migration 生成的命令行脚本。

## 目录结构

长期维护的 helper 入口保留在本目录顶层。
一次性或按日期归档的数据修复 runner 放在：

- `scripts/data_migrations/<topic>_<yyyymm>/`

这类 runner 应在自己的 `README.md` 中保留 dry-run、apply 和 validate 示例。
本地迁移输出和审计 JSONL 文件应写入 `_artifacts/`，该目录已被 Git 忽略。

## 脚本列表

### `data_migrations/tidas_schema_202606/runner.py`

用于对远程数据库中的 TIDAS schema 2026-06 JSON 数据修复进行 plan、apply 和 validate。

用法：

```bash
python scripts/data_migrations/tidas_schema_202606/runner.py plan --environment dev --run-id tidas-schema-202606-dev --out _artifacts/tidas-schema-202606/dev-plan.jsonl --dry-run
```

完整命令面和安全说明见 `scripts/data_migrations/tidas_schema_202606/README.md`。

### `export_remote_schema.py`

用于把目标远程数据库的 schema 导出到：

- `supabase/workspace/remote_schema.sql`

用法：

```bash
python scripts/export_remote_schema.py --environment dev
```

说明：

- 默认环境是 `dev`
- 默认 schema 列表是 `public`
- 可通过 `--schema-file` 覆盖输出路径

### `build_schema_workspace.py`

用于刷新可读的 schema workspace，涉及：

- `supabase/workspace/remote_schema.sql`
- `supabase/workspace/global/`
- `supabase/workspace/schemas/`

用法：

```bash
python scripts/build_schema_workspace.py --environment dev
```

行为：

- 先导出最新远程 schema
- 重建 `global/` 和 `schemas/`
- 保留 `supabase/workspace/README.md`
- 保留 `supabase/workspace/README.zh-CN.md`
- 保留 `supabase/workspace/changes/`

注意：

- `remote_schema.sql`、`global/` 和 `schemas/` 中的手工修改都不稳定
- 刷新时可能覆盖这些生成文件里尚未提交到 Git 的改动
- 如果你希望 `--git-changes` 只反映后续手工修改，应在同步远程数据库并刷新 workspace 之后，先把新的 `supabase/workspace/schemas` 提交到 Git，再开始编辑

### `check_generated_workspace_legacy_tables.py`

检查生成的 schema workspace 是否仍在展示已退休的 public legacy job 表：

- `public.lca_jobs`
- `public.lca_package_jobs`
- `public.dataset_review_submit_jobs`

用法：

```bash
python scripts/check_generated_workspace_legacy_tables.py
```

当目标远程分支已经应用 `worker_jobs` cutover 和旧 job 表退休 migration 后，刷新 `supabase/workspace/**`，再运行这个检查。

### `copy_workspace_file_to_changes.py`

用于把生成目录中的文件复制到稳定的手工修改区：

- 从 `supabase/workspace/schemas/...`
- 到 `supabase/workspace/changes/...`

用法：

```bash
python scripts/copy_workspace_file_to_changes.py --source-path "supabase/workspace/schemas/public/tables/comments/table.sql"
```

```bash
python scripts/copy_workspace_file_to_changes.py --source-path "supabase/workspace/schemas/public/tables/comments"
```

```bash
python scripts/copy_workspace_file_to_changes.py --git-changes
```

行为：

- 保留相对路径
- 支持单个文件或目录
- `--git-changes` 会复制当前 Git 检测到的 `supabase/workspace/schemas` 下所有未提交文件
- 建议流程：先刷新 workspace，再将生成的 `supabase/workspace/schemas` 提交到 Git，然后再进行手工修改并使用 `--git-changes`

### `new_migration.py`

用于从受支持的 schema 对象文件生成 migration SQL，来源路径可以是：

- `supabase/model/schemas/...`
- `supabase/workspace/changes/...`

用法：

```bash
python scripts/new_migration.py --name "update policy roles update" --source-path "supabase/workspace/changes/public/functions/policy_roles_update/definition.sql"
```

输出：

- `supabase/migrations/<timestamp>_<slug>.sql`

当前支持的源文件路径形态：

- `functions/<name>/definition.sql`
- `views/<name>/definition.sql`
- `materialized_views/<name>/definition.sql`
- `tables/<table>/policies/<name>.sql`
- `tables/<table>/triggers/<name>.sql`

当前不支持：

- `table.sql`
- indexes
- sequences
- schema 级 SQL
- 其他生成出来的 workspace 文件

### `_db_workflow.py`

这是上面几个脚本共用的内部模块。

通常不作为日常命令行入口直接使用。

## Local Docpact Push Gate

The repository now includes a local pre-push docpact gate in `scripts/docpact-gate.sh`. The gate resolves the CLI through `scripts/docpact`. It is documentation-governance tooling and does not change database schema workspace behavior.
