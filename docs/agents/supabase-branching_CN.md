---
title: Supabase Branching
docType: guide
scope: repo
status: active
authoritative: false
owner: database-engine
language: zh-CN
whenToUse:
  - 当你需要确认 Supabase 分支绑定、preview 行为或持久化 dev 自动化时
  - 当你需要用中文查看本仓的 branch-specific 数据库工作流时
whenToUpdate:
  - 当分支绑定、Vault secret 规则或持久化 dev 部署路径变化时
  - 当这里的分支运维说明与 repo contract 或验证指南不一致时
checkPaths:
  - docs/agents/supabase-branching_CN.md
  - AGENTS.md
  - .docpact/config.yaml
  - supabase/config.toml
  - .github/workflows/supabase-dev.yml
  - .env.supabase.dev.local.example
  - .env.supabase.main.local.example
lastReviewedAt: 2026-04-23
lastReviewedCommit: 4495c2c5771c03789c0ec26de5852f6a33001fec
related:
  - ../../AGENTS.md
  - ../../.docpact/config.yaml
  - ./repo-validation.md
  - ./repo-architecture.md
  - ./supabase-branching.md
---

# Supabase Branching

`database-engine` 是 TianGong LCA workspace 中唯一的 Supabase 真相源仓库。

本仓库负责：

- `supabase/config.toml`
- `supabase/migrations/*.sql`
- `supabase/seed.sql`
- `supabase/seeds/*`
- `supabase/tests/*.sql`
- `.env.supabase.dev.local.example`
- `.env.supabase.main.local.example`
- `.github/workflows/supabase-dev.yml`
- 数据库交付相关的 branching 与运维文档

本仓库**不**负责：

- 前端运行时 `.env` / `.env.development` 之类的环境文件
- 应用侧 Supabase client 代码
- Edge Function 运行时代码

这些职责保留在 `tiangong-lca-next`、`tiangong-lca-edge-functions` 等消费者仓库中。

## 分支契约

- Git `main` -> 生产基线
- Git `dev` -> 持久化 Supabase `dev` 分支
- PR / feature 分支 -> 由 Supabase GitHub integration 自动创建的 preview branch

规则：

- GitHub default branch 继续保持 `main`，这是平台层例外。
- 日常 trunk 是 Git `dev`。
- routine feature / fix 分支从 `dev` 拉出，并向 `dev` 发起 PR。
- `dev -> main` 是正式晋升路径。
- 不要只根据 GitHub default-branch UI 推断实际工作 trunk。

## 仓库契约

- 在 Git 中只维护一套共享的 `supabase/` 目录。
- 把 `supabase/migrations/` 中已提交的文件视为 production、`dev` 和 preview 分支共同遵循的 schema 真相源。
- 分支差异放在 `supabase/config.toml` 的 `[remotes.<branch>]` 中。
- 不要为不同 Git 分支复制多套 `supabase/` 目录。
- 把 `.github/workflows/supabase-dev.yml` 作为本仓唯一会对持久化 Supabase `dev` 分支执行 `supabase db push` 的 GitHub Actions 流程。
- 不要先手改远端数据库再回头补 migration。

## 需要维护的文件

- `supabase/config.toml`：共享基线加 `[remotes.dev]`
- `.github/workflows/supabase-dev.yml`：在 Git `dev` 更新时，把已提交 migration 推送到持久化 Supabase `dev` 分支
- `supabase/migrations/*.sql`：已提交的 migration 历史
- `supabase/seed.sql`：共享 seed 数据
- `supabase/seeds/dev.sql`：可选的持久化 dev 专属 seed 数据
- `supabase/tests/*.sql`：数据库断言与安全检查
- `.env.supabase.dev.local.example`：持久化 `dev` 分支绑定模板
- `.env.supabase.main.local.example`：`main` 分支绑定模板
- `docs/agents/supabase-branching.md`：英文 branching 文档
- `docs/agents/supabase-branching_CN.md`：中文 branching 文档

消费者仓的前端 env 文件不会放在这里维护。

## 运维环境文件

仓库根目录需要维护以下分支绑定模板：

- `.env.supabase.dev.local.example`
- `.env.supabase.main.local.example`

使用规则：

- 复制为 `.env.supabase.dev.local` 或 `.env.supabase.main.local` 后再填写本地真实密钥。
- 真实 `.local` 文件禁止提交。
- 这组文件用于需要 `SUPABASE_PROJECT_REF` 或 `SUPABASE_DB_URL` 的运维动作，对应持久化 `dev` 与 `main` 分支。
- 前端 `.env` / `.env.development` 仍然归 `tiangong-lca-next` 等消费者仓维护。

## GitHub integration 与密钥

生产项目的 Supabase GitHub integration 应绑定到：

- repository: `tiangong-lca/database-engine`
- relative path: `supabase`

`.github/workflows/supabase-dev.yml` 依赖以下仓库配置：

- variable `SUPABASE_DEV_PROJECT_ID`
- secret `SUPABASE_ACCESS_TOKEN`
- secret `SUPABASE_DEV_DB_PASSWORD`

## Vault secret 契约

数据库侧函数或 trigger 调用 Edge Function 时，必须读取 branch-specific Vault secret。

当前标准名称：

- `project_url`
- `project_secret_key`
- `project_x_key` 仅用于兼容旧的 `generate_flow_embedding()` 路径

规则：

- 不要把 branch URL 或 service key 硬编码进 SQL、migration 或导出的 baseline 文件。
- 这些值是 branch-specific 的。`main`、持久化 `dev`，以及任何需要执行 webhook 的 preview branch 都要各自提供所需 secret。
- 如果 branch 被重建或重新关联，测试 webhook 之前要重新核对 Vault entries。

## 默认工作流

### 常规 schema 变更

1. 同步本地 Git `dev`。
2. 从 `dev` 创建 feature 分支。
3. 启动本地 Supabase。
4. 在本地完成 schema 变更。
5. 用 `supabase migration new <name>` 或 `supabase db diff -f <name>` 生成 migration。
6. 用 `supabase db reset` 和相关 SQL 测试完成验证。
7. 把 migration、seed、测试和 config 一起提交。
8. 向 Git `dev` 发起 PR。
9. 让 Supabase 为该 PR 自动创建或更新 preview branch。
10. 合并后，在持久化远端 `dev` 分支验证结果。
11. 准备发布时，再把 `dev` 晋升到 `main`。

### 持久化 `dev` 分支部署

- 对 Git `dev` 的 push 会触发 `.github/workflows/supabase-dev.yml`。
- 该 workflow 会连接持久化 Supabase `dev` 分支并执行 `supabase db push`。
- 不要再增加第二条会对同一目标执行 push 的自动化链路。

### Hotfix 流程

1. 从 Git `main` 拉分支。
2. 修复问题。
3. 合并回 `main`。
4. 再把 `main` 回合并到 `dev`。
5. 保持两条长期分支上的 migration 历史一致。

## 消费者仓边界

以下变更应在 `database-engine` 完成：

- schema、policy、SQL function、trigger、seed、config
- preview / persistent branch 行为
- 数据库侧测试与 migration 恢复

以下内容保留在消费者仓完成：

- 前端 env 选择与应用侧 Supabase client
- Edge Function 运行时代码
- 应用对 `dev`、preview、`main` 的联调验证

如果一个需求同时改数据库和应用行为，数据库部分仍然从这里开始。

## 恢复规则

- 如果本地和远端 migration history 不一致，先用 `supabase migration list` 查清楚再继续。
- `supabase db pull` 只用于为既有远端 schema 建 baseline，或把远端独有的 drift 回收到 Git。
- 如果某个 branch 进入 `MIGRATIONS_FAILED`，优先在 Git 中修 migration 并重建失败分支，而不是手工硬改远端状态。
- 如果远端 history 元数据本身错了，再有意识地执行 `supabase migration repair`，然后重新核对结果。

## 本地命令

在本仓内统一使用 Supabase CLI。

- `supabase start`
- `supabase db diff -f <name>`
- `supabase migration new <name>`
- `supabase db reset`
- `supabase migration list`
- `supabase link --project-ref <ref>`
- `supabase db push`
