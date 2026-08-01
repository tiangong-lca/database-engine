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
lastReviewedAt: 2026-08-01
lastReviewedCommit: b32072d5a38509c4a25d866692958f0ced1303cf
lastReviewedNote: "已针对 Issue #346 复核：持久化 dev 显式串行执行 migration 与白名单 PostgREST 配置，production 仍由 Supabase integration 管理。"
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

- Git `main` -> 生产基线，由 Supabase GitHub integration 自动迁移
- Git `dev` -> 持久化 Supabase `dev` 分支，由 `.github/workflows/supabase-dev.yml` 迁移
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
- 把 `.github/workflows/supabase-dev.yml` 作为本仓唯一会修改持久化 Supabase `dev` 的 GitHub Actions 流程。该流程必须串行部署，先执行 `supabase db push`，再调用仓库自有的 PostgREST 白名单配置 gate。
- 不要为 Git `main` 增加 checked-in 的 GitHub Actions 生产部署流程；生产项目由绑定到本仓的 Supabase GitHub integration 自动迁移。
- 不要先手改远端数据库再回头补 migration。
- Data API schema 必须配置即代码：只暴露 `api`、`public` 和 `graphql_public`；不得把 `private`、`util` 或 `archive` 加入 exposed schemas 或 `extra_search_path`。
- Supabase GitHub 部署 DAG 会先执行 `Configure`，后执行 `Migrate`。因此必须先用现有暴露配置部署并验证新 schema 及其 API 对象，后续再通过单独的 commit/PR 暴露已存在的 schema；不得在同一次部署中首次创建并暴露 schema。
- `scripts/apply_postgrest_config.py` 是持久化 dev 的窄例外：必须精确匹配一个 `[remotes.*].project_id`，只允许 `db_schema`、`db_extra_search_path`、`max_rows`，只 PATCH 漂移字段，并在同一 project ref 上 GET readback。不得读取、修改或记录 `jwt_secret` 及其他服务配置。
- CI 中不得使用无条件 `supabase config push`；它会对齐整个本地/远端配置面，可能覆盖无关的 Auth、Storage 或 Realtime 漂移。

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

当 Git `main` 前进时，该 integration 会自动把已提交的 migration 应用到生产项目。
仓库中没有针对 `main` 的 checked-in GitHub Actions workflow，并不表示生产迁移只能手动执行。

`.github/workflows/supabase-dev.yml` 依赖以下仓库配置：

- variable `SUPABASE_DEV_PROJECT_ID`
- secret `SUPABASE_ACCESS_TOKEN`
- secret `SUPABASE_DEV_DB_PASSWORD`

## PR 到 Supabase migration 路径

已提交的 migration 文件不会因为文件存在就影响远端数据库，只有下面这些部署路径运行后才会生效。

常规 PR 路径：

1. feature 分支包含新的 `supabase/migrations/` 文件。
2. PR 目标分支是 Git `dev`。
3. Supabase GitHub integration 根据已提交的 `supabase/` 目录创建或更新该 PR 的 preview branch。
4. preview branch 只用于 PR 级别验证；它不是持久化 Supabase `dev` 分支。
5. PR 合并后，对 Git `dev` 的 push 会触发 `.github/workflows/supabase-dev.yml`。
6. 该 workflow 会连接 `SUPABASE_DEV_PROJECT_ID` 并执行 `supabase db push --include-all`。
7. 尚未应用的已提交 migrations 随后才会应用到持久化 Supabase `dev` 分支。

`--include-all` 表示所有尚未出现在远端 migration history 中的已提交 migration
都可以被应用。受治理的 `main -> dev` 回合并可能带入时间戳早于 `dev` 已记录新
migration 的提交，因此必须使用该参数；已经存在于远端 history 中的 migration
仍会被跳过。

Promote 路径：

1. `dev -> main` promote PR 合并到 Git `main`。
2. 生产项目的 Supabase GitHub integration 会读取 Git `main` 中已提交的 `supabase/` 目录。
3. 尚未应用的已提交 migrations 会自动应用到生产项目。
4. 运维人员在 promote merge 后验证生产 migration 状态和应用行为。

本仓目前没有 checked-in 的 `workflow_dispatch` 生产 Supabase 部署流程。这是有意设计：
Git `main` 由 Supabase GitHub integration 处理。运维人员仍可在本地执行
`supabase link` 和 `supabase db push`，但这只能作为明确的兜底或恢复路径，
并且必须在验证记录或事故记录中说明。

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

## Hosted Data API 安全运维门

Schema boundary Expand 阶段，hosted PostgREST 必须精确暴露
`api,public,graphql_public`（受审顺序）。保留 `public` 是明确的兼容阶段；`private`、`util`、
`archive` 不得暴露。Issue #339 通过 `scripts/hosted_security_acl.py` 独立执行
Management API readback 和真实 REST negative gate；该脚本保持只读，配置变更仅通过
`scripts/apply_postgrest_config.py` 的 allowlisted diff/readback/rollback gate。不能用本地 config 或单独 SQL
catalog 查询替代 hosted exposure 证据。

需要调用 `private` helper 的 public search wrapper 通过 non-login、non-BYPASSRLS
且继承 authenticated transport prerequisite 的 `api_internal_executor` 执行；browser role 不获得直接 `private`
USAGE/EXECUTE。两个 lifecycle bundle RPC 仍是 authenticated compatibility
contract。Expand 明确记录该边界；consumer-zero 证据成立后，Contract 才移除它们。

Migration 负责关闭 `postgres` owner 的 default privileges。future function 必须
使用 database-wide 的 `ALTER DEFAULT PRIVILEGES ... REVOKE EXECUTE ON FUNCTIONS`，
因为 PostgreSQL 内建的 `PUBLIC EXECUTE` 是 global default，per-schema revoke 无法
从中减权；table/sequence default 仍限定在五个 application schema。catalog/hosted
gate 必须合并计算 built-in、explicit global 与 additive per-schema 三层 effective
defaults，不能把缺少 `pg_default_acl` 显式行当成安全。内部 `supabase_admin` 的
effective residue 继续由 #352 fail-closed 跟踪，直到受支持的 platform-owner 通道
收口并使 `hostedOperatorReady=true`。

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
12. 验证生产 Supabase 项目已经由 Supabase GitHub integration 自动完成迁移。

### 持久化 `dev` 分支部署

- 对 Git `dev` 的 push 会触发 `.github/workflows/supabase-dev.yml`。
- 该 workflow 会连接持久化 Supabase `dev` 分支并执行 `supabase db push --include-all`，从而让受治理的回合并可以应用远端 history 中缺失的全部已提交 migration，包括时间戳更早的条目。
- 不要再增加第二条会对同一目标执行 push 的自动化链路。

### 生产 `main` 部署

- 对 Git `main` 的 push 由生产项目的 Supabase GitHub integration 处理。
- 该 integration 监听 repository `tiangong-lca/database-engine`，relative path 为 `supabase`。
- 当 `main` 前进时，已提交且尚未应用的 migrations 会自动应用到生产项目。
- 不要把缺少 checked-in 的 `main` GitHub Actions workflow 理解为需要手动部署。
- 本地 `supabase db push` 仅作为明确的兜底或恢复路径使用，并需要记录该动作。

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
