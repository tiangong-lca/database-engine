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
lastReviewedAt: 2026-08-08
lastReviewedCommit: 1d1d153edb92aa01dd5fb7717441b16bedc4a96b
lastReviewedNote: "已为 Issue #422 更新：当 Git dev 原生同步仍然绑定时，增加准确 Edge SHA 的强制恢复与回读 Gate。"
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
- Git `dev` -> 持久化 Supabase `dev` 分支，由 `.github/workflows/supabase-dev.yml` 迁移并验证
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
- 把 `.github/workflows/supabase-dev.yml` 作为持久化 `dev` 的唯一 migration 部署者；它可以执行 `supabase link` 和准确一次 `supabase db push --include-all`，但不得部署/删除 Edge Functions 或推送项目配置。
- 优先在该 workflow 进入 `dev` 前关闭 Git `dev` 的 Supabase 原生部署绑定。
- 如果当前无法解除该绑定，应把原生 integration 视为独立的 Functions 写入者；每次 Git `dev` 数据库部署后都必须完成准确 Edge SHA 的恢复与回读 Gate，而 checked-in 数据库 workflow 仍然只负责数据库。
- 不要为 Git `main` 增加 checked-in 的 GitHub Actions 生产部署流程；生产项目由绑定到本仓的 Supabase GitHub integration 自动迁移。
- 不要先手改远端数据库再回头补 migration。

### Edge Function 所有权验证

- `Supabase Preview` 检查成功只表示原生分支流程已经运行，不能单独证明已部署的 Edge Function 内容发生了变化。
- `tiangong-lca-edge-functions` 仍是 Edge Function 运行时代码的真相源与部署者；本仓不得新增或部署 Edge Function 源码。
- 要判断 database-native 流程是否修改了持久化 Dev Functions，应在流程运行前后，对同一组 Edge 仓所拥有的 function slug 与托管内容哈希进行排序并计算确定性清单摘要，然后比较两次结果。
- 摘要不变表示受管 Function 内容得到保留；如果摘要、受管函数清单、`verify_jwt` 设置或 active 状态发生变化，则视为所有权边界失败并继续调查。
- 内容哈希摘要只用于检测原生流程运行前后的漂移，不是可复现构建标识；即使源码相同，一次干净的重新打包部署也可能产生不同的托管内容哈希。因此恢复证明应依赖准确源码 SHA、完整受管清单、源码路径/状态/鉴权回读以及行为探测，而不是要求重部署后的摘要等于旧摘要。

### Git `dev` 原生同步仍绑定时的强制 Edge 恢复 Gate

在确认原生绑定已经解除且不会同步 Functions 之前，以下流程是当前持久化 Dev 的发布 Gate：

1. 等待 `.github/workflows/supabase-dev.yml` 成功结束。数据库部署尚未达到准确 migration head、托管边界检查尚未通过时，不得提前执行 Edge 恢复。
2. 选择准备部署到持久化 Dev 的准确、已评审 `tiangong-lca-edge-functions` commit，并从该 checkout 使用 Edge 仓正式部署入口：

   ```bash
   npm run deploy:dev -- <完整受管 function slug 清单>
   ```

   必须传入该准确 Edge commit 所拥有的完整 active 清单，只排除 Edge 仓自身明确标记为 retired 或 disabled 的函数。不能只重新部署怀疑被覆盖的函数，因为下一次原生 integration 可能改动另一组函数。
3. 回读托管 Functions；除非每个预期受管 slug 都存在、状态为 `ACTIVE`、使用 Edge 所定义的 `verify_jwt` 设置，并且不存在生产项目或其他外部来源路径残留，否则 Gate 失败。同时确认不属于 Edge 受管清单的远端 legacy functions 没有被修改。
4. 从同一个 Edge checkout 把探测目标显式绑定到持久化 Dev Functions URL 后执行；如果凭据或运行成本要求缩小范围，则使用有记录的代表性子集：

   ```bash
   EDGE_BASE_URL="https://<dev-project-ref>.supabase.co/functions/v1"
   npm run probe:auth -- --base-url "$EDGE_BASE_URL"
   ```

   这样可确保验证的是函数侧鉴权与非法 payload 行为，而不仅是 Management API 元数据，同时避免含义不明确的 `REMOTE_ENDPOINT` 误探测生产环境。
5. 把恢复后的新摘要记录为下一次原生流程漂移检测的 baseline，并在交付 Issue 或 PR 中记录 database merge SHA、数据库 workflow run、准确 Edge SHA、部署清单以及回读/探测结果。

该恢复步骤不会把 Edge Function 所有权转移给 `database-engine`，也不得加入 `.github/workflows/supabase-dev.yml`。只有在原生绑定已经解除，或独立证明其不会写入 Functions 后，才能移除此 Gate；移除时必须在同一个变更中更新本文档。

## 需要维护的文件

- `supabase/config.toml`：共享基线加 `[remotes.dev]`
- `.github/workflows/supabase-dev.yml`：重建本地合同、把已提交 migration 部署到持久化 `dev`，并验证准确的托管结果
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
5. PR 合并前，确认 Git `dev` 是否仍绑定 Supabase 原生部署；如果仍然绑定，安排强制 Edge 恢复 Gate。
6. 合并后，`.github/workflows/supabase-dev.yml` 先完成本地空库重建；本地合同通过后，绑定配置的持久化 Dev 项目并执行 `supabase db push --include-all`。
7. workflow 从当前 checkout 的 migration 目录推导期望 head，再等待 service-only readback 报告该准确 head；workflow 中不手工固定 migration head。
8. workflow 通过 Management API 回读 `public,api,graphql_public` 与
   `public,api,extensions`，并验证托管 Data API 边界；`db push` 后的这些检查均为只读。
9. 如果 Git `dev` 原生绑定仍然存在，则部署准确、已评审 Edge SHA 的完整受管 Function 清单，并在回读 Gate 完成后才宣布持久化 Dev 部署完成。

`--include-all` 表示所有尚未出现在远端 migration history 中的已提交 migration
都可以被应用。受治理的 `main -> dev` 回合并可能带入时间戳早于 `dev` 已记录新
migration 的提交，因此必须使用该参数；已经存在于远端 history 中的 migration
仍会被跳过。

Promote 路径：

1. `dev -> main` promote PR 合并到 Git `main`。
2. 生产项目的 Supabase GitHub integration 会读取 Git `main` 中已提交的 `supabase/` 目录。
3. 尚未应用的已提交 migrations 会自动应用到生产项目。
4. 如果 `supabase/config.toml` 有变化，运维人员应在 migration 已应用后执行
   `supabase config push --project-ref <production-project-ref> --yes`，并通过
   Management API 校验 PostgREST 配置。
5. 运维人员在 promote merge 后验证生产 migration 状态和应用行为。

对于 schema 边界的一次性切换，Preview 与持久化 `dev` 验证必须覆盖：无 profile
时通过托管端默认 `public` 访问核心实体、显式 `public` 实体访问、显式 `api` RPC、
拒绝 `private`，以及旧 `public` RPC 路由不存在。Data API 消费者必须为实体选择
`public`、为 RPC 选择 `api`，不能依赖本地 CLI 的 schema 排序。生产迁移可以使用
短时维护窗口，但所有消费者修改必须先在持久化 `dev` 完成验证，再执行
`dev -> main` promote。

本仓目前没有 checked-in 的 `workflow_dispatch` 生产 Supabase 部署流程。这是有意设计：
Git `main` 由 Supabase GitHub integration 处理。运维人员仍可在本地执行
`supabase link` 和 `supabase db push --include-all`，但这只能作为明确的兜底或恢复路径，
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

- 对 Git `dev` 的 push 由 `.github/workflows/supabase-dev.yml` 部署。
- workflow 先在本地重建并验证完整 migration history；Hosted job 依赖该结果，
  随后绑定配置的 Dev 项目，并准确执行一次 `supabase db push --include-all`。
- 部署后从当前 checkout 推导期望 head，并在 exact-head readback、Management API
  回读或 REST profile 探测不符合合同时失败。
- workflow 只负责数据库 migration，不得执行 `supabase functions deploy`、
  `supabase functions delete` 或 `supabase config push`。
- 优先解除 Git `dev` 的 Supabase 原生部署绑定。如果绑定仍然存在，则必须重新部署准确、已评审 Edge SHA 的完整受管清单并通过上述强制回读 Gate，持久化 Dev 部署才算完成。

### 生产 `main` 部署

- 对 Git `main` 的 push 由生产项目的 Supabase GitHub integration 处理。
- 该 integration 监听 repository `tiangong-lca/database-engine`，relative path 为 `supabase`。
- 当 `main` 前进时，已提交且尚未应用的 migrations 会自动应用到生产项目。
- 不假定项目配置会随 migration 自动同步；`supabase/config.toml` 变化时，必须
  显式推送到生产项目并验证托管 PostgREST 设置。
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
