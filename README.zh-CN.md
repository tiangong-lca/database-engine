# database-engine

`database-engine` 是 TianGong LCA workspace 中用于管理 Supabase 数据库治理的仓库。

它负责维护已经提交到仓库中的数据库真相源：

- `supabase/config.toml`
- `supabase/migrations/*.sql`
- `supabase/seed.sql`
- `supabase/seeds/*`
- `supabase/tests/*.sql`
- `.env.supabase.dev.local.example`
- `.env.supabase.main.local.example`
- Supabase 分支与运维文档
- 将已提交 migration 推送到持久化 Supabase `dev` 分支的 GitHub Actions 流程

它**不**负责以下内容：

- 前端运行时环境文件，例如 `.env` 或 `.env.development`
- 应用侧 Supabase client
- Edge Function 运行时代码
- 把在 Dashboard 上临时手工修改作为长期工作流

这些内容应保留在消费方仓库中，例如：

- `tiangong-lca-next`：前端环境文件与应用集成
- `tiangong-lca-edge-functions`：Edge Function 运行时代码

## 分支模型

- GitHub 默认分支：`main`
- 日常主干分支：`dev`
- 常规 PR 目标分支：`dev`
- 提升路径：`dev -> main`

## 快速开始

1. 从最新的 `dev` 开始。
2. 基于 `dev` 创建功能分支。
3. 使用本地 Supabase 进行 schema 编写和验证。
4. 将 migrations、seeds、tests 和 config 变更一起提交。
5. 向 `dev` 发起 PR。
6. 验证 PR 对应创建的 Supabase 预览分支。
7. 合并后，验证持久化的 `dev` 分支。
8. 准备发布时，将 `dev` 提升到 `main`。

## 运维环境文件

本仓库维护运维侧 Supabase 分支绑定模板：

- `.env.supabase.dev.local.example`
- `.env.supabase.main.local.example`

可将它们复制为 `.env.supabase.dev.local` 或 `.env.supabase.main.local`，用于本地私有凭据配置。
这些真实的 `.local` 文件会被 Git 忽略，因为其中可能包含远程数据库密码。
前端运行时环境文件应保留在 `tiangong-lca-next` 中。

## 文档

- 英文：`docs/agents/supabase-branching.md`
- 中文：`docs/agents/supabase-branching_CN.md`
