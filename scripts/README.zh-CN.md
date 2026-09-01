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
lastReviewedAt: 2026-09-02
lastReviewedCommit: de28dd30f365cd3f94a4278f982fba29c0e70af9
lastReviewedNote: "为 Issue #582 复核：Supabase workflow 合同现在证明精确的 docs-only Preview 跳过，并对所有 supabase/ 变更保留 fail-closed 托管证据。"
related:
  - ../AGENTS.md
  - ../.docpact/config.yaml
  - ../docs/agents/repo-architecture.md
  - ../docs/agents/repo-validation.md
  - README.md
---

# Scripts

这个目录包含用于远程 schema 导出、workspace 刷新、修改复制、migration 生成和受控数据迁移的命令行脚本。

## 目录结构

长期维护的 helper 入口保留在本目录顶层。
一次性或按日期归档的数据修复 runner 放在：

- `scripts/data_migrations/<topic>_<yyyymm>/`

这类 runner 应在自己的 `README.md` 中保留 dry-run、apply 和 validate 示例。
本地迁移输出和审计 JSONL 文件应写入 `_artifacts/`，该目录已被 Git 忽略。

## 脚本列表

### `check_auth_email_templates.py`

在不连接 Supabase 的情况下校验密码恢复邮件契约。它要求
`supabase/config.toml` 使用准确绑定和非空主题，并要求按钮与可见、可复制的链接都指向
同一个完整 `{{ .ConfirmationURL }}`；手工拼接 token hash 的 magic link 会被拒绝。

```bash
python3 scripts/check_auth_email_templates.py
python3 scripts/test_check_auth_email_templates.py
```

### `test_full_schema_cutover_upgrade.sh`

把本地数据库重建到全量 Schema 切换前的最后一个 migration，写入一条代表性业务数据，
快照关系与函数身份、触发器、RLS 策略、约束及精确行数，复现生产已先执行较晚
reuse-binding hotfix 的 ledger 顺序，运行 fail-closed bridge，再应用切换与契约收口
migration，验证对象和数据完整保留、能力清单、幂等索引及 Data Product 消费者 façade
已安装，并确认 API 函数不再向 PostgreSQL `PUBLIC` 角色开放执行权限。脚本还会应用
Issue #422 运行时 ACL 修复，并在有数据升级后校验 authenticated RLS helper 与 Edge
release façade 的精确授权，最后证明 bridge 在已完成切换的数据库上严格 no-op。

用法：

```bash
scripts/test_full_schema_cutover_upgrade.sh
```

此脚本仅用于本地验证，并会重置本地 Supabase 数据库。

### `test_search_text_array_upgrade.sh`

把本地数据库重建到 `20260810200000`，执行与生产相同的 `search_text`
migration，并证明七张表的 OID 与 heap relfilenode 都不变，同时代表性业务行完整保留。
随后脚本会再次重建迁移前状态，写入一个非空 scalar，证明 migration 会在修改任何列或
丢弃该值之前 fail closed。最后会把本地数据库完整重置到当前 checkout 的 migration head。

```bash
scripts/test_search_text_array_upgrade.sh
```

### `test_portal_projection_upgrade_recovery.sh`

在显式确认且隔离的本地 Supabase 项目中验证 Issue 531/532/539/543/551/563 Portal projection
上线。脚本使用真实并发连接覆盖有效更新、删除、状态失效、主键变更以及仅 embedding
更新竞态；主动制造 card/facet reconcile 锁超时与 cutover guard 失败；证明 facet
expand COMMIT/history 缺口、四分片幂等重试、同名 concurrent index 受控清理、Flow
eligibility guard 回滚、事务性 sitemap expand COMMIT-gap/reset、public cutover 与
forward repair 的回滚/重试、exact-version child 的 PK/FK/index、唯一 same-key upsert
trigger，以及所有旧 winner 对象均被移除。同一 identity 不同 version 的真实
insert/update/delete 必须全部提交，无 writer 侧 retry，并让 child rows 与已提交公开
facet version 集精确相等。脚本还证明已记录迁移重复执行不会重建八个索引，并拒绝
非 canonical 的 cursor numeric scale；SHA-pinned 的准确旧 Preview fixture 还必须
证明 populated 且已记录的 `134101`/`134102` 状态只应用 `134103` 即可收敛。
所需环境变量与恢复边界见
`docs/agents/portal-projection-migration-recovery.md`。正式证据还要求干净 HEAD、
Supabase CLI `2.109.1`，以及完整 299-file migration tree 的逐字相等和 aggregate
SHA-256。

### `test_portal_facet_projection_populated_upgrade.sh`

在同一类显式隔离的 Issue-531/532/539/543/551/563 项目中，对 126,246 条既有 parent card 逐字执行七个
Facet migration。每条 backfill statement 必须在 120 秒门下保留至少 2 倍余量，
每个完整 UUID-quarter 文件必须低于 120 秒；成功 reconcile fence 必须在 5 秒内
完成，并要求 key coverage、确定性抽样 facts 与 DTO 聚合计数精确一致。runner
随后在全部 126,246 行上按 60/15/15 秒证据预算（120/30/30 秒外层超时）计时三条
sitemap migration，并要求 facet/sitemap exact-version rows 双向相等、复合 PK/FK、
history-order index、唯一 same-key trigger、shard capacity 与两个 public RPC 精确
一致。runner 退出时总会把隔离项目重置到完整 HEAD。

### `run_portal_projection_benchmark.sh`

仅在显式确认的 Issue 531/532/539/543/551/563 隔离本地 Supabase 项目中运行代表性 Process/Flow
Search、Hybrid、Facets、写路径、fence、plan 与 ANN recall 基准。runner 会把完整
299-file migration tree 与仓库逐字比较，把结果写入操作员提供的新私有目录，并在运行前后
reset 隔离数据库，避免已回滚的 HNSW 页面持续累积。环境合同与 recovery runner
一致，另要求 `PORTAL_PROJECTION_BENCHMARK_OUTPUT_DIR`。

独立的 catalog-summary cardinality SQL 还会对动态选出的 classification 与 Flow
CAS example 各执行 20 个样本。它在临时 writer clone 上分别记录 combined
eligibility index 与 exact Flow CAS index 的 build time、bytes 和增量四次更新 p95；
该探针不会 drop 或 rebuild 真实 projection/index。`cas-pressure` profile 会让
10,000 个 current Flow card 共用一个 CAS，同时保留一个唯一 CAS，并要求受限选择器
保持 summary p95 <250 ms、已发布 CAS 只返回一条结果。同一代表性 fixture 还会捕获
forced-RLS exact-CAS 自然计划；除非 CAS 等值位于
`portal_catalog_search_flow_cas_v1_idx` 的 `Index Cond` 且没有 CAS JSON filter，否则失败。

完整 candidate benchmark 还会记录 20-sample
`process_single_character` / `flow_single_character` label；它们验证一个未转义
code point 的同步窄 character pre-limit，并要求 Search
p95 <2 秒。所有多 code point 与
已转义 literal 仍保留既有 PGroonga 模板。writer 证据包含唯一 child upsert
Trigger，populated/recovery runner 证明 child/parent parity。

`PORTAL_PROJECTION_BENCHMARK_PROFILE` 用于选择 fail-closed 命名 profile：

- `release` 使用代表性行数/向量数和 21,000 条旧 Flow 版本压力，记录自然
  raw-ANN 分支，直接验收两类完整规模 exact helper，并捕获生产一致的
  5,000-to-200 ANN 阶段；
- `sparse-zero` 使用代表性行数但不写 embedding；
- `sparse-199` 为每类数据只写 199 条 embedding；
- `diagnostic` 允许显式传入较小规模，不能作为发布证据；`auto` 会根据
  精确参数识别命名 profile。

所有命名 gate 都要求干净且精确的 HEAD，覆盖完整公开请求形态，保持
Search/Facets p95 <= 2 秒、Hybrid p95 <= 6 秒、每次 Hybrid < 8 秒。正式
semantic plan 必须含可解析的 shared-buffer 证据、低于 750,000 total / 250,000
read blocks、exact 在 5 秒内完成、formal ANN+exact 合计不超过 6 秒，且没有
temp/disk spill。每次运行必须使用新的 mode-0700 输出目录。正式 lexical plan
使用与 Process/Flow pattern helper 完全一致的 leaf，并保持所有常规 planner
路径开启。代表性 Flow 基数必须自然命中其 PGroonga scan node；Process 基数较小，
因此记录自然成本计划而不强制某个索引，迁移期 catalog guard 负责证明其 PGroonga
索引，命名 timing 独立覆盖 Process 性能、排序和 cursor。两类 lexical probe 都
必须满足精确 needle fixture identity 且无 spill。
当 Process 行数不少于 10,000 时，每个 profile 还要求 exact-name/classification
探针自然命中 `portal_catalog_search_process_exact_rank_v1_gin`，记录索引字节，并把
新增表达式索引纳入既有 Process writer delta/ratio 门。
基准还会捕获匿名 Process/Flow 空 Facets 与过滤 Facets 计划，要求独立空路径在固定
32-MB 工作区内零 temp/disk spill，测量 parent-first facet reconcile fence，并把
facet child upsert 纳入现有
writer delta/ratio 门。
每个 profile 还会记录精确的 Flow embedding universe probe。sparse profile 必须
自然命中窄 partial eligibility B-tree，且不得扫描宽 Flow heap；release profile
记录全量 vector 的自然计划，不强制使用该索引。只有 release 必须命名两个 source
HNSW index；sparse source probe 可以选择 eligibility/empty-set plan，但仍必须提供
buffers、execution time 且没有 temp/disk spill。

所有命名 profile 都会构造 evidence-complete card context：每个 Process 都解析准确
公开 reference Flow 与真实 FlowProperty/UnitGroup functional-unit 支撑链，Process/Flow
还包含公开 source/database，Process 另包含 review/technology 字段。四个专用
Search-50/Hybrid-20 label 必须分别返回准确 50/20 个完整 item、20 个 timing sample，
并输出完整 wrapper `EXPLAIN (ANALYZE, BUFFERS)`。runner 会记录 temp-buffer 使用，
拒绝字段缺失、超过 750,000 total / 250,000 read shared blocks，以及超过既有 Search 2 秒 / Hybrid
6 秒门槛的结果。
证据文件还会单独保存空 query、`geography=cn` Flow Search-50 的完整计划，
因为它是代表性的过滤最坏路径；对应 timing label 也必须准确返回 50 个完整 item，
避免空结果或过窄结果让性能门假绿。

sitemap profile 的 126,246-row 单版本 fixture 保持最大 shard 为 2,066 个 identity，
记录的 shard-read p95 约为 11 ms。独立 history-density probe 将 2,048 个 identity
各扩展为 64 个 version，共 131,072 行；其自然 `DISTINCT ON` 计划必须通过精确
history-order index 走 index-only path，不得出现 `Sort` / `Incremental Sort`，不得
产生 temp spill，并须在 4 秒内完成。响应数量、字节与 timeout 仍有界，但扫描行数
会随保留 version 历史增长。

### `check_portal_projection_manifest.py`

同时验证已提交的 Portal digest：十一函数 stored-card 闭包、两函数窄 Facet
闭包，以及独立的 limit 后 context/decorator 闭包。它禁止后续 mutation 任一
闭包/控制函数，校验四个 Facet 分片、reconcile/cutover，要求 context migration
不新增 table/index/trigger，并保留 Flow eligibility index 的精确 catalog guard，
同时不改变两个 #531 digest。它还要求 Flow geography Search follow-up 只能是
单一 query-only kernel replacement，不得新增 table/index/trigger 或改写 writer；
runtime 在读取该 child projection 前还必须独立验证 Facet manifest。

检查器还冻结 Issue #563 的三步序列：dormant immutable rank helper、唯一 standalone
concurrent partial expression GIN，以及仅覆盖 multi-code-point、non-UUID、无过滤 Process
relevance 的原子 coordinator cutover。内部 writer 与 `postgres` 维护 ACL 必须准确，
不得改动公开 wrapper/Trigger/table，后续 migration 也不得静默修改该 helper closure。

它还会冻结 Issue #539 的 64-bucket exact-version child：table/PK、带
`ON UPDATE RESTRICT` / `ON DELETE CASCADE` 的 facet exact FK、history-order index，
以及唯一的 `AFTER INSERT OR UPDATE` same-key upsert trigger。公开 shard reader 必须
保留 index-ordered `DISTINCT ON (dataset_kind,id)` 选择、4,096-item / 2-MiB / 4 秒
边界和显式 history-density plan gate。`134103` forward repair 必须在一个事务内锁定
facet writer、建立并完整 backfill shadow child、替换 assertion/reader，并移除旧
winner table/helpers。shard cursor 字节必须等于精确期望对象的新编码，因此 JSONB
等价的 `1.0` / `64.0` numeric scale 也会被拒绝；旧 sitemap façade 仍逐字不变。

```bash
python3 scripts/check_portal_projection_manifest.py
```

### `check_portal_card_schema_parity.py`

除非 lexical Search 与 Hybrid candidate item 在各自版本化 `match` 之外具有
逐字一致且 exhaustive 的 card properties，并共同引用精确五字段
`PublicCardContext` 定义，否则立即失败。

```bash
python3 scripts/check_portal_card_schema_parity.py
```

### `resolve_migration_head.py`

从当前 checkout 的 `supabase/migrations` 目录输出最新的有效 migration 版本。
如果目录为空、SQL migration 文件名不合法，或存在重复的 14 位版本号，脚本会
fail closed，避免托管验证静默选择含糊的 head。

```bash
python scripts/resolve_migration_head.py
```

运行对应回归测试：

```bash
python scripts/test_resolve_migration_head.py
```

### `test_supabase_dev_workflow_contract.py`

除非 `.github/workflows/supabase-dev.yml` 保持两条相互隔离的托管路径，否则立即
失败。push-only 持久化 Dev job 必须绑定配置的 Dev 项目，准确执行一次
`supabase db push --include-all`，从 checkout 推导 migration head，并执行准确一次
三字段 PostgREST PATCH。pull-request-only Preview job 对 fork 跳过，并先验证事件
base/head commit 和唯一准确的 `supabase/` diff。零 diff PR 输出 `required=false`，不执行
任何 hosted Preview 动作。只要 Supabase 有变更，缺少 access token、main-parent ref 或
persistent-Dev ref 任一项仍 fail closed；随后必须把准确 head 上来自官方 Supabase App
的唯一成功 check，与 `branches list` 中按 Git branch、PR number、parent 匹配的唯一
non-default/non-persistent BranchResponse 绑定。两个 ref 必须相等且都不同于 main/Dev，
才能执行 Preview 的一次相同 PATCH 与回读。

合同还要求通过独立且不带 `reveal` 的 Management API key 读取，使用原始
`disabled` 状态与准确公共 key 形态筛选。只有已 mask 的启用 publishable 或 legacy
anon key 能进入下一 step；此前必须清除原始 JSON 与 PAT。匿名 Hybrid step 本身不得
包含 PAT、`Authorization`、`Cookie` 或 service credential。整个 workflow 唯一允许的
Management API 修改，是持久化 Dev 与 Preview 各一次 PostgREST PATCH；Functions
命令、广义 `config push`、手工固定 migration head 及其他修改仍全部拒绝。

合同还要求失败诊断只能输出 HTTP status 与通过响应形态校验的
PostgREST/SQLSTATE code；禁止打印原始 response body，形态异常或包含额外字段的
错误 envelope 必须统一记为 `unclassified`。

同一匿名凭据边界还会在最多 300 秒内轮询 sitemap manifest，验证准确 64 个不透明
descriptor，把其中一个 cursor 逐字传给 shard RPC，严格校验两个 exhaustive JSON
Schema，并要求伪造 cursor 只返回受限的 `22023` envelope。

```bash
python scripts/test_supabase_dev_workflow_contract.py
```

### `data_migrations/tidas_schema_202606/runner.py`

用于对远程数据库中的 TIDAS schema 2026-06 JSON 数据修复进行 plan、apply 和 validate。

用法：

```bash
python scripts/data_migrations/tidas_schema_202606/runner.py plan --environment dev --run-id tidas-schema-202606-dev --out _artifacts/tidas-schema-202606/dev-plan.jsonl --dry-run
```

完整命令面和安全说明见 `scripts/data_migrations/tidas_schema_202606/README.md`。

### `data_migrations/root_reference_review_v2_local.sh`

用于在 Root/Reference Review v2 切换前创建本地加密备份，并由操作员逐条迁移存量审核。

备份内容包括完整 custom-format dump、审核相关表 dump，以及七类业务表中受影响的数据行。备份目录必须位于 Git 工作树之外。只有操作员已独立验证恢复能力、确认第二份本地副本并生成 dry-run 清单后，`apply` 才会解锁。

用法：

```bash
DATABASE_URL='postgresql://...' \
REVIEW_BACKUP_PASSWORD_FILE='<本地密码文件的绝对路径>' \
scripts/data_migrations/root_reference_review_v2_local.sh backup \
  '/absolute/path/review-v2-backup'

DATABASE_URL='postgresql://...' \
scripts/data_migrations/root_reference_review_v2_local.sh dry-run \
  '/absolute/path/review-v2-backup'

DATABASE_URL='postgresql://...' \
scripts/data_migrations/root_reference_review_v2_local.sh verify \
  '/absolute/path/review-v2-backup'

DATABASE_URL='postgresql://...' \
scripts/data_migrations/root_reference_review_v2_local.sh apply \
  '/absolute/path/review-v2-backup'
```

操作员只能在真实完成恢复验证和第二份本地副本验证后，创建
`RESTORE_VERIFIED` 与 `SECOND_LOCAL_COPY_VERIFIED` 标记。不得把密码文件、
加密备份、标记文件或迁移清单提交到 Git。

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
- 未传 `--db-url` 时，`--environment local` 使用 Supabase CLI 的 `db dump --local`；显式 `--db-url` 仍使用该 URL

### `build_schema_workspace.py`

用于刷新可读的 schema workspace，涉及：

- `supabase/workspace/remote_schema.sql`
- `supabase/workspace/global/`
- `supabase/workspace/schemas/`

用法：

```bash
python scripts/build_schema_workspace.py --environment dev
```

如需按本地已应用 migration 精确重建：

```bash
python scripts/build_schema_workspace.py --environment local
```

行为：

- 先导出最新远程 schema
- 规范化生成 SQL 视图的行尾空白，保证审查 diff 可重复
- 重建 `global/` 和 `schemas/`
- 保留 `supabase/workspace/README.md`
- 保留 `supabase/workspace/README.zh-CN.md`
- 保留 `supabase/workspace/changes/`

注意：

- `remote_schema.sql`、`global/` 和 `schemas/` 中的手工修改都不稳定
- 刷新时可能覆盖这些生成文件里尚未提交到 Git 的改动
- 如果你希望 `--git-changes` 只反映后续手工修改，应在同步远程数据库并刷新 workspace 之后，先把新的 `supabase/workspace/schemas` 提交到 Git，再开始编辑
- 远程 `dev` 仍是生成 schema 的权威目标。Schema 变更 PR 可以提交 exact-local 审查快照，但必须先通过空库 migration 重建、定向合同测试，并再次生成证明无漂移。合并后，数据库专用 Dev 部署必须到达准确 head、托管 catalog 检查必须通过，还要将 remote-Dev 刷新结果与审查快照比较；若有漂移，以后续提交收口。

### `build_database_types.py`

生成纳入版本控制的 TypeScript 数据合同，仅覆盖 Data API 暴露的 `public` 与 `api` 两个 schema。

```bash
python scripts/build_database_types.py --environment local
```

只有在刻意以已链接的 Supabase 项目为来源时才使用 `--environment linked`。CI 会从本地完整 migration 状态重新生成，并在 `supabase/workspace/database.types.ts` 漂移时失败。

### `build_portal_contract_types.py`

为每个 exhaustive Portal JSON Schema 生成一个纳入版本控制的 TypeScript module。
本仓没有 Node package manifest，因此脚本遵循既有 CI 依赖政策，通过 `npx`
调用精确固定的 `json-schema-to-typescript@15.0.4`。

```bash
python3 scripts/build_portal_contract_types.py
python3 scripts/build_portal_contract_types.py --check
```

普通生成会让 `contracts/portal/generated/*.d.ts` 与按名称排序的
`contracts/portal/*.schema.json` source set 同步。`--check` 只在临时目录渲染，
若 module 缺失、变化或多余则失败，且不修改 checkout；CI 使用该只读检查。

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

### `test_scope_closure_staged_write_set_v2_fixture.sh`

用于校验 Worker/数据库逐字共享的 staged write-set v2 fixture，包括
canonical JSON、descriptor-set SHA-256、bounded batch 上限、status 字段
不透明性、状态转换以及保留 one-shot 兼容窗口。

用法：

```bash
scripts/test_scope_closure_staged_write_set_v2_fixture.sh
```

这是只读的本地合同检查；不会刷新生成的 schema workspace，也不会连接远程数据库。

### Scope-closure provider 资格验证适配器

`run_scope_closure_database_qualification.sh` 和
`run_scope_closure_storage_qualification.sh` 生成 Worker provider aggregator
直接消费的 `lcia.scope-closure-provider-owned-result.v1` 记录。两个适配器都要求
Worker 提供 `--run-id`，把 `componentSha` 绑定到当前 database-engine commit，
只接受 loopback 或已明确加入许可清单的非生产目标 fingerprint，拒绝生产或不明确
目标，并固定输出 `productionMutation=false`。

数据库适配器对显式 `QUALIFICATION_DATABASE_URL` 执行 #308/#316
pgTAP 合同。存储适配器使用显式 S3-compatible endpoint，并验证 bounded 生成
文件、有效及过期签名 HEAD/range 请求、multipart 边界、重试和精确 prefix GC。
结果中不会写入凭据、object locator、signed URL 或 payload 内容。

在精确 Worker commit `e5a7f769` 下，loopback 执行只构成协议、故障注入和
adapter 证据，不是最终 provider-specific 非生产资格验证。Worker #188 提供
已验证的非生产目标分类后，再使用同一组 owner adapter 运行最终验证；歧义目标
或生产目标仍必须 fail closed。

用法：

```bash
scripts/run_scope_closure_database_qualification.sh \
  --output <new-result-path> \
  --run-id <worker-supplied-uuid>

scripts/run_scope_closure_storage_qualification.sh \
  --output <new-result-path> \
  --run-id <same-worker-supplied-uuid>
```

必需环境变量：

- 两个适配器：`QUALIFICATION_NON_PRODUCTION_CONFIRMATION`
- 数据库：`QUALIFICATION_DATABASE_URL`、`QUALIFICATION_SUPABASE_URL`、
  `QUALIFICATION_SUPABASE_SERVICE_ROLE_KEY`
- 存储：`QUALIFICATION_DATABASE_URL`、`QUALIFICATION_S3_ENDPOINT`、
  `QUALIFICATION_S3_ACCESS_KEY_ID`、
  `QUALIFICATION_S3_SECRET_ACCESS_KEY`、`QUALIFICATION_S3_BUCKET`，以及可选的
  `QUALIFICATION_S3_REGION`
- 非 loopback 目标：`QUALIFICATION_VERIFIED_NON_PRODUCTION_FINGERPRINTS`，值为
  qualification coordinator 批准的、以逗号分隔的精确 SHA-256 目标身份；远程
  数据库和 provider endpoint 必须使用 TLS

不得改变记录 schema；用精确 Worker compatibility verifier 接收并合并两条记录：

```bash
scripts/verify_scope_closure_worker_aggregator.py \
  --worker-repo <worker-checkout-containing-e5a7f769> \
  <database-result> <storage-result>
```

离线控制流和安全回归命令：

```bash
python3 -m unittest scripts/test_scope_closure_provider_qualification.py
```

## Local Docpact Push Gate

The repository now includes a local pre-push docpact gate in `scripts/docpact-gate.sh`. The gate resolves the CLI through `scripts/docpact`. It is documentation-governance tooling and does not change database schema workspace behavior.
