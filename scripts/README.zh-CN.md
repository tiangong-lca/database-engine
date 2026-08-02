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
lastReviewedAt: 2026-08-03
lastReviewedCommit: c5356d2b0d340f9c5c31a645479be5f3d19a52db
lastReviewedNote: "已为 Issue #405 复核：exact-head inventory 刷新与对比只接受显式 loopback URL，不生成 DDL，也不授权 Contract。"
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

### `test_issue_398_result_gc_runtime.py`

该脚本只对显式 loopback 数据库 URL 运行破坏性的 result-GC 真实登录角色与多会话验证。
它强制要求 `--confirm-isolated-destructive-test`，创建唯一临时登录角色，覆盖
renew/fail/takeover/finalize 与并发竞态，并精确清理、回读数据库和角色零残留。
必须使用 `docs/agents/lca-result-gc-contract.md` 中固定的唯一 project ID 与端口，
不得指向 linked 或持久化数据库。

## 脚本列表

### `apply_postgrest_config.py`

该脚本只对一个精确的持久化 Supabase 分支同步已审核的 PostgREST 字段。`--check` 只读；`--apply` 只 PATCH 发生漂移的 `db_schema`、`db_extra_search_path`、`max_rows`，随后 GET 回读验证。目标 project ref 必须且只能匹配一个 checked-in `[remotes.*].project_id`。

```bash
SUPABASE_ACCESS_TOKEN='<由 secret store 注入>' \
python scripts/apply_postgrest_config.py \
  --project-ref fotofiyqnuyvgtotswie \
  --check
```

修改 gate 前运行 `python -m unittest scripts/test_apply_postgrest_config.py`。PATCH 后回读失败时，脚本会恢复并验证之前的白名单字段快照。不得用无条件 `supabase config push` 替代，也不得打印 Management API response body。

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
- 默认 schema 列表是 `public`、`api`、`private`、`util`、`archive`
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
- 默认包含 `public`、`api`、`private`、`util`、`archive`；只有在明确需要缩小检查范围时才使用 `--schemas`
- 重建 `global/` 和 `schemas/`
- 保留 `supabase/workspace/README.md`
- 保留 `supabase/workspace/README.zh-CN.md`
- 保留 `supabase/workspace/changes/`

注意：

- `remote_schema.sql`、`global/` 和 `schemas/` 中的手工修改都不稳定
- 刷新时可能覆盖这些生成文件里尚未提交到 Git 的改动
- 如果你希望 `--git-changes` 只反映后续手工修改，应在同步远程数据库并刷新 workspace 之后，先把新的 `supabase/workspace/schemas` 提交到 Git，再开始编辑
- 远程 `dev` 仍是生成 schema 的权威目标。只有在已应用 migration 与托管目标一致，并对本次合同完成托管 catalog 定向检查后，才能提交本地重建产物。

### `schema_boundary_phase.py`

根据 live catalog 与已提交 public-object inventory 检查版本化 Expand/Contract 边界合同。Expand 要求九张核心 public 表继续存在，且其余 inventory public 表都有 non-public target；Contract 才执行最终精确 public allowlist。

```bash
DATABASE_URL='postgresql://...' python scripts/schema_boundary_phase.py
```

该 checker 只读，并由 `run_database_contract.py` 调用。Issue #354 还提供 `test_schema_boundary_data_api.py` 用于本地 PostgREST profile/角色验证，以及 `test_schema_boundary_rollback.py` 用于本地 operator rollback/roll-forward OID 验证。

owner-only rollback 会 fail-closed，必须显式提供预部署保留的 ACL 证据。只能使用以下二者之一：

```bash
psql "$DATABASE_URL" -v source_service_role_maintain=false -f supabase/operator/issue_354_restore_schema_boundary.sql
psql "$DATABASE_URL" -v source_service_role_maintain=true -f supabase/operator/issue_354_restore_schema_boundary.sql
```

只有保留的 source readback 证明存在显式 `service_role MAINTAIN` grant 时才选择 `true`。缺失或其他值会在 canonical view 移动前被拒绝。

Catalog export 在写入前还会拒绝被 Issue #339/#354 rollback 或 blank replay 污染的状态：14 个已复核的 PostgreSQL-17 replay relation 以及五个 canonical/compatibility view 名称不得有 `service_role MAINTAIN`；internal helper 与四个已复核的 public helper facade 必须保留 browser-role 边界；两个 lifecycle bundle RPC 必须继续拒绝 anon/authenticated。该 guard 有意限定在本 Issue 范围内；它不声称所有无关 application relation 的 `service_role MAINTAIN` 都为零。与已复核 catalog artifact 的 byte equality 仍是独立门禁。

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

### `run_database_contract.py`

裸跑 `supabase test db` 不是仓库级门禁：递归发现会把 benchmark、fixture、
Preview、upgrade 与 transition-migration 支持 SQL 混入 pgTAP。使用 manifest
runner 查看精确清单，不会访问数据库：

```bash
python scripts/run_database_contract.py --suite canonical-local --validate-manifest-only
python scripts/run_database_contract.py --suite canonical-local --list
```

当前 manifest 动态得到 64 个 canonical 顶层 pgTAP，并保留 19 个带分类、
处置状态、跟踪 Issue 与 replacement 元数据的显式 exclusion。清单证据同时记录
exact commit、migration head、CLI 版本、manifest/文件清单 hash 与 worktree dirty
状态，避免把本地 dirty 内容误写成 clean commit 证据。`lca-private-expand`
使用 `--if-activated` 时，仅在仓库尚未跟踪任何 #357 generator、contract 或
migration 激活路径时报告未激活。一旦出现任一激活路径，完整的 freeze/receipt、
sidecar/schema、capture/generator 脚本与两阶段 reviewed migration 都必须各自
唯一匹配；versioned physical-object、动态 exposure-surface、fingerprint 与 receipt
语义委托给 #357 官方 freezer 校验。官方 `check-delivery` 必须验证两份 JSON
Schema、receipt 对 phase 的明确授权，并重新生成 API pre-expand/physical-cut SQL
逐字节对比；激活后还必须运行 #357 freezer/generator/exposure 专项单测。闭包不完整、
未授权、SQL 漂移或匹配不唯一会在执行 SQL 前
fail-closed，不会产生空绿色结果。

### `test_lca_snapshot_family_upgrade.py`

对一个显式 disposable loopback Supabase 数据库执行 Issue #376 的本地破坏性迁移
qualification：

```bash
python scripts/test_lca_snapshot_family_upgrade.py \
  --db-url "$ISSUE_376_DB_URL"
```

checked contract 固定 ancestor database commit 及其精确 predecessor migration
head、唯一允许的 committed migration delta、10,000 行 network/artifact fixture 与
lock/time/WAL budgets。runner 验证 OID、行数、主键与完整行内容 hash，第二个
`ALTER TABLE` 的失败原子性、clean upgrade、直接迁移重试、
private-state drift 拒绝，以及 committed rollback/roll-forward。它拒绝非 loopback
URL，并会破坏性 reset 所选数据库。

### `test_issue_390_pre_ddl_gate.py`

对 LCA result/cache/latest/factorization family 执行只读、离线的 pre-DDL
授权门禁：

```bash
python -m pip install --disable-pip-version-check "jsonschema==4.23.0" "pglast==8.4"
python -m unittest scripts.test_issue_390_pre_ddl_gate
```

checked contract 绑定精确 `dev` 基线与 migration head、七个目标对象及其递归应用对象
依赖闭包、digest-bound repository catalog 与 hosted owner receipt、active consumer canonical/candidate
tuple、可复算且明确不授权 DDL 的 runtime receipt、advisor baseline 与 owner sign-off
状态。`ddlAuthorized=false` 时，已提交 migration history 保持 append-only，新增的
target-neutral static migration 可以继续进入仓库。单独交付并测试的 additive
service-only `api` facade 必须在 migration 第一次提交的同一 commit 中匹配精确受审的
path/blob/classification，并通过固定版本 `pglast` 的 PostgreSQL AST 语义校验；后续
commit 不能追溯授权。opaque/dynamic execution、
relation-moving DDL、提前撤销历史 authenticated access，以及 browser role 的
`private` grant 都是不可被 allowlist 覆盖的 hard deny。static 排除项包括但不限于
顶层 DML、CTAS/SELECT、index 或 exclusion-index build、已验证 constraint 与
partition、column type/storage rewrite、非纯 metadata 的 add-column、`SET NOT NULL`、
custom type/access method、trigger/rule 状态变更，以及 migration identity/owner 切换，
因为既有 trigger、view、FDW、operator、cast、constraint 或 access method 可能执行
尚未证明安全的代码。任何对象移入 exposed `api`/`public` 都被拒绝；新 exposed view
必须为 security-invoker，exposed routine 不得为 security-definer 或引用 internal
对象。受审 facade signature 必须显式使用 `pg_catalog` type，避免 migration session
中的 type shadow 改变 identity；已创建的
`api.lca_*` 与 `api.cmd_lca_*` facade 也持续禁止后续 replacement、权限或 security
mode 弱化。HEAD、index 与 worktree
分别读取各自版本的合同。单次日志零命中也不构成 burn-in。canonical
manifest contract 会导入该 test case，因此沿用既有 CI 而不新增第二条 workflow。

Issue #323 的 Review-progress migration 不会放宽上述通用 hard deny。只有迁移首次
出现时的精确 path 与 Git blob 同时匹配合同中的
`review-progress-least-privilege-reviewed` 分类，才会进入专用
`issue_323_review_progress_semantic_gate.py` qualification。该 qualification 绑定
规范化 AST 和精确语句顺序，并证明 executor 为不可登录、不可继承，只拥有两张表的
只读 ACL、五个 helper 的 EXECUTE、可信 search path、固定 RPC signature 与 browser
ACL，同时要求用于 owner transfer 的临时 `postgres` membership 由原 grantor 在同一
migration 中撤销。source、AST、role、ACL、owner、search path、relation 或 procedural
body 任一漂移都会 fail closed；其他 migration 的原始 hard-deny 信号保持不变。

配套的 `issue_323_review_notification_semantic_gate.py` qualification 同样不会
抑制 notification-event identity migration 原有的五个通用 hard-deny 信号。它只允许
替换一个仅覆盖 legacy row 的 partial unique index，以及对既有 validation notification
command 的一次 byte/AST 锁定 replacement。验证器冻结 predecessor security envelope、
signature、authorization/error contract、精确 public relation/helper 集合、legacy
conflict predicate、owner/ACL 不变性与 command-audit 写入；增加任何 statement、
relation、privilege、动态执行或改变 event-key predicate 都会 fail closed。

### `issue_390_physical_qualification.py`

在 Issue #390 尚无 relation-moving DDL 时，先定义不可授权的 physical-move
qualification harness：

```bash
python scripts/issue_390_physical_qualification.py --check
python scripts/issue_390_physical_qualification.py --print-run-plan
python -m unittest scripts.test_issue_390_physical_qualification
```

默认 suite 会跳过真实数据库 case。使用以下 opt-in 命令在 exact predecessor 数据库上
完整执行 SQL query 与 receipt validator：

```bash
ISSUE_390_BASELINE_DB_URL='<explicit-loopback-url>' \
  python -m unittest \
  scripts.test_issue_390_physical_qualification.Issue390PhysicalQualificationLiveIntegrationTest
```

v1 plan 精确绑定 `database-engine/dev@a29f26a9` 与 migration head
`20260803090000`，不绑定 migration、rollback 或 populated fixture，并保持
`ddlAuthorized`、`relationMovingDdlAllowed`、
`historicalAuthenticatedSelectRemovalAllowed` 与破坏性 qualification execution
全部为 false，因此 `--qualify` 必须 fail closed。

显式 loopback 数据库可以运行 `--capture-baseline --db-url ... --output ...`，但只允许
`postgres` 或 `supabase_admin`，且必须证明其通过 superuser、`BYPASSRLS` 或全部对象
owner 身份获得四个精确普通 relation 的完整行可见性，并验证三个精确 function。
loopback 与只读都不证明数据库 disposable，也不证明实例相互独立。数据库 receipt
要求 loopback 的是客户端连接 endpoint；容器内 PostgreSQL 的 `inet_server_addr()`
可以如实返回 bridge interface。数据库 receipt 独立绑定 database name/OID、server
address/port、cluster system identifier 与实际应用的
完整 migration set；Git 推导的 repository plan 另行记录。receipt 覆盖 OID、owner、
ACL/column ACL、RLS/policy、双向 FK、index、trigger、publication、行数、PK/content
按主键排序的 canonical SHA-256 PK/完整行 digest、routine property/definition hash，
以及递归 `pg_depend`、view/`pg_rewrite`、
composite/rowtype、dynamic-SQL、regclass candidate。它不存储行 payload，也不提出
授权结论。每个 target 超过 100,000 行或 statement 超过 120 秒时 capture 会失败；
更大表需要 successor 单独受审的 bounded/streaming 设计。

未来 run plan 预留 cluster system identifier 必须不同的独立 fresh/populated upgrade、failure atomicity、lock timeout、
WAL/time budget、retry、rollback 与 roll-forward receipt，并要求不同的
`--fresh-db-url` / `--populated-db-url` loopback 身份及外部 `--receipt-dir`。只有受审 successor
contract 精确绑定 candidate blobs 且全部 pre-DDL gate 独立完成后，才可实现这些执行
阶段；v1 不允许直接改成授权合同。

### `issue_390_external_git_tree.py`

直接从八个外部仓库的精确 Git commit 构建 Issue #397 的非授权 consumer ledger。
脚本校验 canonical origin，通过 Git object 命令遍历每个 regular blob（包括 Next
运行时使用的数据库快照），只保留 blob/行 hash 与语义分类；unsupported entry、
active-runtime 未解析 token 和动态 selector 都 fail closed。Next Edge mirror receipt、
其精确来源树、是否陈旧及内容 parity 分别验证。被规则识别的直接 token 出现次数
不得表述为穷尽性的 consumer 数量。

```bash
python scripts/issue_390_external_git_tree.py --check
python scripts/issue_390_external_git_tree.py --verify-external /absolute/path/to/lca-workspace
python -m unittest scripts.test_issue_390_external_git_tree
```

`--scan-external` 会重写 canonical JSON artifact 与 SHA sidecar，只用于受审的证据
刷新。所有命令都不授权 DDL，也不连接 Supabase Hosted 项目。

### `public_inventory_closure.py`

`public_inventory_closure.py --check` 现在转发到 Issue #405 的离线 exact-head
校验器；旧 #338 generator/provenance 命令只保留给 genesis lineage 与 fixed-SHA
consumer evidence。

```bash
python scripts/public_inventory_closure.py --scan-consumers <lca-workspace-root>
python scripts/public_inventory_closure.py --verify-provenance <lca-workspace-root>
python scripts/public_inventory_closure.py --check
python scripts/public_inventory_exact_head.py --check
python scripts/public_inventory_exact_head.py --check-live --db-url postgresql://...
python scripts/public_inventory_exact_head.py --compare-catalogs \
  --db-url postgresql://... --other-db-url postgresql://...
python -m unittest scripts.test_public_inventory_exact_head scripts.test_public_inventory_closure
```

只有显式 disposable loopback database 可以刷新当前 artifact：

```bash
python scripts/public_inventory_exact_head.py --refresh --db-url postgresql://...
```

v2 绑定 exact source `c5356d2`、migration head `20260803090000`、397 个 live
identity、`9+37+117+230+4` exactly-once partition，以及完整的 388-residue
Contract DROP identity checklist。checklist 不是可执行 SQL：所有 identity 均为
`blocked`，不生成 migration，`contractReady=false` 永远成立。missing、unknown、
duplicate、count、schema、hash、counterpart 或 live-ledger drift 都会失败关闭。

旧 #338 artifact/hash 继续以 `public_object_inventory.genesis.*` 保持不可变；
security lineage 仍引用 genesis，不把 v2 刷新解释成历史重写。

### `security_definer_audit.py`

校验冻结的 public SECURITY DEFINER 基线产物，或显式对照 exact genesis schema：

```bash
python scripts/security_definer_audit.py --write
python scripts/security_definer_audit.py --check
DATABASE_URL=<genesis-loopback-url> python scripts/security_definer_audit.py --check-live-baseline
python -m unittest scripts/test_security_definer_audit.py
```

`--check` 仅校验 artifact，不表示当前 catalog 状态；`--check-live-baseline` 仅适用于
exact genesis schema。当前状态由 live v2 audit gate 负责。v1 产物保留全部 241 个
exact signatures：129 个 #333 owner/runtime residue
（90 api、39 private）、14 个 #339 RLS-bound facade，以及 98 个 inventory static
closure。逐项字段严格区分 observed catalog evidence、inferred signal、required
Contract proof 与 confirmed fact；静态 signal 不等于 runtime authorization 证明。
#352 继续 Blocked，#358 负责物理迁移，gate 始终保持 `contractReady=false`。

### `security_definer_audit_v2.py`

保留 immutable v1 public baseline，同时生成跨 schema privileged-routine lineage
与当前 endpoint 审计：

```bash
DATABASE_URL=<loopback-url> python scripts/security_definer_audit_v2.py --bootstrap-write
DATABASE_URL=<loopback-url> python scripts/security_definer_audit_v2.py --write
DATABASE_URL=<loopback-url> python scripts/security_definer_audit_v2.py --check
python scripts/security_definer_audit_v2.py --plan-transition-advance \
  --batch issue-356-worker-control-plane --database-schema-sha <exact-40-hex-commit>
python -m unittest scripts/test_security_definer_audit_v2.py
ISSUE333_DATABASE_URL=<loopback-url> \
  python -m unittest scripts/test_security_definer_audit_v2_postgrest_conformance.py
```

`--bootstrap-write` 仅用于受审 exact baseline schema。后续迁移批次必须显式更新
lineage mapping，并在 exact-SHA clean reset 后使用 `--write`。审计覆盖
`public`、`api`、`private`、`util`、`archive`；每个 SECURITY DEFINER endpoint
必须且只能作为一个 active lineage 的 canonical。compatibility alias 必须是
SECURITY INVOKER；未来若需 privileged 例外，必须新增受审 lineage/version，不能
把它记作 privileged alias。Invoker wrapper 只能是 alias，不能替代 canonical。role matrix 分别记录 schema USAGE、
effective EXECUTE、effective callable 与 Data API exposure。Data API 证据保留 exposed
schema 的受审顺序，分别证明 PostgREST schema cache、请求解析与直接 SQL 调用能力，
并验证 `authenticator` 对受支持 transport role 的 `SET ROLE` 权限。v14.7
conformance test 会用一次性、仅 loopback 暴露的 PostgREST 容器，对账 anon OpenAPI
routes、无 Profile 时的首 schema 路由，以及内部 schema 的负向 Profile。数据库密码
仅通过 `PGPASSWORD` 传给 `psql`，不进入 argv。transition 引用的文件必须是规范仓库
相对路径下的 Git 普通文件，并以 no-follow 方式读取。已提交的 #356 fixture
证明 11 个 Worker move 和一个 composite-signature move 后仍保留 315 个 lineage，
23 个 invoker aliases 不增加 privileged endpoint 总数。
transition-advance plan 会确定性冻结当前 v2 bytes、生成 immutable predecessor/produced
artifact receipt、结算 sequence 0 并开启 sequence 1，同时输出 exact reviewed-code constants。
迁移 PR 必须物化并校验 plan 中的全部文件，禁止手工拼接 `completedTransitions`，也不能
把会被覆盖的 `security_definer_audit_v2.json` 当作历史产物。

资格收据本身必须是当前 clean source HEAD 中的受审普通文件。为避免收据把自身所在
commit SHA 写进自身而形成不可满足的 Git 哈希自引用，收据中的 `source.commitSha`
明确指向包含 migration 与 rollback 精确 bytes 的受审祖先提交。runner 直接从该
commit 的 immutable Git blob 重放 SQL，并证明 base → source → receipt HEAD 的祖先链；
fixture 与资格收据仍由当前 HEAD 的精确 SHA-256 单独绑定。

#356 PR gate 还必须运行真实的双 stack integration harness；不存在可计为成功的 skip：

```bash
python scripts/run_database_contract.py --suite canonical-local \
  --security-definer-transition-workdir <clean-stack-a> \
  --security-definer-transition-workdir <clean-stack-b> \
  --security-definer-transition-source-workdir <clean-source-worktree> \
  --security-definer-transition-migration <issue-356-migration.sql> \
  --security-definer-transition-rollback <issue-356-operator-rollback.sql> \
  --security-definer-transition-qualification-receipt \
    supabase/tests/contracts/security_definer_transition_qualification_receipt.issue-356.json \
  --security-definer-transition-qualification-receipt-sha256 <exact-sha256> \
  --security-definer-transition-migration-sha256 <exact-sha256> \
  --security-definer-transition-rollback-sha256 <exact-sha256> \
  --security-definer-transition-base 597072ca34a62cdc93df9bf0896a9d361901852c
```

两个 workdir 必须是处于 exact base 的独立 loopback stack。gate 会执行 baseline audit、
migration、live transition audit、operator rollback、baseline bytes 恢复、rollforward，
并从 immutable sequence-0 lineage/audit 读取 baseline，禁止结算后用 mutable current
lineage 替代历史基线，同时比较第二个 stack 的 bytes/SHA。缺少输入、SQL bytes 改变、
reset 失败或 audit drift
都会 fail closed。

### Security ACL Expand 验证

`test_security_acl_upgrade.py` 对 Issue #339 migration 执行带数据 base-to-head、
事务内故障注入、重试、数据 parity、按环境快照恢复 ACL 和再次 roll-forward。

`test_security_acl_global_function_defaults_upgrade.py` 从 PostgreSQL 17 follow-up
migration 之前启动，以真实新函数复现内建 global `PUBLIC EXECUTE`，再通过 scratch
non-application schema 证明 revoke 影响 `postgres` 在整个数据库创建的未来函数。
它同时覆盖带 grant option 的显式 global row、owner execute、built-in/global/per-schema
effective catalog、当前对象 ACL 与 application row-count parity、事务故障、幂等重试、
global 与五个 additive per-schema 层的精确 restore 和 roll-forward。custom per-schema
grant option 证明 layering/grantability 被精确恢复；custom table-default role 证明 snapshot
动态移除所有 non-owner grantee，同时保留 `postgres` owner。本迁移不扫描或改写 application relation，因此百万行
夹具不会改变其锁、WAL 或执行行为，不适用。共享 local stack 不干净时，通过
`SUPABASE_WORKDIR` 指向独立的一次性项目根目录。

```bash
python scripts/test_security_acl_upgrade.py
python scripts/test_security_acl_global_function_defaults_upgrade.py
```

`hosted_security_acl.py` 是 fail-closed hosted operator gate：组合数据库 posture、
Management API `db_schema` readback 和真实 anon REST negative probes。默认只读；
hosted PostgREST 配置须另行通过 `scripts/apply_postgrest_config.py` 的受审 diff、
readback 和 rollback 合同应用或协调。
posture 会计算 built-in、global 和 per-schema effective defaults。repo-owned global
revoke 影响 `postgres` 在整个数据库创建的未来函数，gate 的部署目标仍是五个 application
schema。platform-owned `supabase_admin` residue 继续作为 #352 的 fail-closed blocker，
本 migration 不得宣称已修复它。

精确 exposed schema 集合及受审顺序为 `api,public,graphql_public`；不得包含
`private`、`util` 或 `archive`。opaque `sb_publishable_` credential 仅作为
`apikey` 发送；legacy JWT 形态 anon key 才同时作为 Bearer。离线参数合同用以下命令验证：

```bash
python -m unittest scripts/test_hosted_security_acl.py
```

### identity/collaboration Expand 验证

`test_identity_collaboration_data_api.py` 验证 Issue #355 版本化 DTO、notification
浏览器 RPC-only 合同、authenticated/service/anonymous 角色分离、PostgREST schema
cache reload 和 internal schema 负向 profile。`test_identity_collaboration_concurrency.py`
使用 8 个并发 session 执行 800 次 public/private checksum parity 调用。静态测试固定
16 个精确 inventory 对象、consumer SHA、版本化 DTO、事务/timeout 与 Contract gate。
`test_identity_collaboration_policy_variants.py` 只允许在 disposable local 运行：它复现
blank/repository 与 persistent Dev/Production 两个精确 users-policy fingerprint，证明
Expand 保留任一 predecessor、所有 security-invoker projection 权限不超过 source，并在
mutation 前拒绝第三种 unknown variant。live legacy variant 只是兼容证据，不是批准的
安全目标；收紧继续由 Next #753 与 database-engine #358 跟踪。

```bash
python -m unittest scripts/test_identity_collaboration_expand_static.py
python scripts/test_identity_collaboration_policy_variants.py
python scripts/test_identity_collaboration_data_api.py
python scripts/test_identity_collaboration_concurrency.py
```

### Issue #390/#395 result API facade runtime

`test_issue_390_result_api_facade_runtime.py` 是仅允许 loopback 的八个 service-only
`api` routine 验证器。它冻结服务成功 DTO、anon/authenticated/private profile 精确拒绝、
8 请求 HTTP admission race、8 个独立 PostgreSQL backend race、same-binding replay 与
清理后的零残留。Issue #395 进一步用 8 个并发请求验证同一 cancelled Worker job
收敛为 failed：每次调用仅增加一次 hit 且不改变任何身份字段，随后第二阶段 retry
原子清除旧 result/error 并重新绑定。canonical-local 选中 Issue #390 facade pgTAP
合同时会自动执行该脚本。

```bash
python scripts/test_issue_390_result_api_facade_runtime.py
```

canonical runner 只解析一次显式 loopback stack：先把 database identity 与所选
`SUPABASE_WORKDIR` 对账，再向每一个 SQL、Data API、catalog、schema phase、inventory、
lint 和 SECURITY DEFINER gate 覆盖并传递同一组 `DATABASE_URL`、REST credentials 与
workdir。`test_database_contract_targeting.py` 提供离线双 stack 混拼负向测试。
canonical runner 默认不执行 destructive identity DDL；只可在 disposable local stack
上用 `--run-destructive-identity-qualification` 显式启用。该 mandatory gate 先执行
dual exact-hash preservation/retry、unknown 原子拒绝与逐角色 RLS matrix harness，再执行
rollback/roll-forward/lock-failure harness；该演练会先把选定 stack reset 到 Issue #355
精确 head `20260801061000`，完成后恢复到当前仓库 head。资格验证或恢复任一失败都会终止
canonical runner。
`--skip-data-api` 只跳过 HTTP probe，不选择或重定向破坏性测试目标。runner control-flow
合同位于 `test_database_contract_identity_qualification.py`。

可重复 operator rollback 为
`supabase/operator/issue_355_restore_identity_collaboration_expand.sql`；它只删除新增
api/private Expand 对象，不改动受审 public 物理 routine 或其 OID。
rollback harness 还验证 reviewed predecessor routine/额外 overload 拒绝、任意 custom
ACL/owner 收敛、精确 migration head 与完整 target fingerprint 删除门、tamper/partial-state
拒绝、连续两次 rollback 后目标对象为零，以及精确 roll-forward。

### `test_production_equivalent_upgrade.py`

这是只连接当前本地 Supabase 项目的全局 populated-upgrade 资格验证入口。它从
审定的 `20260731124000` base 升级到仓库精确 head，生成身份、评审、通知、审计、
Worker 生命周期、package、cache、release、closure 以及百万行 package evidence
夹具；对全库行数、主键和内容哈希建立 oracle（仅规范化 reset 生成的通用
`created_at`/`updated_at`/`modified_at` 重置时间戳和 migration evidence 的
`captured_at` 时间戳）；对每个待执行 migration 注入事务
故障并逐项 rehearsal；在真实 CLI roll-forward 前使用合同固定的 Issue #339 operator
rollback 清理其 database-global rehearsal role，再 reset 并确定性加载 populated
base；验证五秒锁超时及并发只读兼容；证明所有 base relation oracle 保持不变，
同时让新增物理 evidence relation 精确匹配 rehearsed head；最后将 constraint、
ACL/RLS、policy、trigger、publication 对账到该 head，并验证 WAL、重试和预期对象。

脚本不会连接 linked/hosted 项目，也不会把凭据或原始行写入证据；命令和失败
输出中的 `postgres://` 与 `postgresql://` URL 都会脱敏。证据路径必须显式指定
并放在 worktree 外，只能以 exclusive/no-follow 语义新建 mode-0600 regular
file；成功前会 fsync 文件与目录，任何已存在目标（包括 symlink）都会被拒绝。
正式资格验证要求 clean commit 和至少一百万行；`--allow-dirty` 与较小规模只用于
开发。`--db-url` 可显式选择隔离的本地 stack，但会拒绝所有非 loopback host；
no-op migration retry 必须产生精确的零 WAL bytes。

```bash
python scripts/test_production_equivalent_upgrade.py \
  --evidence-out /tmp/database-engine-upgrade-evidence.json
```

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
