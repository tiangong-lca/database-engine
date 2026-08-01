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
lastReviewedAt: 2026-08-01
lastReviewedCommit: a1be848fefc88d68c1073f98c9e3ecf866095399
lastReviewedNote: "已为 Issues #353/#354 与 #333 复核：保留 immutable provenance 和五 schema 验证入口，并记录 deterministic SECURITY DEFINER audit、role matrix 与 fail-closed #352/#358 边界。"
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

### `public_inventory_closure.py`

该脚本把 workspace #533 的逐对象 ledger 与 database #337 merge head 的实时
catalog 合并为稳定合同，覆盖 table、view、materialized view、function/procedure、
精确 routine identity arguments、ACL/RLS/default privileges，以及
FK/rewrite/trigger/policy/composite/function-body 依赖。输出还包含 SCC-aware Expand
顺序、反向 Contract 顺序、固定 consumer commit SHA 的静态证据和显式 residue。

```bash
python scripts/public_inventory_closure.py --scan-consumers <lca-workspace-root>
python scripts/public_inventory_closure.py --verify-provenance <lca-workspace-root>
python scripts/public_inventory_closure.py --write
python scripts/public_inventory_closure.py --check
python -m unittest scripts/test_public_inventory_closure.py
```

只有 consumer SHA 变化时才重新扫描。`contractReady=false` 表示仍有 dynamic SQL
或 runtime/owner 证据待关闭；缺少 mapping、无效 target、非精确 SHA、重复 key 或
live/ledger 漂移会直接失败。未知 consumer 始终保留为 blocker，不能据此退休对象。

`source` 将 workspace baseline、其精确 `database-engine` gitlink、历史
review/source/merge-base lineage，与唯一用于 migration/catalog 重放的
`databaseSchemaSha` 明确分离；旧 #338 artifact hash 仅作为 lineage。
`--verify-provenance` 不读取移动 remote 即可重放这些关系；`--check` 先验证
canonical JSON bytes 与 committed SHA-256，再执行 committed-vs-generated 对比。

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

```bash
python scripts/test_security_acl_upgrade.py
```

`hosted_security_acl.py` 是 fail-closed hosted operator gate：组合数据库 posture、
Management API `db_schema` readback 和真实 anon REST negative probes。默认只读；
hosted PostgREST 配置须另行通过 `scripts/apply_postgrest_config.py` 的受审 diff、
readback 和 rollback 合同应用或协调。
`supabase_admin` default privileges 由有权限的 owner session 单独执行
`supabase/operator/issue_339_supabase_admin_default_privileges.sql`。

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

```bash
python -m unittest scripts/test_identity_collaboration_expand_static.py
python scripts/test_identity_collaboration_data_api.py
python scripts/test_identity_collaboration_concurrency.py
```

三个 probe 都通过 `IDENTITY_COLLABORATION_DATABASE_URL`（或 `DATABASE_URL`）
绑定同一个显式 loopback database，并与所选 `SUPABASE_WORKDIR` stack 做 identity
对账。canonical runner 默认不执行 rollback DDL；只可在 disposable local stack 上用
`--run-destructive-identity-qualification` 显式启用。`--skip-data-api` 只跳过 HTTP
probe，不选择或重定向破坏性测试目标。

可重复 operator rollback 为
`supabase/operator/issue_355_restore_identity_collaboration_expand.sql`；它只删除新增
api/private Expand 对象，不改动受审 public 物理 routine 或其 OID。
rollback harness 还验证任意 custom ACL/owner 收敛、连续两次 rollback 后目标对象为零、
dependency/comment/property 指纹与 roll-forward。

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
