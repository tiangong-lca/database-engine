begin;

set local lock_timeout = '5s';
set local statement_timeout = '2min';

create index lca_result_gc_operations_live_result_idx
  on private.lca_result_gc_operations (live_result_id);

create index lca_result_gc_finalize_context_operation_idx
  on private.lca_result_gc_finalize_context (operation_id);

commit;
