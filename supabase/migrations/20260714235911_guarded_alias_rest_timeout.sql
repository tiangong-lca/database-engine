-- Give only the authenticated whole-plan REST RPC enough time to finish its
-- bounded validation and atomic write. PostgREST hoists this function setting
-- before the RPC main query; direct SQL callers must establish their own
-- statement timeout before invoking the function.
alter function public.cmd_dataset_alias_plan_guarded(jsonb)
  set statement_timeout = '45s';

-- The internal executor inherits the one whole-plan budget. It must not start
-- or advertise a separate per-dimension timeout budget.
alter function public.cmd_dataset_alias_batch_guarded(jsonb)
  reset statement_timeout;

notify pgrst, 'reload schema';
