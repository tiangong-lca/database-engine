-- The versioned Task Center RPC is the only authenticated feed entry point.
-- `ALTER FUNCTION ... RENAME` preserves the original EXECUTE ACL, so the
-- implementation function renamed in 20260722043552 must be explicitly
-- closed after the wrapper is created.
revoke all on function public.get_task_summary_v2_feed_unversioned(
  text, text[], text[], timestamptz, timestamptz, uuid, integer, boolean
) from public, anon, authenticated, service_role;

-- These implementation functions were already closed by the original
-- migration. Repeat the revocations defensively so this forward migration
-- leaves every internal closure-command alias non-callable.
revoke all on function public.cmd_lcia_scope_closure_check_request_v2_untracked(
  jsonb, text, jsonb
) from public, anon, authenticated, service_role;
revoke all on function public.cmd_lcia_result_build_request_v2_envelope(
  text, jsonb, text, text, jsonb, text, uuid, text, text, jsonb
) from public, anon, authenticated, service_role;

-- Preserve the public product contract: authenticated callers use only the
-- versioned wrapper, which adds the TaskSummaryV2 schema marker.
revoke all on function public.get_task_summary_v2_feed(
  text, text[], text[], timestamptz, timestamptz, uuid, integer, boolean
) from public, anon, authenticated, service_role;
grant execute on function public.get_task_summary_v2_feed(
  text, text[], text[], timestamptz, timestamptz, uuid, integer, boolean
) to authenticated;
