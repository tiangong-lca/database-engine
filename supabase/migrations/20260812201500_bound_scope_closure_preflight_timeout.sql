-- The zero-release candidate snapshot currently aggregates and hashes a
-- production-scale manifest before enqueueing the closure check. Keep the
-- extended budget on this actor RPC only; all other authenticated calls retain
-- the role-level statement timeout.
alter function api.cmd_lcia_scope_closure_check_request_v2(jsonb, text, jsonb)
  set statement_timeout = '60s';

notify pgrst, 'reload schema';
