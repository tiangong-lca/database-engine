-- Supabase Branching replays this historical migration through `db push` on a
-- fresh database. The CLI does not classify DROP INDEX CONCURRENTLY as a
-- pipeline-incompatible statement, so the hosted runner otherwise aborts with
-- SQLSTATE 25001 before later migrations can run. Fresh replay has no live
-- workload to protect, and existing remote histories do not reapply this file.
drop index if exists public.flows_json_pgroonga;
