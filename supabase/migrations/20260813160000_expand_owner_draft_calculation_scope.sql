-- Owner identity and draft state determine calculation eligibility.
-- team_id and review_id are collaboration workflow metadata; they do not
-- remove an actor-owned state-zero Process from public_plus_owner_draft.

set lock_timeout = '5s';
set statement_timeout = '120s';

do $owner_draft_scope$
declare
  fn text;
begin
  fn := pg_get_functiondef(
    'private.search_processes_latest_v2_impl(text,jsonb,bigint,bigint,text,text,uuid,integer,text,text[],boolean)'::regprocedure
  );

  if strpos(fn, 'p.state_code = 0 and p.team_id is null and p.review_id is null') = 0
     or strpos(fn, 'p2.state_code = 0 and p2.team_id is null and p2.review_id is null') = 0 then
    raise exception 'owner-draft Process search predicate no longer matches the reviewed predecessor';
  end if;

  fn := replace(
    fn,
    'p.state_code = 0 and p.team_id is null and p.review_id is null',
    'p.state_code = 0'
  );
  fn := replace(
    fn,
    'p2.state_code = 0 and p2.team_id is null and p2.review_id is null',
    'p2.state_code = 0'
  );
  execute fn;

  fn := pg_get_functiondef(
    'private.search_processes_latest_v2_impl(text,jsonb,bigint,bigint,text,text,uuid,integer,text,text[],boolean)'::regprocedure
  );
  if strpos(fn, 'owner_draft_only') = 0
     or strpos(fn, 'p.state_code = 0') = 0
     or strpos(fn, 'p2.state_code = 0') = 0
     or strpos(fn, 'p.team_id is null') > 0
     or strpos(fn, 'p.review_id is null') > 0
     or strpos(fn, 'p2.team_id is null') > 0
     or strpos(fn, 'p2.review_id is null') > 0 then
    raise exception 'owner-draft Process search predicate did not converge on actor plus state';
  end if;
end
$owner_draft_scope$;

comment on function private.search_processes_latest_v2_impl(text,jsonb,bigint,bigint,text,text,uuid,integer,text,text[],boolean) is
  'Canonical Process search implementation. owner_draft_only admits actor-owned state_code=0 rows regardless of team_id or review_id workflow metadata and fails closed outside my-data scope.';
