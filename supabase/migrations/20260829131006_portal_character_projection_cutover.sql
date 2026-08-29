-- Issue #551: reconcile the narrow character projection and atomically route
-- validated one-code-point, unfiltered relevance Search through its bounded
-- pre-limit kernel. Every other Search shape retains the existing candidate
-- and PGroonga implementation.

begin;

set local lock_timeout = '5s';
set local statement_timeout = '120s';

do $portal_character_cutover_prerequisite_guard$
declare
  v_coordinator_sha256 text;
begin
  select pg_catalog.encode(
    extensions.digest(
      pg_catalog.convert_to(
        pg_catalog.pg_get_functiondef(
          'private.portal_search_v1(text,text,jsonb,text,text,integer)'::regprocedure
        ),
        'UTF8'
      ),
      'sha256'
    ),
    'hex'
  )
  into v_coordinator_sha256;

  if v_coordinator_sha256 <>
       '09f69323095469234c0183338fae3dadaa6fd54c47335a7e0f334c9038ef017f'
     or pg_catalog.to_regclass(
       'private.portal_catalog_character_rows_v1'
     ) is null
     or pg_catalog.to_regprocedure(
       'private.sync_portal_catalog_character_row_v1()'
     ) is null
     or pg_catalog.to_regprocedure(
       'private.catalog_portal_single_character_search_v1_impl(text,text,text,uuid,text,integer,text)'
     ) is not null
     or pg_catalog.to_regprocedure(
       'private.assert_portal_catalog_character_contract_v1()'
     ) is not null then
    raise exception 'Portal character cutover prerequisites drifted'
      using errcode = '55000';
  end if;
end
$portal_character_cutover_prerequisite_guard$;

lock table private.portal_catalog_search_rows_v1
in share row exclusive mode;

grant api_internal_executor to postgres;
set role api_internal_executor;

insert into private.portal_catalog_character_rows_v1 (
  dataset_kind,
  id,
  version,
  state_code,
  modified_at,
  document_characters,
  name_characters,
  name_exact_characters,
  classification_characters,
  classification_exact_characters,
  character_contract_version
)
select projection.dataset_kind,
  projection.id,
  projection.version,
  projection.state_code,
  projection.modified_at,
  private.portal_catalog_character_set_v1(projection.document),
  private.portal_catalog_character_field_set_v1(
    projection.card -> 'names', 'value', false
  ),
  private.portal_catalog_character_field_set_v1(
    projection.card -> 'names', 'value', true
  ),
  private.portal_catalog_character_field_set_v1(
    projection.card -> 'classifications', 'code', false
  ),
  private.portal_catalog_character_field_set_v1(
    projection.card -> 'classifications', 'code', true
  ),
  1
from private.portal_catalog_search_rows_v1 as projection
left join private.portal_catalog_character_rows_v1 as character_row
  on character_row.dataset_kind = projection.dataset_kind
 and character_row.id = projection.id
 and character_row.version = projection.version
where character_row.id is null
on conflict (dataset_kind, id, version) do nothing;

delete from private.portal_catalog_character_rows_v1 as character_row
where not exists (
  select 1
  from private.portal_catalog_search_rows_v1 as projection
  where projection.dataset_kind = character_row.dataset_kind
    and projection.id = character_row.id
    and projection.version = character_row.version
);

do $verify_portal_character_reconcile$
begin
  if exists (
       (
         select projection.dataset_kind,
           projection.id,
           projection.version,
           projection.state_code,
           projection.modified_at
         from private.portal_catalog_search_rows_v1 as projection
         except
         select character_row.dataset_kind,
           character_row.id,
           character_row.version,
           character_row.state_code,
           character_row.modified_at
         from private.portal_catalog_character_rows_v1 as character_row
       )
       union all
       (
         select character_row.dataset_kind,
           character_row.id,
           character_row.version,
           character_row.state_code,
           character_row.modified_at
         from private.portal_catalog_character_rows_v1 as character_row
         except
         select projection.dataset_kind,
           projection.id,
           projection.version,
           projection.state_code,
           projection.modified_at
         from private.portal_catalog_search_rows_v1 as projection
       )
     )
     or exists (
       select 1
       from private.portal_catalog_search_rows_v1 as projection
       join private.portal_catalog_character_rows_v1 as character_row
         on character_row.dataset_kind = projection.dataset_kind
        and character_row.id = projection.id
        and character_row.version = projection.version
       where pg_catalog.mod(
           pg_catalog.abs(pg_catalog.hashtextextended(
             projection.dataset_kind || ':' || projection.id::text ||
               ':' || projection.version,
             551
           )),
           1024
         ) = 0
         and (
           character_row.document_characters is distinct from
             private.portal_catalog_character_set_v1(projection.document)
           or character_row.name_characters is distinct from
             private.portal_catalog_character_field_set_v1(
               projection.card -> 'names', 'value', false
             )
           or character_row.name_exact_characters is distinct from
             private.portal_catalog_character_field_set_v1(
               projection.card -> 'names', 'value', true
             )
           or character_row.classification_characters is distinct from
             private.portal_catalog_character_field_set_v1(
               projection.card -> 'classifications', 'code', false
             )
           or character_row.classification_exact_characters is distinct from
             private.portal_catalog_character_field_set_v1(
               projection.card -> 'classifications', 'code', true
             )
         )
     ) then
    raise exception 'Portal character projection reconciliation failed'
      using errcode = '55000';
  end if;
end
$verify_portal_character_reconcile$;

reset role;
revoke api_internal_executor from postgres;

grant portal_public_executor to postgres;
grant create on schema private to portal_public_executor;
set role portal_public_executor;

create function private.assert_portal_catalog_character_contract_v1()
returns void
language plpgsql
stable
parallel restricted
security definer
set search_path = ''
set row_security = 'on'
as $function$
begin
  if (
       select not relation.relrowsecurity
         or not relation.relforcerowsecurity
         or relation.relowner <> 'postgres'::regrole
       from pg_catalog.pg_class as relation
       where relation.oid =
         'private.portal_catalog_character_rows_v1'::regclass
     ) is not false
     or (
       select count(*)
       from pg_catalog.pg_attribute as attribute
       where attribute.attrelid =
           'private.portal_catalog_character_rows_v1'::regclass
         and attribute.attnum > 0
         and not attribute.attisdropped
         and attribute.attname in (
           'dataset_kind',
           'id',
           'version',
           'state_code',
           'modified_at',
           'document_characters',
           'name_characters',
           'name_exact_characters',
           'classification_characters',
           'classification_exact_characters',
           'character_contract_version'
         )
     ) <> 11
     or not exists (
       select 1
       from pg_catalog.pg_constraint as parent_fk
       where parent_fk.conrelid =
           'private.portal_catalog_character_rows_v1'::regclass
         and parent_fk.confrelid =
           'private.portal_catalog_search_rows_v1'::regclass
         and parent_fk.conname = 'portal_catalog_character_parent_v1_fk'
         and parent_fk.contype = 'f'
         and parent_fk.convalidated
         and parent_fk.confupdtype = 'r'
         and parent_fk.confdeltype = 'c'
     )
     or (
       select not index_record.indisvalid
         or not index_record.indisready
         or not index_record.indislive
       from pg_catalog.pg_index as index_record
       where index_record.indexrelid =
         'private.portal_catalog_character_rows_latest_v1_idx'::regclass
     ) is not false
     or not exists (
       select 1
       from pg_catalog.pg_trigger as trigger
       where trigger.tgrelid =
           'private.portal_catalog_search_rows_v1'::regclass
         and trigger.tgname = 'portal_catalog_character_sync_v1'
         and not trigger.tgisinternal
         and pg_catalog.pg_get_triggerdef(trigger.oid) ~
           'AFTER INSERT OR UPDATE'
     )
     or (
       select count(*)
       from pg_catalog.pg_policies as policy
       where policy.schemaname = 'private'
         and policy.tablename = 'portal_catalog_character_rows_v1'
         and policy.policyname =
           'portal_catalog_character_rows_portal_select_v1'
         and policy.roles = array['portal_public_executor']::name[]
         and policy.cmd = 'SELECT'
         and policy.qual = 'true'
         and policy.with_check is null
     ) <> 1
     or (
       select count(*)
       from pg_catalog.pg_proc as routine
       where routine.oid in (
           'private.portal_catalog_character_set_v1(text)'::regprocedure,
           'private.portal_catalog_character_field_set_v1(jsonb,text,boolean)'::regprocedure
         )
         and routine.proowner = 'portal_public_executor'::regrole
         and routine.provolatile = 'i'
         and routine.proparallel = 's'
         and routine.prosecdef
         and routine.proconfig @> array['search_path=""']::text[]
     ) <> 2
     or (
       select routine.proowner <> 'api_internal_executor'::regrole
         or not routine.prosecdef
         or not (coalesce(routine.proconfig, '{}'::text[]) @> array[
           'search_path=""',
           'row_security=on'
         ]::text[])
       from pg_catalog.pg_proc as routine
       where routine.oid =
         'private.sync_portal_catalog_character_row_v1()'::regprocedure
     ) is not false then
    raise exception using
      errcode = '55000',
      message = 'Portal character projection contract drifted';
  end if;
end
$function$;

create function private.catalog_portal_single_character_search_v1_impl(
  p_kind text,
  p_query text,
  p_cursor_rank text,
  p_cursor_id uuid,
  p_cursor_version text,
  p_limit integer,
  p_query_fingerprint text
)
returns jsonb
language plpgsql
stable
parallel restricted
security definer
set search_path = ''
set statement_timeout = '8s'
set work_mem = '32MB'
set plan_cache_mode = 'force_custom_plan'
set jit = 'off'
set row_security = 'on'
as $function$
declare
  v_items jsonb;
  v_next_cursor_payload jsonb;
begin
  perform private.assert_portal_catalog_character_contract_v1();

  if p_kind not in ('process', 'flow')
     or pg_catalog.char_length(p_query) <> 1
     or p_limit not between 1 and 50 then
    raise exception 'invalid Portal character Search'
      using errcode = '22023';
  end if;

  with latest as materialized (
    select distinct on (character_row.id)
      character_row.id,
      character_row.version,
      character_row.state_code,
      character_row.modified_at,
      character_row.document_characters,
      character_row.name_characters,
      character_row.name_exact_characters,
      character_row.classification_characters,
      character_row.classification_exact_characters
    from private.portal_catalog_character_rows_v1 as character_row
    where character_row.dataset_kind = p_kind
    order by character_row.id,
      character_row.version desc,
      character_row.modified_at desc,
      character_row.state_code desc
  ), scored as materialized (
    select latest.*,
      case
        when pg_catalog.strpos(
          latest.name_exact_characters, p_query
        ) > 0 then 0.95::numeric
        when pg_catalog.strpos(
          latest.classification_exact_characters, p_query
        ) > 0 then 0.92::numeric
        else 0.70::numeric
      end as score,
      case
        when pg_catalog.strpos(latest.name_characters, p_query) > 0
          then pg_catalog.jsonb_build_array('name')
        when pg_catalog.strpos(
          latest.classification_characters, p_query
        ) > 0 then pg_catalog.jsonb_build_array('classification')
        else pg_catalog.jsonb_build_array('full_text')
      end as reason_codes
    from latest
    where pg_catalog.strpos(latest.document_characters, p_query) > 0
  ), after_cursor as materialized (
    select scored.*
    from scored
    where p_cursor_rank is null
      or scored.score < p_cursor_rank::numeric
      or (
        scored.score = p_cursor_rank::numeric
        and (
          scored.id > p_cursor_id
          or (
            scored.id = p_cursor_id
            and scored.version < p_cursor_version
          )
        )
      )
  ), ordered as materialized (
    select after_cursor.*,
      pg_catalog.row_number() over (
        order by after_cursor.score desc,
          after_cursor.id asc,
          after_cursor.version desc
      ) as page_rank
    from after_cursor
    order by after_cursor.score desc,
      after_cursor.id asc,
      after_cursor.version desc
    limit p_limit + 1
  ), hydrated as materialized (
    select ordered.*,
      projection.card
    from ordered
    join private.portal_catalog_search_rows_v1 as projection
      on projection.dataset_kind = p_kind
     and projection.id = ordered.id
     and projection.version = ordered.version
  )
  select coalesce(
      pg_catalog.jsonb_agg(
        pg_catalog.jsonb_build_object(
          'key', pg_catalog.jsonb_build_object(
            'kind', p_kind,
            'id', hydrated.id::text,
            'version', hydrated.version
          ),
          'accessLevel', hydrated.card -> 'accessLevel',
          'capabilities', hydrated.card -> 'capabilities',
          'names', hydrated.card -> 'names',
          'summary', hydrated.card -> 'summary',
          'geography', hydrated.card -> 'geography',
          'referenceYear', hydrated.card -> 'referenceYear',
          'modifiedAt', pg_catalog.to_char(
            hydrated.modified_at at time zone 'UTC',
            'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'
          ),
          'match', pg_catalog.jsonb_build_object(
            'kind', 'lexical',
            'score', hydrated.score,
            'reasonCodes', hydrated.reason_codes
          )
        )
        order by hydrated.page_rank
      ) filter (where hydrated.page_rank <= p_limit),
      '[]'::jsonb
    ),
    case
      when pg_catalog.max(hydrated.page_rank) > p_limit then
        (
          pg_catalog.jsonb_agg(
            pg_catalog.jsonb_build_object(
              'v', 1,
              'fp', p_query_fingerprint,
              'rankKey', hydrated.score::text,
              'kind', p_kind,
              'id', hydrated.id::text,
              'version', hydrated.version
            )
            order by hydrated.page_rank
          ) filter (where hydrated.page_rank = p_limit)
        ) -> 0
      else null
    end
  into v_items, v_next_cursor_payload
  from hydrated;

  return pg_catalog.jsonb_build_object(
    'items', v_items,
    'nextCursorPayload', v_next_cursor_payload
  );
end
$function$;

revoke all on function
  private.assert_portal_catalog_character_contract_v1()
from public, anon, authenticated, service_role;
grant execute on function
  private.assert_portal_catalog_character_contract_v1()
to api_internal_executor;
revoke all on function
  private.catalog_portal_single_character_search_v1_impl(
    text,text,text,uuid,text,integer,text
  )
from public, anon, authenticated, service_role, api_internal_executor;

create or replace function private.portal_search_v1(
  p_kind text,
  p_query text,
  p_filters jsonb,
  p_sort text,
  p_cursor text,
  p_limit integer
)
returns jsonb
language plpgsql
stable
parallel restricted
set search_path = ''
as $function$
declare
  v_query text;
  v_filters jsonb;
  v_sort text;
  v_limit integer := coalesce(p_limit, 20);
  v_fingerprint text;
  v_cursor jsonb;
  v_cursor_rank text;
  v_cursor_id uuid;
  v_cursor_version text;
  v_kernel jsonb;
  v_next_cursor_payload jsonb;
begin
  perform private.assert_portal_catalog_projection_contract_v1();

  perform private.portal_validate_search_v1(
    p_kind,
    coalesce(p_query, ''),
    coalesce(p_filters, '{}'::jsonb),
    coalesce(p_sort, 'relevance'),
    v_limit
  );
  v_query := pg_catalog.lower(pg_catalog.btrim(coalesce(p_query, '')));
  v_filters := private.portal_normalize_filters_v1(p_filters);
  v_sort := pg_catalog.lower(pg_catalog.btrim(coalesce(p_sort, 'relevance')));
  v_fingerprint := private.portal_query_fingerprint_v1(
    p_kind,
    v_query,
    v_filters,
    v_sort
  );
  if p_cursor is not null then
    v_cursor := private.portal_cursor_decode_v1(p_cursor);
    if v_cursor is null
       or (select count(*) from pg_catalog.jsonb_object_keys(v_cursor)) <> 6
       or v_cursor ->> 'v' <> '1'
       or v_cursor ->> 'fp' <> v_fingerprint
       or v_cursor ->> 'kind' <> p_kind
       or coalesce(v_cursor ->> 'rankKey', '') = ''
       or coalesce(v_cursor ->> 'id', '')
         !~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
       or coalesce(v_cursor ->> 'version', '') !~ '^\d{2}\.\d{2}\.\d{3}$' then
      raise exception using errcode = '22023', message = 'invalid portal request';
    end if;
    v_cursor_rank := v_cursor ->> 'rankKey';
    v_cursor_id := (v_cursor ->> 'id')::uuid;
    v_cursor_version := v_cursor ->> 'version';
    if v_sort = 'relevance'
       and v_cursor_rank !~ '^(0(\.\d+)?|1(\.0+)?)$' then
      raise exception using errcode = '22023', message = 'invalid portal request';
    elsif v_sort = 'modified_desc'
       and private.portal_datetime_v1(v_cursor_rank) is null then
      raise exception using errcode = '22023', message = 'invalid portal request';
    end if;
  end if;

  if pg_catalog.char_length(v_query) = 1
     and v_filters = '{}'::jsonb
     and v_sort = 'relevance' then
    v_kernel := private.catalog_portal_single_character_search_v1_impl(
      p_kind,
      v_query,
      v_cursor_rank,
      v_cursor_id,
      v_cursor_version,
      v_limit,
      v_fingerprint
    );
  else
    v_kernel := private.catalog_portal_search_v1_impl(
      p_kind,
      v_query,
      v_filters,
      v_sort,
      v_cursor_rank,
      v_cursor_id,
      v_cursor_version,
      v_limit,
      v_fingerprint
    );
  end if;

  v_next_cursor_payload := nullif(
    v_kernel -> 'nextCursorPayload',
    'null'::jsonb
  );

  return pg_catalog.jsonb_build_object(
    'schemaVersion', 'portal.public-search-page.v1',
    'kind', p_kind,
    'queryFingerprint', v_fingerprint,
    'items', coalesce(v_kernel -> 'items', '[]'::jsonb),
    'nextCursor', case when v_next_cursor_payload is null then null
      else private.portal_cursor_encode_v1(v_next_cursor_payload)
    end
  );
end
$function$;

comment on function
  private.catalog_portal_single_character_search_v1_impl(
    text,text,text,uuid,text,integer,text
  ) is
  'Bounded one-code-point unfiltered relevance Search over the synchronized narrow character projection.';
comment on function private.portal_search_v1(
  text,text,jsonb,text,text,integer
) is
  'Validates and normalizes public Search, routing only one-code-point unfiltered relevance to the narrow character pre-limit kernel.';

reset role;
revoke create on schema private from portal_public_executor;
revoke portal_public_executor from postgres;

grant portal_public_executor to postgres;
set role portal_public_executor;
select private.assert_portal_catalog_character_contract_v1();
reset role;
revoke portal_public_executor from postgres;

do $verify_portal_character_cutover$
begin
  if (
       select count(*)
       from private.portal_catalog_search_rows_v1
     ) <> (
       select count(*)
       from private.portal_catalog_character_rows_v1
     )
     or (
       select routine.prosrc !~
           'catalog_portal_single_character_search_v1_impl'
         or routine.prosrc !~ 'char_length\(v_query\) = 1'
         or routine.prosrc !~ 'catalog_portal_search_v1_impl'
       from pg_catalog.pg_proc as routine
       where routine.oid =
         'private.portal_search_v1(text,text,jsonb,text,text,integer)'::regprocedure
     ) is not false
     or (
       select routine.proowner <> 'portal_public_executor'::regrole
         or not routine.prosecdef
         or not (coalesce(routine.proconfig, '{}'::text[]) @> array[
           'search_path=""',
           'statement_timeout=8s',
           'work_mem=32MB',
           'plan_cache_mode=force_custom_plan',
           'jit=off',
           'row_security=on'
         ]::text[])
       from pg_catalog.pg_proc as routine
       where routine.oid =
         'private.catalog_portal_single_character_search_v1_impl(text,text,text,uuid,text,integer,text)'::regprocedure
     ) is not false then
    raise exception 'Portal character Search cutover drifted'
      using errcode = '55000';
  end if;
end
$verify_portal_character_cutover$;

commit;
