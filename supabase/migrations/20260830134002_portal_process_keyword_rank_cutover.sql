-- Issue #563: route only multi-code-point, unfiltered Process relevance
-- searches through the exact-rank key selector installed by the preceding
-- migrations. Flow, UUID, empty-query, one-code-point, filtered, and alternate
-- sort shapes retain their existing kernels byte-for-byte.

begin;

set local lock_timeout = '5s';
set local statement_timeout = '120s';

do $portal_process_keyword_rank_cutover_guard$
declare
  v_search_sha text;
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
  ) into v_search_sha;

  if v_search_sha is distinct from
       'c06af7df366bbe544891619d19ef074d106ed6e68f27e581b7ed1e4c303f0594'
     or pg_catalog.to_regprocedure(
       'private.catalog_portal_process_keyword_relevance_v1_impl(text,text,uuid,text,integer,text)'
     ) is null
     or pg_catalog.to_regprocedure(
       'private.assert_portal_process_keyword_rank_contract_v1()'
     ) is null
     or pg_catalog.to_regclass(
       'private.portal_catalog_search_process_exact_rank_v1_gin'
     ) is null then
    raise exception 'Portal Process keyword rank cutover prerequisites drifted'
      using errcode = '55000';
  end if;
end
$portal_process_keyword_rank_cutover_guard$;

grant portal_public_executor to postgres;
grant create on schema private to portal_public_executor;
set role portal_public_executor;

select private.assert_portal_process_keyword_rank_contract_v1();

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
  elsif p_kind = 'process'
     and pg_catalog.char_length(v_query) > 1
     and v_query !~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
     and v_filters = '{}'::jsonb
     and v_sort = 'relevance' then
    perform private.assert_portal_process_keyword_rank_contract_v1();
    v_kernel := private.catalog_portal_process_keyword_relevance_v1_impl(
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

comment on function private.portal_search_v1(
  text,text,jsonb,text,text,integer
) is
  'Validates public Search; routes one-code-point searches to the character child and unfiltered Process keyword relevance to bounded key selection before card hydration.';

revoke execute on function private.portal_process_rank_name_keys_v1(jsonb)
from postgres;
revoke execute on function private.portal_process_rank_classification_keys_v1(
  jsonb
) from postgres;

reset role;
revoke create on schema private from portal_public_executor;
revoke portal_public_executor from postgres;

do $verify_portal_process_keyword_rank_cutover$
declare
  v_search_sha text;
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
  ) into v_search_sha;

  if v_search_sha is distinct from
       '603538f613f8d6bb64c480da2ede2bfa9eaf92d1565b144ad7441301ad1dbcb2'
     or (
       select routine.proowner <> 'portal_public_executor'::regrole
         or routine.prosecdef
         or routine.provolatile <> 's'
         or routine.proparallel <> 'r'
         or routine.proconfig is distinct from array['search_path=""']::text[]
         or routine.prosrc !~ 'catalog_portal_single_character_search_v1_impl'
         or routine.prosrc !~ 'catalog_portal_process_keyword_relevance_v1_impl'
         or routine.prosrc !~ 'catalog_portal_search_v1_impl'
       from pg_catalog.pg_proc as routine
       where routine.oid =
         'private.portal_search_v1(text,text,jsonb,text,text,integer)'::regprocedure
     ) is not false
     or has_function_privilege(
       'postgres',
       'private.portal_process_rank_name_keys_v1(jsonb)',
       'EXECUTE'
     )
     or has_function_privilege(
       'postgres',
       'private.portal_process_rank_classification_keys_v1(jsonb)',
       'EXECUTE'
     ) then
    raise exception 'Portal Process keyword rank cutover drifted'
      using errcode = '55000';
  end if;
end
$verify_portal_process_keyword_rank_cutover$;

commit;
