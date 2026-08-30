-- Issue #563: install a dormant, output-equivalent Process keyword ranking
-- kernel. The public wrapper remains unchanged until the exact expression GIN
-- index is built and verified by the later cutover migration.

begin;

set local lock_timeout = '5s';
set local statement_timeout = '120s';

do $portal_process_keyword_rank_prerequisite_guard$
declare
  v_expected jsonb := pg_catalog.jsonb_build_object(
    'private.catalog_portal_card_facts_v1(jsonb,jsonb,text)',
      '935aacc6742c7f877f8353b4fa186c9b4ffe009b361f9fa0f436ab9526ad72c4',
    'private.catalog_portal_process_pattern_versions_v1(text)',
      '200c6a346606b9152d7a2714a4c46ce5b64a6335bf898f3d8fa1003ee8aa41a6',
    'private.catalog_portal_search_v1_impl(text,text,jsonb,text,text,uuid,text,integer,text)',
      'a5aae6144bdd0e6d07ec04956de59e9ca6431851f979fd5f4dc38f8d1f5abbb8',
    'private.portal_search_v1(text,text,jsonb,text,text,integer)',
      'c06af7df366bbe544891619d19ef074d106ed6e68f27e581b7ed1e4c303f0594'
  );
  v_identity text;
  v_expected_sha text;
  v_actual_sha text;
begin
  if not exists (
    select 1
    from pg_catalog.pg_roles
    where rolname = 'portal_public_executor'
      and not rolcanlogin
      and not rolinherit
      and not rolbypassrls
      and not rolsuper
      and not rolreplication
  ) or pg_catalog.to_regclass(
    'private.portal_catalog_search_rows_v1'
  ) is null then
    raise exception 'Portal Process keyword rank prerequisite is unsafe'
      using errcode = '42501';
  end if;

  for v_identity, v_expected_sha in
    select entry.key, entry.value
    from pg_catalog.jsonb_each_text(v_expected) as entry
  loop
    if pg_catalog.to_regprocedure(v_identity) is null then
      raise exception 'Portal Process keyword rank function prerequisite is missing'
        using errcode = '55000';
    end if;
    select pg_catalog.encode(
      extensions.digest(
        pg_catalog.convert_to(
          pg_catalog.pg_get_functiondef(
            pg_catalog.to_regprocedure(v_identity)
          ),
          'UTF8'
        ),
        'sha256'
      ),
      'hex'
    ) into v_actual_sha;
    if v_actual_sha is distinct from v_expected_sha then
      raise exception 'Portal Process keyword rank function prerequisite drifted'
        using errcode = '55000';
    end if;
  end loop;

  if pg_catalog.to_regclass(
       'private.portal_catalog_search_process_exact_rank_v1_gin'
     ) is not null
     or pg_catalog.to_regprocedure(
       'private.portal_process_rank_name_keys_v1(jsonb)'
     ) is not null
     or pg_catalog.to_regprocedure(
       'private.catalog_portal_process_keyword_relevance_v1_impl(text,text,uuid,text,integer,text)'
     ) is not null then
    raise exception 'Portal Process keyword rank target already exists'
      using errcode = '55000';
  end if;
end
$portal_process_keyword_rank_prerequisite_guard$;

grant portal_public_executor to postgres;
grant create on schema private to portal_public_executor;
set role portal_public_executor;

create function private.portal_process_rank_name_keys_v1(p_card jsonb)
returns text[]
language sql
immutable
parallel safe
set search_path = ''
as $function$
  select coalesce(
    pg_catalog.array_agg(
      distinct normalized.value order by normalized.value
    ),
    '{}'::text[]
  )
  from pg_catalog.jsonb_array_elements(
    case when pg_catalog.jsonb_typeof(p_card -> 'names') = 'array'
      then p_card -> 'names' else '[]'::jsonb end
  ) as item(value)
  cross join lateral (
    select pg_catalog.lower(
      pg_catalog.btrim(item.value ->> 'value')
    ) as value
  ) as normalized
  where nullif(normalized.value, '') is not null
$function$;

create function private.portal_process_rank_classification_keys_v1(
  p_card jsonb
)
returns text[]
language sql
immutable
parallel safe
set search_path = ''
as $function$
  select coalesce(
    pg_catalog.array_agg(
      distinct normalized.value order by normalized.value
    ),
    '{}'::text[]
  )
  from pg_catalog.jsonb_array_elements(
    case when pg_catalog.jsonb_typeof(p_card -> 'classifications') = 'array'
      then p_card -> 'classifications' else '[]'::jsonb end
  ) as item(value)
  cross join lateral (
    select pg_catalog.lower(
      pg_catalog.btrim(item.value ->> 'code')
    ) as value
  ) as normalized
  where nullif(normalized.value, '') is not null
$function$;

create function private.catalog_portal_process_keyword_keys_v1(
  p_query text,
  p_cursor_rank text,
  p_cursor_id uuid,
  p_cursor_version text,
  p_limit integer
)
returns table(id uuid, version text, score numeric)
language plpgsql
stable
parallel restricted
security definer
set search_path = ''
set statement_timeout = '8s'
set plan_cache_mode = 'force_custom_plan'
set row_security = 'on'
as $function$
declare
  v_like_pattern text;
begin
  v_like_pattern := '%' || pg_catalog.replace(
    pg_catalog.replace(
      pg_catalog.replace(
        p_query,
        pg_catalog.chr(92),
        pg_catalog.chr(92) || pg_catalog.chr(92)
      ),
      '%',
      pg_catalog.chr(92) || '%'
    ),
    '_',
    pg_catalog.chr(92) || '_'
  ) || '%';

  return query
  with matched_versions as materialized (
    select matched.id, matched.version
    from private.catalog_portal_process_pattern_versions_v1(
      v_like_pattern
    ) as matched
  ), candidate_ids as materialized (
    select distinct matched.id
    from matched_versions as matched
  ), latest_keys as materialized (
    select distinct on (projection.id)
      projection.id,
      projection.version
    from private.portal_catalog_search_rows_v1 as projection
    join candidate_ids using (id)
    where projection.dataset_kind = 'process'
    order by projection.id,
      projection.version desc,
      projection.modified_at desc,
      projection.state_code desc
  ), eligible_keys as materialized (
    select latest.id, latest.version
    from latest_keys as latest
    join matched_versions as matched
      on matched.id = latest.id
     and matched.version = latest.version
  ), exact_source as materialized (
    select projection.id,
      projection.version,
      case
        when private.portal_process_rank_name_keys_v1(projection.card)
          @> array[p_query] then 0.95::numeric
        else 0.92::numeric
      end as score
    from private.portal_catalog_search_rows_v1 as projection
    where projection.dataset_kind = 'process'
      and (
        private.portal_process_rank_name_keys_v1(projection.card)
          @> array[p_query]
        or private.portal_process_rank_classification_keys_v1(
          projection.card
        ) @> array[p_query]
      )
  ), exact_keys as materialized (
    select exact_source.*
    from exact_source
    join eligible_keys using (id, version)
    where p_cursor_rank is null
      or exact_source.score < p_cursor_rank::numeric
      or (
        exact_source.score = p_cursor_rank::numeric
        and (
          exact_source.id > p_cursor_id
          or (
            exact_source.id = p_cursor_id
            and exact_source.version < p_cursor_version
          )
        )
      )
  ), general_keys as materialized (
    select eligible.id, eligible.version, 0.70::numeric as score
    from eligible_keys as eligible
    left join exact_source using (id, version)
    where exact_source.id is null
      and (
        p_cursor_rank is null
        or 0.70::numeric < p_cursor_rank::numeric
        or (
          0.70::numeric = p_cursor_rank::numeric
          and (
            eligible.id > p_cursor_id
            or (
              eligible.id = p_cursor_id
              and eligible.version < p_cursor_version
            )
          )
        )
      )
    order by eligible.id, eligible.version desc
    limit p_limit + 1
  ), combined as (
    select exact_keys.* from exact_keys
    union all
    select general_keys.* from general_keys
  )
  select combined.id, combined.version, combined.score
  from combined
  order by combined.score desc, combined.id, combined.version desc
  limit p_limit + 1;
end
$function$;

create function private.catalog_portal_process_keyword_relevance_v1_impl(
  p_query text,
  p_cursor_rank text,
  p_cursor_id uuid,
  p_cursor_version text,
  p_limit integer,
  p_query_fingerprint text
)
returns jsonb
language sql
stable
parallel restricted
security definer
set search_path = ''
set statement_timeout = '8s'
set plan_cache_mode = 'force_custom_plan'
set row_security = 'on'
as $function$
  with selected_keys as materialized (
    select selected.id, selected.version, selected.score,
      pg_catalog.row_number() over (
        order by selected.score desc, selected.id, selected.version desc
      ) as page_rank
    from private.catalog_portal_process_keyword_keys_v1(
      p_query,
      p_cursor_rank,
      p_cursor_id,
      p_cursor_version,
      p_limit
    ) as selected
  ), hydrated as materialized (
    select selected.page_rank,
      projection.id,
      projection.version,
      projection.modified_at,
      projection.card,
      selected.score,
      private.catalog_portal_card_facts_v1(
        projection.card,
        '{}'::jsonb,
        p_query
      ) as facts
    from selected_keys as selected
    join private.portal_catalog_search_rows_v1 as projection
      on projection.dataset_kind = 'process'
     and projection.id = selected.id
     and projection.version = selected.version
  ), result as (
    select coalesce(
      pg_catalog.jsonb_agg(
        pg_catalog.jsonb_build_object(
          'key', pg_catalog.jsonb_build_object(
            'kind', 'process',
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
            'kind', case
              when (hydrated.facts ->> 'nameExact')::boolean
                or (hydrated.facts ->> 'nameContains')::boolean
                then 'lexical'
              when (hydrated.facts ->> 'classificationExact')::boolean
                or (hydrated.facts ->> 'classificationContains')::boolean
                then 'identifier'
              else 'lexical'
            end,
            'score', hydrated.score,
            'reasonCodes', case
              when (hydrated.facts ->> 'nameExact')::boolean
                or (hydrated.facts ->> 'nameContains')::boolean
                then pg_catalog.jsonb_build_array('name')
              when (hydrated.facts ->> 'classificationExact')::boolean
                or (hydrated.facts ->> 'classificationContains')::boolean
                then pg_catalog.jsonb_build_array('classification')
              else pg_catalog.jsonb_build_array('full_text')
            end
          )
        ) order by hydrated.page_rank
      ) filter (where hydrated.page_rank <= p_limit),
      '[]'::jsonb
    ) as items,
    case when pg_catalog.max(hydrated.page_rank) > p_limit then
      (
        pg_catalog.jsonb_agg(
          pg_catalog.jsonb_build_object(
            'v', 1,
            'fp', p_query_fingerprint,
            'rankKey', hydrated.score::text,
            'kind', 'process',
            'id', hydrated.id::text,
            'version', hydrated.version
          ) order by hydrated.page_rank
        ) filter (where hydrated.page_rank = p_limit)
      ) -> 0
    else null end as next_cursor_payload
    from hydrated
  )
  select pg_catalog.jsonb_build_object(
    'items', result.items,
    'nextCursorPayload', result.next_cursor_payload
  )
  from result
$function$;

create function private.portal_process_keyword_rank_manifest_sha256_v1()
returns text
language sql
stable
parallel restricted
security definer
set search_path = ''
set row_security = 'on'
as $function$
  with expected(identity) as (
    values
      ('private.portal_process_rank_name_keys_v1(jsonb)'::text),
      ('private.portal_process_rank_classification_keys_v1(jsonb)'),
      ('private.catalog_portal_process_keyword_keys_v1(text,text,uuid,text,integer)'),
      ('private.catalog_portal_process_keyword_relevance_v1_impl(text,text,uuid,text,integer,text)')
  ), manifest_entries as (
    select expected.identity,
      pg_catalog.jsonb_build_object(
        'identity', expected.identity,
        'definition', pg_catalog.pg_get_functiondef(routine.oid),
        'owner', pg_catalog.pg_get_userbyid(routine.proowner),
        'language', language.lanname,
        'volatility', routine.provolatile,
        'parallel', routine.proparallel,
        'securityDefiner', routine.prosecdef,
        'config', coalesce(
          pg_catalog.to_jsonb(routine.proconfig),
          'null'::jsonb
        )
      )::text as entry
    from expected
    join pg_catalog.pg_proc as routine
      on routine.oid = pg_catalog.to_regprocedure(expected.identity)
    join pg_catalog.pg_language as language
      on language.oid = routine.prolang
  )
  select pg_catalog.encode(
    extensions.digest(
      pg_catalog.convert_to(
        pg_catalog.string_agg(
          manifest_entries.entry,
          E'\n'
          order by manifest_entries.identity
        ),
        'UTF8'
      ),
      'sha256'
    ),
    'hex'
  )
  from manifest_entries
$function$;

create function private.assert_portal_process_keyword_rank_contract_v1()
returns void
language plpgsql
stable
parallel restricted
security definer
set search_path = ''
set row_security = 'on'
as $function$
declare
  v_expected_digest constant text :=
    '3dd65dc6b0dbd5ca8108d0a996610030bad1b5478d61ee9674a57580433e6bbf';
  v_expected_index constant text :=
    'CREATE INDEX portal_catalog_search_process_exact_rank_v1_gin ON private.portal_catalog_search_rows_v1 USING gin (private.portal_process_rank_name_keys_v1(card), private.portal_process_rank_classification_keys_v1(card)) WHERE (dataset_kind = ''process''::text)';
begin
  perform private.assert_portal_catalog_projection_contract_v1();
  if private.portal_process_keyword_rank_manifest_sha256_v1()
       is distinct from v_expected_digest
     or pg_catalog.to_regclass(
       'private.portal_catalog_search_process_exact_rank_v1_gin'
     ) is null
     or (
       select not index_catalog.indisvalid
         or not index_catalog.indisready
         or not index_catalog.indislive
         or index_catalog.indisunique
         or access_method.amname <> 'gin'
         or pg_catalog.pg_get_indexdef(index_relation.oid)
           <> v_expected_index
       from pg_catalog.pg_class as index_relation
       join pg_catalog.pg_index as index_catalog
         on index_catalog.indexrelid = index_relation.oid
       join pg_catalog.pg_am as access_method
         on access_method.oid = index_relation.relam
       where index_relation.oid =
         'private.portal_catalog_search_process_exact_rank_v1_gin'::regclass
     ) is not false then
    raise exception using
      errcode = '55000',
      message = 'Portal Process keyword rank contract drifted';
  end if;
end
$function$;

comment on function private.portal_process_rank_name_keys_v1(jsonb) is
  'Extracts bounded normalized exact Process name values from one frozen public card for the expression GIN rank probe.';
comment on function private.portal_process_rank_classification_keys_v1(jsonb) is
  'Extracts bounded normalized exact Process classification codes from one frozen public card for the expression GIN rank probe.';
comment on function private.catalog_portal_process_keyword_keys_v1(
  text,text,uuid,text,integer
) is
  'Selects exact-name/classification plus general Process keyword keys before reading wide cards, preserving the stable relevance cursor.';
comment on function private.catalog_portal_process_keyword_relevance_v1_impl(
  text,text,uuid,text,integer,text
) is
  'Hydrates only the bounded Process keyword relevance page after exact-rank and general-key selection.';
comment on function private.portal_process_keyword_rank_manifest_sha256_v1() is
  'Live SHA-256 for the exact Process keyword rank helper closure.';
comment on function private.assert_portal_process_keyword_rank_contract_v1() is
  'Fails closed before the Process keyword fast path when helper or exact-rank GIN contract drifts.';

revoke all on function private.portal_process_rank_name_keys_v1(jsonb)
from public, anon, authenticated, service_role, api_internal_executor;
revoke all on function private.portal_process_rank_classification_keys_v1(
  jsonb
) from public, anon, authenticated, service_role, api_internal_executor;
revoke all on function private.catalog_portal_process_keyword_keys_v1(
  text,text,uuid,text,integer
) from public, anon, authenticated, service_role, api_internal_executor;
revoke all on function private.catalog_portal_process_keyword_relevance_v1_impl(
  text,text,uuid,text,integer,text
) from public, anon, authenticated, service_role, api_internal_executor;
revoke all on function private.portal_process_keyword_rank_manifest_sha256_v1()
from public, anon, authenticated, service_role, api_internal_executor;
revoke all on function private.assert_portal_process_keyword_rank_contract_v1()
from public, anon, authenticated, service_role, api_internal_executor;

-- The projection writer must evaluate the immutable index expressions on
-- every Process card write. The next standalone migration additionally
-- builds the expression index as postgres, whose temporary grants are removed
-- by the cutover migration.
grant execute on function private.portal_process_rank_name_keys_v1(jsonb)
to api_internal_executor;
grant execute on function private.portal_process_rank_classification_keys_v1(
  jsonb
) to api_internal_executor;
grant execute on function private.portal_process_rank_name_keys_v1(jsonb)
to postgres;
grant execute on function private.portal_process_rank_classification_keys_v1(
  jsonb
) to postgres;

reset role;
revoke create on schema private from portal_public_executor;
revoke portal_public_executor from postgres;

do $verify_portal_process_keyword_rank_helpers$
begin
  if exists (
    select 1
    from pg_catalog.pg_proc as routine
    where routine.oid in (
      'private.portal_process_rank_name_keys_v1(jsonb)'::regprocedure,
      'private.portal_process_rank_classification_keys_v1(jsonb)'::regprocedure,
      'private.catalog_portal_process_keyword_keys_v1(text,text,uuid,text,integer)'::regprocedure,
      'private.catalog_portal_process_keyword_relevance_v1_impl(text,text,uuid,text,integer,text)'::regprocedure,
      'private.portal_process_keyword_rank_manifest_sha256_v1()'::regprocedure,
      'private.assert_portal_process_keyword_rank_contract_v1()'::regprocedure
    ) and routine.proowner <> 'portal_public_executor'::regrole
  ) then
    raise exception 'Portal Process keyword rank helper metadata drifted'
      using errcode = '55000';
  end if;
end
$verify_portal_process_keyword_rank_helpers$;

commit;
