-- Database #628: additive composite-name projection. Frozen v1 remains intact.
begin;
set local lock_timeout = '5s';
set local statement_timeout = '120s';
set local check_function_bodies = off;
select extensions.vector_dims('[1]'::extensions.vector);
grant portal_public_executor, api_internal_executor to postgres;
grant create on schema private to portal_public_executor, api_internal_executor;
select private.assert_portal_catalog_projection_contract_v1();
select private.assert_portal_catalog_projection_contract_cn1();
select private.assert_portal_catalog_character_contract_cn1();
select private.assert_portal_process_keyword_rank_contract_cn1();
select private.assert_portal_catalog_facet_contract_v1();
lock table public.processes, public.flows in share row exclusive mode;
set local statement_timeout = '15s';
do $ready$ begin
  if (select count(*) from private.portal_names_backfill_v2) <> 16
     or exists (
       (select dataset_kind,id,version,state_code,modified_at from private.portal_catalog_search_rows_v1
        except select dataset_kind,id,version,state_code,modified_at from private.portal_catalog_search_rows_v2)
       union all
       (select dataset_kind,id,version,state_code,modified_at from private.portal_catalog_search_rows_v2
        except select dataset_kind,id,version,state_code,modified_at from private.portal_catalog_search_rows_v1)
     )
     or (select count(*) from private.portal_catalog_character_rows_v2) <>
        (select count(*) from private.portal_catalog_search_rows_v2)
     or (select count(*) from pg_catalog.pg_trigger where
         tgrelid in ('public.processes'::regclass,'public.flows'::regclass)
         and tgname='portal_catalog_projection_content_sync_v2' and tgenabled='O'
         and tgfoid='private.sync_portal_catalog_search_row_v2()'::regprocedure) <> 2 then
    raise exception 'Portal composite-name projection is not ready for cutover';
  end if;
end $ready$;
do $indexes$ begin
  if not exists(select 1 from pg_catalog.pg_index i where
    i.indexrelid=pg_catalog.to_regclass('private.portal_catalog_search_flow_cas_v2_idx') and i.indisvalid and i.indisready and i.indislive
    and pg_catalog.pg_get_indexdef(i.indexrelid)='CREATE INDEX portal_catalog_search_flow_cas_v2_idx ON private.portal_catalog_search_rows_v2 USING btree (((card ->> ''casNumber''::text)), id, version DESC, modified_at DESC, state_code DESC) WHERE ((dataset_kind = ''flow''::text) AND (jsonb_typeof((card -> ''casNumber''::text)) = ''string''::text) AND ((card ->> ''casNumber''::text) ~ ''^[0-9]{2,7}-[0-9]{2}-[0-9]$''::text) AND ((length((card ->> ''casNumber''::text)) >= 7) AND (length((card ->> ''casNumber''::text)) <= 12)))') then
    raise exception 'Portal composite-name index portal_catalog_search_flow_cas_v2_idx is invalid or drifted';
  end if;
  if not exists(select 1 from pg_catalog.pg_index i where
    i.indexrelid=pg_catalog.to_regclass('private.portal_catalog_search_flow_document_v2_pgroonga') and i.indisvalid and i.indisready and i.indislive
    and pg_catalog.pg_get_indexdef(i.indexrelid)='CREATE INDEX portal_catalog_search_flow_document_v2_pgroonga ON private.portal_catalog_search_rows_v2 USING pgroonga (document) WITH (tokenizer=''TokenBigram'', normalizer=''NormalizerAuto'') WHERE (dataset_kind = ''flow''::text)') then
    raise exception 'Portal composite-name index portal_catalog_search_flow_document_v2_pgroonga is invalid or drifted';
  end if;
  if not exists(select 1 from pg_catalog.pg_index i where
    i.indexrelid=pg_catalog.to_regclass('private.portal_catalog_search_process_document_v2_pgroonga') and i.indisvalid and i.indisready and i.indislive
    and pg_catalog.pg_get_indexdef(i.indexrelid)='CREATE INDEX portal_catalog_search_process_document_v2_pgroonga ON private.portal_catalog_search_rows_v2 USING pgroonga (document) WITH (tokenizer=''TokenBigram'', normalizer=''NormalizerAuto'') WHERE (dataset_kind = ''process''::text)') then
    raise exception 'Portal composite-name index portal_catalog_search_process_document_v2_pgroonga is invalid or drifted';
  end if;
  if not exists(select 1 from pg_catalog.pg_index i where
    i.indexrelid=pg_catalog.to_regclass('private.portal_catalog_search_process_exact_rank_v2_gin') and i.indisvalid and i.indisready and i.indislive
    and pg_catalog.pg_get_indexdef(i.indexrelid)='CREATE INDEX portal_catalog_search_process_exact_rank_v2_gin ON private.portal_catalog_search_rows_v2 USING gin (private.portal_process_rank_name_keys_v1(card), private.portal_process_rank_classification_keys_v1(card)) WHERE (dataset_kind = ''process''::text)') then
    raise exception 'Portal composite-name index portal_catalog_search_process_exact_rank_v2_gin is invalid or drifted';
  end if;
  if not exists(select 1 from pg_catalog.pg_index i where
    i.indexrelid=pg_catalog.to_regclass('private.portal_catalog_search_rows_latest_v2_idx') and i.indisvalid and i.indisready and i.indislive
    and pg_catalog.pg_get_indexdef(i.indexrelid)='CREATE INDEX portal_catalog_search_rows_latest_v2_idx ON private.portal_catalog_search_rows_v2 USING btree (dataset_kind, id, version DESC, modified_at DESC, state_code DESC)') then
    raise exception 'Portal composite-name index portal_catalog_search_rows_latest_v2_idx is invalid or drifted';
  end if;
  if not exists(select 1 from pg_catalog.pg_index i where
    i.indexrelid=pg_catalog.to_regclass('private.portal_catalog_summary_eligibility_v2_idx') and i.indisvalid and i.indisready and i.indislive
    and pg_catalog.pg_get_indexdef(i.indexrelid)='CREATE INDEX portal_catalog_summary_eligibility_v2_idx ON private.portal_catalog_search_rows_v2 USING btree (dataset_kind, id, version DESC, modified_at DESC, state_code DESC) WHERE (((dataset_kind = ''flow''::text) AND (jsonb_typeof((card -> ''casNumber''::text)) = ''string''::text) AND ((card ->> ''casNumber''::text) ~ ''^[0-9]{2,7}-[0-9]{2}-[0-9]$''::text)) OR ((jsonb_typeof((card -> ''classifications''::text)) = ''array''::text) AND (jsonb_array_length((card -> ''classifications''::text)) > 0)))') then
    raise exception 'Portal composite-name index portal_catalog_summary_eligibility_v2_idx is invalid or drifted';
  end if;
end $indexes$;
CREATE OR REPLACE FUNCTION api.portal_catalog_summary_v1()
 RETURNS jsonb
 LANGUAGE plpgsql
 STABLE PARALLEL RESTRICTED SECURITY DEFINER
 SET search_path TO ''
 SET statement_timeout TO '2s'
 SET work_mem TO '32MB'
 SET plan_cache_mode TO 'force_custom_plan'
 SET max_parallel_workers_per_gather TO '0'
 SET jit TO 'off'
 SET row_security TO 'on'
AS $function$
declare
  v_counts jsonb;
  v_latest_modified_at text;
  v_uuid_example jsonb;
  v_cas_example jsonb;
  v_classification_example jsonb;
  v_result jsonb;
begin
  perform private.assert_portal_catalog_projection_contract_cn1();
  perform private.assert_portal_catalog_facet_contract_v1();

  with latest as materialized (
    select distinct on (facet.dataset_kind, facet.id)
      facet.dataset_kind,
      facet.id,
      facet.version,
      facet.modified_at,
      facet.state_code
    from private.portal_catalog_facet_rows_v1 as facet
    where facet.facet_contract_version = 1
    order by facet.dataset_kind,
      facet.id,
      facet.version desc,
      facet.modified_at desc,
      facet.state_code desc
  ), counts as (
    select pg_catalog.jsonb_build_object(
        'process', pg_catalog.count(*) filter (
          where latest.dataset_kind = 'process'
        ),
        'flow', pg_catalog.count(*) filter (
          where latest.dataset_kind = 'flow'
        ),
        'total', pg_catalog.count(*)
      ) as value,
      private.portal_timestamp_v1(
        pg_catalog.max(latest.modified_at)
      ) as latest_modified_at
    from latest
  ), uuid_candidates as materialized (
    (
      select 0 as preference,
        candidate.dataset_kind,
        candidate.id,
        candidate.version,
        private.portal_catalog_summary_label_v1(candidate.card) as label
      from private.portal_catalog_search_rows_v2 as candidate
      join latest
        on latest.dataset_kind = candidate.dataset_kind
       and latest.id = candidate.id
       and latest.version = candidate.version
      where candidate.dataset_kind = 'process'
        and pg_catalog.jsonb_array_length(
          private.portal_catalog_summary_label_v1(candidate.card)
        ) > 0
      order by candidate.id,
        candidate.version desc,
        candidate.modified_at desc,
        candidate.state_code desc
      limit 1
    )
    union all
    (
      select 1 as preference,
        candidate.dataset_kind,
        candidate.id,
        candidate.version,
        private.portal_catalog_summary_label_v1(candidate.card) as label
      from private.portal_catalog_search_rows_v2 as candidate
      join latest
        on latest.dataset_kind = candidate.dataset_kind
       and latest.id = candidate.id
       and latest.version = candidate.version
      where candidate.dataset_kind = 'flow'
        and pg_catalog.jsonb_array_length(
          private.portal_catalog_summary_label_v1(candidate.card)
        ) > 0
      order by candidate.id,
        candidate.version desc,
        candidate.modified_at desc,
        candidate.state_code desc
      limit 1
    )
  ), uuid_example as (
    select pg_catalog.jsonb_build_object(
      'queryKind', 'uuid',
      'datasetKind', candidate.dataset_kind,
      'query', candidate.id::text,
      'label', candidate.label
    ) as value
    from uuid_candidates as candidate
    order by candidate.preference
    limit 1
  ), cas_unique_values as materialized (
    select candidate.card ->> 'casNumber' as cas_number,
      pg_catalog.min(candidate.id::text)::uuid as id
    from private.portal_catalog_search_rows_v2 as candidate
    where candidate.dataset_kind = 'flow'
      and pg_catalog.jsonb_typeof(candidate.card -> 'casNumber') = 'string'
      and candidate.card ->> 'casNumber' ~
        '^[0-9]{2,7}-[0-9]{2}-[0-9]$'
      and pg_catalog.length(
        candidate.card ->> 'casNumber'
      ) between 7 and 12
      and private.portal_catalog_summary_valid_cas_v1(
        candidate.card ->> 'casNumber'
      )
    group by candidate.card ->> 'casNumber'
    having pg_catalog.count(*) = 1
    order by candidate.card ->> 'casNumber'
    limit 64
  ), cas_candidates as materialized (
    select candidate.dataset_kind,
      candidate.id,
      candidate.version,
      candidate.modified_at,
      candidate.state_code,
      unique_cas.cas_number,
      private.portal_catalog_summary_label_v1(candidate.card) as label
    from cas_unique_values as unique_cas
    join private.portal_catalog_search_rows_v2 as candidate
      on candidate.dataset_kind = 'flow'
     and candidate.id = unique_cas.id
     and candidate.card ->> 'casNumber' = unique_cas.cas_number
    join latest
      on latest.dataset_kind = candidate.dataset_kind
     and latest.id = candidate.id
     and latest.version = candidate.version
    where pg_catalog.jsonb_array_length(
      private.portal_catalog_summary_label_v1(candidate.card)
    ) > 0
    order by unique_cas.cas_number,
      candidate.id,
      candidate.version desc,
      candidate.modified_at desc,
      candidate.state_code desc
    limit 1
  ), cas_example as (
    select pg_catalog.jsonb_build_object(
      'queryKind', 'cas',
      'datasetKind', 'flow',
      'query', candidate.cas_number,
      'label', candidate.label
    ) as value
    from cas_candidates as candidate
    order by candidate.id,
      candidate.version desc,
      candidate.modified_at desc,
      candidate.state_code desc
    limit 1
  ), classification_candidates as materialized (
    (
      select 0 as preference,
        candidate.dataset_kind,
        candidate.id,
        candidate.version,
        candidate.modified_at,
        candidate.state_code,
        classification.ordinality,
        pg_catalog.btrim(classification.value ->> 'code') as code,
        private.portal_catalog_summary_label_v1(candidate.card) as label
      from private.portal_catalog_search_rows_v2 as candidate
      join latest
        on latest.dataset_kind = candidate.dataset_kind
       and latest.id = candidate.id
       and latest.version = candidate.version
      cross join lateral pg_catalog.jsonb_array_elements(
        candidate.card -> 'classifications'
      ) with ordinality as classification(value, ordinality)
      where candidate.dataset_kind = 'process'
        and pg_catalog.jsonb_typeof(
          candidate.card -> 'classifications'
        ) = 'array'
        and pg_catalog.jsonb_array_length(
          candidate.card -> 'classifications'
        ) > 0
        and pg_catalog.jsonb_typeof(classification.value) = 'object'
        and pg_catalog.jsonb_typeof(classification.value -> 'code') = 'string'
        and pg_catalog.length(
          pg_catalog.btrim(classification.value ->> 'code')
        ) between 4 and 128
        and pg_catalog.octet_length(
          pg_catalog.btrim(classification.value ->> 'code')
        ) <= 512
        and pg_catalog.btrim(
          classification.value ->> 'code'
        ) !~ '[[:cntrl:]]'
        and pg_catalog.jsonb_array_length(
          private.portal_catalog_summary_label_v1(candidate.card)
        ) > 0
      order by candidate.id,
        candidate.version desc,
        candidate.modified_at desc,
        candidate.state_code desc,
        classification.ordinality,
        pg_catalog.btrim(
          classification.value ->> 'code'
        ) collate pg_catalog."C"
      limit 1
    )
    union all
    (
      select 1 as preference,
        candidate.dataset_kind,
        candidate.id,
        candidate.version,
        candidate.modified_at,
        candidate.state_code,
        classification.ordinality,
        pg_catalog.btrim(classification.value ->> 'code') as code,
        private.portal_catalog_summary_label_v1(candidate.card) as label
      from private.portal_catalog_search_rows_v2 as candidate
      join latest
        on latest.dataset_kind = candidate.dataset_kind
       and latest.id = candidate.id
       and latest.version = candidate.version
      cross join lateral pg_catalog.jsonb_array_elements(
        candidate.card -> 'classifications'
      ) with ordinality as classification(value, ordinality)
      where candidate.dataset_kind = 'flow'
        and pg_catalog.jsonb_typeof(
          candidate.card -> 'classifications'
        ) = 'array'
        and pg_catalog.jsonb_array_length(
          candidate.card -> 'classifications'
        ) > 0
        and pg_catalog.jsonb_typeof(classification.value) = 'object'
        and pg_catalog.jsonb_typeof(classification.value -> 'code') = 'string'
        and pg_catalog.length(
          pg_catalog.btrim(classification.value ->> 'code')
        ) between 4 and 128
        and pg_catalog.octet_length(
          pg_catalog.btrim(classification.value ->> 'code')
        ) <= 512
        and pg_catalog.btrim(
          classification.value ->> 'code'
        ) !~ '[[:cntrl:]]'
        and pg_catalog.jsonb_array_length(
          private.portal_catalog_summary_label_v1(candidate.card)
        ) > 0
      order by candidate.id,
        candidate.version desc,
        candidate.modified_at desc,
        candidate.state_code desc,
        classification.ordinality,
        pg_catalog.btrim(
          classification.value ->> 'code'
        ) collate pg_catalog."C"
      limit 1
    )
  ), classification_example as (
    select pg_catalog.jsonb_build_object(
      'queryKind', 'classification',
      'datasetKind', candidate.dataset_kind,
      'query', candidate.code,
      'label', candidate.label
    ) as value
    from classification_candidates as candidate
    order by candidate.preference
    limit 1
  )
  select counts.value,
    counts.latest_modified_at,
    uuid_example.value,
    cas_example.value,
    classification_example.value
  into v_counts,
    v_latest_modified_at,
    v_uuid_example,
    v_cas_example,
    v_classification_example
  from counts
  left join uuid_example on true
  left join cas_example on true
  left join classification_example on true;

  select pg_catalog.jsonb_build_object(
    'schemaVersion', 'portal.public-catalog-summary.v1',
    'counts', v_counts,
    'latestModifiedAt', v_latest_modified_at,
    'examples', coalesce(pg_catalog.jsonb_agg(
      example.value order by example.ordinality
    ) filter (where example.value is not null), '[]'::jsonb)
  )
  into v_result
  from (values
    (1, v_uuid_example),
    (2, v_cas_example),
    (3, v_classification_example)
  ) as example(ordinality, value);

  if pg_catalog.octet_length(v_result::text) > 16384 then
    raise exception using
      errcode = '54000',
      message = 'Portal catalog summary exceeded its response budget';
  end if;

  return v_result;
exception
  when query_canceled then
    raise exception using errcode = 'P0001', message = 'portal catalog unavailable';
  when others then
    raise exception using errcode = 'P0001', message = 'portal catalog unavailable';
end
$function$
;
alter function api.portal_catalog_summary_v1() owner to portal_public_executor;

CREATE OR REPLACE FUNCTION api.portal_get_dataset_v1(p_kind text, p_id uuid, p_version text)
 RETURNS jsonb
 LANGUAGE plpgsql
 STABLE SECURITY DEFINER
 SET search_path TO ''
 SET statement_timeout TO '8s'
AS $function$
begin
  if p_kind not in ('process', 'flow')
     or p_id is null
     or p_version is null
     or p_version !~ '^\d{2}\.\d{2}\.\d{3}$' then
    raise exception using errcode = '22023', message = 'invalid portal request';
  end if;
  return private.portal_lcia_decorate_dataset_v1(
    private.portal_dataset_projection_cn1(p_kind, p_id, p_version)
  );
exception
  when sqlstate '22023' then
    raise exception using errcode = '22023', message = 'invalid portal request';
  when query_canceled then
    raise exception using errcode = 'P0001', message = 'portal catalog unavailable';
  when others then
    raise exception using errcode = 'P0001', message = 'portal catalog unavailable';
end
$function$
;
alter function api.portal_get_dataset_v1(text,uuid,text) owner to portal_public_executor;

CREATE OR REPLACE FUNCTION api.portal_search_processes_v1(p_query text, p_filters jsonb DEFAULT '{}'::jsonb, p_sort text DEFAULT 'relevance'::text, p_cursor text DEFAULT NULL::text, p_limit integer DEFAULT 20)
 RETURNS jsonb
 LANGUAGE plpgsql
 STABLE SECURITY DEFINER
 SET search_path TO ''
 SET statement_timeout TO '8s'
AS $function$
begin
  return private.portal_decorate_card_context_v1(
    private.portal_lcia_decorate_item_page_v1(
      private.portal_search_cn1(
        'process', p_query, p_filters, p_sort, p_cursor, p_limit
      )
    )
  );
exception
  when sqlstate '22023' then
    raise exception using errcode = '22023', message = 'invalid portal request';
  when query_canceled then
    raise exception using errcode = 'P0001', message = 'portal catalog unavailable';
  when others then
    raise exception using errcode = 'P0001', message = 'portal catalog unavailable';
end
$function$
;
alter function api.portal_search_processes_v1(text,jsonb,text,text,integer) owner to portal_public_executor;

CREATE OR REPLACE FUNCTION api.portal_search_flows_v1(p_query text, p_filters jsonb DEFAULT '{}'::jsonb, p_sort text DEFAULT 'relevance'::text, p_cursor text DEFAULT NULL::text, p_limit integer DEFAULT 20)
 RETURNS jsonb
 LANGUAGE plpgsql
 STABLE SECURITY DEFINER
 SET search_path TO ''
 SET statement_timeout TO '8s'
AS $function$
begin
  return private.portal_decorate_card_context_v1(
    private.portal_search_cn1(
      'flow', p_query, p_filters, p_sort, p_cursor, p_limit
    )
  );
exception
  when sqlstate '22023' then
    raise exception using errcode = '22023', message = 'invalid portal request';
  when query_canceled then
    raise exception using errcode = 'P0001', message = 'portal catalog unavailable';
  when others then
    raise exception using errcode = 'P0001', message = 'portal catalog unavailable';
end
$function$
;
alter function api.portal_search_flows_v1(text,jsonb,text,text,integer) owner to portal_public_executor;

CREATE OR REPLACE FUNCTION api.portal_facets_v1(p_kind text, p_query text, p_filters jsonb DEFAULT '{}'::jsonb)
 RETURNS jsonb
 LANGUAGE plpgsql
 STABLE SECURITY DEFINER
 SET search_path TO ''
 SET statement_timeout TO '8s'
AS $function$
declare
  v_kind text;
  v_query text;
  v_filters jsonb;
  v_fingerprint text;
  v_exact_id uuid;
  v_like_pattern text;
begin
  perform private.assert_portal_catalog_projection_contract_cn1();

  if pg_catalog.octet_length(coalesce(p_kind, '')) > 32 then
    raise exception using errcode = '22023', message = 'invalid portal request';
  end if;
  v_kind := pg_catalog.lower(pg_catalog.btrim(coalesce(p_kind, '')));
  perform private.portal_validate_search_v1(
    v_kind,
    coalesce(p_query, ''),
    coalesce(p_filters, '{}'::jsonb),
    'relevance',
    1
  );
  v_query := pg_catalog.lower(pg_catalog.btrim(coalesce(p_query, '')));
  v_filters := private.portal_normalize_filters_v1(p_filters);
  v_fingerprint := private.portal_query_fingerprint_v1(
    v_kind,
    v_query,
    v_filters,
    'relevance'
  );

  if v_query = '' and v_filters = '{}'::jsonb then
    perform private.assert_portal_catalog_facet_contract_v1();
    return private.catalog_portal_facets_empty_v1_impl(
      v_kind,
      v_fingerprint
    );
  end if;

  if v_query ~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' then
    v_exact_id := v_query::uuid;
  end if;
  if v_query <> '' then
    v_like_pattern := '%' || pg_catalog.replace(
      pg_catalog.replace(
        pg_catalog.replace(
          v_query,
          pg_catalog.chr(92),
          pg_catalog.chr(92) || pg_catalog.chr(92)
        ),
        '%',
        pg_catalog.chr(92) || '%'
      ),
      '_',
      pg_catalog.chr(92) || '_'
    ) || '%';
  end if;

  return private.catalog_portal_facets_cn1_impl(
    v_kind,
    v_query,
    v_exact_id,
    v_like_pattern,
    v_filters,
    v_fingerprint
  );
exception
  when sqlstate '22023' then
    raise exception using errcode = '22023', message = 'invalid portal request';
  when query_canceled then
    raise exception using errcode = 'P0001', message = 'portal catalog unavailable';
  when others then
    raise exception using errcode = 'P0001', message = 'portal catalog unavailable';
end
$function$
;
alter function api.portal_facets_v1(text,text,jsonb) owner to portal_public_executor;

CREATE OR REPLACE FUNCTION api.portal_hybrid_search_v1(p_kind text, p_query_terms text[], p_query_embedding text, p_filters jsonb, p_limit integer)
 RETURNS jsonb
 LANGUAGE plpgsql
 STABLE PARALLEL RESTRICTED SECURITY DEFINER
 SET search_path TO ''
 SET statement_timeout TO '20s'
AS $function$
declare
  v_input jsonb;
  v_page jsonb;
begin
  v_input := private.portal_public_hybrid_input_v1(
    p_kind,
    p_query_terms,
    p_query_embedding,
    p_filters,
    p_limit
  );
  v_page := private.portal_decorate_card_context_v1(
    private.portal_lcia_decorate_item_page_v1(
      private.portal_projection_hybrid_search_cn1_impl(
        v_input ->> 'kind',
        array(
          select term.value
          from pg_catalog.jsonb_array_elements_text(v_input -> 'queryTerms')
            with ordinality as term(value, ordinality)
          order by term.ordinality
        ),
        (v_input ->> 'queryEmbedding')::extensions.vector(1024),
        v_input -> 'filters',
        (v_input ->> 'limit')::integer,
        v_input ->> 'queryFingerprint'
      )
    )
  );
  if v_page is null
     or pg_catalog.octet_length(
       pg_catalog.convert_to(v_page::text, 'UTF8')
     ) > 524288 then
    raise exception using
      errcode = '54000',
      message = 'portal hybrid response too large';
  end if;
  return v_page;
exception
  when sqlstate '22023' then
    raise exception using errcode = '22023', message = 'invalid portal request';
  when query_canceled then
    raise exception using errcode = 'P0001', message = 'portal hybrid unavailable';
  when others then
    raise exception using errcode = 'P0001', message = 'portal hybrid unavailable';
end
$function$
;
alter function api.portal_hybrid_search_v1(text,text[],text,jsonb,integer) owner to portal_public_executor;

CREATE OR REPLACE FUNCTION api.portal_hybrid_search_v2(p_kind text, p_query_terms text[], p_query_embedding text, p_filters jsonb, p_limit integer, p_cursor text DEFAULT NULL::text)
 RETURNS jsonb
 LANGUAGE plpgsql
 STABLE PARALLEL RESTRICTED SECURITY DEFINER
 SET search_path TO ''
 SET statement_timeout TO '20s'
AS $function$
declare
  v_input jsonb;
  v_fingerprint text;
  v_cursor jsonb;
  v_page jsonb;
begin
  v_input := private.portal_public_hybrid_input_v1(
    p_kind,p_query_terms,p_query_embedding,p_filters,p_limit);
  v_fingerprint := pg_catalog.encode(extensions.digest(
    pg_catalog.convert_to('portal-hybrid-rank-v2:composite-names-v2:' || (v_input ->> 'queryFingerprint'),'UTF8'),
    'sha256'),'hex');
  if p_cursor is not null then
    v_cursor := private.portal_cursor_decode_v1(p_cursor);
    if v_cursor is null
      or (select count(*) from pg_catalog.jsonb_object_keys(v_cursor)) <> 6
      or not (v_cursor ?& array['v','fp','kind','rankKey','id','version'])
      or v_cursor ->> 'v' is distinct from '1'
      or v_cursor ->> 'fp' is distinct from v_fingerprint
      or v_cursor ->> 'kind' is distinct from p_kind
      or pg_catalog.jsonb_typeof(v_cursor -> 'rankKey') is distinct from 'string'
      or coalesce(v_cursor ->> 'rankKey','') !~ '^(0(\.\d{1,12})?|1(\.0{1,12})?)$'
      or coalesce(v_cursor ->> 'id','') !~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
      or coalesce(v_cursor ->> 'version','') !~ '^\d{2}\.\d{2}\.\d{3}$'
      or private.portal_cursor_encode_v1(v_cursor) is distinct from p_cursor then
      raise exception using errcode = '22023', message = 'invalid portal request';
    end if;
  end if;
  v_page := private.portal_decorate_card_context_v1(private.portal_lcia_decorate_item_page_v1(
    private.portal_projection_hybrid_search_cn2_impl(
      p_kind,
      array(select term.value from pg_catalog.jsonb_array_elements_text(v_input -> 'queryTerms')
        with ordinality as term(value,ordinality) order by term.ordinality),
      (v_input ->> 'queryEmbedding')::extensions.vector(1024),
      v_input -> 'filters',p_limit,v_fingerprint,v_cursor
    )
  ));
  v_page := pg_catalog.jsonb_set(v_page,'{schemaVersion}','"portal.public-hybrid-candidate-page.v2"'::jsonb);
  v_page := (v_page - 'nextCursorPayload') || pg_catalog.jsonb_build_object(
    'nextCursor',case when nullif(v_page -> 'nextCursorPayload','null'::jsonb) is null then null
      else private.portal_cursor_encode_v1(v_page -> 'nextCursorPayload') end
  );
  if v_page is null or pg_catalog.octet_length(pg_catalog.convert_to(v_page::text,'UTF8')) > 524288 then
    raise exception using errcode = '54000', message = 'portal hybrid response too large';
  end if;
  return v_page;
exception
  when sqlstate '22023' then
    raise exception using errcode = '22023', message = 'invalid portal request';
  when query_canceled then
    raise exception using errcode = 'P0001', message = 'portal hybrid unavailable';
  when others then
    raise exception using errcode = 'P0001', message = 'portal hybrid unavailable';
end;
$function$
;
alter function api.portal_hybrid_search_v2(text,text[],text,jsonb,integer,text) owner to portal_public_executor;

CREATE OR REPLACE FUNCTION api.portal_search_processes_v2(p_query text, p_filters jsonb DEFAULT '{}'::jsonb, p_sort text DEFAULT 'relevance'::text, p_cursor text DEFAULT NULL::text, p_limit integer DEFAULT 20)
 RETURNS jsonb
 LANGUAGE plpgsql
 STABLE SECURITY DEFINER
 SET search_path TO ''
 SET statement_timeout TO '8s'
AS $function$
begin
  return pg_catalog.jsonb_set(private.portal_decorate_card_context_v1(
    private.portal_lcia_decorate_item_page_v1(
      private.portal_search_cn2(
        'process', p_query, p_filters, p_sort, p_cursor, p_limit
      )
    )
  ), '{schemaVersion}', '"portal.public-search-page.v2"'::jsonb);
exception
  when sqlstate '22023' then
    raise exception using errcode = '22023', message = 'invalid portal request';
  when query_canceled then
    raise exception using errcode = 'P0001', message = 'portal catalog unavailable';
  when others then
    raise exception using errcode = 'P0001', message = 'portal catalog unavailable';
end
$function$
;
alter function api.portal_search_processes_v2(text,jsonb,text,text,integer) owner to portal_public_executor;

CREATE OR REPLACE FUNCTION api.portal_search_flows_v2(p_query text, p_filters jsonb DEFAULT '{}'::jsonb, p_sort text DEFAULT 'relevance'::text, p_cursor text DEFAULT NULL::text, p_limit integer DEFAULT 20)
 RETURNS jsonb
 LANGUAGE plpgsql
 STABLE SECURITY DEFINER
 SET search_path TO ''
 SET statement_timeout TO '8s'
AS $function$
begin
  return pg_catalog.jsonb_set(private.portal_decorate_card_context_v1(
    private.portal_search_cn2(
      'flow', p_query, p_filters, p_sort, p_cursor, p_limit
    )
  ), '{schemaVersion}', '"portal.public-search-page.v2"'::jsonb);
exception
  when sqlstate '22023' then
    raise exception using errcode = '22023', message = 'invalid portal request';
  when query_canceled then
    raise exception using errcode = 'P0001', message = 'portal catalog unavailable';
  when others then
    raise exception using errcode = 'P0001', message = 'portal catalog unavailable';
end
$function$
;
alter function api.portal_search_flows_v2(text,jsonb,text,text,integer) owner to portal_public_executor;

CREATE OR REPLACE FUNCTION api.portal_facets_v2(p_kind text, p_query text, p_filters jsonb DEFAULT '{}'::jsonb)
 RETURNS jsonb
 LANGUAGE plpgsql
 STABLE SECURITY DEFINER
 SET search_path TO ''
 SET statement_timeout TO '8s'
AS $function$
declare
  v_kind text;
  v_query text;
  v_filters jsonb;
  v_fingerprint text;
  v_exact_id uuid;
  v_like_pattern text;
begin
  perform private.assert_portal_catalog_projection_contract_cn1();

  if pg_catalog.octet_length(coalesce(p_kind, '')) > 32 then
    raise exception using errcode = '22023', message = 'invalid portal request';
  end if;
  v_kind := pg_catalog.lower(pg_catalog.btrim(coalesce(p_kind, '')));
  perform private.portal_validate_search_v1(
    v_kind,
    coalesce(p_query, ''),
    coalesce(p_filters, '{}'::jsonb),
    'relevance',
    1
  );
  v_query := pg_catalog.lower(pg_catalog.btrim(coalesce(p_query, '')));
  v_filters := private.portal_normalize_filters_v1(p_filters);
  v_fingerprint := private.portal_query_fingerprint_v1(
    v_kind,
    v_query,
    v_filters,
    'relevance'
  );

  v_fingerprint := pg_catalog.encode(extensions.digest(
    pg_catalog.convert_to('portal-search-versions-v2:' || v_fingerprint,'UTF8'),'sha256'),'hex');
  if v_query = '' and v_filters = '{}'::jsonb then
    perform private.assert_portal_catalog_facet_contract_v1();
    return private.catalog_portal_facets_empty_v2_impl(
      v_kind,
      v_fingerprint
    );
  end if;

  if v_query ~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' then
    v_exact_id := v_query::uuid;
  end if;
  if v_query <> '' then
    v_like_pattern := '%' || pg_catalog.replace(
      pg_catalog.replace(
        pg_catalog.replace(
          v_query,
          pg_catalog.chr(92),
          pg_catalog.chr(92) || pg_catalog.chr(92)
        ),
        '%',
        pg_catalog.chr(92) || '%'
      ),
      '_',
      pg_catalog.chr(92) || '_'
    ) || '%';
  end if;

  return private.catalog_portal_facets_cn2_impl(
    v_kind,
    v_query,
    v_exact_id,
    v_like_pattern,
    v_filters,
    v_fingerprint
  );
exception
  when sqlstate '22023' then
    raise exception using errcode = '22023', message = 'invalid portal request';
  when query_canceled then
    raise exception using errcode = 'P0001', message = 'portal catalog unavailable';
  when others then
    raise exception using errcode = 'P0001', message = 'portal catalog unavailable';
end
$function$
;
alter function api.portal_facets_v2(text,text,jsonb) owner to portal_public_executor;


revoke create on schema private from portal_public_executor, api_internal_executor;
revoke portal_public_executor, api_internal_executor from postgres;
commit;
