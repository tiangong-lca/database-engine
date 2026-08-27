CREATE OR REPLACE FUNCTION "private"."portal_decorate_card_context_v1"("p_page" "jsonb") RETURNS "jsonb"
    LANGUAGE "plpgsql" STABLE SECURITY DEFINER PARALLEL RESTRICTED
    SET "search_path" TO ''
    SET "statement_timeout" TO '8s'
    SET "plan_cache_mode" TO 'force_custom_plan'
    SET "row_security" TO 'on'
    AS $_$
declare
  v_schema_version text := p_page ->> 'schemaVersion';
  v_kind text := p_page ->> 'kind';
  v_expected integer;
  v_actual integer;
  v_items jsonb;
begin
  perform private.assert_portal_card_context_contract_v1();

  if pg_catalog.jsonb_typeof(p_page) <> 'object'
     or pg_catalog.jsonb_typeof(p_page -> 'items') <> 'array'
     or v_schema_version not in (
       'portal.public-search-page.v1',
       'portal.public-hybrid-candidate-page.v1'
     )
     or v_kind not in ('process', 'flow') then
    raise exception 'Portal card context page is invalid'
      using errcode = '55000';
  end if;
  v_expected := pg_catalog.jsonb_array_length(p_page -> 'items');
  if v_expected > (
    case
      when v_schema_version = 'portal.public-search-page.v1' then 50
      else 20
    end
  ) then
    raise exception 'Portal card context page exceeds its fixed bound'
      using errcode = '54000';
  end if;

  if v_kind = 'process' then
    with input as materialized (
      select item.value, item.ordinality,
        case
          when item.value #>> '{key,id}'
            ~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
            then (item.value #>> '{key,id}')::uuid
          else null
        end as id,
        item.value #>> '{key,version}' as version
      from pg_catalog.jsonb_array_elements(p_page -> 'items')
        with ordinality as item(value, ordinality)
      where pg_catalog.jsonb_typeof(item.value) = 'object'
        and item.value #>> '{key,kind}' = 'process'
        and item.value #>> '{key,version}' ~ '^\d{2}\.\d{2}\.\d{3}$'
    ), hydrated as materialized (
      select input.value, input.ordinality,
        private.portal_card_context_v1(
          'process', source.state_code, source.json
        ) as card_context
      from input
      join public.processes as source
        on source.id = input.id
       and source.version::text = input.version
       and source.state_code in (100, 200)
    )
    select count(*), coalesce(
      pg_catalog.jsonb_agg(
        hydrated.value || pg_catalog.jsonb_build_object(
          'context', hydrated.card_context
        ) order by hydrated.ordinality
      ),
      '[]'::jsonb
    )
    into v_actual, v_items
    from hydrated
    where pg_catalog.jsonb_typeof(hydrated.card_context) = 'object';
  else
    with input as materialized (
      select item.value, item.ordinality,
        case
          when item.value #>> '{key,id}'
            ~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
            then (item.value #>> '{key,id}')::uuid
          else null
        end as id,
        item.value #>> '{key,version}' as version
      from pg_catalog.jsonb_array_elements(p_page -> 'items')
        with ordinality as item(value, ordinality)
      where pg_catalog.jsonb_typeof(item.value) = 'object'
        and item.value #>> '{key,kind}' = 'flow'
        and item.value #>> '{key,version}' ~ '^\d{2}\.\d{2}\.\d{3}$'
    ), hydrated as materialized (
      select input.value, input.ordinality,
        private.portal_card_context_v1(
          'flow', source.state_code, source.json
        ) as card_context
      from input
      join public.flows as source
        on source.id = input.id
       and source.version::text = input.version
       and source.state_code in (100, 200)
    )
    select count(*), coalesce(
      pg_catalog.jsonb_agg(
        hydrated.value || pg_catalog.jsonb_build_object(
          'context', hydrated.card_context
        ) order by hydrated.ordinality
      ),
      '[]'::jsonb
    )
    into v_actual, v_items
    from hydrated
    where pg_catalog.jsonb_typeof(hydrated.card_context) = 'object';
  end if;

  if v_actual is distinct from v_expected then
    raise exception 'Portal card context exact-key hydration failed'
      using errcode = '55000';
  end if;
  return pg_catalog.jsonb_set(p_page, '{items}', v_items, false);
end
$_$;

ALTER FUNCTION "private"."portal_decorate_card_context_v1"("p_page" "jsonb") OWNER TO "portal_public_executor";

REVOKE ALL ON FUNCTION "private"."portal_decorate_card_context_v1"("p_page" "jsonb") FROM PUBLIC;
