CREATE OR REPLACE FUNCTION "private"."portal_validate_search_v1"("p_kind" "text", "p_query" "text", "p_filters" "jsonb", "p_sort" "text", "p_limit" integer) RETURNS "void"
    LANGUAGE "plpgsql" STABLE PARALLEL SAFE
    SET "search_path" TO ''
    AS $_$
declare
  v_key text;
  v_allowed text[] := array[
    'accessLevel', 'geography', 'classification', 'referenceYearFrom',
    'referenceYearTo', 'source'
  ];
begin
  if p_kind not in ('process', 'flow', 'all')
     or p_query is null
     or length(p_query) > 512
     or pg_catalog.octet_length(p_query) > 2048
     or p_query ~ '[[:cntrl:]]'
     or p_sort is null
     or length(p_sort) > 32
     or pg_catalog.octet_length(p_sort) > 64
     or lower(btrim(p_sort)) not in ('relevance', 'modified_desc', 'name_asc')
     or p_limit is null
     or p_limit < 1
     or p_limit > 50
     or p_filters is null
     or jsonb_typeof(p_filters) <> 'object'
     or pg_catalog.pg_column_size(p_filters) > 4096
     or (select count(*) from jsonb_object_keys(p_filters)) > 7 then
    raise exception using errcode = '22023', message = 'invalid portal request';
  end if;
  if p_kind in ('process', 'all') then
    v_allowed := pg_catalog.array_append(v_allowed, 'processSubtype');
  end if;
  for v_key in select jsonb_object_keys(p_filters)
  loop
    if not (v_key = any(v_allowed)) then
      raise exception using errcode = '22023', message = 'invalid portal request';
    end if;
  end loop;
  if p_filters ? 'accessLevel'
     and (
       jsonb_typeof(p_filters -> 'accessLevel') <> 'string'
       or p_filters ->> 'accessLevel' not in ('open', 'metadata_only')
     ) then
    raise exception using errcode = '22023', message = 'invalid portal request';
  end if;
  foreach v_key in array array['geography', 'classification', 'processSubtype', 'source']
  loop
    if p_filters ? v_key
       and (
         jsonb_typeof(p_filters -> v_key) <> 'string'
         or length(btrim(p_filters ->> v_key)) not between 1 and 128
       ) then
      raise exception using errcode = '22023', message = 'invalid portal request';
    end if;
  end loop;
  foreach v_key in array array['referenceYearFrom', 'referenceYearTo']
  loop
    if p_filters ? v_key
       and (
         jsonb_typeof(p_filters -> v_key) <> 'number'
         or (p_filters ->> v_key) !~ '^[0-9]{1,4}$'
       ) then
      raise exception using errcode = '22023', message = 'invalid portal request';
    end if;
  end loop;
  if p_filters ? 'referenceYearFrom'
     and p_filters ? 'referenceYearTo'
     and (p_filters ->> 'referenceYearFrom')::integer > (p_filters ->> 'referenceYearTo')::integer then
    raise exception using errcode = '22023', message = 'invalid portal request';
  end if;
end
$_$;

ALTER FUNCTION "private"."portal_validate_search_v1"("p_kind" "text", "p_query" "text", "p_filters" "jsonb", "p_sort" "text", "p_limit" integer) OWNER TO "portal_public_executor";

REVOKE ALL ON FUNCTION "private"."portal_validate_search_v1"("p_kind" "text", "p_query" "text", "p_filters" "jsonb", "p_sort" "text", "p_limit" integer) FROM PUBLIC;
