CREATE OR REPLACE FUNCTION "private"."portal_public_hybrid_input_v1"("p_kind" "text", "p_query_terms" "text"[], "p_query_embedding" "text", "p_filters" "jsonb", "p_limit" integer) RETURNS "jsonb"
    LANGUAGE "plpgsql" IMMUTABLE PARALLEL SAFE
    SET "search_path" TO ''
    AS $_$
declare
  v_terms text[];
  v_term text;
  v_filters jsonb;
  v_key text;
  v_year numeric;
  v_embedding extensions.vector(1024);
  v_embedding_components text[];
  v_embedding_text text;
  v_embedding_sha256 text;
  v_fingerprint text;
begin
  if p_kind is null
     or p_kind not in ('process', 'flow')
     or p_limit is null
     or p_limit not between 1 and 20
     or p_query_terms is null
     or pg_catalog.array_ndims(p_query_terms) <> 1
     or pg_catalog.cardinality(p_query_terms) not between 1 and 12
     or exists (
       select 1
       from pg_catalog.unnest(p_query_terms) as supplied(term)
       where supplied.term is null
     ) then
    raise exception using errcode = '22023', message = 'invalid portal request';
  end if;

  select pg_catalog.array_agg(
    pg_catalog.lower(
      pg_catalog.btrim(supplied.term) collate pg_catalog."und-x-icu"
    )
    order by supplied.ordinality
  )
  into v_terms
  from pg_catalog.unnest(p_query_terms) with ordinality as supplied(term, ordinality);

  foreach v_term in array v_terms
  loop
    if pg_catalog.char_length(v_term) not between 1 and 512
       or pg_catalog.octet_length(v_term) > 2048
       or exists (
         select 1
         from pg_catalog.generate_series(1, pg_catalog.char_length(v_term)) as position(value)
         where pg_catalog.ascii(pg_catalog.substr(v_term, position.value, 1))
           between 0 and 31
            or pg_catalog.ascii(pg_catalog.substr(v_term, position.value, 1))
              between 127 and 159
       ) then
      raise exception using errcode = '22023', message = 'invalid portal request';
    end if;
  end loop;
  if (
    select count(distinct supplied.term)
    from pg_catalog.unnest(v_terms) as supplied(term)
  ) <> pg_catalog.cardinality(v_terms) then
    raise exception using errcode = '22023', message = 'invalid portal request';
  end if;

  if p_query_embedding is null
     or pg_catalog.octet_length(p_query_embedding) > 65536
     or pg_catalog.left(p_query_embedding, 1) <> '['
     or pg_catalog.right(p_query_embedding, 1) <> ']' then
    raise exception using errcode = '22023', message = 'invalid portal request';
  end if;
  v_embedding_components := pg_catalog.string_to_array(
    pg_catalog.substr(p_query_embedding, 2, pg_catalog.char_length(p_query_embedding) - 2),
    ','
  );
  if pg_catalog.cardinality(v_embedding_components) <> 1024
     or exists (
       select 1
       from pg_catalog.unnest(v_embedding_components) as component(value)
       where component.value
         !~ '^-?(?:0|[1-9][0-9]*)(?:\.[0-9]+)?(?:[eE][+-]?[0-9]+)?$'
     ) then
    raise exception using errcode = '22023', message = 'invalid portal request';
  end if;
  begin
    v_embedding := p_query_embedding::extensions.vector(1024);
  exception
    when others then
      raise exception using errcode = '22023', message = 'invalid portal request';
  end;
  if extensions.vector_dims(v_embedding) <> 1024 then
    raise exception using errcode = '22023', message = 'invalid portal request';
  end if;
  v_embedding_text := v_embedding::text;
  v_embedding_sha256 := pg_catalog.encode(
    extensions.digest(pg_catalog.convert_to(v_embedding_text, 'UTF8'), 'sha256'),
    'hex'
  );

  if p_filters is null or pg_catalog.jsonb_typeof(p_filters) <> 'object' then
    raise exception using errcode = '22023', message = 'invalid portal request';
  end if;
  if exists (
    select 1
    from pg_catalog.jsonb_object_keys(p_filters) as supplied(key)
    where supplied.key not in (
      'accessLevel', 'geography', 'classification', 'referenceYearFrom',
      'referenceYearTo', 'processSubtype', 'source'
    )
  ) then
    raise exception using errcode = '22023', message = 'invalid portal request';
  end if;
  select coalesce(
    pg_catalog.jsonb_object_agg(
      supplied.key,
      case
        when supplied.key in (
          'geography', 'classification', 'processSubtype', 'source'
        ) and pg_catalog.jsonb_typeof(supplied.value) = 'string'
          then pg_catalog.to_jsonb(pg_catalog.lower(
            pg_catalog.btrim(supplied.value #>> '{}') collate pg_catalog."und-x-icu"
          ))
        else supplied.value
      end
      order by supplied.key
    ),
    '{}'::jsonb
  )
  into v_filters
  from pg_catalog.jsonb_each(p_filters) as supplied(key, value);
  if pg_catalog.octet_length(pg_catalog.convert_to(v_filters::text, 'UTF8')) > 4096
     or (p_kind = 'flow' and v_filters ? 'processSubtype')
     or (
       v_filters ? 'accessLevel'
       and (
         pg_catalog.jsonb_typeof(v_filters -> 'accessLevel') <> 'string'
         or v_filters ->> 'accessLevel' not in ('open', 'metadata_only')
       )
     ) then
    raise exception using errcode = '22023', message = 'invalid portal request';
  end if;

  foreach v_key in array array['geography', 'classification', 'processSubtype', 'source']
  loop
    if v_filters ? v_key
       and (
         pg_catalog.jsonb_typeof(v_filters -> v_key) <> 'string'
         or pg_catalog.char_length(v_filters ->> v_key) not between 1 and 128
         or pg_catalog.octet_length(v_filters ->> v_key) > 1024
         or exists (
           select 1
           from pg_catalog.generate_series(
             1,
             pg_catalog.char_length(v_filters ->> v_key)
           ) as position(value)
           where pg_catalog.ascii(
             pg_catalog.substr(v_filters ->> v_key, position.value, 1)
           ) between 0 and 31
              or pg_catalog.ascii(
                pg_catalog.substr(v_filters ->> v_key, position.value, 1)
              ) between 127 and 159
         )
       ) then
      raise exception using errcode = '22023', message = 'invalid portal request';
    end if;
  end loop;

  foreach v_key in array array['referenceYearFrom', 'referenceYearTo']
  loop
    if v_filters ? v_key then
      if pg_catalog.jsonb_typeof(v_filters -> v_key) <> 'number' then
        raise exception using errcode = '22023', message = 'invalid portal request';
      end if;
      v_year := (v_filters ->> v_key)::numeric;
      if v_year <> pg_catalog.trunc(v_year) or v_year not between 0 and 9999 then
        raise exception using errcode = '22023', message = 'invalid portal request';
      end if;
    end if;
  end loop;
  if v_filters ? 'referenceYearFrom'
     and v_filters ? 'referenceYearTo'
     and (v_filters ->> 'referenceYearFrom')::numeric
       > (v_filters ->> 'referenceYearTo')::numeric then
    raise exception using errcode = '22023', message = 'invalid portal request';
  end if;

  v_fingerprint := pg_catalog.encode(
    extensions.digest(
      pg_catalog.convert_to(
        pg_catalog.jsonb_build_object(
          'algorithmVersion', 'portal-hybrid-rank-v1',
          'kind', p_kind,
          'queryTerms', pg_catalog.to_jsonb(v_terms),
          'queryEmbeddingSha256', v_embedding_sha256,
          'filters', v_filters,
          'limit', p_limit
        )::text,
        'UTF8'
      ),
      'sha256'
    ),
    'hex'
  );

  return pg_catalog.jsonb_build_object(
    'kind', p_kind,
    'queryTerms', pg_catalog.to_jsonb(v_terms),
    'queryEmbedding', v_embedding_text,
    'filters', v_filters,
    'limit', p_limit,
    'queryFingerprint', v_fingerprint
  );
end
$_$;

ALTER FUNCTION "private"."portal_public_hybrid_input_v1"("p_kind" "text", "p_query_terms" "text"[], "p_query_embedding" "text", "p_filters" "jsonb", "p_limit" integer) OWNER TO "portal_public_executor";

REVOKE ALL ON FUNCTION "private"."portal_public_hybrid_input_v1"("p_kind" "text", "p_query_terms" "text"[], "p_query_embedding" "text", "p_filters" "jsonb", "p_limit" integer) FROM PUBLIC;
