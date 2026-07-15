create or replace function private.dataset_alias_js_object_key_sort_key_v1(
  p_value text
) returns bytea
language plpgsql
immutable
strict
parallel safe
set search_path = ''
as $$
declare
  v_result bytea := '\x01'::bytea;
  v_array_index bigint;
  v_code_point integer;
  v_supplementary integer;
begin
  -- stableJsonText sorts keys by JavaScript UTF-16 order before rebuilding an
  -- object. JSON.stringify then enumerates canonical array-index keys first in
  -- ascending numeric order. Reproduce both rules with a binary prefix plus a
  -- big-endian numeric or UTF-16 payload. PostgreSQL text cannot contain lone
  -- surrogates.
  if p_value ~ '^(0|[1-9][0-9]{0,9})$' then
    v_array_index := p_value::bigint;
    if v_array_index <= 4294967294 then
      return '\x00'::bytea || int8send(v_array_index);
    end if;
  end if;

  for v_character_index in 1..character_length(p_value) loop
    v_code_point := ascii(substring(p_value from v_character_index for 1));

    if v_code_point <= 65535 then
      v_result := v_result || decode(
        lpad(to_hex(v_code_point), 4, '0'),
        'hex'
      );
    else
      v_supplementary := v_code_point - 65536;
      v_result := v_result || decode(
        lpad(to_hex(55296 + (v_supplementary / 1024)), 4, '0')
          || lpad(to_hex(56320 + (v_supplementary % 1024)), 4, '0'),
        'hex'
      );
    end if;
  end loop;

  return v_result;
end;
$$;

alter function private.dataset_alias_js_object_key_sort_key_v1(text)
  owner to postgres;

revoke all on function private.dataset_alias_js_object_key_sort_key_v1(text)
  from public, anon, authenticated, service_role;

comment on function private.dataset_alias_js_object_key_sort_key_v1(text) is
  'Builds a binary sort key matching stableJsonText object enumeration: canonical array indexes first, then JavaScript UTF-16 string order.';

create or replace function private.dataset_alias_canonical_jsonb_v1(
  p_value jsonb
) returns text
language plpgsql
stable
strict
set search_path = ''
as $$
declare
  v_result text;
begin
  case jsonb_typeof(p_value)
    when 'object' then
      select '{' || coalesce(string_agg(
        to_jsonb(object_item.key)::text
          || ':'
          || private.dataset_alias_canonical_jsonb_v1(object_item.value),
        ',' order by private.dataset_alias_js_object_key_sort_key_v1(object_item.key)
      ), '') || '}'
      into v_result
      from jsonb_each(p_value) as object_item(key, value);
    when 'array' then
      select '[' || coalesce(string_agg(
        private.dataset_alias_canonical_jsonb_v1(array_item.value),
        ',' order by array_item.ordinality
      ), '') || ']'
      into v_result
      from jsonb_array_elements(p_value)
        with ordinality as array_item(value, ordinality);
    else
      v_result := p_value::text;
  end case;

  return v_result;
end;
$$;

alter function private.dataset_alias_canonical_jsonb_v1(jsonb)
  owner to postgres;

revoke all on function private.dataset_alias_canonical_jsonb_v1(jsonb)
  from public, anon, authenticated, service_role;

comment on function private.dataset_alias_canonical_jsonb_v1(jsonb) is
  'Serializes JSON with recursively stableJsonText-compatible object key order and compact separators for guarded alias exchange evidence hashing.';
