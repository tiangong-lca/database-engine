-- Root/reference review v2 contract.
--
-- This migration is deliberately additive:
--   * no physical relation tables are introduced;
--   * legacy reviews remain readable with review_kind IS NULL;
--   * the seven dataset tables keep their historical review_id/reviews shape;
--   * scope_history on the root review is the normalized relationship fact.

create or replace function private.review_canonical_json_text_v1(p_value jsonb)
returns text
language plpgsql
immutable
strict
parallel safe
set search_path = ''
as $$
declare
  v_result text;
begin
  case pg_catalog.jsonb_typeof(p_value)
    when 'object' then
      select '{' || coalesce(pg_catalog.string_agg(
        pg_catalog.to_jsonb(item.key)::text
          || ':'
          || private.review_canonical_json_text_v1(item.value),
        ',' order by item.key collate "C"
      ), '') || '}'
      into v_result
      from pg_catalog.jsonb_each(p_value) as item(key, value);
    when 'array' then
      select '[' || coalesce(pg_catalog.string_agg(
        private.review_canonical_json_text_v1(item.value),
        ',' order by item.ordinality
      ), '') || ']'
      into v_result
      from pg_catalog.jsonb_array_elements(p_value)
        with ordinality as item(value, ordinality);
    else
      v_result := p_value::text;
  end case;

  return v_result;
end;
$$;

alter function private.review_canonical_json_text_v1(jsonb) owner to postgres;
revoke all on function private.review_canonical_json_text_v1(jsonb)
  from public, anon, authenticated, service_role;

comment on function private.review_canonical_json_text_v1(jsonb) is
  'Canonical JSON writer for review revision fingerprints. Object keys use UTF-8/C byte order, arrays retain order, and separators are compact, matching app_dataset_submit_review Gate hashing.';

create or replace function public.review_revision_fingerprint_v1(
  p_target_table text,
  p_target_row jsonb
)
returns text
language plpgsql
immutable
strict
parallel safe
set search_path = ''
as $$
declare
  v_document jsonb;
begin
  if lower(p_target_table) not in (
    'contacts',
    'sources',
    'unitgroups',
    'flowproperties',
    'flows',
    'processes',
    'lifecyclemodels'
  ) then
    raise exception using
      errcode = '22023',
      message = 'REVIEW_TARGET_TABLE_INVALID';
  end if;

  v_document := coalesce(
    p_target_row->'json_ordered',
    p_target_row->'json',
    '{}'::jsonb
  );

  return pg_catalog.encode(
    extensions.digest(
      pg_catalog.convert_to(
        private.review_canonical_json_text_v1(v_document),
        'UTF8'
      ),
      'sha256'
    ),
    'hex'
  );
end;
$$;

alter function public.review_revision_fingerprint_v1(text, jsonb)
  owner to postgres;
revoke all on function public.review_revision_fingerprint_v1(text, jsonb)
  from public, anon, authenticated, service_role;
grant execute on function public.review_revision_fingerprint_v1(text, jsonb)
  to service_role;

comment on function public.review_revision_fingerprint_v1(text, jsonb) is
  'Returns the unprefixed 64-character lowercase SHA-256 revision checksum shared with the Process review-submit Gate.';

create or replace function public.review_scope_current_snapshot_v1(
  p_scope_history jsonb
)
returns jsonb
language sql
immutable
parallel safe
set search_path = ''
as $$
  select case
    when pg_catalog.jsonb_typeof(p_scope_history) <> 'object'
      or p_scope_history->>'schema_version' <> 'review_scope.v1'
      or pg_catalog.jsonb_typeof(p_scope_history->'snapshots') <> 'array'
      then null
    else (
      select snapshot.value
      from pg_catalog.jsonb_array_elements(p_scope_history->'snapshots')
        with ordinality as snapshot(value, ordinality)
      where snapshot.value->>'version_no' = p_scope_history->>'current_version'
      order by snapshot.ordinality desc
      limit 1
    )
  end
$$;

alter function public.review_scope_current_snapshot_v1(jsonb) owner to postgres;
revoke all on function public.review_scope_current_snapshot_v1(jsonb)
  from public, anon, authenticated, service_role;

create or replace function public.review_scope_current_items_v1(
  p_scope_history jsonb
)
returns table (
  item_kind text,
  target_table text,
  data_id uuid,
  data_version text,
  submitted_revision_checksum text,
  reference_review_id uuid,
  target_owner_id uuid,
  target_team_id uuid,
  relation_type text,
  relation_path text,
  introduced_by text,
  introduced_field_path text
)
language sql
immutable
parallel safe
set search_path = ''
as $$
  select
    item.value->>'item_kind',
    item.value->>'target_table',
    nullif(item.value->>'data_id', '')::uuid,
    item.value->>'data_version',
    item.value->>'submitted_revision_checksum',
    nullif(item.value->>'reference_review_id', '')::uuid,
    nullif(item.value->>'target_owner_id', '')::uuid,
    nullif(item.value->>'target_team_id', '')::uuid,
    item.value->>'relation_type',
    item.value->>'relation_path',
    item.value->>'introduced_by',
    nullif(item.value->>'introduced_field_path', '')
  from pg_catalog.jsonb_array_elements(
    coalesce(
      public.review_scope_current_snapshot_v1(p_scope_history)->'items',
      '[]'::jsonb
    )
  ) as item(value)
$$;

alter function public.review_scope_current_items_v1(jsonb) owner to postgres;
revoke all on function public.review_scope_current_items_v1(jsonb)
  from public, anon, authenticated, service_role;

create or replace function public.review_scope_current_reference_ids_v1(
  p_scope_history jsonb
)
returns uuid[]
language sql
immutable
parallel safe
set search_path = ''
as $$
  select coalesce(
    pg_catalog.array_agg(distinct item.reference_review_id
      order by item.reference_review_id)
      filter (where item.reference_review_id is not null),
    array[]::uuid[]
  )
  from public.review_scope_current_items_v1(p_scope_history) as item
  where item.item_kind = 'reference'
$$;

alter function public.review_scope_current_reference_ids_v1(jsonb)
  owner to postgres;
revoke all on function public.review_scope_current_reference_ids_v1(jsonb)
  from public, anon, authenticated, service_role;

create or replace function public.review_scope_all_reference_ids_v1(
  p_scope_history jsonb
)
returns uuid[]
language sql
immutable
parallel safe
set search_path = ''
as $$
  select case
    when pg_catalog.jsonb_typeof(p_scope_history) <> 'object'
      or p_scope_history->>'schema_version' <> 'review_scope.v1'
      or pg_catalog.jsonb_typeof(p_scope_history->'snapshots') <> 'array'
      then array[]::uuid[]
    else coalesce((
      select pg_catalog.array_agg(distinct
        nullif(item.value->>'reference_review_id', '')::uuid
        order by nullif(item.value->>'reference_review_id', '')::uuid
      ) filter (
        where item.value->>'item_kind' = 'reference'
          and coalesce(item.value->>'reference_review_id', '')
            ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
      )
      from pg_catalog.jsonb_array_elements(p_scope_history->'snapshots')
        as snapshot(value)
      cross join lateral pg_catalog.jsonb_array_elements(
        coalesce(snapshot.value->'items', '[]'::jsonb)
      ) as item(value)
    ), array[]::uuid[])
  end
$$;

alter function public.review_scope_all_reference_ids_v1(jsonb)
  owner to postgres;
revoke all on function public.review_scope_all_reference_ids_v1(jsonb)
  from public, anon, authenticated, service_role;

alter table public.reviews
  add column if not exists review_kind text,
  add column if not exists target_table text,
  add column if not exists submitted_revision_checksum text,
  add column if not exists approved_revision_checksum text,
  add column if not exists target_owner_id uuid,
  add column if not exists target_team_id uuid,
  add column if not exists scope_schema_version text,
  add column if not exists scope_history jsonb;

alter table public.reviews
  add column if not exists current_reference_review_ids uuid[]
    generated always as (
      public.review_scope_current_reference_ids_v1(scope_history)
    ) stored,
  add column if not exists all_reference_review_ids uuid[]
    generated always as (
      public.review_scope_all_reference_ids_v1(scope_history)
    ) stored;

alter table public.reviews
  add constraint reviews_review_kind_v2_chk
  check (review_kind is null or review_kind in ('root', 'reference'))
  not valid,
  add constraint reviews_target_table_v2_chk
  check (
    target_table is null
    or target_table in (
      'contacts',
      'sources',
      'unitgroups',
      'flowproperties',
      'flows',
      'processes',
      'lifecyclemodels'
    )
  )
  not valid,
  add constraint reviews_submitted_checksum_v2_chk
  check (
    submitted_revision_checksum is null
    or submitted_revision_checksum ~ '^[a-f0-9]{64}$'
  )
  not valid,
  add constraint reviews_approved_checksum_v2_chk
  check (
    approved_revision_checksum is null
    or approved_revision_checksum ~ '^[a-f0-9]{64}$'
  )
  not valid,
  add constraint reviews_kind_scope_v2_chk
  check (
    review_kind is null
    or (
      target_table is not null
      and data_id is not null
      and data_version is not null
      and submitted_revision_checksum is not null
      and (
        (
          review_kind = 'root'
          and scope_schema_version = 'review_scope.v1'
          and scope_history is not null
          and scope_history->>'schema_version' = scope_schema_version
        )
        or (
          review_kind = 'reference'
          and scope_schema_version is null
          and scope_history is null
        )
      )
    )
  )
  not valid;

alter table public.reviews
  validate constraint reviews_review_kind_v2_chk;
alter table public.reviews
  validate constraint reviews_target_table_v2_chk;
alter table public.reviews
  validate constraint reviews_submitted_checksum_v2_chk;
alter table public.reviews
  validate constraint reviews_approved_checksum_v2_chk;
alter table public.reviews
  validate constraint reviews_kind_scope_v2_chk;

create unique index if not exists reviews_reference_revision_active_uidx
  on public.reviews (
    target_table,
    data_id,
    data_version,
    submitted_revision_checksum
  )
  where review_kind = 'reference'
    and state_code in (0, 1, 2);

create index if not exists reviews_root_current_reference_ids_gin_idx
  on public.reviews using gin (current_reference_review_ids)
  where review_kind = 'root';

create unique index if not exists notifications_review_event_recipient_uidx
  on public.notifications (
    recipient_user_id,
    type,
    (json->>'event_key')
  )
  where nullif(json->>'event_key', '') is not null;

create or replace function public.review_scope_checksum_v1(p_items jsonb)
returns text
language sql
immutable
strict
parallel safe
set search_path = ''
as $$
  select pg_catalog.encode(
    extensions.digest(
      pg_catalog.convert_to(
        private.review_canonical_json_text_v1(p_items),
        'UTF8'
      ),
      'sha256'
    ),
    'hex'
  )
$$;

alter function public.review_scope_checksum_v1(jsonb) owner to postgres;
revoke all on function public.review_scope_checksum_v1(jsonb)
  from public, anon, authenticated, service_role;

create or replace function public.review_validate_scope_history_v1(
  p_root_review_id uuid,
  p_scope_history jsonb
)
returns void
language plpgsql
security definer
set search_path = ''
as $$
declare
  v_snapshot jsonb;
  v_item jsonb;
  v_expected_version integer := 1;
  v_reference public.reviews%rowtype;
begin
  if pg_catalog.jsonb_typeof(p_scope_history) <> 'object'
    or p_scope_history->>'schema_version' <> 'review_scope.v1'
    or pg_catalog.jsonb_typeof(p_scope_history->'snapshots') <> 'array'
    or pg_catalog.jsonb_array_length(p_scope_history->'snapshots') = 0 then
    raise exception using errcode = '22023', message = 'REVIEW_SCOPE_INVALID';
  end if;

  if pg_catalog.jsonb_array_length(p_scope_history->'snapshots') > 100 then
    raise exception using errcode = '54000', message = 'REVIEW_SCOPE_SNAPSHOT_LIMIT';
  end if;

  if pg_catalog.pg_column_size(p_scope_history) > 8388608 then
    raise exception using errcode = '54000', message = 'REVIEW_SCOPE_SIZE_LIMIT';
  end if;

  for v_snapshot in
    select value
    from pg_catalog.jsonb_array_elements(p_scope_history->'snapshots')
      with ordinality as snapshot(value, ordinality)
    order by snapshot.ordinality
  loop
    if coalesce((v_snapshot->>'version_no')::integer, 0) <> v_expected_version
      or v_snapshot->>'scope_basis' not in (
        'submitted',
        'review_metadata',
        'approved',
        'reference_repair',
        'migration'
      )
      or coalesce(v_snapshot->>'root_revision_checksum', '')
        !~ '^[a-f0-9]{64}$'
      or coalesce(v_snapshot->>'scope_checksum', '')
        !~ '^[a-f0-9]{64}$'
      or pg_catalog.jsonb_typeof(v_snapshot->'items') <> 'array'
      or pg_catalog.jsonb_array_length(v_snapshot->'items') = 0
      or pg_catalog.jsonb_array_length(v_snapshot->'items') > 5000 then
      raise exception using errcode = '22023', message = 'REVIEW_SCOPE_INVALID';
    end if;

    if v_snapshot->>'scope_checksum'
      <> public.review_scope_checksum_v1(v_snapshot->'items') then
      raise exception using errcode = '22023', message = 'REVIEW_SCOPE_CHECKSUM_MISMATCH';
    end if;

    if (
      select count(*)
      from pg_catalog.jsonb_array_elements(v_snapshot->'items') as item(value)
    ) <> (
      select count(*)
      from (
        select distinct
          item.value->>'target_table',
          item.value->>'data_id',
          item.value->>'data_version',
          item.value->>'submitted_revision_checksum'
        from pg_catalog.jsonb_array_elements(v_snapshot->'items') as item(value)
      ) as distinct_items
    ) then
      raise exception using errcode = '22023', message = 'REVIEW_SCOPE_ITEM_DUPLICATE';
    end if;

    for v_item in
      select value
      from pg_catalog.jsonb_array_elements(v_snapshot->'items')
    loop
      if v_item->>'item_kind' not in ('root', 'reference')
        or v_item->>'target_table' not in (
          'contacts',
          'sources',
          'unitgroups',
          'flowproperties',
          'flows',
          'processes',
          'lifecyclemodels'
        )
        or coalesce(v_item->>'data_id', '')
          !~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
        or nullif(v_item->>'data_version', '') is null
        or coalesce(v_item->>'submitted_revision_checksum', '')
          !~ '^[a-f0-9]{64}$'
        or coalesce(v_item->>'target_owner_id', '')
          !~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
        or (
          nullif(v_item->>'target_team_id', '') is not null
          and v_item->>'target_team_id'
            !~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
        ) then
        raise exception using errcode = '22023', message = 'REVIEW_SCOPE_ITEM_INVALID';
      end if;

      if v_item->>'item_kind' = 'root' then
        if nullif(v_item->>'reference_review_id', '') is not null then
          raise exception using errcode = '22023', message = 'REVIEW_SCOPE_ROOT_REFERENCE_INVALID';
        end if;
      else
        if coalesce(v_item->>'reference_review_id', '')
          !~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' then
          raise exception using errcode = '22023', message = 'REVIEW_SCOPE_REFERENCE_REQUIRED';
        end if;

        select review_row.*
        into v_reference
        from public.reviews as review_row
        where review_row.id = (v_item->>'reference_review_id')::uuid;

        if not found
          or v_reference.review_kind <> 'reference'
          or v_reference.target_table <> v_item->>'target_table'
          or v_reference.data_id <> (v_item->>'data_id')::uuid
          or btrim(v_reference.data_version::text) <> v_item->>'data_version'
          or v_reference.submitted_revision_checksum
            <> v_item->>'submitted_revision_checksum' then
          raise exception using errcode = '23503', message = 'REVIEW_SCOPE_REFERENCE_MISMATCH';
        end if;
      end if;
    end loop;

    v_expected_version := v_expected_version + 1;
  end loop;

  if coalesce((p_scope_history->>'current_version')::integer, 0)
    <> v_expected_version - 1 then
    raise exception using errcode = '22023', message = 'REVIEW_SCOPE_CURRENT_VERSION_INVALID';
  end if;

  if p_root_review_id is not null and not exists (
    select 1
    from public.reviews as root_review
    where root_review.id = p_root_review_id
      and root_review.review_kind = 'root'
  ) then
    raise exception using errcode = '23503', message = 'ROOT_REVIEW_NOT_FOUND';
  end if;
end;
$$;

alter function public.review_validate_scope_history_v1(uuid, jsonb)
  owner to postgres;
revoke all on function public.review_validate_scope_history_v1(uuid, jsonb)
  from public, anon, authenticated, service_role;

create or replace function public.review_append_scope_snapshot_v1(
  p_root_review_id uuid,
  p_scope_basis text,
  p_root_revision_checksum text,
  p_items jsonb,
  p_created_by uuid default auth.uid()
)
returns jsonb
language plpgsql
security definer
set search_path = ''
as $$
declare
  v_root public.reviews%rowtype;
  v_version integer;
  v_snapshot jsonb;
  v_history jsonb;
begin
  select review_row.*
  into v_root
  from public.reviews as review_row
  where review_row.id = p_root_review_id
  for update;

  if not found or v_root.review_kind <> 'root' then
    raise exception using errcode = 'P0002', message = 'ROOT_REVIEW_NOT_FOUND';
  end if;

  if p_scope_basis not in (
    'submitted',
    'review_metadata',
    'approved',
    'reference_repair',
    'migration'
  ) or p_root_revision_checksum !~ '^[a-f0-9]{64}$'
    or pg_catalog.jsonb_typeof(p_items) <> 'array' then
    raise exception using errcode = '22023', message = 'REVIEW_SCOPE_INVALID';
  end if;

  v_version := coalesce((v_root.scope_history->>'current_version')::integer, 0) + 1;
  v_snapshot := pg_catalog.jsonb_build_object(
    'version_no', v_version,
    'scope_basis', p_scope_basis,
    'root_revision_checksum', p_root_revision_checksum,
    'scope_checksum', public.review_scope_checksum_v1(p_items),
    'created_by', p_created_by,
    'created_at', pg_catalog.to_jsonb(pg_catalog.now()),
    'items', p_items
  );

  v_history := case
    when v_root.scope_history is null then pg_catalog.jsonb_build_object(
      'schema_version', 'review_scope.v1',
      'current_version', v_version,
      'snapshots', pg_catalog.jsonb_build_array(v_snapshot)
    )
    else pg_catalog.jsonb_set(
      pg_catalog.jsonb_set(
        v_root.scope_history,
        '{current_version}',
        pg_catalog.to_jsonb(v_version),
        false
      ),
      '{snapshots}',
      (v_root.scope_history->'snapshots') || pg_catalog.jsonb_build_array(v_snapshot),
      false
    )
  end;

  perform public.review_validate_scope_history_v1(p_root_review_id, v_history);

  perform pg_catalog.set_config('app.review_scope_write', 'on', true);
  update public.reviews
  set scope_history = v_history,
      modified_at = pg_catalog.now()
  where id = p_root_review_id;
  perform pg_catalog.set_config('app.review_scope_write', 'off', true);

  return v_history;
exception
  when others then
    perform pg_catalog.set_config('app.review_scope_write', 'off', true);
    raise;
end;
$$;

alter function public.review_append_scope_snapshot_v1(
  uuid, text, text, jsonb, uuid
) owner to postgres;
revoke all on function public.review_append_scope_snapshot_v1(
  uuid, text, text, jsonb, uuid
) from public, anon, authenticated, service_role;

create or replace function private.review_scope_history_guard_v1()
returns trigger
language plpgsql
security definer
set search_path = ''
as $$
begin
  if tg_op = 'DELETE' and old.review_kind in ('root', 'reference') then
    raise exception using errcode = '55000', message = 'REVIEW_HISTORY_DELETE_FORBIDDEN';
  end if;

  if tg_op = 'UPDATE' then
    if old.review_kind is null and new.review_kind is not null then
      -- Reserved for the explicit legacy migration.
      if pg_catalog.current_setting('app.review_legacy_migration', true)
        is distinct from 'on' then
        raise exception using errcode = '55000', message = 'LEGACY_REVIEW_READ_ONLY';
      end if;
    elsif old.review_kind is distinct from new.review_kind
      or old.target_table is distinct from new.target_table
      or old.data_id is distinct from new.data_id
      or old.data_version is distinct from new.data_version
      or old.submitted_revision_checksum
        is distinct from new.submitted_revision_checksum
      or old.target_owner_id is distinct from new.target_owner_id
      or old.target_team_id is distinct from new.target_team_id
      or old.scope_schema_version is distinct from new.scope_schema_version then
      raise exception using errcode = '55000', message = 'REVIEW_IDENTITY_IMMUTABLE';
    end if;

    if old.scope_history is distinct from new.scope_history then
      if pg_catalog.current_setting('app.review_scope_write', true)
        is distinct from 'on' then
        raise exception using errcode = '55000', message = 'REVIEW_SCOPE_DIRECT_WRITE_FORBIDDEN';
      end if;

      perform public.review_validate_scope_history_v1(new.id, new.scope_history);

      if old.scope_history is not null and (
        pg_catalog.jsonb_array_length(new.scope_history->'snapshots')
          <> pg_catalog.jsonb_array_length(old.scope_history->'snapshots') + 1
        or (
          select pg_catalog.jsonb_agg(snapshot.value order by snapshot.ordinality)
          from pg_catalog.jsonb_array_elements(new.scope_history->'snapshots')
            with ordinality as snapshot(value, ordinality)
          where snapshot.ordinality
            <= pg_catalog.jsonb_array_length(old.scope_history->'snapshots')
        ) is distinct from old.scope_history->'snapshots'
      ) then
        raise exception using errcode = '55000', message = 'REVIEW_SCOPE_APPEND_ONLY';
      end if;
    end if;
  end if;

  if tg_op = 'DELETE' then
    return old;
  end if;

  return new;
end;
$$;

alter function private.review_scope_history_guard_v1() owner to postgres;
revoke all on function private.review_scope_history_guard_v1()
  from public, anon, authenticated, service_role;

drop trigger if exists reviews_scope_history_guard_v1 on public.reviews;
create trigger reviews_scope_history_guard_v1
before update or delete on public.reviews
for each row execute function private.review_scope_history_guard_v1();

create or replace function private.review_dataset_content_guard_v1()
returns trigger
language plpgsql
security definer
set search_path = ''
as $$
begin
  if old.state_code in (20, 100)
    and pg_catalog.current_setting('app.review_controlled_write', true)
      is distinct from 'on'
    and (
      to_jsonb(old)
        - array['state_code', 'review_id', 'reviews', 'modified_at']
      is distinct from
      to_jsonb(new)
        - array['state_code', 'review_id', 'reviews', 'modified_at']
    ) then
    raise exception using
      errcode = '55000',
      message = case
        when old.state_code = 100 then 'APPROVED_DATASET_IMMUTABLE'
        else 'DATASET_UNDER_REVIEW_IMMUTABLE'
      end;
  end if;

  return new;
end;
$$;

alter function private.review_dataset_content_guard_v1() owner to postgres;
revoke all on function private.review_dataset_content_guard_v1()
  from public, anon, authenticated, service_role;

do $$
declare
  v_table text;
begin
  foreach v_table in array array[
    'contacts',
    'sources',
    'unitgroups',
    'flowproperties',
    'flows',
    'processes',
    'lifecyclemodels'
  ] loop
    execute format(
      'drop trigger if exists review_dataset_content_guard_v1 on public.%I',
      v_table
    );
    execute format(
      'create trigger review_dataset_content_guard_v1
         before update on public.%I
         for each row execute function private.review_dataset_content_guard_v1()',
      v_table
    );
  end loop;
end;
$$;

create or replace function public.qry_root_review_reference_progress(
  p_root_review_id uuid
)
returns table (
  reference_review_id uuid,
  target_table text,
  data_id uuid,
  data_version text,
  submitted_revision_checksum text,
  state_code integer,
  reviewer_count integer,
  completed_reviewer_count integer,
  relation_paths jsonb
)
language plpgsql
stable
security definer
set search_path = ''
as $$
declare
  v_actor uuid := auth.uid();
begin
  if v_actor is null or not public.cmd_review_is_review_admin(v_actor) then
    raise exception using errcode = '42501', message = 'REVIEW_ADMIN_REQUIRED';
  end if;

  return query
  select
    reference_review.id,
    item.target_table,
    item.data_id,
    item.data_version,
    item.submitted_revision_checksum,
    reference_review.state_code,
    pg_catalog.jsonb_array_length(
      coalesce(reference_review.reviewer_id, '[]'::jsonb)
    )::integer,
    count(comment_row.reviewer_id)
      filter (where comment_row.state_code in (1, -3, 2))::integer,
    coalesce(
      pg_catalog.jsonb_agg(distinct item.relation_path)
        filter (where item.relation_path is not null),
      '[]'::jsonb
    )
  from public.reviews as root_review
  cross join lateral public.review_scope_current_items_v1(
    root_review.scope_history
  ) as item
  join public.reviews as reference_review
    on reference_review.id = item.reference_review_id
    and reference_review.review_kind = 'reference'
  left join public.comments as comment_row
    on comment_row.review_id = reference_review.id
    and comment_row.state_code <> -2
  where root_review.id = p_root_review_id
    and root_review.review_kind = 'root'
    and item.item_kind = 'reference'
  group by
    reference_review.id,
    item.target_table,
    item.data_id,
    item.data_version,
    item.submitted_revision_checksum,
    reference_review.state_code,
    reference_review.reviewer_id
  order by item.target_table, item.data_id, item.data_version;
end;
$$;

alter function public.qry_root_review_reference_progress(uuid) owner to postgres;
revoke all on function public.qry_root_review_reference_progress(uuid)
  from public, anon;
grant execute on function public.qry_root_review_reference_progress(uuid)
  to authenticated, service_role;

create or replace function public.qry_reference_review_impacted_roots(
  p_reference_review_id uuid,
  p_include_history boolean default false
)
returns table (
  root_review_id uuid,
  target_table text,
  data_id uuid,
  data_version text,
  state_code integer,
  is_current boolean
)
language plpgsql
stable
security definer
set search_path = ''
as $$
declare
  v_actor uuid := auth.uid();
begin
  if v_actor is null or not public.cmd_review_is_review_admin(v_actor) then
    raise exception using errcode = '42501', message = 'REVIEW_ADMIN_REQUIRED';
  end if;

  return query
  select
    root_review.id,
    root_review.target_table,
    root_review.data_id,
    btrim(root_review.data_version::text),
    root_review.state_code,
    root_review.current_reference_review_ids
      @> array[p_reference_review_id]::uuid[]
  from public.reviews as root_review
  where root_review.review_kind = 'root'
    and (
      root_review.current_reference_review_ids
        @> array[p_reference_review_id]::uuid[]
      or (
        coalesce(p_include_history, false)
        and root_review.all_reference_review_ids
          @> array[p_reference_review_id]::uuid[]
      )
    )
  order by root_review.modified_at desc, root_review.id;
end;
$$;

alter function public.qry_reference_review_impacted_roots(uuid, boolean)
  owner to postgres;
revoke all on function public.qry_reference_review_impacted_roots(uuid, boolean)
  from public, anon;
grant execute on function public.qry_reference_review_impacted_roots(uuid, boolean)
  to authenticated, service_role;

comment on index public.reviews_reference_revision_active_uidx is
  'One reusable Reference Review per exact table/id/version/revision while pending, assigned, or approved.';
comment on index public.reviews_root_current_reference_ids_gin_idx is
  'Supports current Reference Review to Root Review reverse lookup without indexing historical scope.';
