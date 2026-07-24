-- Initial data-product closure preflight must be possible before the first
-- formal LCA release exists.  The deployed closure Worker still consumes the
-- v2 release-shaped snapshot contract, so the database freezes an explicit
-- candidate-public-state snapshot and provides a deterministic compatibility
-- projection in currentPublicRelease.  candidateData is the authoritative
-- source discriminator; the zero UUIDs are never publication identities.

create or replace function private.lcia_scope_closure_worker_canonical_json_text(
  p_value jsonb
) returns text
language plpgsql
immutable
strict
parallel safe
set search_path = ''
as $$
declare
  v_result text;
begin
  case jsonb_typeof(p_value)
    when 'object' then
      select '{' || coalesce(string_agg(
        to_jsonb(item.key)::text
          || ':'
          || private.lcia_scope_closure_worker_canonical_json_text(item.value),
        ',' order by item.key collate "C"
      ), '') || '}'
      into v_result
      from jsonb_each(p_value) as item(key, value);
    when 'array' then
      select '[' || coalesce(string_agg(
        private.lcia_scope_closure_worker_canonical_json_text(item.value),
        ',' order by item.ordinality
      ), '') || ']'
      into v_result
      from jsonb_array_elements(p_value)
        with ordinality as item(value, ordinality);
    else
      v_result := p_value::text;
  end case;

  return v_result;
end;
$$;

alter function private.lcia_scope_closure_worker_canonical_json_text(jsonb)
  owner to postgres;
revoke all on function private.lcia_scope_closure_worker_canonical_json_text(jsonb)
  from public, anon, authenticated, service_role;

comment on function private.lcia_scope_closure_worker_canonical_json_text(jsonb) is
  'Serializes JSON exactly as the scope-closure Worker canonical JSON writer: recursively sorted object keys, preserved array order, and compact separators.';

create or replace function private.lcia_scope_closure_worker_canonical_sha256(
  p_value jsonb
) returns text
language sql
immutable
strict
parallel safe
set search_path = ''
as $$
  select encode(
    extensions.digest(
      convert_to(
        private.lcia_scope_closure_worker_canonical_json_text(p_value),
        'UTF8'
      ),
      'sha256'
    ),
    'hex'
  )
$$;

alter function private.lcia_scope_closure_worker_canonical_sha256(jsonb)
  owner to postgres;
revoke all on function private.lcia_scope_closure_worker_canonical_sha256(jsonb)
  from public, anon, authenticated, service_role;

comment on function private.lcia_scope_closure_worker_canonical_sha256(jsonb) is
  'Computes the canonicalContentHash checked by the deployed scope-closure Worker.';

create or replace function private.lcia_scope_closure_lcia_method_identity(
  p_locator_id uuid,
  p_version text,
  p_document jsonb
) returns uuid
language sql
immutable
set search_path = ''
as $$
  select case
    -- The reviewed LCIA bundle contains one canonical-method/artifact-locator
    -- alias.  Keep this mapping identical to Worker RELEASE_METHOD_IDENTITIES.
    when p_locator_id = '9ec743ea-6b00-400d-a53b-61547a3fc03c'::uuid
      and btrim(p_version) = '01.01.000'
      and p_document #>> '{LCIAMethodDataSet,LCIAMethodInformation,dataSetInformation,common:UUID}'
        = '503699e0-eca9-4089-8bf8-e0f49c93e578'
    then '503699e0-eca9-4089-8bf8-e0f49c93e578'::uuid
    else p_locator_id
  end
$$;

alter function private.lcia_scope_closure_lcia_method_identity(uuid, text, jsonb)
  owner to postgres;
revoke all on function private.lcia_scope_closure_lcia_method_identity(uuid, text, jsonb)
  from public, anon, authenticated, service_role;

comment on function private.lcia_scope_closure_lcia_method_identity(uuid, text, jsonb) is
  'Maps the reviewed LCIA artifact locator to the canonical method identity used by the closure Worker.';

create table if not exists private.lcia_scope_closure_reviewed_lcia_methods (
  method_id uuid not null,
  method_version text not null,
  artifact_locator_id uuid not null,
  primary key (method_id, method_version),
  unique (artifact_locator_id, method_version)
);

alter table private.lcia_scope_closure_reviewed_lcia_methods owner to postgres;
revoke all on private.lcia_scope_closure_reviewed_lcia_methods
  from public, anon, authenticated, service_role;

insert into private.lcia_scope_closure_reviewed_lcia_methods(
  method_id,
  method_version,
  artifact_locator_id
) values
  ('01500b74-7ffb-463e-9bd4-72f17c2263ff','01.00.000','01500b74-7ffb-463e-9bd4-72f17c2263ff'),
  ('05316e7a-b254-4bea-9cf0-6bf33eb5c630','01.00.000','05316e7a-b254-4bea-9cf0-6bf33eb5c630'),
  ('14af9ca7-aa1d-4832-b1d9-ab05a06dcb12','01.00.000','14af9ca7-aa1d-4832-b1d9-ab05a06dcb12'),
  ('2299222a-bbd8-474f-9d4f-4dd1f18aea7c','01.01.000','2299222a-bbd8-474f-9d4f-4dd1f18aea7c'),
  ('503699e0-eca9-4089-8bf8-e0f49c93e578','01.01.000','9ec743ea-6b00-400d-a53b-61547a3fc03c'),
  ('6209b35f-9447-40b5-b68c-a1099e3674a0','01.00.000','6209b35f-9447-40b5-b68c-a1099e3674a0'),
  ('706261af-a357-4cc0-a50a-f3033fcbd556','01.00.000','706261af-a357-4cc0-a50a-f3033fcbd556'),
  ('7cfdcfcf-b222-4b26-888a-a55f9fbf7ac8','01.00.000','7cfdcfcf-b222-4b26-888a-a55f9fbf7ac8'),
  ('7fce5b3a-66b8-4ce1-91e8-a925aee1f186','01.00.000','7fce5b3a-66b8-4ce1-91e8-a925aee1f186'),
  ('8c3141e9-1f15-43b5-bff2-182e49893a46','01.00.000','8c3141e9-1f15-43b5-bff2-182e49893a46'),
  ('9d1d43a2-e1aa-4c16-acd4-3dd8a6a2fb85','01.00.000','9d1d43a2-e1aa-4c16-acd4-3dd8a6a2fb85'),
  ('b2ad6110-c78d-11e6-9d9d-cec0c932ce01','01.00.010','b2ad6110-c78d-11e6-9d9d-cec0c932ce01'),
  ('b2ad6494-c78d-11e6-9d9d-cec0c932ce01','01.00.010','b2ad6494-c78d-11e6-9d9d-cec0c932ce01'),
  ('b2ad66ce-c78d-11e6-9d9d-cec0c932ce01','03.00.014','b2ad66ce-c78d-11e6-9d9d-cec0c932ce01'),
  ('b2ad6890-c78d-11e6-9d9d-cec0c932ce01','01.00.010','b2ad6890-c78d-11e6-9d9d-cec0c932ce01'),
  ('b53ec18f-7377-4ad3-86eb-cc3f4f276b2b','01.00.010','b53ec18f-7377-4ad3-86eb-cc3f4f276b2b'),
  ('b5c602c6-def3-11e6-bf01-fe55135034f3','02.00.011','b5c602c6-def3-11e6-bf01-fe55135034f3'),
  ('b5c610fe-def3-11e6-bf01-fe55135034f3','02.01.000','b5c610fe-def3-11e6-bf01-fe55135034f3'),
  ('b5c611c6-def3-11e6-bf01-fe55135034f3','01.04.000','b5c611c6-def3-11e6-bf01-fe55135034f3'),
  ('b5c614d2-def3-11e6-bf01-fe55135034f3','01.02.009','b5c614d2-def3-11e6-bf01-fe55135034f3'),
  ('b5c619fa-def3-11e6-bf01-fe55135034f3','02.00.010','b5c619fa-def3-11e6-bf01-fe55135034f3'),
  ('b5c629d6-def3-11e6-bf01-fe55135034f3','02.00.012','b5c629d6-def3-11e6-bf01-fe55135034f3'),
  ('b5c632be-def3-11e6-bf01-fe55135034f3','01.00.011','b5c632be-def3-11e6-bf01-fe55135034f3'),
  ('dacd48b5-4da5-49aa-aff4-cd5f5495c037','01.00.000','dacd48b5-4da5-49aa-aff4-cd5f5495c037'),
  ('fd530f00-9325-424a-92ef-aaac67922fd9','01.00.000','fd530f00-9325-424a-92ef-aaac67922fd9')
on conflict (method_id, method_version) do update
set artifact_locator_id = excluded.artifact_locator_id;

comment on table private.lcia_scope_closure_reviewed_lcia_methods is
  'Database copy of the deployed Worker RELEASE_METHOD_IDENTITIES allowlist used only to freeze compatible first-release candidate snapshots.';

create table if not exists private.lcia_scope_closure_candidate_document_hashes (
  dataset_type text not null,
  dataset_id uuid not null,
  dataset_version text not null,
  source_locator_id uuid not null,
  role text not null,
  canonical_content_hash text not null,
  source_modified_at timestamptz,
  refreshed_at timestamptz not null default now(),
  primary key (dataset_type, dataset_id, dataset_version),
  unique (dataset_type, source_locator_id, dataset_version),
  check (dataset_type in (
    'contacts',
    'flowproperties',
    'flows',
    'lciamethods',
    'lifecyclemodels',
    'processes',
    'sources',
    'unitgroups'
  )),
  check (role in ('unit_process', 'support')),
  check (canonical_content_hash ~ '^[0-9a-f]{64}$')
);

alter table private.lcia_scope_closure_candidate_document_hashes
  owner to postgres;
revoke all on private.lcia_scope_closure_candidate_document_hashes
  from public, anon, authenticated, service_role;

comment on table private.lcia_scope_closure_candidate_document_hashes is
  'Incremental Worker-canonical content hashes for candidate-public-state closure snapshots. Backfilled once by migration and maintained by table triggers.';

create or replace function private.lcia_scope_closure_refresh_candidate_document_hash()
returns trigger
language plpgsql
security definer
set search_path = ''
as $$
declare
  v_document jsonb;
  v_dataset_id uuid;
  v_role text;
  v_is_eligible boolean;
begin
  if tg_op <> 'INSERT' then
    delete from private.lcia_scope_closure_candidate_document_hashes
    where dataset_type = tg_table_name
      and source_locator_id = old.id
      and dataset_version = btrim(old.version::text);
  end if;

  if tg_op = 'DELETE' then
    return old;
  end if;

  if tg_table_name = 'lciamethods' then
    -- LCIA methods are the separately reviewed 25-method static bundle. Their
    -- authoring lifecycle state_code remains 0 in production and is not the
    -- candidate-public-data eligibility predicate used by other datasets.
    v_document := coalesce(new.json, new.json_ordered::jsonb);
    v_is_eligible := v_document is not null;
    v_dataset_id := private.lcia_scope_closure_lcia_method_identity(
      new.id,
      btrim(new.version::text),
      v_document
    );
    v_is_eligible := v_is_eligible and exists (
      select 1
      from private.lcia_scope_closure_reviewed_lcia_methods reviewed
      where reviewed.method_id = v_dataset_id
        and reviewed.method_version = btrim(new.version::text)
        and reviewed.artifact_locator_id = new.id
    );
    v_role := 'support';
  else
    v_document := new.json_ordered::jsonb;
    v_is_eligible :=
      new.state_code between 100 and 199 and v_document is not null;
    v_dataset_id := new.id;
    v_role := case
      when tg_table_name = 'processes' then 'unit_process'
      else 'support'
    end;
  end if;

  if v_is_eligible then
    insert into private.lcia_scope_closure_candidate_document_hashes(
      dataset_type,
      dataset_id,
      dataset_version,
      source_locator_id,
      role,
      canonical_content_hash,
      source_modified_at,
      refreshed_at
    ) values (
      tg_table_name,
      v_dataset_id,
      btrim(new.version::text),
      new.id,
      v_role,
      private.lcia_scope_closure_worker_canonical_sha256(v_document),
      new.modified_at,
      now()
    )
    on conflict (dataset_type, dataset_id, dataset_version)
    do update set
      source_locator_id = excluded.source_locator_id,
      role = excluded.role,
      canonical_content_hash = excluded.canonical_content_hash,
      source_modified_at = excluded.source_modified_at,
      refreshed_at = excluded.refreshed_at;
  end if;

  return new;
end;
$$;

alter function private.lcia_scope_closure_refresh_candidate_document_hash()
  owner to postgres;
revoke all on function private.lcia_scope_closure_refresh_candidate_document_hash()
  from public, anon, authenticated, service_role;

comment on function private.lcia_scope_closure_refresh_candidate_document_hash() is
  'Maintains the candidate closure hash cache whenever an exact source document or its eligibility changes.';

do $$
declare
  v_table text;
begin
  foreach v_table in array array[
    'contacts',
    'flowproperties',
    'flows',
    'lciamethods',
    'lifecyclemodels',
    'processes',
    'sources',
    'unitgroups'
  ]
  loop
    execute format(
      'drop trigger if exists lcia_scope_closure_candidate_hash_refresh on public.%I',
      v_table
    );
    execute format(
      'create trigger lcia_scope_closure_candidate_hash_refresh
       after insert or delete or update of id, version, state_code, json, json_ordered
       on public.%I
       for each row execute function private.lcia_scope_closure_refresh_candidate_document_hash()',
      v_table
    );
  end loop;
end;
$$;

-- The one-time backfill is deliberately paid during migration rather than on
-- an interactive completeness-check request. Production currently has more
-- than 100k eligible Flow rows; subsequent writes refresh only the changed row.
insert into private.lcia_scope_closure_candidate_document_hashes(
  dataset_type,
  dataset_id,
  dataset_version,
  source_locator_id,
  role,
  canonical_content_hash,
  source_modified_at
)
with candidate_documents as (
  select 'contacts'::text as dataset_type, c.id as dataset_id,
    btrim(c.version::text) as dataset_version, c.id as source_locator_id,
    'support'::text as role, c.json_ordered::jsonb as document,
    c.modified_at as source_modified_at
  from public.contacts c
  where c.state_code between 100 and 199 and c.json_ordered is not null
  union all
  select 'flowproperties', f.id, btrim(f.version::text), f.id, 'support',
    f.json_ordered::jsonb, f.modified_at
  from public.flowproperties f
  where f.state_code between 100 and 199 and f.json_ordered is not null
  union all
  select 'flows', f.id, btrim(f.version::text), f.id, 'support',
    f.json_ordered::jsonb, f.modified_at
  from public.flows f
  where f.state_code between 100 and 199 and f.json_ordered is not null
  union all
  select 'lifecyclemodels', m.id, btrim(m.version::text), m.id, 'support',
    m.json_ordered::jsonb, m.modified_at
  from public.lifecyclemodels m
  where m.state_code between 100 and 199 and m.json_ordered is not null
  union all
  select 'processes', p.id, btrim(p.version::text), p.id, 'unit_process',
    p.json_ordered::jsonb, p.modified_at
  from public.processes p
  where p.state_code between 100 and 199 and p.json_ordered is not null
  union all
  select 'sources', s.id, btrim(s.version::text), s.id, 'support',
    s.json_ordered::jsonb, s.modified_at
  from public.sources s
  where s.state_code between 100 and 199 and s.json_ordered is not null
  union all
  select 'unitgroups', u.id, btrim(u.version::text), u.id, 'support',
    u.json_ordered::jsonb, u.modified_at
  from public.unitgroups u
  where u.state_code between 100 and 199 and u.json_ordered is not null
  union all
  select 'lciamethods',
    method.method_id,
    method.dataset_version,
    method.locator_id,
    'support',
    method.document,
    method.modified_at
  from (
    select reviewed.method_id, m.id as locator_id,
      btrim(m.version::text) as dataset_version,
      coalesce(m.json, m.json_ordered::jsonb) as document,
      m.modified_at
    from public.lciamethods m
    join private.lcia_scope_closure_reviewed_lcia_methods reviewed
      on reviewed.artifact_locator_id = m.id
     and reviewed.method_version = btrim(m.version::text)
    where coalesce(m.json, m.json_ordered::jsonb) is not null
  ) method
),
unique_documents as (
  select distinct on (dataset_type, dataset_id, dataset_version)
    dataset_type,
    dataset_id,
    dataset_version,
    source_locator_id,
    role,
    document,
    source_modified_at
  from candidate_documents
  order by dataset_type, dataset_id, dataset_version,
    case
      when source_locator_id = '9ec743ea-6b00-400d-a53b-61547a3fc03c'::uuid
        then 0
      else 1
    end
)
select dataset_type,
  dataset_id,
  dataset_version,
  source_locator_id,
  role,
  private.lcia_scope_closure_worker_canonical_sha256(document),
  source_modified_at
from unique_documents
on conflict (dataset_type, dataset_id, dataset_version)
do update set
  source_locator_id = excluded.source_locator_id,
  role = excluded.role,
  canonical_content_hash = excluded.canonical_content_hash,
  source_modified_at = excluded.source_modified_at,
  refreshed_at = now();

create or replace function private.lcia_scope_closure_candidate_dataset_manifest()
returns jsonb
language sql
stable
security definer
set search_path = ''
as $$
  select coalesce(jsonb_agg(
    jsonb_build_object(
      'datasetType', dataset_type,
      'datasetId', dataset_id,
      'datasetVersion', dataset_version,
      'role', role,
      -- Worker v2 requires all three hashes.  Candidate snapshots define each
      -- compatibility field over the exact frozen full document.
      'versionSignificantHash', canonical_content_hash,
      'semanticHash', canonical_content_hash,
      'canonicalContentHash', canonical_content_hash
    )
    order by dataset_type, dataset_id, dataset_version, role
  ), '[]'::jsonb)
  from private.lcia_scope_closure_candidate_document_hashes
$$;

alter function private.lcia_scope_closure_candidate_dataset_manifest()
  owner to postgres;
revoke all on function private.lcia_scope_closure_candidate_dataset_manifest()
  from public, anon, authenticated, service_role;

comment on function private.lcia_scope_closure_candidate_dataset_manifest() is
  'Freezes every exact state_code 100..199 document readable by the deployed closure Worker for first-release candidate preflight.';

create or replace function public.lcia_scope_closure_normalize_request(
  p_requested_scope jsonb
) returns jsonb
language plpgsql
security definer
set search_path = public, pg_temp
as $$
declare
  v_mode text := lower(trim(coalesce(p_requested_scope->>'coverageMode', '')));
  v_processes jsonb;
  v_methods jsonb;
  v_policy jsonb;
  v_freshness text;
  v_release_id uuid;
  v_count integer;
  v_requested integer;
  v_duplicate integer;
  v_predicate text;
begin
  if jsonb_typeof(coalesce(p_requested_scope, 'null'::jsonb)) <> 'object'
     or v_mode not in ('subset', 'global_eligible') then
    raise exception using errcode = '22023', message = 'invalid_closure_scope';
  end if;
  if jsonb_typeof(coalesce(p_requested_scope->'processes', '[]'::jsonb)) <> 'array'
     or jsonb_typeof(coalesce(p_requested_scope->'lciaMethods', '[]'::jsonb)) <> 'array' then
    raise exception using errcode = '22023', message = 'invalid_closure_scope_identity_list';
  end if;

  select release_run_id
  into v_release_id
  from public.lca_release_publications
  where is_current = true and status = 'current'
  order by published_at desc
  limit 1;

  if v_release_id is not null then
    v_predicate := 'current-public-release-manifest:v2';
    if v_mode = 'global_eligible' then
      if jsonb_array_length(coalesce(p_requested_scope->'processes', '[]'::jsonb)) <> 0 then
        raise exception using errcode = '22023', message = 'global_eligible_scope_must_not_supply_processes';
      end if;
      select coalesce(jsonb_agg(
        jsonb_build_object('id', dataset_uuid, 'version', dataset_version)
        order by dataset_uuid, dataset_version
      ), '[]'::jsonb)
      into v_processes
      from public.lca_release_dataset_versions
      where release_run_id = v_release_id
        and dataset_type = 'process'
        and dataset_role = 'unit_process';
      if jsonb_array_length(v_processes) = 0 then
        raise exception using errcode = '22023', message = 'current_release_has_no_eligible_processes';
      end if;
    else
      with requested as (
        select (item.value->>'id')::uuid as id,
          btrim(item.value->>'version') as version
        from jsonb_array_elements(p_requested_scope->'processes') item(value)
      ),
      resolved as (
        select r.id, r.version
        from requested r
        join public.lca_release_dataset_versions d
          on d.release_run_id = v_release_id
         and d.dataset_type = 'process'
         and d.dataset_role = 'unit_process'
         and d.dataset_uuid = r.id
         and d.dataset_version = r.version
      )
      select count(*), (select count(*) from requested),
        (select count(*) - count(distinct (id, version)) from requested),
        coalesce(jsonb_agg(
          jsonb_build_object('id', id, 'version', version) order by id, version
        ), '[]'::jsonb)
      into v_count, v_requested, v_duplicate, v_processes
      from resolved;
      if coalesce(v_requested, 0) = 0 or v_count <> v_requested or v_duplicate <> 0 then
        raise exception using errcode = '22023', message = 'process_not_in_current_public_release';
      end if;
    end if;

    with requested as (
      select (item.value->>'id')::uuid as id,
        btrim(item.value->>'version') as version
      from jsonb_array_elements(p_requested_scope->'lciaMethods') item(value)
    ),
    resolved as (
      select r.id, r.version
      from requested r
      join public.lca_release_dataset_versions d
        on d.release_run_id = v_release_id
       and d.dataset_type = 'lciamethod'
       and d.dataset_uuid = r.id
       and d.dataset_version = r.version
    )
    select count(*), (select count(*) from requested),
      (select count(*) - count(distinct (id, version)) from requested),
      coalesce(jsonb_agg(
        jsonb_build_object('id', id, 'version', version) order by id, version
      ), '[]'::jsonb)
    into v_count, v_requested, v_duplicate, v_methods
    from resolved;
    if coalesce(v_requested, 0) = 0 or v_count <> v_requested or v_duplicate <> 0 then
      raise exception using errcode = '22023', message = 'lcia_method_not_in_current_public_release';
    end if;
  else
    v_predicate := 'candidate-public-state-code-100-199:v1';
    if v_mode = 'global_eligible' then
      if jsonb_array_length(coalesce(p_requested_scope->'processes', '[]'::jsonb)) <> 0 then
        raise exception using errcode = '22023', message = 'global_eligible_scope_must_not_supply_processes';
      end if;
      with ranked as (
        select p.id, btrim(p.version::text) as version,
          row_number() over (
            partition by p.id
            order by btrim(p.version::text) desc, p.modified_at desc nulls last
          ) as rank
        from public.processes p
        where p.state_code between 100 and 199
          and p.json_ordered is not null
      )
      select coalesce(jsonb_agg(
        jsonb_build_object('id', id, 'version', version) order by id, version
      ), '[]'::jsonb)
      into v_processes
      from ranked
      where rank = 1;
      if jsonb_array_length(v_processes) = 0 then
        raise exception using errcode = '22023', message = 'candidate_scope_has_no_eligible_processes';
      end if;
    else
      with requested as (
        select (item.value->>'id')::uuid as id,
          btrim(item.value->>'version') as version
        from jsonb_array_elements(p_requested_scope->'processes') item(value)
      ),
      resolved as (
        select r.id, r.version
        from requested r
        join public.processes p
          on p.id = r.id
         and btrim(p.version::text) = r.version
         and p.state_code between 100 and 199
         and p.json_ordered is not null
      )
      select count(*), (select count(*) from requested),
        (select count(*) - count(distinct (id, version)) from requested),
        coalesce(jsonb_agg(
          jsonb_build_object('id', id, 'version', version) order by id, version
        ), '[]'::jsonb)
      into v_count, v_requested, v_duplicate, v_processes
      from resolved;
      if coalesce(v_requested, 0) = 0 or v_count <> v_requested or v_duplicate <> 0 then
        raise exception using errcode = '22023', message = 'invalid_or_ineligible_process_selection';
      end if;
    end if;

    with requested as (
      select (item.value->>'id')::uuid as id,
        btrim(item.value->>'version') as version
      from jsonb_array_elements(p_requested_scope->'lciaMethods') item(value)
    ),
    eligible_methods as (
      select reviewed.method_id as id,
        reviewed.method_version as version
      from public.lciamethods m
      join private.lcia_scope_closure_reviewed_lcia_methods reviewed
        on reviewed.artifact_locator_id = m.id
       and reviewed.method_version = btrim(m.version::text)
      where coalesce(m.json, m.json_ordered::jsonb) is not null
    ),
    resolved as (
      select r.id, r.version
      from requested r
      join eligible_methods m using (id, version)
    )
    select count(*), (select count(*) from requested),
      (select count(*) - count(distinct (id, version)) from requested),
      coalesce(jsonb_agg(
        jsonb_build_object('id', id, 'version', version) order by id, version
      ), '[]'::jsonb)
    into v_count, v_requested, v_duplicate, v_methods
    from resolved;
    if coalesce(v_requested, 0) = 0 or v_count <> v_requested or v_duplicate <> 0 then
      raise exception using errcode = '22023', message = 'invalid_lcia_method_selection';
    end if;
  end if;

  v_freshness := coalesce(
    nullif(trim(p_requested_scope->>'certificateFreshnessPolicy'), ''),
    'frozen-artifact-reusable-v1'
  );
  if v_freshness not in (
    'frozen-artifact-reusable-v1',
    'current-membership-required-v1'
  ) then
    raise exception using errcode = '22023', message = 'invalid_certificate_freshness_policy';
  end if;

  v_policy := coalesce(p_requested_scope->'linkPolicy', '{}'::jsonb);
  if jsonb_typeof(v_policy) <> 'object'
     or coalesce(v_policy->>'linkSemanticsVersion', 'signed-flow-balance-v1') <> 'signed-flow-balance-v1'
     or coalesce(v_policy->>'flowIdentityPolicy', 'exact-flow-version-reference-unit-v2') <> 'exact-flow-version-reference-unit-v2'
     or coalesce(v_policy->>'allocationSemanticsVersion', 'tidas-reference-allocation-v3') <> 'tidas-reference-allocation-v3'
     or coalesce(v_policy->>'technosphereBoundaryPolicy', 'closed') not in ('closed', 'open', 'cutoff')
     or coalesce(v_policy->>'providerUniversePolicy', 'scope_only') not in ('scope_only', 'eligible_transitive_expansion-v1') then
    raise exception using errcode = '22023', message = 'invalid_closure_link_policy';
  end if;

  return jsonb_build_object(
    'schemaVersion', 'lcia.scope-manifest.v1',
    'coverageMode', v_mode,
    'eligibilityPredicateVersion', v_predicate,
    'processes', v_processes,
    'lciaMethods', v_methods,
    'versionResolutionPolicy', 'reference-version-resolution-v1',
    'legacyOmittedVersionPolicy', 'reject',
    'certificateFreshnessPolicy', v_freshness,
    'linkPolicy', jsonb_build_object(
      'linkSemanticsVersion', 'signed-flow-balance-v1',
      'flowIdentityPolicy', 'exact-flow-version-reference-unit-v2',
      'allocationSemanticsVersion', 'tidas-reference-allocation-v3',
      'technosphereBoundaryPolicy',
        coalesce(v_policy->>'technosphereBoundaryPolicy', 'closed'),
      'providerUniversePolicy',
        coalesce(v_policy->>'providerUniversePolicy', 'scope_only')
    ),
    'processManifestHash',
      public.lcia_scope_closure_sha256(jsonb_build_object('processes', v_processes))
  );
exception
  when invalid_text_representation then
    raise exception using errcode = '22023', message = 'invalid_scope_identity';
end;
$$;

comment on function public.lcia_scope_closure_normalize_request(jsonb) is
  'Normalizes closure roots against the current formal release when present, otherwise against exact eligible candidate-public-state documents.';

create or replace function public.cmd_lcia_scope_closure_check_request_v2(
  p_requested_scope jsonb,
  p_request_idempotency_token text,
  p_audit jsonb default '{}'::jsonb
) returns jsonb
language plpgsql
security definer
set search_path = public, pg_temp
as $$
declare
  v_actor uuid := auth.uid();
  v_scope jsonb;
  v_policy jsonb;
  v_requested_scope_hash text;
  v_policy_fingerprint text;
  v_expected_validator text;
  v_snapshot_manifest jsonb;
  v_snapshot_token text;
  v_request_fingerprint text;
  v_request_key text;
  v_result jsonb;
  v_check public.lcia_scope_closure_checks%rowtype;
  v_job public.worker_jobs%rowtype;
  v_execution public.lcia_scope_closure_scan_executions%rowtype;
  v_publication public.lca_release_publications%rowtype;
  v_run public.lca_release_runs%rowtype;
  v_dataset_manifest jsonb;
  v_candidate_manifest_hash text;
  v_publication_epoch bigint := 0;
  v_missing_root_count integer;
begin
  if v_actor is null then
    return public.lcia_scope_closure_error(
      'auth_required', 401, 'Authentication required'
    );
  end if;
  if not public.lcia_scope_closure_is_manager() then
    return public.lcia_scope_closure_error(
      'not_data_product_manager', 403, 'Data product manager role is required'
    );
  end if;
  if coalesce(nullif(trim(p_request_idempotency_token), ''), '') = '' then
    return public.lcia_scope_closure_error(
      'invalid_closure_request', 400, 'Idempotency token is required'
    );
  end if;

  v_scope := public.lcia_scope_closure_normalize_request(p_requested_scope);

  if v_scope->>'eligibilityPredicateVersion' = 'current-public-release-manifest:v2' then
    select *
    into v_publication
    from public.lca_release_publications
    where is_current = true and status = 'current'
    order by published_at desc
    limit 1;

    if v_publication.id is null then
      return public.lcia_scope_closure_error(
        'closure_snapshot_source_changed',
        409,
        'The current release changed while the closure snapshot was being frozen; retry the check'
      );
    end if;
    select *
    into v_run
    from public.lca_release_runs
    where id = v_publication.release_run_id;
    if v_run.id is null or v_run.release_manifest_hash is null then
      return public.lcia_scope_closure_error(
        'closure_evidence_unavailable',
        503,
        'Current public release manifest is unavailable'
      );
    end if;
    select coalesce(jsonb_agg(
      jsonb_build_object(
        'datasetType', d.dataset_type,
        'datasetId', d.dataset_uuid,
        'datasetVersion', d.dataset_version,
        'role', d.dataset_role,
        'sourceProcessId', d.source_process_uuid,
        'sourceProcessVersion', d.source_process_version,
        'versionSignificantHash', d.version_significant_hash,
        'semanticHash', d.semantic_hash,
        'canonicalContentHash', d.canonical_content_hash
      )
      order by d.dataset_type, d.dataset_uuid, d.dataset_version, d.dataset_role
    ), '[]'::jsonb)
    into v_dataset_manifest
    from public.lca_release_dataset_versions d
    where d.release_run_id = v_run.id;

    if jsonb_array_length(v_dataset_manifest) = 0 then
      return public.lcia_scope_closure_error(
        'closure_evidence_unavailable',
        503,
        'Current public release dataset manifest is empty'
      );
    end if;

    v_snapshot_manifest := jsonb_build_object(
      'schemaVersion', 'lcia.scope-closure-data-snapshot.v2',
      'requestedScope', v_scope,
      'currentPublicRelease', jsonb_build_object(
        'publicationId', v_publication.id,
        'releaseRunId', v_run.id,
        'releaseVersion', v_run.release_version,
        'publishedAt', v_publication.published_at,
        'releaseManifestHash', v_run.release_manifest_hash
      ),
      'datasets', v_dataset_manifest
    );
    v_publication_epoch := extract(epoch from v_publication.published_at)::bigint;
  else
    if v_scope->>'certificateFreshnessPolicy' = 'current-membership-required-v1' then
      return public.lcia_scope_closure_error(
        'current_release_required',
        409,
        'Current-membership freshness requires a current formal release'
      );
    end if;

    v_dataset_manifest :=
      private.lcia_scope_closure_candidate_dataset_manifest();
    if jsonb_array_length(v_dataset_manifest) = 0 then
      return public.lcia_scope_closure_error(
        'closure_evidence_unavailable',
        503,
        'Candidate public-state dataset manifest is empty'
      );
    end if;

    with roots as (
      select 'processes'::text as dataset_type,
        (item.value->>'id')::uuid as dataset_id,
        item.value->>'version' as dataset_version
      from jsonb_array_elements(v_scope->'processes') item(value)
      union all
      select 'lciamethods',
        (item.value->>'id')::uuid,
        item.value->>'version'
      from jsonb_array_elements(v_scope->'lciaMethods') item(value)
    ),
    frozen as (
      select item.value->>'datasetType' as dataset_type,
        (item.value->>'datasetId')::uuid as dataset_id,
        item.value->>'datasetVersion' as dataset_version
      from jsonb_array_elements(v_dataset_manifest) item(value)
    )
    select count(*)
    into v_missing_root_count
    from roots r
    left join frozen f using (dataset_type, dataset_id, dataset_version)
    where f.dataset_id is null;

    if v_missing_root_count <> 0 then
      return public.lcia_scope_closure_error(
        'closure_snapshot_source_changed',
        409,
        'Candidate data changed while the closure snapshot was being frozen; retry the check'
      );
    end if;

    v_candidate_manifest_hash := public.lcia_scope_closure_sha256(
      jsonb_build_object(
        'eligibilityPredicateVersion',
          v_scope->>'eligibilityPredicateVersion',
        'datasets', v_dataset_manifest
      )
    );
    v_snapshot_manifest := jsonb_build_object(
      'schemaVersion', 'lcia.scope-closure-data-snapshot.v2',
      'requestedScope', v_scope,
      -- Required only for deployed Worker v2 compatibility.  candidateData
      -- below is the authoritative source description.
      'currentPublicRelease', jsonb_build_object(
        'publicationId', '00000000-0000-0000-0000-000000000000',
        'releaseRunId', '00000000-0000-0000-0000-000000000000',
        'releaseVersion', 'candidate-public-state-v1',
        'publishedAt', '1970-01-01T00:00:00Z',
        'releaseManifestHash', v_candidate_manifest_hash
      ),
      'candidateData', jsonb_build_object(
        'sourceKind', 'candidate-public-state',
        'eligibilityPredicateVersion',
          v_scope->>'eligibilityPredicateVersion',
        'datasetManifestHash', v_candidate_manifest_hash,
        'workerV2CompatibilityProjection', true
      ),
      'datasets', v_dataset_manifest
    );
  end if;

  v_snapshot_token := public.lcia_scope_closure_sha256(v_snapshot_manifest);
  v_requested_scope_hash := public.lcia_scope_closure_sha256(v_scope);
  v_policy := jsonb_build_object(
    'scopePolicy', v_scope - 'processes' - 'lciaMethods' - 'processManifestHash',
    'visibilityScope', 'data_product_manager.v1'
  );
  v_policy_fingerprint := public.lcia_scope_closure_sha256(v_policy);
  select expected_validator_scanner_fingerprint
  into v_expected_validator
  from public.lcia_scope_closure_config
  where singleton;
  if v_expected_validator is null then
    return public.lcia_scope_closure_error(
      'closure_evidence_unavailable',
      503,
      'Closure validator configuration is unavailable'
    );
  end if;

  v_request_fingerprint := encode(extensions.digest(
    v_requested_scope_hash || '|' || v_policy_fingerprint || '|'
      || v_expected_validator || '|' || v_snapshot_token,
    'sha256'
  ), 'hex');
  v_request_key := encode(extensions.digest(
    v_actor::text || '|' || trim(p_request_idempotency_token) || '|'
      || v_request_fingerprint,
    'sha256'
  ), 'hex');

  select *
  into v_check
  from public.lcia_scope_closure_checks
  where requested_by = v_actor and request_key = v_request_key
  for update;
  if v_check.id is not null then
    select * into v_job from public.worker_jobs where id = v_check.worker_job_id;
    return jsonb_build_object(
      'ok', true,
      'data', jsonb_build_object(
        'closureCheckId', v_check.id,
        'requestedScopeHash', v_check.requested_scope_hash,
        'policyFingerprint', v_check.policy_fingerprint,
        'dataSnapshotToken', v_check.data_snapshot_token,
        'scanExecutionId', v_check.scan_execution_id,
        'workerJob', public.worker_job_payload(v_job, false),
        'reused', true
      )
    );
  end if;

  v_result := public.cmd_lcia_scope_closure_check_request_v2_untracked(
    p_requested_scope,
    p_request_idempotency_token,
    p_audit
  );
  if coalesce((v_result->>'ok')::boolean, false) is not true then
    return v_result;
  end if;
  select *
  into v_check
  from public.lcia_scope_closure_checks
  where id = nullif(v_result->'data'->>'closureCheckId', '')::uuid
  for update;
  if v_check.id is null then
    return public.lcia_scope_closure_error(
      'closure_check_not_found', 404, 'Closure check not found'
    );
  end if;
  if coalesce((v_result->'data'->>'reused')::boolean, false)
     and v_check.request_key <> v_request_key
     and v_check.data_snapshot_token <> v_snapshot_token then
    return public.lcia_scope_closure_error(
      'idempotency_token_bound_to_different_snapshot',
      409,
      'Idempotency token is already bound to a different data snapshot'
    );
  end if;

  insert into public.lcia_scope_closure_data_snapshots(
    data_snapshot_token,
    root_manifest,
    root_manifest_hash,
    publication_epoch
  ) values (
    v_snapshot_token,
    v_snapshot_manifest,
    public.lcia_scope_closure_sha256(v_snapshot_manifest),
    v_publication_epoch
  )
  on conflict (data_snapshot_token) do nothing;

  update public.lcia_scope_closure_checks
  set data_snapshot_token = v_snapshot_token,
      requested_scope_manifest = v_scope,
      requested_scope_hash = v_requested_scope_hash,
      policy_fingerprint = v_policy_fingerprint,
      request_fingerprint = v_request_fingerprint,
      request_key = v_request_key,
      updated_at = now()
  where id = v_check.id
  returning * into v_check;

  select *
  into v_execution
  from public.lcia_scope_closure_scan_executions
  where request_fingerprint = v_request_fingerprint
  for update;
  if v_execution.id is null then
    insert into public.lcia_scope_closure_scan_executions(
      request_fingerprint,
      requested_scope_hash,
      policy_fingerprint,
      data_snapshot_token,
      validator_scanner_fingerprint
    ) values (
      v_request_fingerprint,
      v_requested_scope_hash,
      v_policy_fingerprint,
      v_snapshot_token,
      v_expected_validator
    )
    returning * into v_execution;
  end if;

  update public.lcia_scope_closure_checks
  set scan_execution_id = v_execution.id,
      updated_at = now()
  where id = v_check.id
  returning * into v_check;

  update public.worker_jobs
  set request_hash = v_request_fingerprint,
      concurrency_key = v_request_key,
      payload_json = payload_json || jsonb_build_object(
        'coverage_mode', v_scope->>'coverageMode',
        'input_manifest', jsonb_build_object(
          'predicateVersion', v_scope->>'eligibilityPredicateVersion',
          'selectionMode', 'closure_certificate',
          'processes', v_scope->'processes'
        ),
        'input_manifest_hash',
          public.lcia_scope_closure_sha256(
            jsonb_build_object('processes', v_scope->'processes')
          ),
        'lcia_method_set', v_scope->'lciaMethods',
        'request_fingerprint', v_request_fingerprint,
        'scan_execution_id', v_execution.id,
        'data_snapshot_token', v_snapshot_token
      ),
      updated_at = now()
  where id = v_check.worker_job_id
  returning * into v_job;

  return jsonb_build_object(
    'ok', true,
    'data', jsonb_build_object(
      'closureCheckId', v_check.id,
      'requestedScopeHash', v_check.requested_scope_hash,
      'policyFingerprint', v_check.policy_fingerprint,
      'dataSnapshotToken', v_snapshot_token,
      'scanExecutionId', v_execution.id,
      'workerJob', public.worker_job_payload(v_job, false),
      'reused', false
    )
  );
exception
  when sqlstate '22023' then
    return public.lcia_scope_closure_error(
      'invalid_closure_scope', 400, sqlerrm
    );
end;
$$;

revoke all on function public.cmd_lcia_scope_closure_check_request_v2(jsonb, text, jsonb)
  from public, anon, authenticated, service_role;
grant execute on function public.cmd_lcia_scope_closure_check_request_v2(jsonb, text, jsonb)
  to authenticated;

comment on function public.cmd_lcia_scope_closure_check_request_v2(jsonb, text, jsonb) is
  'Creates a release-bound closure preflight when a formal release exists, or a deterministic candidate-public-state preflight for initial release bootstrap.';
