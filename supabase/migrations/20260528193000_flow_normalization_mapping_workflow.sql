create or replace function public.flow_normalization_json_array(p_value jsonb)
returns jsonb
language sql
immutable
set search_path = public, pg_temp
as $$
  select case coalesce(jsonb_typeof(p_value), 'null')
    when 'array' then p_value
    when 'object' then jsonb_build_array(p_value)
    else '[]'::jsonb
  end;
$$;

create or replace function public.flow_normalization_first_text(p_value jsonb)
returns text
language sql
immutable
set search_path = public, pg_temp
as $$
  select nullif(
    btrim(
      coalesce(
        case coalesce(jsonb_typeof(p_value), 'null')
          when 'string' then p_value #>> '{}'
          when 'number' then p_value #>> '{}'
          when 'boolean' then p_value #>> '{}'
          when 'object' then coalesce(
            p_value->>'#text',
            p_value->>'common:shortDescription',
            p_value->>'@refObjectId'
          )
          when 'array' then (
            select coalesce(item.value->>'#text', item.value #>> '{}')
            from jsonb_array_elements(p_value) as item(value)
            where nullif(btrim(coalesce(item.value->>'#text', item.value #>> '{}')), '') is not null
            order by case item.value->>'@xml:lang'
              when 'en' then 0
              when 'zh' then 1
              when 'zh-CN' then 1
              else 2
            end
            limit 1
          )
          else null
        end,
        ''
      )
    ),
    ''
  );
$$;

create or replace function public.flow_normalization_normalize_text(p_value text)
returns text
language sql
immutable
set search_path = public, pg_temp
as $$
  select nullif(
    btrim(
      regexp_replace(
        regexp_replace(lower(btrim(coalesce(p_value, ''))), '[[:space:][:punct:]]+', ' ', 'g'),
        '[[:space:]]+',
        ' ',
        'g'
      )
    ),
    ''
  );
$$;

create or replace function public.flow_normalization_classification_key(p_json jsonb)
returns text
language sql
immutable
set search_path = public, pg_temp
as $$
  with raw_items as (
    select item.value
    from jsonb_array_elements(
      public.flow_normalization_json_array(
        p_json #> '{flowDataSet,flowInformation,dataSetInformation,classificationInformation,common:elementaryFlowCategorization,common:category}'
      )
    ) as item(value)
    union all
    select item.value
    from jsonb_array_elements(
      public.flow_normalization_json_array(
        p_json #> '{flowDataSet,flowInformation,dataSetInformation,classificationInformation,common:classification,common:class}'
      )
    ) as item(value)
  ),
  normalized as (
    select public.flow_normalization_normalize_text(
      coalesce(
        value->>'@classId',
        value->>'@catId',
        value->>'#text',
        value #>> '{}'
      )
    ) as key_part
    from raw_items
  )
  select nullif(string_agg(distinct key_part, '|' order by key_part), '')
  from normalized
  where key_part is not null;
$$;

create table if not exists public.flow_normalization_canonical_flows (
  canonical_flow_uuid uuid primary key default gen_random_uuid(),
  flow_id uuid not null,
  flow_version character(9) not null,
  canonical_status text not null default 'proposed',
  canonical_name text not null,
  canonical_basis text not null,
  lca_semantics_preservation text not null,
  evidence jsonb not null default '[]'::jsonb,
  created_by uuid default auth.uid(),
  created_at timestamp with time zone not null default now(),
  modified_at timestamp with time zone not null default now(),
  constraint flow_normalization_canonical_flows_status_check
    check (canonical_status in ('proposed', 'confirmed', 'retired')),
  constraint flow_normalization_canonical_flows_basis_check
    check (btrim(canonical_basis) <> ''),
  constraint flow_normalization_canonical_flows_name_check
    check (btrim(canonical_name) <> ''),
  constraint flow_normalization_canonical_flows_semantics_check
    check (btrim(lca_semantics_preservation) <> ''),
  constraint flow_normalization_canonical_flows_evidence_array_check
    check (jsonb_typeof(evidence) = 'array'),
  constraint flow_normalization_canonical_flows_confirmed_evidence_check
    check (canonical_status <> 'confirmed' or jsonb_array_length(evidence) > 0),
  constraint flow_normalization_canonical_flows_flow_fk
    foreign key (flow_id, flow_version) references public.flows(id, version)
);

create unique index if not exists flow_normalization_canonical_flows_active_uidx
on public.flow_normalization_canonical_flows (flow_id, flow_version)
where canonical_status in ('proposed', 'confirmed');

alter table public.flow_normalization_canonical_flows
  add constraint flow_normalization_canonical_flows_ref_uq
  unique (canonical_flow_uuid, flow_id, flow_version);

create table if not exists public.flow_normalization_clusters (
  cluster_id uuid primary key default gen_random_uuid(),
  cluster_status text not null default 'candidate',
  cluster_basis text not null default 'identity_signature',
  normalized_identity_key text not null,
  normalized_name text,
  type_of_data_set text,
  flow_property_ref_id text,
  flow_property_name text,
  cas_number text,
  classification_key text,
  location_of_supply text,
  candidate_signature jsonb not null default '{}'::jsonb,
  canonical_flow_uuid uuid references public.flow_normalization_canonical_flows(canonical_flow_uuid),
  lca_semantics_preservation text,
  evidence jsonb not null default '[]'::jsonb,
  review_notes text,
  created_by uuid default auth.uid(),
  created_at timestamp with time zone not null default now(),
  modified_at timestamp with time zone not null default now(),
  constraint flow_normalization_clusters_status_check
    check (cluster_status in ('candidate', 'under_review', 'confirmed', 'rejected', 'superseded')),
  constraint flow_normalization_clusters_basis_check
    check (cluster_basis in ('identity_signature', 'manual_review', 'external_evidence', 'data_foundry')),
  constraint flow_normalization_clusters_key_check
    check (btrim(normalized_identity_key) <> ''),
  constraint flow_normalization_clusters_signature_object_check
    check (jsonb_typeof(candidate_signature) = 'object'),
  constraint flow_normalization_clusters_evidence_array_check
    check (jsonb_typeof(evidence) = 'array'),
  constraint flow_normalization_clusters_confirmed_check
    check (
      cluster_status <> 'confirmed'
      or (
        canonical_flow_uuid is not null
        and nullif(btrim(coalesce(lca_semantics_preservation, '')), '') is not null
        and jsonb_array_length(evidence) > 0
      )
    )
);

create index if not exists flow_normalization_clusters_identity_idx
on public.flow_normalization_clusters (normalized_identity_key);

create unique index if not exists flow_normalization_clusters_active_identity_uidx
on public.flow_normalization_clusters (normalized_identity_key)
where cluster_status in ('candidate', 'under_review', 'confirmed');

create index if not exists flow_normalization_clusters_status_idx
on public.flow_normalization_clusters (cluster_status, modified_at desc);

create table if not exists public.flow_normalization_cluster_members (
  cluster_id uuid not null references public.flow_normalization_clusters(cluster_id) on delete cascade,
  flow_id uuid not null,
  flow_version character(9) not null,
  member_role text not null default 'candidate_member',
  match_basis text not null,
  similarity_score numeric(5, 4),
  evidence jsonb not null default '[]'::jsonb,
  created_at timestamp with time zone not null default now(),
  primary key (cluster_id, flow_id, flow_version),
  constraint flow_normalization_cluster_members_flow_fk
    foreign key (flow_id, flow_version) references public.flows(id, version),
  constraint flow_normalization_cluster_members_role_check
    check (member_role in ('candidate_member', 'canonical', 'historical', 'excluded')),
  constraint flow_normalization_cluster_members_basis_check
    check (btrim(match_basis) <> ''),
  constraint flow_normalization_cluster_members_score_check
    check (similarity_score is null or (similarity_score >= 0 and similarity_score <= 1)),
  constraint flow_normalization_cluster_members_evidence_array_check
    check (jsonb_typeof(evidence) = 'array')
);

create unique index if not exists flow_normalization_cluster_members_one_canonical_uidx
on public.flow_normalization_cluster_members (cluster_id)
where member_role = 'canonical';

create index if not exists flow_normalization_cluster_members_flow_idx
on public.flow_normalization_cluster_members (flow_id, flow_version);

create table if not exists public.flow_normalization_mappings (
  mapping_id uuid primary key default gen_random_uuid(),
  cluster_id uuid not null references public.flow_normalization_clusters(cluster_id),
  historical_flow_id uuid not null,
  historical_flow_version character(9) not null,
  canonical_flow_uuid uuid not null,
  canonical_flow_id uuid not null,
  canonical_flow_version character(9) not null,
  mapping_status text not null default 'proposed',
  mapping_reason text not null,
  lca_semantics_preservation text not null,
  calculation_semantics_preserved boolean not null default false,
  evidence jsonb not null default '[]'::jsonb,
  created_by uuid default auth.uid(),
  created_at timestamp with time zone not null default now(),
  modified_at timestamp with time zone not null default now(),
  constraint flow_normalization_mappings_historical_flow_fk
    foreign key (historical_flow_id, historical_flow_version) references public.flows(id, version),
  constraint flow_normalization_mappings_canonical_flow_fk
    foreign key (canonical_flow_uuid, canonical_flow_id, canonical_flow_version)
    references public.flow_normalization_canonical_flows(canonical_flow_uuid, flow_id, flow_version),
  constraint flow_normalization_mappings_status_check
    check (mapping_status in ('proposed', 'confirmed', 'rejected', 'superseded')),
  constraint flow_normalization_mappings_reason_check
    check (btrim(mapping_reason) <> ''),
  constraint flow_normalization_mappings_semantics_check
    check (btrim(lca_semantics_preservation) <> ''),
  constraint flow_normalization_mappings_evidence_array_check
    check (jsonb_typeof(evidence) = 'array'),
  constraint flow_normalization_mappings_no_self_map_check
    check ((historical_flow_id, historical_flow_version) <> (canonical_flow_id, canonical_flow_version)),
  constraint flow_normalization_mappings_confirmed_check
    check (
      mapping_status <> 'confirmed'
      or (calculation_semantics_preserved and jsonb_array_length(evidence) > 0)
    )
);

create unique index if not exists flow_normalization_mappings_cluster_member_uidx
on public.flow_normalization_mappings (cluster_id, historical_flow_id, historical_flow_version);

create unique index if not exists flow_normalization_mappings_confirmed_historical_uidx
on public.flow_normalization_mappings (historical_flow_id, historical_flow_version)
where mapping_status = 'confirmed';

create index if not exists flow_normalization_mappings_canonical_idx
on public.flow_normalization_mappings (canonical_flow_id, canonical_flow_version);

create or replace function public.flow_normalization_flow_identity_rows(
  p_state_codes integer[] default array[0, 20, 100, 200]
)
returns table (
  flow_id uuid,
  flow_version character(9),
  state_code integer,
  modified_at timestamp with time zone,
  normalized_name text,
  type_of_data_set text,
  flow_property_ref_id text,
  flow_property_name text,
  cas_number text,
  classification_key text,
  location_of_supply text,
  identity_key text,
  signal_payload jsonb
)
language sql
stable
set search_path = public, pg_temp
as $$
  with latest_flows as (
    select distinct on (f.id)
      f.id,
      f.version,
      f.state_code,
      f.modified_at,
      f.json
    from public.flows f
    where p_state_codes is null
      or f.state_code = any(p_state_codes)
    order by f.id, f.version desc, f.modified_at desc
  ),
  extracted as (
    select
      f.id as flow_id,
      f.version as flow_version,
      f.state_code,
      f.modified_at,
      public.flow_normalization_normalize_text(
        public.flow_normalization_first_text(
          f.json #> '{flowDataSet,flowInformation,dataSetInformation,name,baseName}'
        )
      ) as normalized_name,
      nullif(btrim(f.json #>> '{flowDataSet,modellingAndValidation,LCIMethod,typeOfDataSet}'), '') as type_of_data_set,
      nullif(
        btrim(fp.flow_property #>> '{referenceToFlowPropertyDataSet,@refObjectId}'),
        ''
      ) as flow_property_ref_id,
      public.flow_normalization_first_text(
        fp.flow_property #> '{referenceToFlowPropertyDataSet,common:shortDescription}'
      ) as flow_property_name,
      nullif(btrim(f.json #>> '{flowDataSet,flowInformation,dataSetInformation,CASNumber}'), '') as cas_number,
      public.flow_normalization_classification_key(f.json) as classification_key,
      nullif(btrim(f.json #>> '{flowDataSet,flowInformation,geography,locationOfSupply}'), '') as location_of_supply
    from latest_flows f
    left join lateral (
      select item.value as flow_property
      from jsonb_array_elements(
        public.flow_normalization_json_array(
          f.json #> '{flowDataSet,flowProperties,flowProperty}'
        )
      ) as item(value)
      order by case
        when item.value->>'@dataSetInternalID' ~ '^[0-9]+$'
          then (item.value->>'@dataSetInternalID')::integer
        else null
      end nulls last
      limit 1
    ) fp on true
  )
  select
    e.flow_id,
    e.flow_version,
    e.state_code,
    e.modified_at,
    e.normalized_name,
    e.type_of_data_set,
    e.flow_property_ref_id,
    e.flow_property_name,
    e.cas_number,
    e.classification_key,
    e.location_of_supply,
    md5(concat_ws(
      '|',
      coalesce(e.normalized_name, ''),
      coalesce(public.flow_normalization_normalize_text(e.type_of_data_set), ''),
      coalesce(public.flow_normalization_normalize_text(e.flow_property_ref_id), ''),
      coalesce(public.flow_normalization_normalize_text(e.flow_property_name), ''),
      coalesce(public.flow_normalization_normalize_text(e.cas_number), ''),
      coalesce(e.classification_key, ''),
      coalesce(public.flow_normalization_normalize_text(e.location_of_supply), '')
    )) as identity_key,
    jsonb_build_object(
      'normalized_name', e.normalized_name,
      'type_of_data_set', e.type_of_data_set,
      'flow_property_ref_id', e.flow_property_ref_id,
      'flow_property_name', e.flow_property_name,
      'cas_number', e.cas_number,
      'classification_key', e.classification_key,
      'location_of_supply', e.location_of_supply
    ) as signal_payload
  from extracted e
  where e.normalized_name is not null;
$$;

create or replace function public.qry_flow_normalization_candidate_clusters(
  p_min_members integer default 2,
  p_state_codes integer[] default array[0, 20, 100, 200],
  p_limit integer default 100
)
returns table (
  identity_key text,
  candidate_signature jsonb,
  flow_count bigint,
  flow_ids uuid[],
  latest_modified_at timestamp with time zone,
  normalized_name text,
  type_of_data_set text,
  flow_property_ref_id text,
  flow_property_name text,
  cas_number text,
  classification_key text,
  location_of_supply text,
  members jsonb
)
language sql
stable
set search_path = public, pg_temp
as $$
  with identity_rows as (
    select *
    from public.flow_normalization_flow_identity_rows(p_state_codes)
  )
  select
    r.identity_key,
    jsonb_build_object(
      'normalized_name', r.normalized_name,
      'type_of_data_set', r.type_of_data_set,
      'flow_property_ref_id', r.flow_property_ref_id,
      'flow_property_name', r.flow_property_name,
      'cas_number', r.cas_number,
      'classification_key', r.classification_key,
      'location_of_supply', r.location_of_supply
    ) as candidate_signature,
    count(*) as flow_count,
    array_agg(r.flow_id order by r.modified_at desc, r.flow_id) as flow_ids,
    max(r.modified_at) as latest_modified_at,
    r.normalized_name,
    r.type_of_data_set,
    r.flow_property_ref_id,
    r.flow_property_name,
    r.cas_number,
    r.classification_key,
    r.location_of_supply,
    jsonb_agg(
      jsonb_build_object(
        'flow_id', r.flow_id,
        'flow_version', btrim(r.flow_version),
        'state_code', r.state_code,
        'modified_at', r.modified_at,
        'signals', r.signal_payload
      )
      order by r.modified_at desc, r.flow_id
    ) as members
  from identity_rows r
  group by
    r.identity_key,
    r.normalized_name,
    r.type_of_data_set,
    r.flow_property_ref_id,
    r.flow_property_name,
    r.cas_number,
    r.classification_key,
    r.location_of_supply
  having count(*) >= greatest(coalesce(p_min_members, 2), 2)
  order by count(*) desc, max(r.modified_at) desc
  limit greatest(coalesce(p_limit, 100), 1);
$$;

create trigger flow_normalization_canonical_flows_set_modified_at
before update on public.flow_normalization_canonical_flows
for each row execute function public.update_modified_at();

create trigger flow_normalization_clusters_set_modified_at
before update on public.flow_normalization_clusters
for each row execute function public.update_modified_at();

create trigger flow_normalization_mappings_set_modified_at
before update on public.flow_normalization_mappings
for each row execute function public.update_modified_at();

alter table public.flow_normalization_canonical_flows enable row level security;
alter table public.flow_normalization_clusters enable row level security;
alter table public.flow_normalization_cluster_members enable row level security;
alter table public.flow_normalization_mappings enable row level security;

revoke all on public.flow_normalization_canonical_flows from anon, authenticated;
revoke all on public.flow_normalization_clusters from anon, authenticated;
revoke all on public.flow_normalization_cluster_members from anon, authenticated;
revoke all on public.flow_normalization_mappings from anon, authenticated;

grant all on public.flow_normalization_canonical_flows to service_role;
grant all on public.flow_normalization_clusters to service_role;
grant all on public.flow_normalization_cluster_members to service_role;
grant all on public.flow_normalization_mappings to service_role;

revoke all on function public.flow_normalization_json_array(jsonb) from public;
revoke all on function public.flow_normalization_first_text(jsonb) from public;
revoke all on function public.flow_normalization_normalize_text(text) from public;
revoke all on function public.flow_normalization_classification_key(jsonb) from public;
revoke all on function public.flow_normalization_flow_identity_rows(integer[]) from public;
revoke all on function public.qry_flow_normalization_candidate_clusters(integer, integer[], integer) from public;

grant execute on function public.flow_normalization_json_array(jsonb) to authenticated, service_role;
grant execute on function public.flow_normalization_first_text(jsonb) to authenticated, service_role;
grant execute on function public.flow_normalization_normalize_text(text) to authenticated, service_role;
grant execute on function public.flow_normalization_classification_key(jsonb) to authenticated, service_role;
grant execute on function public.flow_normalization_flow_identity_rows(integer[]) to authenticated, service_role;
grant execute on function public.qry_flow_normalization_candidate_clusters(integer, integer[], integer) to authenticated, service_role;
