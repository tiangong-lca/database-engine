-- Issue #531 prerequisite: install the immutable Portal-only lexical
-- allowlist expression before the two concurrent index migrations.

begin;

set local lock_timeout = '5s';
set local statement_timeout = '120s';

do $portal_candidate_document_role_guard$
begin
  if not exists (
    select 1
    from pg_catalog.pg_roles
    where rolname = 'portal_public_executor'
      and not rolcanlogin
      and not rolbypassrls
      and not rolsuper
      and not rolreplication
  ) or not exists (
    select 1
    from pg_catalog.pg_roles
    where rolname = 'api_internal_executor'
      and not rolcanlogin
      and not rolbypassrls
      and not rolsuper
      and not rolreplication
  ) then
    raise exception 'Portal candidate document executor role is missing or unsafe'
      using errcode = '42501';
  end if;
end
$portal_candidate_document_role_guard$;

grant portal_public_executor to postgres;
grant create on schema private to portal_public_executor;
set role portal_public_executor;

create or replace function private.catalog_portal_document_v1(
  p_kind text,
  p_json jsonb
)
returns text
language plpgsql
immutable
parallel restricted
security definer
set search_path = ''
as $function$
declare
  v_information jsonb;
  v_modelling jsonb;
  v_location jsonb;
  v_names jsonb := '[]'::jsonb;
  v_synonyms jsonb := '[]'::jsonb;
  v_summary jsonb := '[]'::jsonb;
  v_technology jsonb := '[]'::jsonb;
  v_geography_code text;
  v_classifications jsonb := '[]'::jsonb;
  v_reference_year integer;
  v_process_subtype text;
  v_cas text;
  v_source_metadata jsonb;
  v_source text;
  v_document text;
begin
  if p_kind = 'process' then
    v_information := p_json #> '{processDataSet,processInformation}';
    v_modelling := p_json #> '{processDataSet,modellingAndValidation}';
    v_location := v_information #> '{geography,locationOfOperationSupplyOrProduction}';
    v_names := private.portal_localized_text_v1(
      v_information #> '{dataSetInformation,name,baseName}'
    );
    v_summary := private.portal_localized_text_v1(
      v_information #> '{dataSetInformation,common:generalComment}'
    );
    v_technology := private.portal_localized_text_v1(
      v_information #> '{technology,technologyDescriptionAndIncludedProcesses}'
    ) || private.portal_localized_text_v1(
      v_information #> '{technology,technologicalApplicability}'
    );
    v_classifications := private.portal_classifications_v1(
      v_information #> '{dataSetInformation,classificationInformation}'
    );
    v_reference_year := private.portal_safe_year_v1(
      v_information #>> '{time,common:referenceYear}'
    );
    v_process_subtype := nullif(private.portal_scalar_text_v1(
      v_modelling #> '{LCIMethodAndAllocation,typeOfDataSet}'
    ), '');
    v_geography_code := nullif(
      private.portal_scalar_text_v1(v_location -> '@location'),
      ''
    );
  elsif p_kind = 'flow' then
    v_information := p_json #> '{flowDataSet,flowInformation}';
    v_location := v_information -> 'geography';
    v_names := private.portal_localized_text_v1(
      v_information #> '{dataSetInformation,name,baseName}'
    );
    v_synonyms := private.portal_localized_text_v1(
      v_information #> '{dataSetInformation,common:synonyms}'
    );
    v_summary := private.portal_localized_text_v1(
      v_information #> '{dataSetInformation,common:generalComment}'
    );
    v_classifications := private.portal_classifications_v1(
      v_information #> '{dataSetInformation,classificationInformation}'
    );
    v_cas := nullif(pg_catalog.btrim(coalesce(
      v_information #>> '{dataSetInformation,CASNumber}',
      v_information #>> '{dataSetInformation,common:CASNumber}'
    )), '');
    if v_cas !~ '^[0-9]{2,7}-[0-9]{2}-[0-9]$' then
      v_cas := null;
    end if;
    v_geography_code := case pg_catalog.jsonb_typeof(v_location -> 'locationOfSupply')
      when 'string' then nullif(
        private.portal_scalar_text_v1(v_location -> 'locationOfSupply'),
        ''
      )
      when 'object' then nullif(
        private.portal_scalar_text_v1(
          v_location #> '{locationOfSupply,@location}'
        ),
        ''
      )
      else null
    end;
  else
    return '';
  end if;

  v_source_metadata := private.portal_source_v1(p_kind, p_json);
  select pg_catalog.string_agg(item ->> 'value', ' ' order by item ->> 'language')
  into v_source
  from pg_catalog.jsonb_array_elements(
    v_source_metadata -> 'providerName'
  ) as localized(item);

  select pg_catalog.lower(pg_catalog.concat_ws(' ',
    (select pg_catalog.string_agg(item ->> 'value', ' ')
     from pg_catalog.jsonb_array_elements(v_names) as localized(item)),
    (select pg_catalog.string_agg(item ->> 'value', ' ')
     from pg_catalog.jsonb_array_elements(v_synonyms) as localized(item)),
    (select pg_catalog.string_agg(item ->> 'value', ' ')
     from pg_catalog.jsonb_array_elements(v_summary) as localized(item)),
    (select pg_catalog.string_agg(item ->> 'code', ' ')
     from pg_catalog.jsonb_array_elements(v_classifications) as classification(item)),
    (select pg_catalog.string_agg(item ->> 'value', ' ')
     from pg_catalog.jsonb_array_elements(v_technology) as localized(item)),
    v_geography_code,
    v_reference_year::text,
    v_process_subtype,
    v_cas,
    v_source
  )) into v_document;

  return coalesce(v_document, '');
end
$function$;

comment on function private.catalog_portal_document_v1(text, jsonb) is
  'Immutable Portal-only lexical allowlist expression kept byte-equivalent to portal_catalog_card_v1.document; used only for candidate indexes and exact recheck.';

revoke all on function private.catalog_portal_document_v1(text, jsonb)
  from public, anon, authenticated, service_role;
grant execute on function private.catalog_portal_document_v1(text, jsonb)
  to postgres;

create or replace function private.catalog_portal_projection_payload_v1(
  p_kind text,
  p_state_code integer,
  p_json jsonb
)
returns jsonb
language plpgsql
stable
parallel restricted
security definer
set search_path = ''
as $function$
declare
  v_card jsonb;
begin
  v_card := private.portal_catalog_card_v1(
    p_kind,
    p_state_code,
    p_json
  );
  if pg_catalog.jsonb_typeof(v_card) <> 'object' then
    return null;
  end if;
  return pg_catalog.jsonb_build_object(
    'card', v_card,
    'document', coalesce(v_card ->> 'document', '')
  );
end
$function$;

comment on function private.catalog_portal_projection_payload_v1(
  text, integer, jsonb
) is
  'Pure Portal public-card/document projection used only by the private synchronized search relation.';

revoke all on function private.catalog_portal_projection_payload_v1(
  text, integer, jsonb
) from public, anon, authenticated, service_role, api_internal_executor;
grant execute on function private.catalog_portal_projection_payload_v1(
  text, integer, jsonb
) to api_internal_executor;

reset role;
revoke create on schema private from portal_public_executor;
revoke portal_public_executor from postgres;

create table private.portal_catalog_search_rows_v1 (
  dataset_kind text not null
    check (dataset_kind in ('process', 'flow')),
  id uuid not null,
  version text not null
    check (version ~ '^\d{2}\.\d{2}\.\d{3}$'),
  state_code integer not null
    check (state_code in (100, 200)),
  modified_at timestamptz not null,
  card jsonb not null
    check (pg_catalog.jsonb_typeof(card) = 'object'),
  document text not null,
  embedding_ft extensions.vector(1024),
  primary key (dataset_kind, id, version),
  check (coalesce(card ->> 'document', '') = document)
);

alter table private.portal_catalog_search_rows_v1 owner to postgres;
alter table private.portal_catalog_search_rows_v1 enable row level security;
alter table private.portal_catalog_search_rows_v1 force row level security;

create policy portal_catalog_search_rows_portal_select_v1
on private.portal_catalog_search_rows_v1
for select
to portal_public_executor
using (state_code in (100, 200));

create policy portal_catalog_search_rows_internal_all_v1
on private.portal_catalog_search_rows_v1
for all
to api_internal_executor
using (state_code in (100, 200))
with check (state_code in (100, 200));

revoke all on table private.portal_catalog_search_rows_v1
  from public, anon, authenticated, service_role;
grant select (
  dataset_kind,
  id,
  version,
  state_code,
  modified_at,
  card,
  document
) on table private.portal_catalog_search_rows_v1
  to portal_public_executor;
grant select, insert, update, delete on table private.portal_catalog_search_rows_v1
  to api_internal_executor;

create index portal_catalog_search_rows_latest_v1_idx
  on private.portal_catalog_search_rows_v1 (
    dataset_kind,
    id,
    version desc,
    modified_at desc,
    state_code desc
  );

create or replace function private.sync_portal_catalog_search_row_v1()
returns trigger
language plpgsql
security definer
set search_path = ''
set row_security = 'on'
as $function$
declare
  v_kind text := case tg_table_name
    when 'processes' then 'process'
    when 'flows' then 'flow'
    else null
  end;
  v_root_key text := case v_kind
    when 'process' then 'processDataSet'
    when 'flow' then 'flowDataSet'
    else null
  end;
  v_payload jsonb;
begin
  if v_kind is null then
    raise exception 'unsupported Portal projection trigger source'
      using errcode = '55000';
  end if;

  if tg_op = 'DELETE' then
    delete from private.portal_catalog_search_rows_v1 as projection
    where projection.dataset_kind = v_kind
      and projection.id = old.id
      and projection.version = old.version::text;
    return old;
  end if;

  if tg_op = 'UPDATE'
     and (old.id, old.version::text) is distinct from (new.id, new.version::text) then
    delete from private.portal_catalog_search_rows_v1 as projection
    where projection.dataset_kind = v_kind
      and projection.id = old.id
      and projection.version = old.version::text;
  end if;

  if tg_argv[0] = 'embedding' then
    update private.portal_catalog_search_rows_v1 as projection
    set embedding_ft = new.embedding_ft
    where projection.dataset_kind = v_kind
      and projection.id = new.id
      and projection.version = new.version::text;
    if not found
       and new.state_code in (100, 200)
       and new.modified_at is not null
       and pg_catalog.jsonb_typeof(new.json) = 'object'
       and pg_catalog.jsonb_typeof(new.json -> v_root_key) = 'object' then
      v_payload := private.catalog_portal_projection_payload_v1(
        v_kind,
        new.state_code,
        new.json
      );
      insert into private.portal_catalog_search_rows_v1 (
        dataset_kind,
        id,
        version,
        state_code,
        modified_at,
        card,
        document,
        embedding_ft
      ) values (
        v_kind,
        new.id,
        new.version::text,
        new.state_code,
        new.modified_at,
        v_payload -> 'card',
        v_payload ->> 'document',
        new.embedding_ft
      )
      on conflict (dataset_kind, id, version) do update
      set embedding_ft = excluded.embedding_ft;
    end if;
    return new;
  end if;

  if new.state_code in (100, 200)
     and new.modified_at is not null
     and pg_catalog.jsonb_typeof(new.json) = 'object'
     and pg_catalog.jsonb_typeof(new.json -> v_root_key) = 'object' then
    v_payload := private.catalog_portal_projection_payload_v1(
      v_kind,
      new.state_code,
      new.json
    );
    if pg_catalog.jsonb_typeof(v_payload) <> 'object' then
      raise exception 'Portal projection payload is invalid'
        using errcode = '55000';
    end if;
    insert into private.portal_catalog_search_rows_v1 (
      dataset_kind,
      id,
      version,
      state_code,
      modified_at,
      card,
      document,
      embedding_ft
    ) values (
      v_kind,
      new.id,
      new.version::text,
      new.state_code,
      new.modified_at,
      v_payload -> 'card',
      v_payload ->> 'document',
      new.embedding_ft
    )
    on conflict (dataset_kind, id, version) do update
    set state_code = excluded.state_code,
        modified_at = excluded.modified_at,
        card = excluded.card,
        document = excluded.document,
        embedding_ft = excluded.embedding_ft;
  else
    delete from private.portal_catalog_search_rows_v1 as projection
    where projection.dataset_kind = v_kind
      and projection.id = new.id
      and projection.version = new.version::text;
  end if;
  return new;
end
$function$;

grant api_internal_executor to postgres;
grant create on schema private to api_internal_executor;
alter function private.sync_portal_catalog_search_row_v1()
  owner to api_internal_executor;
set role api_internal_executor;
revoke all on function private.sync_portal_catalog_search_row_v1()
  from public, anon, authenticated, service_role,
    portal_public_executor, api_internal_executor;
grant execute on function private.sync_portal_catalog_search_row_v1()
  to postgres;
reset role;

create trigger portal_catalog_projection_content_sync_v1
after insert or delete or update of id, version, json, state_code, modified_at
on public.processes
for each row execute function private.sync_portal_catalog_search_row_v1('content');

create trigger portal_catalog_projection_embedding_sync_v1
after update of embedding_ft
on public.processes
for each row execute function private.sync_portal_catalog_search_row_v1('embedding');

create trigger portal_catalog_projection_content_sync_v1
after insert or delete or update of id, version, json, state_code, modified_at
on public.flows
for each row execute function private.sync_portal_catalog_search_row_v1('content');

create trigger portal_catalog_projection_embedding_sync_v1
after update of embedding_ft
on public.flows
for each row execute function private.sync_portal_catalog_search_row_v1('embedding');

set role api_internal_executor;
comment on function private.sync_portal_catalog_search_row_v1() is
  'NOLOGIN/NOBYPASSRLS writer maintains exact visible public-card rows; derivative-only embedding updates never rebuild cards.';
revoke execute on function private.sync_portal_catalog_search_row_v1()
  from postgres;
reset role;
revoke create on schema private from api_internal_executor;
revoke api_internal_executor from postgres;

create or replace function private.backfill_portal_catalog_search_range_v1(
  p_lower uuid,
  p_upper uuid
)
returns jsonb
language plpgsql
set search_path = ''
set statement_timeout = '120s'
set row_security = 'on'
as $function$
declare
  v_process_rows integer;
  v_flow_rows integer;
begin
  if p_lower is null or (p_upper is not null and p_upper <= p_lower) then
    raise exception 'invalid Portal projection backfill range'
      using errcode = '22023';
  end if;

  insert into private.portal_catalog_search_rows_v1 (
    dataset_kind, id, version, state_code, modified_at,
    card, document, embedding_ft
  )
  select
    'process',
    process.id,
    process.version::text,
    process.state_code,
    process.modified_at,
    payload.value -> 'card',
    payload.value ->> 'document',
    process.embedding_ft
  from public.processes as process
  cross join lateral (
    select private.catalog_portal_projection_payload_v1(
      'process', process.state_code, process.json
    ) as value
  ) as payload
  where process.id >= p_lower
    and (p_upper is null or process.id < p_upper)
    and process.state_code in (100, 200)
    and process.modified_at is not null
    and pg_catalog.jsonb_typeof(process.json) = 'object'
    and pg_catalog.jsonb_typeof(process.json -> 'processDataSet') = 'object'
    and pg_catalog.jsonb_typeof(payload.value) = 'object'
  on conflict (dataset_kind, id, version) do nothing;
  get diagnostics v_process_rows = row_count;

  insert into private.portal_catalog_search_rows_v1 (
    dataset_kind, id, version, state_code, modified_at,
    card, document, embedding_ft
  )
  select
    'flow',
    flow.id,
    flow.version::text,
    flow.state_code,
    flow.modified_at,
    payload.value -> 'card',
    payload.value ->> 'document',
    flow.embedding_ft
  from public.flows as flow
  cross join lateral (
    select private.catalog_portal_projection_payload_v1(
      'flow', flow.state_code, flow.json
    ) as value
  ) as payload
  where flow.id >= p_lower
    and (p_upper is null or flow.id < p_upper)
    and flow.state_code in (100, 200)
    and flow.modified_at is not null
    and pg_catalog.jsonb_typeof(flow.json) = 'object'
    and pg_catalog.jsonb_typeof(flow.json -> 'flowDataSet') = 'object'
    and pg_catalog.jsonb_typeof(payload.value) = 'object'
  on conflict (dataset_kind, id, version) do nothing;
  get diagnostics v_flow_rows = row_count;

  return pg_catalog.jsonb_build_object(
    'processRows', v_process_rows,
    'flowRows', v_flow_rows
  );
end
$function$;

grant api_internal_executor to postgres;
grant create on schema private to api_internal_executor;
alter function private.backfill_portal_catalog_search_range_v1(uuid, uuid)
  owner to api_internal_executor;
set role api_internal_executor;
revoke all on function private.backfill_portal_catalog_search_range_v1(uuid, uuid)
  from public, anon, authenticated, service_role,
    portal_public_executor, api_internal_executor;
grant execute on function private.backfill_portal_catalog_search_range_v1(uuid, uuid)
  to api_internal_executor;
reset role;
revoke create on schema private from api_internal_executor;
revoke api_internal_executor from postgres;

comment on table private.portal_catalog_search_rows_v1 is
  'Private synchronized, public-safe Portal card/document/vector projection. Browser and service roles have no direct ACL.';

commit;
