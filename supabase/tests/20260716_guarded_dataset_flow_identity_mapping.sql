begin;

create extension if not exists pgtap with schema extensions;
create extension if not exists dblink with schema extensions;
set local search_path = extensions, public, auth;

select plan(96);

create or replace function pg_temp.flow_identity_id(p_kind text)
returns uuid
language sql
immutable
as $$
  select (case p_kind
    when 'owner' then 'f1000000-0000-4000-8000-000000000001'
    when 'other' then 'f1000000-0000-4000-8000-000000000002'
    when 'unitgroup' then 'f2000000-0000-4000-8000-000000000001'
    when 'flowproperty' then 'f3000000-0000-4000-8000-000000000001'
    when 'source_flow' then 'f4000000-0000-4000-8000-000000000001'
    when 'target_flow' then 'f4000000-0000-4000-8000-000000000002'
    when 'pending_flow' then 'f4000000-0000-4000-8000-000000000003'
    when 'process' then 'f5000000-0000-4000-8000-000000000001'
  end)::uuid
$$;

create or replace function pg_temp.flow_identity_orphan_id(p_ordinal integer)
returns uuid
language sql
immutable
strict
as $$
  select ('e0000000-0000-4000-8000-' ||
    lpad(p_ordinal::text, 12, '0'))::uuid
$$;

create or replace function pg_temp.flow_identity_reference(
  p_id uuid,
  p_version text,
  p_name text
) returns jsonb
language sql
immutable
as $$
  select jsonb_build_object(
    '@refObjectId', p_id,
    '@type', 'flow data set',
    '@uri', '../flows/' || p_id::text || '_' || p_version || '.xml',
    '@version', p_version,
    'common:shortDescription', jsonb_build_object(
      '@xml:lang', 'en', '#text', p_name
    )
  )
$$;

create or replace function pg_temp.flow_identity_unitgroup_payload()
returns jsonb
language sql
immutable
as $$
  select jsonb_build_object(
    'unitGroupDataSet', jsonb_build_object(
      'unitGroupInformation', jsonb_build_object(
        'dataSetInformation', jsonb_build_object(
          'common:UUID', pg_temp.flow_identity_id('unitgroup'),
          'common:name', jsonb_build_object('#text', 'kg')
        ),
        'quantitativeReference', jsonb_build_object(
          'referenceToReferenceUnit', '1'
        )
      ),
      'units', jsonb_build_object('unit', jsonb_build_array(
        jsonb_build_object(
          '@dataSetInternalID', '1', 'meanValue', '1', 'name', 'kg'
        )
      )),
      'administrativeInformation', jsonb_build_object(
        'publicationAndOwnership', jsonb_build_object(
          'common:dataSetVersion', '01.00.000'
        )
      )
    )
  )
$$;

create or replace function pg_temp.flow_identity_flowproperty_payload()
returns jsonb
language sql
immutable
as $$
  select jsonb_build_object(
    'flowPropertyDataSet', jsonb_build_object(
      'flowPropertiesInformation', jsonb_build_object(
        'dataSetInformation', jsonb_build_object(
          'common:UUID', pg_temp.flow_identity_id('flowproperty'),
          'common:name', jsonb_build_object('#text', 'Mass')
        ),
        'quantitativeReference', jsonb_build_object(
          'referenceToReferenceUnitGroup', jsonb_build_object(
            '@refObjectId', pg_temp.flow_identity_id('unitgroup'),
            '@type', 'unit group data set',
            '@uri', '../unitgroups/' ||
              pg_temp.flow_identity_id('unitgroup')::text || '_01.00.000.xml',
            '@version', '01.00.000',
            'common:shortDescription', jsonb_build_object('#text', 'kg')
          )
        )
      ),
      'administrativeInformation', jsonb_build_object(
        'publicationAndOwnership', jsonb_build_object(
          'common:dataSetVersion', '01.00.000'
        )
      )
    )
  )
$$;

create or replace function pg_temp.flow_identity_flow_payload(
  p_id uuid,
  p_name text
) returns jsonb
language sql
immutable
as $$
  select jsonb_build_object(
    'flowDataSet', jsonb_build_object(
      'flowInformation', jsonb_build_object(
        'dataSetInformation', jsonb_build_object(
          'common:UUID', p_id,
          'name', jsonb_build_object('baseName', jsonb_build_object(
            '@xml:lang', 'en', '#text', p_name
          )),
          'classificationInformation', jsonb_build_object(
            'common:classification', jsonb_build_object(
              'common:class', jsonb_build_object(
                '@level', '0', '@classId', 'emissions', '#text', 'Emissions'
              )
            )
          )
        ),
        'quantitativeReference', jsonb_build_object(
          'referenceToReferenceFlowProperty', '1'
        )
      ),
      'modellingAndValidation', jsonb_build_object(
        'LCIMethod', jsonb_build_object('typeOfDataSet', 'Elementary flow')
      ),
      'flowProperties', jsonb_build_object(
        'flowProperty', jsonb_build_object(
          '@dataSetInternalID', '1',
          'meanValue', '1',
          'referenceToFlowPropertyDataSet', jsonb_build_object(
            '@refObjectId', pg_temp.flow_identity_id('flowproperty'),
            '@type', 'flow property data set',
            '@uri', '../flowproperties/' ||
              pg_temp.flow_identity_id('flowproperty')::text ||
              '_01.00.000.xml',
            '@version', '01.00.000',
            'common:shortDescription', jsonb_build_object('#text', 'Mass')
          )
        )
      ),
      'administrativeInformation', jsonb_build_object(
        'publicationAndOwnership', jsonb_build_object(
          'common:dataSetVersion', '01.00.000'
        )
      )
    )
  )
$$;

create or replace function pg_temp.flow_identity_process_payload(
  p_after boolean default false
) returns jsonb
language sql
immutable
as $$
  select jsonb_build_object(
    'processDataSet', jsonb_build_object(
      'processInformation', jsonb_build_object(
        'dataSetInformation', jsonb_build_object(
          'common:UUID', pg_temp.flow_identity_id('process'),
          'name', jsonb_build_object('baseName', jsonb_build_object(
            '@xml:lang', 'en', '#text', 'Step 3 test process'
          ))
        )
      ),
      'exchanges', jsonb_build_object('exchange', jsonb_build_array(
        jsonb_build_object(
          '@dataSetInternalID', '1',
          'exchangeDirection', 'Input',
          'meanAmount', '5',
          'resultingAmount', '5',
          'generalComment', jsonb_build_object('#text', 'preserve me'),
          'referenceToFlowDataSet', case when p_after
            then pg_temp.flow_identity_reference(
              pg_temp.flow_identity_id('target_flow'),
              '01.00.000', 'Public target'
            )
            else pg_temp.flow_identity_reference(
              pg_temp.flow_identity_id('source_flow'),
              '01.00.000', 'Owner source'
            )
          end
        ),
        jsonb_build_object(
          '@dataSetInternalID', '2',
          'exchangeDirection', 'Output',
          'meanAmount', '7',
          'resultingAmount', '7',
          'referenceToFlowDataSet', pg_temp.flow_identity_reference(
            pg_temp.flow_identity_id('target_flow'),
            '01.00.000', 'Public target'
          )
        ),
        jsonb_build_object(
          '@dataSetInternalID', '3',
          'exchangeDirection', 'Input',
          'meanAmount', '11',
          'resultingAmount', '11',
          'referenceToFlowDataSet', pg_temp.flow_identity_reference(
            pg_temp.flow_identity_id('pending_flow'),
            '01.00.000', 'Protected pending'
          )
        )
      )),
      'administrativeInformation', jsonb_build_object(
        'publicationAndOwnership', jsonb_build_object(
          'common:dataSetVersion', '01.00.000'
        )
      )
    )
  )
$$;

insert into auth.users (
  instance_id, id, aud, role, email, encrypted_password,
  email_confirmed_at, raw_app_meta_data, raw_user_meta_data,
  created_at, updated_at, is_sso_user, is_anonymous
) values
  (
    '00000000-0000-0000-0000-000000000000',
    pg_temp.flow_identity_id('owner'), 'authenticated', 'authenticated',
    'flow-owner@example.com', 'test-password-hash', now(),
    '{"provider":"email","providers":["email"]}'::jsonb,
    jsonb_build_object('sub', pg_temp.flow_identity_id('owner')), now(), now(),
    false, false
  ),
  (
    '00000000-0000-0000-0000-000000000000',
    pg_temp.flow_identity_id('other'), 'authenticated', 'authenticated',
    'flow-other@example.com', 'test-password-hash', now(),
    '{"provider":"email","providers":["email"]}'::jsonb,
    jsonb_build_object('sub', pg_temp.flow_identity_id('other')), now(), now(),
    false, false
  );

insert into public.unitgroups (
  id, version, user_id, state_code, json, json_ordered, modified_at
) values (
  pg_temp.flow_identity_id('unitgroup'), '01.00.000',
  pg_temp.flow_identity_id('other'), 100,
  pg_temp.flow_identity_unitgroup_payload(),
  pg_temp.flow_identity_unitgroup_payload()::json,
  '2026-07-16T01:00:00Z'
);

insert into public.flowproperties (
  id, version, user_id, state_code, json, json_ordered, modified_at
) values (
  pg_temp.flow_identity_id('flowproperty'), '01.00.000',
  pg_temp.flow_identity_id('other'), 100,
  pg_temp.flow_identity_flowproperty_payload(),
  pg_temp.flow_identity_flowproperty_payload()::json,
  '2026-07-16T01:00:00Z'
);

alter table public.flows disable trigger user;
alter table public.processes disable trigger user;

insert into public.flows (
  id, version, user_id, state_code, json, json_ordered, modified_at,
  extracted_text, extracted_md
) values
  (
    pg_temp.flow_identity_id('source_flow'), '01.00.000',
    pg_temp.flow_identity_id('owner'), 0,
    pg_temp.flow_identity_flow_payload(
      pg_temp.flow_identity_id('source_flow'), 'Owner source'
    ),
    pg_temp.flow_identity_flow_payload(
      pg_temp.flow_identity_id('source_flow'), 'Owner source'
    )::json,
    '2026-07-16T01:00:00Z', 'source text', 'source markdown'
  ),
  (
    pg_temp.flow_identity_id('target_flow'), '01.00.000',
    pg_temp.flow_identity_id('other'), 100,
    pg_temp.flow_identity_flow_payload(
      pg_temp.flow_identity_id('target_flow'), 'Public target'
    ),
    pg_temp.flow_identity_flow_payload(
      pg_temp.flow_identity_id('target_flow'), 'Public target'
    )::json,
    '2026-07-16T01:00:00Z', 'target text', 'target markdown'
  ),
  (
    pg_temp.flow_identity_id('pending_flow'), '01.00.000',
    pg_temp.flow_identity_id('owner'), 0,
    pg_temp.flow_identity_flow_payload(
      pg_temp.flow_identity_id('pending_flow'), 'Protected pending'
    ),
    pg_temp.flow_identity_flow_payload(
      pg_temp.flow_identity_id('pending_flow'), 'Protected pending'
    )::json,
    '2026-07-16T01:00:00Z', 'pending text', 'pending markdown'
  );

insert into public.flows (
  id, version, user_id, state_code, json, json_ordered, modified_at,
  extracted_text, extracted_md
)
select
  pg_temp.flow_identity_orphan_id(n), '01.00.000',
  pg_temp.flow_identity_id('owner'), 0,
  pg_temp.flow_identity_flow_payload(
    pg_temp.flow_identity_orphan_id(n), 'Protected orphan ' || n
  ),
  pg_temp.flow_identity_flow_payload(
    pg_temp.flow_identity_orphan_id(n), 'Protected orphan ' || n
  )::json,
  '2026-07-16T01:00:00Z', 'orphan text ' || n, 'orphan markdown ' || n
from generate_series(1, 303) as n;

insert into public.processes (
  id, version, user_id, state_code, json, json_ordered, modified_at,
  extracted_text, extracted_md, model_id, rule_verification
) values (
  pg_temp.flow_identity_id('process'), '01.00.000',
  pg_temp.flow_identity_id('owner'), 0,
  pg_temp.flow_identity_process_payload(false),
  pg_temp.flow_identity_process_payload(false)::json,
  '2026-07-16T01:00:00Z', 'process text', 'process markdown', null, null
);

alter table public.flows enable trigger user;
alter table public.processes enable trigger user;

create temp table flow_identity_state (
  key text primary key,
  value jsonb not null
) on commit drop;

grant select, insert, update, delete on flow_identity_state to authenticated;
grant select on public.command_audit_log to authenticated;
grant select on util.dataset_derivative_rebuild_requests to authenticated;
grant select on util.dataset_flow_identity_process_ledger to authenticated;
grant select on util.dataset_flow_identity_scopes to authenticated;
grant select on util.dataset_flow_identity_capture_receipts to authenticated;
-- Request builders run in the authenticated section to exercise the public
-- surface.  These private helper grants exist only inside this rolled-back
-- pgTAP transaction; production migration grants remain unchanged.
grant usage on schema private, util to authenticated;
grant execute on function private.dataset_alias_canonical_jsonb_v1(jsonb)
  to authenticated;
grant execute on function private.dataset_alias_js_object_key_sort_key_v1(text)
  to authenticated;
grant execute on function util.dataset_flow_identity_sha256(jsonb)
  to authenticated;
grant execute on function util.dataset_flow_identity_restricted_sha256_v2(jsonb)
  to authenticated;
grant execute on function private.dataset_flow_identity_safe_json_v2(jsonb)
  to authenticated;
grant execute on function private.dataset_flow_identity_exact_keys(jsonb, text[])
  to authenticated;
grant execute on function private.dataset_flow_identity_short_description_v2(jsonb)
  to authenticated;
grant execute on function private.dataset_flow_identity_row_sha256(
  uuid, text, uuid, integer, timestamp with time zone, text
) to authenticated;
grant execute on function private.dataset_flow_identity_exchanges(jsonb)
  to authenticated;
grant execute on function private.dataset_flow_identity_reference(jsonb)
  to authenticated;
grant execute on function private.dataset_flow_identity_collision_ledger(
  jsonb, jsonb
) to authenticated;
grant execute on function util.dataset_derivative_rebuild_snapshot(
  public.processes
) to authenticated;
grant execute on function util.dataset_derivative_rebuild_sha256(text)
  to authenticated;
grant execute on function util.read_dataset_derivative_rebuild_batch_any(
  uuid, uuid
) to authenticated;
grant execute on function util.read_dataset_flow_identity_derivative_set(
  uuid, uuid
) to authenticated;

create or replace function pg_temp.complete_flow_identity_derivative(
  p_request_id uuid,
  p_suffix text
) returns void
language plpgsql
as $$
declare
  v_request util.dataset_derivative_rebuild_requests%rowtype;
  v_markdown text;
  v_markdown_sha256 text;
  v_markdown_id bigint;
  v_embedding extensions.vector(1024);
  v_embedding_id bigint;
  v_snapshot jsonb;
begin
  select request.* into strict v_request
  from util.dataset_derivative_rebuild_requests as request
  where request.id = p_request_id;

  v_markdown := 'flow identity derivative ' || p_suffix;
  v_markdown_sha256 := util.dataset_derivative_rebuild_sha256(v_markdown);
  v_embedding := (
    '[' || array_to_string(array_fill('0'::text, array[1024]), ',') || ']'
  )::extensions.vector;

  insert into util.dataset_derivative_rebuild_proposals (
    request_id, proposal_kind, extracted_md, extracted_md_sha256, status
  ) values (
    p_request_id, 'markdown', v_markdown, v_markdown_sha256, 'accepted'
  ) returning id into v_markdown_id;

  insert into util.dataset_derivative_rebuild_proposals (
    request_id, proposal_kind, embedding_ft, embedding_ft_sha256,
    embedding_ft_at, source_extracted_md_sha256, status
  ) values (
    p_request_id, 'embedding', v_embedding,
    util.dataset_derivative_rebuild_sha256(v_embedding::text),
    clock_timestamp() + interval '1 second', v_markdown_sha256, 'captured'
  ) returning id into v_embedding_id;

  update util.dataset_derivative_rebuild_requests
  set
    markdown_proposal_id = v_markdown_id,
    accepted_extracted_md_sha256 = v_markdown_sha256,
    markdown_request_id = -9100000,
    markdown_dispatched_at = clock_timestamp() - interval '3 seconds',
    markdown_response_status = 200,
    markdown_response_received_at = clock_timestamp() - interval '2 seconds',
    embedding_queue_msg_id = -8100000,
    embedding_queued_at = clock_timestamp() - interval '1 second'
  where id = p_request_id;

  perform util.commit_dataset_derivative_rebuild_proposal(
    p_request_id, v_markdown_id, v_embedding_id
  );

  v_snapshot := util.dataset_derivative_rebuild_snapshot(
    v_request.target_table, v_request.target_id, v_request.target_version
  );
  update util.dataset_derivative_rebuild_requests
  set
    status = 'completed',
    phase = 'completed',
    completed_snapshot_sha256 = v_snapshot->>'snapshot_sha256',
    completed_at = clock_timestamp(),
    terminal_at = clock_timestamp(),
    drained_at = clock_timestamp()
  where id = p_request_id;

  perform util.record_dataset_derivative_rebuild_terminal(p_request_id);
end
$$;

delete from vault.secrets
where name in ('project_secret_key', 'project_url');
select vault.create_secret(
  'flow-identity-test-service-secret',
  'project_secret_key',
  'transaction-local Step 3 test key'
);
select vault.create_secret(
  'http://127.0.0.1:55321',
  'project_url',
  'transaction-local Step 3 local URL'
);

create or replace function pg_temp.flow_guard(p_target boolean)
returns jsonb
language plpgsql
stable
as $$
declare
  v_flow public.flows%rowtype;
  v_payload_sha text;
  v_guard jsonb;
begin
  select flow.* into v_flow
  from public.flows as flow
  where flow.id = pg_temp.flow_identity_id(
    case when p_target then 'target_flow' else 'source_flow' end
  ) and flow.version = '01.00.000';
  v_payload_sha := util.dataset_flow_identity_sha256(v_flow.json_ordered::jsonb);
  v_guard := jsonb_build_object(
    'id', v_flow.id,
    'version', btrim(v_flow.version::text),
    'user_id', v_flow.user_id,
    'state_code', v_flow.state_code,
    'modified_at', v_flow.modified_at,
    'payload_sha256', v_payload_sha,
    'row_sha256', private.dataset_flow_identity_row_sha256(
      v_flow.id, btrim(v_flow.version::text), v_flow.user_id,
      v_flow.state_code, v_flow.modified_at, v_payload_sha
    ),
    'flow_type', 'Elementary flow',
    'flow_property_id', pg_temp.flow_identity_id('flowproperty'),
    'flow_property_version', '01.00.000',
    'unit_group_id', pg_temp.flow_identity_id('unitgroup'),
    'unit_group_version', '01.00.000',
    'category_path_sha256', util.dataset_flow_identity_sha256(
      v_flow.json #>
        '{flowDataSet,flowInformation,dataSetInformation,classificationInformation}'
    )
  );
  if p_target then
    return v_guard || jsonb_build_object(
      'reference', pg_temp.flow_identity_reference(
        v_flow.id, btrim(v_flow.version::text), 'Public target'
      )
    );
  end if;
  return v_guard || jsonb_build_object(
    'source_trace_sha256', repeat('a', 64)
  );
end;
$$;

create or replace function pg_temp.flow_identity_mapping()
returns jsonb
language plpgsql
stable
as $$
declare
  v_mapping jsonb;
begin
  v_mapping := jsonb_build_object(
    'source', pg_temp.flow_guard(false),
    'target', pg_temp.flow_guard(true),
    'compatibility', jsonb_build_object(
      'policy_sha256', repeat('1', 64),
      'mode', 'identity',
      'confidence', 'approved',
      'flow_property_compatible', true,
      'unit_group_compatible', true,
      'direction_compatible', true,
      'compartment_compatible', true,
      'conversion_factor', '1',
      'evidence_sha256', repeat('2', 64),
      'flow_schema', jsonb_build_object(
        'status', 'legacy_warning',
        'warning_set_sha256', repeat('3', 64)
      ),
      'process_schema_required', 'pass'
    )
  );
  return jsonb_build_object(
    'ordinal', 1,
    'mapping_id', util.dataset_flow_identity_sha256(v_mapping)
  ) || v_mapping;
end;
$$;

create or replace function pg_temp.flow_identity_protected_closure()
returns jsonb
language plpgsql
stable
as $$
declare
  v_exchange jsonb := private.dataset_flow_identity_exchanges(
    pg_temp.flow_identity_process_payload(false)
  )->2;
  v_occurrences jsonb;
  v_pending jsonb;
  v_orphans jsonb;
begin
  v_occurrences := jsonb_build_array(jsonb_build_object(
    'process_id', pg_temp.flow_identity_id('process'),
    'process_version', '01.00.000',
    'exchange_index', 2,
    'internal_id', '3',
    'direction', 'Input',
    'reference_sha256', util.dataset_flow_identity_sha256(
      private.dataset_flow_identity_reference(v_exchange)
    )
  ));
  v_pending := jsonb_build_array(jsonb_build_object(
    'source_id', pg_temp.flow_identity_id('pending_flow'),
    'source_version', '01.00.000',
    'expected_reference_count', 1,
    'occurrences', v_occurrences,
    'occurrence_set_sha256', util.dataset_flow_identity_sha256(v_occurrences),
    'evidence_sha256', repeat('4', 64)
  ));
  select jsonb_agg(jsonb_build_object(
    'source_id', pg_temp.flow_identity_orphan_id(n),
    'source_version', '01.00.000',
    'evidence_sha256', encode(digest('orphan-' || n, 'sha256'), 'hex')
  ) order by pg_temp.flow_identity_orphan_id(n)::text)
  into v_orphans
  from generate_series(1, 303) as n;
  return jsonb_build_object(
    'schema_version', 'dataset-flow-identity-protected-closure.v1',
    'pending', v_pending,
    'blockers', '[]'::jsonb,
    'orphans', v_orphans,
    'pending_set_sha256', util.dataset_flow_identity_sha256(v_pending),
    'blocker_set_sha256', util.dataset_flow_identity_sha256('[]'::jsonb),
    'orphan_set_sha256', util.dataset_flow_identity_sha256(v_orphans),
    'total_expected_reference_count', 1
  );
end;
$$;

create or replace function pg_temp.flow_identity_support_snapshots()
returns jsonb
language plpgsql
stable
as $$
declare
  v_result jsonb;
begin
  select jsonb_agg(entry order by ordinal)
  into v_result
  from (
    select 1 as ordinal, jsonb_build_object(
      'ordinal', 1, 'table', 'flowproperties', 'id', fp.id,
      'version', btrim(fp.version::text), 'user_id', fp.user_id,
      'state_code', fp.state_code, 'modified_at', fp.modified_at,
      'payload_sha256', util.dataset_flow_identity_sha256(fp.json_ordered::jsonb),
      'row_sha256', private.dataset_flow_identity_row_sha256(
        fp.id, btrim(fp.version::text), fp.user_id, fp.state_code,
        fp.modified_at, util.dataset_flow_identity_sha256(fp.json_ordered::jsonb)
      )
    ) as entry
    from public.flowproperties fp
    where fp.id = pg_temp.flow_identity_id('flowproperty')
      and fp.version = '01.00.000'
    union all
    select 2, jsonb_build_object(
      'ordinal', 2, 'table', 'unitgroups', 'id', ug.id,
      'version', btrim(ug.version::text), 'user_id', ug.user_id,
      'state_code', ug.state_code, 'modified_at', ug.modified_at,
      'payload_sha256', util.dataset_flow_identity_sha256(ug.json_ordered::jsonb),
      'row_sha256', private.dataset_flow_identity_row_sha256(
        ug.id, btrim(ug.version::text), ug.user_id, ug.state_code,
        ug.modified_at, util.dataset_flow_identity_sha256(ug.json_ordered::jsonb)
      )
    )
    from public.unitgroups ug
    where ug.id = pg_temp.flow_identity_id('unitgroup')
      and ug.version = '01.00.000'
  ) support;
  return v_result;
end;
$$;

create or replace function pg_temp.flow_identity_source_universe()
returns jsonb
language sql
stable
as $$
  select jsonb_agg(jsonb_build_object(
    'id', f.id, 'version', btrim(f.version::text),
    'user_id', f.user_id, 'state_code', f.state_code,
    'flow_type', 'Elementary flow'
  ) order by f.id::text, btrim(f.version::text))
  from public.flows f
  where f.user_id = pg_temp.flow_identity_id('owner')
    and f.state_code = 0
    and f.json #>> '{flowDataSet,modellingAndValidation,LCIMethod,typeOfDataSet}'
      = 'Elementary flow'
$$;

create or replace function pg_temp.flow_identity_rewrites()
returns jsonb
language plpgsql
stable
as $$
declare
  v_source jsonb := pg_temp.flow_identity_reference(
    pg_temp.flow_identity_id('source_flow'), '01.00.000', 'Owner source'
  );
  v_target jsonb := pg_temp.flow_identity_reference(
    pg_temp.flow_identity_id('target_flow'), '01.00.000', 'Public target'
  );
begin
  return jsonb_build_array(jsonb_build_object(
    'ordinal', 1,
    'exchange_index', 0,
    'internal_id', '1',
    'direction', 'Input',
    'mapping_id', pg_temp.flow_identity_mapping()->>'mapping_id',
    'source_reference', v_source,
    'target_reference', v_target,
    'before_reference_sha256', util.dataset_flow_identity_sha256(v_source),
    'after_reference_sha256', util.dataset_flow_identity_sha256(v_target)
  ));
end;
$$;

create or replace function pg_temp.flow_identity_collision()
returns jsonb
language sql
stable
as $$
  select private.dataset_flow_identity_collision_ledger(
    private.dataset_flow_identity_exchanges(
      pg_temp.flow_identity_process_payload(true)
    ),
    pg_temp.flow_identity_rewrites()
  )
$$;

create or replace function pg_temp.flow_identity_manifest()
returns jsonb
language plpgsql
stable
as $$
declare
  v_process public.processes%rowtype;
  v_before jsonb;
  v_desired jsonb := pg_temp.flow_identity_process_payload(true);
  v_before_sha text;
  v_snapshot jsonb;
  v_closure jsonb := pg_temp.flow_identity_protected_closure();
  v_manifest jsonb;
begin
  select process.* into v_process
  from public.processes as process
  where process.id = pg_temp.flow_identity_id('process')
    and process.version = '01.00.000';
  v_before := v_process.json_ordered::jsonb;
  v_before_sha := util.dataset_flow_identity_sha256(v_before);
  v_snapshot := util.dataset_derivative_rebuild_snapshot(v_process);
  v_manifest := jsonb_build_object(
    'ordinal', 1,
    'id', v_process.id,
    'version', btrim(v_process.version::text),
    'user_id', v_process.user_id,
    'state_code', v_process.state_code,
    'modified_at', v_process.modified_at,
    'model_id', v_process.model_id,
    'rule_verification', v_process.rule_verification,
    'before_row_sha256', util.dataset_flow_identity_sha256(jsonb_build_object(
      'id', v_process.id,
      'version', btrim(v_process.version::text),
      'user_id', v_process.user_id,
      'state_code', v_process.state_code,
      'modified_at', v_process.modified_at,
      'model_id', v_process.model_id,
      'rule_verification', v_process.rule_verification,
      'payload_sha256', v_before_sha
    )),
    'before_payload_sha256', v_before_sha,
    'before_exchange_set_sha256', util.dataset_flow_identity_sha256(
      private.dataset_flow_identity_exchanges(v_before)
    ),
    'before_exchange_count', 3,
    'desired_payload_sha256', util.dataset_flow_identity_sha256(v_desired),
    'desired_exchange_set_sha256', util.dataset_flow_identity_sha256(
      private.dataset_flow_identity_exchanges(v_desired)
    ),
    'rewrite_count', 1,
    'rewrite_set_sha256', util.dataset_flow_identity_sha256(
      pg_temp.flow_identity_rewrites()
    ),
    'collision_ledger_sha256', util.dataset_flow_identity_sha256(
      pg_temp.flow_identity_collision()
    ),
    'rewrites', pg_temp.flow_identity_rewrites(),
    'collision_ledger', pg_temp.flow_identity_collision(),
    'derivative_baseline_snapshot_sha256', v_snapshot->>'snapshot_sha256',
    'process_schema', jsonb_build_object(
      'status', 'pass', 'evidence_sha256', repeat('5', 64)
    ),
    'pending_blocker_closure_sha256',
      util.dataset_flow_identity_sha256(v_closure)
  );
  return v_manifest || jsonb_build_object(
    'process_template_sha256',
    util.dataset_flow_identity_sha256(v_manifest)
  );
end;
$$;

create or replace function pg_temp.flow_identity_protected_intent()
returns jsonb
language plpgsql
stable
as $$
declare
  v_closure jsonb := pg_temp.flow_identity_protected_closure();
  v_pending jsonb;
  v_blockers jsonb;
  v_orphans jsonb;
begin
  select coalesce(jsonb_agg(jsonb_build_object(
    'source_id', entry.value->>'source_id',
    'source_version', entry.value->>'source_version',
    'expected_reference_count', entry.value->'expected_reference_count',
    'occurrences', coalesce((select jsonb_agg(jsonb_build_object(
      'process_id', occurrence.value->>'process_id',
      'process_version', occurrence.value->>'process_version',
      'exchange_index', occurrence.value->'exchange_index',
      'internal_id', occurrence.value->>'internal_id',
      'direction', occurrence.value->>'direction'
    ) order by occurrence.ordinality)
    from jsonb_array_elements(entry.value->'occurrences')
      with ordinality as occurrence(value, ordinality)), '[]'::jsonb),
    'evidence_sha256', entry.value->>'evidence_sha256'
  ) order by entry.ordinality), '[]'::jsonb)
  into v_pending
  from jsonb_array_elements(v_closure->'pending')
    with ordinality as entry(value, ordinality);
  select coalesce(jsonb_agg(jsonb_build_object(
    'source_id', entry.value->>'source_id',
    'source_version', entry.value->>'source_version',
    'expected_reference_count', entry.value->'expected_reference_count',
    'occurrences', coalesce((select jsonb_agg(jsonb_build_object(
      'process_id', occurrence.value->>'process_id',
      'process_version', occurrence.value->>'process_version',
      'exchange_index', occurrence.value->'exchange_index',
      'internal_id', occurrence.value->>'internal_id',
      'direction', occurrence.value->>'direction'
    ) order by occurrence.ordinality)
    from jsonb_array_elements(entry.value->'occurrences')
      with ordinality as occurrence(value, ordinality)), '[]'::jsonb),
    'evidence_sha256', entry.value->>'evidence_sha256'
  ) order by entry.ordinality), '[]'::jsonb)
  into v_blockers
  from jsonb_array_elements(v_closure->'blockers')
    with ordinality as entry(value, ordinality);
  select coalesce(jsonb_agg(jsonb_build_object(
    'source_id', entry.value->>'source_id',
    'source_version', entry.value->>'source_version',
    'evidence_sha256', entry.value->>'evidence_sha256'
  ) order by entry.ordinality), '[]'::jsonb)
  into v_orphans
  from jsonb_array_elements(v_closure->'orphans')
    with ordinality as entry(value, ordinality);
  return jsonb_build_object(
    'schema_version', 'dataset-flow-identity-protected-intent.v2',
    'pending', v_pending, 'blockers', v_blockers, 'orphans', v_orphans
  );
end;
$$;

create or replace function pg_temp.flow_identity_mapping_intent()
returns jsonb
language sql
stable
as $$
  select jsonb_build_object(
    'ordinal', 1,
    'source', jsonb_build_object(
      'id', pg_temp.flow_identity_id('source_flow'),
      'version', '01.00.000',
      'source_trace_sha256', repeat('a', 64)
    ),
    'target', jsonb_build_object(
      'id', pg_temp.flow_identity_id('target_flow'),
      'version', '01.00.000',
      'reference', pg_temp.flow_identity_reference(
        pg_temp.flow_identity_id('target_flow'), '01.00.000', 'Public target'
      )
    ),
    'compatibility', pg_temp.flow_identity_mapping()->'compatibility'
  )
$$;

create or replace function pg_temp.flow_identity_process_intent()
returns jsonb
language sql
stable
as $$
  select jsonb_build_object(
    'ordinal', 1,
    'id', pg_temp.flow_identity_id('process'),
    'version', '01.00.000',
    'rewrites', jsonb_build_array(jsonb_build_object(
      'ordinal', 1, 'exchange_index', 0, 'internal_id', '1',
      'direction', 'Input', 'mapping_ordinal', 1
    )),
    'process_schema', jsonb_build_object(
      'status', 'pass', 'evidence_sha256', repeat('5', 64)
    )
  )
$$;

create or replace function pg_temp.flow_identity_capture()
returns jsonb
language sql
stable
as $$
  select jsonb_build_object(
    'schema_version', 'dataset-flow-identity-capture-attest.v2',
    'request_id', 'fb000000-0000-4000-8000-000000000001',
    'environment', 'local',
    'project_ref', 'local',
    'actor', jsonb_build_object(
      'user_id', pg_temp.flow_identity_id('owner'),
      'email', 'flow-owner@example.com'
    ),
    'target_visibility', 'owner_draft',
    'operation_id', 'flow-identity-test-operation',
    'compatibility_policy', jsonb_build_object(
      'schema_version', 'dataset-flow-identity-compatibility-policy.v1',
      'policy_sha256', repeat('1', 64),
      'evidence_resolution_sha256', repeat('b', 64),
      'approved_at_utc', '2026-07-16T02:00:00Z',
      'approval_text_sha256', repeat('9', 64)
    ),
    'artifact_evidence', jsonb_build_object(
      'review_ledger_sha256', repeat('e', 64),
      'live_capture_artifact_sha256', repeat('f', 64),
      'toolchain_evidence_sha256', repeat('a', 64)
    ),
    'mappings', jsonb_build_array(pg_temp.flow_identity_mapping_intent()),
    'process_intents', jsonb_build_array(pg_temp.flow_identity_process_intent()),
    'protected_closure', pg_temp.flow_identity_protected_intent()
  );
$$;

create or replace function pg_temp.flow_identity_preflight()
returns jsonb
language plpgsql
stable
as $$
declare
  v_capture jsonb;
begin
  select value into v_capture from flow_identity_state where key = 'capture';
  return jsonb_build_object(
    'schema_version', 'dataset-flow-identity-scope-preflight.v2',
    'request_id', 'fa000000-0000-4000-8000-000000000001',
    'receipt_id', v_capture->>'receipt_id',
    'receipt_proof_sha256', v_capture->>'receipt_proof_sha256',
    'environment', 'local', 'project_ref', 'local',
    'actor', jsonb_build_object(
      'user_id', pg_temp.flow_identity_id('owner'),
      'email', 'flow-owner@example.com'
    ),
    'target_visibility', 'owner_draft',
    'operation_id', 'flow-identity-test-operation',
    'plan_sha256', repeat('6', 64),
    'freeze_sha256', repeat('7', 64),
    'policy_approval_text_sha256', repeat('9', 64),
    'execution_approval_request_sha256', repeat('8', 64),
    'execution_approval_text_sha256', repeat('c', 64),
    'execution_approval_identity_sha256', repeat('d', 64),
    'toolchain_evidence_sha256', repeat('a', 64)
  );
end;
$$;

create or replace function pg_temp.flow_identity_bad_capture()
returns jsonb
language sql
stable
as $$
  select jsonb_set(
    pg_temp.flow_identity_capture() || jsonb_build_object(
      'request_id', 'fb000000-0000-4000-8000-000000000099',
      'operation_id', 'flow-identity-bad-later-template'
    ),
    '{process_intents,0,rewrites,0,internal_id}', '"999"'::jsonb, false
  )
$$;

select has_function(
    'public', 'cmd_dataset_flow_identity_scope_preflight_guarded',
    array['jsonb'],
  'scope preflight RPC exists'
);
select has_function(
    'public', 'cmd_dataset_flow_identity_process_rewrite_guarded',
    array['uuid', 'jsonb'],
  'one-process rewrite RPC exists'
);
select has_function(
  'public', 'cmd_dataset_flow_identity_scope_read', array['uuid'],
  'scope read RPC exists'
);
select has_function(
    'public', 'cmd_dataset_flow_identity_scope_finalize_guarded',
    array['uuid', 'jsonb'],
  'scope finalize RPC exists'
);
select has_function(
    'public', 'cmd_dataset_flow_identity_capture_attest_guarded',
    array['jsonb'],
  'v2 database capture/attestation RPC exists'
);
select function_privs_are(
  'public', 'cmd_dataset_flow_identity_scope_preflight_guarded',
  array['jsonb'], 'authenticated', array['EXECUTE'],
  'preflight is authenticated-only'
);
select function_privs_are(
  'public', 'cmd_dataset_flow_identity_process_rewrite_guarded',
  array['uuid', 'jsonb'], 'authenticated', array['EXECUTE'],
  'process mutation is authenticated-only'
);
select function_privs_are(
  'public', 'cmd_dataset_flow_identity_scope_read',
  array['uuid'], 'authenticated', array['EXECUTE'],
  'scope read is authenticated-only'
);
select function_privs_are(
  'public', 'cmd_dataset_flow_identity_scope_finalize_guarded',
  array['uuid', 'jsonb'], 'authenticated', array['EXECUTE'],
  'finalize is authenticated-only'
);
select function_privs_are(
  'public', 'cmd_dataset_flow_identity_capture_attest_guarded',
  array['jsonb'], 'authenticated', array['EXECUTE'],
  'capture attestation is authenticated-only'
);
select is(
  util.dataset_flow_identity_sha256(
    '{"z":"天工","a":[1,1.0,-0.0,0.000001,100000000000000000000],"é":"值","😀":"emoji"}'::jsonb
  ),
  '9335d798454451154be24dc993efb6730888061d4b0a87e288c6e4df75af93df',
  'canonical hash has a fixed CLI-independent Unicode/numeric golden vector'
);
select is(
  util.dataset_flow_identity_source_universe(
    pg_temp.flow_identity_id('owner'),
    pg_temp.flow_identity_source_universe(),
    util.dataset_flow_identity_sha256(pg_temp.flow_identity_source_universe())
  )->>'ok',
  'true',
  'source universe proves the exact live 305-row owner-draft elementary set'
);
select is(
  util.dataset_flow_identity_source_universe(
    pg_temp.flow_identity_id('owner'),
    pg_temp.flow_identity_source_universe() - 304,
    util.dataset_flow_identity_sha256(pg_temp.flow_identity_source_universe() - 304)
  )->>'ok',
  'false',
  'source universe rejects one omitted row'
);
select is(
  util.dataset_flow_identity_source_universe(
    pg_temp.flow_identity_id('owner'),
    (pg_temp.flow_identity_source_universe() - 304)
      || jsonb_build_array(pg_temp.flow_identity_source_universe()->0),
    util.dataset_flow_identity_sha256(
      (pg_temp.flow_identity_source_universe() - 304)
        || jsonb_build_array(pg_temp.flow_identity_source_universe()->0)
    )
  )->>'ok',
  'false',
  'source universe rejects a duplicate that preserves cardinality'
);
select is(
  util.dataset_flow_identity_source_universe(
    pg_temp.flow_identity_id('owner'),
    jsonb_set(
      pg_temp.flow_identity_source_universe(), '{304,id}',
      '"ffffffff-ffff-4fff-8fff-ffffffffffff"'::jsonb, false
    ),
    util.dataset_flow_identity_sha256(jsonb_set(
      pg_temp.flow_identity_source_universe(), '{304,id}',
      '"ffffffff-ffff-4fff-8fff-ffffffffffff"'::jsonb, false
    ))
  )->>'ok',
  'false',
  'source universe rejects a phantom identity that preserves cardinality'
);
select is(
  util.dataset_flow_identity_validate_support_set(
    pg_temp.flow_identity_id('owner'),
    pg_temp.flow_identity_support_snapshots(),
    util.dataset_flow_identity_sha256(pg_temp.flow_identity_support_snapshots())
  )->>'ok',
  'true',
  'full FP/UG support snapshot set reproves owner/state/time/payload/row hashes'
);
select is(
  util.dataset_flow_identity_validate_flow_guard(
    pg_temp.flow_identity_id('owner'),
    jsonb_set(pg_temp.flow_guard(true), '{reference,@uri}',
      to_jsonb('../flows/' || pg_temp.flow_identity_id('target_flow')::text
        || '_01.00.000.json'), false),
    true, pg_temp.flow_identity_support_snapshots()
  )->>'ok',
  'false',
  'target guard rejects a non-canonical .json URI'
);
select is(
  util.dataset_flow_identity_validate_flow_guard(
    pg_temp.flow_identity_id('owner'),
    jsonb_set(pg_temp.flow_guard(true), '{reference,@refObjectId}',
      to_jsonb(pg_temp.flow_identity_id('source_flow')::text), false),
    true, pg_temp.flow_identity_support_snapshots()
  )->>'ok',
  'false',
  'target guard rejects a reference with the wrong target id'
);
select is(
  util.dataset_flow_identity_validate_flow_guard(
    pg_temp.flow_identity_id('owner'),
    jsonb_set(pg_temp.flow_guard(true), '{reference,@version}',
      '"02.00.000"'::jsonb, false),
    true, pg_temp.flow_identity_support_snapshots()
  )->>'ok',
  'false',
  'target guard rejects a reference with the wrong target version'
);
select is(
  util.dataset_flow_identity_protected_closure(
    pg_temp.flow_identity_id('owner'),
    jsonb_set(
      jsonb_set(
        pg_temp.flow_identity_protected_closure(), '{orphans,0,source_id}',
        to_jsonb(pg_temp.flow_identity_id('source_flow')::text), false
      ),
      '{orphan_set_sha256}',
      to_jsonb(util.dataset_flow_identity_sha256(
        jsonb_set(
          pg_temp.flow_identity_protected_closure()->'orphans',
          '{0,source_id}',
          to_jsonb(pg_temp.flow_identity_id('source_flow')::text), false
        )
      )), false
    )
  )->>'ok',
  'false',
  'an alleged orphan with a live process reference is rejected'
);
select ok(
  pg_get_functiondef(
    'private.dataset_flow_identity_whole_scope_proof_v2(uuid,uuid,uuid,boolean)'::regprocedure
  ) like '%read_dataset_flow_identity_derivative_set%',
  'whole-scope verifier uses one set-based derivative proof'
);
select ok(
  pg_get_functiondef(
    'public.cmd_dataset_flow_identity_scope_finalize_guarded(uuid,jsonb)'::regprocedure
  ) not like '%read_dataset_derivative_rebuild_batch_any%',
  'installed finalize contains no per-child derivative reader'
);
select ok(
  pg_get_functiondef(
    'public.cmd_dataset_flow_identity_scope_preflight_guarded(jsonb)'::regprocedure
  ) not ilike '%lock table%',
  'preflight takes no table-level SHARE locks'
);
select is(
  private.dataset_flow_identity_collision_ledger(
    private.dataset_flow_identity_exchanges(
      pg_temp.flow_identity_process_payload(true)
    ),
    pg_temp.flow_identity_rewrites()
  ) #> '{entries,0,mapping_ids}',
  jsonb_build_array(
    pg_temp.flow_identity_mapping()->>'mapping_id', null
  ),
  'collision ledger uses mapping id for rewritten row and JSON null for existing target row'
);
select is(
  util.dataset_flow_identity_protected_closure(
    pg_temp.flow_identity_id('owner'),
    pg_temp.flow_identity_protected_closure()
  )->>'ok',
  'true',
  'protected closure proves exact process/index/internal-id/direction occurrence'
);

set local role authenticated;
select set_config('request.jwt.claim.role', 'authenticated', true);
select set_config(
  'request.jwt.claim.sub', pg_temp.flow_identity_id('owner')::text, true
);
select set_config(
  'request.jwt.claim.email', 'flow-owner@example.com', true
);

select is(
  public.cmd_dataset_flow_identity_capture_attest_guarded(
    pg_temp.flow_identity_bad_capture()
  )->>'code',
  'FLOW_IDENTITY_CAPTURE_FAILED',
  'a bad semantic process intent fails during DB-owned capture materialization'
);
select is(
  (select count(*)::integer
   from util.dataset_flow_identity_capture_receipts
   where operation_id = 'flow-identity-bad-later-template'),
  0,
  'bad semantic intent causes zero durable receipt or primary writes'
);

insert into flow_identity_state(key, value)
select 'capture', public.cmd_dataset_flow_identity_capture_attest_guarded(
  pg_temp.flow_identity_capture()
);
select is(
  (select value->>'ok' from flow_identity_state where key = 'capture'),
  'true',
  'fresh semantic capture returns a DB-owned immutable receipt'
);
select is(
  public.cmd_dataset_flow_identity_capture_attest_guarded(
    pg_temp.flow_identity_capture()
  )->>'replay',
  'true',
  'exact capture replay returns the same immutable receipt'
);

insert into flow_identity_state(key, value)
select 'preflight',
  public.cmd_dataset_flow_identity_scope_preflight_guarded(
    pg_temp.flow_identity_preflight()
  );

select is(
  (select value->>'ok' from flow_identity_state where key = 'preflight'),
  'true',
  'fresh exact scope preflight succeeds'
);
select is(
  (select value->>'status' from flow_identity_state where key = 'preflight'),
  'sealed',
  'preflight returns sealed status'
);
select is(
  (select value->>'receipt_id' from flow_identity_state where key = 'preflight'),
  (select value->>'receipt_id' from flow_identity_state where key = 'capture'),
  'sealed scope response exactly binds the DB capture receipt'
);
select is(
  public.cmd_dataset_flow_identity_scope_preflight_guarded(
    pg_temp.flow_identity_preflight()
  )->>'replay',
  'true',
  'exact preflight replay returns the durable scope without duplication'
);
select is(
  public.cmd_dataset_flow_identity_scope_preflight_guarded(
    jsonb_set(
      pg_temp.flow_identity_preflight(),
      '{freeze_sha256}', to_jsonb(repeat('0', 64)), false
    )
  )->>'code',
  'FLOW_IDENTITY_PREFLIGHT_OPERATION_REUSE_MISMATCH',
  'tampered sealed artifact fails closed before scope reuse'
);
select is(
  (select jsonb_build_object(
    'derivative_batch_id_is_null',
      result #> '{processes,0,derivative_batch_id}' = 'null'::jsonb,
    'derivative_request_id_is_null',
      result #> '{processes,0,derivative_request_id}' = 'null'::jsonb,
    'derivative_status_is_null',
      result #> '{processes,0,derivative_status}' = 'null'::jsonb
  )
  from public.cmd_dataset_flow_identity_scope_read(
    (select (value->>'scope_id')::uuid
      from flow_identity_state where key = 'preflight')
  ) as result),
  jsonb_build_object(
    'derivative_batch_id_is_null', true,
    'derivative_request_id_is_null', true,
    'derivative_status_is_null', true
  ),
  'sealed pending process has exact NULL derivative batch/request/status fields'
);

insert into flow_identity_state(key, value)
select 'process_request', request || jsonb_build_object(
  'process_request_sha256',
    util.dataset_flow_identity_restricted_sha256_v2(request)
)
from (
  select jsonb_build_object(
    'schema_version', 'dataset-flow-identity-process-rewrite.v2',
    'request_id', 'fa000000-0000-4000-8000-000000000002',
    'scope_proof_sha256', preflight.value->>'scope_proof_sha256',
    'ordinal', 1,
    'process_intent_proof_sha256', ledger.process_intent_proof_sha256
  ) as request
  from flow_identity_state as preflight
  join util.dataset_flow_identity_process_ledger as ledger
    on ledger.scope_id = (preflight.value->>'scope_id')::uuid
    and ledger.ordinal = 1
  where preflight.key = 'preflight'
) as prepared;

insert into flow_identity_state(key, value)
select 'process_result',
  public.cmd_dataset_flow_identity_process_rewrite_guarded(
    (select (value->>'scope_id')::uuid
      from flow_identity_state where key = 'preflight'),
    value
  )
from flow_identity_state where key = 'process_request';

select is(
  (select value->>'ok' from flow_identity_state where key = 'process_result'),
  'true',
  'one-process guarded rewrite succeeds'
);
select is(
  (select value->>'replay' from flow_identity_state where key = 'process_result'),
  'false',
  'first one-process call is not a replay'
);
select is(
  (select value->>'completed_process_count'
   from flow_identity_state where key = 'process_result'),
  '1',
  'process success returns the exact post-commit completed count'
);
select is(
  (select json #>>
    '{processDataSet,exchanges,exchange,0,referenceToFlowDataSet,@refObjectId}'
    from public.processes
    where id = pg_temp.flow_identity_id('process')),
  pg_temp.flow_identity_id('target_flow')::text,
  'approved source reference is rewritten to exact public identity'
);
select is(
  (select json #>> '{processDataSet,exchanges,exchange,0,meanAmount}'
    from public.processes
    where id = pg_temp.flow_identity_id('process')),
  '5',
  'amount remains byte-semantically unchanged'
);
select is(
  (select json #>> '{processDataSet,exchanges,exchange,0,generalComment,#text}'
    from public.processes
    where id = pg_temp.flow_identity_id('process')),
  'preserve me',
  'comment remains unchanged'
);
select is(
  (select json #>>
    '{processDataSet,exchanges,exchange,2,referenceToFlowDataSet,@refObjectId}'
    from public.processes
    where id = pg_temp.flow_identity_id('process')),
  pg_temp.flow_identity_id('pending_flow')::text,
  'pending reference remains unchanged'
);
select is(
  (select jsonb_array_length(json #> '{processDataSet,exchanges,exchange}')
    from public.processes
    where id = pg_temp.flow_identity_id('process')),
  3,
  'exchange count and converged rows are preserved'
);
select is(
  (select count(*)::integer from public.command_audit_log
    where command = 'cmd_dataset_flow_identity_process_rewrite_guarded'
      and target_id = pg_temp.flow_identity_id('process')),
  1,
  'one unique primary audit is recorded'
);
select is(
  (select count(*)::integer
    from util.dataset_derivative_rebuild_requests as child
    where child.batch_id = (
      select (value->>'derivative_batch_id')::uuid
      from flow_identity_state where key = 'process_result'
    )),
  1,
  'one protected derivative child is admitted atomically'
);
select is(
  util.read_dataset_derivative_rebuild_batch_any(
    pg_temp.flow_identity_id('owner'),
    (select (value->>'derivative_batch_id')::uuid
      from flow_identity_state where key = 'process_result')
  )->>'target_count',
  '1',
  'dynamic derivative reader accepts a one-target Step 3 batch'
);
select is(
  public.cmd_dataset_flow_identity_process_rewrite_guarded(
    (select (value->>'scope_id')::uuid
      from flow_identity_state where key = 'preflight'),
    (select value from flow_identity_state where key = 'process_request')
  )->>'replay',
  'true',
  'exact lost-response replay is proof-only'
);
select is(
  (select count(*)::integer from public.command_audit_log
    where command = 'cmd_dataset_flow_identity_process_rewrite_guarded'
      and target_id = pg_temp.flow_identity_id('process')),
  1,
  'exact replay does not create a second audit or write'
);
select is(
  public.cmd_dataset_flow_identity_scope_read(
    (select (value->>'scope_id')::uuid
      from flow_identity_state where key = 'preflight')
  )->>'status',
  'derivatives_pending',
  'scope read resumes from durable derivative-pending state'
);
select is(
  public.cmd_dataset_flow_identity_scope_read(
    (select (value->>'scope_id')::uuid
      from flow_identity_state where key = 'preflight')
  )->>'protected_closure_current',
  'true',
  'scope read reproves protected occurrence closure'
);

insert into flow_identity_state(key, value)
select 'finalize_request', jsonb_build_object(
  'schema_version', 'dataset-flow-identity-scope-finalize.v2',
  'request_id', 'fa000000-0000-4000-8000-000000000003',
  'scope_proof_sha256', preflight.value->>'scope_proof_sha256',
  'expected', jsonb_build_object(
    'process_count', 1,
    'rewrite_count', 1,
    'completed_process_count', 1
  )
)
from flow_identity_state as preflight
where preflight.key = 'preflight'
;

insert into flow_identity_state(key, value)
select 'finalize_result',
  public.cmd_dataset_flow_identity_scope_finalize_guarded(
    (select (value->>'scope_id')::uuid
      from flow_identity_state where key = 'preflight'),
    (select value from flow_identity_state where key = 'finalize_request')
  );

select is(
  (select value->>'status'
    from flow_identity_state where key = 'finalize_result'),
  'derivatives_pending',
  'finalize proves primary/protected closure but waits for causal derivatives'
);
select is(
  (select state_code from public.flows
    where id = pg_temp.flow_identity_id('source_flow')),
  0,
  'source flow remains an owner draft and is never mutated'
);
select is(
  (select state_code from public.flows
    where id = pg_temp.flow_identity_id('target_flow')),
  100,
  'public target remains published and read-only'
);

-- A terminal derivative failure must not make the committed process
-- transaction replayable.  It yields an exact derivative-only compensation
-- target; admission still requires a separate plan/freeze/approval outside
-- this scope.
reset role;
update util.dataset_derivative_rebuild_requests
set
  status = 'failed',
  phase = 'failed',
  terminal_at = clock_timestamp(),
  drained_at = clock_timestamp(),
  failure_release_not_before = clock_timestamp(),
  last_error = jsonb_build_object('code', 'TEST_TERMINAL_FAILURE')
where batch_id = (
  select (value->>'derivative_batch_id')::uuid
  from flow_identity_state where key = 'process_result'
);
set local role authenticated;

insert into flow_identity_state(key, value)
select 'failed_scope_read', public.cmd_dataset_flow_identity_scope_read(
  (select (value->>'scope_id')::uuid
    from flow_identity_state where key = 'preflight')
);
select is(
  (select value->>'status'
    from flow_identity_state where key = 'failed_scope_read'),
  'derivatives_pending',
  'terminal derivative failure leaves the primary scope recoverable'
);
select is(
  (select value->>'compensation_required'
    from flow_identity_state where key = 'failed_scope_read'),
  'true',
  'scope read explicitly requires a separately approved compensation'
);
select is(
  (select value->>'code'
    from flow_identity_state where key = 'failed_scope_read'),
  'FLOW_IDENTITY_DERIVATIVE_COMPENSATION_REQUIRED',
  'failed scope read emits the exact CLI compensation code'
);
select is(
  (select jsonb_build_object(
      'original_status', value #>> '{compensation_targets,0,original_status}',
      'original_code', value #>> '{compensation_targets,0,original_code}'
    ) from flow_identity_state where key = 'failed_scope_read'),
  jsonb_build_object(
    'original_status', 'failed',
    'original_code', 'TEST_TERMINAL_FAILURE'
  ),
  'failed scope read binds exact original status and error code'
);
select is(
  (select jsonb_agg(keys.object_key order by keys.object_key)
   from flow_identity_state state
   cross join lateral jsonb_object_keys(
     state.value #> '{compensation_targets,0}'
   ) as keys(object_key)
   where state.key = 'failed_scope_read'),
  to_jsonb(array[
    'automatic_retry', 'components', 'current_json_ordered_sha256',
    'current_modified_at', 'current_snapshot_sha256',
    'desired_payload_sha256', 'id', 'latest_compensation_plan_sha256',
    'latest_compensation_request_id', 'latest_compensation_status',
    'operation_id_prefix', 'ordinal', 'original_batch_id', 'original_code',
    'original_error', 'original_request_id', 'original_status', 'reason_code',
    'requires_new_plan_freeze_approval', 'table', 'version'
  ]::text[]),
  'scope-read compensation target has the exact CLI response keys'
);
select is(
  (select value #>> '{compensation_targets,0,current_snapshot_sha256}'
    from flow_identity_state where key = 'failed_scope_read'),
  (select util.dataset_derivative_rebuild_snapshot(process)->>'snapshot_sha256'
    from public.processes as process
    where process.id = pg_temp.flow_identity_id('process')),
  'scope read returns the exact current derivative snapshot hash'
);

insert into flow_identity_state(key, value)
select 'failed_finalize',
  public.cmd_dataset_flow_identity_scope_finalize_guarded(
    (select (value->>'scope_id')::uuid
      from flow_identity_state where key = 'preflight'),
    (select value from flow_identity_state where key = 'finalize_request')
  );
select is(
  (select value->>'code'
    from flow_identity_state where key = 'failed_finalize'),
  'FLOW_IDENTITY_DERIVATIVE_COMPENSATION_REQUIRED',
  'finalize reports compensation instead of terminally failing the scope'
);
select is(
  (select jsonb_build_object(
      'original_status', value #>> '{compensation_targets,0,original_status}',
      'original_code', value #>> '{compensation_targets,0,original_code}'
    ) from flow_identity_state where key = 'failed_finalize'),
  jsonb_build_object(
    'original_status', 'failed',
    'original_code', 'TEST_TERMINAL_FAILURE'
  ),
  'failed finalize compensation binds original status and error code'
);
select is(
  (select jsonb_agg(keys.object_key order by keys.object_key)
   from flow_identity_state state
   cross join lateral jsonb_object_keys(
     state.value #> '{compensation_targets,0}'
   ) as keys(object_key)
   where state.key = 'failed_finalize'),
  to_jsonb(array[
    'automatic_retry', 'components', 'current_json_ordered_sha256',
    'current_modified_at', 'current_snapshot_sha256',
    'desired_payload_sha256', 'id', 'latest_compensation_plan_sha256',
    'latest_compensation_request_id', 'latest_compensation_status',
    'operation_id_prefix', 'ordinal', 'original_batch_id', 'original_code',
    'original_status', 'reason_code', 'requires_new_plan_freeze_approval',
    'table', 'version'
  ]::text[]),
  'finalize compensation target has the exact CLI response keys'
);

reset role;
update util.dataset_derivative_rebuild_requests
set status = 'stale', phase = 'failed',
  last_error = jsonb_build_object('code', 'TEST_TERMINAL_STALE')
where batch_id = (
  select (value->>'derivative_batch_id')::uuid
  from flow_identity_state where key = 'process_result'
);
set local role authenticated;
insert into flow_identity_state(key, value)
select 'stale_scope_read', public.cmd_dataset_flow_identity_scope_read(
  (select (value->>'scope_id')::uuid
    from flow_identity_state where key = 'preflight')
);
select is(
  (select value->>'code'
    from flow_identity_state where key = 'stale_scope_read'),
  'FLOW_IDENTITY_DERIVATIVE_COMPENSATION_REQUIRED',
  'stale scope read emits the exact CLI compensation code'
);
select is(
  (select jsonb_build_object(
      'original_status', value #>> '{compensation_targets,0,original_status}',
      'original_code', value #>> '{compensation_targets,0,original_code}'
    ) from flow_identity_state where key = 'stale_scope_read'),
  jsonb_build_object(
    'original_status', 'stale',
    'original_code', 'TEST_TERMINAL_STALE'
  ),
  'stale scope read binds exact original status and error code'
);
insert into flow_identity_state(key, value)
select 'stale_finalize',
  public.cmd_dataset_flow_identity_scope_finalize_guarded(
    (select (value->>'scope_id')::uuid
      from flow_identity_state where key = 'preflight'),
    (select value from flow_identity_state where key = 'finalize_request')
  );
select is(
  (select jsonb_build_object(
      'original_status', value #>> '{compensation_targets,0,original_status}',
      'original_code', value #>> '{compensation_targets,0,original_code}'
    ) from flow_identity_state where key = 'stale_finalize'),
  jsonb_build_object(
    'original_status', 'stale',
    'original_code', 'TEST_TERMINAL_STALE'
  ),
  'stale finalize compensation binds original status and error code'
);
reset role;
update util.dataset_derivative_rebuild_requests
set status = 'failed', phase = 'failed',
  last_error = jsonb_build_object('code', 'TEST_TERMINAL_FAILURE')
where batch_id = (
  select (value->>'derivative_batch_id')::uuid
  from flow_identity_state where key = 'process_result'
);
set local role authenticated;

insert into flow_identity_state(key, value)
select 'compensation_plan', jsonb_build_object(
  'schema_version', 'dataset-derivative-rebuild-plan.v1',
  'plan_sha256', repeat('c', 64),
  'operation_id', 'FLOW_IDENTITY_SCOPE_COMPENSATION:'
    || (preflight.value->>'scope_id') || ':1:attempt-1',
  'target_visibility', 'owner_draft',
  'actions', jsonb_build_array(jsonb_build_object(
    'action_id', 'FLOW_IDENTITY_SCOPE_COMPENSATION:'
      || (preflight.value->>'scope_id') || ':1:attempt-1',
    'action', 'rebuild_derivatives',
    'table', 'processes',
    'id', pg_temp.flow_identity_id('process'),
    'version', '01.00.000',
    'expected_state_code', 0,
    'expected_snapshot_sha256', snapshot.value->>'snapshot_sha256',
    'components', jsonb_build_array('extracted_md', 'embedding_ft'),
    'reason_code', 'FLOW_IDENTITY_SCOPE_COMPENSATION:'
      || (preflight.value->>'scope_id') || ':1'
  ))
)
from flow_identity_state as preflight
cross join public.processes as process
cross join lateral (
  select util.dataset_derivative_rebuild_snapshot(process) as value
) as snapshot
where preflight.key = 'preflight'
  and process.id = pg_temp.flow_identity_id('process');

insert into flow_identity_state(key, value)
select 'compensation_result',
  public.cmd_dataset_derivative_rebuild_plan_guarded(value)
from flow_identity_state where key = 'compensation_plan';
select is(
  (select value->>'ok'
    from flow_identity_state where key = 'compensation_result'),
  'true',
  'a separately frozen derivative-only compensation can be admitted'
);
select is(
  util.read_dataset_derivative_rebuild_batch_any(
    pg_temp.flow_identity_id('owner'),
    (select (value->>'request_id')::uuid
      from flow_identity_state where key = 'compensation_result')
  )->>'reference_kind',
  'request',
  'dynamic causal reader accepts the exact single compensation request id'
);

insert into flow_identity_state(key, value)
select 'compensation_finalize',
  public.cmd_dataset_flow_identity_scope_finalize_guarded(
    (select (value->>'scope_id')::uuid
      from flow_identity_state where key = 'preflight'),
    (select value from flow_identity_state where key = 'finalize_request')
  );
select is(
  (select value->>'code'
    from flow_identity_state where key = 'compensation_finalize'),
  'FLOW_IDENTITY_DERIVATIVES_PENDING',
  'finalize follows the exact compensation request without replaying primary'
);
select is(
  util.read_dataset_flow_identity_derivative_set(
    pg_temp.flow_identity_id('owner'),
    (select (value->>'scope_id')::uuid
      from flow_identity_state where key = 'preflight')
  ) #>> '{targets,0,effective_reference_kind}',
  'separate_compensation',
  'finalize binds the separately approved compensation as effective proof'
);

reset role;
select pg_temp.complete_flow_identity_derivative(
  (select (value->>'request_id')::uuid
    from flow_identity_state where key = 'compensation_result'),
  'first compensation'
);
set local role authenticated;

insert into flow_identity_state(key, value)
select 'completed_finalize',
  public.cmd_dataset_flow_identity_scope_finalize_guarded(
    (select (value->>'scope_id')::uuid
      from flow_identity_state where key = 'preflight'),
    (select value from flow_identity_state where key = 'finalize_request')
  );
select is(
  (select value->>'status'
    from flow_identity_state where key = 'completed_finalize'),
  'completed',
  'completed separate compensation permits one real terminal finalize'
);

insert into flow_identity_state(key, value)
select 'completed_scope_history', to_jsonb(scope)
from util.dataset_flow_identity_scopes as scope
where scope.id = (
  select (value->>'scope_id')::uuid
  from flow_identity_state where key = 'preflight'
);
insert into flow_identity_state(key, value)
select 'completed_terminal_audit', to_jsonb(audit)
from public.command_audit_log as audit
where audit.command = 'cmd_dataset_flow_identity_scope_finalize_guarded'
  and audit.target_table is null
  and audit.payload->>'scope_id' = (
    select value->>'scope_id'
    from flow_identity_state where key = 'preflight'
  )
order by audit.id desc limit 1;

select is(
  public.cmd_dataset_flow_identity_scope_finalize_guarded(
    (select (value->>'scope_id')::uuid
      from flow_identity_state where key = 'preflight'),
    (select value from flow_identity_state where key = 'finalize_request')
  )->>'replay',
  'true',
  'completed finalize replay is proof-only'
);
select is(
  (select to_jsonb(scope)
   from util.dataset_flow_identity_scopes as scope
   where scope.id = (
     select (value->>'scope_id')::uuid
     from flow_identity_state where key = 'preflight'
   )),
  (select value from flow_identity_state where key = 'completed_scope_history'),
  'completed finalize replay leaves the scope row byte-identical'
);
select is(
  (select to_jsonb(audit)
   from public.command_audit_log as audit
   where audit.command = 'cmd_dataset_flow_identity_scope_finalize_guarded'
     and audit.target_table is null
     and audit.payload->>'scope_id' = (
       select value->>'scope_id'
       from flow_identity_state where key = 'preflight'
     )
   order by audit.id desc limit 1),
  (select value from flow_identity_state where key = 'completed_terminal_audit'),
  'completed finalize replay leaves the terminal audit byte-identical'
);

-- Completed history remains immutable while live derivative proof can drift.
reset role;
update public.processes
set extracted_md = 'post-completion derivative drift'
where id = pg_temp.flow_identity_id('process');
set local role authenticated;
insert into flow_identity_state(key, value)
select 'completed_scope_derivative_drift',
  public.cmd_dataset_flow_identity_scope_read(
    (select (value->>'scope_id')::uuid
      from flow_identity_state where key = 'preflight')
  );
select is(
  (select value->>'status' from flow_identity_state
    where key = 'completed_scope_derivative_drift'),
  'derivatives_pending',
  'completed scope response downgrades when live derivative proof drifts'
);
select is(
  (select value->>'derivatives_current' from flow_identity_state
    where key = 'completed_scope_derivative_drift'),
  'false',
  'completed scope never reports derivatives current from persisted status'
);
select is(
  (select value #>> '{derivative_set_proof,status}'
    from flow_identity_state where key = 'completed_scope_derivative_drift'),
  'compensation_required',
  'scope read returns the complete live derivative-set failure proof'
);
select is(
  (select value->'derivative_set_proof' from flow_identity_state
    where key = 'completed_scope_derivative_drift'),
  util.read_dataset_flow_identity_derivative_set(
    pg_temp.flow_identity_id('owner'),
    (select (value->>'scope_id')::uuid
      from flow_identity_state where key = 'preflight')
  ),
  'scope read nests the exact unmodified dynamic derivative-set proof'
);
select is(
  (select value->>'derivative_proof_set_sha256'
    from flow_identity_state where key = 'completed_scope_derivative_drift'),
  (select value #>> '{derivative_set_proof,proof_sha256}'
    from flow_identity_state where key = 'completed_scope_derivative_drift'),
  'scope-read proof SHA convenience field is derived from the nested proof'
);
select is(
  (select jsonb_build_object(
    'original_status', value #>> '{compensation_targets,0,original_status}',
    'original_code', value #>> '{compensation_targets,0,original_code}'
  ) from flow_identity_state where key = 'completed_scope_derivative_drift'),
  jsonb_build_object(
    'original_status', 'failed',
    'original_code', 'TEST_TERMINAL_FAILURE'
  ),
  'post-completion causal drift preserves the existing failed-child identity'
);
select is(
  (select scope.status
   from util.dataset_flow_identity_scopes scope
   join flow_identity_state preflight
     on scope.id = (preflight.value->>'scope_id')::uuid
   where preflight.key = 'preflight'),
  'completed',
  'read-time derivative drift never rewrites completed scope history'
);
select is(
  (select to_jsonb(scope)
   from util.dataset_flow_identity_scopes as scope
   where scope.id = (
     select (value->>'scope_id')::uuid
     from flow_identity_state where key = 'preflight'
   )),
  (select value from flow_identity_state where key = 'completed_scope_history'),
  'derivative drift does not mutate completed scope history'
);
select is(
  (select to_jsonb(audit)
   from public.command_audit_log as audit
   where audit.command = 'cmd_dataset_flow_identity_scope_finalize_guarded'
     and audit.target_table is null
     and audit.payload->>'scope_id' = (
       select value->>'scope_id'
       from flow_identity_state where key = 'preflight'
     )
   order by audit.id desc limit 1),
  (select value from flow_identity_state where key = 'completed_terminal_audit'),
  'derivative drift does not mutate completed terminal audit history'
);

insert into flow_identity_state(key, value)
select 'completed_drift_finalize',
  public.cmd_dataset_flow_identity_scope_finalize_guarded(
    (select (value->>'scope_id')::uuid
      from flow_identity_state where key = 'preflight'),
    (select value from flow_identity_state where key = 'finalize_request')
  );
select is(
  (select jsonb_build_object(
    'status', value->>'status',
    'code', value->>'code',
    'replay', value->>'replay'
  ) from flow_identity_state where key = 'completed_drift_finalize'),
  jsonb_build_object(
    'status', 'derivatives_pending',
    'code', 'FLOW_IDENTITY_DERIVATIVE_COMPENSATION_REQUIRED',
    'replay', 'true'
  ),
  'completed finalize dynamically reports derivative compensation without writing'
);
select is(
  (select to_jsonb(scope)
   from util.dataset_flow_identity_scopes as scope
   where scope.id = (
     select (value->>'scope_id')::uuid
     from flow_identity_state where key = 'preflight'
   )),
  (select value from flow_identity_state where key = 'completed_scope_history'),
  'drifted completed finalize leaves the scope row byte-identical'
);
select is(
  (select to_jsonb(audit)
   from public.command_audit_log as audit
   where audit.command = 'cmd_dataset_flow_identity_scope_finalize_guarded'
     and audit.target_table is null
     and audit.payload->>'scope_id' = (
       select value->>'scope_id'
       from flow_identity_state where key = 'preflight'
     )
   order by audit.id desc limit 1),
  (select value from flow_identity_state where key = 'completed_terminal_audit'),
  'drifted completed finalize leaves the terminal audit byte-identical'
);

-- A missing protected derivative child is a parseable compensation state,
-- not NULL status/phase ambiguity and not primary live drift.
reset role;
delete from util.dataset_derivative_rebuild_requests
where batch_id = (
    select (value->>'derivative_batch_id')::uuid
    from flow_identity_state where key = 'process_result'
  )
  or id = (
    select (value->>'request_id')::uuid
    from flow_identity_state where key = 'compensation_result'
  );
set local role authenticated;
insert into flow_identity_state(key, value)
select 'missing_child_read', public.cmd_dataset_flow_identity_scope_read(
  (select (value->>'scope_id')::uuid
    from flow_identity_state where key = 'preflight')
);
select is(
  (select jsonb_build_object(
    'status', value #>> '{derivative_set_proof,targets,0,status}',
    'request_status',
      value #>> '{derivative_set_proof,targets,0,request_status}',
    'phase', value #>> '{derivative_set_proof,targets,0,phase}',
    'effective_reference_id_is_null',
      value #> '{derivative_set_proof,targets,0,effective_reference_id}'
        = 'null'::jsonb,
    'lineage_ok', value #>> '{derivative_set_proof,targets,0,lineage_ok}',
    'causal_terminal_proof',
      value #>> '{derivative_set_proof,targets,0,causal_terminal_proof}'
  ) from flow_identity_state where key = 'missing_child_read'),
  jsonb_build_object(
    'status', 'failed',
    'request_status', 'missing',
    'phase', 'missing',
    'effective_reference_id_is_null', true,
    'lineage_ok', 'false',
    'causal_terminal_proof', 'false'
  ),
  'missing child exposes the exact nullable-reference sentinel proof'
);
select is(
  (select jsonb_build_object(
    'derivative_request_id_is_null',
      value #> '{processes,0,derivative_request_id}' = 'null'::jsonb,
    'derivative_status', value #>> '{processes,0,derivative_status}',
    'code', value->>'code',
    'compensation_required', value->>'compensation_required'
  ) from flow_identity_state where key = 'missing_child_read'),
  jsonb_build_object(
    'derivative_request_id_is_null', true,
    'derivative_status', 'missing',
    'code', 'FLOW_IDENTITY_DERIVATIVE_COMPENSATION_REQUIRED',
    'compensation_required', 'true'
  ),
  'scope process and status envelope expose the exact missing-child contract'
);
select is(
  (select jsonb_build_object(
    'count', jsonb_array_length(value->'compensation_targets'),
    'original_request_id_is_null',
      value #> '{compensation_targets,0,original_request_id}' = 'null'::jsonb,
    'original_status',
      value #>> '{compensation_targets,0,original_status}',
    'original_code', value #>> '{compensation_targets,0,original_code}'
  ) from flow_identity_state where key = 'missing_child_read'),
  jsonb_build_object(
    'count', 1,
    'original_request_id_is_null', true,
    'original_status', 'missing',
    'original_code', 'DERIVATIVE_BATCH_CHILD_MISSING'
  ),
  'missing child always emits one exact separately approvable compensation target'
);

insert into flow_identity_state(key, value)
select 'missing_child_finalize',
  public.cmd_dataset_flow_identity_scope_finalize_guarded(
    (select (value->>'scope_id')::uuid
      from flow_identity_state where key = 'preflight'),
    (select value from flow_identity_state where key = 'finalize_request')
  );
select is(
  (select jsonb_build_object(
    'status', value->>'status', 'code', value->>'code',
    'compensation_required', value->>'compensation_required',
    'replay', value->>'replay'
  ) from flow_identity_state where key = 'missing_child_finalize'),
  jsonb_build_object(
    'status', 'derivatives_pending',
    'code', 'FLOW_IDENTITY_DERIVATIVE_COMPENSATION_REQUIRED',
    'compensation_required', 'true',
    'replay', 'true'
  ),
  'completed finalize exposes missing-child compensation without history writes'
);

insert into flow_identity_state(key, value)
select 'second_compensation_plan', jsonb_build_object(
  'schema_version', 'dataset-derivative-rebuild-plan.v1',
  'plan_sha256', repeat('d', 64),
  'operation_id', 'FLOW_IDENTITY_SCOPE_COMPENSATION:'
    || (preflight.value->>'scope_id') || ':1:attempt-2',
  'target_visibility', 'owner_draft',
  'actions', jsonb_build_array(jsonb_build_object(
    'action_id', 'FLOW_IDENTITY_SCOPE_COMPENSATION:'
      || (preflight.value->>'scope_id') || ':1:attempt-2',
    'action', 'rebuild_derivatives',
    'table', 'processes',
    'id', pg_temp.flow_identity_id('process'),
    'version', '01.00.000',
    'expected_state_code', 0,
    'expected_snapshot_sha256', snapshot.value->>'snapshot_sha256',
    'components', jsonb_build_array('extracted_md', 'embedding_ft'),
    'reason_code', 'FLOW_IDENTITY_SCOPE_COMPENSATION:'
      || (preflight.value->>'scope_id') || ':1'
  ))
)
from flow_identity_state as preflight
cross join public.processes as process
cross join lateral (
  select util.dataset_derivative_rebuild_snapshot(process) as value
) as snapshot
where preflight.key = 'preflight'
  and process.id = pg_temp.flow_identity_id('process');

insert into flow_identity_state(key, value)
select 'second_compensation_result',
  public.cmd_dataset_derivative_rebuild_plan_guarded(value)
from flow_identity_state where key = 'second_compensation_plan';
select is(
  (select value->>'ok'
    from flow_identity_state where key = 'second_compensation_result'),
  'true',
  'missing child can be recovered only by a new separately admitted compensation'
);
select is(
  util.read_dataset_flow_identity_derivative_set(
    pg_temp.flow_identity_id('owner'),
    (select (value->>'scope_id')::uuid
      from flow_identity_state where key = 'preflight')
  ) #>> '{targets,0,effective_reference_kind}',
  'separate_compensation',
  'new compensation replaces the missing protected child as effective proof'
);

reset role;
select pg_temp.complete_flow_identity_derivative(
  (select (value->>'request_id')::uuid
    from flow_identity_state where key = 'second_compensation_result'),
  'second compensation'
);
set local role authenticated;
insert into flow_identity_state(key, value)
select 'recovered_completed_finalize',
  public.cmd_dataset_flow_identity_scope_finalize_guarded(
    (select (value->>'scope_id')::uuid
      from flow_identity_state where key = 'preflight'),
    (select value from flow_identity_state where key = 'finalize_request')
  );
select is(
  (select jsonb_build_object(
    'status', value->>'status',
    'derivatives_current', value->>'derivatives_current',
    'replay', value->>'replay'
  ) from flow_identity_state where key = 'recovered_completed_finalize'),
  jsonb_build_object(
    'status', 'completed',
    'derivatives_current', 'true',
    'replay', 'true'
  ),
  'completed separate compensation restores dynamic terminal proof on replay'
);
select is(
  (select to_jsonb(scope)
   from util.dataset_flow_identity_scopes as scope
   where scope.id = (
     select (value->>'scope_id')::uuid
     from flow_identity_state where key = 'preflight'
   )),
  (select value from flow_identity_state where key = 'completed_scope_history'),
  'recovered completed finalize leaves original terminal scope byte-identical'
);
select is(
  (select to_jsonb(audit)
   from public.command_audit_log as audit
   where audit.command = 'cmd_dataset_flow_identity_scope_finalize_guarded'
     and audit.target_table is null
     and audit.payload->>'scope_id' = (
       select value->>'scope_id'
       from flow_identity_state where key = 'preflight'
     )
   order by audit.id desc limit 1),
  (select value from flow_identity_state where key = 'completed_terminal_audit'),
  'recovered completed finalize leaves original terminal audit byte-identical'
);

select set_config(
  'request.jwt.claim.sub', pg_temp.flow_identity_id('other')::text, true
);
select set_config(
  'request.jwt.claim.email', 'flow-other@example.com', true
);
select is(
  public.cmd_dataset_flow_identity_scope_read(
    (select (value->>'scope_id')::uuid
      from flow_identity_state where key = 'preflight')
  )->>'code',
  'FLOW_IDENTITY_SCOPE_NOT_FOUND',
  'foreign actor cannot read another owner scope'
);

select set_config('request.jwt.claim.sub', '', true);
select set_config('request.jwt.claim.email', '', true);
select is(
  public.cmd_dataset_flow_identity_scope_preflight_guarded(
    '{}'::jsonb
  )->>'code',
  'AUTH_REQUIRED',
  'preflight requires an authenticated actor'
);

reset role;

select * from finish();
rollback;
