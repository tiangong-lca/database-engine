-- Disposable hosted-Preview fixture for guarded Step 3 flow-identity E2E.
--
-- The Node runner replaces each whole {{..._SQL}} token with a validated SQL
-- literal.  The matching cleanup file is mandatory.  Never run this fixture
-- against persistent development or production.

begin;

set local search_path = extensions, public, auth;
set local lock_timeout = '5s';
set local statement_timeout = '180s';

create temporary table preview_flow_identity_fixture_config (
  actor_user_id uuid primary key,
  actor_email text not null,
  foreign_user_id uuid not null unique,
  foreign_email text not null,
  scenario_id text not null,
  request_id uuid not null,
  preview_ref text not null,
  preview_url text not null,
  service_role_key text not null,
  vault_description text not null,
  operation_id text not null,
  fixture_modified_at timestamptz not null
) on commit drop;

insert into preview_flow_identity_fixture_config values (
  ({{ACTOR_UUID_SQL}})::uuid,
  lower(btrim(({{ACTOR_EMAIL_SQL}})::text)),
  ({{FOREIGN_UUID_SQL}})::uuid,
  lower(btrim(({{FOREIGN_EMAIL_SQL}})::text)),
  btrim(({{SCENARIO_NAMESPACE_SQL}})::text),
  ({{REQUEST_ID_SQL}})::uuid,
  lower(btrim(({{PREVIEW_REF_SQL}})::text)),
  btrim(({{PREVIEW_URL_SQL}})::text),
  ({{SERVICE_ROLE_KEY_SQL}})::text,
  'database-engine#269 flow-identity-preview-e2e scenario='
    || btrim(({{SCENARIO_NAMESPACE_SQL}})::text)
    || ' request=' || ({{REQUEST_ID_SQL}})::uuid::text,
  'preview-flow-identity-' || ({{REQUEST_ID_SQL}})::uuid::text,
  '2026-07-17T00:00:00Z'::timestamptz
);

create or replace function pg_temp.preview_flow_identity_uuid(p_name text)
returns uuid
language sql
stable
strict
as $$
  select extensions.uuid_generate_v5(
    extensions.uuid_ns_url(),
    'database-engine#269/' || config.scenario_id || '/' || p_name
  )
  from pg_temp.preview_flow_identity_fixture_config as config
$$;

create or replace function pg_temp.preview_flow_identity_hash(p_name text)
returns text
language sql
stable
strict
as $$
  select encode(digest(
    'database-engine#269/' || config.scenario_id || '/' || p_name,
    'sha256'
  ), 'hex')
  from pg_temp.preview_flow_identity_fixture_config as config
$$;

create or replace function pg_temp.preview_flow_identity_reference(
  p_id uuid,
  p_name text
) returns jsonb
language sql
immutable
strict
as $$
  select jsonb_build_object(
    '@refObjectId', p_id,
    '@type', 'flow data set',
    '@uri', '../flows/' || p_id::text || '_01.00.000.xml',
    '@version', '01.00.000',
    'common:shortDescription', jsonb_build_object(
      '@xml:lang', 'en', '#text', p_name
    )
  )
$$;

create or replace function pg_temp.preview_flow_identity_unitgroup_payload()
returns jsonb
language sql
stable
as $$
  select jsonb_build_object(
    'unitGroupDataSet', jsonb_build_object(
      'unitGroupInformation', jsonb_build_object(
        'dataSetInformation', jsonb_build_object(
          'common:UUID', pg_temp.preview_flow_identity_uuid('unitgroup'),
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

create or replace function pg_temp.preview_flow_identity_flowproperty_payload()
returns jsonb
language sql
stable
as $$
  select jsonb_build_object(
    'flowPropertyDataSet', jsonb_build_object(
      'flowPropertiesInformation', jsonb_build_object(
        'dataSetInformation', jsonb_build_object(
          'common:UUID', pg_temp.preview_flow_identity_uuid('flowproperty'),
          'common:name', jsonb_build_object('#text', 'Mass')
        ),
        'quantitativeReference', jsonb_build_object(
          'referenceToReferenceUnitGroup', jsonb_build_object(
            '@refObjectId', pg_temp.preview_flow_identity_uuid('unitgroup'),
            '@type', 'unit group data set',
            '@uri', '../unitgroups/'
              || pg_temp.preview_flow_identity_uuid('unitgroup')::text
              || '_01.00.000.xml',
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

create or replace function pg_temp.preview_flow_identity_flow_payload(
  p_id uuid,
  p_name text
) returns jsonb
language sql
stable
strict
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
            '@refObjectId', pg_temp.preview_flow_identity_uuid('flowproperty'),
            '@type', 'flow property data set',
            '@uri', '../flowproperties/'
              || pg_temp.preview_flow_identity_uuid('flowproperty')::text
              || '_01.00.000.xml',
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

create or replace function pg_temp.preview_flow_identity_process_payload(
  p_process_id uuid,
  p_process_name text
) returns jsonb
language sql
stable
strict
as $$
  select jsonb_build_object(
    'processDataSet', jsonb_build_object(
      'processInformation', jsonb_build_object(
        'dataSetInformation', jsonb_build_object(
          'common:UUID', p_process_id,
          'name', jsonb_build_object('baseName', jsonb_build_object(
            '@xml:lang', 'en', '#text', p_process_name
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
          'referenceToFlowDataSet', pg_temp.preview_flow_identity_reference(
            pg_temp.preview_flow_identity_uuid('source-flow'), 'Owner source'
          )
        ),
        jsonb_build_object(
          '@dataSetInternalID', '2',
          'exchangeDirection', 'Output',
          'meanAmount', '7',
          'resultingAmount', '7',
          'referenceToFlowDataSet', pg_temp.preview_flow_identity_reference(
            pg_temp.preview_flow_identity_uuid('target-flow'), 'Public target'
          )
        ),
        jsonb_build_object(
          '@dataSetInternalID', '3',
          'exchangeDirection', 'Input',
          'meanAmount', '11',
          'resultingAmount', '11',
          'referenceToFlowDataSet', pg_temp.preview_flow_identity_reference(
            pg_temp.preview_flow_identity_uuid('pending-flow'),
            'Protected pending'
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

create or replace function pg_temp.preview_flow_identity_protected_intent()
returns jsonb
language plpgsql
stable
as $$
declare
  v_orphans jsonb;
begin
  select jsonb_agg(jsonb_build_object(
    'source_id', pg_temp.preview_flow_identity_uuid('orphan/' || ordinal),
    'source_version', '01.00.000',
    'evidence_sha256', pg_temp.preview_flow_identity_hash(
      'orphan-evidence/' || ordinal
    )
  ) order by ordinal)
  into v_orphans
  from generate_series(1, 303) as ordinal;

  return jsonb_build_object(
    'schema_version', 'dataset-flow-identity-protected-intent.v2',
    'pending', jsonb_build_array(jsonb_build_object(
      'source_id', pg_temp.preview_flow_identity_uuid('pending-flow'),
      'source_version', '01.00.000',
      'expected_reference_count', 2,
      'occurrences', jsonb_build_array(
        jsonb_build_object(
          'process_id', pg_temp.preview_flow_identity_uuid('process/1'),
          'process_version', '01.00.000',
          'exchange_index', 2,
          'internal_id', '3',
          'direction', 'Input'
        ),
        jsonb_build_object(
          'process_id', pg_temp.preview_flow_identity_uuid('process/2'),
          'process_version', '01.00.000',
          'exchange_index', 2,
          'internal_id', '3',
          'direction', 'Input'
        )
      ),
      'evidence_sha256', pg_temp.preview_flow_identity_hash('pending-evidence')
    )),
    'blockers', '[]'::jsonb,
    'orphans', v_orphans
  );
end
$$;

create or replace function pg_temp.preview_flow_identity_capture_request()
returns jsonb
language plpgsql
stable
as $$
declare
  config pg_temp.preview_flow_identity_fixture_config%rowtype;
  v_compatibility jsonb;
begin
  select * into strict config
  from pg_temp.preview_flow_identity_fixture_config;

  v_compatibility := jsonb_build_object(
    'policy_sha256', pg_temp.preview_flow_identity_hash('policy'),
    'mode', 'identity',
    'confidence', 'approved',
    'flow_property_compatible', true,
    'unit_group_compatible', true,
    'direction_compatible', true,
    'compartment_compatible', true,
    'conversion_factor', '1',
    'evidence_sha256', pg_temp.preview_flow_identity_hash(
      'mapping-evidence'
    ),
    'flow_schema', jsonb_build_object(
      'status', 'legacy_warning',
      'warning_set_sha256', pg_temp.preview_flow_identity_hash(
        'flow-schema-warning-set'
      )
    ),
    'process_schema_required', 'pass'
  );

  return jsonb_build_object(
    'schema_version', 'dataset-flow-identity-capture-attest.v2',
    'request_id', config.request_id,
    'environment', 'preview',
    'project_ref', config.preview_ref,
    'actor', jsonb_build_object(
      'user_id', config.actor_user_id,
      'email', config.actor_email
    ),
    'target_visibility', 'owner_draft',
    'operation_id', config.operation_id,
    'compatibility_policy', jsonb_build_object(
      'schema_version', 'dataset-flow-identity-compatibility-policy.v1',
      'policy_sha256', pg_temp.preview_flow_identity_hash('policy'),
      'evidence_resolution_sha256',
        pg_temp.preview_flow_identity_hash('evidence-resolution'),
      'approved_at_utc', '2026-07-17T00:00:00Z',
      'approval_text_sha256',
        pg_temp.preview_flow_identity_hash('policy-approval-text')
    ),
    'artifact_evidence', jsonb_build_object(
      'review_ledger_sha256',
        pg_temp.preview_flow_identity_hash('review-ledger'),
      'live_capture_artifact_sha256',
        pg_temp.preview_flow_identity_hash('live-capture-artifact'),
      'toolchain_evidence_sha256',
        pg_temp.preview_flow_identity_hash('toolchain-evidence')
    ),
    'mappings', jsonb_build_array(jsonb_build_object(
      'ordinal', 1,
      'source', jsonb_build_object(
        'id', pg_temp.preview_flow_identity_uuid('source-flow'),
        'version', '01.00.000',
        'source_trace_sha256',
          pg_temp.preview_flow_identity_hash('source-trace')
      ),
      'target', jsonb_build_object(
        'id', pg_temp.preview_flow_identity_uuid('target-flow'),
        'version', '01.00.000',
        'reference', pg_temp.preview_flow_identity_reference(
          pg_temp.preview_flow_identity_uuid('target-flow'), 'Public target'
        )
      ),
      'compatibility', v_compatibility
    )),
    'process_intents', jsonb_build_array(
      jsonb_build_object(
        'ordinal', 1,
        'id', pg_temp.preview_flow_identity_uuid('process/1'),
        'version', '01.00.000',
        'rewrites', jsonb_build_array(jsonb_build_object(
          'ordinal', 1,
          'exchange_index', 0,
          'internal_id', '1',
          'direction', 'Input',
          'mapping_ordinal', 1
        )),
        'process_schema', jsonb_build_object(
          'status', 'pass',
          'evidence_sha256',
            pg_temp.preview_flow_identity_hash('process-schema/1')
        )
      ),
      jsonb_build_object(
        'ordinal', 2,
        'id', pg_temp.preview_flow_identity_uuid('process/2'),
        'version', '01.00.000',
        'rewrites', jsonb_build_array(jsonb_build_object(
          'ordinal', 1,
          'exchange_index', 0,
          'internal_id', '1',
          'direction', 'Input',
          'mapping_ordinal', 1
        )),
        'process_schema', jsonb_build_object(
          'status', 'pass',
          'evidence_sha256',
            pg_temp.preview_flow_identity_hash('process-schema/2')
        )
      )
    ),
    'protected_closure', pg_temp.preview_flow_identity_protected_intent()
  );
end
$$;

do $fixture_gate$
declare
  config pg_temp.preview_flow_identity_fixture_config%rowtype;
  server_context jsonb;
  function_signature text;
begin
  select * into strict config
  from pg_temp.preview_flow_identity_fixture_config;

  if config.actor_user_id = config.foreign_user_id
    or config.actor_email = config.foreign_email
    or config.actor_email !~ '^[^[:space:]@]+@[^[:space:]@]+$'
    or config.foreign_email !~ '^[^[:space:]@]+@[^[:space:]@]+$'
    or config.scenario_id !~ '^fie2e-[a-z0-9_-]+-[0-9a-f]{24}$'
    or config.preview_ref !~ '^[a-z0-9][a-z0-9-]{3,127}$'
    or config.preview_ref = 'qgzvkongdjqiiamzbbts'
    or config.preview_url is distinct from
      'https://' || config.preview_ref || '.supabase.co'
    or config.service_role_key is distinct from btrim(config.service_role_key)
    or config.service_role_key ~ '[[:space:]]'
    or octet_length(config.service_role_key) not between 20 and 16384
    or config.vault_description is distinct from
      'database-engine#269 flow-identity-preview-e2e scenario='
        || config.scenario_id || ' request=' || config.request_id::text then
    raise exception 'invalid flow-identity Preview fixture parameters';
  end if;

  if not exists (
    select 1 from auth.users as actor
    where actor.id = config.actor_user_id
      and lower(btrim(actor.email)) = config.actor_email
  ) or not exists (
    select 1 from auth.users as actor
    where actor.id = config.foreign_user_id
      and lower(btrim(actor.email)) = config.foreign_email
  ) then
    raise exception 'fixture requires both exact disposable auth identities';
  end if;

  foreach function_signature in array array[
    'public.cmd_dataset_flow_identity_capture_attest_guarded(jsonb)',
    'public.cmd_dataset_flow_identity_scope_preflight_guarded(jsonb)',
    'public.cmd_dataset_flow_identity_process_rewrite_guarded(uuid,jsonb,jsonb)',
    'public.cmd_dataset_flow_identity_scope_read(uuid)',
    'public.cmd_dataset_flow_identity_scope_finalize_guarded(uuid,jsonb,jsonb)',
    'public.cmd_dataset_flow_identity_scope_recover_guarded(uuid,jsonb)',
    'public.cmd_dataset_flow_identity_scope_lookup(jsonb)',
    'public.cmd_dataset_flow_identity_scope_cancel_guarded(uuid,jsonb)'
  ] loop
    if to_regprocedure(function_signature) is null
      or has_function_privilege('anon', function_signature, 'EXECUTE')
      or has_function_privilege('service_role', function_signature, 'EXECUTE')
      or not has_function_privilege(
        'authenticated', function_signature, 'EXECUTE'
      ) then
      raise exception 'unexpected Step 3 ACL for %', function_signature;
    end if;
  end loop;

  if exists (
    select 1 from vault.secrets
    where name in ('project_url', 'project_secret_key')
  ) then
    raise exception 'fixture requires both branch-local Vault names absent';
  end if;

  perform vault.create_secret(
    config.preview_url, 'project_url', config.vault_description
  );
  perform vault.create_secret(
    config.service_role_key, 'project_secret_key', config.vault_description
  );

  if (
    select count(*)
    from vault.decrypted_secrets as secret
    where secret.name in ('project_url', 'project_secret_key')
      and secret.description is not distinct from config.vault_description
      and (
        (secret.name = 'project_url'
          and secret.decrypted_secret is not distinct from config.preview_url)
        or (secret.name = 'project_secret_key'
          and secret.decrypted_secret is not distinct from
            config.service_role_key)
      )
  ) is distinct from 2 then
    raise exception 'fixture did not seal both exact Vault values';
  end if;

  server_context := util.dataset_alias_execution_server_context();
  if server_context->>'environment' is distinct from 'preview'
    or server_context->>'project_ref' is distinct from config.preview_ref then
    raise exception 'fixture trusted context is not exact hosted Preview ref';
  end if;

  if exists (
    select 1 from public.command_audit_log as audit
    where audit.command = 'preview_e2e_flow_identity_fixture'
      and audit.actor_user_id = config.actor_user_id
      and audit.target_table = 'preview_e2e_flow_identity'
      and audit.target_id = config.request_id
  ) or exists (
    select 1 from util.dataset_flow_identity_capture_receipts as receipt
    where receipt.actor_user_id = config.actor_user_id
      and receipt.request_id = config.request_id
  ) or exists (
    select 1 from util.dataset_flow_identity_scopes as scope
    where scope.actor_user_id = config.actor_user_id
      and scope.operation_id = config.operation_id
  ) then
    raise exception 'fixture namespace has durable residue; cleanup first';
  end if;

  if exists (
    select 1 from public.unitgroups
    where id = pg_temp.preview_flow_identity_uuid('unitgroup')
  ) or exists (
    select 1 from public.flowproperties
    where id = pg_temp.preview_flow_identity_uuid('flowproperty')
  ) or exists (
    select 1 from public.flows
    where id in (
      pg_temp.preview_flow_identity_uuid('source-flow'),
      pg_temp.preview_flow_identity_uuid('target-flow'),
      pg_temp.preview_flow_identity_uuid('pending-flow')
    ) or id in (
      select pg_temp.preview_flow_identity_uuid('orphan/' || ordinal)
      from generate_series(1, 303) as ordinal
    )
  ) or exists (
    select 1 from public.processes
    where id in (
      pg_temp.preview_flow_identity_uuid('process/1'),
      pg_temp.preview_flow_identity_uuid('process/2')
    )
  ) then
    raise exception 'scenario-derived business keys already exist';
  end if;
end
$fixture_gate$;

insert into public.unitgroups (
  id, version, user_id, state_code, json, json_ordered, modified_at
)
select
  pg_temp.preview_flow_identity_uuid('unitgroup'),
  '01.00.000',
  config.foreign_user_id,
  100,
  pg_temp.preview_flow_identity_unitgroup_payload(),
  pg_temp.preview_flow_identity_unitgroup_payload()::json,
  config.fixture_modified_at
from pg_temp.preview_flow_identity_fixture_config as config;

insert into public.flowproperties (
  id, version, user_id, state_code, json, json_ordered, modified_at
)
select
  pg_temp.preview_flow_identity_uuid('flowproperty'),
  '01.00.000',
  config.foreign_user_id,
  100,
  pg_temp.preview_flow_identity_flowproperty_payload(),
  pg_temp.preview_flow_identity_flowproperty_payload()::json,
  config.fixture_modified_at
from pg_temp.preview_flow_identity_fixture_config as config;

-- Avoid creating hundreds of unrelated async jobs while constructing the
-- disposable source universe.  The guarded rewrite later runs with all
-- production triggers enabled and must admit its own derivative child.
alter table public.flows disable trigger user;
alter table public.processes disable trigger user;

insert into public.flows (
  id, version, user_id, state_code, json, json_ordered, modified_at,
  extracted_text, extracted_md
)
select
  entry.id,
  '01.00.000',
  entry.owner_id,
  entry.state_code,
  pg_temp.preview_flow_identity_flow_payload(entry.id, entry.name),
  pg_temp.preview_flow_identity_flow_payload(entry.id, entry.name)::json,
  config.fixture_modified_at,
  entry.name || ' text',
  entry.name || ' markdown'
from pg_temp.preview_flow_identity_fixture_config as config
cross join lateral (
  values
    (pg_temp.preview_flow_identity_uuid('source-flow'),
      config.actor_user_id, 0, 'Owner source'),
    (pg_temp.preview_flow_identity_uuid('target-flow'),
      config.foreign_user_id, 100, 'Public target'),
    (pg_temp.preview_flow_identity_uuid('pending-flow'),
      config.actor_user_id, 0, 'Protected pending')
) as entry(id, owner_id, state_code, name);

insert into public.flows (
  id, version, user_id, state_code, json, json_ordered, modified_at,
  extracted_text, extracted_md
)
select
  pg_temp.preview_flow_identity_uuid('orphan/' || ordinal),
  '01.00.000',
  config.actor_user_id,
  0,
  pg_temp.preview_flow_identity_flow_payload(
    pg_temp.preview_flow_identity_uuid('orphan/' || ordinal),
    'Protected orphan ' || ordinal
  ),
  pg_temp.preview_flow_identity_flow_payload(
    pg_temp.preview_flow_identity_uuid('orphan/' || ordinal),
    'Protected orphan ' || ordinal
  )::json,
  config.fixture_modified_at,
  'Protected orphan ' || ordinal || ' text',
  'Protected orphan ' || ordinal || ' markdown'
from pg_temp.preview_flow_identity_fixture_config as config
cross join generate_series(1, 303) as ordinal;

insert into public.processes (
  id, version, user_id, state_code, json, json_ordered, modified_at,
  extracted_text, extracted_md, model_id, rule_verification
)
select
  pg_temp.preview_flow_identity_uuid('process/' || ordinal),
  '01.00.000',
  config.actor_user_id,
  0,
  pg_temp.preview_flow_identity_process_payload(
    pg_temp.preview_flow_identity_uuid('process/' || ordinal),
    'Step 3 Preview process ' || ordinal
  ),
  pg_temp.preview_flow_identity_process_payload(
    pg_temp.preview_flow_identity_uuid('process/' || ordinal),
    'Step 3 Preview process ' || ordinal
  )::json,
  config.fixture_modified_at,
  'Step 3 Preview process ' || ordinal || ' text',
  'Step 3 Preview process ' || ordinal || ' markdown',
  null,
  null
from pg_temp.preview_flow_identity_fixture_config as config
cross join generate_series(1, 2) as ordinal;

alter table public.flows enable trigger user;
alter table public.processes enable trigger user;

do $fixture_assertions$
declare
  config pg_temp.preview_flow_identity_fixture_config%rowtype;
  capture_request jsonb;
begin
  select * into strict config
  from pg_temp.preview_flow_identity_fixture_config;
  capture_request := pg_temp.preview_flow_identity_capture_request();

  if (select count(*) from public.flows
      where user_id = config.actor_user_id and state_code = 0
        and json #>>
          '{flowDataSet,modellingAndValidation,LCIMethod,typeOfDataSet}'
          = 'Elementary flow') is distinct from 305
    or (select count(*) from public.processes
        where user_id = config.actor_user_id and state_code = 0) is distinct from 2
    or jsonb_array_length(capture_request->'mappings') is distinct from 1
    or jsonb_array_length(capture_request->'process_intents') is distinct from 2
    or jsonb_array_length(
      capture_request #> '{protected_closure,orphans}'
    ) is distinct from 303
    or capture_request #>> '{actor,user_id}'
      is distinct from config.actor_user_id::text
    or capture_request->>'project_ref' is distinct from config.preview_ref
    or capture_request->>'operation_id' is distinct from config.operation_id then
    raise exception 'flow-identity fixture cardinality or binding mismatch';
  end if;
end
$fixture_assertions$;

insert into public.command_audit_log (
  command, actor_user_id, target_table, target_id, target_version, payload
)
select
  'preview_e2e_flow_identity_fixture',
  config.actor_user_id,
  'preview_e2e_flow_identity',
  config.request_id,
  '00.00.001',
  jsonb_build_object(
    'schema_version', 'protected-flow-identity-preview-fixture.v1',
    'scenario_id', config.scenario_id,
    'request_id', config.request_id,
    'preview_ref', config.preview_ref,
    'operation_id', config.operation_id,
    'fixture_modified_at', config.fixture_modified_at,
    'actor', jsonb_build_object(
      'user_id', config.actor_user_id, 'email', config.actor_email
    ),
    'foreign_actor', jsonb_build_object(
      'user_id', config.foreign_user_id, 'email', config.foreign_email
    ),
    'entities', jsonb_build_object(
      'unitgroup_id', pg_temp.preview_flow_identity_uuid('unitgroup'),
      'flowproperty_id', pg_temp.preview_flow_identity_uuid('flowproperty'),
      'source_flow_id', pg_temp.preview_flow_identity_uuid('source-flow'),
      'target_flow_id', pg_temp.preview_flow_identity_uuid('target-flow'),
      'pending_flow_id', pg_temp.preview_flow_identity_uuid('pending-flow'),
      'process_ids', jsonb_build_array(
        pg_temp.preview_flow_identity_uuid('process/1'),
        pg_temp.preview_flow_identity_uuid('process/2')
      )
    ),
    'expected', jsonb_build_object(
      'source_count', 305,
      'mapping_count', 1,
      'support_count', 2,
      'process_count', 2,
      'rewrite_count', 2,
      'pending_occurrence_count', 2,
      'orphan_count', 303
    ),
    'preflight_hashes', jsonb_build_object(
      'plan_sha256', pg_temp.preview_flow_identity_hash('plan'),
      'freeze_sha256', pg_temp.preview_flow_identity_hash('freeze'),
      'execution_approval_request_sha256',
        pg_temp.preview_flow_identity_hash('execution-approval-request'),
      'execution_approval_text_sha256',
        pg_temp.preview_flow_identity_hash('execution-approval-text'),
      'execution_approval_identity_sha256',
        pg_temp.preview_flow_identity_hash('execution-approval-identity')
    ),
    'capture_request', pg_temp.preview_flow_identity_capture_request()
  )
from pg_temp.preview_flow_identity_fixture_config as config;

select tap
from (values
  ('TAP version 13'),
  ('1..10'),
  ('ok 1 - exact hosted Preview and non-production context is sealed'),
  ('ok 2 - anon and service_role have no Step 3 EXECUTE privileges'),
  ('ok 3 - authenticated has every Step 3 EXECUTE privilege'),
  ('ok 4 - two exact disposable auth identities exist'),
  ('ok 5 - one public unit group and flow property exist'),
  ('ok 6 - one public target and 305 owner-draft elementary flows exist'),
  ('ok 7 - two owner-draft processes contain exact rewrite occurrences'),
  ('ok 8 - 303 orphans and two pending occurrences are sealed'),
  ('ok 9 - one secret-free service-readable fixture manifest is committed'),
  ('ok 10 - exact branch-local Vault execution secrets are committed')
) as fixture_tap(tap);

commit;
