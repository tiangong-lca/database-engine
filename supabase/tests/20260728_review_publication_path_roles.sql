begin;

create extension if not exists pgtap with schema extensions;
set local search_path = extensions, public, auth;

create or replace function pg_temp.disable_trigger_if_exists(
  p_table regclass,
  p_trigger name
)
returns void
language plpgsql
as $$
begin
  if exists (
    select 1
    from pg_trigger
    where tgrelid = p_table
      and tgname = p_trigger
      and not tgisinternal
  ) then
    execute format('alter table %s disable trigger %I', p_table, p_trigger);
  end if;
end;
$$;

select plan(24);
select set_config('request.jwt.claim.role', 'authenticated', true);

create temporary table path_role_webhook_calls (
  edge_function text not null,
  body jsonb not null,
  timeout_milliseconds integer not null
) on commit drop;

create or replace function util.invoke_edge_function(
  name text,
  body jsonb,
  timeout_milliseconds integer default ((5 * 60) * 1000)
) returns void
language plpgsql
security definer
set search_path = ''
as $$
begin
  insert into pg_temp.path_role_webhook_calls (
    edge_function,
    body,
    timeout_milliseconds
  )
  values (name, body, timeout_milliseconds);
end;
$$;

insert into auth.users (
  instance_id,
  id,
  aud,
  role,
  email,
  encrypted_password,
  email_confirmed_at,
  raw_app_meta_data,
  raw_user_meta_data,
  created_at,
  updated_at,
  is_sso_user,
  is_anonymous
)
values
  (
    '00000000-0000-0000-0000-000000000000',
    '12800000-0000-0000-0000-000000000001',
    'authenticated',
    'authenticated',
    'closure-owner@example.com',
    'test-password-hash',
    now(),
    '{"provider":"email","providers":["email"]}'::jsonb,
    '{"sub":"12800000-0000-0000-0000-000000000001","email":"closure-owner@example.com","display_name":"Closure Owner"}'::jsonb,
    now(),
    now(),
    false,
    false
  ),
  (
    '00000000-0000-0000-0000-000000000000',
    '12800000-0000-0000-0000-000000000002',
    'authenticated',
    'authenticated',
    'foreign-owner@example.com',
    'test-password-hash',
    now(),
    '{"provider":"email","providers":["email"]}'::jsonb,
    '{"sub":"12800000-0000-0000-0000-000000000002","email":"foreign-owner@example.com","display_name":"Foreign Owner"}'::jsonb,
    now(),
    now(),
    false,
    false
  ),
  (
    '00000000-0000-0000-0000-000000000000',
    '12800000-0000-0000-0000-000000000010',
    'authenticated',
    'authenticated',
    'closure-admin@example.com',
    'test-password-hash',
    now(),
    '{"provider":"email","providers":["email"]}'::jsonb,
    '{"sub":"12800000-0000-0000-0000-000000000010","email":"closure-admin@example.com","display_name":"Closure Admin"}'::jsonb,
    now(),
    now(),
    false,
    false
  );

insert into public.users (id, raw_user_meta_data)
values
  (
    '12800000-0000-0000-0000-000000000001',
    '{"email":"closure-owner@example.com","display_name":"Closure Owner"}'::jsonb
  ),
  (
    '12800000-0000-0000-0000-000000000002',
    '{"email":"foreign-owner@example.com","display_name":"Foreign Owner"}'::jsonb
  ),
  (
    '12800000-0000-0000-0000-000000000010',
    '{"email":"closure-admin@example.com","display_name":"Closure Admin"}'::jsonb
  );

insert into public.teams (id, json, rank, is_public)
values
  (
    '22800000-0000-0000-0000-000000000001',
    '{"title":"Closure Team"}'::jsonb,
    1,
    false
  ),
  (
    '22800000-0000-0000-0000-000000000002',
    '{"title":"Foreign Secret Team"}'::jsonb,
    2,
    false
  );

insert into public.roles (user_id, team_id, role)
values
  (
    '12800000-0000-0000-0000-000000000001',
    '22800000-0000-0000-0000-000000000001',
    'owner'
  ),
  (
    '12800000-0000-0000-0000-000000000002',
    '22800000-0000-0000-0000-000000000002',
    'owner'
  ),
  (
    '12800000-0000-0000-0000-000000000010',
    '00000000-0000-0000-0000-000000000000',
    'review-admin'
  );

alter table public.flows disable trigger "flows_json_sync_trigger";
alter table public.processes disable trigger "processes_json_sync_trigger";
alter table public.lifecyclemodels disable trigger "lifecyclemodels_json_sync_trigger";

select pg_temp.disable_trigger_if_exists(
  'public.flows'::regclass,
  'flow_extract_md_trigger_insert'
);
select pg_temp.disable_trigger_if_exists(
  'public.flows'::regclass,
  'flow_extract_text_trigger_insert'
);
select pg_temp.disable_trigger_if_exists(
  'public.flows'::regclass,
  'flow_dataset_extraction_trigger_insert'
);
select pg_temp.disable_trigger_if_exists(
  'public.processes'::regclass,
  'process_extract_md_trigger_insert'
);
select pg_temp.disable_trigger_if_exists(
  'public.processes'::regclass,
  'process_extract_text_trigger_insert'
);
select pg_temp.disable_trigger_if_exists(
  'public.processes'::regclass,
  'process_dataset_extraction_trigger_insert'
);
select pg_temp.disable_trigger_if_exists(
  'public.lifecyclemodels'::regclass,
  'lifecyclemodel_extract_md_trigger_insert'
);
select pg_temp.disable_trigger_if_exists(
  'public.lifecyclemodels'::regclass,
  'lifecyclemodels_extract_text_trigger_insert'
);

insert into public.flows (
  id,
  version,
  json,
  json_ordered,
  user_id,
  state_code,
  team_id,
  rule_verification
)
values
  (
    '32800000-0000-0000-0000-000000000001',
    '01.00.000',
    '{"flowDataSet":{"flowInformation":{"dataSetInformation":{"name":{"baseName":[{"#text":"Required support"}]}}}}}'::jsonb,
    '{"flowDataSet":{"flowInformation":{"dataSetInformation":{"name":{"baseName":[{"#text":"Required support"}]}}}}}'::json,
    '12800000-0000-0000-0000-000000000001',
    0,
    '22800000-0000-0000-0000-000000000001',
    true
  ),
  (
    '32800000-0000-0000-0000-000000000002',
    '01.00.000',
    '{"flowDataSet":{"flowInformation":{"dataSetInformation":{"name":{"baseName":[{"#text":"Unknown-path support"}]}}}}}'::jsonb,
    '{"flowDataSet":{"flowInformation":{"dataSetInformation":{"name":{"baseName":[{"#text":"Unknown-path support"}]}}}}}'::json,
    '12800000-0000-0000-0000-000000000001',
    0,
    '22800000-0000-0000-0000-000000000001',
    true
  ),
  (
    '32800000-0000-0000-0000-000000000003',
    '01.00.000',
    '{"flowDataSet":{"flowInformation":{"dataSetInformation":{"name":{"baseName":[{"#text":"Approval support"}]}}}}}'::jsonb,
    '{"flowDataSet":{"flowInformation":{"dataSetInformation":{"name":{"baseName":[{"#text":"Approval support"}]}}}}}'::json,
    '12800000-0000-0000-0000-000000000001',
    20,
    '22800000-0000-0000-0000-000000000001',
    true
  );

insert into public.processes (
  id,
  version,
  json,
  json_ordered,
  user_id,
  state_code,
  team_id,
  model_id,
  rule_verification,
  reviews
)
values
  (
    '33800000-0000-0000-0000-000000000001',
    '00.01.000',
    '{"processDataSet":{"processInformation":{"dataSetInformation":{"name":{"baseName":[{"#text":"Historical predecessor"}]}}}}}'::jsonb,
    '{"processDataSet":{"processInformation":{"dataSetInformation":{"name":{"baseName":[{"#text":"Historical predecessor"}]}}}}}'::json,
    '12800000-0000-0000-0000-000000000001',
    0,
    '22800000-0000-0000-0000-000000000001',
    null,
    true,
    '[]'::jsonb
  ),
  (
    '33800000-0000-0000-0000-000000000002',
    '01.00.000',
    '{
      "processDataSet": {
        "processInformation": {
          "dataSetInformation": {
            "name": { "baseName": [{ "#text": "Submit root" }] },
            "referenceToPrecedingDataSetVersion": {
              "@type": "process data set",
              "@refObjectId": "33800000-0000-0000-0000-000000000001",
              "@version": "00.01.000"
            }
          }
        },
        "exchanges": {
          "exchange": [{
            "referenceToFlowDataSet": {
              "@type": "flow data set",
              "@refObjectId": "32800000-0000-0000-0000-000000000001",
              "@version": "01.00.000"
            }
          }]
        }
      }
    }'::jsonb,
    '{
      "processDataSet": {
        "processInformation": {
          "dataSetInformation": {
            "name": { "baseName": [{ "#text": "Submit root" }] },
            "referenceToPrecedingDataSetVersion": {
              "@type": "process data set",
              "@refObjectId": "33800000-0000-0000-0000-000000000001",
              "@version": "00.01.000"
            }
          }
        },
        "exchanges": {
          "exchange": [{
            "referenceToFlowDataSet": {
              "@type": "flow data set",
              "@refObjectId": "32800000-0000-0000-0000-000000000001",
              "@version": "01.00.000"
            }
          }]
        }
      }
    }'::json,
    '12800000-0000-0000-0000-000000000001',
    0,
    '22800000-0000-0000-0000-000000000001',
    null,
    true,
    '[]'::jsonb
  ),
  (
    '33800000-0000-0000-0000-000000000003',
    '01.00.000',
    '{
      "processDataSet": {
        "processInformation": {
          "dataSetInformation": {
            "name": { "baseName": [{ "#text": "Unknown path root" }] }
          }
        },
        "unexpectedReference": {
          "@type": "flow data set",
          "@refObjectId": "32800000-0000-0000-0000-000000000002",
          "@version": "01.00.000"
        }
      }
    }'::jsonb,
    '{
      "processDataSet": {
        "processInformation": {
          "dataSetInformation": {
            "name": { "baseName": [{ "#text": "Unknown path root" }] }
          }
        },
        "unexpectedReference": {
          "@type": "flow data set",
          "@refObjectId": "32800000-0000-0000-0000-000000000002",
          "@version": "01.00.000"
        }
      }
    }'::json,
    '12800000-0000-0000-0000-000000000001',
    0,
    '22800000-0000-0000-0000-000000000001',
    null,
    true,
    '[]'::jsonb
  ),
  (
    '33800000-0000-0000-0000-000000000004',
    '01.00.000',
    '{"processDataSet":{"processInformation":{"dataSetInformation":{"name":{"baseName":[{"#text":"Foreign Secret Process"}]}}}}}'::jsonb,
    '{"processDataSet":{"processInformation":{"dataSetInformation":{"name":{"baseName":[{"#text":"Foreign Secret Process"}]}}}}}'::json,
    '12800000-0000-0000-0000-000000000002',
    0,
    '22800000-0000-0000-0000-000000000002',
    null,
    true,
    '[]'::jsonb
  ),
  (
    '33800000-0000-0000-0000-000000000005',
    '00.01.000',
    '{"processDataSet":{"processInformation":{"dataSetInformation":{"name":{"baseName":[{"#text":"Approval predecessor"}]}}}}}'::jsonb,
    '{"processDataSet":{"processInformation":{"dataSetInformation":{"name":{"baseName":[{"#text":"Approval predecessor"}]}}}}}'::json,
    '12800000-0000-0000-0000-000000000001',
    20,
    '22800000-0000-0000-0000-000000000001',
    null,
    true,
    '[]'::jsonb
  ),
  (
    '33800000-0000-0000-0000-000000000006',
    '01.00.000',
    '{
      "processDataSet": {
        "processInformation": {
          "dataSetInformation": {
            "name": { "baseName": [{ "#text": "Approval root" }] },
            "referenceToPrecedingDataSetVersion": {
              "@type": "process data set",
              "@refObjectId": "33800000-0000-0000-0000-000000000005",
              "@version": "00.01.000"
            }
          }
        },
        "exchanges": {
          "exchange": [{
            "referenceToFlowDataSet": {
              "@type": "flow data set",
              "@refObjectId": "32800000-0000-0000-0000-000000000003",
              "@version": "01.00.000"
            }
          }]
        }
      }
    }'::jsonb,
    '{
      "processDataSet": {
        "processInformation": {
          "dataSetInformation": {
            "name": { "baseName": [{ "#text": "Approval root" }] },
            "referenceToPrecedingDataSetVersion": {
              "@type": "process data set",
              "@refObjectId": "33800000-0000-0000-0000-000000000005",
              "@version": "00.01.000"
            }
          }
        },
        "exchanges": {
          "exchange": [{
            "referenceToFlowDataSet": {
              "@type": "flow data set",
              "@refObjectId": "32800000-0000-0000-0000-000000000003",
              "@version": "01.00.000"
            }
          }]
        }
      }
    }'::json,
    '12800000-0000-0000-0000-000000000001',
    20,
    '22800000-0000-0000-0000-000000000001',
    null,
    true,
    '[{"key":0,"id":"53800000-0000-0000-0000-000000000001"}]'::jsonb
  ),
  (
    '33800000-0000-0000-0000-000000000007',
    '01.00.000',
    '{"processDataSet":{"processInformation":{"dataSetInformation":{"name":{"baseName":[{"#text":"Paired process"}]}}}}}'::jsonb,
    '{"processDataSet":{"processInformation":{"dataSetInformation":{"name":{"baseName":[{"#text":"Paired process"}]}}}}}'::json,
    '12800000-0000-0000-0000-000000000001',
    0,
    '22800000-0000-0000-0000-000000000001',
    null,
    true,
    '[]'::jsonb
  );

insert into public.lifecyclemodels (
  id,
  version,
  json,
  json_ordered,
  json_tg,
  user_id,
  state_code,
  team_id,
  rule_verification,
  reviews
)
values
  (
    '34800000-0000-0000-0000-000000000001',
    '01.00.000',
    '{
      "lifeCycleModelDataSet": {
        "lifeCycleModelInformation": {
          "dataSetInformation": {
            "name": { "baseName": [{ "#text": "Foreign composition root" }] }
          },
          "technology": {
            "processes": {
              "processInstance": [{
                "referenceToProcess": {
                  "@type": "process data set",
                  "@refObjectId": "33800000-0000-0000-0000-000000000004",
                  "@version": "01.00.000"
                }
              }]
            }
          }
        }
      }
    }'::jsonb,
    '{
      "lifeCycleModelDataSet": {
        "lifeCycleModelInformation": {
          "dataSetInformation": {
            "name": { "baseName": [{ "#text": "Foreign composition root" }] }
          },
          "technology": {
            "processes": {
              "processInstance": [{
                "referenceToProcess": {
                  "@type": "process data set",
                  "@refObjectId": "33800000-0000-0000-0000-000000000004",
                  "@version": "01.00.000"
                }
              }]
            }
          }
        }
      }
    }'::json,
    '{"submodels":[{"id":"33800000-0000-0000-0000-000000000004","version":"01.00.000","type":"secondary"}]}'::jsonb,
    '12800000-0000-0000-0000-000000000001',
    0,
    '22800000-0000-0000-0000-000000000001',
    true,
    '[]'::jsonb
  ),
  (
    '34800000-0000-0000-0000-000000000002',
    '01.00.000',
    '{
      "lifeCycleModelDataSet": {
        "lifeCycleModelInformation": {
          "dataSetInformation": {
            "name": { "baseName": [{ "#text": "Missing composition root" }] }
          },
          "technology": {
            "processes": {
              "processInstance": [{
                "referenceToProcess": {
                  "@type": "process data set",
                  "@refObjectId": "33800000-0000-0000-0000-000000000099",
                  "@version": "01.00.000"
                }
              }]
            }
          }
        }
      }
    }'::jsonb,
    '{
      "lifeCycleModelDataSet": {
        "lifeCycleModelInformation": {
          "dataSetInformation": {
            "name": { "baseName": [{ "#text": "Missing composition root" }] }
          },
          "technology": {
            "processes": {
              "processInstance": [{
                "referenceToProcess": {
                  "@type": "process data set",
                  "@refObjectId": "33800000-0000-0000-0000-000000000099",
                  "@version": "01.00.000"
                }
              }]
            }
          }
        }
      }
    }'::json,
    '{"submodels":[{"id":"33800000-0000-0000-0000-000000000099","version":"01.00.000","type":"secondary"}]}'::jsonb,
    '12800000-0000-0000-0000-000000000001',
    0,
    '22800000-0000-0000-0000-000000000001',
    true,
    '[]'::jsonb
  ),
  (
    '33800000-0000-0000-0000-000000000007',
    '01.00.000',
    '{"lifeCycleModelDataSet":{"lifeCycleModelInformation":{"dataSetInformation":{"name":{"baseName":[{"#text":"Paired lifecycle model"}]}}}}}'::jsonb,
    '{"lifeCycleModelDataSet":{"lifeCycleModelInformation":{"dataSetInformation":{"name":{"baseName":[{"#text":"Paired lifecycle model"}]}}}}}'::json,
    '{"submodels":[]}'::jsonb,
    '12800000-0000-0000-0000-000000000001',
    0,
    '22800000-0000-0000-0000-000000000001',
    true,
    '[]'::jsonb
  );

insert into public.reviews (
  id,
  data_id,
  data_version,
  state_code,
  reviewer_id,
  json
)
values (
  '53800000-0000-0000-0000-000000000001',
  '33800000-0000-0000-0000-000000000006',
  '01.00.000',
  1,
  '[]'::jsonb,
  '{
    "data": {
      "id": "33800000-0000-0000-0000-000000000006",
      "version": "01.00.000"
    },
    "team": { "id": "22800000-0000-0000-0000-000000000001" },
    "user": { "id": "12800000-0000-0000-0000-000000000001" },
    "comment": { "message": "" },
    "logs": []
  }'::jsonb
);

create temporary table path_role_results (
  label text primary key,
  result jsonb not null
) on commit drop;

select set_config(
  'request.jwt.claim.sub',
  '12800000-0000-0000-0000-000000000001',
  true
);

insert into path_role_results (label, result)
values (
  'submit_lineage_and_support',
  public.cmd_review_submit_without_gate(
    'processes',
    '33800000-0000-0000-0000-000000000002',
    '01.00.000',
    '{"test":"path-role-characterization"}'::jsonb
  )
);

select is(
  (select result->>'ok' from path_role_results where label = 'submit_lineage_and_support'),
  'true',
  'control: submit succeeds for a process with schema-confirmed exchange support'
);

select is(
  (
    select state_code::text
    from public.flows
    where id = '32800000-0000-0000-0000-000000000001'
      and version = '01.00.000'
  ),
  '20',
  'control: submit keeps existing required Flow support in the review lifecycle'
);

select todo_start('Issue #304 path-aware lifecycle closure is not implemented yet');

select is(
  (
    select state_code::text
    from public.processes
    where id = '33800000-0000-0000-0000-000000000001'
      and version = '00.01.000'
  ),
  '0',
  'submit does not treat referenceToPrecedingDataSetVersion as a state target'
);

insert into path_role_results (label, result)
values (
  'submit_unknown_path',
  public.cmd_review_submit_without_gate(
    'processes',
    '33800000-0000-0000-0000-000000000003',
    '01.00.000',
    '{"test":"path-role-characterization"}'::jsonb
  )
);

select is(
  (select result->>'code' from path_role_results where label = 'submit_unknown_path'),
  'REFERENCE_ROLE_POLICY_GAP',
  'unknown Process/Flow reference paths fail closed with a stable policy-gap code'
);

select is(
  (
    select state_code::text
    from public.processes
    where id = '33800000-0000-0000-0000-000000000003'
      and version = '01.00.000'
  ),
  '0',
  'unknown reference policy failure leaves the submit root unchanged'
);

select is(
  (
    select state_code::text
    from public.flows
    where id = '32800000-0000-0000-0000-000000000002'
      and version = '01.00.000'
  ),
  '0',
  'unknown reference policy failure leaves the referenced dataset unchanged'
);

insert into path_role_results (label, result)
values
  (
    'submit_foreign_composition',
    public.cmd_review_submit_without_gate(
      'lifecyclemodels',
      '34800000-0000-0000-0000-000000000001',
      '01.00.000',
      '{"test":"path-role-characterization"}'::jsonb
    )
  ),
  (
    'submit_missing_composition',
    public.cmd_review_submit_without_gate(
      'lifecyclemodels',
      '34800000-0000-0000-0000-000000000002',
      '01.00.000',
      '{"test":"path-role-characterization"}'::jsonb
    )
  );

select is(
  (select result->>'code' from path_role_results where label = 'submit_foreign_composition'),
  'MODEL_DEPENDENCY_NOT_PUBLIC',
  'cross-owner private model composition fails closed'
);

select is(
  (select result->>'code' from path_role_results where label = 'submit_missing_composition'),
  'MODEL_DEPENDENCY_NOT_PUBLIC',
  'missing and private model composition use the same non-disclosing safe code'
);

select is(
  (
    select state_code::text
    from public.lifecyclemodels
    where id = '34800000-0000-0000-0000-000000000001'
      and version = '01.00.000'
  ),
  '0',
  'foreign composition failure leaves the lifecycle model unchanged'
);

select is(
  (
    select state_code::text
    from public.processes
    where id = '33800000-0000-0000-0000-000000000004'
      and version = '01.00.000'
  ),
  '0',
  'foreign composition failure leaves the dependency unchanged'
);

select is(
  (
    select count(*)::text
    from public.reviews
    where data_id in (
      '34800000-0000-0000-0000-000000000001',
      '34800000-0000-0000-0000-000000000002'
    )
  ),
  '0',
  'composition admission failures create no review rows'
);

select is(
  (
    select count(*)::text
    from public.command_audit_log
    where command = 'cmd_review_submit'
      and target_id in (
        '34800000-0000-0000-0000-000000000001',
        '34800000-0000-0000-0000-000000000002'
      )
  ),
  '0',
  'composition admission failures create no command audit rows'
);

select ok(
  (
    select result::text not like '%Foreign Secret Process%'
      and result::text not like '%Foreign Secret Team%'
      and result::text not like '%12800000-0000-0000-0000-000000000002%'
      and result::text not like '%state_code%'
    from path_role_results
    where label = 'submit_foreign_composition'
  ),
  'composition errors do not disclose foreign name, owner, team, or state details'
);

select is(
  (
    select result#>>'{details,path}'
    from path_role_results
    where label = 'submit_foreign_composition'
  ),
  'json_tg.submodels[0]',
  'composition error identifies only the submitter-visible reference path'
);

select is(
  (
    select result#>>'{details,reference,id}'
    from path_role_results
    where label = 'submit_foreign_composition'
  ),
  '33800000-0000-0000-0000-000000000004',
  'composition error identifies only the submitter-visible reference identity'
);

select set_config(
  'request.jwt.claim.sub',
  '12800000-0000-0000-0000-000000000010',
  true
);

insert into path_role_results (label, result)
values (
  'approve_lineage_and_support',
  public.cmd_review_approve(
    'processes',
    '53800000-0000-0000-0000-000000000001',
    '{"test":"path-role-characterization"}'::jsonb
  )
);

select is(
  (
    select state_code::text
    from public.processes
    where id = '33800000-0000-0000-0000-000000000005'
      and version = '00.01.000'
  ),
  '20',
  'approve does not publish a referenceToPrecedingDataSetVersion target'
);

select set_config(
  'request.jwt.claim.sub',
  '12800000-0000-0000-0000-000000000001',
  true
);

insert into path_role_results (label, result)
values (
  'publish_paired_process',
  public.cmd_dataset_publish(
    'processes',
    '33800000-0000-0000-0000-000000000007',
    '01.00.000',
    '{"test":"path-role-characterization"}'::jsonb
  )
);

select is(
  (select result->>'code' from path_role_results where label = 'publish_paired_process'),
  'MODEL_DEPENDENCY_NOT_PUBLIC',
  'direct Process publish fails when its paired LifecycleModel is private'
);

select is(
  (
    select state_code::text
    from public.processes
    where id = '33800000-0000-0000-0000-000000000007'
      and version = '01.00.000'
  ),
  '0',
  'failed paired Process publish leaves the Process private'
);

select is(
  (
    select state_code::text
    from public.lifecyclemodels
    where id = '33800000-0000-0000-0000-000000000007'
      and version = '01.00.000'
  ),
  '0',
  'failed paired Process publish leaves the paired LifecycleModel private'
);

insert into path_role_results (label, result)
values (
  'publish_foreign_composition',
  public.cmd_dataset_publish(
    'lifecyclemodels',
    '34800000-0000-0000-0000-000000000001',
    '01.00.000',
    '{"test":"path-role-characterization"}'::jsonb
  )
);

select is(
  (select result->>'code' from path_role_results where label = 'publish_foreign_composition'),
  'MODEL_DEPENDENCY_NOT_PUBLIC',
  'direct LifecycleModel publish fails when an exact included Process is private'
);

select is(
  (
    select state_code::text
    from public.lifecyclemodels
    where id = '34800000-0000-0000-0000-000000000001'
      and version = '01.00.000'
  ),
  '0',
  'failed LifecycleModel publish leaves the model private'
);

select is(
  (
    select state_code::text
    from public.processes
    where id = '33800000-0000-0000-0000-000000000004'
      and version = '01.00.000'
  ),
  '0',
  'failed LifecycleModel publish leaves the exact included Process private'
);

select todo_end();

select is(
  (select result->>'ok' from path_role_results where label = 'approve_lineage_and_support'),
  'true',
  'control: approve succeeds for schema-confirmed required support'
);

select is(
  (
    select state_code::text
    from public.flows
    where id = '32800000-0000-0000-0000-000000000003'
      and version = '01.00.000'
  ),
  '100',
  'control: approve publishes schema-confirmed required Flow support'
);

select * from finish();
rollback;
