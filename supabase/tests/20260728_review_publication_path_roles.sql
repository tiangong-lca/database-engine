begin;

create extension if not exists pgtap with schema extensions;
create extension if not exists dblink with schema extensions;
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

select plan(61);
select set_config('request.jwt.claim.role', 'authenticated', true);

set local role anon;
select ok(
  (
    select count(*) = 1
    from api.cmd_review_extract_refs(
      '{"processDataSet":{"exchanges":{"exchange":{"referenceToFlowDataSet":{"@type":"flow data set","@refObjectId":"32800000-0000-0000-0000-000000000040","@version":"01.00.000"}}}}}'::jsonb
    )
  )
  and not pg_catalog.has_function_privilege(
    current_user,
    'private.cmd_review_reference_roles(text,text,jsonb)',
    'EXECUTE'
  ),
  'anon can call the input-only extractor without direct helper access'
);
reset role;

set local role authenticated;
select ok(
  (
    select count(*) = 1
    from api.cmd_review_extract_refs(
      '{"processDataSet":{"exchanges":{"exchange":{"referenceToFlowDataSet":{"@type":"flow data set","@refObjectId":"32800000-0000-0000-0000-000000000041","@version":"01.00.000"}}}}}'::jsonb
    )
  )
  and not pg_catalog.has_function_privilege(
    current_user,
    'private.cmd_review_reference_roles(text,text,jsonb)',
    'EXECUTE'
  ),
  'authenticated can call the input-only extractor without direct helper access'
);
reset role;

set local role service_role;
select ok(
  (
    select count(*) = 1
    from api.cmd_review_extract_refs(
      '{"processDataSet":{"exchanges":{"exchange":{"referenceToFlowDataSet":{"@type":"flow data set","@refObjectId":"32800000-0000-0000-0000-000000000042","@version":"01.00.000"}}}}}'::jsonb
    )
  )
  and not pg_catalog.has_function_privilege(
    current_user,
    'private.cmd_review_reference_roles(text,text,jsonb)',
    'EXECUTE'
  ),
  'service_role can call the input-only extractor without direct helper access'
);
reset role;

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

insert into private.users (id, raw_user_meta_data)
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

insert into private.teams (id, json, rank, is_public)
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

insert into private.roles (user_id, team_id, role)
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
            "name": { "baseName": [{ "#text": "Submit root" }] }
          },
          "technology": {
            "referenceToIncludedProcesses": {
              "@refObjectId": "33800000-0000-0000-0000-000000000014",
              "@version": "01.00.000"
            }
          }
        },
        "administrativeInformation": {
          "publicationAndOwnership": {
            "common:referenceToPrecedingDataSetVersion": {
              "@type": "process data set",
              "@refObjectId": "33800000-0000-0000-0000-000000000001",
              "@version": "00.01.000"
            }
          }
        },
        "exchanges": {
          "exchange": {
            "referenceToFlowDataSet": [{
              "@type": "flow data set",
              "@refObjectId": "32800000-0000-0000-0000-000000000001",
              "@version": "01.00.000"
            }]
          }
        }
      }
    }'::jsonb,
    '{
      "processDataSet": {
        "processInformation": {
          "dataSetInformation": {
            "name": { "baseName": [{ "#text": "Submit root" }] }
          },
          "technology": {
            "referenceToIncludedProcesses": {
              "@refObjectId": "33800000-0000-0000-0000-000000000014",
              "@version": "01.00.000"
            }
          }
        },
        "administrativeInformation": {
          "publicationAndOwnership": {
            "common:referenceToPrecedingDataSetVersion": {
              "@type": "process data set",
              "@refObjectId": "33800000-0000-0000-0000-000000000001",
              "@version": "00.01.000"
            }
          }
        },
        "exchanges": {
          "exchange": {
            "referenceToFlowDataSet": [{
              "@type": "flow data set",
              "@refObjectId": "32800000-0000-0000-0000-000000000001",
              "@version": "01.00.000"
            }]
          }
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
        "processInformation": {
          "technology": {
            "referenceToIncludedProcesses": {
              "@type": "process data set",
              "@refObjectId": "not-a-uuid",
              "@version": "01.00.000"
            }
          }
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
        "processInformation": {
          "technology": {
            "referenceToIncludedProcesses": {
              "@type": "process data set",
              "@refObjectId": "not-a-uuid",
              "@version": "01.00.000"
            }
          }
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
            "name": { "baseName": [{ "#text": "Approval root" }] }
          },
          "technology": {
            "referenceToIncludedProcesses": [{
              "@type": "process data set",
              "@refObjectId": "33800000-0000-0000-0000-000000000015",
              "@version": "01.00.000"
            }]
          }
        },
        "administrativeInformation": {
          "publicationAndOwnership": {
            "common:referenceToPrecedingDataSetVersion": [{
              "@type": "process data set",
              "@refObjectId": "33800000-0000-0000-0000-000000000005",
              "@version": "00.01.000"
            }]
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
            "name": { "baseName": [{ "#text": "Approval root" }] }
          },
          "technology": {
            "referenceToIncludedProcesses": [{
              "@type": "process data set",
              "@refObjectId": "33800000-0000-0000-0000-000000000015",
              "@version": "01.00.000"
            }]
          }
        },
        "administrativeInformation": {
          "publicationAndOwnership": {
            "common:referenceToPrecedingDataSetVersion": [{
              "@type": "process data set",
              "@refObjectId": "33800000-0000-0000-0000-000000000005",
              "@version": "00.01.000"
            }]
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
    '{"processDataSet":{"processInformation":{"dataSetInformation":{"name":{"baseName":[{"#text":"Paired process"}]}},"technology":{"referenceToIncludedProcesses":[{"@type":"process data set","@refObjectId":"33800000-0000-0000-0000-000000000016","@version":"01.00.000"}]}},"administrativeInformation":{"publicationAndOwnership":{"common:referenceToPrecedingDataSetVersion":{"@type":"process data set","@refObjectId":"33800000-0000-0000-0000-000000000001","@version":"00.01.000"}}}}}'::jsonb,
    '{"processDataSet":{"processInformation":{"dataSetInformation":{"name":{"baseName":[{"#text":"Paired process"}]}},"technology":{"referenceToIncludedProcesses":[{"@type":"process data set","@refObjectId":"33800000-0000-0000-0000-000000000016","@version":"01.00.000"}]}},"administrativeInformation":{"publicationAndOwnership":{"common:referenceToPrecedingDataSetVersion":{"@type":"process data set","@refObjectId":"33800000-0000-0000-0000-000000000001","@version":"00.01.000"}}}}}'::json,
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
            "name": { "baseName": [{ "#text": "Foreign composition root" }] },
            "referenceToResultingProcess": [{
              "@type": "process data set",
              "@refObjectId": "33800000-0000-0000-0000-000000000004",
              "@version": "01.00.000"
            }]
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
            "name": { "baseName": [{ "#text": "Foreign composition root" }] },
            "referenceToResultingProcess": [{
              "@type": "process data set",
              "@refObjectId": "33800000-0000-0000-0000-000000000004",
              "@version": "01.00.000"
            }]
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

insert into private.reviews (
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
    '33800000-0000-0000-0000-000000000011',
    '01.00.000',
    '{"processDataSet":{"processInformation":{"dataSetInformation":{"name":{"baseName":[{"#text":"Approve foreign dependency"}]}}}}}'::jsonb,
    '{"processDataSet":{"processInformation":{"dataSetInformation":{"name":{"baseName":[{"#text":"Approve foreign dependency"}]}}}}}'::json,
    '12800000-0000-0000-0000-000000000002',
    0,
    '22800000-0000-0000-0000-000000000002',
    null,
    true,
    '[]'::jsonb
  ),
  (
    '33800000-0000-0000-0000-000000000012',
    '01.00.000',
    '{"processDataSet":{"processInformation":{"dataSetInformation":{"name":{"baseName":[{"#text":"ILCD composition member"}]}}}}}'::jsonb,
    '{"processDataSet":{"processInformation":{"dataSetInformation":{"name":{"baseName":[{"#text":"ILCD composition member"}]}}}}}'::json,
    '12800000-0000-0000-0000-000000000001',
    0,
    '22800000-0000-0000-0000-000000000001',
    null,
    true,
    '[]'::jsonb
  ),
  (
    '33800000-0000-0000-0000-000000000013',
    '01.00.000',
    '{"processDataSet":{"processInformation":{"dataSetInformation":{"name":{"baseName":[{"#text":"json_tg composition member"}]}}}}}'::jsonb,
    '{"processDataSet":{"processInformation":{"dataSetInformation":{"name":{"baseName":[{"#text":"json_tg composition member"}]}}}}}'::json,
    '12800000-0000-0000-0000-000000000001',
    0,
    '22800000-0000-0000-0000-000000000001',
    null,
    true,
    '[]'::jsonb
  ),
  (
    '33800000-0000-0000-0000-000000000014',
    '01.00.000',
    '{"processDataSet":{"processInformation":{"dataSetInformation":{"name":{"baseName":[{"#text":"Submit included Process"}]}}}}}'::jsonb,
    '{"processDataSet":{"processInformation":{"dataSetInformation":{"name":{"baseName":[{"#text":"Submit included Process"}]}}}}}'::json,
    '12800000-0000-0000-0000-000000000001',
    0,
    '22800000-0000-0000-0000-000000000001',
    null,
    true,
    '[]'::jsonb
  ),
  (
    '33800000-0000-0000-0000-000000000015',
    '01.00.000',
    '{"processDataSet":{"processInformation":{"dataSetInformation":{"name":{"baseName":[{"#text":"Approve included Process"}]}}}}}'::jsonb,
    '{"processDataSet":{"processInformation":{"dataSetInformation":{"name":{"baseName":[{"#text":"Approve included Process"}]}}}}}'::json,
    '12800000-0000-0000-0000-000000000001',
    20,
    '22800000-0000-0000-0000-000000000001',
    null,
    true,
    '[]'::jsonb
  ),
  (
    '33800000-0000-0000-0000-000000000016',
    '01.00.000',
    '{"processDataSet":{"processInformation":{"dataSetInformation":{"name":{"baseName":[{"#text":"Foreign included Process"}]}}}}}'::jsonb,
    '{"processDataSet":{"processInformation":{"dataSetInformation":{"name":{"baseName":[{"#text":"Foreign included Process"}]}}}}}'::json,
    '12800000-0000-0000-0000-000000000002',
    0,
    '22800000-0000-0000-0000-000000000002',
    null,
    true,
    '[]'::jsonb
  ),
  (
    '33800000-0000-0000-0000-000000000017',
    '01.00.000',
    '{"processDataSet":{"processInformation":{"dataSetInformation":{"name":{"baseName":[{"#text":"Bad included version root"}]}},"technology":{"referenceToIncludedProcesses":{"@type":"process data set","@refObjectId":"33800000-0000-0000-0000-000000000014","@version":""}}}}}'::jsonb,
    '{"processDataSet":{"processInformation":{"dataSetInformation":{"name":{"baseName":[{"#text":"Bad included version root"}]}},"technology":{"referenceToIncludedProcesses":{"@type":"process data set","@refObjectId":"33800000-0000-0000-0000-000000000014","@version":""}}}}}'::json,
    '12800000-0000-0000-0000-000000000001',
    0,
    '22800000-0000-0000-0000-000000000001',
    null,
    true,
    '[]'::jsonb
  ),
  (
    '33800000-0000-0000-0000-000000000018',
    '01.00.000',
    '{"processDataSet":{"processInformation":{"dataSetInformation":{"name":{"baseName":[{"#text":"Paired submit root"}]}}}}}'::jsonb,
    '{"processDataSet":{"processInformation":{"dataSetInformation":{"name":{"baseName":[{"#text":"Paired submit root"}]}}}}}'::json,
    '12800000-0000-0000-0000-000000000001',
    0,
    '22800000-0000-0000-0000-000000000001',
    null,
    true,
    '[]'::jsonb
  ),
  (
    '33800000-0000-0000-0000-000000000019',
    '01.00.000',
    '{"processDataSet":{"processInformation":{"dataSetInformation":{"name":{"baseName":[{"#text":"Paired approve root"}]}}}}}'::jsonb,
    '{"processDataSet":{"processInformation":{"dataSetInformation":{"name":{"baseName":[{"#text":"Paired approve root"}]}}}}}'::json,
    '12800000-0000-0000-0000-000000000001',
    20,
    '22800000-0000-0000-0000-000000000001',
    null,
    true,
    '[{"key":0,"id":"53800000-0000-0000-0000-000000000019"}]'::jsonb
  ),
  (
    '33800000-0000-0000-0000-000000000020',
    '01.00.000',
    '{"processDataSet":{"processInformation":{"dataSetInformation":{"name":{"baseName":[{"#text":"Direct included root"}]}},"technology":{"referenceToIncludedProcesses":[{"@type":"process data set","@refObjectId":"33800000-0000-0000-0000-000000000016","@version":"01.00.000"}]}}}}'::jsonb,
    '{"processDataSet":{"processInformation":{"dataSetInformation":{"name":{"baseName":[{"#text":"Direct included root"}]}},"technology":{"referenceToIncludedProcesses":[{"@type":"process data set","@refObjectId":"33800000-0000-0000-0000-000000000016","@version":"01.00.000"}]}}}}'::json,
    '12800000-0000-0000-0000-000000000001',
    0,
    '22800000-0000-0000-0000-000000000001',
    null,
    true,
    '[]'::jsonb
  ),
  (
    '33800000-0000-0000-0000-000000000021',
    '01.00.000',
    '{"processDataSet":{"processInformation":{"dataSetInformation":{"name":{"baseName":[{"#text":"Relational model result"}]}}}}}'::jsonb,
    '{"processDataSet":{"processInformation":{"dataSetInformation":{"name":{"baseName":[{"#text":"Relational model result"}]}}}}}'::json,
    '12800000-0000-0000-0000-000000000001',
    0,
    '22800000-0000-0000-0000-000000000001',
    '34800000-0000-0000-0000-000000000012',
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
    '34800000-0000-0000-0000-000000000011',
    '01.00.000',
    '{
      "lifeCycleModelDataSet": {
        "lifeCycleModelInformation": {
          "dataSetInformation": {
            "name": { "baseName": [{ "#text": "Approve foreign composition root" }] }
          },
          "technology": {
            "processes": {
              "processInstance": {
                "referenceToProcess": [{
                  "@type": "process data set",
                  "@refObjectId": "33800000-0000-0000-0000-000000000011",
                  "@version": "01.00.000"
                }]
              }
            }
          }
        }
      }
    }'::jsonb,
    '{
      "lifeCycleModelDataSet": {
        "lifeCycleModelInformation": {
          "dataSetInformation": {
            "name": { "baseName": [{ "#text": "Approve foreign composition root" }] }
          },
          "technology": {
            "processes": {
              "processInstance": {
                "referenceToProcess": [{
                  "@type": "process data set",
                  "@refObjectId": "33800000-0000-0000-0000-000000000011",
                  "@version": "01.00.000"
                }]
              }
            }
          }
        }
      }
    }'::json,
    '{"submodels":[{"id":"33800000-0000-0000-0000-000000000011","version":"01.00.000","type":"secondary"}]}'::jsonb,
    '12800000-0000-0000-0000-000000000001',
    20,
    '22800000-0000-0000-0000-000000000001',
    true,
    '[{"key":0,"id":"53800000-0000-0000-0000-000000000011"}]'::jsonb
  ),
  (
    '34800000-0000-0000-0000-000000000012',
    '01.00.000',
    '{
      "lifeCycleModelDataSet": {
        "lifeCycleModelInformation": {
          "dataSetInformation": {
            "name": { "baseName": [{ "#text": "Mismatched composition root" }] }
          },
          "technology": {
            "processes": {
              "processInstance": [{
                "referenceToProcess": {
                  "@type": "process data set",
                  "@refObjectId": "33800000-0000-0000-0000-000000000012",
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
            "name": { "baseName": [{ "#text": "Mismatched composition root" }] }
          },
          "technology": {
            "processes": {
              "processInstance": [{
                "referenceToProcess": {
                  "@type": "process data set",
                  "@refObjectId": "33800000-0000-0000-0000-000000000012",
                  "@version": "01.00.000"
                }
              }]
            }
          }
        }
      }
    }'::json,
    '{"submodels":[{"id":"33800000-0000-0000-0000-000000000013","version":"01.00.000","type":"secondary"}]}'::jsonb,
    '12800000-0000-0000-0000-000000000001',
    0,
    '22800000-0000-0000-0000-000000000001',
    true,
    '[]'::jsonb
  ),
  (
    '33800000-0000-0000-0000-000000000018',
    '01.00.000',
    '{"lifeCycleModelDataSet":{"lifeCycleModelInformation":{"dataSetInformation":{"name":{"baseName":[{"#text":"Private paired submit model"}]}}}}}'::jsonb,
    '{"lifeCycleModelDataSet":{"lifeCycleModelInformation":{"dataSetInformation":{"name":{"baseName":[{"#text":"Private paired submit model"}]}}}}}'::json,
    '{"submodels":[]}'::jsonb,
    '12800000-0000-0000-0000-000000000001',
    0,
    '22800000-0000-0000-0000-000000000001',
    true,
    '[]'::jsonb
  ),
  (
    '33800000-0000-0000-0000-000000000019',
    '01.00.000',
    '{"lifeCycleModelDataSet":{"lifeCycleModelInformation":{"dataSetInformation":{"name":{"baseName":[{"#text":"Private paired approve model"}]}}}}}'::jsonb,
    '{"lifeCycleModelDataSet":{"lifeCycleModelInformation":{"dataSetInformation":{"name":{"baseName":[{"#text":"Private paired approve model"}]}}}}}'::json,
    '{"submodels":[]}'::jsonb,
    '12800000-0000-0000-0000-000000000001',
    0,
    '22800000-0000-0000-0000-000000000001',
    true,
    '[]'::jsonb
  );

insert into private.reviews (
  id,
  data_id,
  data_version,
  state_code,
  reviewer_id,
  json
)
values
  (
    '53800000-0000-0000-0000-000000000011',
    '34800000-0000-0000-0000-000000000011',
    '01.00.000',
    1,
    '[]'::jsonb,
    '{
      "data": {
        "id": "34800000-0000-0000-0000-000000000011",
        "version": "01.00.000"
      },
      "team": { "id": "22800000-0000-0000-0000-000000000001" },
      "user": { "id": "12800000-0000-0000-0000-000000000001" },
      "comment": { "message": "" },
      "logs": []
    }'::jsonb
  ),
  (
    '53800000-0000-0000-0000-000000000019',
    '33800000-0000-0000-0000-000000000019',
    '01.00.000',
    1,
    '[]'::jsonb,
    '{
      "data": {
        "id": "33800000-0000-0000-0000-000000000019",
        "version": "01.00.000"
      },
      "team": { "id": "22800000-0000-0000-0000-000000000001" },
      "user": { "id": "12800000-0000-0000-0000-000000000001" },
      "comment": { "message": "" },
      "logs": []
    }'::jsonb
  );

insert into private.comments (
  review_id,
  reviewer_id,
  json,
  state_code
)
values (
  '53800000-0000-0000-0000-000000000011',
  '12800000-0000-0000-0000-000000000010',
  '{
    "modellingAndValidation": {
      "validation": {
        "review": [{
          "common:scope": [{
            "@name": "Approve composition scope",
            "common:method": { "@name": "Approve composition method" }
          }]
        }]
      }
    }
  }'::json,
  1
);

create temporary table path_role_results (
  label text primary key,
  result jsonb not null
) on commit drop;

create temporary table path_role_snapshots (
  label text primary key,
  snapshot jsonb not null
) on commit drop;

create or replace function pg_temp.public_error_shape(p_result jsonb)
returns jsonb
language sql
immutable
as $$
  select jsonb_build_object(
    'ok', p_result->'ok',
    'status', p_result->'status',
    'code', p_result->'code',
    'message', p_result->'message',
    'detailKeys', coalesce((
      select jsonb_agg(key order by key)
      from jsonb_object_keys(coalesce(p_result->'details', '{}'::jsonb)) as key
    ), '[]'::jsonb),
    'referenceKeys', coalesce((
      select jsonb_agg(key order by key)
      from jsonb_object_keys(
        case
          when jsonb_typeof(p_result#>'{details,reference}') = 'object'
            then p_result#>'{details,reference}'
          else '{}'::jsonb
        end
      ) as key
    ), '[]'::jsonb)
  )
$$;

create or replace function pg_temp.is_deeply(
  p_have jsonb,
  p_want jsonb,
  p_description text
)
returns text
language sql
as $$
  select extensions.is(p_have::text, p_want::text, p_description)
$$;

create or replace function pg_temp.concurrent_lifecycle_insert_sqlstate()
returns text
language plpgsql
as $$
declare
  v_state text;
begin
  perform extensions.dblink_connect(
    'path_role_pair_insert',
    'host=db port=5432 dbname=' || current_database()
      || ' user=postgres password=postgres'
  );
  perform extensions.dblink_exec(
    'path_role_pair_insert',
    'set statement_timeout = ''250ms'''
  );
  begin
    perform extensions.dblink_exec(
      'path_role_pair_insert',
      $insert$
        insert into public.lifecyclemodels (
          id,
          version,
          json,
          json_ordered,
          json_tg,
          state_code,
          rule_verification,
          reviews
        )
        values (
          'f5800000-0000-0000-0000-000000000001',
          '01.00.000',
          '{}',
          '{}',
          '{"submodels":[]}',
          0,
          true,
          '[]'
        )
      $insert$
    );
  exception when query_canceled then
    get stacked diagnostics v_state = returned_sqlstate;
  when others then
    get stacked diagnostics v_state = returned_sqlstate;
  end;
  perform extensions.dblink_disconnect('path_role_pair_insert');
  return v_state;
exception when others then
  begin
    perform extensions.dblink_disconnect('path_role_pair_insert');
  exception when others then
    null;
  end;
  raise;
end;
$$;

select is(
  (
    with shapes(label, document) as (
      values
        (
          'object-object',
          '{"processDataSet":{"exchanges":{"exchange":{"referenceToFlowDataSet":{"@type":"flow data set","@refObjectId":"32800000-0000-0000-0000-000000000021","@version":"01.00.000"}}}}}'::jsonb
        ),
        (
          'object-array',
          '{"processDataSet":{"exchanges":{"exchange":{"referenceToFlowDataSet":[{"@type":"flow data set","@refObjectId":"32800000-0000-0000-0000-000000000022","@version":"01.00.000"}]}}}}'::jsonb
        ),
        (
          'array-object',
          '{"processDataSet":{"exchanges":{"exchange":[{"referenceToFlowDataSet":{"@type":"flow data set","@refObjectId":"32800000-0000-0000-0000-000000000023","@version":"01.00.000"}}]}}}'::jsonb
        ),
        (
          'array-array',
          '{"processDataSet":{"exchanges":{"exchange":[{"referenceToFlowDataSet":[{"@type":"flow data set","@refObjectId":"32800000-0000-0000-0000-000000000024","@version":"01.00.000"}]}]}}}'::jsonb
        )
    )
    select count(*)::text
    from shapes
    cross join lateral private.cmd_review_reference_roles(
      'processes',
      shapes.label,
      shapes.document
    ) as role_ref
    where role_ref.lifecycle_role = 'RequiredSupport'
  ),
  '4',
  'exchange and Flow-reference object/array combinations are RequiredSupport'
);

select is(
  (
    with shapes(label, document) as (
      values
        (
          'object-object',
          '{"processDataSet":{"exchanges":{"exchange":{"referencesToDataSource":{"referenceToDataSource":{"@type":"source data set","@refObjectId":"32800000-0000-0000-0000-000000000025","@version":"01.00.000"}}}}}}'::jsonb
        ),
        (
          'object-array',
          '{"processDataSet":{"exchanges":{"exchange":{"referencesToDataSource":{"referenceToDataSource":[{"@type":"source data set","@refObjectId":"32800000-0000-0000-0000-000000000026","@version":"01.00.000"}]}}}}}'::jsonb
        ),
        (
          'array-object',
          '{"processDataSet":{"exchanges":{"exchange":[{"referencesToDataSource":{"referenceToDataSource":{"@type":"source data set","@refObjectId":"32800000-0000-0000-0000-000000000027","@version":"01.00.000"}}}]}}}'::jsonb
        ),
        (
          'array-array',
          '{"processDataSet":{"exchanges":{"exchange":[{"referencesToDataSource":{"referenceToDataSource":[{"@type":"source data set","@refObjectId":"32800000-0000-0000-0000-000000000028","@version":"01.00.000"}]}}]}}}'::jsonb
        )
    )
    select count(*)::text
    from shapes
    cross join lateral private.cmd_review_reference_roles(
      'processes',
      shapes.label,
      shapes.document
    ) as role_ref
    where role_ref.lifecycle_role = 'RequiredSupport'
  ),
  '4',
  'exchange and data-source reference object/array combinations are RequiredSupport'
);

select is(
  (
    with shapes(label, document) as (
      values
        (
          'object-object',
          '{"lifeCycleModelDataSet":{"lifeCycleModelInformation":{"technology":{"processes":{"processInstance":{"referenceToProcess":{"@type":"process data set","@refObjectId":"33800000-0000-0000-0000-000000000021","@version":"01.00.000"}}}}}}}'::jsonb
        ),
        (
          'object-array',
          '{"lifeCycleModelDataSet":{"lifeCycleModelInformation":{"technology":{"processes":{"processInstance":{"referenceToProcess":[{"@type":"process data set","@refObjectId":"33800000-0000-0000-0000-000000000022","@version":"01.00.000"}]}}}}}}'::jsonb
        ),
        (
          'array-object',
          '{"lifeCycleModelDataSet":{"lifeCycleModelInformation":{"technology":{"processes":{"processInstance":[{"referenceToProcess":{"@type":"process data set","@refObjectId":"33800000-0000-0000-0000-000000000023","@version":"01.00.000"}}]}}}}}'::jsonb
        ),
        (
          'array-array',
          '{"lifeCycleModelDataSet":{"lifeCycleModelInformation":{"technology":{"processes":{"processInstance":[{"referenceToProcess":[{"@type":"process data set","@refObjectId":"33800000-0000-0000-0000-000000000024","@version":"01.00.000"}]}]}}}}}'::jsonb
        )
    )
    select count(*)::text
    from shapes
    cross join lateral private.cmd_review_reference_roles(
      'lifecyclemodels',
      shapes.label,
      shapes.document
    ) as role_ref
    where role_ref.lifecycle_role = 'ModelComposition'
  ),
  '4',
  'processInstance and Process-reference object/array combinations are ModelComposition'
);

select is(
  (
    with roles as (
      select lifecycle_role
      from private.cmd_review_reference_roles(
        'lifecyclemodels',
        'object',
        '{"lifeCycleModelDataSet":{"lifeCycleModelInformation":{"dataSetInformation":{"referenceToResultingProcess":{"@type":"process data set","@refObjectId":"33800000-0000-0000-0000-000000000025","@version":"01.00.000"}}}}}'::jsonb
      )
      union all
      select lifecycle_role
      from private.cmd_review_reference_roles(
        'lifecyclemodels',
        'array',
        '{"lifeCycleModelDataSet":{"lifeCycleModelInformation":{"dataSetInformation":{"referenceToResultingProcess":[{"@type":"process data set","@refObjectId":"33800000-0000-0000-0000-000000000026","@version":"01.00.000"}]}}}}'::jsonb
      )
    )
    select count(*)::text from roles where lifecycle_role = 'Descriptive'
  ),
  '2',
  'resulting Process object and array references are Descriptive'
);

select is(
  (
    with shapes(label, document) as (
      values
        (
          'object-object',
          '{"modellingAndValidation":{"validation":{"review":{"common:referenceToReviewDetails":{"@type":"source data set","@refObjectId":"32800000-0000-0000-0000-000000000029","@version":"01.00.000"}}}}}'::jsonb
        ),
        (
          'object-array',
          '{"modellingAndValidation":{"validation":{"review":{"common:referenceToReviewDetails":[{"@type":"source data set","@refObjectId":"32800000-0000-0000-0000-000000000030","@version":"01.00.000"}]}}}}'::jsonb
        ),
        (
          'array-object',
          '{"modellingAndValidation":{"validation":{"review":[{"common:referenceToReviewDetails":{"@type":"source data set","@refObjectId":"32800000-0000-0000-0000-000000000031","@version":"01.00.000"}}]}}}'::jsonb
        ),
        (
          'array-array',
          '{"modellingAndValidation":{"validation":{"review":[{"common:referenceToReviewDetails":[{"@type":"source data set","@refObjectId":"32800000-0000-0000-0000-000000000032","@version":"01.00.000"}]}]}}}'::jsonb
        )
    )
    select count(*)::text
    from shapes
    cross join lateral private.cmd_review_reference_roles(
      'comments',
      shapes.label,
      shapes.document
    ) as role_ref
    where role_ref.lifecycle_role = 'RequiredSupport'
  ),
  '4',
  'review and review-detail reference object/array combinations are RequiredSupport'
);

select is(
  (
    select lifecycle_role
    from private.cmd_review_reference_roles(
      'processes',
      'unknown',
      '{"processDataSet":{"unknownList":[{"referenceToFlowDataSet":[{"@type":"flow data set","@refObjectId":"32800000-0000-0000-0000-000000000033","@version":"01.00.000"}]}]}}'::jsonb
    )
  ),
  'PolicyGap',
  'numeric segments at unknown repeated nodes remain fail-closed'
);

select is(
  (
    with roles as (
      select lifecycle_role
      from private.cmd_review_reference_roles(
        'processes',
        'object',
        '{"processDataSet":{"administrativeInformation":{"publicationAndOwnership":{"common:referenceToPrecedingDataSetVersion":{"@type":"process data set","@refObjectId":"33800000-0000-0000-0000-000000000034","@version":"00.01.000"}}}}}'::jsonb
      )
      union all
      select lifecycle_role
      from private.cmd_review_reference_roles(
        'processes',
        'array',
        '{"processDataSet":{"administrativeInformation":{"publicationAndOwnership":{"common:referenceToPrecedingDataSetVersion":[{"@type":"process data set","@refObjectId":"33800000-0000-0000-0000-000000000035","@version":"00.01.000"}]}}}}'::jsonb
      )
    )
    select count(*)::text from roles where lifecycle_role = 'Lineage'
  ),
  '2',
  'Process preceding-version object and array references are read-only Lineage'
);

select is(
  (
    with roles as (
      select lifecycle_role
      from private.cmd_review_reference_roles(
        'flows',
        'object',
        '{"flowDataSet":{"administrativeInformation":{"publicationAndOwnership":{"common:referenceToPrecedingDataSetVersion":{"@type":"flow data set","@refObjectId":"32800000-0000-0000-0000-000000000034","@version":"00.01.000"}}}}}'::jsonb
      )
      union all
      select lifecycle_role
      from private.cmd_review_reference_roles(
        'flows',
        'array',
        '{"flowDataSet":{"administrativeInformation":{"publicationAndOwnership":{"common:referenceToPrecedingDataSetVersion":[{"@type":"flow data set","@refObjectId":"32800000-0000-0000-0000-000000000035","@version":"00.01.000"}]}}}}'::jsonb
      )
    )
    select count(*)::text from roles where lifecycle_role = 'Lineage'
  ),
  '2',
  'Flow preceding-version object and array references are read-only Lineage'
);

select is(
  (
    select lifecycle_role
    from private.cmd_review_reference_roles(
      'processes',
      'wrong-path',
      '{"processDataSet":{"processInformation":{"dataSetInformation":{"common:referenceToPrecedingDataSetVersion":{"@type":"process data set","@refObjectId":"33800000-0000-0000-0000-000000000036","@version":"00.01.000"}}}}}'::jsonb
    )
  ),
  'PolicyGap',
  'preceding-version key outside its schema path remains fail-closed'
);

select is(
  (
    with roles as (
      select lifecycle_role
      from private.cmd_review_reference_roles(
        'processes',
        'object-without-type',
        '{"processDataSet":{"processInformation":{"technology":{"referenceToIncludedProcesses":{"@refObjectId":"33800000-0000-0000-0000-000000000037","@version":"01.00.000"}}}}}'::jsonb
      )
      union all
      select lifecycle_role
      from private.cmd_review_reference_roles(
        'processes',
        'array',
        '{"processDataSet":{"processInformation":{"technology":{"referenceToIncludedProcesses":[{"@type":"process data set","@refObjectId":"33800000-0000-0000-0000-000000000038","@version":"01.00.000"}]}}}}'::jsonb
      )
    )
    select count(*)::text from roles where lifecycle_role = 'ModelComposition'
  ),
  '2',
  'included Process object/array references are ModelComposition and type is path-inferable'
);

select is(
  (
    select lifecycle_role
    from private.cmd_review_reference_roles(
      'processes',
      'bad-uuid',
      '{"processDataSet":{"processInformation":{"technology":{"referenceToIncludedProcesses":{"@type":"process data set","@refObjectId":"not-a-uuid","@version":"01.00.000"}}}}}'::jsonb
    )
  ),
  'PolicyGap',
  'malformed included Process UUID is an explicit policy gap'
);

select is(
  (
    select lifecycle_role
    from private.cmd_review_reference_roles(
      'processes',
      'bad-version',
      '{"processDataSet":{"processInformation":{"technology":{"referenceToIncludedProcesses":{"@type":"process data set","@refObjectId":"33800000-0000-0000-0000-000000000039","@version":""}}}}}'::jsonb
    )
  ),
  'PolicyGap',
  'malformed included Process version is an explicit policy gap'
);

select set_config(
  'request.jwt.claim.sub',
  '12800000-0000-0000-0000-000000000001',
  true
);

insert into path_role_results (label, result)
values (
  'publish_included_process',
  api.cmd_dataset_publish(
    'processes',
    '33800000-0000-0000-0000-000000000020',
    '01.00.000',
    '{"test":"path-role-characterization"}'::jsonb
  )
);

select is(
  (select result->>'code' from path_role_results where label = 'publish_included_process'),
  'MODEL_DEPENDENCY_NOT_PUBLIC',
  'direct Process publish fails when an exact included Process is private'
);

select is(
  (
    select result#>>'{details,path}'
    from path_role_results
    where label = 'publish_included_process'
  ),
  'json.processDataSet.processInformation.technology.referenceToIncludedProcesses[0]',
  'direct Process publish identifies the caller-visible included Process path'
);

select pg_temp.is_deeply(
  jsonb_build_object(
    'rootState', (
      select state_code
      from public.processes
      where id = '33800000-0000-0000-0000-000000000020'
        and version = '01.00.000'
    ),
    'dependencyState', (
      select state_code
      from public.processes
      where id = '33800000-0000-0000-0000-000000000016'
        and version = '01.00.000'
    ),
    'auditCount', (
      select count(*)
      from private.command_audit_log
      where target_id = '33800000-0000-0000-0000-000000000020'
    )
  ),
  '{"rootState":0,"dependencyState":0,"auditCount":0}'::jsonb,
  'failed direct included Process publish leaves root, dependency, and audit unchanged'
);

insert into path_role_results (label, result)
values (
  'submit_lineage_and_support',
  api.cmd_review_submit_without_gate(
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

select is(
  (
    select state_code::text
    from public.processes
    where id = '33800000-0000-0000-0000-000000000014'
      and version = '01.00.000'
  ),
  '20',
  'submit includes a path-inferred Process composition dependency in the review lifecycle'
);

select ok(
  exists (
    select 1
    from pg_catalog.pg_locks
    where pid = pg_catalog.pg_backend_pid()
      and relation = 'public.lifecyclemodels'::regclass
      and mode = 'ShareRowExclusiveLock'
      and granted
  ),
  'Process-root submit holds the lifecyclemodels phantom fence through the transaction'
);

select is(
  pg_temp.concurrent_lifecycle_insert_sqlstate(),
  '57014',
  'a concurrent unrelated LifecycleModel insert waits behind the Process-root phantom fence'
);

insert into path_role_results (label, result)
values (
  'submit_unknown_path',
  api.cmd_review_submit_without_gate(
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

select pg_temp.is_deeply(
  jsonb_build_object(
    'reviewCount', (
      select count(*)
      from private.reviews
      where data_id = '33800000-0000-0000-0000-000000000003'
    ),
    'auditCount', (
      select count(*)
      from private.command_audit_log
      where target_id = '33800000-0000-0000-0000-000000000003'
    )
  ),
  '{"reviewCount":0,"auditCount":0}'::jsonb,
  'malformed included Process UUID writes no review or audit rows'
);

insert into path_role_results (label, result)
values
  (
    'submit_bad_composition_version',
    api.cmd_review_submit_without_gate(
      'processes',
      '33800000-0000-0000-0000-000000000017',
      '01.00.000',
      '{"test":"path-role-characterization"}'::jsonb
    )
  ),
  (
    'submit_private_paired_model',
    api.cmd_review_submit_without_gate(
      'processes',
      '33800000-0000-0000-0000-000000000018',
      '01.00.000',
      '{"test":"path-role-characterization"}'::jsonb
    )
  );

select is(
  (select result->>'code' from path_role_results where label = 'submit_bad_composition_version'),
  'REFERENCE_ROLE_POLICY_GAP',
  'malformed included Process version fails submit with a stable policy-gap code'
);

select pg_temp.is_deeply(
  jsonb_build_object(
    'rootState', (
      select state_code
      from public.processes
      where id = '33800000-0000-0000-0000-000000000017'
        and version = '01.00.000'
    ),
    'reviewCount', (
      select count(*)
      from private.reviews
      where data_id = '33800000-0000-0000-0000-000000000017'
    ),
    'auditCount', (
      select count(*)
      from private.command_audit_log
      where target_id = '33800000-0000-0000-0000-000000000017'
    )
  ),
  '{"rootState":0,"reviewCount":0,"auditCount":0}'::jsonb,
  'malformed included Process version leaves submit state, reviews, and audits unchanged'
);

select is(
  (select result->>'code' from path_role_results where label = 'submit_private_paired_model'),
  'MODEL_DEPENDENCY_NOT_PUBLIC',
  'Process-root submit fails closed when an exact private paired LifecycleModel exists'
);

select pg_temp.is_deeply(
  jsonb_build_object(
    'processState', (
      select state_code
      from public.processes
      where id = '33800000-0000-0000-0000-000000000018'
        and version = '01.00.000'
    ),
    'modelState', (
      select state_code
      from public.lifecyclemodels
      where id = '33800000-0000-0000-0000-000000000018'
        and version = '01.00.000'
    ),
    'reviewCount', (
      select count(*)
      from private.reviews
      where data_id = '33800000-0000-0000-0000-000000000018'
    ),
    'auditCount', (
      select count(*)
      from private.command_audit_log
      where target_id = '33800000-0000-0000-0000-000000000018'
    )
  ),
  '{"processState":0,"modelState":0,"reviewCount":0,"auditCount":0}'::jsonb,
  'failed paired Process submit leaves Process, model, reviews, and audits unchanged'
);

insert into path_role_results (label, result)
values
  (
    'submit_mismatched_composition',
    api.cmd_review_submit_without_gate(
      'lifecyclemodels',
      '34800000-0000-0000-0000-000000000012',
      '01.00.000',
      '{"test":"path-role-characterization"}'::jsonb
    )
  ),
  (
    'submit_foreign_composition',
    api.cmd_review_submit_without_gate(
      'lifecyclemodels',
      '34800000-0000-0000-0000-000000000001',
      '01.00.000',
      '{"test":"path-role-characterization"}'::jsonb
    )
  ),
  (
    'submit_missing_composition',
    api.cmd_review_submit_without_gate(
      'lifecyclemodels',
      '34800000-0000-0000-0000-000000000002',
      '01.00.000',
      '{"test":"path-role-characterization"}'::jsonb
    )
  );

select is(
  (select result->>'ok' from path_role_results where label = 'submit_mismatched_composition'),
  'true',
  'stale json_tg composition does not block authoritative ILCD and relational model targets'
);

select pg_temp.is_deeply(
  jsonb_build_object(
    'rootState', (
      select state_code
      from public.lifecyclemodels
      where id = '34800000-0000-0000-0000-000000000012'
        and version = '01.00.000'
    ),
    'ilcdProcessState', (
      select state_code
      from public.processes
      where id = '33800000-0000-0000-0000-000000000012'
        and version = '01.00.000'
    ),
    'jsonTgProcessState', (
      select state_code
      from public.processes
      where id = '33800000-0000-0000-0000-000000000013'
        and version = '01.00.000'
    ),
    'relationProcessState', (
      select state_code
      from public.processes
      where id = '33800000-0000-0000-0000-000000000021'
        and version = '01.00.000'
    ),
    'reviewCount', (
      select count(*)
      from private.reviews
      where data_id = '34800000-0000-0000-0000-000000000012'
    ),
    'auditCount', (
      select count(*)
      from private.command_audit_log
      where target_id = '34800000-0000-0000-0000-000000000012'
    )
  ),
  '{
    "rootState": 20,
    "ilcdProcessState": 20,
    "jsonTgProcessState": 0,
    "relationProcessState": 20,
    "reviewCount": 1,
    "auditCount": 1
  }'::jsonb,
  'review follows ILCD sources and relational model results while ignoring json_tg-only entries'
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

select pg_temp.is_deeply(
  pg_temp.public_error_shape((
    select result
    from path_role_results
    where label = 'submit_foreign_composition'
  )),
  pg_temp.public_error_shape((
    select result
    from path_role_results
    where label = 'submit_missing_composition'
  )),
  'private and missing composition expose the same public error envelope and key schema'
);

select ok(
  (
    select bool_and(
      result::text !~* '(not found|private|state_code|owner|Foreign Secret)'
    )
    from path_role_results
    where label in ('submit_foreign_composition', 'submit_missing_composition')
  ),
  'private and missing composition messages do not reveal existence, state, owner, name, or content'
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
    from private.reviews
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
    from private.command_audit_log
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
  'json.lifeCycleModelDataSet.lifeCycleModelInformation.technology.processes.processInstance[0].referenceToProcess',
  'composition error identifies only the authoritative ILCD reference path'
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

insert into path_role_snapshots (label, snapshot)
values (
  'paired_process_approve_before',
  jsonb_build_object(
    'process', (
      select to_jsonb(process)
      from public.processes as process
      where id = '33800000-0000-0000-0000-000000000019'
        and version = '01.00.000'
    ),
    'model', (
      select to_jsonb(model)
      from public.lifecyclemodels as model
      where id = '33800000-0000-0000-0000-000000000019'
        and version = '01.00.000'
    ),
    'review', (
      select to_jsonb(review)
      from private.reviews as review
      where id = '53800000-0000-0000-0000-000000000019'
    ),
    'comments', coalesce((
      select jsonb_agg(to_jsonb(comment) order by comment.created_at)
      from private.comments as comment
      where review_id = '53800000-0000-0000-0000-000000000019'
    ), '[]'::jsonb),
    'audits', coalesce((
      select jsonb_agg(to_jsonb(audit) order by audit.id)
      from private.command_audit_log as audit
      where target_id in (
        '33800000-0000-0000-0000-000000000019',
        '53800000-0000-0000-0000-000000000019'
      )
    ), '[]'::jsonb)
  )
);

insert into path_role_results (label, result)
values (
  'approve_private_paired_model',
  api.cmd_review_approve(
    'processes',
    '53800000-0000-0000-0000-000000000019',
    '{"test":"path-role-characterization"}'::jsonb
  )
);

select is(
  (select result->>'code' from path_role_results where label = 'approve_private_paired_model'),
  'MODEL_DEPENDENCY_NOT_PUBLIC',
  'Process-root approve fails closed when an exact private paired LifecycleModel exists'
);

select pg_temp.is_deeply(
  jsonb_build_object(
    'process', (
      select to_jsonb(process)
      from public.processes as process
      where id = '33800000-0000-0000-0000-000000000019'
        and version = '01.00.000'
    ),
    'model', (
      select to_jsonb(model)
      from public.lifecyclemodels as model
      where id = '33800000-0000-0000-0000-000000000019'
        and version = '01.00.000'
    ),
    'review', (
      select to_jsonb(review)
      from private.reviews as review
      where id = '53800000-0000-0000-0000-000000000019'
    ),
    'comments', coalesce((
      select jsonb_agg(to_jsonb(comment) order by comment.created_at)
      from private.comments as comment
      where review_id = '53800000-0000-0000-0000-000000000019'
    ), '[]'::jsonb),
    'audits', coalesce((
      select jsonb_agg(to_jsonb(audit) order by audit.id)
      from private.command_audit_log as audit
      where target_id in (
        '33800000-0000-0000-0000-000000000019',
        '53800000-0000-0000-0000-000000000019'
      )
    ), '[]'::jsonb)
  ),
  (
    select snapshot
    from path_role_snapshots
    where label = 'paired_process_approve_before'
  ),
  'failed paired Process approve leaves Process, model, review, comments, and audits byte-for-byte unchanged'
);

insert into path_role_results (label, result)
values (
  'approve_lineage_and_support',
  api.cmd_review_approve(
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

select is(
  (
    select state_code::text
    from public.processes
    where id = '33800000-0000-0000-0000-000000000015'
      and version = '01.00.000'
  ),
  '100',
  'approve publishes an exact included Process version in the review lifecycle'
);

insert into path_role_snapshots (label, snapshot)
values (
  'approve_composition_before',
  jsonb_build_object(
    'root', (
      select to_jsonb(model)
      from public.lifecyclemodels as model
      where id = '34800000-0000-0000-0000-000000000011'
        and version = '01.00.000'
    ),
    'dependency', (
      select to_jsonb(process)
      from public.processes as process
      where id = '33800000-0000-0000-0000-000000000011'
        and version = '01.00.000'
    ),
    'review', (
      select to_jsonb(review)
      from private.reviews as review
      where id = '53800000-0000-0000-0000-000000000011'
    )
  )
);

insert into path_role_snapshots (label, snapshot)
values (
  'approve_comments_before',
  coalesce((
    select jsonb_agg(
      to_jsonb(comment)
      order by comment.review_id, comment.reviewer_id, comment.created_at
    )
    from private.comments as comment
    where review_id = '53800000-0000-0000-0000-000000000011'
  ), '[]'::jsonb)
);

insert into path_role_snapshots (label, snapshot)
values (
  'approve_audits_before',
  coalesce((
    select jsonb_agg(to_jsonb(audit) order by audit.id)
    from private.command_audit_log as audit
    where target_id in (
      '34800000-0000-0000-0000-000000000011',
      '53800000-0000-0000-0000-000000000011'
    )
  ), '[]'::jsonb)
);

insert into path_role_results (label, result)
values (
  'approve_foreign_composition',
  api.cmd_review_approve(
    'lifecyclemodels',
    '53800000-0000-0000-0000-000000000011',
    '{"test":"path-role-characterization"}'::jsonb
  )
);

select is(
  (select result->>'code' from path_role_results where label = 'approve_foreign_composition'),
  'MODEL_DEPENDENCY_NOT_PUBLIC',
  'approve fails closed for a cross-owner private exact included Process'
);

select pg_temp.is_deeply(
  jsonb_build_object(
    'root', (
      select to_jsonb(model)
      from public.lifecyclemodels as model
      where id = '34800000-0000-0000-0000-000000000011'
        and version = '01.00.000'
    ),
    'dependency', (
      select to_jsonb(process)
      from public.processes as process
      where id = '33800000-0000-0000-0000-000000000011'
        and version = '01.00.000'
    ),
    'review', (
      select to_jsonb(review)
      from private.reviews as review
      where id = '53800000-0000-0000-0000-000000000011'
    )
  ),
  (
    select snapshot
    from path_role_snapshots
    where label = 'approve_composition_before'
  ),
  'failed approve leaves root, dependency, and review rows byte-for-byte unchanged'
);

select pg_temp.is_deeply(
  coalesce((
    select jsonb_agg(
      to_jsonb(comment)
      order by comment.review_id, comment.reviewer_id, comment.created_at
    )
    from private.comments as comment
    where review_id = '53800000-0000-0000-0000-000000000011'
  ), '[]'::jsonb),
  (
    select snapshot
    from path_role_snapshots
    where label = 'approve_comments_before'
  ),
  'failed approve leaves review comments byte-for-byte unchanged'
);

select pg_temp.is_deeply(
  coalesce((
    select jsonb_agg(to_jsonb(audit) order by audit.id)
    from private.command_audit_log as audit
    where target_id in (
      '34800000-0000-0000-0000-000000000011',
      '53800000-0000-0000-0000-000000000011'
    )
  ), '[]'::jsonb),
  (
    select snapshot
    from path_role_snapshots
    where label = 'approve_audits_before'
  ),
  'failed approve writes no root or review command audit'
);

select ok(
  (
    select result::text not like '%Approve foreign dependency%'
      and result::text not like '%12800000-0000-0000-0000-000000000002%'
      and result::text !~* '(not found|private|state_code|owner)'
    from path_role_results
    where label = 'approve_foreign_composition'
  ),
  'approve composition failure does not disclose foreign existence, state, owner, name, or content'
);

select set_config(
  'request.jwt.claim.sub',
  '12800000-0000-0000-0000-000000000001',
  true
);

insert into path_role_results (label, result)
values (
  'publish_paired_process',
  api.cmd_dataset_publish(
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
  api.cmd_dataset_publish(
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
