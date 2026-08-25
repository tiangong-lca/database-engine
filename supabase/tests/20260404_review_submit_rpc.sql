begin;

create extension if not exists pgtap with schema extensions;
set local search_path = extensions, public, auth;

create or replace function pg_temp.disable_trigger_if_exists(p_table regclass, p_trigger name)
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
    '12000000-0000-0000-0000-000000000001',
    'authenticated',
    'authenticated',
    'review-owner@example.com',
    'test-password-hash',
    now(),
    '{"provider":"email","providers":["email"]}'::jsonb,
    '{"sub":"12000000-0000-0000-0000-000000000001","email":"review-owner@example.com","display_name":"Review Owner"}'::jsonb,
    now(),
    now(),
    false,
    false
  ),
  (
    '00000000-0000-0000-0000-000000000000',
    '12000000-0000-0000-0000-000000000002',
    'authenticated',
    'authenticated',
    'outsider@example.com',
    'test-password-hash',
    now(),
    '{"provider":"email","providers":["email"]}'::jsonb,
    '{"sub":"12000000-0000-0000-0000-000000000002","email":"outsider@example.com","display_name":"Outsider"}'::jsonb,
    now(),
    now(),
    false,
    false
  );

insert into private.users (id, raw_user_meta_data)
values
  (
    '12000000-0000-0000-0000-000000000001',
    '{"email":"review-owner@example.com","display_name":"Review Owner"}'::jsonb
  ),
  (
    '12000000-0000-0000-0000-000000000002',
    '{"email":"outsider@example.com","display_name":"Outsider"}'::jsonb
  );

insert into private.teams (id, json, rank, is_public)
values
  ('22000000-0000-0000-0000-000000000001', '{"title":"Review Team"}'::jsonb, 1, false);

insert into private.roles (user_id, team_id, role)
values
  ('12000000-0000-0000-0000-000000000001', '22000000-0000-0000-0000-000000000001', 'owner');

alter table public.sources disable trigger "sources_json_sync_trigger";
alter table public.flowproperties disable trigger "flowproperties_json_sync_trigger";
alter table public.flows disable trigger "flows_json_sync_trigger";
alter table public.processes disable trigger "processes_json_sync_trigger";
alter table public.lifecyclemodels disable trigger "lifecyclemodels_json_sync_trigger";

alter table public.processes disable trigger "process_extract_md_trigger_insert";
select pg_temp.disable_trigger_if_exists('public.processes'::regclass, 'process_extract_text_trigger_insert');
do $$
begin
  if exists (
    select 1 from pg_trigger
    where tgrelid = 'public.flows'::regclass
      and tgname = 'flow_extract_md_trigger_insert'
  ) then
    alter table public.flows disable trigger "flow_extract_md_trigger_insert";
  end if;

  if exists (
    select 1 from pg_trigger
    where tgrelid = 'public.flows'::regclass
      and tgname = 'flow_extract_text_trigger_insert'
  ) then
    alter table public.flows disable trigger "flow_extract_text_trigger_insert";
  end if;

  if exists (
    select 1 from pg_trigger
    where tgrelid = 'public.flows'::regclass
      and tgname = 'flow_dataset_extraction_trigger_insert'
  ) then
    alter table public.flows disable trigger "flow_dataset_extraction_trigger_insert";
  end if;
end
$$;
alter table public.lifecyclemodels disable trigger "lifecyclemodel_extract_md_trigger_insert";
select pg_temp.disable_trigger_if_exists('public.lifecyclemodels'::regclass, 'lifecyclemodels_extract_text_trigger_insert');

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
values (
  '32000000-0000-0000-0000-000000000001',
  '01.00.000',
  '{
    "flowDataSet": {
      "flowInformation": {
        "dataSetInformation": {
          "name": {
            "baseName": [
              { "@xml:lang": "en", "#text": "Draft Flow" }
            ]
          }
        }
      },
      "flowProperties": {
        "flowProperty": [
          {
            "referenceToFlowPropertyDataSet": {
              "@type": "flow property data set",
              "@refObjectId": "32000000-0000-0000-0000-000000000004",
              "@version": "01.00.000"
            }
          }
        ]
      }
    }
  }'::jsonb,
  '{
    "flowDataSet": {
      "flowInformation": {
        "dataSetInformation": {
          "name": {
            "baseName": [
              { "@xml:lang": "en", "#text": "Draft Flow" }
            ]
          }
        }
      },
      "flowProperties": {
        "flowProperty": [
          {
            "referenceToFlowPropertyDataSet": {
              "@type": "flow property data set",
              "@refObjectId": "32000000-0000-0000-0000-000000000004",
              "@version": "01.00.000"
            }
          }
        ]
      }
    }
  }'::json,
  '12000000-0000-0000-0000-000000000001',
  0,
  '22000000-0000-0000-0000-000000000001',
  true
);

insert into public.flowproperties (
  id,
  version,
  json,
  json_ordered,
  user_id,
  state_code,
  team_id,
  rule_verification
)
values (
  '32000000-0000-0000-0000-000000000004',
  '01.00.000',
  '{
    "flowPropertyDataSet": {
      "flowPropertiesInformation": {
        "dataSetInformation": {
          "common:name": [
            { "@xml:lang": "en", "#text": "Draft Flow Property" }
          ]
        }
      }
    }
  }'::jsonb,
  '{
    "flowPropertyDataSet": {
      "flowPropertiesInformation": {
        "dataSetInformation": {
          "common:name": [
            { "@xml:lang": "en", "#text": "Draft Flow Property" }
          ]
        }
      }
    }
  }'::json,
  '12000000-0000-0000-0000-000000000001',
  0,
  '22000000-0000-0000-0000-000000000001',
  true
);

insert into public.sources (
  id,
  version,
  json,
  json_ordered,
  user_id,
  state_code,
  team_id,
  rule_verification
)
values (
  '32000000-0000-0000-0000-000000000002',
  '01.00.000',
  '{
    "sourceDataSet": {
      "sourceInformation": {
        "dataSetInformation": {
          "common:shortName": [
            { "@xml:lang": "en", "#text": "Published Source" }
          ]
        }
      }
    }
  }'::jsonb,
  '{
    "sourceDataSet": {
      "sourceInformation": {
        "dataSetInformation": {
          "common:shortName": [
            { "@xml:lang": "en", "#text": "Published Source" }
          ]
        }
      }
    }
  }'::json,
  null,
  100,
  '22000000-0000-0000-0000-000000000001',
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
  rule_verification
)
values (
  '32000000-0000-0000-0000-000000000003',
  '01.00.000',
  '{
    "processDataSet": {
      "processInformation": {
        "dataSetInformation": {
          "name": {
            "baseName": [
              { "@xml:lang": "en", "#text": "Draft Process" }
            ]
          }
        }
      },
      "exchanges": {
        "exchange": [
          {
            "referenceToFlowDataSet": {
              "@type": "flow data set",
              "@refObjectId": "32000000-0000-0000-0000-000000000001",
              "@version": "01.00.000"
            }
          },
          {
            "referencesToDataSource": {
              "referenceToDataSource": {
                "@type": "source data set",
                "@refObjectId": "32000000-0000-0000-0000-000000000002",
                "@version": "01.00.000"
              }
            }
          }
        ]
      }
    }
  }'::jsonb,
  '{
    "processDataSet": {
      "processInformation": {
        "dataSetInformation": {
          "name": {
            "baseName": [
              { "@xml:lang": "en", "#text": "Draft Process" }
            ]
          }
        }
      },
      "exchanges": {
        "exchange": [
          {
            "referenceToFlowDataSet": {
              "@type": "flow data set",
              "@refObjectId": "32000000-0000-0000-0000-000000000001",
              "@version": "01.00.000"
            }
          },
          {
            "referencesToDataSource": {
              "referenceToDataSource": {
                "@type": "source data set",
                "@refObjectId": "32000000-0000-0000-0000-000000000002",
                "@version": "01.00.000"
              }
            }
          }
        ]
      }
    }
  }'::json,
  '12000000-0000-0000-0000-000000000001',
  0,
  '22000000-0000-0000-0000-000000000001',
  '42000000-0000-0000-0000-000000000001',
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
  rule_verification
)
values
  (
    '32000000-0000-0000-0000-000000000010',
    '01.00.000',
    '{
      "processDataSet": {
        "processInformation": {
          "dataSetInformation": {
            "name": {
              "baseName": [
                { "@xml:lang": "en", "#text": "Model Root Process" }
              ]
            }
          }
        }
      }
    }'::jsonb,
    '{
      "processDataSet": {
        "processInformation": {
          "dataSetInformation": {
            "name": {
              "baseName": [
                { "@xml:lang": "en", "#text": "Model Root Process" }
              ]
            }
          }
        }
      }
    }'::json,
    '12000000-0000-0000-0000-000000000001',
    0,
    '22000000-0000-0000-0000-000000000001',
    '42000000-0000-0000-0000-000000000002',
    true
  ),
  (
    '32000000-0000-0000-0000-000000000011',
    '01.00.000',
    '{
      "processDataSet": {
        "processInformation": {
          "dataSetInformation": {
            "name": {
              "baseName": [
                { "@xml:lang": "en", "#text": "Secondary Submodel Process" }
              ]
            }
          }
        }
      }
    }'::jsonb,
    '{
      "processDataSet": {
        "processInformation": {
          "dataSetInformation": {
            "name": {
              "baseName": [
                { "@xml:lang": "en", "#text": "Secondary Submodel Process" }
              ]
            }
          }
        }
      }
    }'::json,
    '12000000-0000-0000-0000-000000000001',
    0,
    '22000000-0000-0000-0000-000000000001',
    '42000000-0000-0000-0000-000000000002',
    true
  ),
  (
    '32000000-0000-0000-0000-000000000020',
    '01.00.000',
    '{
      "processDataSet": {
        "processInformation": {
          "dataSetInformation": {
            "name": {
              "baseName": [
                { "@xml:lang": "en", "#text": "Under Review Process" }
              ]
            }
          }
        }
      }
    }'::jsonb,
    '{
      "processDataSet": {
        "processInformation": {
          "dataSetInformation": {
            "name": {
              "baseName": [
                { "@xml:lang": "en", "#text": "Under Review Process" }
              ]
            }
          }
        }
      }
    }'::json,
    '12000000-0000-0000-0000-000000000001',
    20,
    '22000000-0000-0000-0000-000000000001',
    '42000000-0000-0000-0000-000000000003',
    true
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
  rule_verification
)
values (
  '32000000-0000-0000-0000-000000000010',
  '01.00.000',
  '{
    "lifeCycleModelDataSet": {
      "lifeCycleModelInformation": {
        "dataSetInformation": {
          "name": {
            "baseName": [
              { "@xml:lang": "en", "#text": "Draft Lifecycle Model" }
            ]
          }
        },
        "technology": {
          "processes": {
            "processInstance": [
              {
                "referenceToProcess": {
                  "@type": "process data set",
                  "@refObjectId": "32000000-0000-0000-0000-000000000011",
                  "@version": "01.00.000"
                }
              }
            ]
          }
        }
      }
    }
  }'::jsonb,
  '{
    "lifeCycleModelDataSet": {
      "lifeCycleModelInformation": {
        "dataSetInformation": {
          "name": {
            "baseName": [
              { "@xml:lang": "en", "#text": "Draft Lifecycle Model" }
            ]
          }
        },
        "technology": {
          "processes": {
            "processInstance": [
              {
                "referenceToProcess": {
                  "@type": "process data set",
                  "@refObjectId": "32000000-0000-0000-0000-000000000011",
                  "@version": "01.00.000"
                }
              }
            ]
          }
        }
      }
    }
  }'::json,
  '{
    "submodels": [
      { "id": "32000000-0000-0000-0000-000000000011", "type": "secondary" }
    ]
  }'::jsonb,
  '12000000-0000-0000-0000-000000000001',
  0,
  '22000000-0000-0000-0000-000000000001',
  true
);

set local role authenticated;
select set_config('request.jwt.claim.sub', '12000000-0000-0000-0000-000000000001', true);

select is(
  api.cmd_review_submit(
    p_target_table => 'processes',
    p_target_id => '32000000-0000-0000-0000-000000000003',
    p_target_version => '01.00.000',
    p_audit => '{"command":"review_submit"}'::jsonb
  )->>'ok',
  'true',
  'dataset owner can submit a draft process without Gate metadata'
);

select is(
  (select state_code::text
   from public.processes
   where id = '32000000-0000-0000-0000-000000000003'
     and version = '01.00.000'),
  '20',
  'cmd_review_submit marks the root dataset under review'
);

select is(
  (select state_code::text
   from public.flows
   where id = '32000000-0000-0000-0000-000000000001'
     and version = '01.00.000'),
  '20',
  'cmd_review_submit marks draft referenced datasets under review'
);

select is(
  (select state_code::text
   from public.flowproperties
   where id = '32000000-0000-0000-0000-000000000004'
     and version = '01.00.000'),
  '20',
  'cmd_review_submit marks referenced flow properties under review'
);

select is(
  (select state_code::text
   from public.sources
   where id = '32000000-0000-0000-0000-000000000002'
     and version = '01.00.000'),
  '100',
  'cmd_review_submit leaves already published references unchanged'
);

reset role;

select ok(
  exists (
    select 1
    from private.reviews
    where review_kind = 'reference'
      and target_table = 'sources'
      and data_id = '32000000-0000-0000-0000-000000000002'
      and btrim(data_version::text) = '01.00.000'
      and state_code = 2
      and target_owner_id is null
  ),
  'cmd_review_submit auto-approves an ownerless published reference'
);

set local role authenticated;
select set_config('request.jwt.claim.sub', '12000000-0000-0000-0000-000000000001', true);

select is(
  (select count(*)::text
   from private.reviews
   where data_id = '32000000-0000-0000-0000-000000000003'
     and data_version = '01.00.000'),
  '1',
  'cmd_review_submit creates one review row for the root dataset'
);

select ok(
  exists(
    select 1
    from private.reviews
    where data_id = '32000000-0000-0000-0000-000000000003'
      and data_version = '01.00.000'
      and json->'user'->>'id' = '12000000-0000-0000-0000-000000000001'
      and json->'team'->>'id' = '22000000-0000-0000-0000-000000000001'
  ),
  'cmd_review_submit records review json metadata for the submitter and team'
);

reset role;

select ok(
  exists(
    select 1
    from private.command_audit_log
    where command = 'cmd_review_submit'
      and target_id = '32000000-0000-0000-0000-000000000003'
      and target_version = '01.00.000'
  ),
  'cmd_review_submit writes a command audit log entry'
);

set local role authenticated;
select set_config('request.jwt.claim.sub', '12000000-0000-0000-0000-000000000001', true);

select is(
  api.cmd_review_submit(
    'lifecyclemodels',
    '32000000-0000-0000-0000-000000000010',
    '01.00.000',
    '{"command":"review_submit"}'::jsonb
  )->>'ok',
  'true',
  'dataset owner can submit a lifecycle model and its linked draft processes for review'
);

select is(
  (select state_code::text
   from public.lifecyclemodels
   where id = '32000000-0000-0000-0000-000000000010'
     and version = '01.00.000'),
  '20',
  'cmd_review_submit marks the root lifecycle model under review'
);

select ok(
  (
    select count(*)
    from public.processes
    where id in (
      '32000000-0000-0000-0000-000000000010',
      '32000000-0000-0000-0000-000000000011'
    )
      and version = '01.00.000'
      and state_code = 20
  ) = 2,
  'cmd_review_submit promotes linked lifecycle model process rows into under-review state'
);

reset role;

set local role authenticated;
select set_config('request.jwt.claim.sub', '12000000-0000-0000-0000-000000000002', true);

select is(
  api.cmd_review_submit(
    'processes',
    '32000000-0000-0000-0000-000000000003',
    '01.00.000',
    '{}'::jsonb
  )->>'code',
  'ROOT_DATASET_NOT_OWNED',
  'non-owners cannot submit another user''s dataset for review'
);

reset role;

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
values (
  '32000000-0000-0000-0000-000000000021',
  '01.00.000',
  '{
    "flowDataSet": {
      "flowInformation": {
        "dataSetInformation": {
          "name": {
            "baseName": [
              { "@xml:lang": "en", "#text": "Under Review Flow" }
            ]
          }
        }
      }
    }
  }'::jsonb,
  '{
    "flowDataSet": {
      "flowInformation": {
        "dataSetInformation": {
          "name": {
            "baseName": [
              { "@xml:lang": "en", "#text": "Under Review Flow" }
            ]
          }
        }
      }
    }
  }'::json,
  '12000000-0000-0000-0000-000000000001',
  20,
  '22000000-0000-0000-0000-000000000001',
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
  rule_verification
)
values (
  '32000000-0000-0000-0000-000000000022',
  '01.00.000',
  '{
    "processDataSet": {
      "processInformation": {
        "dataSetInformation": {
          "name": {
            "baseName": [
              { "@xml:lang": "en", "#text": "Shared Reference Process" }
            ]
          }
        }
      },
      "exchanges": {
        "exchange": [
          {
            "referenceToFlowDataSet": {
              "@type": "flow data set",
              "@refObjectId": "32000000-0000-0000-0000-000000000021",
              "@version": "01.00.000"
            }
          }
        ]
      }
    }
  }'::jsonb,
  '{
    "processDataSet": {
      "processInformation": {
        "dataSetInformation": {
          "name": {
            "baseName": [
              { "@xml:lang": "en", "#text": "Shared Reference Process" }
            ]
          }
        }
      },
      "exchanges": {
        "exchange": [
          {
            "referenceToFlowDataSet": {
              "@type": "flow data set",
              "@refObjectId": "32000000-0000-0000-0000-000000000021",
              "@version": "01.00.000"
            }
          }
        ]
      }
    }
  }'::json,
  '12000000-0000-0000-0000-000000000001',
  0,
  '22000000-0000-0000-0000-000000000001',
  '42000000-0000-0000-0000-000000000004',
  true
);

set local role authenticated;
select set_config('request.jwt.claim.sub', '12000000-0000-0000-0000-000000000001', true);

select is(
  api.cmd_review_submit(
    p_target_table => 'processes',
    p_target_id => '32000000-0000-0000-0000-000000000022',
    p_target_version => '01.00.000',
    p_audit => '{}'::jsonb
  )->>'ok',
  'true',
  'review submission succeeds when a referenced dataset is already under review'
);

select is(
  (select count(*)::text
   from private.reviews
   where data_id = '32000000-0000-0000-0000-000000000022'
     and data_version = '01.00.000'),
  '1',
  'review submission with an under-review reference creates one review row'
);

select is(
  (select state_code::text
   from public.flows
   where id = '32000000-0000-0000-0000-000000000021'
     and version = '01.00.000'),
  '20',
  'review submission preserves the referenced dataset under-review state'
);

reset role;

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
values (
  '32000000-0000-0000-0000-000000000021',
  '02.00.000',
  '{
    "flowDataSet": {
      "flowInformation": {
        "dataSetInformation": {
          "name": {
            "baseName": [
              { "@xml:lang": "en", "#text": "Parallel Review Flow" }
            ]
          }
        }
      }
    }
  }'::jsonb,
  '{
    "flowDataSet": {
      "flowInformation": {
        "dataSetInformation": {
          "name": {
            "baseName": [
              { "@xml:lang": "en", "#text": "Parallel Review Flow" }
            ]
          }
        }
      }
    }
  }'::json,
  '12000000-0000-0000-0000-000000000001',
  0,
  '22000000-0000-0000-0000-000000000001',
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
  rule_verification
)
values (
  '32000000-0000-0000-0000-000000000023',
  '01.00.000',
  '{
    "processDataSet": {
      "processInformation": {
        "dataSetInformation": {
          "name": {
            "baseName": [
              { "@xml:lang": "en", "#text": "Parallel Version Reference Process" }
            ]
          }
        }
      },
      "exchanges": {
        "exchange": [
          {
            "referenceToFlowDataSet": {
              "@type": "flow data set",
              "@refObjectId": "32000000-0000-0000-0000-000000000021",
              "@version": "02.00.000"
            }
          }
        ]
      }
    }
  }'::jsonb,
  '{
    "processDataSet": {
      "processInformation": {
        "dataSetInformation": {
          "name": {
            "baseName": [
              { "@xml:lang": "en", "#text": "Parallel Version Reference Process" }
            ]
          }
        }
      },
      "exchanges": {
        "exchange": [
          {
            "referenceToFlowDataSet": {
              "@type": "flow data set",
              "@refObjectId": "32000000-0000-0000-0000-000000000021",
              "@version": "02.00.000"
            }
          }
        ]
      }
    }
  }'::json,
  '12000000-0000-0000-0000-000000000001',
  0,
  '22000000-0000-0000-0000-000000000001',
  '42000000-0000-0000-0000-000000000004',
  true
);

set local role authenticated;
select set_config('request.jwt.claim.sub', '12000000-0000-0000-0000-000000000001', true);

select is(
  api.cmd_review_submit(
    p_target_table => 'processes',
    p_target_id => '32000000-0000-0000-0000-000000000023',
    p_target_version => '01.00.000',
    p_audit => '{}'::jsonb
  )->>'ok',
  'true',
  'review submission succeeds while another referenced dataset version is under review'
);

reset role;

select is(
  (select count(*)::text
   from private.reviews
   where review_kind = 'reference'
     and target_table = 'flows'
     and data_id = '32000000-0000-0000-0000-000000000021'
     and state_code in (0, 1)),
  '2',
  'different versions keep distinct active Reference Reviews'
);

select is(
  (select count(*)::text
   from private.review_derive_current_references_v1(array[
     (select id
      from private.reviews
      where review_kind = 'root'
        and target_table = 'processes'
        and data_id = '32000000-0000-0000-0000-000000000023'
        and data_version = '01.00.000')
   ])
   where target_table = 'flows'
     and data_id = '32000000-0000-0000-0000-000000000021'
     and data_version = '02.00.000'),
  '1',
  'the new Root derives only its exact referenced version'
);

select is(
  (select count(*)::text
   from public.flows
   where id = '32000000-0000-0000-0000-000000000021'
     and version in ('01.00.000', '02.00.000')
     and state_code = 20),
  '2',
  'parallel reviewed versions keep independent dataset rows'
);

insert into public.flowproperties (
  id,
  version,
  json,
  json_ordered,
  user_id,
  state_code,
  team_id,
  rule_verification,
  reviews
)
values (
  '32000000-0000-4000-8000-000000000031',
  '01.00.000',
  '{"flowPropertyDataSet":{"flowPropertiesInformation":{"dataSetInformation":{"common:name":[{"@xml:lang":"en","#text":"Ownerless draft property"}]}}}}'::jsonb,
  '{"flowPropertyDataSet":{"flowPropertiesInformation":{"dataSetInformation":{"common:name":[{"@xml:lang":"en","#text":"Ownerless draft property"}]}}}}'::json,
  null,
  0,
  null,
  true,
  '[]'::jsonb
);

insert into public.flows (
  id,
  version,
  json,
  json_ordered,
  user_id,
  state_code,
  team_id,
  rule_verification,
  reviews
)
values (
  '32000000-0000-4000-8000-000000000030',
  '01.00.000',
  '{"flowDataSet":{"flowInformation":{"dataSetInformation":{"name":{"baseName":[{"@xml:lang":"en","#text":"Ownerless draft reference root"}]}}},"flowProperties":{"flowProperty":[{"referenceToFlowPropertyDataSet":{"@type":"flow property data set","@refObjectId":"32000000-0000-4000-8000-000000000031","@version":"01.00.000"}}]}}}'::jsonb,
  '{"flowDataSet":{"flowInformation":{"dataSetInformation":{"name":{"baseName":[{"@xml:lang":"en","#text":"Ownerless draft reference root"}]}}},"flowProperties":{"flowProperty":[{"referenceToFlowPropertyDataSet":{"@type":"flow property data set","@refObjectId":"32000000-0000-4000-8000-000000000031","@version":"01.00.000"}}]}}}'::json,
  '12000000-0000-0000-0000-000000000001',
  0,
  null,
  true,
  '[]'::jsonb
);

set local role authenticated;
select set_config('request.jwt.claim.sub', '12000000-0000-0000-0000-000000000001', true);

select is(
  api.cmd_review_submit(
    'flows',
    '32000000-0000-4000-8000-000000000030',
    '01.00.000',
    '{}'::jsonb
  )->>'code',
  'REFERENCE_OWNER_UNRESOLVED',
  'draft references still require a resolvable owner'
);

reset role;

select is(
  (
    select state_code
    from public.flows
    where id = '32000000-0000-4000-8000-000000000030'
      and version = '01.00.000'
  ),
  0,
  'ownerless draft reference rejection leaves the root draft unchanged'
);

select is(
  (
    select state_code
    from public.flowproperties
    where id = '32000000-0000-4000-8000-000000000031'
      and version = '01.00.000'
  ),
  0,
  'ownerless draft reference rejection leaves the reference draft unchanged'
);

select is(
  (
    select count(*)::integer
    from private.reviews
    where data_id in (
      '32000000-0000-4000-8000-000000000030',
      '32000000-0000-4000-8000-000000000031'
    )
  ),
  0,
  'ownerless draft reference rejection creates no partial review rows'
);

select * from finish();
rollback;
