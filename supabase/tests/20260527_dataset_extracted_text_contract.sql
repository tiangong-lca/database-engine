begin;

create extension if not exists pgtap with schema extensions;
set local search_path = extensions, public, auth;

select plan(24);

select ok(
  util.dataset_json_search_text(
    'flows',
    '{
      "flowDataSet": {
        "@xsi:schemaLocation": "http://lca.jrc.it/ILCD/Flow ../../schemas/ILCD_FlowDataSet.xsd",
        "flowInformation": {
          "dataSetInformation": {
            "common:UUID": "97000000-0000-0000-0000-000000000088",
            "name": {
              "baseName": [
                {"@xml:lang":"zh","#text":"交流电"},
                {"@xml:lang":"en","#text":"electricity"},
                {"@xml:lang":"de","#text":"Wechselstrom"}
              ]
            },
            "common:synonyms": [
              {"@xml:lang":"zh","#text":"交流电力"},
              {"@xml:lang":"en","#text":"alternating current electricity"}
            ],
            "classificationInformation": {
              "common:classification": {
                "common:class": {"@classId":"class-ac-001", "#text":"能源"}
              }
            },
            "common:other": {
              "ecn:ECNumber": "EC-123-456"
            }
          }
        },
        "administrativeInformation": {
          "dataEntryBy": {
            "common:referenceToDataSetFormat": {
              "@uri": "../sources/97000000-0000-0000-0000-000000000088.xml",
              "@refObjectId": "97000000-0000-0000-0000-000000000088",
              "common:shortDescription": {"@xml:lang":"en","#text":"ILCD format"}
            }
          }
        }
      }
    }'::jsonb
  ) ~ '交流电'
  and util.dataset_json_search_text(
    'flows',
    '{
      "flowDataSet": {
        "flowInformation": {
          "dataSetInformation": {
            "name": {
              "baseName": [
                {"@xml:lang":"zh","#text":"交流电"},
                {"@xml:lang":"en","#text":"electricity"},
                {"@xml:lang":"de","#text":"Wechselstrom"}
              ]
            },
            "common:synonyms": [{"@xml:lang":"en","#text":"alternating current electricity"}],
            "classificationInformation": {
              "common:classification": {
                "common:class": {"@classId":"class-ac-001", "#text":"能源"}
              }
            },
            "common:other": {"ecn:ECNumber": "EC-123-456"}
          }
        },
        "administrativeInformation": {
          "dataEntryBy": {
            "common:referenceToDataSetFormat": {
              "@uri": "../sources/97000000-0000-0000-0000-000000000088.xml",
              "@refObjectId": "97000000-0000-0000-0000-000000000088",
              "common:shortDescription": {"@xml:lang":"en","#text":"ILCD format"}
            }
          }
        }
      }
    }'::jsonb
  ) ~ 'electricity'
  and util.dataset_json_search_text(
    'flows',
    '{
      "flowDataSet": {
        "flowInformation": {
          "dataSetInformation": {
            "name": {"baseName": [{"@xml:lang":"de","#text":"Wechselstrom"}]},
            "classificationInformation": {
              "common:classification": {
                "common:class": {"@classId":"class-ac-001", "#text":"能源"}
              }
            },
            "common:other": {"ecn:ECNumber": "EC-123-456"}
          }
        }
      }
    }'::jsonb
  ) ~ 'Wechselstrom'
  and util.dataset_json_search_text(
    'flows',
    '{
      "flowDataSet": {
        "flowInformation": {
          "dataSetInformation": {
            "name": {"baseName": [{"@xml:lang":"zh","#text":"交流电"}]},
            "classificationInformation": {
              "common:classification": {
                "common:class": {"@classId":"class-ac-001", "#text":"能源"}
              }
            },
            "common:other": {"ecn:ECNumber": "EC-123-456"}
          }
        }
      }
    }'::jsonb
  ) ~ '能源'
  and util.dataset_json_search_text(
    'flows',
    '{
      "flowDataSet": {
        "flowInformation": {
          "dataSetInformation": {
            "name": {"baseName": [{"@xml:lang":"zh","#text":"交流电"}]},
            "common:other": {"ecn:ECNumber": "EC-123-456"}
          }
        }
      }
    }'::jsonb
  ) ~ 'EC-123-456'
  and util.dataset_json_search_text(
    'flows',
    '{
      "flowDataSet": {
        "@xsi:schemaLocation": "http://lca.jrc.it/ILCD/Flow ../../schemas/ILCD_FlowDataSet.xsd",
        "flowInformation": {
          "dataSetInformation": {
            "common:UUID": "97000000-0000-0000-0000-000000000088",
            "name": {"baseName": [{"@xml:lang":"zh","#text":"交流电"}]},
            "classificationInformation": {
              "common:classification": {
                "common:class": {"@classId":"class-ac-001", "#text":"能源"}
              }
            }
          }
        },
        "administrativeInformation": {
          "dataEntryBy": {
            "common:referenceToDataSetFormat": {
              "@uri": "../sources/97000000-0000-0000-0000-000000000088.xml",
              "@refObjectId": "97000000-0000-0000-0000-000000000088",
              "common:shortDescription": {"@xml:lang":"en","#text":"ILCD format"}
            }
          }
        }
      }
    }'::jsonb
  ) !~ '(class-ac-001|97000000-0000-0000-0000-000000000088|ILCD format|schemas)'
  and strpos(util.dataset_json_search_text(
    'flows',
    '{
      "flowDataSet": {
        "flowInformation": {
          "dataSetInformation": {
            "name": {"baseName": [{"@xml:lang":"zh","#text":"交流电"}]}
          }
        },
        "administrativeInformation": {
          "dataEntryBy": {
            "common:referenceToDataSetFormat": {
              "@uri": "../sources/97000000-0000-0000-0000-000000000088.xml",
              "common:shortDescription": {"@xml:lang":"en","#text":"ILCD format"}
            }
          }
        }
      }
    }'::jsonb
  ), '../') = 0,
  'dataset searchable text keeps core authored values and excludes reference metadata noise'
);

select is(
  (
    select count(*)::integer
    from pg_trigger
    where tgname in (
      'zz_contacts_extracted_text_sync_trigger',
      'zz_sources_extracted_text_sync_trigger',
      'zz_unitgroups_extracted_text_sync_trigger',
      'zz_flowproperties_extracted_text_sync_trigger',
      'zz_flows_extracted_text_sync_trigger',
      'zz_processes_extracted_text_sync_trigger',
      'zz_lifecyclemodels_extracted_text_sync_trigger'
    )
      and not tgisinternal
  ),
  7,
  'all searchable dataset entities have database-side extracted_text sync triggers'
);

select is(
  (
    select count(*)::integer
    from pg_trigger
    where tgname in (
      'flow_extract_text_trigger_insert',
      'flow_extract_text_trigger_update',
      'process_extract_text_trigger_insert',
      'process_extract_text_trigger_update',
      'lifecyclemodels_extract_text_trigger_insert',
      'lifecyclemodels_extract_text_trigger_update'
    )
      and not tgisinternal
  ),
  0,
  'core dataset text extraction webhooks are removed'
);

select ok(
  strpos(pg_get_functiondef('util.queue_dataset_extraction_jobs()'::regprocedure), '''extracted_md''') > 0
    and strpos(pg_get_functiondef('util.queue_dataset_extraction_jobs()'::regprocedure), '''extracted_text''') = 0,
  'dataset extraction queue only enqueues markdown extraction jobs'
);

select ok(
  has_function_privilege('service_role', 'public.cmd_dataset_extracted_text_backfill(text, integer, uuid, text, text)', 'execute')
    and not has_function_privilege('authenticated', 'public.cmd_dataset_extracted_text_backfill(text, integer, uuid, text, text)', 'execute')
    and not has_function_privilege('anon', 'public.cmd_dataset_extracted_text_backfill(text, integer, uuid, text, text)', 'execute'),
  'dataset extracted_text historical backfill RPC is service-role only'
);

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
values (
  '00000000-0000-0000-0000-000000000000',
  '96000000-0000-0000-0000-000000000088',
  'authenticated',
  'authenticated',
  'dataset-searchable-text@example.com',
  'test-password-hash',
  now(),
  '{"provider":"email","providers":["email"]}'::jsonb,
  '{"sub":"96000000-0000-0000-0000-000000000088","email":"dataset-searchable-text@example.com"}'::jsonb,
  now(),
  now(),
  false,
  false
);

insert into public.users (id, raw_user_meta_data, contact)
values (
  '96000000-0000-0000-0000-000000000088',
  '{"email":"dataset-searchable-text@example.com"}'::jsonb,
  null
);

insert into public.teams (id, json, rank, is_public)
values ('26000000-0000-0000-0000-000000000088', '{"name":"Searchable Text Team"}'::jsonb, 1, false);

do $$
begin
  if exists (
    select 1 from pg_trigger
    where tgrelid = 'public.flows'::regclass
      and tgname = 'flow_dataset_extraction_trigger_insert'
  ) then
    alter table public.flows disable trigger "flow_dataset_extraction_trigger_insert";
  end if;

  alter table public.processes disable trigger "process_extract_md_trigger_insert";
  alter table public.lifecyclemodels disable trigger "lifecyclemodel_extract_md_trigger_insert";
end
$$;

insert into public.flows (
  id,
  version,
  json_ordered,
  user_id,
  state_code,
  team_id,
  rule_verification
)
values (
  '97000000-0000-0000-0000-000000000088',
  '01.00.000',
  '{
    "flowDataSet": {
      "@xsi:schemaLocation": "http://lca.jrc.it/ILCD/Flow ../../schemas/ILCD_FlowDataSet.xsd",
      "flowInformation": {
        "dataSetInformation": {
          "common:UUID": "97000000-0000-0000-0000-000000000088",
          "name": {
            "baseName": [
              {"@xml:lang": "zh", "#text": "交流电"},
              {"@xml:lang": "en", "#text": "electricity"},
              {"@xml:lang": "de", "#text": "Wechselstrom"}
            ]
          },
          "common:synonyms": [
            {"@xml:lang": "zh", "#text": "交流电力"},
            {"@xml:lang": "en", "#text": "alternating current electricity"}
          ],
          "common:generalComment": [
            {"@xml:lang": "zh", "#text": "用于测试的交流电流"},
            {"@xml:lang": "en", "#text": "Alternating current for search testing"}
          ],
          "common:other": {
            "ecn:ECNumber": "EC-123-456"
          },
          "classificationInformation": {
            "common:classification": {
              "common:class": [{"@classId": "flow-energy", "#text": "能源"}]
            }
          }
        }
      },
      "administrativeInformation": {
        "dataEntryBy": {
          "common:referenceToDataSetFormat": {
            "@uri": "../sources/97000000-0000-0000-0000-000000000088.xml",
            "@type": "source data set",
            "@version": "01.00.000",
            "@refObjectId": "97000000-0000-0000-0000-000000000088",
            "common:shortDescription": {"@xml:lang": "en", "#text": "ILCD format"}
          }
        },
        "publicationAndOwnership": {
          "common:permanentDataSetURI": "https://lcdn.tiangong.earth/flows/97000000-0000-0000-0000-000000000088?version=01.00.000",
          "common:dataSetVersion": "01.00.000"
        }
      }
    }
  }'::json,
  '96000000-0000-0000-0000-000000000088',
  100,
  '26000000-0000-0000-0000-000000000088',
  true
);

insert into public.processes (
  id,
  version,
  json_ordered,
  user_id,
  state_code,
  team_id,
  rule_verification
)
values (
  '98000000-0000-0000-0000-000000000088',
  '01.00.000',
  '{
    "processDataSet": {
      "@xsi:schemaLocation": "http://lca.jrc.it/ILCD/Process ../../schemas/ILCD_ProcessDataSet.xsd",
      "processInformation": {
        "dataSetInformation": {
          "common:UUID": "98000000-0000-0000-0000-000000000088",
          "name": {
            "baseName": [
              {"@xml:lang": "zh", "#text": "交流电过程"},
              {"@xml:lang": "en", "#text": "electricity process"},
              {"@xml:lang": "de", "#text": "Wechselstrom Prozess"}
            ]
          },
          "classificationInformation": {
            "common:classification": {
              "common:class": {"@classId": "process-energy", "#text": "能源过程"}
            }
          }
        },
        "geography": {
          "locationOfOperationSupplyOrProduction": {
            "descriptionOfRestrictions": [
              {"@xml:lang": "zh", "#text": "中国电网"},
              {"@xml:lang": "en", "#text": "China grid"}
            ]
          }
        },
        "technology": {
          "technologyDescriptionAndIncludedProcesses": [
            {"@xml:lang": "zh", "#text": "交流电生产技术"},
            {"@xml:lang": "en", "#text": "alternating current production technology"}
          ]
        },
        "quantitativeReference": {
          "referenceToReferenceFlow": {
            "@uri": "../flows/98000000-0000-0000-0000-000000000088.xml",
            "@refObjectId": "98000000-0000-0000-0000-000000000088",
            "common:shortDescription": [
              {"@xml:lang": "zh", "#text": "交流电产品"},
              {"@xml:lang": "en", "#text": "electricity product"}
            ]
          }
        }
      },
      "modellingAndValidation": {
        "LCIMethod": {
          "typeOfDataSet": "Unit process"
        }
      },
      "exchanges": {
        "exchange": {
          "referenceToFlowDataSet": {
            "@uri": "../flows/11111111-1111-1111-1111-111111111111.xml",
            "@refObjectId": "11111111-1111-1111-1111-111111111111",
            "common:shortDescription": {"@xml:lang": "en", "#text": "exchange side noise"}
          }
        }
      },
      "administrativeInformation": {
        "publicationAndOwnership": {
          "common:permanentDataSetURI": "https://lcdn.tiangong.earth/processes/98000000-0000-0000-0000-000000000088?version=01.00.000",
          "common:dataSetVersion": "01.00.000"
        }
      }
    }
  }'::json,
  '96000000-0000-0000-0000-000000000088',
  100,
  '26000000-0000-0000-0000-000000000088',
  true
);

insert into public.lifecyclemodels (
  id,
  version,
  json_ordered,
  user_id,
  state_code,
  team_id,
  rule_verification
)
values (
  '99000000-0000-0000-0000-000000000088',
  '01.00.000',
  '{
    "lifeCycleModelDataSet": {
      "@xsi:schemaLocation": "http://lca.jrc.it/ILCD/LCIAMethod ../../schemas/ILCD_LifeCycleModelDataSet.xsd",
      "lifeCycleModelInformation": {
        "dataSetInformation": {
          "common:UUID": "99000000-0000-0000-0000-000000000088",
          "name": {
            "baseName": [
              {"@xml:lang": "zh", "#text": "交流电模型"},
              {"@xml:lang": "en", "#text": "electricity model"},
              {"@xml:lang": "de", "#text": "Wechselstrom Modell"}
            ]
          },
          "classificationInformation": {
            "common:classification": {
              "common:class": {"@classId": "model-energy", "#text": "能源模型"}
            }
          }
        },
        "technology": {
          "processes": {
            "processInstance": {
              "referenceToProcess": {
                "@uri": "../processes/99000000-0000-0000-0000-000000000088.xml",
                "@refObjectId": "99000000-0000-0000-0000-000000000088",
                "common:shortDescription": [
                  {"@xml:lang": "zh", "#text": "交流电生产过程"},
                  {"@xml:lang": "en", "#text": "electricity production process"}
                ]
              }
            }
          }
        }
      },
      "administrativeInformation": {
        "publicationAndOwnership": {
          "common:permanentDataSetURI": "https://lcdn.tiangong.earth/models/99000000-0000-0000-0000-000000000088?version=01.00.000",
          "common:dataSetVersion": "01.00.000"
        }
      }
    }
  }'::json,
  '96000000-0000-0000-0000-000000000088',
  100,
  '26000000-0000-0000-0000-000000000088',
  true
);

select ok(
  (
    select extracted_text ~ '交流电'
       and extracted_text ~ 'electricity'
       and extracted_text ~ 'Wechselstrom'
       and extracted_text ~ '交流电力'
       and extracted_text ~ 'alternating current electricity'
       and extracted_text ~ 'EC-123-456'
       and extracted_text ~ '能源'
    from public.flows
    where id = '97000000-0000-0000-0000-000000000088'
  ),
  'flow extracted_text trigger preserves all authored languages and core fields'
);

select ok(
  (
    select extracted_text !~ '(flow-energy|97000000-0000-0000-0000-000000000088|ILCD format|schemas|https?://)'
       and strpos(extracted_text, '../') = 0
    from public.flows
    where id = '97000000-0000-0000-0000-000000000088'
  ),
  'flow extracted_text trigger excludes UUID, schema, URI, and reference metadata noise'
);

select ok(
  (
    select extracted_text ~ '交流电过程'
       and extracted_text ~ 'electricity process'
       and extracted_text ~ 'Wechselstrom Prozess'
       and extracted_text ~ '中国电网'
       and extracted_text ~ 'China grid'
       and extracted_text ~ '交流电产品'
       and extracted_text ~ 'electricity product'
    from public.processes
    where id = '98000000-0000-0000-0000-000000000088'
  ),
  'process extracted_text trigger preserves all authored languages and core fields'
);

select ok(
  (
    select extracted_text !~ '(process-energy|98000000-0000-0000-0000-000000000088|exchange side noise|schemas|https?://)'
       and strpos(extracted_text, '../') = 0
    from public.processes
    where id = '98000000-0000-0000-0000-000000000088'
  ),
  'process extracted_text trigger excludes reference metadata and exchange noise'
);

select ok(
  (
    select extracted_text ~ '交流电模型'
       and extracted_text ~ 'electricity model'
       and extracted_text ~ 'Wechselstrom Modell'
       and extracted_text ~ '交流电生产过程'
       and extracted_text ~ 'electricity production process'
    from public.lifecyclemodels
    where id = '99000000-0000-0000-0000-000000000088'
  ),
  'lifecyclemodel extracted_text trigger preserves all authored languages and process references'
);

select ok(
  (
    select extracted_text !~ '(model-energy|99000000-0000-0000-0000-000000000088|schemas|https?://)'
       and strpos(extracted_text, '../') = 0
    from public.lifecyclemodels
    where id = '99000000-0000-0000-0000-000000000088'
  ),
  'lifecyclemodel extracted_text trigger excludes reference metadata noise'
);

update public.flows
   set extracted_text = ''
 where id = '97000000-0000-0000-0000-000000000088';

do $$
declare
  v_payload jsonb;
  v_after_id uuid;
  v_after_version text;
begin
  for i in 1..20 loop
    v_payload := public.cmd_dataset_extracted_text_backfill(
      'flows',
      5000,
      v_after_id,
      v_after_version,
      'empty'
    );
    v_after_id := (v_payload->>'last_id')::uuid;
    v_after_version := v_payload->>'last_version';

    exit when coalesce((v_payload->>'has_more')::boolean, false) is false
      or exists (
        select 1
        from public.flows
        where id = '97000000-0000-0000-0000-000000000088'
          and extracted_text ~ '交流电'
          and extracted_text ~ 'electricity'
          and extracted_text ~ 'Wechselstrom'
      );
  end loop;
end
$$;

select ok(
  (select extracted_text ~ '交流电'
      and extracted_text ~ 'electricity'
      and extracted_text ~ 'Wechselstrom'
   from public.flows
   where id = '97000000-0000-0000-0000-000000000088'),
  'dataset extracted_text empty-mode backfill repairs missing rows in bounded batches'
);

update public.flows
   set extracted_text = 'stale extracted text'
 where id = '97000000-0000-0000-0000-000000000088';

do $$
declare
  v_payload jsonb;
  v_after_id uuid;
  v_after_version text;
begin
  for i in 1..20 loop
    v_payload := public.cmd_dataset_extracted_text_backfill(
      'flows',
      5000,
      v_after_id,
      v_after_version,
      'stale'
    );
    v_after_id := (v_payload->>'last_id')::uuid;
    v_after_version := v_payload->>'last_version';

    exit when coalesce((v_payload->>'has_more')::boolean, false) is false
      or exists (
        select 1
        from public.flows
        where id = '97000000-0000-0000-0000-000000000088'
          and extracted_text ~ '交流电'
          and extracted_text ~ 'electricity'
          and extracted_text ~ 'Wechselstrom'
      );
  end loop;
end
$$;

select ok(
  (select extracted_text ~ '交流电'
      and extracted_text ~ 'electricity'
      and extracted_text ~ 'Wechselstrom'
   from public.flows
   where id = '97000000-0000-0000-0000-000000000088'),
  'dataset extracted_text stale-mode backfill repairs non-empty stale rows'
);

select ok(
  (select extracted_text !~ '(flow-energy|97000000-0000-0000-0000-000000000088|ILCD format|schemas|https?://)'
      and strpos(extracted_text, '../') = 0
   from public.flows
   where id = '97000000-0000-0000-0000-000000000088'),
  'dataset extracted_text stale-mode backfill keeps repaired rows free of metadata noise'
);

update public.flows
   set extracted_text = '../sources/97000000-0000-0000-0000-000000000088.xml ILCD format'
 where id = '97000000-0000-0000-0000-000000000088';

do $$
declare
  v_payload jsonb;
  v_after_id uuid;
  v_after_version text;
begin
  for i in 1..20 loop
    v_payload := public.cmd_dataset_extracted_text_backfill(
      'flows',
      5000,
      v_after_id,
      v_after_version,
      'noisy'
    );
    v_after_id := (v_payload->>'last_id')::uuid;
    v_after_version := v_payload->>'last_version';

    exit when coalesce((v_payload->>'has_more')::boolean, false) is false
      or exists (
        select 1
        from public.flows
        where id = '97000000-0000-0000-0000-000000000088'
          and extracted_text ~ '交流电'
          and extracted_text !~ 'ILCD format'
          and strpos(extracted_text, '../') = 0
      );
  end loop;
end
$$;

select ok(
  (select extracted_text ~ '交流电'
      and extracted_text !~ 'ILCD format'
      and strpos(extracted_text, '../') = 0
   from public.flows
   where id = '97000000-0000-0000-0000-000000000088'),
  'dataset extracted_text noisy-mode backfill repairs metadata-polluted rows'
);

set local role authenticated;
select set_config('request.jwt.claim.sub', '96000000-0000-0000-0000-000000000088', true);

select is(
  (select id::text from public.search_flows_latest('交流电', '{}'::jsonb, '{}'::jsonb, 10, 1, 'tg', '96000000-0000-0000-0000-000000000088') where id = '97000000-0000-0000-0000-000000000088'),
  '97000000-0000-0000-0000-000000000088',
  'flow latest search recalls Chinese authored text'
);

select is(
  (select id::text from public.search_flows_latest('Wechselstrom', '{}'::jsonb, '{}'::jsonb, 10, 1, 'tg', '96000000-0000-0000-0000-000000000088') where id = '97000000-0000-0000-0000-000000000088'),
  '97000000-0000-0000-0000-000000000088',
  'flow latest search recalls non-English authored text'
);

select is(
  (select id::text from public.search_flows_latest('electricity', '{}'::jsonb, '{}'::jsonb, 10, 1, 'tg', '96000000-0000-0000-0000-000000000088') where id = '97000000-0000-0000-0000-000000000088'),
  '97000000-0000-0000-0000-000000000088',
  'flow latest search recalls English authored text'
);

select is(
  (select id::text from public.search_processes_latest('交流电过程', '{}'::jsonb, '{}'::jsonb, 10, 1, 'tg', '96000000-0000-0000-0000-000000000088', null, null, 'all') where id = '98000000-0000-0000-0000-000000000088'),
  '98000000-0000-0000-0000-000000000088',
  'process latest search recalls Chinese authored text'
);

select is(
  (select id::text from public.search_processes_latest('Wechselstrom', '{}'::jsonb, '{}'::jsonb, 10, 1, 'tg', '96000000-0000-0000-0000-000000000088', null, null, 'all') where id = '98000000-0000-0000-0000-000000000088'),
  '98000000-0000-0000-0000-000000000088',
  'process latest search recalls non-English authored text'
);

select is(
  (select id::text from public.search_processes_latest('electricity', '{}'::jsonb, '{}'::jsonb, 10, 1, 'tg', '96000000-0000-0000-0000-000000000088', null, null, 'all') where id = '98000000-0000-0000-0000-000000000088'),
  '98000000-0000-0000-0000-000000000088',
  'process latest search recalls English authored text'
);

select is(
  (select id::text from public.search_lifecyclemodels_latest('交流电模型', '{}'::jsonb, '{}'::jsonb, 10, 1, 'tg', '96000000-0000-0000-0000-000000000088') where id = '99000000-0000-0000-0000-000000000088'),
  '99000000-0000-0000-0000-000000000088',
  'lifecyclemodel latest search recalls Chinese authored text'
);

select is(
  (select id::text from public.search_lifecyclemodels_latest('Wechselstrom', '{}'::jsonb, '{}'::jsonb, 10, 1, 'tg', '96000000-0000-0000-0000-000000000088') where id = '99000000-0000-0000-0000-000000000088'),
  '99000000-0000-0000-0000-000000000088',
  'lifecyclemodel latest search recalls non-English authored text'
);

select is(
  (select id::text from public.search_lifecyclemodels_latest('electricity', '{}'::jsonb, '{}'::jsonb, 10, 1, 'tg', '96000000-0000-0000-0000-000000000088') where id = '99000000-0000-0000-0000-000000000088'),
  '99000000-0000-0000-0000-000000000088',
  'lifecyclemodel latest search recalls English authored text'
);

select * from finish();

rollback;
