#!/usr/bin/env python3
"""Measure the 126246-row synthetic composite-name rollout on isolated #628 only."""
from __future__ import annotations
import argparse
import json
from pathlib import Path
import re
import subprocess
import time

ROOT = Path(__file__).resolve().parents[1]
parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--local-container', required=True)
args = parser.parse_args()
if not re.fullmatch(r'supabase_db_database-engine-628(?:-[a-z0-9]+)?', args.local_container):
    parser.error('only an explicitly isolated Database #628 container is supported')
CMD = ['docker','exec','-i',args.local_container,'psql','-X','-qAt','-U','postgres','-d','postgres','-v','ON_ERROR_STOP=1']
OWNER = '62830000-0000-4000-8000-000000000001'

def sql(s):
    result = subprocess.run(CMD, input=s, text=True, capture_output=True)
    if result.returncode:
        raise RuntimeError('Isolated benchmark SQL failed: ' + result.stderr[-2000:])
    return result.stdout.strip()

if sql('select count(*) from public.processes') != '0' or sql('select count(*) from public.flows') != '0':
    raise RuntimeError('benchmark requires an empty isolated stack; refuses to mix with existing source records')
legacy = sql("select coalesce(to_regclass('private.portal_catalog_search_rows_v2')::text,'')") == ''
statuses=json.loads(sql("select json_agg(json_build_object('table',tgrelid::regclass::text,'name',tgname,'enabled',tgenabled)) from pg_trigger where tgrelid in ('public.processes'::regclass,'public.flows'::regclass) and not tgisinternal"))
try:
    for table in ['processes','flows']:
        sql(f'alter table public.{table} disable trigger user; alter table public.{table} enable trigger portal_catalog_projection_content_sync_v1;')
    # Seed legacy-only sources with synthetic, bounded multilingual source documents.
    for kind,table,root,info,count in [('process','processes','processDataSet','processInformation',17299),('flow','flows','flowDataSet','flowInformation',108947)]:
        t=time.monotonic()
        for lo in range(1,count+1,2000):
            hi=min(count,lo+1999)
            sql(f"""insert into public.{table}(id,version,user_id,state_code,modified_at,json)
select md5('628-volume-{kind}:'||i)::uuid,'01.00.000','{OWNER}',100,'2026-09-08T00:00:00Z',
jsonb_build_object('{root}',jsonb_build_object('{info}',jsonb_build_object('dataSetInformation',jsonb_build_object(
'name',jsonb_build_object(
'baseName',jsonb_build_array(jsonb_build_object('@xml:lang','en','#text','VolumeName628 '||i),jsonb_build_object('@xml:lang','zh','#text','规模过程 '||i)),
'treatmentStandardsRoutes',jsonb_build_object('@xml:lang','en','#text','Route'||i||'needle628'),
'mixAndLocationTypes',jsonb_build_object('@xml:lang','en','#text','consumer mix; at user'),
'functionalUnitFlowProperties',jsonb_build_object('@xml:lang','en','#text','medium voltage 1–35 kV')),
'common:generalComment',jsonb_build_object('@xml:lang','en','#text',repeat('Synthetic fixture context. ',20))))))
from generate_series({lo},{hi}) i;""")
        print(json.dumps({'seed':kind,'rows':count,'seconds':round(time.monotonic()-t,2)}),flush=True)
    if not legacy:
        sql('alter table public.processes enable trigger portal_catalog_projection_content_sync_v2;')
    times=[]
    backfill_times=[]
    pattern='20260908*.sql' if legacy else '202609080901*.sql'
    for p in sorted((ROOT/'supabase/migrations').glob(pattern)):
        t=time.monotonic();sql(p.read_text());elapsed=time.monotonic()-t;times.append(elapsed)
        if 'backfill' in p.name: backfill_times.append(elapsed)
        print(json.dumps({'shard':p.name,'seconds':round(elapsed,3)}),flush=True)
        assert elapsed<60, 'backfill lacks 2x headroom under 120s statement budget'
    assert sql('select count(*) from private.portal_catalog_search_rows_v2')=='17299'
    assert sql('select count(*) from private.portal_catalog_character_rows_v2')=='17299'
    sql('analyze private.portal_catalog_search_rows_v2; analyze private.portal_catalog_character_rows_v2;')
    # Exact equality for Flow and non-name Process fields; timestamps are source-owned.
    assert sql("select count(*) from private.portal_catalog_search_rows_v1 o join private.portal_catalog_search_current_v2 n using(dataset_kind,id,version) where o.modified_at<>n.modified_at or (o.dataset_kind='flow' and o.card<>n.card) or (o.dataset_kind='process' and o.card-'names'-'document'<>n.card-'names'-'document')")=='0'
    t=time.monotonic();sql((ROOT/'supabase/migrations/20260908090300_portal_composite_names_cutover.sql').read_text());cutover=time.monotonic()-t
    samples=[]
    for _ in range(20):
        t=time.monotonic();sql("set role anon; select jsonb_array_length(api.portal_search_processes_v2('Route17000needle628')->'items');");samples.append(time.monotonic()-t)
    assert sql("select count(*) from private.portal_catalog_search_rows_v2 where dataset_kind='flow'")=='0'
    assert sql("select count(*) from pg_trigger where tgrelid='public.flows'::regclass and tgname='portal_catalog_projection_content_sync_v2'")=='0'
    # Natural selective plans must prune the other storage generation and retain indexes.
    def walk(plan):
        yield plan
        for child in plan.get('Plans',[]): yield from walk(child)
    plan_evidence=[]
    for kind,needle,table,index in [('process','Route17000needle628','portal_catalog_search_rows_v2','portal_catalog_search_process_document_v2_pgroonga'),('flow','VolumeName628 17000','portal_catalog_search_rows_v1','portal_catalog_search_flow_document_v1_pgroonga')]:
        plan=json.loads(sql(f"begin; grant portal_public_executor to postgres; set local role portal_public_executor; explain (analyze,buffers,format json) select id,version from private.portal_catalog_search_current_v2 where dataset_kind='{kind}' and document operator(extensions.&@) '{needle}' limit 20; rollback;"))[0]
        nodes=list(walk(plan['Plan']))
        assert index in [n.get('Index Name') for n in nodes], f'{kind}: selective lexical index missing'
        relations={n['Relation Name'] for n in nodes if 'Relation Name' in n}
        assert relations=={table}, f'{kind}: union route did not prune the other generation: {relations}'
        plan_evidence.append({'kind':kind,'index':index,'executionMs':plan['Execution Time'],'relations':sorted(relations)})
    print(json.dumps({'naturalRoutingPlans':plan_evidence,'canonicalBaseUpgrade':legacy}),flush=True)
    sizes=sql("select json_object_agg(relname,pg_total_relation_size(oid)) from pg_class where relnamespace='private'::regnamespace and relname in ('portal_catalog_search_rows_v1','portal_catalog_search_rows_v2','portal_catalog_character_rows_v1','portal_catalog_character_rows_v2')")
    print(json.dumps({'evidence':'isolated-synthetic','rows':126246,'maxShardSeconds':max(backfill_times),'maxMigrationSeconds':max(times),'cutoverSeconds':cutover,'searchRoundtripP95Seconds':sorted(samples)[18],'relationBytes':json.loads(sizes),'flowAndUnrelatedFields':'byte-equal'}),flush=True)
finally:
    for table in ['processes','flows']:
        sql(f'delete from public.{table} where user_id=\'{OWNER}\';')
    for s in statuses:
        action={'O':'enable','D':'disable','R':'enable replica','A':'enable always'}[s['enabled']]
        name='"'+s['name'].replace('"','""')+'"'
        sql(f"alter table {s['table']} {action} trigger {name}")
