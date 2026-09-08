#!/usr/bin/env python3
"""Exercise #628 shard replay/concurrency on an explicitly isolated local stack."""
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
    parser.error('requires an isolated Database #628 container; shared/hosted targets are forbidden')
CMD = ['docker', 'exec', '-i', args.local_container, 'psql', '-X', '-qAt', '-U', 'postgres', '-d', 'postgres', '-v', 'ON_ERROR_STOP=1']

def sql(statement: str) -> str:
    result = subprocess.run(CMD, input=statement, text=True, capture_output=True, check=True)
    return result.stdout.strip()

# IDs and payloads are synthetic. All fixture source changes are cleaned up.
IDS = ['62820000-0000-4000-8000-' + f'{i:012d}' for i in range(1, 5)]
shard = (ROOT / 'supabase/migrations/20260908090101_portal_composite_names_backfill_1.sql').read_text()

def writer(statement: str) -> subprocess.Popen:
    p = subprocess.Popen(CMD, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    p.stdin.write("begin; set local application_name='portal-names-628-race'; " + statement + "; select pg_sleep(3); commit;")
    p.stdin.close()
    # Wait for the writer to hold its source lock instead of guessing startup time.
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        if sql("select count(*) from pg_stat_activity where application_name='portal-names-628-race' and wait_event='PgSleep'") == '1':
            return p
        time.sleep(0.05)
    raise RuntimeError('writer did not reach the bounded lock-holding phase')

statuses = sql("select json_agg(json_build_object('table',tgrelid::regclass::text,'name',tgname,'enabled',tgenabled)) from pg_trigger where tgrelid='public.processes'::regclass and not tgisinternal")
try:
    sql('begin; alter table public.processes disable trigger user; alter table public.processes enable trigger portal_catalog_projection_content_sync_v1; alter table public.processes enable trigger portal_catalog_projection_content_sync_v2; commit;')
    parts = {'baseName': 'RaceBase628', 'treatmentStandardsRoutes': 'Before628'}
    payload = {'processDataSet': {'processInformation': {'dataSetInformation': {'name': parts}}}}
    for identity in IDS:
        sql(f"insert into public.processes(id,version,json,state_code,modified_at) values ('{identity}','01.00.000',$json${json.dumps(payload)}$json$,100,'2026-09-08T00:00:00Z')")
    # Old API semantics and new API semantics coexist without changing v1 bytes.
    assert sql(f"select card#>>'{{names,0,value}}' from private.portal_catalog_search_rows_v1 where id='{IDS[0]}'") == 'RaceBase628'
    assert sql(f"select card#>>'{{names,0,value}}' from private.portal_catalog_search_rows_v2 where id='{IDS[0]}'") == 'RaceBase628; Before628'
    races = [
        ('update', f"update public.processes set json=jsonb_set(json,'{{processDataSet,processInformation,dataSetInformation,name,treatmentStandardsRoutes}}','\"After628\"') where id='{IDS[0]}'"),
        ('withdraw', f"update public.processes set state_code=20 where id='{IDS[1]}'"),
        ('delete', f"delete from public.processes where id='{IDS[2]}'"),
        ('key-change', f"update public.processes set version='01.00.001' where id='{IDS[3]}'"),
    ]
    for label, statement in races:
        p = writer(statement)
        started = time.monotonic()
        sql(shard)
        assert p.wait(timeout=10) == 0, p.stderr.read()
        print(json.dumps({'race': label, 'shardSeconds': round(time.monotonic() - started, 3)}), flush=True)
    assert sql(f"select card#>>'{{names,0,value}}' from private.portal_catalog_search_rows_v2 where id='{IDS[0]}'") == 'RaceBase628; After628'
    assert sql(f"select count(*) from private.portal_catalog_search_rows_v2 where id in ('{IDS[1]}','{IDS[2]}')") == '0'
    assert sql(f"select version from private.portal_catalog_search_rows_v2 where id='{IDS[3]}'") == '01.00.001'
    assert sql(f"select modified_at='2026-09-08T00:00:00Z'::timestamptz from public.processes where id='{IDS[0]}'") == 't'
    before = sql("select md5(coalesce(string_agg(row(dataset_kind,id,version,state_code,modified_at,card,document)::text,'' order by dataset_kind,id,version),'')) from private.portal_catalog_search_rows_v2")
    sql(shard)
    after = sql("select md5(coalesce(string_agg(row(dataset_kind,id,version,state_code,modified_at,card,document)::text,'' order by dataset_kind,id,version),'')) from private.portal_catalog_search_rows_v2")
    assert before == after, 'shard replay changed the existing projection'
    print(json.dumps({'replay': 'byte-identical', 'sourceModifiedAt': 'unchanged', 'withdrawal': 'absent', 'keyChange': 'exact'}))
finally:
    sql("delete from public.processes where id in (" + ','.join("'" + i + "'" for i in IDS) + ')')
    for status in json.loads(statuses):
        action = {'O': 'enable', 'D': 'disable', 'R': 'enable replica', 'A': 'enable always'}[status['enabled']]
        # Catalog identifiers are quoted, never interpreted as SQL source.
        name = '"' + status['name'].replace('"', '""') + '"'
        sql(f"alter table public.processes {action} trigger {name}")
