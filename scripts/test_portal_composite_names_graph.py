#!/usr/bin/env python3
"""Prove the composite read graph changes only storage routing and cursor epoch."""
import argparse
import json
import re
import subprocess

parser=argparse.ArgumentParser(description=__doc__)
parser.add_argument('--local-container',required=True)
a=parser.parse_args()
if not re.fullmatch(r'supabase_db_database-engine(?:-[a-z0-9]+)?',a.local_container):
    parser.error('requires a local Database container')
q="""select json_agg(json_build_object('name',p.proname,'body',p.prosrc,'owner',p.proowner,'config',p.proconfig,'volatility',p.provolatile,'parallel',p.proparallel,'definer',p.prosecdef)) from pg_proc p where pronamespace='private'::regnamespace and prokind='f'"""
rows=json.loads(subprocess.check_output(['docker','exec',a.local_container,'psql','-X','-qAt','-U','postgres','-d','postgres','-c',q],text=True))
functions={r['name']:r for r in rows}
skipped={'portal_catalog_card_cn1','portal_dataset_metadata_cn1'}
count=0
for name,new in functions.items():
    if not re.search(r'_cn[12](?:_|$)',name) or name in skipped or name.startswith('assert_') or '_manifest_' in name:
        continue
    old=functions[re.sub(r'_cn(?=[12](?:_|$))','_v',name)]
    body=re.sub(r'_cn(?=[12](?:_|\b))','_v',new['body'])
    body=body.replace('portal_catalog_search_rows_v2','portal_catalog_search_rows_v1').replace('portal_catalog_character_rows_v2','portal_catalog_character_rows_v1')
    body=re.sub(r"^  v_fingerprint := pg_catalog.encode\(extensions.digest\(pg_catalog.convert_to\('composite-names-v2:'.*\n",'',body,flags=re.M)
    assert body==old['body'], f'{name}: query body changed beyond routing/epoch'
    for key in ['owner','config','volatility','parallel','definer']:
        assert new[key]==old[key], f'{name}: {key} changed'
    count+=1
assert count==33, f'unexpected comparison count: {count}'
print(f'PASS: {count} private routines preserve complete query bodies, executor, timeout/planner configuration and volatility after routing/epoch normalization')
