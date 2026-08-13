begin;

create extension if not exists pgtap with schema extensions;
set local search_path = extensions, public, private, auth;

select plan(20);

select is(
  (
    select count(*)::integer
    from information_schema.columns
    where table_schema = 'public'
      and table_name in (
        'contacts', 'flowproperties', 'flows', 'lifecyclemodels',
        'processes', 'sources', 'unitgroups'
      )
      and column_name = 'search_text'
      and data_type = 'ARRAY'
      and udt_name = '_text'
      and is_nullable = 'YES'
      and column_default is null
  ),
  7,
  'all seven search_text columns are nullable text[] values with no default'
);

select extensions.ok(
  pg_get_functiondef(
    'private.review_dataset_content_guard_v1()'::regprocedure
  ) like '%array[''extracted_md'', ''search_text'', ''embedding_ft'', ''embedding_ft_at'']%'
  and pg_get_functiondef(
    'private.review_dataset_content_guard_v1()'::regprocedure
  ) not like '%array[''state_code'', ''review_id'', ''reviews'', ''modified_at'']%',
  'review guard allowlists exactly the four derived projection columns'
);

select is(
  (
    select count(*)::integer
    from pg_trigger
    where tgrelid in (
      'public.contacts'::regclass,
      'public.flowproperties'::regclass,
      'public.flows'::regclass,
      'public.lifecyclemodels'::regclass,
      'public.processes'::regclass,
      'public.sources'::regclass,
      'public.unitgroups'::regclass
    )
      and tgname = 'review_dataset_content_guard_v1'
      and not tgisinternal
  ),
  7,
  'all seven dataset tables retain the review content guard trigger'
);

select is(
  (
    select count(*)::integer
    from unnest(array[
      'contacts', 'flowproperties', 'flows', 'lifecyclemodels',
      'processes', 'sources', 'unitgroups'
    ]::text[]) as table_name
    where not has_table_privilege(
      'anon', format('public.%I', table_name), 'UPDATE'
    )
  ),
  7,
  'anon has no direct UPDATE privilege on any dataset table'
);

select is(
  (
    select count(*)::integer
    from unnest(array[
      'contacts', 'flowproperties', 'flows', 'lifecyclemodels',
      'processes', 'sources', 'unitgroups'
    ]::text[]) as table_name
    where not has_table_privilege(
      'authenticated', format('public.%I', table_name), 'UPDATE'
    )
  ),
  7,
  'authenticated has no direct UPDATE privilege on any dataset table'
);

-- Disable only asynchronous projection side effects for deterministic trigger
-- assertions.  The review guard and modified_at trigger stay enabled.
alter table public.contacts disable trigger "contacts_json_sync_trigger";
alter table public.contacts disable trigger "contact_dataset_extraction_trigger_insert";
alter table public.contacts disable trigger "contact_dataset_extraction_trigger_update";
alter table public.contacts disable trigger "contact_embedding_ft_on_extract_md_update";

insert into public.contacts (
  id, version, json, json_ordered, user_id, state_code, rule_verification
)
values
  (
    '45910000-0000-4000-8000-000000000001', '01.00.000',
    '{"name":"draft"}'::jsonb, '{"name":"draft"}'::json,
    '45910000-0000-4000-8000-000000000010', 0, true
  ),
  (
    '45910000-0000-4000-8000-000000000002', '01.00.000',
    '{"name":"under-review"}'::jsonb, '{"name":"under-review"}'::json,
    '45910000-0000-4000-8000-000000000010', 20, true
  ),
  (
    '45910000-0000-4000-8000-000000000003', '01.00.000',
    '{"name":"approved"}'::jsonb, '{"name":"approved"}'::json,
    '45910000-0000-4000-8000-000000000010', 100, true
  );

create temporary table issue_459_guard_baseline as
select id, modified_at
from public.contacts
where id in (
  '45910000-0000-4000-8000-000000000002'::uuid,
  '45910000-0000-4000-8000-000000000003'::uuid
);

select extensions.lives_ok(
  $test$
    update public.contacts
    set extracted_md = '# draft derivative',
        search_text = array['draft', 'derivative']::text[],
        embedding_ft = array_fill(0::real, array[1024])::extensions.vector(1024),
        embedding_ft_at = clock_timestamp()
    where id = '45910000-0000-4000-8000-000000000001'::uuid
  $test$,
  'draft permits the four-field derived projection update'
);

select extensions.lives_ok(
  $test$
    update public.contacts
    set json = '{"name":"draft-authored-change"}'::jsonb
    where id = '45910000-0000-4000-8000-000000000001'::uuid
  $test$,
  'draft still permits the existing authored update path'
);

select extensions.lives_ok(
  $test$
    update public.contacts
    set extracted_md = '# under-review derivative',
        search_text = array['under-review', 'derivative']::text[],
        embedding_ft = array_fill(0::real, array[1024])::extensions.vector(1024),
        embedding_ft_at = clock_timestamp()
    where id = '45910000-0000-4000-8000-000000000002'::uuid
  $test$,
  'under-review permits only the four-field derived projection update'
);

select extensions.is(
  (
    select contacts.modified_at
    from public.contacts
    where contacts.id = '45910000-0000-4000-8000-000000000002'::uuid
  ),
  (select baseline.modified_at
   from issue_459_guard_baseline as baseline
   where baseline.id = '45910000-0000-4000-8000-000000000002'::uuid),
  'under-review derived update leaves modified_at unchanged'
);

select extensions.lives_ok(
  $test$
    update public.contacts
    set extracted_md = '# approved derivative',
        search_text = array['approved', 'derivative']::text[],
        embedding_ft = array_fill(0::real, array[1024])::extensions.vector(1024),
        embedding_ft_at = clock_timestamp()
    where id = '45910000-0000-4000-8000-000000000003'::uuid
  $test$,
  'approved permits only the four-field derived projection update'
);

select extensions.is(
  (
    select contacts.modified_at
    from public.contacts
    where contacts.id = '45910000-0000-4000-8000-000000000003'::uuid
  ),
  (select baseline.modified_at
   from issue_459_guard_baseline as baseline
   where baseline.id = '45910000-0000-4000-8000-000000000003'::uuid),
  'approved derived update leaves modified_at unchanged'
);

select extensions.throws_ok(
  $test$
    update public.contacts
    set json = '{"name":"under-review-authored-change"}'::jsonb
    where id = '45910000-0000-4000-8000-000000000002'::uuid
  $test$,
  '55000',
  'DATASET_UNDER_REVIEW_IMMUTABLE',
  'under-review authored content remains immutable'
);

select extensions.throws_ok(
  $test$
    update public.contacts
    set modified_at = modified_at + interval '1 second'
    where id = '45910000-0000-4000-8000-000000000002'::uuid
  $test$,
  '55000',
  'DATASET_UNDER_REVIEW_IMMUTABLE',
  'under-review modified_at cannot be changed through the derivative path'
);

select extensions.throws_ok(
  $test$
    update public.contacts
    set rule_verification = false
    where id = '45910000-0000-4000-8000-000000000003'::uuid
  $test$,
  '55000',
  'APPROVED_DATASET_IMMUTABLE',
  'approved authored content remains immutable'
);

select extensions.throws_ok(
  $test$
    update public.contacts
    set modified_at = modified_at + interval '1 second'
    where id = '45910000-0000-4000-8000-000000000003'::uuid
  $test$,
  '55000',
  'APPROVED_DATASET_IMMUTABLE',
  'approved modified_at cannot be changed through the derivative path'
);

select extensions.throws_ok(
  $test$
    update public.contacts
    set search_text = array['under-review', 'modified']::text[],
        modified_at = modified_at + interval '1 second'
    where id = '45910000-0000-4000-8000-000000000002'::uuid
  $test$,
  '55000',
  'DATASET_UNDER_REVIEW_IMMUTABLE',
  'under-review search_text plus modified_at is rejected by the trigger'
);

select extensions.throws_ok(
  $test$
    update public.contacts
    set extracted_md = '# under-review modified',
        modified_at = modified_at + interval '1 second'
    where id = '45910000-0000-4000-8000-000000000002'::uuid
  $test$,
  '55000',
  'DATASET_UNDER_REVIEW_IMMUTABLE',
  'under-review extracted_md plus modified_at is rejected by the trigger'
);

select extensions.throws_ok(
  $test$
    update public.contacts
    set embedding_ft = array_fill(0::real, array[1024])::extensions.vector(1024),
        modified_at = modified_at + interval '1 second'
    where id = '45910000-0000-4000-8000-000000000003'::uuid
  $test$,
  '55000',
  'APPROVED_DATASET_IMMUTABLE',
  'approved embedding_ft plus modified_at is rejected by the trigger'
);

select extensions.throws_ok(
  $test$
    update public.contacts
    set embedding_ft_at = clock_timestamp(),
        modified_at = modified_at + interval '1 second'
    where id = '45910000-0000-4000-8000-000000000003'::uuid
  $test$,
  '55000',
  'APPROVED_DATASET_IMMUTABLE',
  'approved embedding_ft_at plus modified_at is rejected by the trigger'
);

select set_config('app.review_controlled_write', 'on', true);

select extensions.lives_ok(
  $test$
    update public.contacts
    set json = '{"name":"review-controlled-change"}'::jsonb
    where id = '45910000-0000-4000-8000-000000000003'::uuid
  $test$,
  'review-controlled command context retains its authored mutation semantics'
);

select set_config('app.review_controlled_write', 'off', true);

alter table public.contacts enable trigger "contacts_json_sync_trigger";
alter table public.contacts enable trigger "contact_dataset_extraction_trigger_insert";
alter table public.contacts enable trigger "contact_dataset_extraction_trigger_update";
alter table public.contacts enable trigger "contact_embedding_ft_on_extract_md_update";

select * from extensions.finish();

rollback;
