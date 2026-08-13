-- Database A / workspace#565: make the seven search projections arrays and
-- close the reviewed-row derivative boundary.
--
-- This is a forward-only follow-up to 20260810200000. Production verification
-- established that all seven additive scalar columns are empty. Replacing an
-- empty nullable column avoids the heap rewrite required by ALTER COLUMN TYPE;
-- the migration fails closed if the catalog shape, dependencies, or data have
-- drifted. The later service/Edge backfill writes the normalized projection.

set lock_timeout = '5s';
set statement_timeout = '120s';

do $replace_empty_search_text_columns$
declare
  target_table name;
  target_relation regclass;
  search_attnum smallint;
  search_type regtype;
  search_not_null boolean;
  search_has_default boolean;
  search_identity "char";
  search_generated "char";
  search_acl aclitem[];
  search_options text[];
  search_fdw_options text[];
  dependent_objects bigint;
  populated_rows bigint;
begin
  -- Validate every target before taking locks or changing any catalog entry.
  foreach target_table in array array[
    'contacts', 'flowproperties', 'flows', 'lifecyclemodels',
    'processes', 'sources', 'unitgroups'
  ]::name[]
  loop
    target_relation := pg_catalog.to_regclass(
      pg_catalog.format('public.%I', target_table)
    );

    if target_relation is null then
      raise exception using
        errcode = '55000',
        message = pg_catalog.format(
          'search_text conversion target public.%I is absent', target_table
        );
    end if;

    select
      attribute.attnum,
      attribute.atttypid::regtype,
      attribute.attnotnull,
      attribute.atthasdef,
      attribute.attidentity,
      attribute.attgenerated,
      attribute.attacl,
      attribute.attoptions,
      attribute.attfdwoptions
    into
      search_attnum,
      search_type,
      search_not_null,
      search_has_default,
      search_identity,
      search_generated,
      search_acl,
      search_options,
      search_fdw_options
    from pg_catalog.pg_attribute as attribute
    where attribute.attrelid = target_relation
      and attribute.attname = 'search_text'
      and attribute.attnum > 0
      and not attribute.attisdropped;

    if not found then
      raise exception using
        errcode = '55000',
        message = pg_catalog.format(
          'search_text conversion target public.%I.search_text is absent',
          target_table
        );
    end if;

    if search_type = 'text[]'::regtype then
      continue;
    end if;

    if search_type <> 'text'::regtype
       or search_not_null
       or search_has_default
       or search_identity <> ''
       or search_generated <> ''
       or search_acl is not null
       or search_options is not null
       or search_fdw_options is not null then
      raise exception using
        errcode = '55000',
        message = pg_catalog.format(
          'public.%I.search_text catalog contract drifted', target_table
        ),
        detail = pg_catalog.format(
          'type=%s not_null=%s has_default=%s identity=%s generated=%s column_acl=%s options=%s fdw_options=%s',
          search_type,
          search_not_null,
          search_has_default,
          search_identity,
          search_generated,
          search_acl,
          search_options,
          search_fdw_options
        );
    end if;

    select pg_catalog.count(*)
    into dependent_objects
    from pg_catalog.pg_depend as dependency
    where dependency.refclassid = 'pg_catalog.pg_class'::regclass
      and dependency.refobjid = target_relation
      and dependency.refobjsubid = search_attnum;

    if dependent_objects <> 0 then
      raise exception using
        errcode = '2BP01',
        message = pg_catalog.format(
          'public.%I.search_text has %s dependent objects',
          target_table,
          dependent_objects
        );
    end if;
  end loop;

  -- Lock and recheck all data before changing the first column. This closes
  -- the race with a concurrent derivative writer; lock_timeout keeps the
  -- migration fail-fast when production traffic cannot yield the tables.
  foreach target_table in array array[
    'contacts', 'flowproperties', 'flows', 'lifecyclemodels',
    'processes', 'sources', 'unitgroups'
  ]::name[]
  loop
    target_relation := pg_catalog.to_regclass(
      pg_catalog.format('public.%I', target_table)
    );

    select attribute.atttypid::regtype
    into search_type
    from pg_catalog.pg_attribute as attribute
    where attribute.attrelid = target_relation
      and attribute.attname = 'search_text'
      and attribute.attnum > 0
      and not attribute.attisdropped;

    if search_type = 'text[]'::regtype then
      continue;
    end if;

    execute pg_catalog.format(
      'lock table public.%I in access exclusive mode', target_table
    );
    execute pg_catalog.format(
      'select pg_catalog.count(search_text) from public.%I', target_table
    ) into populated_rows;

    if populated_rows <> 0 then
      raise exception using
        errcode = '55000',
        message = pg_catalog.format(
          'public.%I.search_text contains %s non-NULL values; refusing metadata-only replacement',
          target_table,
          populated_rows
        );
    end if;
  end loop;

  foreach target_table in array array[
    'contacts', 'flowproperties', 'flows', 'lifecyclemodels',
    'processes', 'sources', 'unitgroups'
  ]::name[]
  loop
    target_relation := pg_catalog.to_regclass(
      pg_catalog.format('public.%I', target_table)
    );

    select attribute.atttypid::regtype
    into search_type
    from pg_catalog.pg_attribute as attribute
    where attribute.attrelid = target_relation
      and attribute.attname = 'search_text'
      and attribute.attnum > 0
      and not attribute.attisdropped;

    if search_type = 'text[]'::regtype then
      continue;
    end if;

    execute pg_catalog.format(
      'alter table public.%I drop column search_text, add column search_text text[]',
      target_table
    );
  end loop;
end
$replace_empty_search_text_columns$;

comment on column public.contacts.search_text is
  'Edge-owned multilingual lexical projection as nullable text[]. The empty scalar precursor was replaced without a heap rewrite; the later backfill writes the complete projection. Not a lexical search source until Database B.';
comment on column public.flowproperties.search_text is
  'Edge-owned multilingual lexical projection as nullable text[]. The empty scalar precursor was replaced without a heap rewrite; the later backfill writes the complete projection. Not a lexical search source until Database B.';
comment on column public.flows.search_text is
  'Edge-owned multilingual lexical projection as nullable text[]. The empty scalar precursor was replaced without a heap rewrite; the later backfill writes the complete projection. Not a lexical search source until Database B.';
comment on column public.lifecyclemodels.search_text is
  'Edge-owned multilingual lexical projection as nullable text[]. The empty scalar precursor was replaced without a heap rewrite; the later backfill writes the complete projection. Not a lexical search source until Database B.';
comment on column public.processes.search_text is
  'Edge-owned multilingual lexical projection as nullable text[]. The empty scalar precursor was replaced without a heap rewrite; the later backfill writes the complete projection. Not a lexical search source until Database B.';
comment on column public.sources.search_text is
  'Edge-owned multilingual lexical projection as nullable text[]. The empty scalar precursor was replaced without a heap rewrite; the later backfill writes the complete projection. Not a lexical search source until Database B.';
comment on column public.unitgroups.search_text is
  'Edge-owned multilingual lexical projection as nullable text[]. The empty scalar precursor was replaced without a heap rewrite; the later backfill writes the complete projection. Not a lexical search source until Database B.';

-- State 20/100 rows are immutable in authored content.  The only exception
-- for an uncontrolled trusted derivative writer is the exact four-column
-- projection set below.  In particular, modified_at and review metadata are
-- not part of the exception.  Review commands retain their existing
-- app.review_controlled_write = on path.
create or replace function private.review_dataset_content_guard_v1()
returns trigger
language plpgsql
security definer
set search_path = ''
as $$
begin
  if old.state_code in (20, 100)
    and pg_catalog.current_setting('app.review_controlled_write', true)
      is distinct from 'on'
    and (
      to_jsonb(old)
        - array['extracted_md', 'search_text', 'embedding_ft', 'embedding_ft_at']
      is distinct from
      to_jsonb(new)
        - array['extracted_md', 'search_text', 'embedding_ft', 'embedding_ft_at']
    ) then
    raise exception using
      errcode = '55000',
      message = case
        when old.state_code = 100 then 'APPROVED_DATASET_IMMUTABLE'
        else 'DATASET_UNDER_REVIEW_IMMUTABLE'
      end;
  end if;

  return new;
end;
$$;

alter function private.review_dataset_content_guard_v1() owner to postgres;
revoke all on function private.review_dataset_content_guard_v1()
  from public, anon, authenticated, service_role;

comment on function private.review_dataset_content_guard_v1() is
  'State 20/100 authored content is immutable; uncontrolled writes may change only extracted_md, search_text, embedding_ft, and embedding_ft_at. Review-controlled commands retain their existing bypass context.';
