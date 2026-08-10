-- Database A / workspace#565: make the seven search projections arrays and
-- close the reviewed-row derivative boundary.
--
-- This is a forward-only follow-up to 20260810200000.  A non-NULL legacy
-- scalar is kept as one complete array element, including any newlines.  The
-- later service/Edge backfill replaces that compatibility value with the
-- complete normalized projection; this migration does not run that backfill.

set lock_timeout = '5s';
set statement_timeout = '120s';

alter table public.contacts
  alter column search_text type text[]
  using case
    when search_text is null then null
    else array[search_text]
  end;

alter table public.flowproperties
  alter column search_text type text[]
  using case
    when search_text is null then null
    else array[search_text]
  end;

alter table public.flows
  alter column search_text type text[]
  using case
    when search_text is null then null
    else array[search_text]
  end;

alter table public.lifecyclemodels
  alter column search_text type text[]
  using case
    when search_text is null then null
    else array[search_text]
  end;

alter table public.processes
  alter column search_text type text[]
  using case
    when search_text is null then null
    else array[search_text]
  end;

alter table public.sources
  alter column search_text type text[]
  using case
    when search_text is null then null
    else array[search_text]
  end;

alter table public.unitgroups
  alter column search_text type text[]
  using case
    when search_text is null then null
    else array[search_text]
  end;

comment on column public.contacts.search_text is
  'Edge-owned multilingual lexical projection as nullable text[]. A legacy non-NULL text value is preserved as one array element during migration; the later backfill replaces it with the complete projection. Not a lexical search source until Database B.';
comment on column public.flowproperties.search_text is
  'Edge-owned multilingual lexical projection as nullable text[]. A legacy non-NULL text value is preserved as one array element during migration; the later backfill replaces it with the complete projection. Not a lexical search source until Database B.';
comment on column public.flows.search_text is
  'Edge-owned multilingual lexical projection as nullable text[]. A legacy non-NULL text value is preserved as one array element during migration; the later backfill replaces it with the complete projection. Not a lexical search source until Database B.';
comment on column public.lifecyclemodels.search_text is
  'Edge-owned multilingual lexical projection as nullable text[]. A legacy non-NULL text value is preserved as one array element during migration; the later backfill replaces it with the complete projection. Not a lexical search source until Database B.';
comment on column public.processes.search_text is
  'Edge-owned multilingual lexical projection as nullable text[]. A legacy non-NULL text value is preserved as one array element during migration; the later backfill replaces it with the complete projection. Not a lexical search source until Database B.';
comment on column public.sources.search_text is
  'Edge-owned multilingual lexical projection as nullable text[]. A legacy non-NULL text value is preserved as one array element during migration; the later backfill replaces it with the complete projection. Not a lexical search source until Database B.';
comment on column public.unitgroups.search_text is
  'Edge-owned multilingual lexical projection as nullable text[]. A legacy non-NULL text value is preserved as one array element during migration; the later backfill replaces it with the complete projection. Not a lexical search source until Database B.';

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
