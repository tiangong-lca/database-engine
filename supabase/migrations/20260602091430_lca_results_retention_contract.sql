alter table public.lca_results
  add column if not exists expires_at timestamp with time zone,
  add column if not exists is_pinned boolean;

update public.lca_results
set
  expires_at = coalesce(expires_at, now() + interval '30 days'),
  is_pinned = coalesce(is_pinned, false)
where expires_at is null
   or is_pinned is null;

alter table public.lca_results
  alter column expires_at set default (now() + interval '30 days'),
  alter column expires_at set not null,
  alter column is_pinned set default false,
  alter column is_pinned set not null;

create index if not exists lca_results_expires_at_idx
  on public.lca_results (expires_at, created_at)
  where is_pinned = false;

create index if not exists lca_results_created_desc_idx
  on public.lca_results (created_at desc);

comment on column public.lca_results.expires_at is
  'Result artifact retention deadline used by tiangong-lca-worker lca.result_gc; existing rows created before this contract receive a 30 day migration grace period.';

comment on column public.lca_results.is_pinned is
  'When true, protects the LCA result artifact metadata row from automatic result GC.';
