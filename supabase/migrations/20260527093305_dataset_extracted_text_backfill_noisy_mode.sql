create or replace function public.cmd_dataset_extracted_text_backfill(
  p_table text,
  p_batch_size integer default 1000,
  p_after_id uuid default null,
  p_after_version text default null,
  p_mode text default 'empty'
) returns jsonb
language plpgsql
security definer
set search_path to ''
as $$
declare
  v_table text := lower(btrim(coalesce(p_table, '')));
  v_mode text := lower(btrim(coalesce(p_mode, 'empty')));
  v_batch_size integer := least(greatest(coalesce(p_batch_size, 1000), 1), 5000);
  v_scanned_count integer := 0;
  v_updated_count integer := 0;
  v_last_id uuid;
  v_last_version text;
begin
  if v_table not in (
    'flows',
    'processes',
    'lifecyclemodels',
    'contacts',
    'sources',
    'unitgroups',
    'flowproperties'
  ) then
    return jsonb_build_object(
      'ok', false,
      'code', 'UNSUPPORTED_DATASET_TABLE',
      'message', format('Unsupported dataset table: %s', coalesce(p_table, '<null>'))
    );
  end if;

  if v_mode not in ('empty', 'stale', 'noisy') then
    return jsonb_build_object(
      'ok', false,
      'code', 'UNSUPPORTED_BACKFILL_MODE',
      'message', format('Unsupported extracted_text backfill mode: %s', coalesce(p_mode, '<null>'))
    );
  end if;

  execute format(
    $sql$
      with page as (
        select id, version
          from public.%1$I
         where json is not null
           and ($4 <> 'empty' or coalesce(extracted_text, '') = '')
           and (
             $4 <> 'noisy'
             or extracted_text like '%%../%%'
             or extracted_text like '%%schemas/%%'
             or extracted_text ~* 'https?://'
             or extracted_text ~* '[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}'
           )
           and ($1 is null or (id, version) > ($1, coalesce($2, '')::character(9)))
         order by id, version
         limit $3
         for update skip locked
      ),
      computed as (
        select dataset.id,
               dataset.version,
               util.dataset_json_search_text($5, dataset.json) as next_extracted_text
          from public.%1$I as dataset
          join page on page.id = dataset.id
                   and page.version = dataset.version
      ),
      updated as (
        update public.%1$I as dataset
           set extracted_text = computed.next_extracted_text
          from computed
         where dataset.id = computed.id
           and dataset.version = computed.version
         returning 1
      )
      select
        (select count(*)::integer from page),
        (select count(*)::integer from updated),
        (select id from page order by id desc, version desc limit 1),
        (select version::text from page order by id desc, version desc limit 1)
    $sql$,
    v_table
  )
  using p_after_id, p_after_version, v_batch_size, v_mode, v_table
  into v_scanned_count, v_updated_count, v_last_id, v_last_version;

  return jsonb_build_object(
    'ok', true,
    'table', v_table,
    'mode', v_mode,
    'scanned_count', v_scanned_count,
    'updated_count', v_updated_count,
    'last_id', v_last_id,
    'last_version', v_last_version,
    'has_more', v_scanned_count = v_batch_size
  );
end;
$$;

alter function public.cmd_dataset_extracted_text_backfill(text, integer, uuid, text, text) owner to postgres;
revoke all on function public.cmd_dataset_extracted_text_backfill(text, integer, uuid, text, text) from public;
revoke all on function public.cmd_dataset_extracted_text_backfill(text, integer, uuid, text, text) from anon;
revoke all on function public.cmd_dataset_extracted_text_backfill(text, integer, uuid, text, text) from authenticated;
grant execute on function public.cmd_dataset_extracted_text_backfill(text, integer, uuid, text, text) to service_role;

comment on function public.cmd_dataset_extracted_text_backfill(text, integer, uuid, text, text) is
  'Service-role RPC for bounded historical extracted_text backfill. Modes: empty repairs empty rows, noisy rewrites rows with reference/schema metadata noise, stale rewrites every selected row.';
