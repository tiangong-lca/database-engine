-- Deliberately omit IF NOT EXISTS. A failed concurrent build can leave an
-- INVALID relation; deployment must stop so an operator can inspect and drop
-- that artifact instead of silently accepting a missing candidate index.
create index concurrently
  processes_json_ordered_alias_exchange_gin_idx
on public.processes using gin (
  (
    private.dataset_alias_jsonb_array_v1(
      json_ordered::jsonb
        #> '{processDataSet,exchanges,exchange}'
    )
  ) jsonb_path_ops
);
