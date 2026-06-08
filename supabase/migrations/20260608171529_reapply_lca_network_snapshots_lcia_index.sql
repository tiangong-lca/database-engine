create index concurrently if not exists lca_network_snapshots_lcia_idx
  on public.lca_network_snapshots (lcia_method_id, lcia_method_version)
  where lcia_method_id is not null;
