-- Issue #539: add one narrow, writer-safe index for 64 deterministic sitemap
-- buckets. The stable MD5 bucket is a distribution primitive, not a security
-- decision. This standalone concurrent statement does not change any writer,
-- projection, visibility, or existing Portal API semantics.

create index concurrently portal_sitemap_shard_v1_idx
on private.portal_catalog_facet_rows_v1 (
  (
    pg_catalog.get_byte(
      pg_catalog.decode(
        pg_catalog.md5(dataset_kind || ':'::text || id::text),
        'hex'::text
      ),
      0
    ) / 4
  ),
  dataset_kind,
  id,
  version desc,
  modified_at desc,
  state_code desc,
  facet_contract_version
);
