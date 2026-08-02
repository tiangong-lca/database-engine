alter function public.qry_review_get_admin_root_queue_items_v2(
  text,
  integer,
  integer,
  text,
  text
)
set search_path to pg_catalog, public, pg_temp;

alter function public.qry_review_get_member_root_queue_items_v2(
  text,
  integer,
  integer,
  text,
  text
)
set search_path to pg_catalog, public, pg_temp;

alter function public.qry_root_review_reference_progress_v2(uuid)
set search_path to pg_catalog, public, pg_temp;
