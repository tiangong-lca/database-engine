insert into public.worker_job_kinds (
  job_kind,
  worker_runtime,
  worker_queue,
  default_visibility,
  default_priority,
  default_max_attempts,
  default_lease_seconds,
  payload_schema_version,
  result_schema_version,
  user_visible,
  description
) values (
  'national_carbon.process_flow_graph_cache_build',
  'calculator',
  'maintenance',
  'operator',
  0,
  1,
  3600,
  'national_carbon.process_flow_graph_cache_build.request.v1',
  'national_carbon.process_flow_graph_cache_build.result.v1',
  false,
  'Build national carbon process-flow graph cache through the maintenance worker'
)
on conflict (job_kind) do update
set worker_runtime = excluded.worker_runtime,
    worker_queue = excluded.worker_queue,
    default_visibility = excluded.default_visibility,
    default_priority = excluded.default_priority,
    default_max_attempts = excluded.default_max_attempts,
    default_lease_seconds = excluded.default_lease_seconds,
    payload_schema_version = excluded.payload_schema_version,
    result_schema_version = excluded.result_schema_version,
    user_visible = excluded.user_visible,
    description = excluded.description,
    updated_at = now();
