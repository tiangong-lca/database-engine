#!/usr/bin/env bash
set -euo pipefail
umask 077

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
supabase_cli="${SUPABASE_CLI:-supabase}"
test_workdir="${PORTAL_PROJECTION_SUPABASE_WORKDIR:-}"
output_dir="${PORTAL_PROJECTION_BENCHMARK_OUTPUT_DIR:-}"

if [[ "${PORTAL_PROJECTION_BENCHMARK_TARGET:-}" != "local-isolated" ]]; then
  echo "set PORTAL_PROJECTION_BENCHMARK_TARGET=local-isolated" >&2
  exit 2
fi
if [[ -z "$test_workdir" || "$test_workdir" != /* ]]; then
  echo "PORTAL_PROJECTION_SUPABASE_WORKDIR must be an absolute isolated project path" >&2
  exit 2
fi
if [[ -z "$output_dir" || "$output_dir" != /* || ! -d "$output_dir" \
   || -L "$output_dir" || ! -O "$output_dir" ]]; then
  echo "PORTAL_PROJECTION_BENCHMARK_OUTPUT_DIR must be an existing absolute directory" >&2
  exit 2
fi
if output_mode="$(stat -f '%Lp' "$output_dir" 2>/dev/null)"; then
  :
else
  output_mode="$(stat -c '%a' "$output_dir")"
fi
if [[ ! "$output_mode" =~ ^[0-7]{3,4}$ ]] \
   || ((8#$output_mode & 077)); then
  echo "benchmark output directory must deny all group/other permissions" >&2
  exit 2
fi
if [[ ! -x "$supabase_cli" ]] && ! command -v "$supabase_cli" >/dev/null 2>&1; then
  echo "SUPABASE_CLI does not resolve to an executable" >&2
  exit 2
fi
if [[ ! -f "$test_workdir/supabase/config.toml" ]]; then
  echo "isolated Supabase config is missing" >&2
  exit 2
fi

project_id="$({
  sed -n 's/^project_id = "\([^"]*\)"$/\1/p' \
    "$test_workdir/supabase/config.toml" | head -n 1
})"
if [[ ! "$project_id" =~ ^database-engine-531-[a-z0-9-]+$ ]]; then
  echo "refusing non-Issue-531 Supabase project_id: $project_id" >&2
  exit 2
fi

shopt -s nullglob
repo_migrations=("$repo_root"/supabase/migrations/*.sql)
test_migrations=("$test_workdir"/supabase/migrations/*.sql)
if [[ "${#repo_migrations[@]}" -ne 257 \
   || "${#test_migrations[@]}" -ne 257 ]]; then
  echo "complete migration tree must contain exactly 257 files" >&2
  exit 2
fi
migration_manifest_payload=""
for migration_index in "${!repo_migrations[@]}"; do
  repo_migration="${repo_migrations[$migration_index]}"
  test_migration="${test_migrations[$migration_index]}"
  repo_name="$(basename "$repo_migration")"
  test_name="$(basename "$test_migration")"
  if [[ "$repo_name" != "$test_name" ]] \
     || ! cmp -s "$repo_migration" "$test_migration"; then
    echo "isolated migration tree differs from repository HEAD: $repo_name/$test_name" >&2
    exit 2
  fi
  migration_hash="$(shasum -a 256 "$repo_migration" | awk '{print $1}')"
  migration_manifest_payload="${migration_manifest_payload}${migration_hash}  ${repo_name}\n"
done
migration_tree_sha256="$(
  printf '%b' "$migration_manifest_payload" | shasum -a 256 | awk '{print $1}'
)"
repository_head="$(git -C "$repo_root" rev-parse HEAD)"
benchmark_sql_sha256="$(
  shasum -a 256 \
    "$repo_root/supabase/tests/benchmarks/20260826_portal_candidate_first_explain.sql" \
    | awk '{print $1}'
)"
benchmark_runner_sha256="$(
  shasum -a 256 "$repo_root/scripts/run_portal_projection_benchmark.sh" \
    | awk '{print $1}'
)"
if [[ ! "$repository_head" =~ ^[0-9a-f]{40}$ \
   || ! "$migration_tree_sha256" =~ ^[0-9a-f]{64}$ \
   || ! "$benchmark_sql_sha256" =~ ^[0-9a-f]{64}$ \
   || ! "$benchmark_runner_sha256" =~ ^[0-9a-f]{64}$ ]]; then
  echo "repository/migration aggregate identity is malformed" >&2
  exit 2
fi
if [[ -e "$test_workdir/supabase/migrations/20260826080354_portal_projection_process_hnsw.sql" \
   || -e "$test_workdir/supabase/migrations/20260826080357_portal_projection_flow_hnsw.sql" ]]; then
  echo "isolated project retains retired projection HNSW migrations" >&2
  exit 2
fi

container_name="supabase_db_${project_id}"
if ! docker inspect "$container_name" >/dev/null 2>&1; then
  echo "isolated database container is not running: $container_name" >&2
  exit 2
fi

results_log="$output_dir/portal-projection-benchmark-results.log"
explain_log="$output_dir/portal-projection-benchmark-explain.log"
if [[ -e "$results_log" || -L "$results_log" \
   || -e "$explain_log" || -L "$explain_log" ]]; then
  echo "benchmark output files already exist; use a new private directory" >&2
  exit 2
fi

container_explain="/tmp/database-engine-531-portal-explain-$$.log"
reset_completed=false
cleanup() {
  local status=$?
  trap - EXIT INT TERM
  set +e
  docker exec "$container_name" rm -f "$container_explain" >/dev/null 2>&1
  local reset_status=0
  if [[ "$reset_completed" != "true" ]]; then
    "$supabase_cli" --workdir "$test_workdir" \
      db reset --local --no-seed >/dev/null 2>&1
    reset_status=$?
  fi
  if [[ "$status" -eq 0 && "$reset_status" -ne 0 ]]; then
    status=$reset_status
  fi
  exit "$status"
}
trap cleanup EXIT INT TERM

benchmark_samples="${PORTAL_PROJECTION_BENCHMARK_SAMPLES:-20}"
process_rows="${PORTAL_PROJECTION_BENCHMARK_PROCESS_ROWS:-17299}"
flow_rows="${PORTAL_PROJECTION_BENCHMARK_FLOW_ROWS:-108947}"
process_vector_rows="${PORTAL_PROJECTION_BENCHMARK_PROCESS_VECTOR_ROWS:-17299}"
flow_vector_rows="${PORTAL_PROJECTION_BENCHMARK_FLOW_VECTOR_ROWS:-108947}"
process_old_rows="${PORTAL_PROJECTION_BENCHMARK_PROCESS_OLD_ROWS:-100}"
flow_old_rows="${PORTAL_PROJECTION_BENCHMARK_FLOW_OLD_ROWS:-21000}"
draft_vector_rows="${PORTAL_PROJECTION_BENCHMARK_DRAFT_ROWS:-100}"
writer_samples="${PORTAL_PROJECTION_BENCHMARK_WRITER_SAMPLES:-50}"

validate_integer() {
  local name="$1"
  local value="$2"
  local minimum="$3"
  local maximum="$4"
  if [[ ! "$value" =~ ^[0-9]+$ ]] \
     || ((10#$value < minimum || 10#$value > maximum)); then
    echo "$name must be a decimal integer in [$minimum,$maximum]" >&2
    exit 2
  fi
}

validate_integer benchmark_samples "$benchmark_samples" 1 100
validate_integer process_rows "$process_rows" 1 200000
validate_integer flow_rows "$flow_rows" 1 300000
validate_integer process_vector_rows "$process_vector_rows" 0 200000
validate_integer flow_vector_rows "$flow_vector_rows" 0 300000
validate_integer process_old_rows "$process_old_rows" 0 200000
validate_integer flow_old_rows "$flow_old_rows" 0 300000
validate_integer draft_vector_rows "$draft_vector_rows" 0 200000
validate_integer writer_samples "$writer_samples" 1 1000
if ((10#$process_vector_rows > 10#$process_rows \
   || 10#$flow_vector_rows > 10#$flow_rows \
   || 10#$process_old_rows > 10#$process_rows \
   || 10#$flow_old_rows > 10#$flow_rows \
   || 10#$draft_vector_rows > 10#$process_rows \
   || 10#$draft_vector_rows > 10#$flow_rows)); then
  echo "vector/old/draft benchmark counts must not exceed their source row counts" >&2
  exit 2
fi

release_profile=false
if [[ "$benchmark_samples" == "20" \
   && "$process_rows" == "17299" \
   && "$flow_rows" == "108947" \
   && "$process_vector_rows" == "17299" \
   && "$flow_vector_rows" == "108947" \
   && "$process_old_rows" == "100" \
   && "$flow_old_rows" == "21000" \
   && "$draft_vector_rows" == "100" \
   && "$writer_samples" == "50" ]]; then
  release_profile=true
fi
if [[ "$release_profile" == "true" \
   && -n "$(git -C "$repo_root" status --porcelain)" ]]; then
  echo "release benchmark requires a clean exact repository HEAD" >&2
  exit 2
fi

supabase_cli_version="$($supabase_cli --version)"
if [[ "$supabase_cli_version" != "2.109.1" ]]; then
  echo "Supabase CLI must be the reviewed 2.109.1, found $supabase_cli_version" >&2
  exit 2
fi

echo "Supabase CLI: $supabase_cli_version" | tee "$results_log"
echo "Benchmark target: $project_id" | tee -a "$results_log"
echo "Benchmark profile: $release_profile" | tee -a "$results_log"
echo "Repository HEAD: $repository_head" | tee -a "$results_log"
echo "Migration tree SHA-256 (257 files): $migration_tree_sha256" \
  | tee -a "$results_log"
echo "Benchmark SQL SHA-256: $benchmark_sql_sha256" | tee -a "$results_log"
echo "Benchmark runner SHA-256: $benchmark_runner_sha256" \
  | tee -a "$results_log"
echo "Rows/vectors: process=$process_rows/$process_vector_rows flow=$flow_rows/$flow_vector_rows" \
  | tee -a "$results_log"
echo "Pressure rows: process-old=$process_old_rows flow-old=$flow_old_rows draft=$draft_vector_rows" \
  | tee -a "$results_log"
echo "Hosted promotion must re-read and record project/ref/time plus pg_database_size before/after migration; PGroonga external storage is not represented by pg_relation_size(index)." \
  | tee -a "$results_log"

"$supabase_cli" --workdir "$test_workdir" \
  db reset --local --no-seed >/dev/null 2>&1

docker exec -i "$container_name" \
  psql -X -v ON_ERROR_STOP=1 -U postgres -d postgres \
    -v benchmark_target=local \
    -v explain_output="$container_explain" \
    -v benchmark_samples="$benchmark_samples" \
    -v process_rows="$process_rows" \
    -v flow_rows="$flow_rows" \
    -v process_vector_rows="$process_vector_rows" \
    -v flow_vector_rows="$flow_vector_rows" \
    -v process_old_version_rows="$process_old_rows" \
    -v flow_old_version_rows="$flow_old_rows" \
    -v draft_vector_rows="$draft_vector_rows" \
    -v writer_samples="$writer_samples" \
  <"$repo_root/supabase/tests/benchmarks/20260826_portal_candidate_first_explain.sql" \
  2>&1 | tee -a "$results_log"

expected_sql_status=DIAGNOSTIC_PASS
if [[ "$release_profile" == "true" ]]; then
  expected_sql_status=RELEASE_PASS
fi
if ! grep -q "^SQL_STATUS=${expected_sql_status}$" "$results_log"; then
  echo "benchmark SQL reported failure" >&2
  exit 1
fi

docker cp "$container_name:$container_explain" "$explain_log" >/dev/null
chmod 600 "$explain_log"

for expected_index in \
  portal_catalog_search_process_document_v1_pgroonga \
  portal_catalog_search_flow_document_v1_pgroonga \
  processes_embedding_ft_hnsw_idx \
  flows_embedding_ft_hnsw_idx
do
  if ! grep -q "$expected_index" "$explain_log"; then
    echo "missing representative plan index: $expected_index" >&2
    exit 1
  fi
done

docker exec "$container_name" rm -f "$container_explain" >/dev/null
"$supabase_cli" --workdir "$test_workdir" \
  db reset --local --no-seed >/dev/null 2>&1
reset_completed=true

if [[ "$release_profile" == "true" ]]; then
  echo "BENCHMARK_STATUS=PASS" | tee -a "$results_log"
else
  echo "BENCHMARK_STATUS=DIAGNOSTIC_PASS" | tee -a "$results_log"
fi
echo "Portal projection representative benchmark passed after isolated reset" \
  | tee -a "$results_log"
