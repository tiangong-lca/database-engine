#!/usr/bin/env bash
set -euo pipefail

usage() {
  echo "Usage: DATABASE_URL=... REVIEW_BACKUP_PASSWORD_FILE=... $0 <backup|dry-run|apply|verify> <backup-dir>"
}

command_name="${1:-}"
backup_dir="${2:-}"
if [[ -z "$command_name" || -z "$backup_dir" || -z "${DATABASE_URL:-}" ]]; then
  usage
  exit 2
fi

repo_root="$(git rev-parse --show-toplevel)"
backup_dir="$(mkdir -p "$backup_dir" && cd "$backup_dir" && pwd)"
case "$backup_dir/" in
  "$repo_root/"*) echo "Backup directory must be outside the Git worktree" >&2; exit 2 ;;
esac
chmod 700 "$backup_dir"

password_file="${REVIEW_BACKUP_PASSWORD_FILE:-}"
require_password() {
  if [[ -z "$password_file" || ! -f "$password_file" ]]; then
    echo "REVIEW_BACKUP_PASSWORD_FILE must point to a local password file" >&2
    exit 2
  fi
}

encrypt_file() {
  local source_file="$1"
  local encrypted_file="$2"
  openssl enc -aes-256-cbc -salt -pbkdf2 \
    -pass "file:$password_file" \
    -in "$source_file" \
    -out "$encrypted_file"
}

case "$command_name" in
  backup)
    require_password
    temp_dir="$(mktemp -d)"
    trap 'rm -rf "$temp_dir"' EXIT

    pg_dump --format=custom --no-owner --no-privileges \
      "$DATABASE_URL" \
      --file "$temp_dir/full-database.dump"
    encrypt_file "$temp_dir/full-database.dump" "$backup_dir/full-database.dump.enc"

    pg_dump --format=custom --no-owner --no-privileges \
      --table=private.reviews \
      --table=private.comments \
      --table=private.notifications \
      --table=private.command_audit_log \
      "$DATABASE_URL" \
      --file "$temp_dir/review-tables.dump"
    encrypt_file "$temp_dir/review-tables.dump" "$backup_dir/review-tables.dump.enc"

    for table_name in contacts sources unitgroups flowproperties flows processes lifecyclemodels; do
      psql "$DATABASE_URL" --csv --no-psqlrc --command \
        "select * from public.${table_name}
         where state_code in (20, 100)
            or review_id is not null
            or coalesce(reviews, '[]'::jsonb) <> '[]'::jsonb
         order by id, version" \
        > "$temp_dir/${table_name}-affected.csv"
      encrypt_file \
        "$temp_dir/${table_name}-affected.csv" \
        "$backup_dir/${table_name}-affected.csv.enc"
    done

    {
      echo "created_at=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
      echo "database_version=$(psql "$DATABASE_URL" --tuples-only --no-align --command 'show server_version')"
      echo "migration_commit=$(git rev-parse HEAD)"
      echo "watermark=$(psql "$DATABASE_URL" --tuples-only --no-align --command 'select clock_timestamp()')"
    } > "$backup_dir/METADATA"

    (
      cd "$backup_dir"
      shasum -a 256 ./*.enc METADATA > SHA256SUMS
    )
    ;;

  dry-run)
    psql "$DATABASE_URL" --csv --no-psqlrc --command \
      "select
         review_row.id as legacy_review_id,
         gen_random_uuid() as new_root_review_id,
         review_row.state_code,
         review_row.data_id,
         btrim(review_row.data_version::text) as data_version,
         review_row.reviewer_id,
         review_row.created_at,
         review_row.modified_at
       from private.reviews as review_row
       where review_row.review_kind is null
       order by review_row.created_at, review_row.id" \
      > "$backup_dir/legacy-review-migration-manifest.csv"
    ;;

  verify)
    (
      cd "$backup_dir"
      shasum -a 256 --check SHA256SUMS
    )
    ;;

  apply)
    if [[ ! -f "$backup_dir/RESTORE_VERIFIED" \
      || ! -f "$backup_dir/SECOND_LOCAL_COPY_VERIFIED" \
      || ! -f "$backup_dir/legacy-review-migration-manifest.csv" ]]; then
      echo "Apply is locked until restore, second-local-copy, and dry-run markers exist" >&2
      exit 2
    fi
    (
      cd "$backup_dir"
      shasum -a 256 --check SHA256SUMS
    )

    tail -n +2 "$backup_dir/legacy-review-migration-manifest.csv" |
      while IFS=, read -r legacy_review_id new_root_review_id _rest; do
        legacy_review_id="${legacy_review_id%\"}"
        legacy_review_id="${legacy_review_id#\"}"
        new_root_review_id="${new_root_review_id%\"}"
        new_root_review_id="${new_root_review_id#\"}"
        psql "$DATABASE_URL" --no-psqlrc --set ON_ERROR_STOP=1 \
          --command "set request.jwt.claim.role = 'service_role';
            select private.cmd_review_migrate_legacy_v2(
              '${legacy_review_id}'::uuid,
              '${new_root_review_id}'::uuid,
              jsonb_build_object('source', 'local_legacy_migration')
            );"
      done
    ;;

  *)
    usage
    exit 2
    ;;
esac
