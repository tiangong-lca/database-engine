#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

supabase db reset --version 20260728083000 --no-seed
supabase test db \
  supabase/tests/upgrade/20260729_scope_closure_artifact_retention_base.sql
supabase migration up --local
supabase test db \
  supabase/tests/upgrade/20260729_scope_closure_artifact_retention_head.sql
