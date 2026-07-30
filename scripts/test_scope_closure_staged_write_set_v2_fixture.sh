#!/usr/bin/env bash
set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"
fixture="$repo_root/supabase/tests/fixtures/20260730_scope_closure_staged_write_set_v2_contract.json"

command -v jq >/dev/null
command -v shasum >/dev/null
jq empty "$fixture"

schema_version="$(jq -r '.schemaVersion' "$fixture")"
contract_version="$(jq -r '.contractVersion' "$fixture")"
recorded_canonical="$(jq -r '.canonicalization.descriptorSetCanonicalJson' "$fixture")"
generated_canonical="$(
  jq -cS '{contractVersion, descriptors: .example.descriptors}' "$fixture"
)"
recorded_digest="$(jq -r '.canonicalization.descriptorSetSha256' "$fixture")"
generated_digest="$(
  printf '%s' "$generated_canonical" | shasum -a 256 | awk '{print $1}'
)"

test "$schema_version" = "lcia.scope-closure-staged-write-set-fixture.v2"
test "$contract_version" = "lcia.scope-closure-artifact-write-set.v2"
test "$recorded_canonical" = "$generated_canonical"
test "$recorded_digest" = "$generated_digest"
test "$(jq '.limits.ordinalBase' "$fixture")" -eq 1
test "$(jq '.limits.maximumBatchDescriptorCount' "$fixture")" -eq 500
test "$(jq '.limits.maximumExpectedDescriptorCount' "$fixture")" -eq 100000
test "$(jq '.example.descriptors | length' "$fixture")" -eq 4
test "$(jq '.requiredPrimaryRoles.fresh | length' "$fixture")" -eq 3
test "$(jq '.requiredPrimaryRoles.reused | length' "$fixture")" -eq 1

jq -e '
  (.rpc.status.fields | length) == 21
  and ((.rpc.status.fields - .rpc.status.forbiddenFields) | length)
    == (.rpc.status.fields | length)
  and .rpc.create.uploadEligible == false
  and .states.registration_open.uploadEligible == false
  and .states.staging.uploadEligible == true
  and .rpc.registerBatch.postSealExactReplay == "ok=true,reused=true"
  and .rpc.registerBatch.postSealMutation
    == "artifact_write_set_v2_registration_closed"
  and .legacyAdapter.maximumDescriptorCount == 500
  and .legacyAdapter.removal
    == "separate explicit contraction audit only after deployed consumers migrate"
' "$fixture" >/dev/null

fixture_digest="$(shasum -a 256 "$fixture" | awk '{print $1}')"
jq -n \
  --arg schemaVersion "lcia.scope-closure-staged-write-set-fixture-proof.v1" \
  --arg fixtureSha256 "$fixture_digest" \
  --arg descriptorSetSha256 "$generated_digest" \
  '{
    schemaVersion: $schemaVersion,
    fixtureSha256: $fixtureSha256,
    descriptorSetSha256: $descriptorSetSha256,
    descriptorCount: 4,
    maximumBatchDescriptorCount: 500,
    statusForbiddenFieldsDisjoint: true,
    legacyAdapterRetained: true
  }'
