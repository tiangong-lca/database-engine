-- Advance the internal scanner cache identity after Worker scope-closure
-- semantics stop treating Source digital-file locators as dataset references.
-- Historical requests retain their recorded fingerprint and remain readable;
-- new requests receive a distinct request and scan-execution identity.

set lock_timeout = '5s';
set statement_timeout = '120s';

insert into private.lcia_scope_closure_config (
  singleton,
  expected_validator_scanner_fingerprint
)
values (
  true,
  'scope-closure-validator-scanner.v1+cutoff-readiness-r3'
)
on conflict (singleton) do update
set expected_validator_scanner_fingerprint = excluded.expected_validator_scanner_fingerprint,
    updated_at = statement_timestamp();
