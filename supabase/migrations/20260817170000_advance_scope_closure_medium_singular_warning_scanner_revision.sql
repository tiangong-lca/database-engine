-- Advance the internal scanner cache identity after certificate-grade Scope
-- Closure retains medium singular risk as a non-blocking warning. Historical
-- requests keep their recorded fingerprint; new requests must not reuse an r3
-- completed blocked scan whose verdict used the former readiness policy.

set lock_timeout = '5s';
set statement_timeout = '120s';

insert into private.lcia_scope_closure_config (
  singleton,
  expected_validator_scanner_fingerprint
)
values (
  true,
  'scope-closure-validator-scanner.v1+cutoff-readiness-r4'
)
on conflict (singleton) do update
set expected_validator_scanner_fingerprint = excluded.expected_validator_scanner_fingerprint,
    updated_at = statement_timestamp();
