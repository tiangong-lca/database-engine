-- Read-only transport preflight for the guarded Step 3 Hosted Preview E2E.
--
-- Run this through the exact pinned Supabase CLI, exact regular nested Docker
-- executable, exact cached pg_prove image ID/digest/platform, database URL,
-- child environment, and network path that the full runner will use. It must run
-- before approval is requested and the full runner repeats it before creating
-- disposable auth actors.  Never point it at production.

\set ON_ERROR_STOP on

begin read only;

set local statement_timeout = '15s';

do $transport$
begin
  if current_database() <> 'postgres' then
    raise exception 'unexpected database';
  end if;
  if current_setting('transaction_read_only') <> 'on' then
    raise exception 'transport preflight is not read only';
  end if;
end
$transport$;

select tap
from (values
  ('TAP version 13'),
  ('1..1'),
  ('ok 1 - authenticated read-only Preview database transport is reachable')
) as transport_tap(tap);

rollback;
