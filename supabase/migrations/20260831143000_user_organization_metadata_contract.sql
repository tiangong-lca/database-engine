-- Keep organization profile metadata bounded without changing the existing
-- Supabase Auth -> private.users ownership and synchronization path.

alter table private.users
  add constraint users_organization_metadata_contract
  check (
    raw_user_meta_data is null
    or not (raw_user_meta_data ? 'organization')
    or (
      jsonb_typeof(raw_user_meta_data -> 'organization') = 'string'
      and (raw_user_meta_data ->> 'organization')
        !~ '^[[:space:]]|[[:space:]]$'
      and char_length(raw_user_meta_data ->> 'organization') <= 200
    )
  ) not valid;

alter table private.users
  validate constraint users_organization_metadata_contract;
