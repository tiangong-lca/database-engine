-- Identity Center (IC) SSO integration: local user mapping + webhook idempotency tables.
--
-- Additive and dormant. These two tables are created on every deployment but are only
-- read/written by the identity_center edge functions (identity_login_sync /
-- identity_center_webhook) when IC authentication is enabled. Deployments that do not use
-- IC auth simply carry two empty, unused tables; no existing object is modified.
--
-- Access model: RLS is enabled with NO policies, and table access is granted only to
-- service_role (used by the edge functions, which additionally bypass RLS). anon and
-- authenticated get no grants — the cross-system identity mapping is service-side only.
-- keycloak_sub (the Keycloak `sub`) is the cross-system identity key; email is never used.

create table if not exists public.identity_center_users (
    keycloak_sub text not null,
    user_id uuid,
    status character varying(32) default 'active'::character varying not null,
    desired_role character varying(64),
    metadata jsonb default '{}'::jsonb,
    created_at timestamp with time zone default now(),
    modified_at timestamp with time zone,
    constraint identity_center_users_pkey primary key (keycloak_sub),
    constraint identity_center_users_user_id_key unique (user_id),
    constraint identity_center_users_user_id_fkey foreign key (user_id) references auth.users (id)
);

alter table public.identity_center_users owner to postgres;
alter table public.identity_center_users enable row level security;
grant all on table public.identity_center_users to service_role;

create table if not exists public.identity_center_processed_events (
    event_id text not null,
    event_type text not null,
    processed_at timestamp with time zone default now(),
    constraint identity_center_processed_events_pkey primary key (event_id)
);

alter table public.identity_center_processed_events owner to postgres;
alter table public.identity_center_processed_events enable row level security;
grant all on table public.identity_center_processed_events to service_role;
