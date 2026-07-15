-- Shared seed data for the production baseline, local development, and preview branches.
-- Keep one executable no-op statement so hosted Preview seeding never sends a
-- comments-only batch, which some deployment clients reject as an empty query.
select 1;
