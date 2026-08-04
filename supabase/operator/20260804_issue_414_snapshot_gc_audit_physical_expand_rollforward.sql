-- Controlled roll-forward after the paired Issue #414 emergency rollback.
-- The migration ledger remains applied during rollback, so ordinary Supabase
-- migration replay will not execute the physical migration again.

\set ON_ERROR_STOP on
\ir ../migrations/20260804123000_issue_414_snapshot_gc_audit_physical_expand.sql
