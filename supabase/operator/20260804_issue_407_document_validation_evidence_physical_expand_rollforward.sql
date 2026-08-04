-- Controlled roll-forward after the paired Phase B emergency rollback.
-- The migration ledger intentionally remains at 20260804100000 during an
-- operator rollback, so the ordinary Supabase runner will not replay it.

\set ON_ERROR_STOP on
\ir ../migrations/20260804100000_issue_407_document_validation_evidence_physical_expand.sql
