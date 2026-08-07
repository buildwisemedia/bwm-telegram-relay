-- Enable row-level security and revoke anon/authenticated on a table created
-- without it. Anti-drift principle #12 (RLS-on-create, LOCKED 2026-05-20).
-- Predates the gate; surfaced by the fleet sweep
-- (bwm-ops-events scripts/fleet_migration_audit.py, 2026-08-07).
--
-- telegram_outbound already got RLS in 004_telegram_outbound_revoke.sql;
-- telegram_inbound was never given the same treatment. It holds inbound message
-- content from Robert's relay, so anon readability is the wrong default.
--
-- Safe to apply: this repo authenticates with SUPABASE_SERVICE_KEY, which
-- BYPASSES row-level security; there is no anon read path. No policy is added,
-- so with RLS on and no policy anon/authenticated get nothing while
-- service_role is unaffected.

ALTER TABLE public.telegram_inbound ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON TABLE public.telegram_inbound FROM anon, authenticated;
