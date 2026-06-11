-- 004_telegram_outbound_revoke.sql
-- BWM RLS-on-create invariant backfill for telegram_outbound (003 enabled RLS
-- but omitted the REVOKE; RLS default-deny already blocked anon/authenticated,
-- this makes the denial explicit per reference/Storage-Architecture.md #12).

REVOKE ALL ON TABLE public.telegram_outbound FROM anon, authenticated;
