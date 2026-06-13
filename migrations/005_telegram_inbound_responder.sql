-- 005_telegram_inbound_responder.sql
-- Responder-loop tracking columns on telegram_inbound (PROJ-TELEGRAM-MIGRATION-001
-- Sole-Surface gates I2/I3). The telegram-responder daemon (bwm-ops-events/
-- telegram-responder) claims unanswered inbound rows, composes a substantive
-- reply via headless claude, sends through the relay /send route, and records
-- the outcome here. responder_status semantics:
--   NULL             — not yet seen by the responder
--   claimed          — a responder run owns this row (responder_claimed_at set)
--   replied          — substantive reply sent (response_outbound_id set)
--   skipped_ack      — pure emoji / short acknowledgment; no reply warranted
--   skipped_backlog  — older than the responder window at first deploy
--   skipped_command  — bot command (/start etc.); relay handles these
--   error            — processing failed after claim (responder_error set)

ALTER TABLE public.telegram_inbound
  ADD COLUMN IF NOT EXISTS responder_status text,
  ADD COLUMN IF NOT EXISTS responder_claimed_at timestamptz,
  ADD COLUMN IF NOT EXISTS responded_at timestamptz,
  ADD COLUMN IF NOT EXISTS response_outbound_id text,
  ADD COLUMN IF NOT EXISTS responder_session text,
  ADD COLUMN IF NOT EXISTS responder_error text;

-- The responder polls for unclaimed rows in the recent window; keep that scan
-- index-only cheap.
CREATE INDEX IF NOT EXISTS idx_telegram_inbound_responder_pending
  ON public.telegram_inbound (received_at)
  WHERE responder_status IS NULL;
