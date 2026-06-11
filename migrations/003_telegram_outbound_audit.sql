-- 003_telegram_outbound_audit.sql
-- Durable Telegram send audit for both /event and /send routes.

CREATE TABLE IF NOT EXISTS public.telegram_outbound (
  id TEXT PRIMARY KEY,
  source_route TEXT NOT NULL,
  origin_event_id TEXT,
  origin_event_type TEXT,
  origin_session_id TEXT,
  chat_id TEXT,
  parse_mode TEXT,
  text_sha256 TEXT NOT NULL,
  text_redacted TEXT NOT NULL,
  dedupe_key TEXT,
  status TEXT NOT NULL DEFAULT 'queued',
  telegram_message_id BIGINT,
  telegram_response JSONB NOT NULL DEFAULT '{}'::jsonb,
  error TEXT,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  queued_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  sent_at TIMESTAMPTZ,
  failed_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT telegram_outbound_status_check
    CHECK (status IN ('queued', 'sent', 'failed', 'skipped'))
);

CREATE INDEX IF NOT EXISTS idx_telegram_outbound_created
  ON public.telegram_outbound (created_at DESC);

CREATE INDEX IF NOT EXISTS idx_telegram_outbound_origin_event
  ON public.telegram_outbound (origin_event_id);

CREATE INDEX IF NOT EXISTS idx_telegram_outbound_status
  ON public.telegram_outbound (status, created_at DESC);

ALTER TABLE public.telegram_outbound ENABLE ROW LEVEL SECURITY;

ALTER TABLE public.telegram_inbound
  ADD COLUMN IF NOT EXISTS forward_error TEXT,
  ADD COLUMN IF NOT EXISTS router_status INTEGER,
  ADD COLUMN IF NOT EXISTS forwarded_to_classifier_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS classifier_status INTEGER,
  ADD COLUMN IF NOT EXISTS classifier_error TEXT;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'telegram_inbound_forward_status_check'
      AND conrelid = 'public.telegram_inbound'::regclass
  ) THEN
    ALTER TABLE public.telegram_inbound
      ADD CONSTRAINT telegram_inbound_forward_status_check
      CHECK (
        forward_status IS NULL OR forward_status IN (
          'pending',
          'forwarded',
          'error',
          'skipped_command',
          'skipped_empty'
        )
      ) NOT VALID;
  END IF;
END $$;
