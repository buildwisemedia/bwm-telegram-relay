-- 002_telegram_inbound.sql
-- PROJ-ATTN-ROUTING-001 Chip 7a: raw Telegram webhook catch-all persistence.
-- Supabase is the log layer; this table preserves inbound updates before routing.

CREATE TABLE public.telegram_inbound (
  event_id TEXT PRIMARY KEY,
  chat_id BIGINT NOT NULL,
  from_id BIGINT,
  message_id BIGINT NOT NULL,
  text TEXT,
  entities JSONB,
  raw_update JSONB NOT NULL,
  received_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  forwarded_to_router_at TIMESTAMPTZ,
  forward_status TEXT
);

CREATE INDEX idx_telegram_inbound_chat_received
  ON public.telegram_inbound (chat_id, received_at DESC);

CREATE INDEX idx_telegram_inbound_received
  ON public.telegram_inbound (received_at DESC);
