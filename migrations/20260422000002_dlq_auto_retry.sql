ALTER TABLE transaction_dlq
    ADD COLUMN IF NOT EXISTS permanently_failed BOOLEAN NOT NULL DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS next_retry_at TIMESTAMPTZ;

CREATE INDEX IF NOT EXISTS idx_transaction_dlq_retry
    ON transaction_dlq (next_retry_at)
    WHERE permanently_failed = FALSE;
