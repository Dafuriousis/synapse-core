ALTER TABLE transactions ADD COLUMN IF NOT EXISTS priority SMALLINT NOT NULL DEFAULT 0;

CREATE INDEX IF NOT EXISTS idx_transactions_priority
    ON transactions (status, priority DESC, created_at ASC);
