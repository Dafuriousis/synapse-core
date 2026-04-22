DROP INDEX IF EXISTS idx_transaction_dlq_retry;
ALTER TABLE transaction_dlq
    DROP COLUMN IF EXISTS permanently_failed,
    DROP COLUMN IF EXISTS next_retry_at;
