DROP INDEX IF EXISTS idx_transactions_priority;
ALTER TABLE transactions DROP COLUMN IF EXISTS priority;
