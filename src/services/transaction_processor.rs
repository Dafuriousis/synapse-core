use crate::db::models::TransactionDlq;
use crate::services::webhook_dispatcher::WebhookDispatcher;
use sqlx::PgPool;
use tracing::instrument;

/// Maximum number of automatic DLQ retries before marking permanently failed.
/// Overridden by `DLQ_MAX_RETRIES` env var.
fn dlq_max_retries() -> i32 {
    std::env::var("DLQ_MAX_RETRIES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(5)
}

/// Base delay in seconds for exponential backoff.
/// Overridden by `DLQ_BASE_DELAY_SECS` env var.
fn dlq_base_delay_secs() -> i64 {
    std::env::var("DLQ_BASE_DELAY_SECS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(60)
}

/// Compute next retry timestamp: last_retry_at + base_delay * 2^retry_count
pub fn next_retry_at(
    last_retry_at: chrono::DateTime<chrono::Utc>,
    retry_count: i32,
    base_delay_secs: i64,
) -> chrono::DateTime<chrono::Utc> {
    let delay = base_delay_secs * (1_i64 << retry_count.min(30));
    last_retry_at + chrono::Duration::seconds(delay)
}

#[derive(Clone)]
pub struct TransactionProcessor {
    pool: PgPool,
    webhook_dispatcher: Option<WebhookDispatcher>,
}

impl TransactionProcessor {
    pub fn new(pool: PgPool) -> Self {
        Self {
            pool,
            webhook_dispatcher: None,
        }
    }

    /// Attach a WebhookDispatcher so state transitions trigger outgoing webhooks.
    pub fn with_webhook_dispatcher(mut self, dispatcher: WebhookDispatcher) -> Self {
        self.webhook_dispatcher = Some(dispatcher);
        self
    }

    #[instrument(name = "processor.process_transaction", skip(self), fields(transaction.id = %tx_id))]
    pub async fn process_transaction(&self, tx_id: uuid::Uuid) -> anyhow::Result<()> {
        // Get asset_code before update for cache invalidation
        let asset_code: String =
            sqlx::query_scalar("SELECT asset_code FROM transactions WHERE id = $1")
                .bind(tx_id)
                .fetch_one(&self.pool)
                .await?;

        sqlx::query(
            "UPDATE transactions SET status = 'completed', updated_at = NOW() WHERE id = $1",
        )
        .bind(tx_id)
        .execute(&self.pool)
        .await?;

        // Invalidate cache after update
        crate::db::queries::invalidate_caches_for_asset(&asset_code).await;

        Ok(())
    }

    #[instrument(name = "processor.requeue_dlq", skip(self), fields(dlq.id = %dlq_id))]
    pub async fn requeue_dlq(&self, dlq_id: uuid::Uuid) -> anyhow::Result<()> {
        let tx_id: uuid::Uuid =
            sqlx::query_scalar("SELECT transaction_id FROM transaction_dlq WHERE id = $1")
                .bind(dlq_id)
                .fetch_one(&self.pool)
                .await?;

        // Get asset_code for cache invalidation
        let asset_code: String =
            sqlx::query_scalar("SELECT asset_code FROM transactions WHERE id = $1")
                .bind(tx_id)
                .fetch_one(&self.pool)
                .await?;

        sqlx::query("UPDATE transactions SET status = 'pending', updated_at = NOW() WHERE id = $1")
            .bind(tx_id)
            .execute(&self.pool)
            .await?;

        sqlx::query("DELETE FROM transaction_dlq WHERE id = $1")
            .bind(dlq_id)
            .execute(&self.pool)
            .await?;

        // Invalidate cache after update
        crate::db::queries::invalidate_caches_for_asset(&asset_code).await;

        Ok(())
    }

    /// Process one DLQ retry cycle: fetch due entries and retry or mark permanently failed.
    pub async fn process_dlq_retries(&self) -> anyhow::Result<()> {
        let max_retries = dlq_max_retries();
        let base_delay = dlq_base_delay_secs();
        let now = chrono::Utc::now();

        let due: Vec<TransactionDlq> = sqlx::query_as(
            r#"
            SELECT * FROM transaction_dlq
            WHERE permanently_failed = FALSE
              AND (next_retry_at IS NULL OR next_retry_at <= $1)
            ORDER BY moved_to_dlq_at ASC
            LIMIT 50
            "#,
        )
        .bind(now)
        .fetch_all(&self.pool)
        .await?;

        for entry in due {
            if let Err(e) = self.retry_dlq_entry(&entry, max_retries, base_delay).await {
                tracing::error!(dlq_id = %entry.id, "DLQ retry error: {e}");
            }
        }

        Ok(())
    }

    async fn retry_dlq_entry(
        &self,
        entry: &TransactionDlq,
        max_retries: i32,
        base_delay_secs: i64,
    ) -> anyhow::Result<()> {
        let new_retry_count = entry.retry_count + 1;
        let now = chrono::Utc::now();

        if new_retry_count > max_retries {
            tracing::warn!(
                dlq_id = %entry.id,
                transaction_id = %entry.transaction_id,
                retry_count = new_retry_count,
                error_reason = %entry.error_reason,
                "DLQ entry permanently failed after {} retries", max_retries
            );

            sqlx::query(
                "UPDATE transaction_dlq SET permanently_failed = TRUE, last_retry_at = $1 WHERE id = $2",
            )
            .bind(now)
            .bind(entry.id)
            .execute(&self.pool)
            .await?;

            // Emit webhook event for exhausted retries
            if let Some(dispatcher) = &self.webhook_dispatcher {
                let _ = dispatcher
                    .enqueue(
                        entry.transaction_id,
                        "transaction.dlq_exhausted",
                        serde_json::json!({
                            "dlq_id": entry.id,
                            "transaction_id": entry.transaction_id,
                            "error_reason": entry.error_reason,
                            "retry_count": new_retry_count,
                        }),
                    )
                    .await;
            }

            return Ok(());
        }

        tracing::info!(
            dlq_id = %entry.id,
            transaction_id = %entry.transaction_id,
            retry_count = new_retry_count,
            error_reason = %entry.error_reason,
            "Retrying DLQ entry"
        );

        // Requeue the transaction as pending
        sqlx::query(
            "UPDATE transactions SET status = 'pending', updated_at = NOW() WHERE id = $1",
        )
        .bind(entry.transaction_id)
        .execute(&self.pool)
        .await?;

        let next = next_retry_at(now, new_retry_count, base_delay_secs);

        sqlx::query(
            r#"
            UPDATE transaction_dlq
            SET retry_count = $1, last_retry_at = $2, next_retry_at = $3
            WHERE id = $4
            "#,
        )
        .bind(new_retry_count)
        .bind(now)
        .bind(next)
        .bind(entry.id)
        .execute(&self.pool)
        .await?;

        crate::db::queries::invalidate_caches_for_asset(&entry.asset_code).await;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;

    #[test]
    fn test_backoff_calculation() {
        let base = Utc::now();
        let base_delay = 60_i64;

        // retry_count=0: delay = 60 * 2^0 = 60s
        let t0 = next_retry_at(base, 0, base_delay);
        assert_eq!((t0 - base).num_seconds(), 60);

        // retry_count=1: delay = 60 * 2^1 = 120s
        let t1 = next_retry_at(base, 1, base_delay);
        assert_eq!((t1 - base).num_seconds(), 120);

        // retry_count=2: delay = 60 * 2^2 = 240s
        let t2 = next_retry_at(base, 2, base_delay);
        assert_eq!((t2 - base).num_seconds(), 240);

        // retry_count=4: delay = 60 * 2^4 = 960s
        let t4 = next_retry_at(base, 4, base_delay);
        assert_eq!((t4 - base).num_seconds(), 960);
    }
}
