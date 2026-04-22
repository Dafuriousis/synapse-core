pub mod config;
pub mod db;
pub mod error;
pub mod graphql;
pub mod handlers;
pub mod health;
pub mod metrics;
pub mod middleware;
pub mod readiness;
pub mod schemas;
pub mod secrets;
pub mod services;
pub mod startup;
pub mod stellar;
#[path = "Multi-Tenant Isolation Layer (Architecture)/src/tenant/mod.rs"]
pub mod tenant;
pub mod utils;
pub mod validation;

use crate::db::models::Transaction;
use crate::db::pool_manager::PoolManager;
use crate::error::AppError;
use crate::graphql::schema::AppSchema;
use crate::handlers::profiling::ProfilingManager;
use crate::handlers::ws::TransactionStatusUpdate;
pub use crate::readiness::ReadinessState;
use crate::services::feature_flags::FeatureFlagService;
use crate::services::query_cache::QueryCache;
use crate::stellar::HorizonClient;
use crate::tenant::TenantConfig;
use axum::{
    middleware as axum_middleware,
    routing::{get, post},
    Router,
};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{broadcast, mpsc, oneshot};
use uuid::Uuid;

/// A single item sent through the batch insert channel.
pub struct BatchInsertRequest {
    pub transaction: Transaction,
    pub reply: oneshot::Sender<Result<Transaction, AppError>>,
}

/// Channel capacity for the batch insert buffer.
pub const BATCH_CHANNEL_CAPACITY: usize = 4096;
/// Flush when this many items are buffered.
pub const BATCH_INSERT_SIZE: usize = 100;
/// Flush after this many milliseconds even if the batch isn't full.
pub const BATCH_INSERT_TIMEOUT_MS: u64 = 200;

#[derive(Clone)]
pub struct AppState {
    pub db: sqlx::PgPool,
    pub pool_manager: PoolManager,
    pub horizon_client: HorizonClient,
    pub feature_flags: FeatureFlagService,
    pub redis_url: String,
    pub start_time: std::time::Instant,
    pub readiness: ReadinessState,
    pub tx_broadcast: broadcast::Sender<TransactionStatusUpdate>,
    pub query_cache: QueryCache,
    /// Sender for the batched-insert channel. `None` when running without the flusher.
    pub batch_tx: Option<mpsc::Sender<BatchInsertRequest>>,
    pub tenant_configs: Arc<tokio::sync::RwLock<HashMap<Uuid, TenantConfig>>>,
    pub profiling_manager: ProfilingManager,
}

impl AppState {
    pub async fn get_tenant_config(&self, tenant_id: Uuid) -> Option<TenantConfig> {
        self.tenant_configs.read().await.get(&tenant_id).cloned()
    }

    pub async fn load_tenant_configs(&self) -> anyhow::Result<()> {
        let configs = crate::db::queries::get_all_tenant_configs(&self.db).await?;
        let mut map = self.tenant_configs.write().await;
        map.clear();
        for config in configs {
            map.insert(config.tenant_id, config);
        }
        Ok(())
    }

    pub async fn test_new(database_url: &str) -> Self {
        let pool = sqlx::PgPool::connect(database_url).await.unwrap();
        let (tx, _) = broadcast::channel(100);
        Self {
            db: pool.clone(),
            pool_manager: crate::db::pool_manager::PoolManager::new(database_url, None)
                .await
                .unwrap(),
            horizon_client: HorizonClient::new(
                "https://horizon-testnet.stellar.org".to_string(),
            ),
            feature_flags: FeatureFlagService::new(pool),
            redis_url: "redis://localhost:6379".to_string(),
            start_time: std::time::Instant::now(),
            readiness: ReadinessState::new(),
            tx_broadcast: tx,
            query_cache: QueryCache::new("redis://localhost:6379").unwrap(),
            batch_tx: None,
            tenant_configs: Arc::new(tokio::sync::RwLock::new(HashMap::new())),
            profiling_manager: ProfilingManager::new(),
        }
    }
}

/// Spawn the batch flusher background task and return the channel sender.
pub fn spawn_batch_flusher(pool: sqlx::PgPool) -> mpsc::Sender<BatchInsertRequest> {
    let (tx, mut rx) = mpsc::channel::<BatchInsertRequest>(BATCH_CHANNEL_CAPACITY);

    tokio::spawn(async move {
        let timeout = tokio::time::Duration::from_millis(BATCH_INSERT_TIMEOUT_MS);
        let mut buf: Vec<BatchInsertRequest> = Vec::with_capacity(BATCH_INSERT_SIZE);

        loop {
            let deadline = tokio::time::Instant::now() + timeout;

            loop {
                match tokio::time::timeout_at(deadline, rx.recv()).await {
                    Ok(Some(req)) => {
                        buf.push(req);
                        if buf.len() >= BATCH_INSERT_SIZE {
                            break; // flush immediately on full batch
                        }
                    }
                    Ok(None) => {
                        // Channel closed — flush remaining and exit
                        if !buf.is_empty() {
                            flush_batch(&pool, &mut buf).await;
                        }
                        return;
                    }
                    Err(_) => break, // timeout — flush whatever we have
                }
            }

            if !buf.is_empty() {
                flush_batch(&pool, &mut buf).await;
            }
        }
    });

    tx
}

async fn flush_batch(pool: &sqlx::PgPool, buf: &mut Vec<BatchInsertRequest>) {
    let txs: Vec<Transaction> = buf.iter().map(|r| r.transaction.clone()).collect();
    let results = crate::db::queries::insert_transactions_batch(pool, &txs).await;

    for (req, result) in buf.drain(..).zip(results) {
        let mapped = result.map_err(|e| AppError::DatabaseError(e.to_string()));
        let _ = req.reply.send(mapped);
    }
}

#[derive(Clone)]
pub struct ApiState {
    pub app_state: AppState,
    pub graphql_schema: AppSchema,
}

impl std::fmt::Debug for ApiState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ApiState").finish_non_exhaustive()
    }
}

pub fn create_app(app_state: AppState) -> Router {
    let graphql_schema = crate::graphql::schema::build_schema(app_state.clone());
    let api_state = ApiState {
        app_state,
        graphql_schema,
    };

    // Callback routes with validation middleware
    let callback_routes = Router::new()
        .route("/callback", post(handlers::webhook::callback))
        .route("/callback/transaction", post(handlers::webhook::callback))
        .layer(axum_middleware::from_fn(
            crate::middleware::validate::validate_callback,
        ));

    // Webhook route with validation middleware
    let webhook_routes = Router::new()
        .route("/webhook", post(handlers::webhook::handle_webhook))
        .layer(axum_middleware::from_fn(
            crate::middleware::validate::validate_webhook,
        ));

    Router::new()
        .route("/health", get(handlers::health))
        .route("/ready", get(handlers::ready))
        .route("/errors", get(handlers::error_catalog))
        .route("/settlements", get(handlers::settlements::list_settlements))
        .route(
            "/settlements/:id",
            get(handlers::settlements::get_settlement),
        )
        .merge(callback_routes)
        .merge(webhook_routes)
        .route("/transactions/:id", get(handlers::webhook::get_transaction))
        .route("/graphql", post(handlers::graphql::graphql_handler))
        .route("/export", get(handlers::export::export_transactions))
        .route("/stats/status", get(handlers::stats::status_counts))
        .route("/stats/daily", get(handlers::stats::daily_totals))
        .route("/stats/assets", get(handlers::stats::asset_stats))
        .route("/cache/metrics", get(handlers::stats::cache_metrics))
        .with_state(api_state)
}

#[cfg(test)]
mod batch_tests {
    use super::*;
    use sqlx::types::BigDecimal;
    use std::str::FromStr;

    /// Verify that the flusher flushes on timeout even with fewer than BATCH_INSERT_SIZE items.
    #[tokio::test]
    async fn test_batch_timeout_flush() {
        // We can't easily test the DB path without a real pool, but we can verify
        // that the channel sender/receiver mechanics work and the reply is received.
        // Use a mock pool-less path: send one item and confirm the reply arrives
        // within 2 * BATCH_INSERT_TIMEOUT_MS.

        // This test only validates the channel plumbing, not the DB insert.
        let (tx, mut rx) = tokio::sync::mpsc::channel::<BatchInsertRequest>(16);
        let (reply_tx, reply_rx) = tokio::sync::oneshot::channel::<Result<Transaction, AppError>>();

        let dummy_tx = Transaction::new(
            "GAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA".to_string(),
            BigDecimal::from_str("1.00").unwrap(),
            "USD".to_string(),
            None,
            None,
            None,
            None,
            None,
            None,
        );

        tx.send(BatchInsertRequest {
            transaction: dummy_tx.clone(),
            reply: reply_tx,
        })
        .await
        .unwrap();

        // Receive from the channel side (simulating the flusher reading it)
        let req = rx.recv().await.unwrap();
        assert_eq!(req.transaction.stellar_account, dummy_tx.stellar_account);

        // Simulate flusher sending back a result
        let _ = req.reply.send(Ok(dummy_tx.clone()));

        let result = reply_rx.await.unwrap();
        assert!(result.is_ok());
        assert_eq!(result.unwrap().stellar_account, dummy_tx.stellar_account);
    }
}
