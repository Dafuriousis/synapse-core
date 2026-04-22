use bigdecimal::BigDecimal;
use sqlx::migrate::Migrator;
use sqlx::PgPool;
use std::path::Path;
use std::str::FromStr;
use synapse_core::db::models::Transaction;
use synapse_core::services::TransactionProcessor;

async fn setup_db(pool: &PgPool) {
    let migrator = Migrator::new(Path::join(
        Path::new(env!("CARGO_MANIFEST_DIR")),
        "migrations",
    ))
    .await;
    if let Ok(m) = migrator {
        let _ = m.run(pool).await;
    }
}

#[tokio::test]
async fn test_dlq_workflow() {
    let database_url = match std::env::var("DATABASE_URL") {
        Ok(v) => v,
        Err(_) => {
            println!("Skipping DLQ test: DATABASE_URL not set");
            return;
        }
    };

    let pool = PgPool::connect(&database_url)
        .await
        .expect("Failed to connect to test DB");
    setup_db(&pool).await;

    // Create a test transaction
    let tx_id = uuid::Uuid::new_v4();
    let amount = BigDecimal::from_str("100.50").unwrap();

    sqlx::query(
        r#"
        INSERT INTO transactions (
            id, stellar_account, amount, asset_code, status
        ) VALUES ($1, $2, $3, $4, $5)
        "#,
    )
    .bind(tx_id)
    .bind("GABCD1234TEST")
    .bind(&amount)
    .bind("USD")
    .bind("pending")
    .execute(&pool)
    .await
    .expect("Failed to insert test transaction");

    // Process transaction (will succeed in this simple case)
    let processor = TransactionProcessor::new(pool.clone());
    let result = processor.process_transaction(tx_id).await;

    assert!(result.is_ok(), "Transaction processing should succeed");

    // Verify transaction status updated
    let tx = sqlx::query_as::<_, Transaction>("SELECT * FROM transactions WHERE id = $1")
        .bind(tx_id)
        .fetch_one(&pool)
        .await
        .expect("Failed to fetch transaction");

    assert_eq!(tx.status, "completed");

    println!("✓ DLQ workflow test passed");
}

#[tokio::test]
async fn test_requeue_dlq() {
    let database_url = match std::env::var("DATABASE_URL") {
        Ok(v) => v,
        Err(_) => {
            println!("Skipping DLQ test: DATABASE_URL not set");
            return;
        }
    };

    let pool = PgPool::connect(&database_url)
        .await
        .expect("Failed to connect to test DB");
    setup_db(&pool).await;

    // Create a test transaction
    let tx_id = uuid::Uuid::new_v4();
    let amount = BigDecimal::from_str("100.50").unwrap();

    sqlx::query(
        r#"
        INSERT INTO transactions (
            id, stellar_account, amount, asset_code, status
        ) VALUES ($1, $2, $3, $4, $5)
        "#,
    )
    .bind(tx_id)
    .bind("GABCD1234TEST")
    .bind(&amount)
    .bind("USD")
    .bind("dlq")
    .execute(&pool)
    .await
    .expect("Failed to insert test transaction");

    // Create a DLQ entry
    let dlq_id = uuid::Uuid::new_v4();
    sqlx::query(
        r#"
        INSERT INTO transaction_dlq (
            id, transaction_id, stellar_account, amount, asset_code,
            error_reason, retry_count, original_created_at
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, NOW())
        "#,
    )
    .bind(dlq_id)
    .bind(tx_id)
    .bind("GABCD1234TEST")
    .bind(&amount)
    .bind("USD")
    .bind("Test error")
    .bind(3)
    .execute(&pool)
    .await
    .expect("Failed to insert DLQ entry");

    // Requeue the DLQ entry
    let processor = TransactionProcessor::new(pool.clone());
    let result = processor.requeue_dlq(dlq_id).await;

    assert!(result.is_ok(), "Requeue should succeed");

    // Verify transaction status reset to pending
    let tx = sqlx::query_as::<_, Transaction>("SELECT * FROM transactions WHERE id = $1")
        .bind(tx_id)
        .fetch_one(&pool)
        .await
        .expect("Failed to fetch transaction");

    assert_eq!(tx.status, "pending");

    // Verify DLQ entry removed
    let dlq_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM transaction_dlq WHERE id = $1")
        .bind(dlq_id)
        .fetch_one(&pool)
        .await
        .expect("Failed to count DLQ entries");

    assert_eq!(dlq_count, 0, "DLQ entry should be removed");

    println!("✓ Requeue DLQ test passed");
}

#[test]
fn test_backoff_calculation_unit() {
    use chrono::Utc;
    use synapse_core::services::transaction_processor::next_retry_at;

    let base = Utc::now();
    let base_delay = 60_i64;

    assert_eq!((next_retry_at(base, 0, base_delay) - base).num_seconds(), 60);
    assert_eq!((next_retry_at(base, 1, base_delay) - base).num_seconds(), 120);
    assert_eq!((next_retry_at(base, 2, base_delay) - base).num_seconds(), 240);
    assert_eq!((next_retry_at(base, 3, base_delay) - base).num_seconds(), 480);
}

#[tokio::test]
async fn test_dlq_auto_retry_progression() {
    let database_url = match std::env::var("DATABASE_URL") {
        Ok(v) => v,
        Err(_) => {
            println!("Skipping DLQ auto-retry test: DATABASE_URL not set");
            return;
        }
    };

    let pool = PgPool::connect(&database_url)
        .await
        .expect("Failed to connect to test DB");
    setup_db(&pool).await;

    let tx_id = uuid::Uuid::new_v4();
    let amount = BigDecimal::from_str("50.00").unwrap();

    sqlx::query(
        "INSERT INTO transactions (id, stellar_account, amount, asset_code, status) \
         VALUES ($1, $2, $3, $4, 'dlq')",
    )
    .bind(tx_id)
    .bind("GABCD1234TEST")
    .bind(&amount)
    .bind("USD")
    .execute(&pool)
    .await
    .unwrap();

    let dlq_id = uuid::Uuid::new_v4();
    sqlx::query(
        "INSERT INTO transaction_dlq \
         (id, transaction_id, stellar_account, amount, asset_code, error_reason, retry_count, original_created_at) \
         VALUES ($1, $2, $3, $4, $5, $6, 0, NOW())",
    )
    .bind(dlq_id)
    .bind(tx_id)
    .bind("GABCD1234TEST")
    .bind(&amount)
    .bind("USD")
    .bind("transient error")
    .execute(&pool)
    .await
    .unwrap();

    let processor = TransactionProcessor::new(pool.clone());

    // First retry cycle
    processor.process_dlq_retries().await.unwrap();

    let row: (i32, bool, Option<chrono::DateTime<chrono::Utc>>) = sqlx::query_as(
        "SELECT retry_count, permanently_failed, next_retry_at FROM transaction_dlq WHERE id = $1",
    )
    .bind(dlq_id)
    .fetch_one(&pool)
    .await
    .unwrap();

    assert_eq!(row.0, 1, "retry_count should be 1");
    assert!(!row.1, "should not be permanently_failed");
    assert!(row.2.is_some(), "next_retry_at should be set");

    // Verify transaction was requeued as pending
    let status: String =
        sqlx::query_scalar("SELECT status FROM transactions WHERE id = $1")
            .bind(tx_id)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(status, "pending");
}

#[tokio::test]
async fn test_dlq_permanent_failure_after_max_retries() {
    let database_url = match std::env::var("DATABASE_URL") {
        Ok(v) => v,
        Err(_) => {
            println!("Skipping DLQ permanent failure test: DATABASE_URL not set");
            return;
        }
    };

    let pool = PgPool::connect(&database_url)
        .await
        .expect("Failed to connect to test DB");
    setup_db(&pool).await;

    let tx_id = uuid::Uuid::new_v4();
    let amount = BigDecimal::from_str("25.00").unwrap();

    sqlx::query(
        "INSERT INTO transactions (id, stellar_account, amount, asset_code, status) \
         VALUES ($1, $2, $3, $4, 'dlq')",
    )
    .bind(tx_id)
    .bind("GABCD1234TEST")
    .bind(&amount)
    .bind("USD")
    .execute(&pool)
    .await
    .unwrap();

    let dlq_id = uuid::Uuid::new_v4();
    // Insert with retry_count already at max (5)
    sqlx::query(
        "INSERT INTO transaction_dlq \
         (id, transaction_id, stellar_account, amount, asset_code, error_reason, retry_count, original_created_at) \
         VALUES ($1, $2, $3, $4, $5, $6, 5, NOW())",
    )
    .bind(dlq_id)
    .bind(tx_id)
    .bind("GABCD1234TEST")
    .bind(&amount)
    .bind("USD")
    .bind("persistent error")
    .execute(&pool)
    .await
    .unwrap();

    let processor = TransactionProcessor::new(pool.clone());
    processor.process_dlq_retries().await.unwrap();

    let permanently_failed: bool =
        sqlx::query_scalar("SELECT permanently_failed FROM transaction_dlq WHERE id = $1")
            .bind(dlq_id)
            .fetch_one(&pool)
            .await
            .unwrap();

    assert!(permanently_failed, "should be marked permanently_failed");
}
