//! Full example demonstrating all major features of `sqlx-mssql-odbc`:
//!
//! - Connection to MSSQL via ODBC
//! - Running `sqlx` migrations
//! - `query!` / `query_as!` / `query_scalar!` compile-time checked macros
//! - `FromRow` derive macro
//! - Query builder (runtime parameter binding)
//! - Transactions
//! - UUID, NVARCHAR, and DATETIME column handling
//!
//! ## Prerequisites
//!
//! Make sure the database is running and the `MSSQL_DATABASE_URL` environment
//! variable is set, e.g.:
//!
//! ```sh
//! export MSSQL_DATABASE_URL="mssql://sa:Password1!@localhost:1433/testdb"
//! ```
//!
//! For the `query!` / `query_as!` / `query_scalar!` macros to work at compile
//! time, the database must be reachable **during compilation** so that SQLx can
//! describe the query. Set `DATABASE_URL` to the same value:
//!
//! ```sh
//! export DATABASE_URL="$MSSQL_DATABASE_URL"
//! ```
//!
//! Then run the example:
//!
//! ```sh
//! cargo run --example full
//! ```

use chrono::DateTime;

use chrono::Utc;
use sqlx::Row;
use sqlx::migrate;
use sqlx_mssql_odbc::FromRow;
use sqlx_mssql_odbc::MssqlPoolOptions;
use uuid::Uuid;

// ---------------------------------------------------------------------------
// Domain types
// ---------------------------------------------------------------------------

/// A record matching the `tests` table created by the migration.
#[derive(Debug, FromRow)]
struct TestRecord {
    id: Option<Uuid>,
    test_description: Option<String>,
    test_date: DateTime<Utc>,
}

// ---------------------------------------------------------------------------
// Entrypoint
// ---------------------------------------------------------------------------

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // ---- 1. Resolve the database URL -------------------------------------
    let database_url = std::env::var("DATABASE_URL")
        .unwrap();

    println!("Connecting to: {database_url}");

    // ---- 2. Connect ------------------------------------------------------
    let  pool = MssqlPoolOptions::new()
        .connect(&database_url)
        .await
        .unwrap();
    println!("✓ Connected to MSSQL via ODBC");

    // ---- 3. Run pending migrations ---------------------------------------
    migrate!("./migrations").run(&pool).await.unwrap();
    println!("✓ Migrations applied");

    // ---- 4. Query builder (runtime, no compile-time DB needed) -----------
    let id_1 = Uuid::now_v7();
    let description_1 = "Inserted via query builder";
    let date_1 = chrono::Utc::now();

    let result =
        sqlx::query("INSERT INTO tests (id, test_description, test_date) VALUES (?, ?, ?)")
            .bind(id_1)
            .bind(description_1)
            .bind(date_1)
            .execute(&pool)
            .await?;
    println!(
        "✓ Query builder: inserted {} row(s) — id={id_1}",
        result.rows_affected(),
    );

    // ---- 5. Query builder — fetch_optional -------------------------------
    let row = sqlx::query("SELECT id, test_description, test_date FROM tests WHERE id = ?")
        .bind(id_1)
        .fetch_optional(&pool)
        .await?
        .expect("row should exist after insert");

    let fetched_id: Uuid = row.try_get("id")?;
    let fetched_desc: Option<String> = row.try_get("test_description")?;
    let fetched_date: Option<DateTime<Utc>> = row.try_get("test_date")?;
    println!(
        "✓ Query builder fetch: id={fetched_id}, desc={fetched_desc:?}, date={fetched_date:?}",
    );

    // ---- 6. query! macro (compile-time checked) --------------------------
    // Requires `DATABASE_URL` to be set at compile time.  If it's not set,
    // this will produce a compile error — that is expected.
    let row2 = sqlx_mssql_odbc::query!(
        "SELECT id, test_description, test_date FROM tests WHERE id = ?",
        id_1,
    )
    .fetch_one(&pool)
    .await?;
    println!(
        "✓ query! macro: id={}, desc={:?}, date={:?}",
        row2.id.unwrap_or_default(), row2.test_description, row2.test_date,
    );

    // ---- 7. query_scalar! macro ------------------------------------------
    let count: Option<i32> = sqlx_mssql_odbc::query_scalar!("SELECT COUNT(*) FROM tests",)
        .fetch_one(&pool)
        .await?;
    println!("✓ query_scalar! macro: {} total rows", count.unwrap_or(1));

    // ---- 8. FromRow + query_as! macro ------------------------------------
    let records = sqlx_mssql_odbc::query_as!(
        TestRecord,
        "SELECT id, test_description, test_date FROM tests ORDER BY test_date",
    )
    .fetch_all(&pool)
    .await?;
    println!("✓ query_as! macro: fetched {} record(s)", records.len());
    for (i, rec) in records.iter().enumerate() {
        println!("   [{i}] {rec:?}");
    }

    // ---- 9. Insert another record via query_as! / query ------------------
    let id_2 = Uuid::now_v7();
    let description_2 = "Inserted via query builder, second record";
    let date_2 = chrono::Utc::now().naive_utc();

    sqlx::query("INSERT INTO tests (id, test_description, test_date) VALUES (?, ?, ?)")
        .bind(id_2)
        .bind(description_2)
        .bind(date_2)
        .execute(&pool)
        .await?;
    println!("✓ Second record inserted: {id_2}");

    // ---- 10. Transactions ------------------------------------------------
    {
        let mut tx = pool.begin().await?;

        let tx_id = Uuid::now_v7();
        sqlx::query("INSERT INTO tests (id, test_description, test_date) VALUES (?, ?, ?)")
            .bind(tx_id)
            .bind("Created inside a transaction")
            .bind(chrono::Utc::now().naive_utc())
            .execute(&mut *tx)
            .await?;

        tx.commit().await?;
        println!("✓ Transaction committed: {tx_id}");
    }

    // ---- 11. Rollback demo -----------------------------------------------
    {
        let mut tx = pool.begin().await?;

        let rollback_id = Uuid::now_v7();
        sqlx::query("INSERT INTO tests (id, test_description, test_date) VALUES (?, ?, ?)")
            .bind(rollback_id)
            .bind("This will be rolled back")
            .bind(chrono::Utc::now().naive_utc())
            .execute(&mut *tx)
            .await?;

        tx.rollback().await?;
        println!("✓ Transaction rolled back: {rollback_id}");

        // Confirm the rolled-back row is not visible
        let rolled_back = sqlx::query("SELECT COUNT(*) FROM tests WHERE id = ?")
            .bind(rollback_id)
            .fetch_one(&pool)
            .await?;
        let rolled_back_count: i64 = rolled_back.try_get(0)?;
        assert_eq!(rolled_back_count, 0);
        println!("✓ Rollback verified: row is not visible");
    }

    // ---- 12. Fetch all via query builder ---------------------------------
    let all_rows =
        sqlx::query("SELECT id, test_description, test_date FROM tests ORDER BY test_date")
            .fetch_all(&pool)
            .await?;
    println!("✓ All {} row(s) in the `tests` table:", all_rows.len(),);
    for (i, r) in all_rows.iter().enumerate() {
        let id: Uuid = r.try_get("id")?;
        let desc: Option<String> = r.try_get("test_description")?;
        let date: Option<DateTime<Utc>> = r.try_get("test_date")?;
        println!("   [{i}] {id} | {desc:?} | {date:?}");
    }

    // ---- 13. Clean up ----------------------------------------------------
    pool.close().await;
    println!("✓ Connection closed");
    println!();
    println!("🎉 All example operations completed successfully!");

    Ok(())
}
