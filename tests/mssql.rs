use futures_util::TryStreamExt;
use sqlx_core::column::Column;
use sqlx_core::connection::{ConnectOptions, Connection};
use sqlx_core::executor::Executor;
use sqlx_core::row::Row;
use sqlx_core::sql_str::AssertSqlSafe;
use sqlx_core::statement::Statement;
use sqlx_core::value::ValueRef;
use sqlx_core::Either;
use sqlx_mssql_odbc::{MssqlConnectOptions, MssqlConnection, MssqlPoolOptions};
use std::str::FromStr;
use std::sync::atomic::{AtomicU64, Ordering};
static TABLE_ID: AtomicU64 = AtomicU64::new(0);
const MISSING_TABLE_READ: &str = "SELECT contents FROM sqlx_missing_fs WHERE path = ?";
const MISSING_TABLE_EXISTS: &str = "SELECT 1 FROM sqlx_missing_fs WHERE path = ?";
const MISSING_TABLE_MODIFIED: &str =
    "SELECT 1 FROM sqlx_missing_fs WHERE last_modified >= ? AND path = ?";

fn database_url(test_name: &str) -> Option<String> {
    match std::env::var("MSSQL_DATABASE_URL") {
        Ok(value) if !value.trim().is_empty() => Some(value),
        _ => {
            if std::env::var_os("MSSQL_TEST_REQUIRED").is_some() {
                panic!("{test_name} requires MSSQL_DATABASE_URL, but it is not set");
            }

            eprintln!("skipping {test_name}: MSSQL_DATABASE_URL is not set");
            None
        }
    }
}

fn get_blocking_test_conn(
    test_name: &str,
) -> Result<Option<MssqlConnection>, Box<dyn std::error::Error>> {
    let Some(url) = database_url(test_name) else {
        return Ok(None);
    };

    let options = MssqlConnectOptions::from_str(&url)?;
    Ok(Some(options.connect_blocking()?))
}

async fn get_test_conn(
    test_name: &str,
) -> Result<Option<MssqlConnection>, Box<dyn std::error::Error>> {
    let Some(url) = database_url(test_name) else {
        return Ok(None);
    };

    Ok(Some(MssqlConnection::connect(&url).await?))
}

async fn get_test_conn_with<F>(
    test_name: &str,
    configure: F,
) -> Result<Option<MssqlConnection>, Box<dyn std::error::Error>>
where
    F: FnOnce(&mut MssqlConnectOptions),
{
    let Some(url) = database_url(test_name) else {
        return Ok(None);
    };

    let mut options = MssqlConnectOptions::from_str(&url)?;
    configure(&mut options);

    Ok(Some(options.connect().await?))
}

fn test_table_name(prefix: &str) -> String {
    let id = TABLE_ID.fetch_add(1, Ordering::Relaxed);
    format!("sqlx_mssql_{prefix}_{}_{}", std::process::id(), id)
}

async fn drop_table_if_exists(
    conn: &mut MssqlConnection,
    table: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let sql = format!("DROP TABLE IF EXISTS {table}");
    sqlx_core::query::query(AssertSqlSafe(sql))
        .execute(conn)
        .await?;
    Ok(())
}

async fn count_rows(
    conn: &mut MssqlConnection,
    table: &str,
) -> Result<i64, Box<dyn std::error::Error>> {
    let sql = format!("SELECT COUNT(*) FROM {table}");
    let row = sqlx_core::query::query(AssertSqlSafe(sql))
        .fetch_one(conn)
        .await?;
    Ok(row.try_get::<i64, _>(0)?)
}

#[test]
fn mssql_url_parses_correctly() {
    let url = "mssql://sa:Password1!@server.example.com:1433/testdb";
    let options = MssqlConnectOptions::from_str(url).unwrap();
    let cs = options.connection_string();
    assert!(cs.contains("Driver={ODBC Driver 18 for SQL Server}"));
    assert!(cs.contains("Server=server.example.com,1433"));
    assert!(cs.contains("Database=testdb"));
    assert!(cs.contains("UID=sa"));
    assert!(cs.contains("PWD=Password1!"));
}

#[test]
fn integration_connection_string_forms_parse() {
    let dsn = MssqlConnectOptions::from_str("ExampleDsn").unwrap();
    assert_eq!(dsn.connection_string(), "DSN=ExampleDsn");

    let conn_str = MssqlConnectOptions::from_str("DSN=ExampleDsn;UID=user").unwrap();
    assert_eq!(conn_str.connection_string(), "DSN=ExampleDsn;UID=user");

    let legacy = MssqlConnectOptions::from_str("odbc:DSN=ExampleDsn").unwrap();
    assert_eq!(legacy.connection_string(), "DSN=ExampleDsn");
}

#[test]
fn connect_and_ping_when_configured() -> Result<(), Box<dyn std::error::Error>> {
    let Some(mut conn) = get_blocking_test_conn("MSSQL blocking connection test")? else {
        return Ok(());
    };

    conn.ping_blocking()?;
    let _dbms_name = conn.dbms_name()?;

    Ok(())
}

#[tokio::test]
async fn sqlx_connection_connect_ping_and_transaction_when_configured(
) -> Result<(), Box<dyn std::error::Error>> {
    let Some(mut conn) = get_test_conn("MSSQL SQLx connection test").await? else {
        return Ok(());
    };

    conn.ping().await?;

    let tx = conn.begin().await?;
    tx.rollback().await?;

    conn.close().await?;
    Ok(())
}

#[tokio::test]
async fn sqlx_pool_acquires_and_queries_when_configured() -> Result<(), Box<dyn std::error::Error>>
{
    let Some(url) = database_url("MSSQL pool test") else {
        return Ok(());
    };

    let pool = MssqlPoolOptions::new()
        .max_connections(2)
        .connect(&url)
        .await?;
    let row = sqlx_core::query::query("SELECT 1 AS value")
        .fetch_one(&pool)
        .await?;
    assert_eq!(row.try_get::<i32, _>("value")?, 1);

    pool.close().await;
    Ok(())
}

#[tokio::test]
async fn sqlx_query_fetches_basic_row_when_configured() -> Result<(), Box<dyn std::error::Error>> {
    let Some(mut conn) = get_test_conn("MSSQL SQLx row fetch test").await? else {
        return Ok(());
    };

    let row = sqlx_core::query::query("SELECT 1 AS answer")
        .fetch_one(&mut conn)
        .await?;
    let value = ValueRef::to_owned(&row.try_get_raw(0)?);
    assert_eq!(value.as_i64(), Some(1));
    assert_eq!(row.try_get::<i32, _>("answer")?, 1);
    assert_eq!(row.try_get::<i32, _>("ANSWER")?, 1);

    conn.close().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sqlx_runs_independent_connections_in_parallel_when_configured(
) -> Result<(), Box<dyn std::error::Error>> {
    let Some(url) = database_url("MSSQL async parallelism test") else {
        return Ok(());
    };

    let mut tasks = Vec::new();

    for expected in 0_i32..8 {
        let url = url.clone();
        tasks.push(tokio::spawn(async move {
            let mut conn = MssqlConnection::connect(&url).await.map_err(|error| {
                format!("failed to connect ODBC parallel task {expected}: {error}")
            })?;
            let row = sqlx_core::query::query("SELECT CAST(? AS INTEGER)")
                .bind(expected)
                .fetch_one(&mut conn)
                .await
                .map_err(|error| format!("parallel ODBC query {expected} failed: {error}"))?;
            let actual = row
                .try_get::<i32, _>(0)
                .map_err(|error| format!("parallel ODBC decode {expected} failed: {error}"))?;
            conn.close().await.map_err(|error| {
                format!("failed to close ODBC parallel task {expected}: {error}")
            })?;

            if actual != expected {
                return Err(format!(
                    "parallel ODBC task returned {actual}, expected {expected}"
                ));
            }

            Ok::<(), String>(())
        }));
    }

    for task in tasks {
        if let Err(message) = task.await? {
            return Err(std::io::Error::other(message).into());
        }
    }

    Ok(())
}

#[tokio::test]
async fn dropping_large_row_stream_keeps_connection_usable_when_configured(
) -> Result<(), Box<dyn std::error::Error>> {
    let Some(mut conn) = get_test_conn("MSSQL early stream drop test").await? else {
        return Ok(());
    };

    let table = test_table_name("early_drop");
    drop_table_if_exists(&mut conn, &table).await?;
    let create = format!("CREATE TABLE {table} (id INTEGER)");
    sqlx_core::query::query(AssertSqlSafe(create))
        .execute(&mut conn)
        .await?;

    let insert = format!("INSERT INTO {table} (id) VALUES (?)");
    for id in 0_i32..128 {
        sqlx_core::query::query(AssertSqlSafe(insert.as_str()))
            .bind(id)
            .execute(&mut conn)
            .await?;
    }

    {
        let select = format!("SELECT id FROM {table} ORDER BY id");
        let mut rows = sqlx_core::query::query(AssertSqlSafe(select)).fetch(&mut conn);
        let first = rows
            .try_next()
            .await?
            .expect("large ODBC stream should yield at least one row");
        assert_eq!(first.try_get::<i32, _>(0)?, 0);
    }

    let row = sqlx_core::query::query("SELECT 1")
        .fetch_one(&mut conn)
        .await?;
    assert_eq!(row.try_get::<i32, _>(0)?, 1);

    drop_table_if_exists(&mut conn, &table).await?;
    conn.close().await?;
    Ok(())
}

#[tokio::test]
async fn sqlx_fetch_many_ends_rows_with_query_result_when_configured(
) -> Result<(), Box<dyn std::error::Error>> {
    let Some(mut conn) = get_test_conn("MSSQL SQLx fetch_many result test").await? else {
        return Ok(());
    };

    let results: Vec<_> = (&mut conn)
        .fetch_many(sqlx_core::query::query("SELECT 1"))
        .try_collect()
        .await?;

    assert_eq!(results.len(), 2);
    let Either::Right(row) = &results[0] else {
        panic!("first fetch_many item should be a row");
    };
    assert_eq!(row.try_get::<i32, _>(0)?, 1);
    let Either::Left(result) = &results[1] else {
        panic!("last fetch_many item should be a query result");
    };
    assert_eq!(result.rows_affected(), 0);

    conn.close().await?;
    Ok(())
}

#[tokio::test]
async fn sqlx_streams_multiple_rows_when_configured() -> Result<(), Box<dyn std::error::Error>> {
    let Some(mut conn) = get_test_conn("MSSQL SQLx multiple rows test").await? else {
        return Ok(());
    };

    let rows = sqlx_core::query::query("SELECT 1 AS v UNION ALL SELECT 2 UNION ALL SELECT 3")
        .fetch_all(&mut conn)
        .await?;
    let values = rows
        .iter()
        .map(|row| row.try_get::<i32, _>(0))
        .collect::<Result<Vec<_>, _>>()?;

    assert_eq!(values, vec![1, 2, 3]);

    conn.close().await?;
    Ok(())
}

#[tokio::test]
async fn sqlx_fetch_optional_returns_none_for_empty_result_when_configured(
) -> Result<(), Box<dyn std::error::Error>> {
    let Some(mut conn) = get_test_conn("MSSQL SQLx empty result test").await? else {
        return Ok(());
    };

    let row = sqlx_core::query::query("SELECT 1 WHERE 1 = 0")
        .fetch_optional(&mut conn)
        .await?;
    assert!(row.is_none());

    conn.close().await?;
    Ok(())
}



#[tokio::test]
async fn sqlx_query_fetches_basic_row_in_buffered_mode_when_configured(
) -> Result<(), Box<dyn std::error::Error>> {
    let Some(mut conn) = get_test_conn_with("MSSQL SQLx buffered row fetch test", |options| {
        options.batch_size(2).max_column_size(Some(64));
    })
    .await?
    else {
        return Ok(());
    };

    let row = sqlx_core::query::query("SELECT 1")
        .fetch_one(&mut conn)
        .await?;
    assert_eq!(row.try_get::<i32, _>(0)?, 1);

    conn.close().await?;
    Ok(())
}

#[tokio::test]
async fn sqlx_query_decodes_decimal_integer_when_configured(
) -> Result<(), Box<dyn std::error::Error>> {
    let Some(mut conn) = get_test_conn("MSSQL SQLx decimal integer decode test").await? else {
        return Ok(());
    };

    let row = sqlx_core::query::query("SELECT CAST(42 AS DECIMAL(10, 0))")
        .fetch_one(&mut conn)
        .await?;
    assert_eq!(row.try_get::<i32, _>(0)?, 42);

    conn.close().await?;
    Ok(())
}

#[tokio::test]
async fn sqlx_query_decodes_decimal_integer_in_buffered_mode_when_configured(
) -> Result<(), Box<dyn std::error::Error>> {
    let Some(mut conn) = get_test_conn_with("MSSQL SQLx buffered decimal decode test", |options| {
        options.batch_size(2).max_column_size(Some(64));
    })
    .await?
    else {
        return Ok(());
    };

    let row = sqlx_core::query::query("SELECT CAST(42 AS DECIMAL(10, 0))")
        .fetch_one(&mut conn)
        .await?;
    assert_eq!(row.try_get::<i32, _>(0)?, 42);

    conn.close().await?;
    Ok(())
}

#[tokio::test]
async fn sqlx_query_binds_parameter_when_configured() -> Result<(), Box<dyn std::error::Error>> {
    let Some(mut conn) = get_test_conn("MSSQL SQLx parameter binding test").await? else {
        return Ok(());
    };

    let row = sqlx_core::query::query("SELECT CAST(? AS INTEGER)")
        .bind(7_i32)
        .fetch_one(&mut conn)
        .await?;
    assert_eq!(row.try_get::<i32, _>(0)?, 7);

    conn.close().await?;
    Ok(())
}

#[tokio::test]
async fn sqlx_query_binds_heterogeneous_parameters_when_configured(
) -> Result<(), Box<dyn std::error::Error>> {
    let Some(mut conn) = get_test_conn("MSSQL SQLx heterogeneous parameter binding test").await?
    else {
        return Ok(());
    };

    let row = sqlx_core::query::query(
        "SELECT CAST(? AS INTEGER), CAST(? AS VARCHAR(32)), CAST(? AS DOUBLE PRECISION)",
    )
    .bind(7_i32)
    .bind("odbc-param")
    .bind(2.5_f64)
    .fetch_one(&mut conn)
    .await?;

    assert_eq!(row.try_get::<i32, _>(0)?, 7);
    assert_eq!(row.try_get::<String, _>(1)?.trim_end(), "odbc-param");
    assert_eq!(row.try_get::<f64, _>(2)?, 2.5);

    conn.close().await?;
    Ok(())
}

#[tokio::test]
async fn sqlx_query_binds_typed_null_when_configured() -> Result<(), Box<dyn std::error::Error>> {
    let Some(mut conn) = get_test_conn("MSSQL SQLx typed null binding test").await? else {
        return Ok(());
    };

    let row = sqlx_core::query::query("SELECT CAST(? AS INTEGER)")
        .bind(Option::<i32>::None)
        .fetch_one(&mut conn)
        .await?;
    assert!(row.try_get_raw(0)?.is_null());

    conn.close().await?;
    Ok(())
}

#[tokio::test]
async fn sqlx_execute_reports_rows_affected_when_configured(
) -> Result<(), Box<dyn std::error::Error>> {
    let Some(mut conn) = get_test_conn("MSSQL SQLx rows affected test").await? else {
        return Ok(());
    };

    let table = test_table_name("rows_affected");
    drop_table_if_exists(&mut conn, &table).await?;
    let create = format!("CREATE TABLE {table} (id INTEGER)");
    sqlx_core::query::query(AssertSqlSafe(create))
        .execute(&mut conn)
        .await?;

    let insert = format!("INSERT INTO {table} (id) VALUES (?)");
    let result = sqlx_core::query::query(AssertSqlSafe(insert.as_str()))
        .bind(1_i32)
        .execute(&mut conn)
        .await?;
    assert_eq!(result.rows_affected(), 1);

    let update = format!("UPDATE {table} SET id = id + 10 WHERE id = ?");
    let result = sqlx_core::query::query(AssertSqlSafe(update))
        .bind(1_i32)
        .execute(&mut conn)
        .await?;
    assert_eq!(result.rows_affected(), 1);

    let delete = format!("DELETE FROM {table} WHERE id = ?");
    let result = sqlx_core::query::query(AssertSqlSafe(delete))
        .bind(11_i32)
        .execute(&mut conn)
        .await?;
    assert_eq!(result.rows_affected(), 1);

    assert_eq!(count_rows(&mut conn, &table).await?, 0);
    drop_table_if_exists(&mut conn, &table).await?;

    conn.close().await?;
    Ok(())
}

#[tokio::test]
async fn sqlx_transactions_commit_and_rollback_data_when_configured(
) -> Result<(), Box<dyn std::error::Error>> {
    let Some(mut conn) = get_test_conn("MSSQL SQLx transaction data test").await? else {
        return Ok(());
    };

    let table = test_table_name("transactions");
    drop_table_if_exists(&mut conn, &table).await?;
    let create = format!("CREATE TABLE {table} (id INTEGER)");
    sqlx_core::query::query(AssertSqlSafe(create))
        .execute(&mut conn)
        .await?;

    let insert = format!("INSERT INTO {table} (id) VALUES (?)");
    let mut tx = conn.begin().await?;
    sqlx_core::query::query(AssertSqlSafe(insert.as_str()))
        .bind(1_i32)
        .execute(&mut *tx)
        .await?;
    tx.rollback().await?;
    assert_eq!(count_rows(&mut conn, &table).await?, 0);

    let mut tx = conn.begin().await?;
    sqlx_core::query::query(AssertSqlSafe(insert.as_str()))
        .bind(2_i32)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    assert_eq!(count_rows(&mut conn, &table).await?, 1);

    drop_table_if_exists(&mut conn, &table).await?;
    conn.close().await?;
    Ok(())
}

#[tokio::test]
async fn dropped_transaction_rolls_back_when_configured() -> Result<(), Box<dyn std::error::Error>>
{
    let Some(mut conn) = get_test_conn("MSSQL dropped transaction rollback test").await? else {
        return Ok(());
    };

    let table = test_table_name("dropped_tx");
    drop_table_if_exists(&mut conn, &table).await?;
    let create = format!("CREATE TABLE {table} (id INTEGER NOT NULL)");
    sqlx_core::query::query(AssertSqlSafe(create))
        .execute(&mut conn)
        .await?;

    {
        let mut tx = conn.begin().await?;
        let insert = format!("INSERT INTO {table} (id) VALUES (?)");
        sqlx_core::query::query(AssertSqlSafe(insert))
            .bind(1_i32)
            .execute(&mut *tx)
            .await?;
    }

    assert_eq!(count_rows(&mut conn, &table).await?, 0);

    drop_table_if_exists(&mut conn, &table).await?;
    conn.close().await?;
    Ok(())
}

#[tokio::test]
async fn sqlx_prepare_reports_basic_metadata_when_configured(
) -> Result<(), Box<dyn std::error::Error>> {
    let Some(mut conn) = get_test_conn("MSSQL SQLx prepare metadata test").await? else {
        return Ok(());
    };

    let statement = (&mut conn)
        .prepare(sqlx_core::sql_str::SqlStr::from_static(
            "SELECT CAST(? AS INTEGER) AS answer",
        ))
        .await?;

    assert_eq!(statement.parameters(), Some(sqlx_core::Either::Right(1)));
    if let Some(column) = statement.columns().first() {
        assert_eq!(sqlx_core::column::Column::name(column), "answer");
    }

    let row = sqlx_core::query::query("SELECT CAST(? AS INTEGER) AS answer")
        .bind(7_i32)
        .fetch_one(&mut conn)
        .await?;
    assert_eq!(row.try_get::<i32, _>(0)?, 7);

    conn.close().await?;
    Ok(())
}

#[tokio::test]
async fn sqlx_prepare_then_statement_query_when_configured(
) -> Result<(), Box<dyn std::error::Error>> {
    let Some(mut conn) = get_test_conn("MSSQL statement query test").await? else {
        return Ok(());
    };

    let statement = (&mut conn)
        .prepare(sqlx_core::sql_str::SqlStr::from_static(
            "SELECT CAST(? AS INTEGER) AS answer",
        ))
        .await?;
    let row = statement.query().bind(11_i32).fetch_one(&mut conn).await?;

    assert!(
        row.column(0).name().eq_ignore_ascii_case("answer"),
        "unexpected ODBC column name: {}",
        row.column(0).name()
    );
    assert_eq!(row.try_get::<i32, _>(0)?, 11);

    conn.close().await?;
    Ok(())
}

#[tokio::test]
async fn wrong_parameter_count_errors_when_configured() -> Result<(), Box<dyn std::error::Error>> {
    let Some(mut conn) = get_test_conn("MSSQL wrong parameter count test").await? else {
        return Ok(());
    };

    let error = sqlx_core::query::query("SELECT ? AS value")
        .fetch_one(&mut conn)
        .await
        .expect_err("query with a missing bind parameter should fail");
    assert!(
        matches!(
            error,
            sqlx_core::error::Error::Database(_)
                | sqlx_core::error::Error::Protocol(_)
                | sqlx_core::error::Error::Encode(_)
        ),
        "{error:?} should report a normal parameter-count failure"
    );

    conn.close().await?;
    Ok(())
}

#[tokio::test]
async fn statement_cache_is_bounded_and_clearable_when_configured(
) -> Result<(), Box<dyn std::error::Error>> {
    let Some(mut conn) = get_test_conn_with("MSSQL statement cache test", |options| {
        options.statement_cache_capacity(1);
    })
    .await?
    else {
        return Ok(());
    };

    (&mut conn)
        .prepare(sqlx_core::sql_str::SqlStr::from_static("SELECT 1"))
        .await?;
    assert_eq!(conn.cached_statements_size(), 1);

    (&mut conn)
        .prepare(sqlx_core::sql_str::SqlStr::from_static("SELECT 2"))
        .await?;
    assert_eq!(conn.cached_statements_size(), 1);

    conn.clear_cached_statements().await?;
    assert_eq!(conn.cached_statements_size(), 0);

    conn.close().await?;
    Ok(())
}

#[tokio::test]
async fn prepare_missing_table_does_not_return_empty_metadata_when_configured(
) -> Result<(), Box<dyn std::error::Error>> {
    let Some(mut conn) = get_test_conn("MSSQL missing-table prepare metadata test").await? else {
        return Ok(());
    };

    for sql in [
        MISSING_TABLE_READ,
        MISSING_TABLE_EXISTS,
        MISSING_TABLE_MODIFIED,
    ] {
        if let Ok(statement) = (&mut conn)
            .prepare(sqlx_core::sql_str::SqlStr::from_static(sql))
            .await
        {
            assert!(
                !statement.columns().is_empty(),
                "MSSQL prepare must not turn a metadata error into zero columns for {sql}"
            );
        }
    }

    conn.close().await?;
    Ok(())
}

#[tokio::test]
async fn failed_metadata_prepare_does_not_poison_later_execute_when_configured(
) -> Result<(), Box<dyn std::error::Error>> {
    let Some(mut conn) = get_test_conn("MSSQL failed metadata prepare recovery test").await? else {
        return Ok(());
    };

    let _ = (&mut conn)
        .prepare(sqlx_core::sql_str::SqlStr::from_static(MISSING_TABLE_READ))
        .await;

    let error = sqlx_core::query::query(MISSING_TABLE_READ)
        .bind("index.sql")
        .fetch_optional(&mut conn)
        .await
        .expect_err("querying a missing table should fail");
    let message = error.to_string();

    assert!(
        message.contains("sqlx_missing_fs"),
        "failed ODBC prepare metadata poisoned later execute instead of returning a normal missing-table error: {message}"
    );

    conn.close().await?;
    Ok(())
}

#[tokio::test]
async fn invalid_query_errors_are_reported_as_database_errors_when_configured(
) -> Result<(), Box<dyn std::error::Error>> {
    let Some(mut conn) = get_test_conn("MSSQL invalid query error test").await? else {
        return Ok(());
    };

    let error = sqlx_core::query::query("SELECT * FROM sqlx_missing_fs")
        .fetch_optional(&mut conn)
        .await
        .expect_err("fetching from a missing table should fail");

    assert!(
        matches!(error, sqlx_core::error::Error::Database(_)),
        "{error:?} should be a database error"
    );

    let error =
        sqlx_core::query::query("SELECT non_existent_column FROM (SELECT 1 AS existing_column) t")
            .fetch_optional(&mut conn)
            .await
            .expect_err("fetching a missing column should fail");

    assert!(
        matches!(error, sqlx_core::error::Error::Database(_)),
        "{error:?} should be a database error"
    );

    conn.close().await?;
    Ok(())
}


