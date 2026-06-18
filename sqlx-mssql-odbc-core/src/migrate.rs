//! Migration support for MSSQL via ODBC.
//!
//! Implements [`MigrateDatabase`] for [`Mssql`] (database lifecycle) and
//! [`Migrate`] for [`MssqlConnection`] (migration execution and tracking)
//! so that [`Migrator`](sqlx_core::migrate::Migrator) works with this driver.

use crate::connection::offload_blocking;
use crate::{Mssql, MssqlConnection, MssqlConnectOptions};
use futures_core::future::BoxFuture;
use odbc_api::{Cursor, Nullable};
use sqlx_core::error::Error;
use sqlx_core::migrate::{AppliedMigration, Migrate, MigrateDatabase, MigrateError, Migration};
use std::str::FromStr;
use std::time::Duration;
use url::Url;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Extracts the database name from a `mssql://` URL.
fn extract_database_name(url: &str) -> std::result::Result<String, Error> {
    let parsed = Url::parse(url).map_err(|e| {
        Error::Protocol(format!("failed to parse migration URL: {e}"))
    })?;
    let database = parsed.path().trim_start_matches('/').to_owned();
    if database.is_empty() {
        return Err(Error::Configuration(
            "migration URL does not contain a database name".into(),
        ));
    }
    Ok(database)
}

/// Escapes a value for use inside square brackets in T-SQL.
fn escape_sql_bracket(value: &str) -> String {
    value.replace(']', "]]")
}

/// Escapes a string for use inside a `N'...'` T-SQL string literal.
fn escape_sql_string(value: &str) -> String {
    value.replace('\'', "''")
}

/// Formats a byte slice as a T-SQL hex literal (e.g. `0xDEADBEEF`).
fn format_hex(bytes: &[u8]) -> String {
    let mut hex = String::with_capacity(2 + bytes.len() * 2);
    hex.push_str("0x");
    for byte in bytes {
        hex.push_str(&format!("{byte:02X}"));
    }
    hex
}

/// Splits a potentially schema-qualified table name into (schema, table).
/// If no schema is present, defaults to the empty string (caller uses the
/// name as-is).
fn split_table_name(table_name: &str) -> (&str, &str) {
    if let Some(dot) = table_name.find('.') {
        let schema = &table_name[..dot];
        let table = &table_name[dot + 1..];
        (schema, table)
    } else {
        ("", table_name)
    }
}

/// Builds a safe `[schema].[table]` reference.
fn quoted_table_name(table_name: &str) -> String {
    let (schema, table) = split_table_name(table_name);
    if schema.is_empty() {
        format!("[{}]", escape_sql_bracket(table))
    } else {
        format!(
            "[{}].[{}]",
            escape_sql_bracket(schema),
            escape_sql_bracket(table),
        )
    }
}

// ---------------------------------------------------------------------------
// MigrateDatabase — database lifecycle (create / drop / exists)
// ---------------------------------------------------------------------------

impl MigrateDatabase for Mssql {
    fn create_database(url: &str) -> impl std::future::Future<Output = Result<(), Error>> + Send + '_ {
        async move {
            let options = MssqlConnectOptions::from_str(url)?;
            let database = extract_database_name(url)?;
            let master_options = options.with_database("master");
            let conn = MssqlConnection::connect_blocking(&master_options)?;
            conn.exec_sql_blocking(&format!(
                "CREATE DATABASE [{}]",
                escape_sql_bracket(&database),
            ))?;
            drop(conn);
            Ok(())
        }
    }

    fn database_exists(url: &str) -> impl std::future::Future<Output = Result<bool, Error>> + Send + '_ {
        async move {
            let options = MssqlConnectOptions::from_str(url)?;

            // Fast path: try connecting directly to the target database.
            if MssqlConnection::connect_blocking(&options).is_ok() {
                return Ok(true);
            }

            // Fallback: connect to master and check sys.databases.
            let database = extract_database_name(url)?;
            let master_options = options.with_database("master");
            let conn = match MssqlConnection::connect_blocking(&master_options) {
                Ok(conn) => conn,
                Err(_) => return Ok(false),
            };

            let sql = format!(
                "SELECT COUNT(*) FROM sys.databases WHERE name = N'{}'",
                escape_sql_string(&database),
            );
            let count = conn
                .scalar_i64_blocking(&sql)?
                .unwrap_or(0);

            drop(conn);
            Ok(count > 0)
        }
    }

    fn drop_database(url: &str) -> impl std::future::Future<Output = Result<(), Error>> + Send + '_ {
        async move {
            let options = MssqlConnectOptions::from_str(url)?;
            let database = extract_database_name(url)?;
            let master_options = options.with_database("master");
            let conn = MssqlConnection::connect_blocking(&master_options)?;
            conn.exec_sql_blocking(&format!(
                "DROP DATABASE IF EXISTS [{}]",
                escape_sql_bracket(&database),
            ))?;
            drop(conn);
            Ok(())
        }
    }
}

// ---------------------------------------------------------------------------
// Migrate — migration execution and tracking on MssqlConnection
// ---------------------------------------------------------------------------

impl Migrate for MssqlConnection {
    /// MSSQL does not support `CREATE SCHEMA IF NOT EXISTS` as a single
    /// statement, so we use a conditional T-SQL block.
    fn create_schema_if_not_exists<'e>(
        &'e mut self,
        schema_name: &'e str,
    ) -> BoxFuture<'e, Result<(), MigrateError>> {
        let sql = format!(
            "IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = N'{}') \
             EXEC('CREATE SCHEMA [{}]')",
            escape_sql_string(schema_name),
            escape_sql_bracket(schema_name),
        );
        Box::pin(async move {
            self.exec_sql_blocking(&sql).map_err(MigrateError::Execute)?;
            Ok(())
        })
    }

    /// Creates the migrations tracking table if it does not yet exist.
    fn ensure_migrations_table<'e>(
        &'e mut self,
        table_name: &'e str,
    ) -> BoxFuture<'e, Result<(), MigrateError>> {
        let quoted = quoted_table_name(table_name);

        // Determine the schema part for INFORMATION_SCHEMA lookup.
        let (schema, table) = split_table_name(table_name);
        let schema_condition = if schema.is_empty() {
            "TABLE_SCHEMA = 'dbo'".to_owned()
        } else {
            format!("TABLE_SCHEMA = N'{}'", escape_sql_string(schema))
        };

        let create_sql = format!(
            "IF NOT EXISTS ( \
             SELECT * FROM INFORMATION_SCHEMA.TABLES \
             WHERE TABLE_NAME = N'{table}' AND {schema_condition} \
             ) \
             CREATE TABLE {quoted} ( \
             version    BIGINT         NOT NULL PRIMARY KEY, \
             description NVARCHAR(MAX) NOT NULL, \
             migration_type NVARCHAR(20)  NOT NULL, \
             sql        NVARCHAR(MAX) NOT NULL, \
             checksum   VARBINARY(8000)  NOT NULL, \
             executed_at DATETIME2     NOT NULL DEFAULT GETUTCDATE(), \
             no_tx      BIT            NOT NULL DEFAULT 0 \
             )",
            table = escape_sql_string(table),
            schema_condition = schema_condition,
            quoted = quoted,
        );

        Box::pin(async move {
            self.exec_sql_blocking(&create_sql).map_err(MigrateError::Execute)?;
            Ok(())
        })
    }

    /// MSSQL supports transactional DDL, so a dirty (partially applied)
    /// migration cannot occur. Always returns `None`.
    fn dirty_version<'e>(
        &'e mut self,
        _table_name: &'e str,
    ) -> BoxFuture<'e, Result<Option<i64>, MigrateError>> {
        Box::pin(async move { Ok(None) })
    }

    /// Lists all previously applied migrations, ordered by version.
    fn list_applied_migrations<'e>(
        &'e mut self,
        table_name: &'e str,
    ) -> BoxFuture<'e, Result<Vec<AppliedMigration>, MigrateError>> {
        let conn = self.conn.clone();
        let quoted = quoted_table_name(table_name);
        let sql = format!(
            "SELECT version, checksum FROM {quoted} ORDER BY version",
        );

        Box::pin(async move {
            list_applied_migrations_inner(conn, sql)
                .await
                .map_err(MigrateError::Execute)
        })
    }

    /// Acquires an exclusive application-level lock using `sp_getapplock`.
    fn lock(&mut self) -> BoxFuture<'_, Result<(), MigrateError>> {
        Box::pin(async move {
            self.exec_sql_blocking(
                "EXEC sp_getapplock \
                 @Resource = N'sqlx_migration_lock', \
                 @LockMode = 'Exclusive', \
                 @LockOwner = 'Session'",
            )
            .map_err(MigrateError::Execute)?;
            Ok(())
        })
    }

    /// Releases the application-level lock using `sp_releaseapplock`.
    fn unlock(&mut self) -> BoxFuture<'_, Result<(), MigrateError>> {
        Box::pin(async move {
            self.exec_sql_blocking(
                "EXEC sp_releaseapplock \
                 @Resource = N'sqlx_migration_lock', \
                 @LockOwner = 'Session'",
            )
            .map_err(MigrateError::Execute)?;
            Ok(())
        })
    }

    /// Applies a migration: executes the SQL, then records the migration in
    /// the tracking table.
    fn apply<'e>(
        &'e mut self,
        _table_name: &'e str,
        migration: &'e Migration,
    ) -> BoxFuture<'e, Result<Duration, MigrateError>> {
        let conn = self.conn.clone();
        let quoted = quoted_table_name(_table_name);
        let sql = migration.sql.as_str().to_owned();
        let version = migration.version;
        let description = migration.description.to_string();
        let migration_type = format!("{:?}", migration.migration_type);
        let checksum = migration.checksum.to_vec();
        let no_tx = migration.no_tx;

        Box::pin(async move {
            apply_migration_inner(conn, quoted, sql, version, description, migration_type, checksum, no_tx)
                .await
        })
    }

    /// Reverts a migration: executes the down SQL, then removes the tracking
    /// record.
    fn revert<'e>(
        &'e mut self,
        _table_name: &'e str,
        migration: &'e Migration,
    ) -> BoxFuture<'e, Result<Duration, MigrateError>> {
        let conn = self.conn.clone();
        let quoted = quoted_table_name(_table_name);
        let sql = migration.sql.as_str().to_owned();
        let version = migration.version;
        let no_tx = migration.no_tx;

        Box::pin(async move {
            revert_migration_inner(conn, quoted, sql, version, no_tx)
                .await
        })
    }

    /// Marks a migration as applied without executing its SQL.
    fn skip<'e>(
        &'e mut self,
        _table_name: &'e str,
        _migration: &'e Migration,
    ) -> BoxFuture<'e, Result<(), MigrateError>> {
        let quoted = quoted_table_name(_table_name);
        let version = _migration.version;
        let description = _migration.description.to_string();
        let migration_type = format!("{:?}", _migration.migration_type);
        let checksum = _migration.checksum.to_vec();
        let no_tx = _migration.no_tx;

        Box::pin(async move {
            let insert_sql = format!(
                "INSERT INTO {quoted} \
                 (version, description, migration_type, sql, checksum, no_tx) \
                 VALUES ({version}, N'{desc}', N'{mt}', N'', {chk}, {ntx})",
                quoted = quoted,
                version = version,
                desc = escape_sql_string(&description),
                mt = escape_sql_string(&migration_type),
                chk = format_hex(&checksum),
                ntx = if no_tx { 1 } else { 0 },
            );
            self.exec_sql_blocking(&insert_sql)
                .map_err(|e| MigrateError::ExecuteMigration(e, version))
        })
    }
}

// ---------------------------------------------------------------------------
// Async helper functions (offloaded to blocking thread pool)
// ---------------------------------------------------------------------------

/// Locks the shared connection and executes a query against the `Connection`.
macro_rules! with_shared_conn {
    ($conn:expr, |$guard:ident| $body:expr) => {{
        let mut $guard = $conn.lock().map_err(|_| {
            sqlx_core::Error::Protocol(
                "failed to lock the shared ODBC connection".into(),
            )
        })?;
        // Reborrow as a mutable Connection reference (SharedConnection
        // supports DerefMut to Connection).
        let $guard: &mut odbc_api::Connection<'static> = &mut $guard;
        $body
    }};
}

/// Queries the migrations tracking table and returns the list of applied
/// migrations.
async fn list_applied_migrations_inner(
    conn: odbc_api::SharedConnection<'static>,
    sql: String,
) -> std::result::Result<Vec<AppliedMigration>, sqlx_core::Error> {
    offload_blocking(move || {
        with_shared_conn!(conn, |guard| {
            let mut cursor = guard.execute(&sql, (), None).map_err(|error| {
                sqlx_core::Error::from(crate::error::database_error_with_context(
                    error,
                    "failed to query applied migrations",
                ))
            })?
            .ok_or_else(|| {
                sqlx_core::Error::Protocol(
                    "list_applied_migrations returned no result set".into(),
                )
            })?;

            let mut migrations = Vec::new();
            while let Some(mut row) = cursor.next_row().map_err(|error| {
                sqlx_core::Error::from(crate::error::database_error_with_context(
                    error,
                    "failed to read applied migration row",
                ))
            })? {
                let mut version: Nullable<i64> = Nullable::null();
                row.get_data(1, &mut version).map_err(|error| {
                    sqlx_core::Error::from(crate::error::database_error_with_context(
                        error,
                        "failed to read migration version",
                    ))
                })?;

                let mut checksum_bytes = Vec::new();
                let has_value = row.get_binary(2, &mut checksum_bytes).map_err(|error| {
                    sqlx_core::Error::from(crate::error::database_error_with_context(
                        error,
                        "failed to read migration checksum",
                    ))
                })?;

                if let Some(version) = version.into_opt() {
                    migrations.push(AppliedMigration {
                        version,
                        checksum: if has_value {
                            checksum_bytes.into()
                        } else {
                            vec![].into()
                        },
                    });
                }
            }

            Ok(migrations)
        })
    })
    .await
}

/// Executes a migration's SQL inside a DDL transaction, then inserts a
/// tracking record. Returns the elapsed wall-clock time.
async fn apply_migration_inner(
    conn: odbc_api::SharedConnection<'static>,
    quoted: String,
    sql: String,
    version: i64,
    description: String,
    migration_type: String,
    checksum: Vec<u8>,
    no_tx: bool,
) -> std::result::Result<Duration, MigrateError> {
    let start = std::time::Instant::now();

    offload_blocking(move || {
        with_shared_conn!(conn, |guard| {
            // Start a transaction unless the migration opts out.
            if !no_tx {
                guard.set_autocommit(false).map_err(|error| {
                    sqlx_core::Error::from(crate::error::database_error_with_context(
                        error,
                        "failed to start transaction for migration apply",
                    ))
                })?;
            }

            // Execute the migration SQL.
            guard.execute(&sql, (), None).map_err(|error| {
                sqlx_core::Error::from(crate::error::database_error_with_context(
                    error,
                    format!("migration {version} failed"),
                ))
            })?;

            // Insert the tracking record.
            let insert_sql = format!(
                "INSERT INTO {quoted} \
                 (version, description, migration_type, sql, checksum, no_tx) \
                 VALUES ({version}, N'{desc}', N'{mt}', N'{sql_text}', {chk}, {ntx})",
                quoted = quoted,
                version = version,
                desc = escape_sql_string(&description),
                mt = escape_sql_string(&migration_type),
                sql_text = escape_sql_string(&sql),
                chk = format_hex(&checksum),
                ntx = if no_tx { 1 } else { 0 },
            );
            guard.execute(&insert_sql, (), None).map_err(|error| {
                sqlx_core::Error::from(crate::error::database_error_with_context(
                    error,
                    format!("failed to insert tracking record for migration {version}"),
                ))
            })?;

            // Commit the transaction.
            if !no_tx {
                guard.commit().map_err(|error| {
                    sqlx_core::Error::from(crate::error::database_error_with_context(
                        error,
                        format!("failed to commit migration {version}"),
                    ))
                })?;
                guard.set_autocommit(true).map_err(|error| {
                    sqlx_core::Error::from(crate::error::database_error_with_context(
                        error,
                        "failed to restore autocommit after migration apply",
                    ))
                })?;
            }

            Ok(start.elapsed())
        })
    })
    .await
    .map_err(|e| MigrateError::ExecuteMigration(e, version))
}

/// Executes a revert (down) migration's SQL inside a DDL transaction, then
/// removes the tracking record. Returns the elapsed wall-clock time.
async fn revert_migration_inner(
    conn: odbc_api::SharedConnection<'static>,
    quoted: String,
    sql: String,
    version: i64,
    no_tx: bool,
) -> std::result::Result<Duration, MigrateError> {
    let start = std::time::Instant::now();

    offload_blocking(move || {
        with_shared_conn!(conn, |guard| {
            if !no_tx {
                guard.set_autocommit(false).map_err(|error| {
                    sqlx_core::Error::from(crate::error::database_error_with_context(
                        error,
                        "failed to start transaction for migration revert",
                    ))
                })?;
            }

            // Execute the revert SQL.
            guard.execute(&sql, (), None).map_err(|error| {
                sqlx_core::Error::from(crate::error::database_error_with_context(
                    error,
                    format!("revert migration {version} failed"),
                ))
            })?;

            // Remove the tracking record.
            let delete_sql = format!(
                "DELETE FROM {quoted} WHERE version = {version}",
                quoted = quoted,
                version = version,
            );
            guard.execute(&delete_sql, (), None).map_err(|error| {
                sqlx_core::Error::from(crate::error::database_error_with_context(
                    error,
                    format!("failed to delete tracking record for migration {version}"),
                ))
            })?;

            if !no_tx {
                guard.commit().map_err(|error| {
                    sqlx_core::Error::from(crate::error::database_error_with_context(
                        error,
                        format!("failed to commit revert migration {version}"),
                    ))
                })?;
                guard.set_autocommit(true).map_err(|error| {
                    sqlx_core::Error::from(crate::error::database_error_with_context(
                        error,
                        "failed to restore autocommit after migration revert",
                    ))
                })?;
            }

            Ok(start.elapsed())
        })
    })
    .await
    .map_err(|e| MigrateError::ExecuteMigration(e, version))
}
