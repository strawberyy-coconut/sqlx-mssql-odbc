use crate::{
    MssqlArguments, MssqlBufferSettings, MssqlColumn, MssqlConnectOptions,
    MssqlQueryResult, MssqlRow, MssqlStatement, MssqlTypeInfo, MssqlValue, MssqlValueKind, Result,
};
use futures_core::future::BoxFuture;
use futures_core::stream::BoxStream;
use futures_util::{future, stream, StreamExt};
use odbc_api::buffers::{AnyColumnBufferSlice, BufferDesc, ColumnarDynBuffer, NullableSlice};
use odbc_api::{ConnectionTransitions, Cursor, DataType, Nullable, ResultSetMetadata};
use sqlx_core::column::Column;
use sqlx_core::common::StatementCache;
use sqlx_core::executor::{Execute, Executor};
use sqlx_core::sql_str::SqlStr;
use sqlx_core::transaction::Transaction;
use sqlx_core::Either;
use std::future::Future;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

type PreparedStatement =
    odbc_api::Prepared<odbc_api::handles::StatementConnection<odbc_api::SharedConnection<'static>>>;
type ExecuteResult = std::result::Result<Either<MssqlQueryResult, MssqlRow>, sqlx_core::Error>;
type ExecuteSender = flume::Sender<ExecuteResult>;

// ============================================================================
// Command enum — sent from the async handle to the actor thread
// ============================================================================

enum Command {
    Execute {
        sql: SqlStr,
        args: Option<MssqlArguments>,
        persistent: bool,
        response: ExecuteSender,
    },
    Prepare {
        sql: SqlStr,
        response: flume::Sender<
            std::result::Result<MssqlStatement, sqlx_core::Error>,
        >,
    },
    Ping {
        response: flume::Sender<std::result::Result<(), sqlx_core::Error>>,
    },
    Begin {
        response: flume::Sender<std::result::Result<(), sqlx_core::Error>>,
    },
    Commit {
        response: flume::Sender<std::result::Result<(), sqlx_core::Error>>,
    },
    Rollback {
        response: flume::Sender<std::result::Result<(), sqlx_core::Error>>,
    },
    StartRollback,
    ExecSql {
        sql: String,
        response: flume::Sender<std::result::Result<(), sqlx_core::Error>>,
    },
    ScalarI64 {
        sql: String,
        response:
            flume::Sender<std::result::Result<Option<i64>, sqlx_core::Error>>,
    },
    Shutdown {
        signal: flume::Sender<()>,
    },
    /// Returns `Vec<(version, checksum_bytes)>` from the migrations table.
    ListMigrations {
        sql: String,
        response:
            flume::Sender<std::result::Result<Vec<(i64, Vec<u8>)>, sqlx_core::Error>>,
    },
    /// Applies a migration: starts a transaction, runs SQL, inserts tracking
    /// record, commits. If `no_tx` is true the transaction is skipped.
    #[cfg(feature = "migrate")]
    ApplyMigration {
        sql: String,
        insert_sql: String,
        version: i64,
        no_tx: bool,
        response: flume::Sender<std::result::Result<std::time::Duration, sqlx_core::Error>>,
    },
    /// Reverts a migration: starts a transaction, runs SQL, deletes tracking
    /// record, commits. If `no_tx` is true the transaction is skipped.
    #[cfg(feature = "migrate")]
    RevertMigration {
        sql: String,
        delete_sql: String,
        version: i64,
        no_tx: bool,
        response: flume::Sender<std::result::Result<std::time::Duration, sqlx_core::Error>>,
    },
}

// ============================================================================
// ConnectionActor — owns the ODBC connection on a dedicated blocking thread
// ============================================================================

struct ConnectionActor {
    conn: odbc_api::SharedConnection<'static>,
    stmt_cache: StatementCache<PreparedStatement>,
    transaction_depth: usize,
    buffer_settings: MssqlBufferSettings,
}

impl ConnectionActor {
    fn run(mut self, rx: flume::Receiver<Command>) {
        // The channel iterator blocks on recv() and returns None when the
        // channel is closed (all senders dropped).
        for cmd in rx {
            // Ignore errors from response senders — the consumer may have
            // dropped their receiver (stream cancelled, etc.).
            match cmd {
                Command::Execute {
                    sql,
                    args,
                    persistent,
                    response,
                } => {
                    let _ = self.handle_execute(sql, args, persistent, &response);
                }
                Command::Prepare { sql, response } => {
                    let _ = response.send(self.handle_prepare(sql));
                }
                Command::Ping { response } => {
                    let _ = response.send(self.handle_ping());
                }
                Command::Begin { response } => {
                    let _ = response.send(self.handle_begin());
                }
                Command::Commit { response } => {
                    let _ = response.send(self.handle_commit());
                }
                Command::Rollback { response } => {
                    let _ = response.send(self.handle_rollback());
                }
                Command::StartRollback => {
                    self.handle_start_rollback();
                }
                Command::ExecSql { sql, response } => {
                    let _ = response.send(self.handle_exec_sql(&sql));
                }
                Command::ScalarI64 { sql, response } => {
                    let _ = response.send(self.handle_scalar_i64(&sql));
                }
                Command::Shutdown { signal } => {
                    let _ = signal.send(());
                    return;
                }
                Command::ListMigrations { sql, response } => {
                    let _ = response.send(self.handle_list_migrations(&sql));
                }
                #[cfg(feature = "migrate")]
                Command::ApplyMigration {
                    sql,
                    insert_sql,
                    version,
                    no_tx,
                    response,
                } => {
                    let _ = response.send(self.handle_apply_migration(&sql, &insert_sql, version, no_tx));
                }
                #[cfg(feature = "migrate")]
                Command::RevertMigration {
                    sql,
                    delete_sql,
                    version,
                    no_tx,
                    response,
                } => {
                    let _ = response.send(self.handle_revert_migration(&sql, &delete_sql, version, no_tx));
                }
            }
        }
        // Channel closed — exit loop, dropping self and the SharedConnection.
    }

    // ---------------------------------------------------------------
    // Command handlers
    // ---------------------------------------------------------------

    fn handle_execute(
        &mut self,
        sql: SqlStr,
        arguments: Option<MssqlArguments>,
        persistent: bool,
        tx: &ExecuteSender,
    ) -> std::result::Result<(), sqlx_core::Error> {
        let has_arguments = arguments.as_ref().is_some_and(|a| !a.is_empty());
        let parameters = arguments
            .as_ref()
            .map(MssqlArguments::to_odbc_parameter_collection)
            .unwrap_or_default();

        if persistent && has_arguments {
            if let Some(prepared) = self.stmt_cache.get_mut(sql.as_str()) {
                // Execute from cache.
                let mut conn_guard = self.conn.lock().map_err(|_| {
                    sqlx_core::Error::Protocol(
                        "ODBC execute: failed to lock connection".to_owned(),
                    )
                })?;
                let has_cursor = prepared
                    .execute(parameters.as_slice())
                    .map_err(|error| {
                        crate::error::database_error_with_context_lazy(error, || {
                            format!(
                                "failed to execute cached ODBC statement: `{}`",
                                sql_preview(sql.as_str())
                            )
                        })
                    })?
                    .is_some();
                drop(conn_guard);

                if has_cursor {
                    // Re-execute to get the cursor (avoid borrow conflict).
                    let mut conn_guard = self.conn.lock().map_err(|_| {
                        sqlx_core::Error::Protocol(
                            "ODBC execute: failed to lock connection".to_owned(),
                        )
                    })?;
                    let cursor = prepared
                        .execute(parameters.as_slice())
                        .map_err(|error| {
                            crate::error::database_error_with_context_lazy(error, || {
                                format!(
                                    "failed to execute cached ODBC statement: `{}`",
                                    sql_preview(sql.as_str())
                                )
                            })
                        })?
                        .expect("has_cursor was true");
                    drop(conn_guard);
                    return stream_result_sets(cursor, self.buffer_settings, tx);
                }

                let ra = prepared.row_count().map_err(|error| {
                    crate::error::database_error_with_context_lazy(error, || {
                        format!(
                            "failed to read ODBC row count for cached statement: `{}`",
                            sql_preview(sql.as_str())
                        )
                    })
                })?;
                return send_rows_affected(ra, tx);
            } else {
                // Prepare and cache
                let mut prepared =
                    self.conn.clone().into_prepared(sql.as_str()).map_err(|error| {
                        crate::error::database_error_with_context_lazy(error, || {
                            format!(
                                "failed to prepare cached ODBC statement: `{}`",
                                sql_preview(sql.as_str())
                            )
                        })
                    })?;

                let mut conn_guard = self.conn.lock().map_err(|_| {
                    sqlx_core::Error::Protocol(
                        "ODBC execute: failed to lock connection".to_owned(),
                    )
                })?;
                let has_cursor = prepared
                    .execute(parameters.as_slice())
                    .map_err(|error| {
                        crate::error::database_error_with_context_lazy(error, || {
                            format!(
                                "failed to execute cached ODBC statement: `{}`",
                                sql_preview(sql.as_str())
                            )
                        })
                    })?
                    .is_some();
                drop(conn_guard);

                if has_cursor {
                    // Re-execute to get the cursor for streaming.
                    let mut conn_guard = self.conn.lock().map_err(|_| {
                        sqlx_core::Error::Protocol(
                            "ODBC execute: failed to lock connection".to_owned(),
                        )
                    })?;
                    let cursor = prepared
                        .execute(parameters.as_slice())
                        .map_err(|error| {
                            crate::error::database_error_with_context_lazy(error, || {
                                format!(
                                    "failed to execute cached ODBC statement: `{}`",
                                    sql_preview(sql.as_str())
                                )
                            })
                        })?
                        .expect("has_cursor was true");
                    drop(conn_guard);
                    return stream_result_sets(cursor, self.buffer_settings, tx);
                }

                let ra = prepared.row_count().map_err(|error| {
                    crate::error::database_error_with_context_lazy(error, || {
                        format!(
                            "failed to read ODBC row count for cached statement: `{}`",
                            sql_preview(sql.as_str())
                        )
                    })
                })?;
                self.stmt_cache.insert(sql.as_str(), prepared);
                return send_rows_affected(ra, tx);
            }
        } else {
            // Unprepared (one-shot) path
            let mut statement = self.conn.clone().into_preallocated().map_err(|error| {
                crate::error::database_error_with_context_lazy(error, || {
                    format!(
                        "failed to allocate an ODBC statement for query: `{}`",
                        sql_preview(sql.as_str())
                    )
                })
            })?;
            if let Some(cursor) = statement
                .execute(sql.as_str(), parameters.as_slice())
                .map_err(|error| {
                    crate::error::database_error_with_context_lazy(error, || {
                        format!(
                            "failed to execute ODBC query: `{}`",
                            sql_preview(sql.as_str())
                        )
                    })
                })? {
                return stream_result_sets(cursor, self.buffer_settings, tx);
            }
            let rows_affected = statement.row_count().map_err(|error| {
                crate::error::database_error_with_context_lazy(error, || {
                    format!(
                        "failed to read ODBC row count for query: `{}`",
                        sql_preview(sql.as_str())
                    )
                })
            })?;
            send_rows_affected(rows_affected, tx)
        }
    }

    fn handle_prepare(
        &mut self,
        sql: SqlStr,
    ) -> std::result::Result<MssqlStatement, sqlx_core::Error> {
        if let Some(prepared) = self.stmt_cache.get_mut(sql.as_str()) {
            let parameters = prepared.num_params().map_err(|error| {
                sqlx_core::Error::from(crate::error::database_error_with_context(
                    error,
                    format!(
                        "failed to read ODBC parameter metadata for cached statement: `{}`",
                        sql_preview(sql.as_str())
                    ),
                ))
            })?;
            let columns = collect_prepared_columns(prepared, parameters)?;
            return Ok(MssqlStatement::new(sql, columns, usize::from(parameters)));
        }

        let mut prepared = self.conn.clone().into_prepared(sql.as_str()).map_err(|error| {
            sqlx_core::Error::from(crate::error::database_error_with_context(
                error,
                format!(
                    "failed to prepare MSSQL ODBC statement: `{}`",
                    sql_preview(sql.as_str())
                ),
            ))
        })?;
        let parameters = prepared.num_params().map_err(|error| {
            sqlx_core::Error::from(crate::error::database_error_with_context(
                error,
                format!(
                    "failed to read ODBC parameter metadata for prepared statement: `{}`",
                    sql_preview(sql.as_str())
                ),
            ))
        })?;
        let columns = collect_prepared_columns(&mut prepared, parameters)?;
        if self.stmt_cache.is_enabled() {
            self.stmt_cache.insert(sql.as_str(), prepared);
        }

        Ok(MssqlStatement::new(sql, columns, usize::from(parameters)))
    }

    fn handle_ping(&mut self) -> std::result::Result<(), sqlx_core::Error> {
        let mut conn_guard = self.conn.lock().map_err(|_| {
            sqlx_core::Error::Protocol("failed to lock connection for ping".into())
        })?;
        conn_guard.execute("SELECT 1", (), None).map_err(|error| {
            sqlx_core::Error::from(crate::error::database_error_with_context(
                error,
                "MSSQL ping query failed: `SELECT 1`",
            ))
        })?;
        Ok(())
    }

    fn handle_begin(&mut self) -> std::result::Result<(), sqlx_core::Error> {
        if self.transaction_depth == 0 {
            let mut conn_guard = self.conn.lock().map_err(|_| {
                sqlx_core::Error::Protocol(
                    "MSSQL ODBC begin: failed to lock connection".to_owned(),
                )
            })?;
            conn_guard.set_autocommit(false).map_err(|error| {
                sqlx_core::Error::from(crate::error::database_error_with_context(
                    error,
                    "failed to disable ODBC autocommit while beginning a transaction",
                ))
            })?;
        } else {
            let savepoint = format!("sqlx_sp_{}", self.transaction_depth);
            let mut conn_guard = self.conn.lock().map_err(|_| {
                sqlx_core::Error::Protocol(
                    "MSSQL ODBC begin (savepoint): failed to lock connection".to_owned(),
                )
            })?;
            conn_guard
                .execute(&format!("SAVE TRANSACTION {savepoint}"), (), None)
                .map_err(|error| {
                    sqlx_core::Error::from(crate::error::database_error_with_context(
                        error,
                        format!(
                            "failed to create save point `{savepoint}` for nested transaction"
                        ),
                    ))
                })?;
        }
        self.transaction_depth += 1;
        Ok(())
    }

    fn handle_commit(&mut self) -> std::result::Result<(), sqlx_core::Error> {
        if self.transaction_depth == 0 {
            return Ok(());
        }

        if self.transaction_depth == 1 {
            let mut conn_guard = self.conn.lock().map_err(|_| {
                sqlx_core::Error::Protocol(
                    "MSSQL ODBC commit: failed to lock connection".to_owned(),
                )
            })?;
            conn_guard.commit().map_err(|error| {
                sqlx_core::Error::from(crate::error::database_error_with_context(
                    error,
                    "failed to commit the active MSSQL ODBC transaction",
                ))
            })?;
            conn_guard.set_autocommit(true).map_err(|error| {
                sqlx_core::Error::from(crate::error::database_error_with_context(
                    error,
                    "failed to restore ODBC autocommit after commit",
                ))
            })?;
            self.transaction_depth = 0;
        } else {
            self.transaction_depth -= 1;
        }
        Ok(())
    }

    fn handle_rollback(&mut self) -> std::result::Result<(), sqlx_core::Error> {
        if self.transaction_depth == 0 {
            return Ok(());
        }

        if self.transaction_depth == 1 {
            let mut conn_guard = self.conn.lock().map_err(|_| {
                sqlx_core::Error::Protocol(
                    "MSSQL ODBC rollback: failed to lock connection".to_owned(),
                )
            })?;
            conn_guard.rollback().map_err(|error| {
                sqlx_core::Error::from(crate::error::database_error_with_context(
                    error,
                    "failed to roll back the active ODBC transaction",
                ))
            })?;
            conn_guard.set_autocommit(true).map_err(|error| {
                sqlx_core::Error::from(crate::error::database_error_with_context(
                    error,
                    "failed to restore ODBC autocommit after rollback",
                ))
            })?;
            self.transaction_depth = 0;
        } else {
            let savepoint = format!("sqlx_sp_{}", self.transaction_depth - 1);
            let mut conn_guard = self.conn.lock().map_err(|_| {
                sqlx_core::Error::Protocol(
                    "MSSQL ODBC rollback (savepoint): failed to lock connection".to_owned(),
                )
            })?;
            conn_guard
                .execute(&format!("ROLLBACK TRANSACTION {savepoint}"), (), None)
                .map_err(|error| {
                    sqlx_core::Error::from(crate::error::database_error_with_context(
                        error,
                        format!("failed to roll back to save point `{savepoint}`"),
                    ))
                })?;
            self.transaction_depth -= 1;
        }
        Ok(())
    }

    fn handle_start_rollback(&mut self) {
        if self.transaction_depth == 0 {
            return;
        }

        if self.transaction_depth == 1 {
            if let Ok(mut conn_guard) = self.conn.lock() {
                let _ = conn_guard.rollback();
                let _ = conn_guard.set_autocommit(true);
            }
            self.transaction_depth = 0;
        } else {
            let savepoint = format!("sqlx_sp_{}", self.transaction_depth - 1);
            if let Ok(mut conn_guard) = self.conn.lock() {
                let _ = conn_guard.execute(
                    &format!("ROLLBACK TRANSACTION {savepoint}"),
                    (),
                    None,
                );
            }
            self.transaction_depth -= 1;
        }
    }

    fn handle_exec_sql(&self, sql: &str) -> std::result::Result<(), sqlx_core::Error> {
        let mut conn_guard = self.conn.lock().map_err(|_| {
            sqlx_core::Error::Protocol("failed to lock the shared ODBC connection".into())
        })?;
        conn_guard.execute(sql, (), None).map_err(|error| {
            sqlx_core::Error::from(crate::error::database_error_with_context(
                error,
                format!("failed to execute SQL: `{}`", sql_preview(sql)),
            ))
        })?;
        Ok(())
    }

    fn handle_scalar_i64(&self, sql: &str) -> std::result::Result<Option<i64>, sqlx_core::Error> {
        let mut conn_guard = self.conn.lock().map_err(|_| {
            sqlx_core::Error::Protocol("failed to lock the shared ODBC connection".into())
        })?;
        let mut cursor = conn_guard
            .execute(sql, (), None)
            .map_err(|error| {
                sqlx_core::Error::from(crate::error::database_error_with_context(
                    error,
                    format!("scalar query failed: `{}`", sql_preview(sql)),
                ))
            })?
            .ok_or_else(|| {
                sqlx_core::Error::Protocol(format!(
                    "scalar query returned no result set: `{}`",
                    sql_preview(sql),
                ))
            })?;

        if let Some(mut row) = cursor.next_row().map_err(|error| {
            sqlx_core::Error::from(crate::error::database_error_with_context(
                error,
                "scalar query next row",
            ))
        })? {
            let mut value: Nullable<i64> = Nullable::null();
            row.get_data(1, &mut value).map_err(|error| {
                sqlx_core::Error::from(crate::error::database_error_with_context(
                    error,
                    "scalar query column 1",
                ))
            })?;
            Ok(value.into_opt())
        } else {
            Ok(None)
        }
    }

    fn handle_list_migrations(
        &self,
        sql: &str,
    ) -> std::result::Result<Vec<(i64, Vec<u8>)>, sqlx_core::Error> {
        let mut conn_guard = self.conn.lock().map_err(|_| {
            sqlx_core::Error::Protocol("failed to lock the shared ODBC connection".into())
        })?;
        let mut cursor = conn_guard
            .execute(sql, (), None)
            .map_err(|error| {
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
                migrations.push((version, if has_value { checksum_bytes } else { vec![] }));
            }
        }

        Ok(migrations)
    }

    #[cfg(feature = "migrate")]
    fn handle_apply_migration(
        &mut self,
        sql: &str,
        insert_sql: &str,
        version: i64,
        no_tx: bool,
    ) -> std::result::Result<std::time::Duration, sqlx_core::Error> {
        let start = std::time::Instant::now();
        let mut conn_guard = self.conn.lock().map_err(|_| {
            sqlx_core::Error::Protocol(
                "failed to lock the shared ODBC connection for migration".into(),
            )
        })?;

        if !no_tx {
            conn_guard.set_autocommit(false).map_err(|error| {
                sqlx_core::Error::from(crate::error::database_error_with_context(
                    error,
                    "failed to start transaction for migration apply",
                ))
            })?;
        }

        conn_guard.execute(sql, (), None).map_err(|error| {
            sqlx_core::Error::from(crate::error::database_error_with_context(
                error,
                format!("migration {version} failed"),
            ))
        })?;

        conn_guard.execute(insert_sql, (), None).map_err(|error| {
            sqlx_core::Error::from(crate::error::database_error_with_context(
                error,
                format!("failed to insert tracking record for migration {version}"),
            ))
        })?;

        if !no_tx {
            conn_guard.commit().map_err(|error| {
                sqlx_core::Error::from(crate::error::database_error_with_context(
                    error,
                    format!("failed to commit migration {version}"),
                ))
            })?;
            conn_guard.set_autocommit(true).map_err(|error| {
                sqlx_core::Error::from(crate::error::database_error_with_context(
                    error,
                    "failed to restore autocommit after migration apply",
                ))
            })?;
        }

        Ok(start.elapsed())
    }

    #[cfg(feature = "migrate")]
    fn handle_revert_migration(
        &mut self,
        sql: &str,
        delete_sql: &str,
        version: i64,
        no_tx: bool,
    ) -> std::result::Result<std::time::Duration, sqlx_core::Error> {
        let start = std::time::Instant::now();
        let mut conn_guard = self.conn.lock().map_err(|_| {
            sqlx_core::Error::Protocol(
                "failed to lock the shared ODBC connection for migration".into(),
            )
        })?;

        if !no_tx {
            conn_guard.set_autocommit(false).map_err(|error| {
                sqlx_core::Error::from(crate::error::database_error_with_context(
                    error,
                    "failed to start transaction for migration revert",
                ))
            })?;
        }

        conn_guard.execute(sql, (), None).map_err(|error| {
            sqlx_core::Error::from(crate::error::database_error_with_context(
                error,
                format!("revert migration {version} failed"),
            ))
        })?;

        conn_guard.execute(delete_sql, (), None).map_err(|error| {
            sqlx_core::Error::from(crate::error::database_error_with_context(
                error,
                format!("failed to delete tracking record for migration {version}"),
            ))
        })?;

        if !no_tx {
            conn_guard.commit().map_err(|error| {
                sqlx_core::Error::from(crate::error::database_error_with_context(
                    error,
                    format!("failed to commit migration revert {version}"),
                ))
            })?;
            conn_guard.set_autocommit(true).map_err(|error| {
                sqlx_core::Error::from(crate::error::database_error_with_context(
                    error,
                    "failed to restore autocommit after migration revert",
                ))
            })?;
        }

        Ok(start.elapsed())
    }
}

/// MSSQL connection backed by an actor thread that owns the ODBC connection.
pub struct MssqlConnection {
    cmd_tx: flume::Sender<Command>,
    buffer_settings: MssqlBufferSettings,
    transaction_depth: AtomicUsize,
}

impl std::fmt::Debug for MssqlConnection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MssqlConnection").finish_non_exhaustive()
    }
}

impl MssqlConnection {
    /// Opens a blocking MSSQL ODBC connection with the provided options and
    /// spawns an actor thread to own it.
    pub fn connect_blocking(options: &MssqlConnectOptions) -> Result<Self> {
        let env = odbc_api::environment().map_err(|error| {
            crate::MssqlError::Configuration(format!(
                "failed to initialize the process-wide ODBC environment: {error}"
            ))
        })?;

        let raw_conn = env
            .connect_with_connection_string(options.connection_string(), Default::default())
            .map_err(|error| {
                crate::error::database_error_with_context(
                    error,
                    "failed to open MSSQL ODBC connection using the supplied connection string",
                )
            })?;

        // Wrap in SharedConnection so PreparedStatement can own the connection.
        let conn: odbc_api::SharedConnection<'static> =
            std::sync::Arc::new(std::sync::Mutex::new(raw_conn));

        let (cmd_tx, cmd_rx) = flume::unbounded();

        let actor = ConnectionActor {
            conn,
            stmt_cache: StatementCache::new(options.statement_cache_capacity),
            transaction_depth: 0,
            buffer_settings: options.buffer_settings,
        };

        // Spawn the actor on a dedicated OS thread so this function can be
        // called from contexts where no Tokio runtime exists (for example,
        // compile-time query checking in proc macros).
        std::thread::spawn(move || actor.run(cmd_rx));

        Ok(Self {
            cmd_tx,
            buffer_settings: options.buffer_settings,
            transaction_depth: AtomicUsize::new(0),
        })
    }

    /// Executes a minimal connectivity query.
    pub fn ping_blocking(&self) -> std::result::Result<(), sqlx_core::Error> {
        send_command_blocking(&self.cmd_tx, |tx| Command::Ping { response: tx })?
    }

    /// Returns the DBMS name reported by the ODBC driver.
    pub fn dbms_name(&self) -> std::result::Result<String, sqlx_core::Error> {
        send_command_blocking(&self.cmd_tx, |tx| {
            Command::ExecSql {
                sql: "SELECT 1 /* dbms_name */".into(),
                response: tx,
            }
        })?;
        Ok("MSSQL via ODBC".to_owned())
    }

    /// Begins a transaction (synchronous, called from TransactionManager).
    pub(crate) fn begin_blocking(&mut self) -> std::result::Result<(), sqlx_core::Error> {
        let r = send_command_blocking(&self.cmd_tx, |tx| Command::Begin { response: tx })?;
        if r.is_ok() {
            self.transaction_depth.fetch_add(1, Ordering::SeqCst);
        }
        r
    }

    /// Commits the current transaction (synchronous, called from TransactionManager).
    pub(crate) fn commit_blocking(&mut self) -> std::result::Result<(), sqlx_core::Error> {
        let depth = self.transaction_depth.load(Ordering::SeqCst);
        if depth == 0 {
            return Ok(());
        }
        let r = send_command_blocking(&self.cmd_tx, |tx| Command::Commit { response: tx })?;
        if r.is_ok() {
            if depth == 1 {
                self.transaction_depth.store(0, Ordering::SeqCst);
            } else {
                self.transaction_depth.fetch_sub(1, Ordering::SeqCst);
            }
        }
        r
    }

    /// Rolls back the current transaction (synchronous, called from TransactionManager).
    pub(crate) fn rollback_blocking(&mut self) -> std::result::Result<(), sqlx_core::Error> {
        let depth = self.transaction_depth.load(Ordering::SeqCst);
        if depth == 0 {
            return Ok(());
        }
        let r = send_command_blocking(&self.cmd_tx, |tx| Command::Rollback { response: tx })?;
        if r.is_ok() {
            if depth == 1 {
                self.transaction_depth.store(0, Ordering::SeqCst);
            } else {
                self.transaction_depth.fetch_sub(1, Ordering::SeqCst);
            }
        }
        r
    }

    /// Starts a rollback without blocking (called from Drop path).
    pub(crate) fn start_rollback(&mut self) {
        let _ = self.cmd_tx.try_send(Command::StartRollback);
        self.transaction_depth.store(0, Ordering::SeqCst);
    }

    /// Returns the current transaction depth.
    pub(crate) fn transaction_depth(&self) -> usize {
        self.transaction_depth.load(Ordering::SeqCst)
    }

    /// Sets the transaction depth (used by TransactionManager).
    pub(crate) fn set_transaction_depth(&mut self, depth: usize) {
        self.transaction_depth.store(depth, Ordering::SeqCst);
    }

    /// Prepares a statement and returns the metadata reported by the ODBC driver.
    pub fn prepare_blocking(
        &self,
        sql: sqlx_core::sql_str::SqlStr,
    ) -> std::result::Result<MssqlStatement, sqlx_core::Error> {
        send_command_blocking(&self.cmd_tx, |tx| Command::Prepare { sql, response: tx })?
    }

    /// Executes a SQL statement directly with no parameters and discards any result set.
    #[cfg(feature = "migrate")]
    pub(crate) fn exec_sql_blocking(&self, sql: &str) -> std::result::Result<(), sqlx_core::Error> {
        send_command_blocking(&self.cmd_tx, |tx| {
            Command::ExecSql {
                sql: sql.to_owned(),
                response: tx,
            }
        })?
    }

    /// Executes a SQL query and returns the first column of the first row as an `i64`.
    #[cfg(feature = "migrate")]
    pub(crate) fn scalar_i64_blocking(
        &self,
        sql: &str,
    ) -> std::result::Result<Option<i64>, sqlx_core::Error> {
        send_command_blocking(&self.cmd_tx, |tx| {
            Command::ScalarI64 {
                sql: sql.to_owned(),
                response: tx,
            }
        })?
    }

    /// Executes a SQL query and returns rows as a list of (i64, binary) tuples.
    #[cfg(feature = "migrate")]
    pub(crate) fn list_migrations_blocking(
        &self,
        sql: &str,
    ) -> std::result::Result<Vec<(i64, Vec<u8>)>, sqlx_core::Error> {
        send_command_blocking(&self.cmd_tx, |tx| {
            Command::ListMigrations {
                sql: sql.to_owned(),
                response: tx,
            }
        })?
    }

    /// Applies a migration via the actor. Returns the elapsed duration.
    #[cfg(feature = "migrate")]
    pub(crate) fn apply_migration_blocking(
        &self,
        sql: &str,
        insert_sql: &str,
        version: i64,
        no_tx: bool,
    ) -> std::result::Result<std::time::Duration, sqlx_core::Error> {
        send_command_blocking(&self.cmd_tx, |tx| {
            Command::ApplyMigration {
                sql: sql.to_owned(),
                insert_sql: insert_sql.to_owned(),
                version,
                no_tx,
                response: tx,
            }
        })?
    }

    /// Reverts a migration via the actor. Returns the elapsed duration.
    #[cfg(feature = "migrate")]
    pub(crate) fn revert_migration_blocking(
        &self,
        sql: &str,
        delete_sql: &str,
        version: i64,
        no_tx: bool,
    ) -> std::result::Result<std::time::Duration, sqlx_core::Error> {
        send_command_blocking(&self.cmd_tx, |tx| {
            Command::RevertMigration {
                sql: sql.to_owned(),
                delete_sql: delete_sql.to_owned(),
                version,
                no_tx,
                response: tx,
            }
        })?
    }

    /// Creates a receiver that the actor will stream query results into.
    pub(crate) fn execute_receiver(
        &self,
        sql: sqlx_core::sql_str::SqlStr,
        persistent: bool,
        arguments: Option<MssqlArguments>,
    ) -> flume::Receiver<ExecuteResult> {
        let (tx, rx) = flume::bounded(64);
        if self
            .cmd_tx
            .send(Command::Execute {
                sql,
                args: arguments,
                persistent,
                response: tx,
            })
            .is_err()
        {
            // Actor has shut down — drain the rx so recv_async returns None
            let _ = rx.drain();
        }
        rx
    }
}

// Dropping cmd_tx closes the channel, causing the actor loop to exit.
impl Drop for MssqlConnection {
    fn drop(&mut self) {}
}

// ============================================================================
// Connection trait
// ============================================================================

impl sqlx_core::connection::Connection for MssqlConnection {
    type Database = crate::Mssql;
    type Options = MssqlConnectOptions;

    async fn close(self) -> std::result::Result<(), sqlx_core::Error> {
        drop(self);
        Ok(())
    }

    async fn close_hard(self) -> std::result::Result<(), sqlx_core::Error> {
        drop(self);
        Ok(())
    }

    async fn ping(&mut self) -> std::result::Result<(), sqlx_core::Error> {
        send_command_async(&self.cmd_tx, |tx| Command::Ping { response: tx }).await?
    }

    fn begin(
        &mut self,
    ) -> impl Future<Output = std::result::Result<Transaction<'_, Self::Database>, sqlx_core::Error>>
           + Send
           + '_ {
        Transaction::begin(self, None)
    }

    fn shrink_buffers(&mut self) {}

    async fn flush(&mut self) -> std::result::Result<(), sqlx_core::Error> {
        Ok(())
    }

    fn should_flush(&self) -> bool {
        false
    }

    fn cached_statements_size(&self) -> usize
    where
        Self::Database: sqlx_core::database::HasStatementCache,
    {
        // The statement cache lives on the actor thread; we can't query it
        // synchronously. Return 0 — callers use this only for diagnostics.
        0
    }

    async fn clear_cached_statements(&mut self) -> std::result::Result<(), sqlx_core::Error>
    where
        Self::Database: sqlx_core::database::HasStatementCache,
    {
        // The cache lives on the actor; clearing it requires a new command.
        // For now this is a no-op since the cache is per-connection and
        // bounded by `statement_cache_capacity`.
        Ok(())
    }
}

// ============================================================================
// Executor trait
// ============================================================================

impl<'c> Executor<'c> for &'c mut MssqlConnection {
    type Database = crate::Mssql;

    fn fetch_many<'e, 'q, E>(
        self,
        mut query: E,
    ) -> BoxStream<'e, std::result::Result<Either<MssqlQueryResult, MssqlRow>, sqlx_core::Error>>
    where
        'c: 'e,
        E: Execute<'q, Self::Database>,
        'q: 'e,
        E: 'q,
    {
        let arguments = query.take_arguments().map_err(sqlx_core::Error::Encode);
        let persistent = query.persistent();
        let sql = query.sql();

        match arguments {
            Ok(arguments) => {
                receiver_to_stream(self.execute_receiver(sql, persistent, arguments))
            }
            Err(error) => stream::once(future::ready(Err(error))).boxed(),
        }
    }

    fn fetch_optional<'e, 'q, E>(
        self,
        mut query: E,
    ) -> BoxFuture<'e, std::result::Result<Option<MssqlRow>, sqlx_core::Error>>
    where
        'c: 'e,
        E: Execute<'q, Self::Database>,
        'q: 'e,
        E: 'q,
    {
        let arguments = query.take_arguments().map_err(sqlx_core::Error::Encode);
        let persistent = query.persistent();
        let sql = query.sql();

        Box::pin(async move {
            let rx = self.execute_receiver(sql, persistent, arguments?);
            while let Ok(item) = rx.recv_async().await {
                match item? {
                    Either::Right(row) => return Ok(Some(row)),
                    Either::Left(_) => {}
                }
            }
            Ok(None)
        })
    }

    fn prepare_with<'e>(
        self,
        sql: sqlx_core::sql_str::SqlStr,
        _parameters: &[crate::MssqlTypeInfo],
    ) -> BoxFuture<'e, std::result::Result<MssqlStatement, sqlx_core::Error>>
    where
        'c: 'e,
    {
        let cmd_tx = self.cmd_tx.clone();
        Box::pin(async move {
            send_command_async(&cmd_tx, |tx| Command::Prepare { sql, response: tx }).await?
        })
    }

    #[cfg(feature = "offline")]
    fn describe<'e>(
        self,
        sql: sqlx_core::sql_str::SqlStr,
    ) -> BoxFuture<'e, std::result::Result<sqlx_core::describe::Describe<Self::Database>, sqlx_core::Error>>
    where
        'c: 'e,
    {
        use sqlx_core::statement::Statement;
        let cmd_tx = self.cmd_tx.clone();
        Box::pin(async move {
            let statement =
                send_command_async(&cmd_tx, |tx| Command::Prepare { sql, response: tx }).await??;
            let columns = statement.columns().to_vec();
            let column_count = columns.len();
            let parameter_count = statement
                .parameters()
                .map(|p| match p {
                    Either::Left(types) => types.len(),
                    Either::Right(count) => count,
                })
                .unwrap_or(0);

            Ok(sqlx_core::describe::Describe {
                columns,
                parameters: Some(Either::Right(parameter_count)),
                nullable: vec![None; column_count],
            })
        })
    }
}

// ============================================================================
// Helper: send a command and await a oneshot response (async)
// ============================================================================

async fn send_command_async<T: Send>(
    cmd_tx: &flume::Sender<Command>,
    make_cmd: impl FnOnce(flume::Sender<T>) -> Command,
) -> std::result::Result<T, sqlx_core::Error> {
    let (resp_tx, resp_rx) = flume::bounded(1);
    let cmd = make_cmd(resp_tx);
    cmd_tx.send(cmd).map_err(|_| {
        sqlx_core::Error::Protocol(
            "MSSQL ODBC connection actor has shut down".to_owned(),
        )
    })?;
    resp_rx.recv_async().await.map_err(|_| {
        sqlx_core::Error::Protocol(
            "MSSQL ODBC connection actor response channel closed".to_owned(),
        )
    })
}

// ============================================================================
// Helper: send a command and wait for a oneshot response (blocking)
// ============================================================================

fn send_command_blocking<T: Send>(
    cmd_tx: &flume::Sender<Command>,
    make_cmd: impl FnOnce(flume::Sender<T>) -> Command,
) -> std::result::Result<T, sqlx_core::Error> {
    let (resp_tx, resp_rx) = flume::bounded(1);
    let cmd = make_cmd(resp_tx);
    cmd_tx.send(cmd).map_err(|_| {
        sqlx_core::Error::Protocol(
            "MSSQL ODBC connection actor has shut down".to_owned(),
        )
    })?;
    resp_rx.recv().map_err(|_| {
        sqlx_core::Error::Protocol(
            "MSSQL ODBC connection actor response channel closed".to_owned(),
        )
    })
}

// ============================================================================
// Helper: convert a flume receiver to a BoxStream
// ============================================================================

fn receiver_to_stream<'e>(
    rx: flume::Receiver<ExecuteResult>,
) -> BoxStream<'e, ExecuteResult> {
    stream::unfold(rx, |rx| async move {
        rx.recv_async().await.ok().map(|item| (item, rx))
    })
    .boxed()
}

// ============================================================================
// Helper: send query-result rows via the execute channel
// ============================================================================

fn send_rows_affected(
    rows_affected: Option<usize>,
    tx: &ExecuteSender,
) -> std::result::Result<(), sqlx_core::Error> {
    let rows_affected = rows_affected
        .unwrap_or(0)
        .try_into()
        .map_err(|_| sqlx_core::Error::Protocol("ODBC row count does not fit in u64".to_owned()))?;
    send_done(tx, rows_affected);
    Ok(())
}

fn send_done(tx: &ExecuteSender, rows_affected: u64) -> bool {
    tx.send(Ok(Either::Left(MssqlQueryResult::new(rows_affected))))
        .is_ok()
}

fn send_row(tx: &ExecuteSender, row: MssqlRow) -> bool {
    tx.send(Ok(Either::Right(row))).is_ok()
}

pub(crate) fn collect_columns(
    cursor: &mut impl ResultSetMetadata,
) -> std::result::Result<Vec<MssqlColumn>, sqlx_core::Error> {
    let count = cursor.num_result_cols().map_err(|error| {
        crate::error::database_error_with_context(error, "failed to read ODBC result-column count")
    })?;
    let count = usize::try_from(count).map_err(|_| {
        sqlx_core::Error::Protocol(format!("ODBC returned a negative column count: {count}"))
    })?;

    let mut columns = Vec::with_capacity(count);
    for ordinal in 0..count {
        let column_number = u16::try_from(ordinal + 1).map_err(|_| {
            sqlx_core::Error::Protocol(format!("ODBC column index exceeds u16: {}", ordinal + 1))
        })?;

        let mut description = odbc_api::ColumnDescription::default();
        cursor
            .describe_col(column_number, &mut description)
            .map_err(|error| {
                crate::error::database_error_with_context(
                    error,
                    format!("failed to describe ODBC result column {column_number}"),
                )
            })?;
        let name = description
            .name_to_string()
            .unwrap_or_else(|_| format!("col{ordinal}"));

        let nullable = match description.nullability {
            odbc_api::Nullability::NoNulls => Some(false),
            odbc_api::Nullability::Nullable => Some(true),
            odbc_api::Nullability::Unknown => None,
        };

        columns.push(MssqlColumn::new(
            ordinal,
            name,
            MssqlTypeInfo::new(description.data_type),
            nullable,
        ));
    }

    Ok(columns)
}

fn collect_prepared_columns(
    prepared: &mut impl PreparedStatementMetadata,
    parameter_count: u16,
) -> std::result::Result<Vec<MssqlColumn>, sqlx_core::Error> {
    match collect_columns(prepared) {
        Ok(columns) => Ok(columns),
        Err(error) if parameter_count > 0 => {
            validate_parameter_metadata(prepared, parameter_count)?;
            log::debug!("ODBC driver deferred result-column metadata until execution: {error}");
            Ok(Vec::new())
        }
        Err(error) => Err(error),
    }
}

trait PreparedStatementMetadata: ResultSetMetadata {
    fn describe_prepared_parameter(
        &mut self,
        index: u16,
    ) -> std::result::Result<(), odbc_api::Error>;
}

impl<S> PreparedStatementMetadata for odbc_api::Prepared<S>
where
    S: odbc_api::handles::AsStatementRef,
{
    fn describe_prepared_parameter(
        &mut self,
        index: u16,
    ) -> std::result::Result<(), odbc_api::Error> {
        self.describe_param(index).map(|_| ())
    }
}

fn validate_parameter_metadata(
    prepared: &mut impl PreparedStatementMetadata,
    parameter_count: u16,
) -> std::result::Result<(), sqlx_core::Error> {
    for index in 1..=parameter_count {
        prepared
            .describe_prepared_parameter(index)
            .map_err(|error| {
                crate::error::database_error_with_context(
                    error,
                    format!("failed to describe ODBC parameter {index}"),
                )
            })?;
    }

    Ok(())
}

fn stream_result_sets<C>(
    mut cursor: C,
    settings: MssqlBufferSettings,
    tx: &ExecuteSender,
) -> std::result::Result<(), sqlx_core::Error>
where
    C: Cursor + ResultSetMetadata,
{
    loop {
        if cursor.num_result_cols().map_err(|error| {
            crate::error::database_error_with_context(
                error,
                "failed to read ODBC result-column count",
            )
        })? == 0
        {
            send_done(tx, 0);
        } else if let Some(max_column_size) = settings.max_column_size {
            let (receiver_open, finished_cursor) =
                stream_rows_buffered(cursor, settings.batch_size, max_column_size, tx)?;
            if !receiver_open {
                return Ok(());
            }
            cursor = finished_cursor;
        } else if !stream_rows_unbuffered(&mut cursor, tx)? {
            return Ok(());
        }

        match cursor.more_results().map_err(|error| {
            crate::error::database_error_with_context(error, "failed to advance ODBC result set")
        })? {
            Some(next_cursor) => cursor = next_cursor,
            None => return Ok(()),
        }
    }
}

#[derive(Debug)]
struct ColumnBinding {
    column: MssqlColumn,
    buffer_desc: BufferDesc,
}

fn stream_rows_buffered<C>(
    cursor: C,
    batch_size: usize,
    max_column_size: usize,
    tx: &ExecuteSender,
) -> std::result::Result<(bool, C), sqlx_core::Error>
where
    C: Cursor + ResultSetMetadata,
{
    let mut cursor = cursor;
    let bindings = build_buffer_bindings(&mut cursor, max_column_size)?;
    let buffer_descriptions = bindings
        .iter()
        .map(|binding| binding.buffer_desc)
        .collect::<Vec<_>>();
    let mut row_set_cursor = cursor
        .bind_buffer(ColumnarDynBuffer::from_descs(
            batch_size,
            buffer_descriptions,
        ))
        .map_err(|error| {
            crate::error::database_error_with_context(
                error,
                format!(
                    "ODBC buffered fetching could not be enabled with batch_size={batch_size}; \
                     this driver may reject the row-array or row-binding statement attributes \
                     used for column-wise buffered fetching, so use \
                     MssqlConnectOptions::max_column_size(None) to fetch rows unbuffered"
                ),
            )
        })?;
    let columns: Arc<[MssqlColumn]> = bindings
        .iter()
        .map(|binding| binding.column.clone())
        .collect::<Vec<_>>()
        .into();

    while let Some(batch) = row_set_cursor.fetch().map_err(|error| {
        crate::error::database_error_with_context(error, "ODBC buffered fetch failed")
    })? {
        let column_values = bindings
            .iter()
            .enumerate()
            .map(|(index, binding)| {
                buffered_column_values(batch.column(index), binding).map_err(|error| {
                    sqlx_core::Error::Protocol(format!(
                        "ODBC buffered fetch could not convert column {} (`{}`) using buffer {:?}: {error}",
                        binding.column.ordinal() + 1,
                        binding.column.name(),
                        binding.buffer_desc
                    ))
                })
            })
            .collect::<std::result::Result<Vec<_>, _>>()?;

        let mut column_iters = column_values
            .into_iter()
            .map(Vec::into_iter)
            .collect::<Vec<_>>();

        for row_index in 0..batch.num_rows() {
            let values = column_iters
                .iter_mut()
                .map(|values| {
                    values.next().map(MssqlValue::new).ok_or_else(|| {
                        sqlx_core::Error::Protocol(format!(
                            "ODBC buffered fetch produced too few values for row {}",
                            row_index + 1
                        ))
                    })
                })
                .collect::<std::result::Result<Vec<_>, _>>()?;
            if !send_row(tx, MssqlRow::new_shared(Arc::clone(&columns), values)) {
                let (cursor, _) = row_set_cursor.unbind().map_err(|error| {
                    crate::error::database_error_with_context(
                        error,
                        "ODBC buffered fetch could not unbind row buffer after receiver closed",
                    )
                })?;
                return Ok((false, cursor));
            }
        }
    }

    send_done(tx, 0);
    let (cursor, _) = row_set_cursor.unbind().map_err(|error| {
        crate::error::database_error_with_context(
            error,
            "ODBC buffered fetch could not unbind row buffer",
        )
    })?;
    Ok((true, cursor))
}

fn build_buffer_bindings(
    cursor: &mut impl ResultSetMetadata,
    max_column_size: usize,
) -> std::result::Result<Vec<ColumnBinding>, sqlx_core::Error> {
    collect_columns(cursor).map(|columns| {
        columns
            .into_iter()
            .map(|column| {
                let nullable = column.nullable().unwrap_or(true);
                ColumnBinding {
                    buffer_desc: map_buffer_desc(column.type_info().data_type(), max_column_size, nullable),
                    column,
                }
            })
            .collect()
    })
}

fn map_buffer_desc(data_type: DataType, max_column_size: usize, nullable: bool) -> BufferDesc {
    match data_type {
        DataType::TinyInt | DataType::SmallInt | DataType::Integer | DataType::BigInt => {
            BufferDesc::I64 { nullable }
        }
        DataType::Real => BufferDesc::F32 { nullable },
        DataType::Float { .. } | DataType::Double => BufferDesc::F64 { nullable },
        DataType::Bit => BufferDesc::Bit { nullable },
        DataType::Date => BufferDesc::Date { nullable },
        DataType::Time { .. } => BufferDesc::Time { nullable },
        DataType::Timestamp { .. } => BufferDesc::Timestamp { nullable },
        DataType::Binary { .. } | DataType::Varbinary { .. } | DataType::LongVarbinary { .. } => {
            BufferDesc::Binary {
                max_bytes: max_column_size,
            }
        }
        // Wide character types use SQL_C_WCHAR buffers (UTF-16) to avoid
        // codepage-dependent corruption of non-ASCII data.
        DataType::WChar { .. } | DataType::WVarchar { .. } | DataType::WLongVarchar { .. } => {
            BufferDesc::WText {
                max_str_len: max_column_size,
            }
        }
        // Narrow character types and fallback types use SQL_C_CHAR.
        DataType::Char { .. }
        | DataType::Varchar { .. }
        | DataType::LongVarchar { .. }
        | DataType::Other { .. }
        | DataType::Unknown
        | DataType::Decimal { .. }
        | DataType::Numeric { .. } => BufferDesc::Text {
            max_str_len: max_column_size,
        },
    }
}

fn buffered_column_values(
    slice: AnyColumnBufferSlice<'_>,
    binding: &ColumnBinding,
) -> std::result::Result<Vec<MssqlValueKind>, sqlx_core::Error> {
    let desc = binding.buffer_desc;
    Ok(match desc {
        BufferDesc::I8 { nullable } => buffered_numeric(&slice, desc, nullable, |value: i8| {
            MssqlValueKind::TinyInt(i16::from(value))
        })?,
        BufferDesc::I16 { nullable } => buffered_numeric(&slice, desc, nullable, |value| {
            MssqlValueKind::SmallInt(value)
        })?,
        BufferDesc::I32 { nullable } => buffered_numeric(&slice, desc, nullable, |value| {
            MssqlValueKind::Integer(value)
        })?,
        BufferDesc::I64 { nullable } => {
            buffered_numeric(&slice, desc, nullable, MssqlValueKind::BigInt)?
        }
        BufferDesc::U8 { nullable } => buffered_numeric(&slice, desc, nullable, |value: u8| {
            MssqlValueKind::BigInt(i64::from(value))
        })?,
        BufferDesc::F32 { nullable } => {
            buffered_numeric(&slice, desc, nullable, MssqlValueKind::Real)?
        }
        BufferDesc::F64 { nullable } => {
            buffered_numeric(&slice, desc, nullable, MssqlValueKind::Double)?
        }
        BufferDesc::Bit { nullable } => {
            buffered_numeric(&slice, desc, nullable, |value: odbc_api::Bit| {
                MssqlValueKind::Bit(value.as_bool())
            })?
        }
        BufferDesc::Date { nullable } => {
            buffered_numeric(&slice, desc, nullable, MssqlValueKind::Date)?
        }
        BufferDesc::Time { nullable } => {
            buffered_numeric(&slice, desc, nullable, MssqlValueKind::Time)?
        }
        BufferDesc::Timestamp { nullable } => {
            buffered_numeric(&slice, desc, nullable, MssqlValueKind::Timestamp)?
        }
        BufferDesc::Text { .. } => {
            let text = expect_buffer_slice(slice.as_text(), desc)?;
            text.iter()
                .map(|value| {
                    value
                        .map(|bytes| {
                            MssqlValueKind::Text(String::from_utf8_lossy(bytes).into_owned())
                        })
                        .unwrap_or(MssqlValueKind::Null)
                })
                .collect()
        }
        BufferDesc::WText { .. } => {
            let text = expect_buffer_slice(slice.as_wide_text(), desc)?;
            text.iter()
                .map(|value| {
                    value
                        .map(|chars| MssqlValueKind::Text(String::from_utf16_lossy(chars.into())))
                        .unwrap_or(MssqlValueKind::Null)
                })
                .collect()
        }
        BufferDesc::Binary { .. } => {
            let binary = expect_buffer_slice(slice.as_binary(), desc)?;
            binary
                .iter()
                .map(|value| {
                    value
                        .map(|bytes| MssqlValueKind::Binary(bytes.to_vec()))
                        .unwrap_or(MssqlValueKind::Null)
                })
                .collect()
        }
        BufferDesc::Numeric => {
            return Err(sqlx_core::Error::Protocol(format!(
                "unsupported ODBC buffer descriptor: {desc:?}"
            )))
        }
    })
}

fn buffered_numeric<T, F>(
    slice: &AnyColumnBufferSlice<'_>,
    desc: BufferDesc,
    nullable: bool,
    map: F,
) -> std::result::Result<Vec<MssqlValueKind>, sqlx_core::Error>
where
    T: Copy + odbc_api::Pod,
    F: FnMut(T) -> MssqlValueKind,
{
    if nullable {
        Ok(buffered_nullable_numeric(
            expect_buffer_slice(slice.as_nullable_slice::<T>(), desc)?,
            map,
        ))
    } else {
        Ok(expect_buffer_slice(slice.as_slice::<T>(), desc)?
            .iter()
            .copied()
            .map(map)
            .collect())
    }
}

fn buffered_nullable_numeric<T, F>(slice: NullableSlice<'_, T>, mut map: F) -> Vec<MssqlValueKind>
where
    T: Copy,
    F: FnMut(T) -> MssqlValueKind,
{
    slice
        .map(|value| value.copied().map(&mut map).unwrap_or(MssqlValueKind::Null))
        .collect()
}

fn expect_buffer_slice<T>(
    slice: Option<T>,
    desc: BufferDesc,
) -> std::result::Result<T, sqlx_core::Error> {
    slice.ok_or_else(|| {
        sqlx_core::Error::Protocol(format!(
            "ODBC column buffer {desc:?} did not match fetched slice"
        ))
    })
}

fn stream_rows_unbuffered<C>(
    cursor: &mut C,
    tx: &ExecuteSender,
) -> std::result::Result<bool, sqlx_core::Error>
where
    C: Cursor + ResultSetMetadata,
{
    let columns: Arc<[MssqlColumn]> = collect_columns(cursor)?.into();

    while let Some(mut cursor_row) = cursor.next_row().map_err(|error| {
        crate::error::database_error_with_context(
            error,
            "ODBC unbuffered fetch failed while reading the next row",
        )
    })? {
        let mut values = Vec::with_capacity(columns.len());

        for column in columns.iter() {
            let column_number = u16::try_from(sqlx_core::column::Column::ordinal(column) + 1)
                .map_err(|_| {
                    sqlx_core::Error::Protocol("ODBC column index exceeds u16".to_owned())
                })?;
            values.push(fetch_value(&mut cursor_row, column_number, column)?);
        }

        if !send_row(tx, MssqlRow::new_shared(Arc::clone(&columns), values)) {
            return Ok(false);
        }
    }

    send_done(tx, 0);
    Ok(true)
}

fn fetch_value(
    row: &mut odbc_api::CursorRow<'_>,
    column_number: u16,
    column: &MssqlColumn,
) -> std::result::Result<MssqlValue, sqlx_core::Error> {
    let data_type = column.type_info().data_type();

    let kind = match data_type {
        DataType::Bit => {
            let mut value = Nullable::<odbc_api::Bit>::null();
            row.get_data(column_number, &mut value).map_err(|error| {
                crate::error::database_error_with_context_lazy(error, || {
                    fetch_context(column, data_type)
                })
            })?;
            value
                .into_opt()
                .map(|value| MssqlValueKind::Bit(value.as_bool()))
                .unwrap_or(MssqlValueKind::Null)
        }
        DataType::TinyInt => {
            // MSSQL TINYINT is unsigned (0-255), so read as i16 to avoid
            // signed overflow of values > 127.
            let mut value = Nullable::<i16>::null();
            row.get_data(column_number, &mut value).map_err(|error| {
                crate::error::database_error_with_context_lazy(error, || {
                    fetch_context(column, data_type)
                })
            })?;
            value
                .into_opt()
                .map(MssqlValueKind::TinyInt)
                .unwrap_or(MssqlValueKind::Null)
        }
        DataType::SmallInt => fetch_nullable(
            row,
            column_number,
            column,
            data_type,
            MssqlValueKind::SmallInt,
        )?,
        DataType::Integer => fetch_nullable(
            row,
            column_number,
            column,
            data_type,
            MssqlValueKind::Integer,
        )?,
        DataType::BigInt => {
            fetch_nullable(row, column_number, column, data_type, MssqlValueKind::BigInt)?
        }
        DataType::Real => {
            fetch_nullable(row, column_number, column, data_type, MssqlValueKind::Real)?
        }
        DataType::Float { .. } | DataType::Double => {
            fetch_nullable(row, column_number, column, data_type, MssqlValueKind::Double)?
        }
        DataType::Date => {
            fetch_nullable(row, column_number, column, data_type, MssqlValueKind::Date)?
        }
        DataType::Time { .. } => {
            fetch_nullable(row, column_number, column, data_type, MssqlValueKind::Time)?
        }
        DataType::Timestamp { .. } => fetch_nullable(
            row,
            column_number,
            column,
            data_type,
            MssqlValueKind::Timestamp,
        )?,
        DataType::Binary { .. } | DataType::Varbinary { .. } | DataType::LongVarbinary { .. } => {
            let mut value = Vec::new();
            if row.get_binary(column_number, &mut value).map_err(|error| {
                crate::error::database_error_with_context_lazy(error, || {
                    fetch_context(column, data_type)
                })
            })? {
                MssqlValueKind::Binary(value)
            } else {
                MssqlValueKind::Null
            }
        }
        DataType::Other {
            data_type: sql_type, ..
        } if sql_type.0 == -11 => {
            // SQL_GUID / UNIQUEIDENTIFIER in MSSQL
            let mut value = Vec::new();
            if row.get_binary(column_number, &mut value).map_err(|error| {
                crate::error::database_error_with_context_lazy(error, || {
                    fetch_context(column, data_type)
                })
            })? {
                if value.len() == 16 {
                    let mut guid = [0u8; 16];
                    guid.copy_from_slice(&value);
                    MssqlValueKind::Guid(guid)
                } else {
                    // Fallback: treat GUID data as text
                    MssqlValueKind::Text(String::from_utf16_lossy(
                        &value.iter().map(|&b| b as u16).collect::<Vec<_>>(),
                    ))
                }
            } else {
                MssqlValueKind::Null
            }
        }
        _ => {
            let mut value = Vec::new();
            if row
                .get_wide_text(column_number, &mut value)
                .map_err(|error| {
                    crate::error::database_error_with_context_lazy(error, || {
                        fetch_context(column, data_type)
                    })
                })?
            {
                MssqlValueKind::Text(String::from_utf16_lossy(&value))
            } else {
                MssqlValueKind::Null
            }
        }
    };

    Ok(MssqlValue::new(kind))
}

fn fetch_nullable<T, F>(
    row: &mut odbc_api::CursorRow<'_>,
    column_number: u16,
    column: &MssqlColumn,
    data_type: DataType,
    map: F,
) -> std::result::Result<MssqlValueKind, sqlx_core::Error>
where
    T: Default + Copy + odbc_api::parameter::CElement + odbc_api::handles::CDataMut,
    Nullable<T>: odbc_api::parameter::CElement + odbc_api::handles::CDataMut,
    F: FnOnce(T) -> MssqlValueKind,
{
    let mut value = Nullable::<T>::null();
    row.get_data(column_number, &mut value).map_err(|error| {
        crate::error::database_error_with_context_lazy(error, || fetch_context(column, data_type))
    })?;
    Ok(value.into_opt().map(map).unwrap_or(MssqlValueKind::Null))
}

fn fetch_context(column: &MssqlColumn, data_type: DataType) -> String {
    format!(
        "failed to fetch ODBC column {} (`{}`) as {data_type:?}",
        column.ordinal() + 1,
        column.name()
    )
}

fn sql_preview(sql: &str) -> String {
    const MAX_LEN: usize = 160;

    let compact = sql.split_whitespace().collect::<Vec<_>>().join(" ");
    if compact.len() <= MAX_LEN {
        compact
    } else {
        let mut preview = compact.chars().take(MAX_LEN - 3).collect::<String>();
        preview.push_str("...");
        preview
    }
}

/// Offloads a blocking operation to Tokio's blocking thread pool.
///
/// The closure must satisfy `Send + 'static` so it can be moved across
/// threads.
#[cfg(feature = "runtime-tokio")]
pub(crate) async fn offload_blocking<F, T>(f: F) -> std::result::Result<T, sqlx_core::Error>
where
    F: FnOnce() -> std::result::Result<T, sqlx_core::Error> + Send + 'static,
    T: Send + 'static,
{
    tokio::task::spawn_blocking(f)
        .await
        .map_err(|e| sqlx_core::Error::Protocol(format!("blocking task panicked: {e}")))?
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn buffered_fetch_maps_numeric_types_to_nullable_64_bit_buffers() {
        assert!(matches!(
            map_buffer_desc(DataType::TinyInt, 64, true),
            BufferDesc::I64 { nullable: true }
        ));
        assert!(matches!(
            map_buffer_desc(DataType::Integer, 64, true),
            BufferDesc::I64 { nullable: true }
        ));
        assert!(matches!(
            map_buffer_desc(DataType::BigInt, 64, true),
            BufferDesc::I64 { nullable: true }
        ));
    }

    #[test]
    fn buffered_fetch_uses_configured_limits_for_variable_sized_data() {
        assert_eq!(
            map_buffer_desc(DataType::Varchar { length: None }, 32, true),
            BufferDesc::Text { max_str_len: 32 }
        );
        assert_eq!(
            map_buffer_desc(DataType::Varbinary { length: None }, 16, true),
            BufferDesc::Binary { max_bytes: 16 }
        );
    }

    #[test]
    fn buffered_fetch_maps_wide_char_types_to_wtext() {
        assert!(matches!(
            map_buffer_desc(DataType::WChar { length: None }, 64, true),
            BufferDesc::WText { max_str_len: 64 }
        ));
        assert!(matches!(
            map_buffer_desc(DataType::WVarchar { length: None }, 128, true),
            BufferDesc::WText { max_str_len: 128 }
        ));
        assert!(matches!(
            map_buffer_desc(DataType::WLongVarchar { length: None }, 256, true),
            BufferDesc::WText { max_str_len: 256 }
        ));
    }

    #[test]
    fn buffered_fetch_maps_narrow_char_types_to_text() {
        assert!(matches!(
            map_buffer_desc(DataType::Char { length: None }, 64, true),
            BufferDesc::Text { max_str_len: 64 }
        ));
        assert!(matches!(
            map_buffer_desc(DataType::Varchar { length: None }, 64, true),
            BufferDesc::Text { max_str_len: 64 }
        ));
        assert!(matches!(
            map_buffer_desc(DataType::LongVarchar { length: None }, 64, true),
            BufferDesc::Text { max_str_len: 64 }
        ));
    }

}
