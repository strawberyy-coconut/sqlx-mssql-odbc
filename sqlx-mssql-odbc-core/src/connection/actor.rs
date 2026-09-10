use crate::connection::helpers::{collect_prepared_columns, drive_result_sets, sql_preview};
#[cfg(feature = "migrate")]
use crate::connection::helpers::drain_result_sets;
use crate::connection::{ExecuteSender, PreparedStatement};
use crate::{MssqlArguments, MssqlBufferSettings, MssqlStatement, MssqlTypeInfo};

use odbc_api::handles::AsStatementRef;
use odbc_api::{ConnectionTransitions, Cursor, Nullable};

use super::command::Command;
use sqlx_core::common::StatementCache;
use sqlx_core::sql_str::SqlStr;

// ============================================================================
// ConnectionActor — owns the ODBC connection on a dedicated blocking thread
// ============================================================================

pub struct ConnectionActor {
    pub conn: odbc_api::SharedConnection<'static>,
    pub stmt_cache: StatementCache<PreparedStatement>,
    pub transaction_depth: usize,
    pub buffer_settings: MssqlBufferSettings,
}

impl ConnectionActor {
    pub fn run(mut self, rx: flume::Receiver<Command>) {
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
                    if let Err(error) = self.handle_execute(sql, args, persistent, &response) {
                        // `handle_execute` reports results through `response`.
                        // When it fails early (e.g. the statement itself failed
                        // to execute) nothing has been sent yet, so discarding
                        // the error would leave the consumer with an empty
                        // stream and a silent success. Forward it instead.
                        let _ = response.send(Err(error));
                    }
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
                Command::StatementCacheSize { response } => {
                    let _ = response.send(self.stmt_cache.len());
                }
                Command::ClearStatementCache { response } => {
                    self.stmt_cache.clear();
                    let _ = response.send(());
                }
                #[cfg(feature = "migrate")]
                Command::ApplyMigration {
                    sql,
                    insert_sql,
                    version,
                    no_tx,
                    response,
                } => {
                    let _ = response.send(self.handle_apply_migration(
                        &sql,
                        &insert_sql,
                        version,
                        no_tx,
                    ));
                }
                #[cfg(feature = "migrate")]
                Command::RevertMigration {
                    sql,
                    delete_sql,
                    version,
                    no_tx,
                    response,
                } => {
                    let _ = response.send(self.handle_revert_migration(
                        &sql,
                        &delete_sql,
                        version,
                        no_tx,
                    ));
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
                let opt_cursor = {
                    let conn_guard = self.conn.lock().map_err(|_| {
                        sqlx_core::Error::Protocol(
                            "ODBC execute: failed to lock connection".to_owned(),
                        )
                    })?;
                    let opt_cursor = prepared.execute(parameters.as_slice()).map_err(|error| {
                        crate::error::database_error_with_context_lazy(error, || {
                            format!(
                                "failed to execute cached ODBC statement: `{}`",
                                sql_preview(sql.as_str())
                            )
                        })
                    })?;
                    drop(conn_guard);
                    opt_cursor
                };

                // Walk every result set of the batch. A `None` cursor only
                // means the first result carried no columns (e.g. a leading
                // DML statement) — later result sets and diagnostics still
                // have to be read. `into_stmt` keeps the cursor open, so the
                // handle is inspected from the first result.
                if opt_cursor.is_none() {
                    // Drop the empty cursor first so the borrow on `prepared`
                    // ends before taking a fresh statement reference.
                    drop(opt_cursor);
                    let mut handle = prepared.as_stmt_ref();
                    return drive_result_sets(&mut handle, self.buffer_settings, tx);
                }
                let cursor = opt_cursor.expect("checked `is_none` above");
                let mut handle = cursor.into_stmt();
                return drive_result_sets(&mut handle, self.buffer_settings, tx);
            } else {
                // Prepare and cache
                let mut prepared =
                    self.conn
                        .clone()
                        .into_prepared(sql.as_str())
                        .map_err(|error| {
                            crate::error::database_error_with_context_lazy(error, || {
                                format!(
                                    "failed to prepare cached ODBC statement: `{}`",
                                    sql_preview(sql.as_str())
                                )
                            })
                        })?;

                // Execute exactly once. Re-executing would double-run
                // INSERT/UPDATE/DELETE (causing duplicate-key violations on
                // otherwise-empty tables).
                let opt_cursor = {
                    let conn_guard = self.conn.lock().map_err(|_| {
                        sqlx_core::Error::Protocol(
                            "ODBC execute: failed to lock connection".to_owned(),
                        )
                    })?;
                    let opt_cursor = prepared.execute(parameters.as_slice()).map_err(|error| {
                        crate::error::database_error_with_context_lazy(error, || {
                            format!(
                                "failed to execute cached ODBC statement: `{}`",
                                sql_preview(sql.as_str())
                            )
                        })
                    })?;
                    drop(conn_guard);
                    opt_cursor
                };

                if opt_cursor.is_none() {
                    // Drop the empty cursor first so the borrow on `prepared`
                    // ends before taking a fresh statement reference.
                    drop(opt_cursor);
                    let outcome = {
                        let mut handle = prepared.as_stmt_ref();
                        drive_result_sets(&mut handle, self.buffer_settings, tx)
                    };
                    if outcome.is_ok() {
                        self.stmt_cache.insert(sql.as_str(), prepared);
                    }
                    return outcome;
                }

                let cursor = opt_cursor.expect("checked `is_none` above");
                let mut handle = cursor.into_stmt();
                // The cursor borrows the prepared statement, so it is not
                // cached on this path (that happens in `handle_prepare`).
                return drive_result_sets(&mut handle, self.buffer_settings, tx);
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
            let cursor = statement
                .execute(sql.as_str(), parameters.as_slice())
                .map_err(|error| {
                    crate::error::database_error_with_context_lazy(error, || {
                        format!(
                            "failed to execute ODBC query: `{}`",
                            sql_preview(sql.as_str())
                        )
                    })
                })?;

            // A `None` cursor only means the first result had no columns; the
            // batch must still be walked for later results and errors.
            if cursor.is_none() {
                drop(cursor);
                let mut handle = statement.into_handle();
                return drive_result_sets(&mut handle, self.buffer_settings, tx);
            }
            let cursor = cursor.expect("checked `is_none` above");
            let mut handle = cursor.into_stmt();
            drive_result_sets(&mut handle, self.buffer_settings, tx)
        }
    }

    fn handle_prepare(
        &mut self,
        sql: SqlStr,
    ) -> std::result::Result<MssqlStatement, sqlx_core::Error> {
        // Helper to collect parameters
        fn collect_param_types(
            prepared: &mut PreparedStatement,
        ) -> std::result::Result<
            Option<sqlx_core::Either<Vec<MssqlTypeInfo>, usize>>,
            sqlx_core::Error,
        > {
            let count = prepared.num_params().map_err(|error| {
                crate::error::database_error_with_context(
                    error,
                    "failed to read ODBC parameter count",
                )
            })?;

            if count == 0 {
                return Ok(None);
            }

            let mut types = Vec::with_capacity(count as usize);
            for i in 1..=count {
                match prepared.describe_param(i) {
                    Ok(column_type) => {
                        types.push(MssqlTypeInfo::new(column_type.data_type));
                    }
                    Err(_) => {
                        // If describe_param fails for any parameter,
                        // fall back to count-only mode
                        return Ok(Some(sqlx_core::Either::Right(count as usize)));
                    }
                }
            }

            Ok(Some(sqlx_core::Either::Left(types)))
        }

        if let Some(prepared) = self.stmt_cache.get_mut(sql.as_str()) {
            let parameters = collect_param_types(prepared)?;
            let columns = collect_prepared_columns(prepared)?;
            return Ok(MssqlStatement::new(sql, columns, parameters));
        }

        let mut prepared = self
            .conn
            .clone()
            .into_prepared(sql.as_str())
            .map_err(|error| {
                sqlx_core::Error::from(crate::error::database_error_with_context(
                    error,
                    format!(
                        "failed to prepare MSSQL ODBC statement: `{}`",
                        sql_preview(sql.as_str())
                    ),
                ))
            })?;

        let parameters = collect_param_types(&mut prepared)?;
        let columns = collect_prepared_columns(&mut prepared)?;

        if self.stmt_cache.is_enabled() {
            self.stmt_cache.insert(sql.as_str(), prepared);
        }

        Ok(MssqlStatement::new(sql, columns, parameters))
    }

    fn handle_ping(&mut self) -> std::result::Result<(), sqlx_core::Error> {
        let conn_guard = self
            .conn
            .lock()
            .map_err(|_| sqlx_core::Error::Protocol("failed to lock connection for ping".into()))?;
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
            let conn_guard = self.conn.lock().map_err(|_| {
                sqlx_core::Error::Protocol("MSSQL ODBC begin: failed to lock connection".to_owned())
            })?;
            conn_guard.set_autocommit(false).map_err(|error| {
                sqlx_core::Error::from(crate::error::database_error_with_context(
                    error,
                    "failed to disable ODBC autocommit while beginning a transaction",
                ))
            })?;
        } else {
            let savepoint = format!("sqlx_sp_{}", self.transaction_depth);
            let conn_guard = self.conn.lock().map_err(|_| {
                sqlx_core::Error::Protocol(
                    "MSSQL ODBC begin (savepoint): failed to lock connection".to_owned(),
                )
            })?;
            conn_guard
                .execute(&format!("SAVE TRANSACTION {savepoint}"), (), None)
                .map_err(|error| {
                    sqlx_core::Error::from(crate::error::database_error_with_context(
                        error,
                        format!("failed to create save point `{savepoint}` for nested transaction"),
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
            let conn_guard = self.conn.lock().map_err(|_| {
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
            let conn_guard = self.conn.lock().map_err(|_| {
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
            let conn_guard = self.conn.lock().map_err(|_| {
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
            if let Ok(conn_guard) = self.conn.lock() {
                let _ = conn_guard.rollback();
                let _ = conn_guard.set_autocommit(true);
            }
            self.transaction_depth = 0;
        } else {
            let savepoint = format!("sqlx_sp_{}", self.transaction_depth - 1);
            if let Ok(conn_guard) = self.conn.lock() {
                let _ = conn_guard.execute(&format!("ROLLBACK TRANSACTION {savepoint}"), (), None);
            }
            self.transaction_depth -= 1;
        }
    }

    fn handle_exec_sql(&self, sql: &str) -> std::result::Result<(), sqlx_core::Error> {
        let conn_guard = self.conn.lock().map_err(|_| {
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
        let conn_guard = self.conn.lock().map_err(|_| {
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
        let conn_guard = self.conn.lock().map_err(|_| {
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
                sqlx_core::Error::Protocol("list_applied_migrations returned no result set".into())
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
        let conn_guard = self.conn.lock().map_err(|_| {
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

        // Run the whole migration batch and drain every result set: ODBC only
        // reports a statement that fails after the first one once the results
        // are advanced, so the batch must be walked before the migration is
        // recorded as applied.
        let batch = (|| -> std::result::Result<(), sqlx_core::Error> {
            let mut statement = conn_guard.preallocate().map_err(|error| {
                sqlx_core::Error::from(crate::error::database_error_with_context(
                    error,
                    format!("failed to allocate statement for migration {version}"),
                ))
            })?;
            let cursor = statement.execute(sql, ()).map_err(|error| {
                sqlx_core::Error::from(crate::error::database_error_with_context(
                    error,
                    format!("migration {version} failed"),
                ))
            })?;

            if cursor.is_none() {
                drop(cursor);
                let mut handle = statement.into_handle();
                return drain_result_sets(&mut handle);
            }
            let cursor = cursor.expect("checked `is_none` above");
            let mut handle = cursor.into_stmt();
            drain_result_sets(&mut handle)
        })();

        if let Err(error) = batch {
            if !no_tx {
                // Don't leave the connection inside a half-applied transaction.
                let _ = conn_guard.rollback();
                let _ = conn_guard.set_autocommit(true);
            }
            return Err(error);
        }

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
        let conn_guard = self.conn.lock().map_err(|_| {
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

        // See `handle_apply_migration`: the whole batch must be drained so a
        // statement that fails after the first one is surfaced.
        let batch = (|| -> std::result::Result<(), sqlx_core::Error> {
            let mut statement = conn_guard.preallocate().map_err(|error| {
                sqlx_core::Error::from(crate::error::database_error_with_context(
                    error,
                    format!("failed to allocate statement for revert migration {version}"),
                ))
            })?;
            let cursor = statement.execute(sql, ()).map_err(|error| {
                sqlx_core::Error::from(crate::error::database_error_with_context(
                    error,
                    format!("revert migration {version} failed"),
                ))
            })?;

            if cursor.is_none() {
                drop(cursor);
                let mut handle = statement.into_handle();
                return drain_result_sets(&mut handle);
            }
            let cursor = cursor.expect("checked `is_none` above");
            let mut handle = cursor.into_stmt();
            drain_result_sets(&mut handle)
        })();

        if let Err(error) = batch {
            if !no_tx {
                // Don't leave the connection inside a half-reverted transaction.
                let _ = conn_guard.rollback();
                let _ = conn_guard.set_autocommit(true);
            }
            return Err(error);
        }

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
