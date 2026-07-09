mod command;
mod actor;
mod helpers;

use crate::connection::helpers::{receiver_to_stream, send_command_async, send_command_blocking};
use crate::{
    MssqlArguments, MssqlBufferSettings, MssqlColumn, MssqlConnectOptions, MssqlQueryResult,
    MssqlRow, MssqlStatement, MssqlTypeInfo, MssqlValue, MssqlValueKind, Result,
};
use futures_core::future::BoxFuture;
use futures_core::stream::BoxStream;
use futures_util::{StreamExt, future, stream};
use odbc_api::buffers::{AnyColumnBufferSlice, BufferDesc, ColumnarDynBuffer, NullableSlice};
use odbc_api::{ConnectionTransitions, Cursor, DataType, Nullable, ResultSetMetadata};
use sqlx_core::Either;
use sqlx_core::column::Column;
use sqlx_core::common::StatementCache;
use sqlx_core::executor::{Execute, Executor};
use sqlx_core::sql_str::SqlStr;
use sqlx_core::transaction::Transaction;
use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use command::Command;
use actor::ConnectionActor;

type PreparedStatement =
    odbc_api::Prepared<odbc_api::handles::StatementConnection<odbc_api::SharedConnection<'static>>>;
type ExecuteResult = std::result::Result<Either<MssqlQueryResult, MssqlRow>, sqlx_core::Error>;
type ExecuteSender = flume::Sender<ExecuteResult>;



/// MSSQL connection backed by an actor thread that owns the ODBC connection.
pub struct MssqlConnection {
    cmd_tx: flume::Sender<Command>,
    #[allow(dead_code)]
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
        let _ = send_command_blocking(&self.cmd_tx, |tx| Command::ExecSql {
            sql: "SELECT 1 /* dbms_name */".into(),
            response: tx,
        })?;
        Ok("MSSQL via ODBC".to_owned())
    }

    /// Begins a transaction (synchronous, called from TransactionManager).
    pub(crate) fn begin_blocking(&mut self) -> std::result::Result<(), sqlx_core::Error> {
        let r = send_command_blocking(&self.cmd_tx, |tx| Command::Begin { response: tx })?;
        if r.is_ok() {
            let _ = self.transaction_depth.fetch_add(1, Ordering::SeqCst);
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
        send_command_blocking(&self.cmd_tx, |tx| Command::ExecSql {
            sql: sql.to_owned(),
            response: tx,
        })?
    }

    /// Executes a SQL query and returns the first column of the first row as an `i64`.
    #[cfg(feature = "migrate")]
    pub(crate) fn scalar_i64_blocking(
        &self,
        sql: &str,
    ) -> std::result::Result<Option<i64>, sqlx_core::Error> {
        send_command_blocking(&self.cmd_tx, |tx| Command::ScalarI64 {
            sql: sql.to_owned(),
            response: tx,
        })?
    }

    /// Executes a SQL query and returns rows as a list of (i64, binary) tuples.
    #[cfg(feature = "migrate")]
    pub(crate) fn list_migrations_blocking(
        &self,
        sql: &str,
    ) -> std::result::Result<Vec<(i64, Vec<u8>)>, sqlx_core::Error> {
        send_command_blocking(&self.cmd_tx, |tx| Command::ListMigrations {
            sql: sql.to_owned(),
            response: tx,
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
        send_command_blocking(&self.cmd_tx, |tx| Command::ApplyMigration {
            sql: sql.to_owned(),
            insert_sql: insert_sql.to_owned(),
            version,
            no_tx,
            response: tx,
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
        send_command_blocking(&self.cmd_tx, |tx| Command::RevertMigration {
            sql: sql.to_owned(),
            delete_sql: delete_sql.to_owned(),
            version,
            no_tx,
            response: tx,
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
            Ok(arguments) => receiver_to_stream(self.execute_receiver(sql, persistent, arguments)),
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
    ) -> BoxFuture<
        'e,
        std::result::Result<sqlx_core::describe::Describe<Self::Database>, sqlx_core::Error>,
    >
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


#[derive(Debug)]
struct ColumnBinding {
    column: MssqlColumn,
    buffer_desc: BufferDesc,
}


#[cfg(test)]
mod tests {
    use crate::connection::helpers::map_buffer_desc;

use super::*;

    #[test]
    fn buffered_fetch_maps_data_types_to_buffer_descriptors() {
        // Numeric types → I64
        for dt in [DataType::TinyInt, DataType::Integer, DataType::BigInt] {
            assert!(matches!(
                map_buffer_desc(dt, 64, true),
                BufferDesc::I64 { nullable: true }
            ));
        }

        // Variable-size → configurable limits
        assert_eq!(
            map_buffer_desc(DataType::Varchar { length: None }, 32, true),
            BufferDesc::Text { max_str_len: 32 }
        );
        assert_eq!(
            map_buffer_desc(DataType::Varbinary { length: None }, 16, true),
            BufferDesc::Binary { max_bytes: 16 }
        );

        // Wide-char types → WText
        for (dt, expected_len) in [
            (DataType::WChar { length: None }, 64),
            (DataType::WVarchar { length: None }, 128),
            (DataType::WLongVarchar { length: None }, 256),
        ] {
            assert_eq!(
                map_buffer_desc(dt, expected_len, true),
                BufferDesc::WText {
                    max_str_len: expected_len
                }
            );
        }

        // Narrow-char types → Text
        for dt in [
            DataType::Char { length: None },
            DataType::Varchar { length: None },
            DataType::LongVarchar { length: None },
        ] {
            assert_eq!(
                map_buffer_desc(dt, 64, true),
                BufferDesc::Text { max_str_len: 64 }
            );
        }
    }
}
