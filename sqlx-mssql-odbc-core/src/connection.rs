use crate::{
    MssqlArguments, MssqlBufferSettings, MssqlColumn, MssqlConnectOptions, MssqlParameterCollection,
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
use sqlx_core::transaction::Transaction;
use sqlx_core::Either;
use std::future::Future;
use std::sync::{Arc, Mutex};

type PreparedStatement =
    odbc_api::Prepared<odbc_api::handles::StatementConnection<odbc_api::SharedConnection<'static>>>;
type SharedPreparedStatement = Arc<Mutex<PreparedStatement>>;
type ExecuteResult = std::result::Result<Either<MssqlQueryResult, MssqlRow>, sqlx_core::Error>;
type ExecuteSender = flume::Sender<ExecuteResult>;

/// Blocking MSSQL ODBC connection wrapper.
pub struct MssqlConnection {
    conn: odbc_api::SharedConnection<'static>,
    stmt_cache: StatementCache<SharedPreparedStatement>,
    buffer_settings: MssqlBufferSettings,
    transaction_depth: usize,
}

impl std::fmt::Debug for MssqlConnection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MssqlConnection").finish_non_exhaustive()
    }
}

impl MssqlConnection {
    /// Opens a blocking MSSQL ODBC connection with the provided options.
    pub fn connect_blocking(options: &MssqlConnectOptions) -> Result<Self> {
        let env = odbc_api::environment().map_err(|error| {
            crate::MssqlError::Configuration(format!(
                "failed to initialize the process-wide ODBC environment: {error}"
            ))
        })?;

        let conn = env
            .connect_with_connection_string(options.connection_string(), Default::default())
            .map_err(|error| {
                crate::error::database_error_with_context(
                    error,
                    "failed to open MSSQL ODBC connection using the supplied connection string",
                )
            })?;

        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
            stmt_cache: StatementCache::new(options.statement_cache_capacity),
            buffer_settings: options.buffer_settings,
            transaction_depth: 0,
        })
    }

    /// Executes a minimal connectivity query.
    /// Executes a simple connectivity check.
    pub fn ping_blocking(&mut self) -> std::result::Result<(), sqlx_core::Error> {
        self.with_conn("ping", |conn| {
            conn.execute("SELECT 1", (), None).map_err(|error| {
                sqlx_core::Error::from(crate::error::database_error_with_context(
                    error,
                    "MSSQL ping query failed: `SELECT 1`",
                ))
            })?;
            Ok(())
        })
    }

    fn with_conn<R>(
        &self,
        operation: &str,
        f: impl FnOnce(&mut odbc_api::Connection<'static>) -> std::result::Result<R, sqlx_core::Error>,
    ) -> std::result::Result<R, sqlx_core::Error> {
        let mut conn = self.conn.lock().map_err(|_| {
            sqlx_core::Error::Protocol(format!("MSSQL ODBC {operation}: failed to lock connection"))
        })?;
        f(&mut conn)
    }

    /// Returns the DBMS name reported by the ODBC driver.
    pub fn dbms_name(&self) -> std::result::Result<String, sqlx_core::Error> {
        self.with_conn("dbms_name", |conn| {
            conn.database_management_system_name().map_err(|error| {
                sqlx_core::Error::from(crate::error::database_error_with_context(
                    error,
                    "failed to read the DBMS name from SQLGetInfo",
                ))
            })
        })
    }

    pub(crate) fn begin_blocking(&mut self) -> std::result::Result<(), sqlx_core::Error> {
        if self.transaction_depth > 0 {
            return Err(sqlx_core::Error::InvalidSavePointStatement);
        }

        self.with_conn("begin", |conn| {
            conn.set_autocommit(false).map_err(|error| {
                sqlx_core::Error::from(crate::error::database_error_with_context(
                    error,
                    "failed to disable ODBC autocommit while beginning a transaction",
                ))
            })
        })?;
        self.transaction_depth = 1;
        Ok(())
    }

    pub(crate) fn commit_blocking(&mut self) -> std::result::Result<(), sqlx_core::Error> {
        if self.transaction_depth == 0 {
            return Ok(());
        }

        self.with_conn("commit", |conn| {
            conn.commit().map_err(|error| {
                sqlx_core::Error::from(crate::error::database_error_with_context(
                    error,
                    "failed to commit the active MSSQL ODBC transaction",
                ))
            })?;
            conn.set_autocommit(true).map_err(|error| {
                sqlx_core::Error::from(crate::error::database_error_with_context(
                    error,
                    "failed to restore ODBC autocommit after commit",
                ))
            })
        })?;
        self.transaction_depth = 0;
        Ok(())
    }

    pub(crate) fn rollback_blocking(&mut self) -> std::result::Result<(), sqlx_core::Error> {
        if self.transaction_depth == 0 {
            return Ok(());
        }

        self.with_conn("rollback", |conn| {
            conn.rollback().map_err(|error| {
                sqlx_core::Error::from(crate::error::database_error_with_context(
                    error,
                    "failed to roll back the active ODBC transaction",
                ))
            })?;
            conn.set_autocommit(true).map_err(|error| {
                sqlx_core::Error::from(crate::error::database_error_with_context(
                    error,
                    "failed to restore ODBC autocommit after rollback",
                ))
            })
        })?;
        self.transaction_depth = 0;
        Ok(())
    }

    pub(crate) fn start_rollback(&mut self) {
        if self.transaction_depth == 0 {
            return;
        }

        if self
            .with_conn("start_rollback", |conn| {
                conn.rollback().map_err(|error| {
                    sqlx_core::Error::from(crate::error::database_error_with_context(
                        error,
                        "failed to roll back the active ODBC transaction",
                    ))
                })?;
                conn.set_autocommit(true).map_err(|error| {
                    sqlx_core::Error::from(crate::error::database_error_with_context(
                        error,
                        "failed to restore ODBC autocommit after rollback",
                    ))
                })
            })
            .is_ok()
        {
            self.transaction_depth = 0;
        }
    }

    pub(crate) const fn transaction_depth(&self) -> usize {
        self.transaction_depth
    }

    /// Prepares a statement and returns the metadata reported by the ODBC driver.
    pub fn prepare_blocking(
        &mut self,
        sql: sqlx_core::sql_str::SqlStr,
    ) -> std::result::Result<MssqlStatement, sqlx_core::Error> {
        if let Some(prepared) = self
            .stmt_cache
            .get_mut(sql.as_str())
            .map(|prepared| Arc::clone(&*prepared))
        {
            let mut prepared = prepared.lock().map_err(|_| {
                sqlx_core::Error::Protocol(
                    "MSSQL ODBC prepare: failed to lock cached statement".to_owned(),
                )
            })?;
            let parameters = prepared.num_params().map_err(|error| {
                sqlx_core::Error::from(crate::error::database_error_with_context(
                    error,
                    format!(
                        "failed to read ODBC parameter metadata for cached statement: `{}`",
                        sql_preview(sql.as_str())
                    ),
                ))
            })?;
            let columns = collect_prepared_columns(&mut *prepared, parameters)?;

            return Ok(MssqlStatement::new(sql, columns, usize::from(parameters)));
        }

        let mut prepared = Arc::clone(&self.conn)
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
            self.stmt_cache
                .insert(sql.as_str(), Arc::new(Mutex::new(prepared)));
        }

        Ok(MssqlStatement::new(sql, columns, usize::from(parameters)))
    }

    pub(crate) fn execute_receiver(
        &mut self,
        sql: sqlx_core::sql_str::SqlStr,
        persistent: bool,
        arguments: Option<MssqlArguments>,
    ) -> flume::Receiver<ExecuteResult> {
        let (tx, rx) = flume::bounded(64);
        let has_arguments = arguments
            .as_ref()
            .is_some_and(|arguments| !arguments.is_empty());
        let maybe_prepared = match self.maybe_prepare_for_execution(&sql, persistent, has_arguments)
        {
            Ok(maybe_prepared) => maybe_prepared,
            Err(error) => {
                let _ = tx.send(Err(error));
                return rx;
            }
        };
        let buffer_settings = self.buffer_settings;

        std::thread::spawn(move || {
            if let Err(error) =
                execute_sql_to_channel(maybe_prepared, sql, arguments, buffer_settings, &tx)
            {
                let _ = tx.send(Err(error));
            }
        });

        rx
    }

    fn maybe_prepare_for_execution(
        &mut self,
        sql: &sqlx_core::sql_str::SqlStr,
        persistent: bool,
        has_arguments: bool,
    ) -> std::result::Result<MaybePrepared, sqlx_core::Error> {
        if !persistent || !self.stmt_cache.is_enabled() {
            return Ok(MaybePrepared::NotPrepared(Arc::clone(&self.conn)));
        }

        if let Some(prepared) = self
            .stmt_cache
            .get_mut(sql.as_str())
            .map(|prepared| Arc::clone(&*prepared))
        {
            return Ok(MaybePrepared::Prepared(prepared));
        }

        if !has_arguments {
            return Ok(MaybePrepared::NotPrepared(Arc::clone(&self.conn)));
        }

        let prepared = Arc::clone(&self.conn)
            .into_prepared(sql.as_str())
            .map_err(|error| {
                sqlx_core::Error::from(crate::error::database_error_with_context(
                    error,
                    format!(
                        "failed to prepare cached MSSQL ODBC statement: `{}`",
                        sql_preview(sql.as_str())
                    ),
                ))
            })?;
        let prepared = Arc::new(Mutex::new(prepared));
        self.stmt_cache.insert(sql.as_str(), Arc::clone(&prepared));

        Ok(MaybePrepared::Prepared(prepared))
    }
}

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
        self.ping_blocking()
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
        self.stmt_cache.len()
    }

    async fn clear_cached_statements(&mut self) -> std::result::Result<(), sqlx_core::Error>
    where
        Self::Database: sqlx_core::database::HasStatementCache,
    {
        self.stmt_cache.clear();
        Ok(())
    }
}

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
        Box::pin(async move { self.prepare_blocking(sql) })
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
        Box::pin(async move {
            let statement = self.prepare_blocking(sql)?;
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

fn odbc_parameters(arguments: Option<&MssqlArguments>) -> MssqlParameterCollection {
    arguments
        .map(MssqlArguments::to_odbc_parameter_collection)
        .unwrap_or_default()
}

enum MaybePrepared {
    Prepared(SharedPreparedStatement),
    NotPrepared(odbc_api::SharedConnection<'static>),
}

fn receiver_to_stream<'e>(rx: flume::Receiver<ExecuteResult>) -> BoxStream<'e, ExecuteResult> {
    stream::unfold(rx, |rx| async move {
        rx.recv_async().await.ok().map(|item| (item, rx))
    })
    .boxed()
}

fn execute_sql_to_channel(
    maybe_prepared: MaybePrepared,
    sql: sqlx_core::sql_str::SqlStr,
    arguments: Option<MssqlArguments>,
    buffer_settings: MssqlBufferSettings,
    tx: &ExecuteSender,
) -> std::result::Result<(), sqlx_core::Error> {
    let parameters = odbc_parameters(arguments.as_ref());

    match maybe_prepared {
        MaybePrepared::Prepared(prepared) => {
            let mut prepared = prepared.lock().map_err(|_| {
                sqlx_core::Error::Protocol(
                    "ODBC execute: failed to lock cached statement".to_owned(),
                )
            })?;

            if let Some(cursor) = prepared.execute(parameters.as_slice()).map_err(|error| {
                crate::error::database_error_with_context_lazy(error, || {
                    format!(
                        "failed to execute cached ODBC statement: `{}`",
                        sql_preview(sql.as_str())
                    )
                })
            })? {
                stream_result_sets(cursor, buffer_settings, tx)?;
                return Ok(());
            }

            let rows_affected = prepared.row_count().map_err(|error| {
                crate::error::database_error_with_context_lazy(error, || {
                    format!(
                        "failed to read ODBC row count for cached statement: `{}`",
                        sql_preview(sql.as_str())
                    )
                })
            })?;
            send_rows_affected(rows_affected, tx)
        }
        MaybePrepared::NotPrepared(conn) => {
            let mut statement = conn.into_preallocated().map_err(|error| {
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
                })?
            {
                stream_result_sets(cursor, buffer_settings, tx)?;
                return Ok(());
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
}

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

        columns.push(MssqlColumn::new(
            ordinal,
            name,
            MssqlTypeInfo::new(description.data_type),
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
            .map(|column| ColumnBinding {
                buffer_desc: map_buffer_desc(column.type_info().data_type(), max_column_size),
                column,
            })
            .collect()
    })
}

fn map_buffer_desc(data_type: DataType, max_column_size: usize) -> BufferDesc {
    match data_type {
        DataType::TinyInt | DataType::SmallInt | DataType::Integer | DataType::BigInt => {
            BufferDesc::I64 { nullable: true }
        }
        DataType::Real => BufferDesc::F32 { nullable: true },
        DataType::Float { .. } | DataType::Double => BufferDesc::F64 { nullable: true },
        DataType::Bit => BufferDesc::Bit { nullable: true },
        DataType::Date => BufferDesc::Date { nullable: true },
        DataType::Time { .. } => BufferDesc::Time { nullable: true },
        DataType::Timestamp { .. } => BufferDesc::Timestamp { nullable: true },
        DataType::Binary { .. } | DataType::Varbinary { .. } | DataType::LongVarbinary { .. } => {
            BufferDesc::Binary {
                max_bytes: max_column_size,
            }
        }
        DataType::Char { .. }
        | DataType::WChar { .. }
        | DataType::Varchar { .. }
        | DataType::WVarchar { .. }
        | DataType::LongVarchar { .. }
        | DataType::WLongVarchar { .. }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn buffered_fetch_maps_numeric_types_to_nullable_64_bit_buffers() {
        assert!(matches!(
            map_buffer_desc(DataType::TinyInt, 64),
            BufferDesc::I64 { nullable: true }
        ));
        assert!(matches!(
            map_buffer_desc(DataType::Integer, 64),
            BufferDesc::I64 { nullable: true }
        ));
        assert!(matches!(
            map_buffer_desc(DataType::BigInt, 64),
            BufferDesc::I64 { nullable: true }
        ));
    }

    #[test]
    fn buffered_fetch_uses_configured_limits_for_variable_sized_data() {
        assert_eq!(
            map_buffer_desc(DataType::Varchar { length: None }, 32),
            BufferDesc::Text { max_str_len: 32 }
        );
        assert_eq!(
            map_buffer_desc(DataType::Varbinary { length: None }, 16),
            BufferDesc::Binary { max_bytes: 16 }
        );
    }

}
