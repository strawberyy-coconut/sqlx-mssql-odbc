//! `Any` driver integration for MSSQL via ODBC.
//!
//! Implements [`AnyConnectionBackend`] for [`MssqlConnection`] and the required
//! type conversions so that `sqlx-cli` and `AnyConnection` can work with
//! `mssql://` URLs.

use crate::{
    Mssql, MssqlArguments, MssqlColumn, MssqlConnectOptions, MssqlConnection, MssqlQueryResult,
    MssqlRow, MssqlTransactionManager, MssqlTypeInfo,
};
use futures_core::future::BoxFuture;
use futures_core::stream::BoxStream;
use futures_util::{future, stream, FutureExt, StreamExt};
use odbc_api::DataType;
use sqlx_core::any::{
    AnyArguments, AnyColumn, AnyConnectOptions, AnyConnectionBackend, AnyQueryResult, AnyRow,
    AnyStatement, AnyTypeInfo, AnyTypeInfoKind,
};
use sqlx_core::column::Column;
use sqlx_core::connection::Connection;
use sqlx_core::database::Database;
use sqlx_core::encode::{Encode, IsNull};
use sqlx_core::error::BoxDynError;
use sqlx_core::executor::Executor;
use sqlx_core::ext::ustr::UStr;
use sqlx_core::row::Row;
use sqlx_core::sql_str::SqlStr;
use sqlx_core::statement::Statement;
use sqlx_core::transaction::TransactionManager;
use sqlx_core::HashMap;
use std::str::FromStr;
use std::sync::Arc;

sqlx_core::declare_driver_with_optional_migrate!(DRIVER = Mssql);

// ---------------------------------------------------------------------------
// Additional Encode impl needed by AnyArguments::convert_into
//
// The upstream `impl_encode_for_smartpointer!(Arc<T>)` macro generates
// `Arc<T>: Encode<DB>` only when `T: Encode<DB>`.  Since `str: Encode<Mssql>`
// is not implemented (only `&str` is), we must provide `Arc<str>: Encode` manually.
// ---------------------------------------------------------------------------

impl<'q> Encode<'q, Mssql> for Arc<str> {
    fn encode(
        self,
        buf: &mut Vec<crate::MssqlArgumentValue>,
    ) -> Result<IsNull, BoxDynError> {
        buf.push(crate::MssqlArgumentValue::Text(self.to_string()));
        Ok(IsNull::No)
    }

    fn encode_by_ref(
        &self,
        buf: &mut Vec<crate::MssqlArgumentValue>,
    ) -> Result<IsNull, BoxDynError> {
        buf.push(crate::MssqlArgumentValue::Text(self.to_string()));
        Ok(IsNull::No)
    }
}

// ---------------------------------------------------------------------------
// AnyConnectionBackend
// ---------------------------------------------------------------------------

impl AnyConnectionBackend for MssqlConnection {
    fn name(&self) -> &str {
        <Mssql as Database>::NAME
    }

    fn close(self: Box<Self>) -> BoxFuture<'static, sqlx_core::Result<()>> {
        Connection::close(*self).boxed()
    }

    fn close_hard(self: Box<Self>) -> BoxFuture<'static, sqlx_core::Result<()>> {
        Connection::close_hard(*self).boxed()
    }

    fn ping(&mut self) -> BoxFuture<'_, sqlx_core::Result<()>> {
        Connection::ping(self).boxed()
    }

    fn begin(&mut self, statement: Option<SqlStr>) -> BoxFuture<'_, sqlx_core::Result<()>> {
        MssqlTransactionManager::begin(self, statement).boxed()
    }

    fn commit(&mut self) -> BoxFuture<'_, sqlx_core::Result<()>> {
        MssqlTransactionManager::commit(self).boxed()
    }

    fn rollback(&mut self) -> BoxFuture<'_, sqlx_core::Result<()>> {
        MssqlTransactionManager::rollback(self).boxed()
    }

    fn start_rollback(&mut self) {
        MssqlTransactionManager::start_rollback(self)
    }

    fn get_transaction_depth(&self) -> usize {
        MssqlTransactionManager::get_transaction_depth(self)
    }

    fn shrink_buffers(&mut self) {
        Connection::shrink_buffers(self);
    }

    fn flush(&mut self) -> BoxFuture<'_, sqlx_core::Result<()>> {
        Connection::flush(self).boxed()
    }

    fn should_flush(&self) -> bool {
        Connection::should_flush(self)
    }

    fn cached_statements_size(&self) -> usize {
        Connection::cached_statements_size(self)
    }

    fn clear_cached_statements(&mut self) -> BoxFuture<'_, sqlx_core::Result<()>> {
        Connection::clear_cached_statements(self).boxed()
    }

    #[cfg(feature = "migrate")]
    fn as_migrate(
        &mut self,
    ) -> sqlx_core::Result<&mut (dyn sqlx_core::migrate::Migrate + Send + 'static)> {
        Ok(self)
    }

    fn fetch_many(
        &mut self,
        query: SqlStr,
        persistent: bool,
        arguments: Option<AnyArguments>,
    ) -> BoxStream<'_, sqlx_core::Result<sqlx_core::Either<AnyQueryResult, AnyRow>>> {
        let persistent = persistent && arguments.is_some();

        let arguments: Option<MssqlArguments> = match arguments
            .map(|a| a.convert_into::<MssqlArguments>())
            .transpose()
        {
            Ok(args) => args,
            Err(error) => {
                return stream::once(future::ready(Err(sqlx_core::Error::Encode(error)))).boxed()
            }
        };

        let rx = self.execute_receiver(query, persistent, arguments);
        receiver_to_any_stream(rx)
    }

    fn fetch_optional(
        &mut self,
        query: SqlStr,
        persistent: bool,
        arguments: Option<AnyArguments>,
    ) -> BoxFuture<'_, sqlx_core::Result<Option<AnyRow>>> {
        let persistent = persistent && arguments.is_some();

        let arguments: Option<MssqlArguments> = match arguments
            .map(|a| a.convert_into::<MssqlArguments>())
            .transpose()
        {
            Ok(args) => args,
            Err(error) => return Box::pin(future::ready(Err(sqlx_core::Error::Encode(error)))),
        };

        let rx = self.execute_receiver(query, persistent, arguments);
        Box::pin(async move {
            while let Ok(item) = rx.recv_async().await {
                match item? {
                    sqlx_core::Either::Right(row) => return Ok(Some(AnyRow::try_from(&row)?)),
                    sqlx_core::Either::Left(_) => {}
                }
            }
            Ok(None)
        })
    }

    fn prepare_with<'c, 'q: 'c>(
        &'c mut self,
        sql: SqlStr,
        _parameters: &[AnyTypeInfo],
    ) -> BoxFuture<'c, sqlx_core::Result<AnyStatement>> {
        Box::pin(async move {
            let statement = Executor::prepare_with(self, sql, &[]).await?;
            // Clone column names into owned Strings for UStr conversion
            let columns: Vec<MssqlColumn> = statement.columns().to_vec();
            let mut names = HashMap::<UStr, usize>::new();
            for (i, col) in columns.iter().enumerate() {
                names.insert(UStr::from(col.name().to_owned()), i);
            }
            let column_names = Arc::new(names);
            AnyStatement::try_from_statement(statement, column_names)
        })
    }

    #[cfg(feature = "offline")]
    fn describe(
        &mut self,
        sql: SqlStr,
    ) -> BoxFuture<
        '_,
        sqlx_core::Result<sqlx_core::describe::Describe<sqlx_core::any::Any>>,
    > {
        Box::pin(async move {
            let describe = Executor::describe(self, sql).await?;
            describe.try_into_any()
        })
    }
}

// ---------------------------------------------------------------------------
// Type conversions
// ---------------------------------------------------------------------------

impl<'a> TryFrom<&'a MssqlTypeInfo> for AnyTypeInfo {
    type Error = sqlx_core::Error;

    fn try_from(type_info: &'a MssqlTypeInfo) -> Result<Self, Self::Error> {
        let kind = match type_info.data_type() {
            DataType::Bit => AnyTypeInfoKind::Bool,
            DataType::TinyInt | DataType::SmallInt => AnyTypeInfoKind::SmallInt,
            DataType::Integer => AnyTypeInfoKind::Integer,
            DataType::BigInt => AnyTypeInfoKind::BigInt,
            DataType::Real => AnyTypeInfoKind::Real,
            DataType::Float { .. } | DataType::Double => AnyTypeInfoKind::Double,
            // Text types
            DataType::Char { .. }
            | DataType::Varchar { .. }
            | DataType::LongVarchar { .. }
            | DataType::WChar { .. }
            | DataType::WVarchar { .. }
            | DataType::WLongVarchar { .. } => AnyTypeInfoKind::Text,
            // Binary types
            DataType::Binary { .. }
            | DataType::Varbinary { .. }
            | DataType::LongVarbinary { .. } => AnyTypeInfoKind::Blob,
            // Date/time types — no dedicated AnyTypeInfoKind, fall back to Text
            DataType::Date | DataType::Time { .. } | DataType::Timestamp { .. } => {
                AnyTypeInfoKind::Text
            }
            // Decimal / Numeric — no dedicated AnyTypeInfoKind, fall back to Text
            DataType::Decimal { .. } | DataType::Numeric { .. } => AnyTypeInfoKind::Text,
            // Other (GUID, Unknown, Null indicator, etc.) — fall back to Text
            DataType::Other { .. } | DataType::Unknown => AnyTypeInfoKind::Text,
        };

        Ok(AnyTypeInfo { kind })
    }
}

impl<'a> TryFrom<&'a MssqlColumn> for AnyColumn {
    type Error = sqlx_core::Error;

    fn try_from(column: &'a MssqlColumn) -> Result<Self, Self::Error> {
        let type_info = AnyTypeInfo::try_from(column.type_info())?;

        Ok(AnyColumn {
            ordinal: column.ordinal(),
            // Clone the &str to an owned String for UStr conversion
            name: UStr::from(column.name().to_owned()),
            type_info,
        })
    }
}

impl<'a> TryFrom<&'a MssqlRow> for AnyRow {
    type Error = sqlx_core::Error;

    fn try_from(row: &'a MssqlRow) -> Result<Self, Self::Error> {
        // Clone column names into owned Strings for Arc<HashMap<UStr, usize>>
        let columns: Vec<MssqlColumn> = row.columns().to_vec();
        let mut names = HashMap::<UStr, usize>::new();
        for (i, col) in columns.iter().enumerate() {
            names.insert(UStr::from(col.name().to_owned()), i);
        }
        let column_names = Arc::new(names);
        AnyRow::map_from(row, column_names)
    }
}

impl<'a> TryFrom<&'a AnyConnectOptions> for MssqlConnectOptions {
    type Error = sqlx_core::Error;

    fn try_from(any_opts: &'a AnyConnectOptions) -> Result<Self, Self::Error> {
        // Use FromStr to parse the database URL into MssqlConnectOptions
        let mut opts: MssqlConnectOptions =
            FromStr::from_str(any_opts.database_url.as_str())?;
        opts.log_statements = any_opts.log_settings.statements_level;
        opts.log_slow_statements = any_opts.log_settings.slow_statements_level;
        opts.log_slow_statement_duration = any_opts.log_settings.slow_statements_duration;
        Ok(opts)
    }
}

// ---------------------------------------------------------------------------
// Helper: convert an ExecuteResult stream to an AnyResult stream
// ---------------------------------------------------------------------------

fn receiver_to_any_stream(
    rx: flume::Receiver<
        sqlx_core::Result<sqlx_core::Either<MssqlQueryResult, MssqlRow>>,
    >,
) -> BoxStream<'static, sqlx_core::Result<sqlx_core::Either<AnyQueryResult, AnyRow>>> {
    stream::unfold(rx, |rx| async move {
        rx.recv_async().await.ok().map(|item| {
            let mapped = match item {
                Ok(sqlx_core::Either::Left(result)) => {
                    Ok(sqlx_core::Either::Left(map_result(result)))
                }
                Ok(sqlx_core::Either::Right(row)) => {
                    AnyRow::try_from(&row).map(sqlx_core::Either::Right)
                }
                Err(err) => Err(err),
            };
            (mapped, rx)
        })
    })
    .boxed()
}

fn map_result(result: MssqlQueryResult) -> AnyQueryResult {
    AnyQueryResult {
        rows_affected: result.rows_affected(),
        last_insert_id: None,
    }
}
