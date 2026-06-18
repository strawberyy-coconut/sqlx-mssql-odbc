//! MSSQL driver for SQLx via ODBC.
//!
//! `sqlx-mssql-odbc` connects SQLx to Microsoft SQL Server through an ODBC driver
//! manager (unixODBC on Linux/macOS, built-in on Windows).
//!
//! # Connection
//!
//! ```no_run
//! use sqlx_core::connection::Connection;
//! use sqlx_core::row::Row;
//! use sqlx_mssql_odbc_core::MssqlConnection;
//!
//! # async fn run() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
//! let mut conn = MssqlConnection::connect("mssql://sa:Password1!@localhost:1433/testdb").await?;
//!
//! let row = sqlx_core::query::query("SELECT 1")
//!     .fetch_one(&mut conn)
//!     .await?;
//!
//! let value: i32 = row.try_get(0)?;
//! assert_eq!(value, 1);
//!
//! conn.close().await?;
//! # Ok(())
//! # }
//! ```
//!
//! `MssqlConnection::connect()` accepts a standard `mssql://` URL or a raw ODBC
//! connection string.
//!
//! # Requirements
//!
//! On Linux and macOS, install the unixODBC driver manager and the Microsoft
//! ODBC Driver 17 or 18 for SQL Server. On Windows, the driver manager is built
//! in, but the Microsoft ODBC Driver for SQL Server still needs to be installed.
//!
//! Enable the `vendored-unix-odbc` feature to statically link the unixODBC
//! driver manager into your application on Linux or macOS.
//!
//! Buffered fetching can improve throughput, but long text or binary values may
//! be truncated when `max_column_size` is set. Use unbuffered mode for values
//! that may exceed that limit.

#![deny(missing_docs)]
#![deny(rustdoc::broken_intra_doc_links)]
#![warn(future_incompatible, rust_2018_idioms)]

mod arguments;
mod column;
mod connection;
mod database;
mod error;
mod options;
mod query_result;
mod row;
mod statement;
mod transaction;
/// Type-checking support for compile-time query macros.
pub mod type_checking;
mod type_info;
mod types;
mod value;

#[cfg(feature = "offline")]
mod describe;

#[cfg(feature = "offline")]
pub use describe::{describe_blocking, MSSQL_DRIVER};

#[cfg(feature = "migrate")]
mod migrate;

pub use arguments::{MssqlArgumentValue, MssqlArguments, MssqlParameterCollection};
pub use column::MssqlColumn;
pub use connection::MssqlConnection;
pub use database::Mssql;
pub use error::{MssqlDatabaseError, MssqlError, Result};
pub use options::{MssqlBufferSettings, MssqlConnectOptions};
pub use query_result::MssqlQueryResult;
pub use row::MssqlRow;
pub use statement::MssqlStatement;
pub use transaction::MssqlTransactionManager;
pub use type_info::{DataTypeExt, MssqlTypeInfo};
pub use value::{MssqlValue, MssqlValueKind};

/// An alias for [`Pool`][sqlx_core::pool::Pool], specialized for MSSQL.
pub type MssqlPool = sqlx_core::pool::Pool<Mssql>;

/// An alias for [`PoolOptions`][sqlx_core::pool::PoolOptions], specialized for MSSQL.
pub type MssqlPoolOptions = sqlx_core::pool::PoolOptions<Mssql>;

/// An alias for [`Transaction`][sqlx_core::transaction::Transaction], specialized for MSSQL.
pub type MssqlTransaction<'c> = sqlx_core::transaction::Transaction<'c, Mssql>;

/// An alias for [`Executor<'_, Database = Mssql>`][sqlx_core::executor::Executor].
pub trait MssqlExecutor<'c>: sqlx_core::executor::Executor<'c, Database = Mssql> {}
impl<'c, T> MssqlExecutor<'c> for T where T: sqlx_core::executor::Executor<'c, Database = Mssql> {}

sqlx_core::impl_acquire!(Mssql, MssqlConnection);
