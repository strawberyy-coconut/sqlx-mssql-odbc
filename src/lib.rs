//! MSSQL driver for SQLx via ODBC.
//!
//! `sqlx-mssql-odbc` connects SQLx to Microsoft SQL Server through an ODBC driver
//! manager. This is the **facade crate** — it re-exports everything from
//! [`sqlx-mssql-odbc-core`]. Enable the `macros` feature for compile-time checked
//! queries.
//!
//! # Connection
//!
//! ```no_run
//! use sqlx_core::connection::Connection;
//! use sqlx_core::row::Row;
//! use sqlx_mssql_odbc::MssqlConnection;
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

// Re-export everything from sqlx-mssql-odbc-core.
pub use sqlx_mssql_odbc_core::*;

/// Re-export of the core crate for direct access.
pub use sqlx_mssql_odbc_core as core;

// ---------------------------------------------------------------------------
// Macro wrappers — only available with the `macros` feature
// ---------------------------------------------------------------------------

#[cfg(feature = "macros")]
pub use sqlx_mssql_odbc_macros::expand_query;

/// Compile-time checked SQL query for MSSQL via ODBC.
///
/// ```ignore
/// # use sqlx_mssql_odbc::MssqlConnection;
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let mut conn = MssqlConnection::connect("...").await?;
/// let row = sqlx_mssql_odbc::query!("SELECT 1 AS one").fetch_one(&mut conn).await?;
/// assert_eq!(row.one, 1i32);
/// # Ok(())
/// # }
/// ```
#[cfg(feature = "macros")]
#[macro_export]
macro_rules! query {
    ($query:expr) => ({
        $crate::expand_query!(source = $query)
    });
    ($query:expr, $($args:tt)*) => ({
        $crate::expand_query!(source = $query, args = [$($args)*])
    });
}

/// Compile-time checked SQL query for MSSQL via ODBC, mapping to a named struct.
#[cfg(feature = "macros")]
#[macro_export]
macro_rules! query_as {
    ($out_struct:path, $query:expr) => ({
        $crate::expand_query!(record = $out_struct, source = $query)
    });
    ($out_struct:path, $query:expr, $($args:tt)*) => ({
        $crate::expand_query!(record = $out_struct, source = $query, args = [$($args)*])
    });
}

/// Compile-time checked SQL query for MSSQL via ODBC, returning a single scalar.
#[cfg(feature = "macros")]
#[macro_export]
macro_rules! query_scalar {
    ($query:expr) => (
        $crate::expand_query!(scalar = _, source = $query)
    );
    ($query:expr, $($args:tt)*) => (
        $crate::expand_query!(scalar = _, source = $query, args = [$($args)*])
    );
}

/// Compile-time checked SQL query from a file for MSSQL via ODBC.
#[cfg(feature = "macros")]
#[macro_export]
macro_rules! query_file {
    ($path:literal) => ({
        $crate::expand_query!(source_file = $path)
    });
    ($path:literal, $($args:tt)*) => ({
        $crate::expand_query!(source_file = $path, args = [$($args)*])
    });
}

/// Compile-time checked SQL query from a file for MSSQL via ODBC, mapping to a named struct.
#[cfg(feature = "macros")]
#[macro_export]
macro_rules! query_file_as {
    ($out_struct:path, $path:literal) => ({
        $crate::expand_query!(record = $out_struct, source_file = $path)
    });
    ($out_struct:path, $path:literal, $($args:tt)*) => ({
        $crate::expand_query!(record = $out_struct, source_file = $path, args = [$($args)*])
    });
}

/// Compile-time checked SQL query from a file for MSSQL via ODBC, returning a scalar.
#[cfg(feature = "macros")]
#[macro_export]
macro_rules! query_file_scalar {
    ($path:literal) => (
        $crate::expand_query!(scalar = _, source_file = $path)
    );
    ($path:literal, $($args:tt)*) => (
        $crate::expand_query!(scalar = _, source_file = $path, args = [$($args)*])
    );
}

// Re-export derive macros behind `derive` feature.
#[cfg(feature = "derive")]
pub use sqlx_mssql_odbc_macros::{Encode, Decode, Type, FromRow};
