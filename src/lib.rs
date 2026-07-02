//! MSSQL driver for SQLx via ODBC.
//!
//! `sqlx-mssql-odbc` connects SQLx to Microsoft SQL Server through an ODBC driver
//! manager. This is the **facade crate** — it re-exports everything from
//! [`sqlx-mssql-odbc-core`].
//!
//! # Quick start
//!
//! ```toml
//! [dependencies]
//! sqlx-core = "0.9.0"
//! sqlx-mssql-odbc = "0.1"
//! tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
//! ```
//!
//! ```no_run
//! use sqlx_core::connection::Connection;
//! use sqlx_core::row::Row;
//! use sqlx_mssql_odbc::MssqlConnection;
//!
//! # async fn run() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
//! let mut conn = MssqlConnection::connect(
//!     "mssql://user:password@localhost:1433/database"
//! ).await?;
//!
//! let row = sqlx_core::query::query("SELECT 42")
//!     .fetch_one(&mut conn)
//!     .await?;
//!
//! let value: i32 = row.try_get(0)?;
//! assert_eq!(value, 42);
//!
//! conn.close().await?;
//! # Ok(())
//! # }
//! ```
//!
//! `MssqlConnection::connect()` accepts a standard `mssql://` URL, a raw ODBC
//! connection string, or a bare DSN name.
//!
//! # Usage patterns
//!
//! ## Connection pooling
//!
//! ```no_run
//! use sqlx_mssql_odbc::MssqlPoolOptions;
//!
//! # async fn run() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
//! let pool = MssqlPoolOptions::new()
//!     .max_connections(10)
//!     .connect("mssql://user:password@localhost:1433/database")
//!     .await?;
//!
//! let row = sqlx_core::query::query("SELECT 1")
//!     .fetch_one(&pool)
//!     .await?;
//!
//! pool.close().await;
//! # Ok(())
//! # }
//! ```
//!
//! ## Parameterised queries
//!
//! ```no_run
//! use sqlx_core::row::Row;
//! use sqlx_mssql_odbc::MssqlConnection;
//! use sqlx_core::connection::Connection;
//!
//! # async fn run() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
//! let mut conn = MssqlConnection::connect("mssql://…").await?;
//!
//! let row = sqlx_core::query::query("SELECT @p1 + @p2")
//!     .bind(10i32)
//!     .bind(20i32)
//!     .fetch_one(&mut conn)
//!     .await?;
//!
//! let total: i32 = row.try_get(0)?;
//! assert_eq!(total, 30);
//! # Ok(())
//! # }
//! ```
//!
//! ## Compile-time checked queries (`macros` feature)
//!
//! Enable the `macros` feature to get compile-time validation of SQL against a
//! live database:
//!
//! ```toml
//! [dependencies]
//! sqlx-mssql-odbc = { version = "0.1", features = ["macros"] }
//! ```
//!
//! ```ignore
//! # use sqlx_mssql_odbc::MssqlConnection;
//! # use sqlx_core::connection::Connection;
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let mut conn = MssqlConnection::connect("…").await?;
//!
//! // The column name and type are checked at compile time.
//! let row = sqlx_mssql_odbc::query!("SELECT 1 AS one")
//!     .fetch_one(&mut conn)
//!     .await?;
//! assert_eq!(row.one, 1i32);
//!
//! // Bind parameters with $1, $2, etc.
//! let row = sqlx_mssql_odbc::query!("SELECT @p1 + @p2 AS total", 10i32, 20i32)
//!     .fetch_one(&mut conn)
//!     .await?;
//! assert_eq!(row.total, 30);
//! # Ok(())
//! # }
//! ```
//!
//! For CI or offline builds, use `cargo sqlx prepare` to cache the schema so
//! the macros can check queries without a live database.
//!
//! ## Derive macros (`derive` feature)
//!
//! ```toml
//! [dependencies]
//! sqlx-mssql-odbc = { version = "0.1", features = ["derive"] }
//! ```
//!
//! ```ignore
//! use sqlx_mssql_odbc::FromRow;
//!
//! #[derive(Debug, FromRow)]
//! struct User {
//!     id: i32,
//!     name: String,
//!     email: Option<String>,
//! }
//!
//! # async fn example(mut conn: sqlx_mssql_odbc::MssqlConnection)
//! #     -> Result<(), Box<dyn std::error::Error>>
//! # {
//! let users = sqlx_mssql_odbc::query_as!(User, "SELECT id, name, email FROM users")
//!     .fetch_all(&mut conn)
//!     .await?;
//! # Ok(())
//! # }
//! ```
//!
//! ## Transactions
//!
//! ```no_run
//! use sqlx_core::connection::Connection;
//! use sqlx_core::executor::Executor;
//! use sqlx_mssql_odbc::MssqlConnection;
//!
//! # async fn run() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
//! let mut conn = MssqlConnection::connect("mssql://…").await?;
//!
//! conn.begin().await?;
//! sqlx_core::query::query("INSERT INTO users (name) VALUES (@p1)")
//!     .bind("Alice")
//!     .execute(&mut conn)
//!     .await?;
//! conn.commit().await?;
//! # Ok(())
//! # }
//! ```
//!
//! # URL parameters
//!
//! | Parameter | Description |
//! |---|---|
//! | `encrypt=true` | Enable TLS encryption |
//! | `trust_certificate=true` | Skip certificate validation |
//! | `driver=…` | Custom ODBC driver name (default: `ODBC Driver 18 for SQL Server`) |
//!
//! # Requirements
//!
//! On Linux and macOS you need both a driver manager (unixODBC) and the
//! Microsoft ODBC Driver for SQL Server (version 17 or 18). See the
//! [repository README](https://github.com/strawberyy-coconut/sqlx-mssql-odbc)
//! for platform-specific installation instructions.
//!
//! Enable the `vendored-unix-odbc` feature to statically link unixODBC into
//! your application on Linux or macOS.
//!
//! # Features
//!
//! | Feature | Description |
//! |---|---|
//! | `bigdecimal` | [`BigDecimal`] type support |
//! | `chrono` | [`chrono`] datetime types |
//! | `rust_decimal` / `decimal` | [`rust_decimal::Decimal`] support |
//! | `json` | [`serde_json::Value`] support |
//! | `time` | [`time`] crate datetime types |
//! | `uuid` | [`uuid::Uuid`] support |
//! | `macros` | `query!()`, `query_as!()` and other proc macros |
//! | `derive` | `Encode`, `Decode`, `Type`, `FromRow` derive macros |
//! | `offline` | Compile-time query checking with `query!()` |
//! | `migrate` | Database migration support |
//! | `runtime-tokio` | Tokio runtime support |
//! | `tls-none` | No TLS (default) |
//! | `spatial` | [`geo_types`] spatial type support |
//! | `vendored-unix-odbc` | Statically link unixODBC |

#![cfg_attr(docsrs, feature(doc_cfg))]

// Re-export everything from sqlx-mssql-odbc-core.
pub use sqlx_mssql_odbc_core::*;

/// Re-export of the core crate for direct access.
pub use sqlx_mssql_odbc_core as core;

// ---------------------------------------------------------------------------
// Macro wrappers — only available with the `macros` feature
// ---------------------------------------------------------------------------

#[cfg(feature = "macros")]
#[cfg_attr(docsrs, doc(cfg(feature = "macros")))]
pub use sqlx_mssql_odbc_macros::expand_query;

/// Compile-time checked SQL query for MSSQL via ODBC.
///
/// The SQL is sent to a live database at compile time so column names and
/// types are verified. The returned row lets you access columns as named
/// fields with correct Rust types — no runtime mapping needed.
///
/// # Without parameters
///
/// ```ignore
/// # use sqlx_mssql_odbc::MssqlConnection;
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let mut conn = MssqlConnection::connect("…").await?;
///
/// let row = sqlx_mssql_odbc::query!("SELECT id, name, email FROM users WHERE id = 1")
///     .fetch_one(&mut conn)
///     .await?;
///
/// // Fields are checked at compile time — typos become compile errors!
/// println!("{} <{}>", row.name, row.email.unwrap_or_default());
/// # Ok(())
/// # }
/// ```
///
/// # With parameters
///
/// Bind parameters with `@p1`, `@p2`, etc. The macro infers types from the
/// Rust expressions:
///
/// ```ignore
/// # use sqlx_mssql_odbc::MssqlConnection;
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// # let mut conn = MssqlConnection::connect("…").await?;
/// let user_id = 42;
/// let rows = sqlx_mssql_odbc::query!(
///     "SELECT id, name FROM users WHERE id = @p1 OR name LIKE @p2",
///     user_id,
///     "%Smith%"
/// )
///     .fetch_all(&mut conn)
///     .await?;
/// # Ok(())
/// # }
/// ```
///
/// # Inferring multiple result columns
///
/// ```ignore
/// # use sqlx_mssql_odbc::MssqlConnection;
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// # let mut conn = MssqlConnection::connect("…").await?;
/// let row = sqlx_mssql_odbc::query!("SELECT COUNT(*) AS count, AVG(age) AS avg_age FROM users")
///     .fetch_one(&mut conn)
///     .await?;
/// println!("{} users, average age {}", row.count, row.avg_age);
/// # Ok(())
/// # }
/// ```
///
/// Requires the `macros` feature and a live database (or a prepared offline
/// cache via `cargo sqlx prepare`).
#[cfg(feature = "macros")]
#[cfg_attr(docsrs, doc(cfg(feature = "macros")))]
#[macro_export]
macro_rules! query {
    ($query:expr) => ({
        $crate::expand_query!(source = $query)
    });
    ($query:expr, $($args:tt)*) => ({
        $crate::expand_query!(source = $query, args = [$($args)*])
    });
}

/// Compile-time checked SQL query (unchecked variant — skips database
/// verification at compile time).
///
/// Like [`query!`] but does not require `DATABASE_URL` or an offline cache.
/// Column names and types are **not** verified against the database schema;
/// they are inferred from the query text alone.
///
/// Requires the `macros` feature.
#[cfg(feature = "macros")]
#[cfg_attr(docsrs, doc(cfg(feature = "macros")))]
#[macro_export]
macro_rules! query_unchecked {
    ($query:expr) => ({
        $crate::expand_query!(source = $query, checked = false)
    });
    ($query:expr, $($args:tt)*) => ({
        $crate::expand_query!(source = $query, args = [$($args)*], checked = false)
    });
}

/// Compile-time checked SQL query for MSSQL via ODBC, mapping to a named struct.
///
/// Like [`query!`] but deserialises each row into a struct you provide. The
/// struct fields are matched to columns by name and their types are checked
/// against the database schema at compile time.
///
/// ```ignore
/// # use sqlx_mssql_odbc::MssqlConnection;
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// # let mut conn = MssqlConnection::connect("…").await?;
/// struct User {
///     id: i32,
///     name: String,
/// }
///
/// let users = sqlx_mssql_odbc::query_as!(User, "SELECT id, name FROM users")
///     .fetch_all(&mut conn)
///     .await?;
///
/// for user in users {
///     println!("{}: {}", user.id, user.name);
/// }
/// # Ok(())
/// # }
/// ```
///
/// Combine with the `derive` feature's `FromRow` derive to avoid manually
/// writing struct definitions:
///
/// ```ignore
/// # use sqlx_mssql_odbc::{MssqlConnection, FromRow};
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// # let mut conn = MssqlConnection::connect("…").await?;
/// #[derive(Debug, FromRow)]
/// struct Order {
///     id: i32,
///     total: rust_decimal::Decimal,
///     placed_at: chrono::NaiveDateTime,
/// }
///
/// let orders = sqlx_mssql_odbc::query_as!(Order, "SELECT id, total, placed_at FROM orders")
///     .fetch_all(&mut conn)
///     .await?;
/// # Ok(())
/// # }
/// ```
///
/// Requires the `macros` feature.
#[cfg(feature = "macros")]
#[cfg_attr(docsrs, doc(cfg(feature = "macros")))]
#[macro_export]
macro_rules! query_as {
    ($out_struct:path, $query:expr) => ({
        $crate::expand_query!(record = $out_struct, source = $query)
    });
    ($out_struct:path, $query:expr, $($args:tt)*) => ({
        $crate::expand_query!(record = $out_struct, source = $query, args = [$($args)*])
    });
}

/// Compile-time checked SQL query mapping to a struct (unchecked variant).
///
/// Like [`query_as!`] but does **not** verify column names or types against
/// the database at compile time. Useful for CI/deploy environments that lack
/// database connectivity.
///
/// Requires the `macros` feature.
#[cfg(feature = "macros")]
#[cfg_attr(docsrs, doc(cfg(feature = "macros")))]
#[macro_export]
macro_rules! query_as_unchecked {
    ($out_struct:path, $query:expr) => ({
        $crate::expand_query!(record = $out_struct, source = $query, checked = false)
    });
    ($out_struct:path, $query:expr, $($args:tt)*) => ({
        $crate::expand_query!(record = $out_struct, source = $query, args = [$($args)*], checked = false)
    });
}

/// Compile-time checked SQL query for MSSQL via ODBC, returning a single scalar
/// value.
///
/// ```ignore
/// # use sqlx_mssql_odbc::MssqlConnection;
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// # let mut conn = MssqlConnection::connect("…").await?;
/// let count: i64 = sqlx_mssql_odbc::query_scalar!("SELECT COUNT(*) FROM users")
///     .fetch_one(&mut conn)
///     .await?;
///
/// let max_id: Option<i32> = sqlx_mssql_odbc::query_scalar!("SELECT MAX(id) FROM users")
///     .fetch_optional(&mut conn)
///     .await?;
/// # Ok(())
/// # }
/// ```
///
/// Requires the `macros` feature.
#[cfg(feature = "macros")]
#[cfg_attr(docsrs, doc(cfg(feature = "macros")))]
#[macro_export]
macro_rules! query_scalar {
    ($query:expr) => (
        $crate::expand_query!(scalar = _, source = $query)
    );
    ($query:expr, $($args:tt)*) => (
        $crate::expand_query!(scalar = _, source = $query, args = [$($args)*])
    );
}

/// Compile-time checked SQL query returning a single scalar (unchecked variant).
///
/// Like [`query_scalar!`] but skips database verification at compile time.
///
/// Requires the `macros` feature.
#[cfg(feature = "macros")]
#[cfg_attr(docsrs, doc(cfg(feature = "macros")))]
#[macro_export]
macro_rules! query_scalar_unchecked {
    ($query:expr) => (
        $crate::expand_query!(scalar = _, source = $query, checked = false)
    );
    ($query:expr, $($args:tt)*) => (
        $crate::expand_query!(scalar = _, source = $query, args = [$($args)*], checked = false)
    );
}

/// Compile-time checked SQL query read from a file at compile time.
///
/// The file path is relative to the crate root. This keeps large queries out
/// of your Rust source files.
///
/// ```ignore
/// # use sqlx_mssql_odbc::MssqlConnection;
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// # let mut conn = MssqlConnection::connect("…").await?;
/// let reports = sqlx_mssql_odbc::query_file!("queries/monthly_report.sql")
///     .fetch_all(&mut conn)
///     .await?;
/// # Ok(())
/// # }
/// ```
///
/// Requires the `macros` feature.
#[cfg(feature = "macros")]
#[cfg_attr(docsrs, doc(cfg(feature = "macros")))]
#[macro_export]
macro_rules! query_file {
    ($path:literal) => ({
        $crate::expand_query!(source_file = $path)
    });
    ($path:literal, $($args:tt)*) => ({
        $crate::expand_query!(source_file = $path, args = [$($args)*])
    });
}

/// Compile-time SQL query read from a file (unchecked variant).
///
/// Like [`query_file!`] but does **not** verify the query against a live
/// database or offline cache.
///
/// Requires the `macros` feature.
#[cfg(feature = "macros")]
#[cfg_attr(docsrs, doc(cfg(feature = "macros")))]
#[macro_export]
macro_rules! query_file_unchecked {
    ($path:literal) => ({
        $crate::expand_query!(source_file = $path, checked = false)
    });
    ($path:literal, $($args:tt)*) => ({
        $crate::expand_query!(source_file = $path, args = [$($args)*], checked = false)
    });
}

/// Compile-time checked SQL query read from a file, mapping to a named struct.
///
/// Combines [`query_file!`] and [`query_as!`]: the SQL lives in a separate
/// `.sql` file and rows are deserialised into the given struct.
///
/// ```ignore
/// # use sqlx_mssql_odbc::MssqlConnection;
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// # let mut conn = MssqlConnection::connect("…").await?;
/// struct UserReport {
///     name: String,
///     order_count: i64,
/// }
///
/// let reports = sqlx_mssql_odbc::query_file_as!(UserReport, "queries/user_reports.sql")
///     .fetch_all(&mut conn)
///     .await?;
/// # Ok(())
/// # }
/// ```
///
/// Requires the `macros` feature.
#[cfg(feature = "macros")]
#[cfg_attr(docsrs, doc(cfg(feature = "macros")))]
#[macro_export]
macro_rules! query_file_as {
    ($out_struct:path, $path:literal) => ({
        $crate::expand_query!(record = $out_struct, source_file = $path)
    });
    ($out_struct:path, $path:literal, $($args:tt)*) => ({
        $crate::expand_query!(record = $out_struct, source_file = $path, args = [$($args)*])
    });
}

/// Compile-time SQL query read from a file, mapping to a struct (unchecked
/// variant).
///
/// Like [`query_file_as!`] but skips database verification at compile time.
///
/// Requires the `macros` feature.
#[cfg(feature = "macros")]
#[cfg_attr(docsrs, doc(cfg(feature = "macros")))]
#[macro_export]
macro_rules! query_file_as_unchecked {
    ($out_struct:path, $path:literal) => ({
        $crate::expand_query!(record = $out_struct, source_file = $path, checked = false)
    });
    ($out_struct:path, $path:literal, $($args:tt)*) => ({
        $crate::expand_query!(record = $out_struct, source_file = $path, args = [$($args)*], checked = false)
    });
}

/// Compile-time checked SQL query read from a file, returning a single scalar.
///
/// Like [`query_file!`] but returns a single value — useful for report
/// totals, row counts, or other aggregate queries stored in files.
///
/// ```ignore
/// # use sqlx_mssql_odbc::MssqlConnection;
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// # let mut conn = MssqlConnection::connect("…").await?;
/// let total: f64 = sqlx_mssql_odbc::query_file_scalar!("queries/total_revenue.sql")
///     .fetch_one(&mut conn)
///     .await?;
/// # Ok(())
/// # }
/// ```
///
/// Requires the `macros` feature.
#[cfg(feature = "macros")]
#[cfg_attr(docsrs, doc(cfg(feature = "macros")))]
#[macro_export]
macro_rules! query_file_scalar {
    ($path:literal) => (
        $crate::expand_query!(scalar = _, source_file = $path)
    );
    ($path:literal, $($args:tt)*) => (
        $crate::expand_query!(scalar = _, source_file = $path, args = [$($args)*])
    );
}

/// Compile-time SQL query read from a file, returning a single scalar
/// (unchecked variant).
///
/// Like [`query_file_scalar!`] but skips database verification at compile
/// time.
///
/// Requires the `macros` feature.
#[cfg(feature = "macros")]
#[cfg_attr(docsrs, doc(cfg(feature = "macros")))]
#[macro_export]
macro_rules! query_file_scalar_unchecked {
    ($path:literal) => (
        $crate::expand_query!(scalar = _, source_file = $path, checked = false)
    );
    ($path:literal, $($args:tt)*) => (
        $crate::expand_query!(scalar = _, source_file = $path, args = [$($args)*], checked = false)
    );
}

// Re-export derive macros behind `derive` feature.
#[cfg(feature = "derive")]
#[cfg_attr(docsrs, doc(cfg(feature = "derive")))]
pub use sqlx_mssql_odbc_macros::{Encode, Decode, Type, FromRow};
