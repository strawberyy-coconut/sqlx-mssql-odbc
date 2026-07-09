//! Compile-time query description for the MSSQL ODBC macros.
//!
//! Only available with the `offline` feature.

use crate::{Mssql, MssqlConnectOptions, MssqlConnection, MssqlStatement};
use crate::statement::clone_param_info;
use sqlx_core::describe::Describe;
use sqlx_core::sql_str::{AssertSqlSafe, SqlSafeStr};
use sqlx_core::statement::Statement as _;

/// Compile-time query descriptor plugged into [`sqlx_macros_core`].
#[doc(hidden)]
pub const MSSQL_DRIVER: sqlx_macros_core::query::QueryDriver =
    sqlx_macros_core::query::QueryDriver::new::<Mssql>();

impl sqlx_macros_core::database::DatabaseExt for Mssql {
    const DATABASE_PATH: &'static str = "sqlx_mssql_odbc::Mssql";
    const ROW_PATH: &'static str = "sqlx_mssql_odbc::MssqlRow";

    fn describe_blocking(
        query: &str,
        database_url: &str,
        driver_config: &sqlx_core::config::drivers::Config,
    ) -> sqlx_core::Result<Describe<Self>> {
        describe_blocking(query, database_url, driver_config)
    }
}

/// Connects to an MSSQL database via ODBC at compile time and describes a SQL query.
///
/// Returns column metadata, parameter types (when available), and nullability information.
/// This function is `#[doc(hidden)]` — it is only used by the proc macros.
#[doc(hidden)]
pub fn describe_blocking(
    query: &str,
    database_url: &str,
    _driver_config: &sqlx_core::config::drivers::Config,
) -> Result<Describe<Mssql>, sqlx_core::Error> {
    let options: MssqlConnectOptions = database_url
        .parse()
        .map_err(|e| sqlx_core::Error::Configuration(Box::new(e)))?;

    let conn = MssqlConnection::connect_blocking(&options)?;

    let sql_str = AssertSqlSafe(query.to_owned()).into_sql_str();
    let statement: MssqlStatement = conn.prepare_blocking(sql_str)?;

    Ok(Describe {
        columns: statement.columns().to_vec(),
        parameters: clone_param_info(statement.parameters()),
        nullable: statement
            .columns()
            .iter()
            .map(|col| col.nullable())
            .collect(),
    })
}
