use crate::{
    MssqlArguments, MssqlColumn, MssqlConnection, MssqlQueryResult, MssqlRow, MssqlStatement,
    MssqlTransactionManager, MssqlTypeInfo, MssqlValue,
};

/// MSSQL database marker for SQLx-core traits.
#[derive(Debug)]
pub struct Mssql;

impl sqlx_core::database::Database for Mssql {
    type Connection = MssqlConnection;
    type TransactionManager = MssqlTransactionManager;
    type Row = MssqlRow;
    type QueryResult = MssqlQueryResult;
    type Column = MssqlColumn;
    type TypeInfo = MssqlTypeInfo;
    type Value = MssqlValue;
    type ValueRef<'r> = crate::value::MssqlValueRef<'r>;
    type Arguments = MssqlArguments;
    type ArgumentBuffer = Vec<crate::MssqlArgumentValue>;
    type Statement = MssqlStatement;

    const NAME: &'static str = "MSSQL";
    const URL_SCHEMES: &'static [&'static str] = &["mssql"];
}

impl sqlx_core::database::HasStatementCache for Mssql {}
