use crate::{Mssql, MssqlArguments, MssqlColumn, MssqlTypeInfo};
use sqlx_core::sql_str::SqlStr;

pub(crate) type ParamInfo = Option<sqlx_core::Either<Vec<MssqlTypeInfo>, usize>>;

/// Borrows `ParamInfo` into the form returned by [`Statement::parameters`].
pub(crate) fn borrow_param_info(
    info: &ParamInfo,
) -> Option<sqlx_core::Either<&[MssqlTypeInfo], usize>> {
    info.as_ref().map(|p| match p {
        sqlx_core::Either::Left(types) => sqlx_core::Either::Left(types.as_slice()),
        sqlx_core::Either::Right(count) => sqlx_core::Either::Right(*count),
    })
}

/// Converts the borrowed form back into owned `ParamInfo` for [`Describe`].
#[cfg(feature = "offline")]
pub(crate) fn clone_param_info(
    info: Option<sqlx_core::Either<&[MssqlTypeInfo], usize>>,
) -> ParamInfo {
    info.map(|p| match p {
        sqlx_core::Either::Left(types) => sqlx_core::Either::Left(types.to_vec()),
        sqlx_core::Either::Right(count) => sqlx_core::Either::Right(count),
    })
}

/// Prepared statement metadata for MSSQL via ODBC.
#[derive(Debug, Clone)]
pub struct MssqlStatement {
    sql: SqlStr,
    columns: Vec<MssqlColumn>,
    parameters: ParamInfo,
}

impl MssqlStatement {
    /// Creates a statement metadata value.
    pub fn new(sql: SqlStr, columns: Vec<MssqlColumn>, parameters: ParamInfo) -> Self {
        Self {
            sql,
            columns,
            parameters,
        }
    }
}

impl sqlx_core::statement::Statement for MssqlStatement {
    type Database = Mssql;

    fn into_sql(self) -> SqlStr {
        self.sql
    }

    fn sql(&self) -> &SqlStr {
        &self.sql
    }

    fn parameters(&self) -> Option<sqlx_core::Either<&[MssqlTypeInfo], usize>> {
        borrow_param_info(&self.parameters)
    }

    fn columns(&self) -> &[MssqlColumn] {
        &self.columns
    }

    sqlx_core::impl_statement_query!(MssqlArguments);
}

impl sqlx_core::column::ColumnIndex<MssqlStatement> for usize {
    fn index(&self, statement: &MssqlStatement) -> Result<usize, sqlx_core::Error> {
        if *self >= statement.columns.len() {
            return Err(sqlx_core::Error::ColumnIndexOutOfBounds {
                index: *self,
                len: statement.columns.len(),
            });
        }

        Ok(*self)
    }
}

impl sqlx_core::column::ColumnIndex<MssqlStatement> for &str {
    fn index(&self, statement: &MssqlStatement) -> Result<usize, sqlx_core::Error> {
        if let Some(index) = statement
            .columns
            .iter()
            .position(|column| sqlx_core::column::Column::name(column) == *self)
        {
            return Ok(index);
        }

        statement
            .columns
            .iter()
            .position(|column| sqlx_core::column::Column::name(column).eq_ignore_ascii_case(self))
            .ok_or_else(|| sqlx_core::Error::ColumnNotFound((*self).to_owned()))
    }
}
