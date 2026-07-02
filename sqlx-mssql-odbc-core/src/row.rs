use crate::{Mssql, MssqlColumn, MssqlValue};
use std::sync::Arc;

/// Minimal MSSQL row container used by the SQLx-core skeleton.
#[derive(Debug, Clone, Default)]
pub struct MssqlRow {
    columns: Arc<[MssqlColumn]>,
    values: Vec<MssqlValue>,
}

impl MssqlRow {
    /// Creates a row from column metadata and values.
    pub fn new(columns: Vec<MssqlColumn>, values: Vec<MssqlValue>) -> Self {
        Self::new_shared(columns.into(), values)
    }

    pub(crate) fn new_shared(columns: Arc<[MssqlColumn]>, values: Vec<MssqlValue>) -> Self {
        Self { columns, values }
    }
}

impl sqlx_core::row::Row for MssqlRow {
    type Database = Mssql;

    fn columns(&self) -> &[MssqlColumn] {
        self.columns.as_ref()
    }

    fn try_get_raw<I>(
        &self,
        index: I,
    ) -> Result<<Self::Database as sqlx_core::database::Database>::ValueRef<'_>, sqlx_core::Error>
    where
        I: sqlx_core::column::ColumnIndex<Self>,
    {
        let index = index.index(self)?;
        let value = self
            .values
            .get(index)
            .ok_or(sqlx_core::Error::ColumnIndexOutOfBounds {
                index,
                len: self.values.len(),
            })?;

        Ok(sqlx_core::value::Value::as_ref(value))
    }
}

impl sqlx_core::column::ColumnIndex<MssqlRow> for usize {
    fn index(&self, row: &MssqlRow) -> Result<usize, sqlx_core::Error> {
        if *self >= row.columns.len() {
            return Err(sqlx_core::Error::ColumnIndexOutOfBounds {
                index: *self,
                len: row.columns.len(),
            });
        }

        Ok(*self)
    }
}

impl sqlx_core::column::ColumnIndex<MssqlRow> for &str {
    fn index(&self, row: &MssqlRow) -> Result<usize, sqlx_core::Error> {
        if let Some(index) = row
            .columns
            .iter()
            .position(|column| sqlx_core::column::Column::name(column) == *self)
        {
            return Ok(index);
        }

        row.columns
            .iter()
            .position(|column| sqlx_core::column::Column::name(column).eq_ignore_ascii_case(self))
            .ok_or_else(|| sqlx_core::Error::ColumnNotFound((*self).to_owned()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{MssqlTypeInfo, MssqlValueKind};

    fn create_test_row() -> MssqlRow {
        MssqlRow::new(
            vec![
                MssqlColumn::new(
                    0,
                    "lowercase_col",
                    MssqlTypeInfo::new(odbc_api::DataType::Integer),
                    None,
                ),
                MssqlColumn::new(
                    1,
                    "UPPERCASE_COL",
                    MssqlTypeInfo::new(odbc_api::DataType::Varchar { length: None }),
                    None,
                ),
                MssqlColumn::new(
                    2,
                    "MixedCase_Col",
                    MssqlTypeInfo::new(odbc_api::DataType::Double),
                    None,
                ),
            ],
            vec![
                MssqlValue::new(MssqlValueKind::Integer(42)),
                MssqlValue::new(MssqlValueKind::Text("test".to_owned())),
                MssqlValue::new(MssqlValueKind::Double(std::f64::consts::PI)),
            ],
        )
    }

    #[test]
    fn column_lookup_is_case_insensitive() {
        let row = create_test_row();

        // Exact match
        assert_eq!(
            sqlx_core::column::ColumnIndex::<MssqlRow>::index(&"lowercase_col", &row).unwrap(),
            0
        );
        assert_eq!(
            sqlx_core::column::ColumnIndex::<MssqlRow>::index(&"UPPERCASE_COL", &row).unwrap(),
            1
        );
        assert_eq!(
            sqlx_core::column::ColumnIndex::<MssqlRow>::index(&"MixedCase_Col", &row).unwrap(),
            2
        );

        // Case-insensitive match
        assert_eq!(
            sqlx_core::column::ColumnIndex::<MssqlRow>::index(&"LOWERCASE_COL", &row).unwrap(),
            0
        );
        assert_eq!(
            sqlx_core::column::ColumnIndex::<MssqlRow>::index(&"uppercase_col", &row).unwrap(),
            1
        );
        assert_eq!(
            sqlx_core::column::ColumnIndex::<MssqlRow>::index(&"mixedcase_col", &row).unwrap(),
            2
        );
    }

    #[test]
    fn missing_column_reports_name() {
        let row = create_test_row();
        let error = sqlx_core::column::ColumnIndex::<MssqlRow>::index(&"missing", &row).unwrap_err();

        assert!(matches!(error, sqlx_core::Error::ColumnNotFound(name) if name == "missing"));
    }

    #[test]
    fn columns_returns_metadata_in_order() {
        use sqlx_core::column::Column;
        use sqlx_core::row::Row;

        let row = create_test_row();
        let columns = row.columns();

        assert_eq!(columns.len(), 3);
        assert_eq!(columns[0].ordinal(), 0);
        assert_eq!(columns[0].name(), "lowercase_col");
        assert_eq!(columns[1].ordinal(), 1);
        assert_eq!(columns[1].name(), "UPPERCASE_COL");
        assert_eq!(columns[2].ordinal(), 2);
        assert_eq!(columns[2].name(), "MixedCase_Col");
    }
}
