use crate::{Mssql, MssqlTypeInfo};

/// Column metadata for an MSSQL result set via ODBC.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "offline", derive(serde::Serialize, serde::Deserialize))]
pub struct MssqlColumn {
    ordinal: usize,
    name: String,
    type_info: MssqlTypeInfo,
}

impl MssqlColumn {
    /// Creates column metadata.
    pub fn new(ordinal: usize, name: impl Into<String>, type_info: MssqlTypeInfo) -> Self {
        Self {
            ordinal,
            name: name.into(),
            type_info,
        }
    }
}

impl sqlx_core::column::Column for MssqlColumn {
    type Database = Mssql;

    fn ordinal(&self) -> usize {
        self.ordinal
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn type_info(&self) -> &MssqlTypeInfo {
        &self.type_info
    }
}
