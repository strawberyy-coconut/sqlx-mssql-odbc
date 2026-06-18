use odbc_api::DataType;
use std::fmt::{Display, Formatter, Result as FmtResult};

/// Type information for an MSSQL ODBC value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MssqlTypeInfo {
    data_type: DataType,
}

#[cfg(feature = "offline")]
mod serde_impl {
    use super::*;
    use serde::de::{self, Deserializer, MapAccess, Visitor};
    use serde::ser::{SerializeStruct, Serializer};
    use std::fmt;

    impl serde::Serialize for MssqlTypeInfo {
        fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
            use sqlx_core::type_info::TypeInfo;

            let mut state = serializer.serialize_struct("MssqlTypeInfo", 6)?;
            let name = TypeInfo::name(self);
            state.serialize_field("type_name", name)?;
            match &self.data_type {
                DataType::Decimal { precision, scale }
                | DataType::Numeric { precision, scale } => {
                    state.serialize_field("precision", precision)?;
                    state.serialize_field("scale", scale)?;
                }
                DataType::Float { precision } => {
                    state.serialize_field("precision", precision)?;
                }
                DataType::Time { precision } | DataType::Timestamp { precision } => {
                    state.serialize_field("precision", &(*precision as usize))?;
                }
                DataType::Char { length }
                | DataType::Varchar { length }
                | DataType::WChar { length }
                | DataType::WVarchar { length } => {
                    state.serialize_field("length", &length.map(|n| n.get()))?;
                }
                DataType::Binary { length } | DataType::Varbinary { length } => {
                    state.serialize_field("length", &length.map(|n| n.get()))?;
                }
                DataType::Other {
                    data_type: sql_type,
                    column_size,
                    decimal_digits,
                } => {
                    state.serialize_field("sql_data_type", &sql_type.0)?;
                    state.serialize_field("column_size", &column_size.map(|n| n.get()))?;
                    state.serialize_field("decimal_digits", decimal_digits)?;
                }
                _ => {}
            }
            state.end()
        }
    }

    impl<'de> serde::Deserialize<'de> for MssqlTypeInfo {
        fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
            #[derive(serde::Deserialize)]
            #[serde(field_identifier, rename_all = "snake_case")]
            enum Field {
                TypeName,
                Precision,
                Scale,
                Length,
                SqlDataType,
                ColumnSize,
                DecimalDigits,
            }

            struct MssqlTypeInfoVisitor;

            impl<'de> Visitor<'de> for MssqlTypeInfoVisitor {
                type Value = MssqlTypeInfo;

                fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                    formatter.write_str("a MssqlTypeInfo struct")
                }

                fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Self::Value, A::Error> {
                    let mut type_name: Option<String> = None;
                    let mut precision: Option<usize> = None;
                    let mut scale: Option<i16> = None;
                    let mut length: Option<usize> = None;
                    let mut sql_data_type: Option<i16> = None;
                    let mut column_size: Option<usize> = None;
                    let mut decimal_digits: Option<i16> = None;

                    while let Some(key) = map.next_key()? {
                        match key {
                            Field::TypeName => {
                                if type_name.is_some() {
                                    return Err(de::Error::duplicate_field("type_name"));
                                }
                                type_name = Some(map.next_value()?);
                            }
                            Field::Precision => {
                                if precision.is_some() {
                                    return Err(de::Error::duplicate_field("precision"));
                                }
                                precision = Some(map.next_value()?);
                            }
                            Field::Scale => {
                                if scale.is_some() {
                                    return Err(de::Error::duplicate_field("scale"));
                                }
                                scale = Some(map.next_value()?);
                            }
                            Field::Length => {
                                if length.is_some() {
                                    return Err(de::Error::duplicate_field("length"));
                                }
                                length = map.next_value()?;
                            }
                            Field::SqlDataType => {
                                if sql_data_type.is_some() {
                                    return Err(de::Error::duplicate_field("sql_data_type"));
                                }
                                sql_data_type = Some(map.next_value()?);
                            }
                            Field::ColumnSize => {
                                if column_size.is_some() {
                                    return Err(de::Error::duplicate_field("column_size"));
                                }
                                column_size = map.next_value()?;
                            }
                            Field::DecimalDigits => {
                                if decimal_digits.is_some() {
                                    return Err(de::Error::duplicate_field("decimal_digits"));
                                }
                                decimal_digits = Some(map.next_value()?);
                            }
                        }
                    }

                    let type_name =
                        type_name.ok_or_else(|| de::Error::missing_field("type_name"))?;

                    let data_type = match type_name.as_str() {
                        "BIGINT" => DataType::BigInt,
                        "BINARY" => DataType::Binary {
                            length: length.and_then(std::num::NonZeroUsize::new),
                        },
                        "BIT" => DataType::Bit,
                        "CHAR" => DataType::Char {
                            length: length.and_then(std::num::NonZeroUsize::new),
                        },
                        "DATE" => DataType::Date,
                        "DECIMAL" => DataType::Decimal {
                            precision: precision.unwrap_or(0),
                            scale: scale.unwrap_or(0),
                        },
                        "DOUBLE" => DataType::Double,
                        "FLOAT" => DataType::Float {
                            precision: precision.unwrap_or(0),
                        },
                        "INTEGER" => DataType::Integer,
                        "LONGVARBINARY" => DataType::LongVarbinary {
                            length: length.and_then(std::num::NonZeroUsize::new),
                        },
                        "LONGVARCHAR" => DataType::LongVarchar {
                            length: length.and_then(std::num::NonZeroUsize::new),
                        },
                        "NUMERIC" => DataType::Numeric {
                            precision: precision.unwrap_or(0),
                            scale: scale.unwrap_or(0),
                        },
                        "REAL" => DataType::Real,
                        "SMALLINT" => DataType::SmallInt,
                        "TIME" => DataType::Time {
                            precision: precision.unwrap_or(0) as i16,
                        },
                        "TIMESTAMP" => DataType::Timestamp {
                            precision: precision.unwrap_or(0) as i16,
                        },
                        "TINYINT" => DataType::TinyInt,
                        "VARBINARY" => DataType::Varbinary {
                            length: length.and_then(std::num::NonZeroUsize::new),
                        },
                        "VARCHAR" => DataType::Varchar {
                            length: length.and_then(std::num::NonZeroUsize::new),
                        },
                        "WCHAR" => DataType::WChar {
                            length: length.and_then(std::num::NonZeroUsize::new),
                        },
                        "WLONGVARCHAR" => DataType::WLongVarchar {
                            length: length.and_then(std::num::NonZeroUsize::new),
                        },
                        "WVARCHAR" => DataType::WVarchar {
                            length: length.and_then(std::num::NonZeroUsize::new),
                        },
                        "UNKNOWN" => DataType::Unknown,
                        "UNIQUEIDENTIFIER" => DataType::Other {
                            data_type: odbc_api::sys::SqlDataType(-11),
                            column_size: None,
                            decimal_digits: 0,
                        },
                        "SQL_VARIANT" => DataType::Other {
                            data_type: odbc_api::sys::SqlDataType(-150),
                            column_size: None,
                            decimal_digits: 0,
                        },
                        "UDT" => DataType::Other {
                            data_type: odbc_api::sys::SqlDataType(-151),
                            column_size: None,
                            decimal_digits: 0,
                        },
                        "XML" => DataType::Other {
                            data_type: odbc_api::sys::SqlDataType(-152),
                            column_size: None,
                            decimal_digits: 0,
                        },
                        "DATETIMEOFFSET" => DataType::Other {
                            data_type: odbc_api::sys::SqlDataType(-155),
                            column_size: None,
                            decimal_digits: 0,
                        },
                        "HIERARCHYID" => DataType::Other {
                            data_type: odbc_api::sys::SqlDataType(-156),
                            column_size: None,
                            decimal_digits: 0,
                        },
                        "OTHER" => {
                            let raw_type: i16 = sql_data_type.unwrap_or(0);
                            let raw_type = odbc_api::sys::SqlDataType(raw_type);
                            DataType::new(
                                raw_type,
                                column_size.unwrap_or(0),
                                decimal_digits.unwrap_or(0),
                            )
                        }
                        _ => DataType::Other {
                            data_type: odbc_api::sys::SqlDataType::UNKNOWN_TYPE,
                            column_size: length.and_then(std::num::NonZeroUsize::new),
                            decimal_digits: 0,
                        },
                    };

                    Ok(MssqlTypeInfo { data_type })
                }
            }

            deserializer.deserialize_struct(
                "MssqlTypeInfo",
                &[
                    "type_name",
                    "precision",
                    "scale",
                    "length",
                    "sql_data_type",
                    "column_size",
                    "decimal_digits",
                ],
                MssqlTypeInfoVisitor,
            )
        }
    }
}

impl MssqlTypeInfo {
    /// Creates type information from an `odbc-api` data type.
    pub const fn new(data_type: DataType) -> Self {
        Self { data_type }
    }

    /// Returns the underlying `odbc-api` data type.
    pub const fn data_type(&self) -> DataType {
        self.data_type
    }

    /// `BIGINT` type information.
    pub const BIGINT: Self = Self::new(DataType::BigInt);

    /// `BIT` type information.
    pub const BIT: Self = Self::new(DataType::Bit);

    /// `DATE` type information.
    pub const DATE: Self = Self::new(DataType::Date);

    /// `DOUBLE` type information.
    pub const DOUBLE: Self = Self::new(DataType::Double);

    /// `INTEGER` type information.
    pub const INTEGER: Self = Self::new(DataType::Integer);

    /// `REAL` type information.
    pub const REAL: Self = Self::new(DataType::Real);

    /// `SMALLINT` type information.
    pub const SMALLINT: Self = Self::new(DataType::SmallInt);

    /// `TINYINT` type information.
    pub const TINYINT: Self = Self::new(DataType::TinyInt);

    /// `UNKNOWN` type information.
    pub const UNKNOWN: Self = Self::new(DataType::Unknown);

    /// `TIME` type information with zero fractional precision.
    pub const TIME: Self = Self::new(DataType::Time { precision: 0 });

    /// `TIMESTAMP` type information with zero fractional precision.
    pub const TIMESTAMP: Self = Self::new(DataType::Timestamp { precision: 0 });

    /// Creates `CHAR` type information.
    pub const fn char(length: Option<std::num::NonZeroUsize>) -> Self {
        Self::new(DataType::Char { length })
    }

    /// Creates `FLOAT` type information.
    pub const fn float(precision: usize) -> Self {
        Self::new(DataType::Float { precision })
    }

    /// Creates `TIME` type information.
    pub const fn time(precision: i16) -> Self {
        Self::new(DataType::Time { precision })
    }

    /// Creates `TIMESTAMP` type information.
    pub const fn timestamp(precision: i16) -> Self {
        Self::new(DataType::Timestamp { precision })
    }

    /// Creates `VARCHAR` type information.
    pub const fn varchar(length: Option<std::num::NonZeroUsize>) -> Self {
        Self::new(DataType::Varchar { length })
    }

    /// Creates `VARBINARY` type information.
    pub const fn varbinary(length: Option<std::num::NonZeroUsize>) -> Self {
        Self::new(DataType::Varbinary { length })
    }

    /// Creates `DECIMAL` type information.
    pub const fn decimal(precision: usize, scale: i16) -> Self {
        Self::new(DataType::Decimal { precision, scale })
    }

    /// Creates `NUMERIC` type information.
    pub const fn numeric(precision: usize, scale: i16) -> Self {
        Self::new(DataType::Numeric { precision, scale })
    }

    /// Creates `UNIQUEIDENTIFIER` (GUID) type information.
    ///
    /// MSSQL reports this as `DataType::Other` with SQL type code -11 (SQL_GUID).
    pub const fn guid() -> Self {
        Self::new(DataType::Other {
            data_type: odbc_api::sys::SqlDataType(-11),
            column_size: None,
            decimal_digits: 0,
        })
    }

    /// Creates `XML` type information.
    ///
    /// MSSQL reports this as `DataType::Other` with SQL type code -152 (SQL_SS_XML).
    pub const fn xml() -> Self {
        Self::new(DataType::Other {
            data_type: odbc_api::sys::SqlDataType(-152),
            column_size: None,
            decimal_digits: 0,
        })
    }

    /// Creates `DATETIMEOFFSET` type information.
    ///
    /// MSSQL reports this as `DataType::Other` with SQL type code -155 (SQL_SS_TIMESTAMPOFFSET).
    pub const fn datetimeoffset() -> Self {
        Self::new(DataType::Other {
            data_type: odbc_api::sys::SqlDataType(-155),
            column_size: None,
            decimal_digits: 0,
        })
    }

    /// Creates `GEOMETRY`/`GEOGRAPHY` (spatial UDT) type information.
    ///
    /// MSSQL reports spatial types as `DataType::Other` with SQL type code -151
    /// (SQL_SS_UDT — CLR User-Defined Type), which covers both `geometry` and
    /// `geography` columns.
    pub const fn geometry() -> Self {
        Self::new(DataType::Other {
            data_type: odbc_api::sys::SqlDataType(-151),
            column_size: None,
            decimal_digits: 0,
        })
    }
}

impl sqlx_core::type_info::TypeInfo for MssqlTypeInfo {
    fn is_null(&self) -> bool {
        false
    }

    fn name(&self) -> &str {
        match self.data_type {
            DataType::BigInt => "BIGINT",
            DataType::Binary { .. } => "BINARY",
            DataType::Bit => "BIT",
            DataType::Char { .. } => "CHAR",
            DataType::Date => "DATE",
            DataType::Decimal { .. } => "DECIMAL",
            DataType::Double => "DOUBLE",
            DataType::Float { .. } => "FLOAT",
            DataType::Integer => "INTEGER",
            DataType::LongVarbinary { .. } => "LONGVARBINARY",
            DataType::LongVarchar { .. } => "LONGVARCHAR",
            DataType::Numeric { .. } => "NUMERIC",
            DataType::Real => "REAL",
            DataType::SmallInt => "SMALLINT",
            DataType::Time { .. } => "TIME",
            DataType::Timestamp { .. } => "TIMESTAMP",
            DataType::TinyInt => "TINYINT",
            DataType::Varbinary { .. } => "VARBINARY",
            DataType::Varchar { .. } => "VARCHAR",
            DataType::WChar { .. } => "WCHAR",
            DataType::WLongVarchar { .. } => "WLONGVARCHAR",
            DataType::WVarchar { .. } => "WVARCHAR",
            DataType::Unknown => "UNKNOWN",
            DataType::Other {
                data_type: sql_type, ..
            } if sql_type.0 == -11 => "UNIQUEIDENTIFIER",
            DataType::Other {
                data_type: sql_type, ..
            } if sql_type.0 == -150 => "SQL_VARIANT",
            DataType::Other {
                data_type: sql_type, ..
            } if sql_type.0 == -151 => "UDT",
            DataType::Other {
                data_type: sql_type, ..
            } if sql_type.0 == -152 => "XML",
            DataType::Other {
                data_type: sql_type, ..
            } if sql_type.0 == -155 => "DATETIMEOFFSET",
            DataType::Other {
                data_type: sql_type, ..
            } if sql_type.0 == -156 => "HIERARCHYID",
            DataType::Other { .. } => "OTHER",
        }
    }
}

/// Helper predicates for `odbc-api` data type groups.
pub trait DataTypeExt {
    /// Returns the canonical display name for this type.
    fn name(self) -> &'static str;

    /// Returns whether this type carries character data.
    fn accepts_character_data(self) -> bool;

    /// Returns whether this type carries binary data.
    fn accepts_binary_data(self) -> bool;

    /// Returns whether this type carries numeric data.
    fn accepts_numeric_data(self) -> bool;

    /// Returns whether this type carries date or time data.
    fn accepts_datetime_data(self) -> bool;
}

impl DataTypeExt for DataType {
    fn name(self) -> &'static str {
        match self {
            DataType::BigInt => "BIGINT",
            DataType::Binary { .. } => "BINARY",
            DataType::Bit => "BIT",
            DataType::Char { .. } => "CHAR",
            DataType::Date => "DATE",
            DataType::Decimal { .. } => "DECIMAL",
            DataType::Double => "DOUBLE",
            DataType::Float { .. } => "FLOAT",
            DataType::Integer => "INTEGER",
            DataType::LongVarbinary { .. } => "LONGVARBINARY",
            DataType::LongVarchar { .. } => "LONGVARCHAR",
            DataType::Numeric { .. } => "NUMERIC",
            DataType::Real => "REAL",
            DataType::SmallInt => "SMALLINT",
            DataType::Time { .. } => "TIME",
            DataType::Timestamp { .. } => "TIMESTAMP",
            DataType::TinyInt => "TINYINT",
            DataType::Varbinary { .. } => "VARBINARY",
            DataType::Varchar { .. } => "VARCHAR",
            DataType::WChar { .. } => "WCHAR",
            DataType::WLongVarchar { .. } => "WLONGVARCHAR",
            DataType::WVarchar { .. } => "WVARCHAR",
            DataType::Unknown => "UNKNOWN",
            DataType::Other { data_type, .. } if data_type.0 == -11 => "UNIQUEIDENTIFIER",
            DataType::Other { data_type, .. } if data_type.0 == -150 => "SQL_VARIANT",
            DataType::Other { data_type, .. } if data_type.0 == -151 => "UDT",
            DataType::Other { data_type, .. } if data_type.0 == -152 => "XML",
            DataType::Other { data_type, .. } if data_type.0 == -155 => "DATETIMEOFFSET",
            DataType::Other { data_type, .. } if data_type.0 == -156 => "HIERARCHYID",
            DataType::Other { .. } => "OTHER",
        }
    }

    fn accepts_character_data(self) -> bool {
        matches!(
            self,
            DataType::Char { .. }
                | DataType::Varchar { .. }
                | DataType::LongVarchar { .. }
                | DataType::WChar { .. }
                | DataType::WVarchar { .. }
                | DataType::WLongVarchar { .. }
        )
    }

    fn accepts_binary_data(self) -> bool {
        matches!(
            self,
            DataType::Binary { .. } | DataType::Varbinary { .. } | DataType::LongVarbinary { .. }
        )
    }

    fn accepts_numeric_data(self) -> bool {
        matches!(
            self,
            DataType::TinyInt
                | DataType::SmallInt
                | DataType::Integer
                | DataType::BigInt
                | DataType::Real
                | DataType::Float { .. }
                | DataType::Double
                | DataType::Decimal { .. }
                | DataType::Numeric { .. }
        )
    }

    fn accepts_datetime_data(self) -> bool {
        matches!(
            self,
            DataType::Date | DataType::Time { .. } | DataType::Timestamp { .. }
        ) || matches!(self, DataType::Other { data_type, .. } if data_type.0 == -155)
    }
}

impl Display for MssqlTypeInfo {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        f.write_str(sqlx_core::type_info::TypeInfo::name(self))
    }
}
