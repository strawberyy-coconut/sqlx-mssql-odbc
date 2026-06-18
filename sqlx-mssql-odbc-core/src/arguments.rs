use crate::DataTypeExt;
use odbc_api::{
    parameter::{InputParameter, VarBinaryBox, VarCharBox, VarWCharBox, WithDataType},
    IntoParameter, Nullable,
};

/// Values that can currently be bound to MSSQL ODBC parameters.
#[derive(Debug, Clone, PartialEq)]
pub enum MssqlArgumentValue {
    /// UTF-8 text parameter.
    Text(String),
    /// Binary parameter.
    Bytes(Vec<u8>),
    /// Signed integer parameter.
    Int(i64),
    /// Unsigned integer parameter.
    UInt(u64),
    /// Boolean parameter.
    Bit(bool),
    /// Floating point parameter.
    Float(f64),
    /// Date parameter.
    Date(odbc_api::sys::Date),
    /// Time parameter.
    Time(odbc_api::sys::Time),
    /// Timestamp parameter.
    Timestamp(odbc_api::sys::Timestamp),
    /// Typed NULL parameter.
    Null(crate::MssqlTypeInfo),
}

/// Values that can be bound to MSSQL ODBC parameters.
#[derive(Debug, Default, Clone, PartialEq)]
pub struct MssqlArguments {
    values: Vec<MssqlArgumentValue>,
}

/// Owned MSSQL ODBC parameter storage ready to bind with `odbc-api`.
///
/// `odbc-api` implements `ParameterCollectionRef` for `&[Box<dyn InputParameter>]`, so executor
/// code can pass `collection.as_slice()` to `Connection::execute` or `Preallocated::execute`.
#[derive(Default)]
pub struct MssqlParameterCollection {
    parameters: Vec<Box<dyn InputParameter>>,
}

impl std::fmt::Debug for MssqlParameterCollection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MssqlParameterCollection")
            .field("len", &self.parameters.len())
            .finish()
    }
}

impl MssqlParameterCollection {
    /// Converts raw SQLx MSSQL ODBC argument values into owned `odbc-api` input parameters.
    pub fn from_values(values: &[MssqlArgumentValue]) -> Self {
        let parameters = values.iter().map(value_to_parameter).collect();

        Self { parameters }
    }

    /// Returns the number of parameters.
    pub fn len(&self) -> usize {
        self.parameters.len()
    }

    /// Returns `true` when no parameters are present.
    pub fn is_empty(&self) -> bool {
        self.parameters.is_empty()
    }

    /// Returns the parameter slice accepted by `odbc-api` execution methods.
    pub fn as_slice(&self) -> &[Box<dyn InputParameter>] {
        &self.parameters
    }
}

impl MssqlArguments {
    /// Adds a raw ODBC argument value.
    pub fn add_value(&mut self, value: MssqlArgumentValue) {
        self.values.push(value);
    }

    /// Returns the number of arguments.
    pub fn len(&self) -> usize {
        self.values.len()
    }

    /// Returns `true` when no arguments have been added.
    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    /// Returns the raw argument values.
    pub fn values(&self) -> &[MssqlArgumentValue] {
        &self.values
    }

    /// Converts these arguments into owned `odbc-api` parameters.
    pub fn to_odbc_parameter_collection(&self) -> MssqlParameterCollection {
        MssqlParameterCollection::from_values(&self.values)
    }
}

impl sqlx_core::arguments::Arguments for MssqlArguments {
    type Database = crate::Mssql;

    fn reserve(&mut self, additional: usize, _size: usize) {
        self.values.reserve(additional);
    }

    fn add<'t, T>(&mut self, value: T) -> Result<(), sqlx_core::error::BoxDynError>
    where
        T: sqlx_core::encode::Encode<'t, Self::Database> + sqlx_core::types::Type<Self::Database>,
    {
        let _ = value.encode(&mut self.values)?;
        Ok(())
    }

    fn len(&self) -> usize {
        self.values.len()
    }
}

sqlx_core::impl_into_arguments_for_arguments!(MssqlArguments);

impl<'q, T> sqlx_core::encode::Encode<'q, crate::Mssql> for Option<T>
where
    T: sqlx_core::encode::Encode<'q, crate::Mssql> + sqlx_core::types::Type<crate::Mssql> + 'q,
{
    fn encode(
        self,
        buf: &mut Vec<MssqlArgumentValue>,
    ) -> Result<sqlx_core::encode::IsNull, sqlx_core::error::BoxDynError> {
        match self {
            Some(value) => value.encode(buf),
            None => {
                buf.push(MssqlArgumentValue::Null(T::type_info()));
                Ok(sqlx_core::encode::IsNull::Yes)
            }
        }
    }

    fn encode_by_ref(
        &self,
        buf: &mut Vec<MssqlArgumentValue>,
    ) -> Result<sqlx_core::encode::IsNull, sqlx_core::error::BoxDynError> {
        match self {
            Some(value) => value.encode_by_ref(buf),
            None => {
                buf.push(MssqlArgumentValue::Null(T::type_info()));
                Ok(sqlx_core::encode::IsNull::Yes)
            }
        }
    }

    fn produces(&self) -> Option<crate::MssqlTypeInfo> {
        match self {
            Some(value) => value.produces(),
            None => Some(T::type_info()),
        }
    }
}

macro_rules! impl_integer {
    ($ty:ty, $type_info:expr, $($compatible:pat_param)|+ $(,)?) => {
        impl sqlx_core::types::Type<crate::Mssql> for $ty {
            fn type_info() -> crate::MssqlTypeInfo {
                crate::MssqlTypeInfo::new($type_info)
            }

            fn compatible(ty: &crate::MssqlTypeInfo) -> bool {
                matches!(
                    ty.data_type(),
                    $($compatible)|+
                        | odbc_api::DataType::Numeric { .. }
                        | odbc_api::DataType::Decimal { .. }
                ) || ty.data_type().accepts_character_data()
            }
        }

        impl<'q> sqlx_core::encode::Encode<'q, crate::Mssql> for $ty {
            fn encode_by_ref(
                &self,
                buf: &mut Vec<MssqlArgumentValue>,
            ) -> Result<sqlx_core::encode::IsNull, sqlx_core::error::BoxDynError> {
                buf.push(MssqlArgumentValue::Int(i64::from(*self)));
                Ok(sqlx_core::encode::IsNull::No)
            }
        }
    };
}

impl_integer!(
    i8,
    odbc_api::DataType::TinyInt,
    odbc_api::DataType::TinyInt
        | odbc_api::DataType::SmallInt
        | odbc_api::DataType::Integer
        | odbc_api::DataType::BigInt,
);
impl_integer!(
    i16,
    odbc_api::DataType::SmallInt,
    odbc_api::DataType::TinyInt
        | odbc_api::DataType::SmallInt
        | odbc_api::DataType::Integer
        | odbc_api::DataType::BigInt,
);
impl_integer!(
    i32,
    odbc_api::DataType::Integer,
    odbc_api::DataType::TinyInt
        | odbc_api::DataType::SmallInt
        | odbc_api::DataType::Integer
        | odbc_api::DataType::BigInt,
);
impl_integer!(
    i64,
    odbc_api::DataType::BigInt,
    odbc_api::DataType::TinyInt
        | odbc_api::DataType::SmallInt
        | odbc_api::DataType::Integer
        | odbc_api::DataType::BigInt,
);

macro_rules! impl_unsigned {
    ($ty:ty, $type_info:expr, $($compatible:pat_param)|+ $(,)?) => {
        impl sqlx_core::types::Type<crate::Mssql> for $ty {
            fn type_info() -> crate::MssqlTypeInfo {
                crate::MssqlTypeInfo::new($type_info)
            }

            fn compatible(ty: &crate::MssqlTypeInfo) -> bool {
                matches!(
                    ty.data_type(),
                    $($compatible)|+
                        | odbc_api::DataType::Numeric { .. }
                        | odbc_api::DataType::Decimal { .. }
                ) || ty.data_type().accepts_character_data()
            }
        }

        impl<'q> sqlx_core::encode::Encode<'q, crate::Mssql> for $ty {
            fn encode_by_ref(
                &self,
                buf: &mut Vec<MssqlArgumentValue>,
            ) -> Result<sqlx_core::encode::IsNull, sqlx_core::error::BoxDynError> {
                buf.push(MssqlArgumentValue::Int(i64::from(*self)));
                Ok(sqlx_core::encode::IsNull::No)
            }
        }
    };
}

impl_unsigned!(
    u8,
    odbc_api::DataType::TinyInt,
    odbc_api::DataType::TinyInt
        | odbc_api::DataType::SmallInt
        | odbc_api::DataType::Integer
        | odbc_api::DataType::BigInt,
);
impl_unsigned!(
    u16,
    odbc_api::DataType::SmallInt,
    odbc_api::DataType::SmallInt | odbc_api::DataType::Integer | odbc_api::DataType::BigInt,
);
impl_unsigned!(
    u32,
    odbc_api::DataType::Integer,
    odbc_api::DataType::Integer | odbc_api::DataType::BigInt,
);

impl sqlx_core::types::Type<crate::Mssql> for u64 {
    fn type_info() -> crate::MssqlTypeInfo {
        crate::MssqlTypeInfo::BIGINT
    }

    fn compatible(ty: &crate::MssqlTypeInfo) -> bool {
        matches!(
            ty.data_type(),
            odbc_api::DataType::Integer
                | odbc_api::DataType::BigInt
                | odbc_api::DataType::Numeric { .. }
                | odbc_api::DataType::Decimal { .. }
        ) || ty.data_type().accepts_character_data()
    }
}

impl<'q> sqlx_core::encode::Encode<'q, crate::Mssql> for u64 {
    fn encode_by_ref(
        &self,
        buf: &mut Vec<MssqlArgumentValue>,
    ) -> Result<sqlx_core::encode::IsNull, sqlx_core::error::BoxDynError> {
        if let Ok(value) = i64::try_from(*self) {
            buf.push(MssqlArgumentValue::Int(value));
        } else {
            buf.push(MssqlArgumentValue::UInt(*self));
        }

        Ok(sqlx_core::encode::IsNull::No)
    }
}

impl sqlx_core::types::Type<crate::Mssql> for bool {
    fn type_info() -> crate::MssqlTypeInfo {
        crate::MssqlTypeInfo::new(odbc_api::DataType::Bit)
    }

    fn compatible(ty: &crate::MssqlTypeInfo) -> bool {
        ty.data_type().accepts_numeric_data() || ty.data_type().accepts_character_data()
    }
}

impl<'q> sqlx_core::encode::Encode<'q, crate::Mssql> for bool {
    fn encode_by_ref(
        &self,
        buf: &mut Vec<MssqlArgumentValue>,
    ) -> Result<sqlx_core::encode::IsNull, sqlx_core::error::BoxDynError> {
        buf.push(MssqlArgumentValue::Bit(*self));
        Ok(sqlx_core::encode::IsNull::No)
    }
}

impl sqlx_core::types::Type<crate::Mssql> for f32 {
    fn type_info() -> crate::MssqlTypeInfo {
        crate::MssqlTypeInfo::new(odbc_api::DataType::Real)
    }

    fn compatible(ty: &crate::MssqlTypeInfo) -> bool {
        ty.data_type().accepts_numeric_data() || ty.data_type().accepts_character_data()
    }
}

impl<'q> sqlx_core::encode::Encode<'q, crate::Mssql> for f32 {
    fn encode_by_ref(
        &self,
        buf: &mut Vec<MssqlArgumentValue>,
    ) -> Result<sqlx_core::encode::IsNull, sqlx_core::error::BoxDynError> {
        buf.push(MssqlArgumentValue::Float(f64::from(*self)));
        Ok(sqlx_core::encode::IsNull::No)
    }
}

impl sqlx_core::types::Type<crate::Mssql> for f64 {
    fn type_info() -> crate::MssqlTypeInfo {
        crate::MssqlTypeInfo::new(odbc_api::DataType::Double)
    }

    fn compatible(ty: &crate::MssqlTypeInfo) -> bool {
        ty.data_type().accepts_numeric_data() || ty.data_type().accepts_character_data()
    }
}

impl<'q> sqlx_core::encode::Encode<'q, crate::Mssql> for f64 {
    fn encode_by_ref(
        &self,
        buf: &mut Vec<MssqlArgumentValue>,
    ) -> Result<sqlx_core::encode::IsNull, sqlx_core::error::BoxDynError> {
        buf.push(MssqlArgumentValue::Float(*self));
        Ok(sqlx_core::encode::IsNull::No)
    }
}

impl sqlx_core::types::Type<crate::Mssql> for str {
    fn type_info() -> crate::MssqlTypeInfo {
        crate::MssqlTypeInfo::new(odbc_api::DataType::WVarchar { length: None })
    }

    fn compatible(ty: &crate::MssqlTypeInfo) -> bool {
        ty.data_type().accepts_character_data()
    }
}

impl sqlx_core::types::Type<crate::Mssql> for String {
    fn type_info() -> crate::MssqlTypeInfo {
        <str as sqlx_core::types::Type<crate::Mssql>>::type_info()
    }
}

impl<'q> sqlx_core::encode::Encode<'q, crate::Mssql> for &'q str {
    fn encode_by_ref(
        &self,
        buf: &mut Vec<MssqlArgumentValue>,
    ) -> Result<sqlx_core::encode::IsNull, sqlx_core::error::BoxDynError> {
        buf.push(MssqlArgumentValue::Text((*self).to_owned()));
        Ok(sqlx_core::encode::IsNull::No)
    }
}

impl<'q> sqlx_core::encode::Encode<'q, crate::Mssql> for String {
    fn encode_by_ref(
        &self,
        buf: &mut Vec<MssqlArgumentValue>,
    ) -> Result<sqlx_core::encode::IsNull, sqlx_core::error::BoxDynError> {
        buf.push(MssqlArgumentValue::Text(self.clone()));
        Ok(sqlx_core::encode::IsNull::No)
    }
}

impl sqlx_core::types::Type<crate::Mssql> for [u8] {
    fn type_info() -> crate::MssqlTypeInfo {
        crate::MssqlTypeInfo::new(odbc_api::DataType::Varbinary { length: None })
    }

    fn compatible(ty: &crate::MssqlTypeInfo) -> bool {
        ty.data_type().accepts_binary_data() || ty.data_type().accepts_character_data()
    }
}

impl sqlx_core::types::Type<crate::Mssql> for Vec<u8> {
    fn type_info() -> crate::MssqlTypeInfo {
        <[u8] as sqlx_core::types::Type<crate::Mssql>>::type_info()
    }

    fn compatible(ty: &crate::MssqlTypeInfo) -> bool {
        <[u8] as sqlx_core::types::Type<crate::Mssql>>::compatible(ty)
    }
}

impl<'q> sqlx_core::encode::Encode<'q, crate::Mssql> for &'q [u8] {
    fn encode_by_ref(
        &self,
        buf: &mut Vec<MssqlArgumentValue>,
    ) -> Result<sqlx_core::encode::IsNull, sqlx_core::error::BoxDynError> {
        buf.push(MssqlArgumentValue::Bytes((*self).to_owned()));
        Ok(sqlx_core::encode::IsNull::No)
    }
}

impl<'q> sqlx_core::encode::Encode<'q, crate::Mssql> for Vec<u8> {
    fn encode_by_ref(
        &self,
        buf: &mut Vec<MssqlArgumentValue>,
    ) -> Result<sqlx_core::encode::IsNull, sqlx_core::error::BoxDynError> {
        buf.push(MssqlArgumentValue::Bytes(self.clone()));
        Ok(sqlx_core::encode::IsNull::No)
    }
}

impl sqlx_core::types::Type<crate::Mssql> for odbc_api::sys::Date {
    fn type_info() -> crate::MssqlTypeInfo {
        crate::MssqlTypeInfo::DATE
    }

    fn compatible(ty: &crate::MssqlTypeInfo) -> bool {
        matches!(ty.data_type(), odbc_api::DataType::Date)
    }
}

impl<'q> sqlx_core::encode::Encode<'q, crate::Mssql> for odbc_api::sys::Date {
    fn encode_by_ref(
        &self,
        buf: &mut Vec<MssqlArgumentValue>,
    ) -> Result<sqlx_core::encode::IsNull, sqlx_core::error::BoxDynError> {
        buf.push(MssqlArgumentValue::Date(*self));
        Ok(sqlx_core::encode::IsNull::No)
    }
}

impl sqlx_core::types::Type<crate::Mssql> for odbc_api::sys::Time {
    fn type_info() -> crate::MssqlTypeInfo {
        crate::MssqlTypeInfo::TIME
    }

    fn compatible(ty: &crate::MssqlTypeInfo) -> bool {
        matches!(ty.data_type(), odbc_api::DataType::Time { .. })
    }
}

impl<'q> sqlx_core::encode::Encode<'q, crate::Mssql> for odbc_api::sys::Time {
    fn encode_by_ref(
        &self,
        buf: &mut Vec<MssqlArgumentValue>,
    ) -> Result<sqlx_core::encode::IsNull, sqlx_core::error::BoxDynError> {
        buf.push(MssqlArgumentValue::Time(*self));
        Ok(sqlx_core::encode::IsNull::No)
    }
}

impl sqlx_core::types::Type<crate::Mssql> for odbc_api::sys::Timestamp {
    fn type_info() -> crate::MssqlTypeInfo {
        crate::MssqlTypeInfo::TIMESTAMP
    }

    fn compatible(ty: &crate::MssqlTypeInfo) -> bool {
        matches!(ty.data_type(), odbc_api::DataType::Timestamp { .. })
    }
}

impl<'q> sqlx_core::encode::Encode<'q, crate::Mssql> for odbc_api::sys::Timestamp {
    fn encode_by_ref(
        &self,
        buf: &mut Vec<MssqlArgumentValue>,
    ) -> Result<sqlx_core::encode::IsNull, sqlx_core::error::BoxDynError> {
        buf.push(MssqlArgumentValue::Timestamp(*self));
        Ok(sqlx_core::encode::IsNull::No)
    }
}

fn value_to_parameter(value: &MssqlArgumentValue) -> Box<dyn InputParameter> {
    match value {
        MssqlArgumentValue::Text(value) => Box::new(value.clone().into_parameter()),
        MssqlArgumentValue::Bytes(value) => Box::new(value.clone().into_parameter()),
        MssqlArgumentValue::Int(value) => Box::new(Some(*value).into_parameter()),
        MssqlArgumentValue::UInt(value) => Box::new(
            WithDataType::new(Nullable::new(*value), odbc_api::DataType::BigInt).into_parameter(),
        ),
        MssqlArgumentValue::Bit(value) => Box::new(odbc_api::Bit::from_bool(*value)),
        MssqlArgumentValue::Float(value) => Box::new(Some(*value).into_parameter()),
        MssqlArgumentValue::Date(value) => Box::new(Nullable::new(*value).into_parameter()),
        MssqlArgumentValue::Time(value) => Box::new(
            WithDataType::new(
                Nullable::new(*value),
                odbc_api::DataType::Time { precision: 0 },
            )
            .into_parameter(),
        ),
        MssqlArgumentValue::Timestamp(value) => Box::new(
            WithDataType::new(
                Nullable::new(*value),
                odbc_api::DataType::Timestamp { precision: 6 },
            )
            .into_parameter(),
        ),
        MssqlArgumentValue::Null(type_info) => null_parameter(type_info.data_type()),
    }
}

fn null_parameter(data_type: odbc_api::DataType) -> Box<dyn InputParameter> {
    match data_type {
        odbc_api::DataType::TinyInt => Box::new(Nullable::<i8>::null()),
        odbc_api::DataType::SmallInt => Box::new(Nullable::<i16>::null()),
        odbc_api::DataType::Integer => Box::new(Nullable::<i32>::null()),
        odbc_api::DataType::BigInt => Box::new(Nullable::<i64>::null()),
        odbc_api::DataType::Bit => Box::new(Nullable::<odbc_api::Bit>::null()),
        odbc_api::DataType::Real => Box::new(Nullable::<f32>::null()),
        odbc_api::DataType::Double => Box::new(Nullable::<f64>::null()),
        odbc_api::DataType::Float { .. } => {
            Box::new(WithDataType::new(Nullable::<f64>::null(), data_type))
        }
        odbc_api::DataType::Date => Box::new(Nullable::<odbc_api::sys::Date>::null()),
        odbc_api::DataType::Time { .. } => Box::new(WithDataType::new(
            Nullable::<odbc_api::sys::Time>::null(),
            data_type,
        )),
        odbc_api::DataType::Timestamp { .. } => Box::new(WithDataType::new(
            Nullable::<odbc_api::sys::Timestamp>::null(),
            data_type,
        )),
        odbc_api::DataType::Varbinary { .. }
        | odbc_api::DataType::LongVarbinary { .. }
        | odbc_api::DataType::Binary { .. } => {
            Box::new(WithDataType::new(VarBinaryBox::null(), data_type))
        }
        odbc_api::DataType::WVarchar { .. }
        | odbc_api::DataType::WLongVarchar { .. }
        | odbc_api::DataType::WChar { .. } => {
            Box::new(WithDataType::new(VarWCharBox::null(), data_type))
        }
        odbc_api::DataType::Char { .. }
        | odbc_api::DataType::Varchar { .. }
        | odbc_api::DataType::LongVarchar { .. }
        | odbc_api::DataType::Numeric { .. }
        | odbc_api::DataType::Decimal { .. }
        | odbc_api::DataType::Unknown
        | odbc_api::DataType::Other { .. } => {
            Box::new(WithDataType::new(VarCharBox::null(), data_type))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use odbc_api::{
        handles::{CData, HasDataType},
        ParameterCollectionRef,
    };

    #[test]
    fn argument_buffer_tracks_values_in_order() {
        let mut arguments = MssqlArguments::default();

        arguments.add_value(MssqlArgumentValue::Int(7));
        arguments.add_value(MssqlArgumentValue::Text("abc".to_owned()));
        arguments.add_value(MssqlArgumentValue::Null(crate::MssqlTypeInfo::new(
            odbc_api::DataType::Integer,
        )));

        assert_eq!(arguments.len(), 3);
        assert_eq!(
            arguments.values(),
            &[
                MssqlArgumentValue::Int(7),
                MssqlArgumentValue::Text("abc".to_owned()),
                MssqlArgumentValue::Null(crate::MssqlTypeInfo::new(odbc_api::DataType::Integer))
            ]
        );
    }

    #[test]
    fn sqlx_arguments_add_encodes_basic_scalars() {
        let mut arguments = MssqlArguments::default();

        sqlx_core::arguments::Arguments::add(&mut arguments, 7_i32).unwrap();
        sqlx_core::arguments::Arguments::add(&mut arguments, "abc").unwrap();
        sqlx_core::arguments::Arguments::add(&mut arguments, vec![1_u8, 2, 3]).unwrap();

        assert_eq!(
            arguments.values(),
            &[
                MssqlArgumentValue::Int(7),
                MssqlArgumentValue::Text("abc".to_owned()),
                MssqlArgumentValue::Bytes(vec![1, 2, 3])
            ]
        );
    }

    #[test]
    fn sqlx_arguments_add_encodes_large_text_and_binary_slices() {
        let mut arguments = MssqlArguments::default();
        let text = "abc123".repeat(16 * 1024);
        let bytes = [0_u8, 1, 2, 127, 128, 254, 255];

        sqlx_core::arguments::Arguments::add(&mut arguments, text.as_str()).unwrap();
        sqlx_core::arguments::Arguments::add(&mut arguments, &bytes[..]).unwrap();

        assert_eq!(
            arguments.values(),
            &[
                MssqlArgumentValue::Text(text),
                MssqlArgumentValue::Bytes(bytes.to_vec())
            ]
        );
    }

    #[test]
    fn byte_types_are_compatible_with_text_and_binary_columns() {
        use sqlx_core::types::Type;

        let binary = crate::MssqlTypeInfo::new(odbc_api::DataType::Varbinary { length: None });
        let text = crate::MssqlTypeInfo::new(odbc_api::DataType::WVarchar { length: None });
        let integer = crate::MssqlTypeInfo::new(odbc_api::DataType::Integer);

        assert!(<[u8] as Type<crate::Mssql>>::compatible(&binary));
        assert!(<[u8] as Type<crate::Mssql>>::compatible(&text));
        assert!(!<[u8] as Type<crate::Mssql>>::compatible(&integer));
        assert!(<Vec<u8> as Type<crate::Mssql>>::compatible(&binary));
        assert!(<Vec<u8> as Type<crate::Mssql>>::compatible(&text));
        assert!(!<Vec<u8> as Type<crate::Mssql>>::compatible(&integer));
    }

    #[test]
    fn sqlx_arguments_add_preserves_large_unsigned_values() {
        let mut arguments = MssqlArguments::default();

        sqlx_core::arguments::Arguments::add(&mut arguments, u64::MAX).unwrap();

        assert_eq!(arguments.values(), &[MssqlArgumentValue::UInt(u64::MAX)]);
    }

    #[test]
    fn sqlx_arguments_add_encodes_temporal_scalars() {
        let mut arguments = MssqlArguments::default();
        let date = odbc_api::sys::Date {
            year: 2026,
            month: 5,
            day: 29,
        };
        let time = odbc_api::sys::Time {
            hour: 12,
            minute: 30,
            second: 45,
        };
        let timestamp = odbc_api::sys::Timestamp {
            year: 2026,
            month: 5,
            day: 29,
            hour: 12,
            minute: 30,
            second: 45,
            fraction: 123_456_000,
        };

        sqlx_core::arguments::Arguments::add(&mut arguments, date).unwrap();
        sqlx_core::arguments::Arguments::add(&mut arguments, time).unwrap();
        sqlx_core::arguments::Arguments::add(&mut arguments, timestamp).unwrap();

        assert_eq!(
            arguments.values(),
            &[
                MssqlArgumentValue::Date(date),
                MssqlArgumentValue::Time(time),
                MssqlArgumentValue::Timestamp(timestamp)
            ]
        );
    }

    #[test]
    fn sqlx_arguments_add_encodes_typed_null_option() {
        let mut arguments = MssqlArguments::default();

        sqlx_core::arguments::Arguments::add(&mut arguments, Option::<i32>::None).unwrap();

        assert_eq!(
            arguments.values(),
            &[MssqlArgumentValue::Null(crate::MssqlTypeInfo::new(
                odbc_api::DataType::Integer
            ))]
        );

        let collection = arguments.to_odbc_parameter_collection();
        assert_eq!(
            collection.as_slice()[0].data_type(),
            odbc_api::DataType::Integer
        );
    }

    #[test]
    fn sqlx_arguments_reserve_and_len_work() {
        let mut arguments = MssqlArguments::default();

        sqlx_core::arguments::Arguments::reserve(&mut arguments, 2, 16);
        sqlx_core::arguments::Arguments::add(&mut arguments, true).unwrap();
        sqlx_core::arguments::Arguments::add(&mut arguments, 1.5_f64).unwrap();

        assert_eq!(sqlx_core::arguments::Arguments::len(&arguments), 2);
        assert_eq!(
            arguments.values(),
            &[MssqlArgumentValue::Bit(true), MssqlArgumentValue::Float(1.5)]
        );
    }

    #[test]
    fn parameter_collection_converts_basic_values_to_odbc_parameters() {
        let values = [
            MssqlArgumentValue::Text("abc".to_owned()),
            MssqlArgumentValue::Bytes(vec![1, 2, 3]),
            MssqlArgumentValue::Int(7),
            MssqlArgumentValue::UInt(8),
            MssqlArgumentValue::Bit(true),
            MssqlArgumentValue::Float(1.5),
        ];

        let collection = MssqlParameterCollection::from_values(&values);

        assert_eq!(collection.len(), values.len());
        assert!(matches!(
            collection.as_slice()[0].data_type(),
            odbc_api::DataType::Varchar { .. }
                | odbc_api::DataType::WVarchar { .. }
                | odbc_api::DataType::WLongVarchar { .. }
        ));
        assert!(matches!(
            collection.as_slice()[1].data_type(),
            odbc_api::DataType::Varbinary { .. }
        ));
        assert_eq!(
            collection.as_slice()[2].data_type(),
            odbc_api::DataType::BigInt
        );
        assert_eq!(
            collection.as_slice()[3].data_type(),
            odbc_api::DataType::BigInt
        );
        assert_eq!(
            collection.as_slice()[4].data_type(),
            odbc_api::DataType::Bit
        );
        assert_eq!(
            collection.as_slice()[5].data_type(),
            odbc_api::DataType::Double
        );
    }

    #[test]
    fn parameter_collection_converts_temporal_values_to_typed_odbc_parameters() {
        let values = [
            MssqlArgumentValue::Date(odbc_api::sys::Date {
                year: 2026,
                month: 5,
                day: 29,
            }),
            MssqlArgumentValue::Time(odbc_api::sys::Time {
                hour: 12,
                minute: 30,
                second: 45,
            }),
            MssqlArgumentValue::Timestamp(odbc_api::sys::Timestamp {
                year: 2026,
                month: 5,
                day: 29,
                hour: 12,
                minute: 30,
                second: 45,
                fraction: 123_456_789,
            }),
        ];

        let collection = MssqlParameterCollection::from_values(&values);

        assert_eq!(
            collection.as_slice()[0].data_type(),
            odbc_api::DataType::Date
        );
        assert_eq!(
            collection.as_slice()[1].data_type(),
            odbc_api::DataType::Time { precision: 0 }
        );
        assert_eq!(
            collection.as_slice()[2].data_type(),
            odbc_api::DataType::Timestamp { precision: 6 }
        );
    }

    #[test]
    fn parameter_collection_converts_typed_nulls_to_requested_data_types() {
        let values = [
            MssqlArgumentValue::Null(crate::MssqlTypeInfo::new(odbc_api::DataType::Integer)),
            MssqlArgumentValue::Null(crate::MssqlTypeInfo::new(odbc_api::DataType::WVarchar {
                length: None,
            })),
            MssqlArgumentValue::Null(crate::MssqlTypeInfo::new(odbc_api::DataType::Decimal {
                precision: 10,
                scale: 2,
            })),
        ];

        let collection = MssqlParameterCollection::from_values(&values);

        assert_eq!(
            collection.as_slice()[0].data_type(),
            odbc_api::DataType::Integer
        );
        assert_eq!(
            collection.as_slice()[1].data_type(),
            odbc_api::DataType::WVarchar { length: None }
        );
        assert_eq!(
            collection.as_slice()[2].data_type(),
            odbc_api::DataType::Decimal {
                precision: 10,
                scale: 2
            }
        );
    }

    #[test]
    fn parameter_collection_slice_matches_odbc_api_binding_shape() {
        fn assert_parameter_collection_ref<T: ParameterCollectionRef>(_parameters: T) {}

        let mut arguments = MssqlArguments::default();
        sqlx_core::arguments::Arguments::add(&mut arguments, "abc").unwrap();
        let collection = arguments.to_odbc_parameter_collection();

        assert_parameter_collection_ref(collection.as_slice());
    }

    #[test]
    fn fixed_sized_parameter_uses_explicit_non_null_indicator() {
        let mut arguments = MssqlArguments::default();

        sqlx_core::arguments::Arguments::add(&mut arguments, 5_i32).unwrap();

        let collection = arguments.to_odbc_parameter_collection();
        assert_eq!(collection.len(), 1);
        assert!(!collection.as_slice()[0].indicator_ptr().is_null());
    }
}
