use std::borrow::Cow;

/// A small owned MSSQL ODBC value representation.
#[derive(Debug, Clone, PartialEq)]
pub struct MssqlValue {
    kind: MssqlValueKind,
}

impl MssqlValue {
    /// Creates a new value from a raw kind.
    pub fn new(kind: MssqlValueKind) -> Self {
        Self { kind }
    }

    /// Returns the raw value kind.
    pub fn kind(&self) -> &MssqlValueKind {
        &self.kind
    }

    /// Returns whether this value is NULL.
    pub fn is_null(&self) -> bool {
        matches!(self.kind, MssqlValueKind::Null)
    }

    /// Returns this value as a signed integer where possible.
    pub fn as_i64(&self) -> Option<i64> {
        match &self.kind {
            MssqlValueKind::TinyInt(value) => Some(i64::from(*value)),
            MssqlValueKind::SmallInt(value) => Some(i64::from(*value)),
            MssqlValueKind::Integer(value) => Some(i64::from(*value)),
            MssqlValueKind::BigInt(value) => Some(*value),
            MssqlValueKind::Text(value) => parse_integer_text(value),
            _ => None,
        }
    }

    /// Returns this value as `f64` where possible.
    pub fn as_f64(&self) -> Option<f64> {
        match &self.kind {
            MssqlValueKind::Real(value) => Some(f64::from(*value)),
            MssqlValueKind::Double(value) => Some(*value),
            MssqlValueKind::TinyInt(value) => Some(f64::from(*value)),
            MssqlValueKind::SmallInt(value) => Some(f64::from(*value)),
            MssqlValueKind::Integer(value) => Some(f64::from(*value)),
            MssqlValueKind::BigInt(value) => Some(*value as f64),
            MssqlValueKind::Text(value) => value.trim().parse().ok(),
            _ => None,
        }
    }

    /// Returns this value as text where possible.
    pub fn as_str(&self) -> Option<Cow<'_, str>> {
        match &self.kind {
            MssqlValueKind::Text(value) => Some(Cow::Borrowed(value)),
            MssqlValueKind::Guid(bytes) => {
                let guid_str = uuid_guid_to_string(bytes);
                Some(Cow::Owned(guid_str))
            }
            _ => None,
        }
    }

    /// Returns this value as bytes where possible.
    pub fn as_bytes(&self) -> Option<Cow<'_, [u8]>> {
        match &self.kind {
            MssqlValueKind::Binary(value) => Some(Cow::Borrowed(value)),
            MssqlValueKind::Text(value) => Some(Cow::Borrowed(value.as_bytes())),
            MssqlValueKind::Guid(bytes) => Some(Cow::Borrowed(bytes)),
            _ => None,
        }
    }
}

impl sqlx_core::value::Value for MssqlValue {
    type Database = crate::Mssql;

    fn as_ref(&self) -> <Self::Database as sqlx_core::database::Database>::ValueRef<'_> {
        MssqlValueRef { value: self }
    }

    fn type_info(&self) -> Cow<'_, crate::MssqlTypeInfo> {
        Cow::Owned(self.kind.type_info())
    }

    fn is_null(&self) -> bool {
        self.is_null()
    }
}

/// Borrowed MSSQL ODBC value reference.
#[derive(Debug, Clone, Copy)]
pub struct MssqlValueRef<'r> {
    value: &'r MssqlValue,
}

impl<'r> MssqlValueRef<'r> {
    /// Returns this value as a signed integer where possible.
    pub fn as_i64(&self) -> Option<i64> {
        self.value.as_i64()
    }

    /// Returns this value as `f64` where possible.
    pub fn as_f64(&self) -> Option<f64> {
        self.value.as_f64()
    }

    /// Returns this value as borrowed text where possible.
    pub fn as_str(&self) -> Option<&'r str> {
        match &self.value.kind {
            MssqlValueKind::Text(value) => Some(value),
            MssqlValueKind::Guid(bytes) => {
                // We cannot return a borrowed &str for Guid since we'd need to allocate.
                // Fall through to None; the caller should use as_bytes() or to_owned().
                let _ = bytes;
                None
            }
            _ => None,
        }
    }

    /// Returns this value as borrowed bytes where possible.
    pub fn as_bytes(&self) -> Option<&'r [u8]> {
        match &self.value.kind {
            MssqlValueKind::Binary(value) => Some(value),
            MssqlValueKind::Text(value) => Some(value.as_bytes()),
            MssqlValueKind::Guid(bytes) => Some(bytes),
            _ => None,
        }
    }

    /// Returns this value as a boolean where possible.
    pub fn as_bool(&self) -> Option<bool> {
        match &self.value.kind {
            MssqlValueKind::Bit(value) => Some(*value),
            MssqlValueKind::TinyInt(value) => Some(*value != 0),
            MssqlValueKind::SmallInt(value) => Some(*value != 0),
            MssqlValueKind::Integer(value) => Some(*value != 0),
            MssqlValueKind::BigInt(value) => Some(*value != 0),
            MssqlValueKind::Real(value) => Some(*value != 0.0),
            MssqlValueKind::Double(value) => Some(*value != 0.0),
            MssqlValueKind::Text(value) => parse_bool_text(value),
            _ => None,
        }
    }
}

impl<'r> sqlx_core::value::ValueRef<'r> for MssqlValueRef<'r> {
    type Database = crate::Mssql;

    fn to_owned(&self) -> MssqlValue {
        self.value.clone()
    }

    fn type_info(&self) -> Cow<'_, crate::MssqlTypeInfo> {
        Cow::Owned(self.value.kind.type_info())
    }

    fn is_null(&self) -> bool {
        self.value.is_null()
    }
}

macro_rules! impl_decode_integer {
    ($ty:ty) => {
        impl<'r> sqlx_core::decode::Decode<'r, crate::Mssql> for $ty {
            fn decode(value: MssqlValueRef<'r>) -> Result<Self, sqlx_core::error::BoxDynError> {
                let Some(integer) = value.as_i64() else {
                    return Err(decode_error(
                        value,
                        stringify!($ty),
                        "source value is not an integer",
                    )
                    .into());
                };

                Self::try_from(integer).map_err(|_| {
                    decode_error(
                        value,
                        stringify!($ty),
                        format!("integer value {integer} is outside the target range"),
                    )
                    .into()
                })
            }
        }
    };
}

impl_decode_integer!(i8);
impl_decode_integer!(i16);
impl_decode_integer!(i32);
impl_decode_integer!(i64);
impl_decode_integer!(u8);
impl_decode_integer!(u16);
impl_decode_integer!(u32);
impl_decode_integer!(u64);

impl<'r> sqlx_core::decode::Decode<'r, crate::Mssql> for bool {
    fn decode(value: MssqlValueRef<'r>) -> Result<Self, sqlx_core::error::BoxDynError> {
        value.as_bool().ok_or_else(|| {
            decode_error(value, "bool", "source value is not boolean-compatible").into()
        })
    }
}

impl<'r> sqlx_core::decode::Decode<'r, crate::Mssql> for f32 {
    fn decode(value: MssqlValueRef<'r>) -> Result<Self, sqlx_core::error::BoxDynError> {
        value
            .as_f64()
            .map(|value| value as f32)
            .ok_or_else(|| decode_error(value, "f32", "source value is not numeric").into())
    }
}

impl<'r> sqlx_core::decode::Decode<'r, crate::Mssql> for f64 {
    fn decode(value: MssqlValueRef<'r>) -> Result<Self, sqlx_core::error::BoxDynError> {
        value
            .as_f64()
            .ok_or_else(|| decode_error(value, "f64", "source value is not numeric").into())
    }
}

impl<'r> sqlx_core::decode::Decode<'r, crate::Mssql> for String {
    fn decode(value: MssqlValueRef<'r>) -> Result<Self, sqlx_core::error::BoxDynError> {
        if let Some(text) = value.as_str() {
            return Ok(text.to_owned());
        }

        if let Some(bytes) = value.as_bytes() {
            return Ok(String::from_utf8(bytes.to_vec())?);
        }

        Err(decode_error(
            value,
            "String",
            "source value is neither text nor UTF-8 bytes",
        )
        .into())
    }
}

impl<'r> sqlx_core::decode::Decode<'r, crate::Mssql> for &'r str {
    fn decode(value: MssqlValueRef<'r>) -> Result<Self, sqlx_core::error::BoxDynError> {
        if let Some(text) = value.as_str() {
            return Ok(text);
        }

        Err(decode_error(value, "&str", "source value is not text").into())
    }
}

impl<'r> sqlx_core::decode::Decode<'r, crate::Mssql> for Vec<u8> {
    fn decode(value: MssqlValueRef<'r>) -> Result<Self, sqlx_core::error::BoxDynError> {
        value
            .as_bytes()
            .map(<[u8]>::to_vec)
            .ok_or_else(|| decode_error(value, "Vec<u8>", "source value is not binary").into())
    }
}

impl<'r> sqlx_core::decode::Decode<'r, crate::Mssql> for &'r [u8] {
    fn decode(value: MssqlValueRef<'r>) -> Result<Self, sqlx_core::error::BoxDynError> {
        value
            .as_bytes()
            .ok_or_else(|| decode_error(value, "&[u8]", "source value is not binary").into())
    }
}

impl<'r> sqlx_core::decode::Decode<'r, crate::Mssql> for odbc_api::sys::Date {
    fn decode(value: MssqlValueRef<'r>) -> Result<Self, sqlx_core::error::BoxDynError> {
        match value.value.kind() {
            MssqlValueKind::Date(value) => Ok(*value),
            _ => Err(decode_error(value, "Date", "source value is not an ODBC date").into()),
        }
    }
}

impl<'r> sqlx_core::decode::Decode<'r, crate::Mssql> for odbc_api::sys::Time {
    fn decode(value: MssqlValueRef<'r>) -> Result<Self, sqlx_core::error::BoxDynError> {
        match value.value.kind() {
            MssqlValueKind::Time(value) => Ok(*value),
            _ => Err(decode_error(value, "Time", "source value is not an ODBC time").into()),
        }
    }
}

impl<'r> sqlx_core::decode::Decode<'r, crate::Mssql> for odbc_api::sys::Timestamp {
    fn decode(value: MssqlValueRef<'r>) -> Result<Self, sqlx_core::error::BoxDynError> {
        match value.value.kind() {
            MssqlValueKind::Timestamp(value) => Ok(*value),
            _ => Err(
                decode_error(value, "Timestamp", "source value is not an ODBC timestamp").into(),
            ),
        }
    }
}

fn decode_error(value: MssqlValueRef<'_>, target: &str, reason: impl std::fmt::Display) -> String {
    format!(
        "ODBC cannot decode value kind {:?} as {target}: {reason}",
        value.value.kind()
    )
}

fn parse_bool_text(value: &str) -> Option<bool> {
    match value.trim() {
        "0" | "0.0" | "false" | "FALSE" | "f" | "F" => Some(false),
        "1" | "1.0" | "true" | "TRUE" | "t" | "T" => Some(true),
        value => value
            .parse::<f64>()
            .map(|value| value != 0.0)
            .or_else(|_| value.parse::<i64>().map(|value| value != 0))
            .ok(),
    }
}

fn parse_integer_text(value: &str) -> Option<i64> {
    let value = value.trim();

    if let Ok(value) = value.parse() {
        return Some(value);
    }

    let (integer, fraction) = value.split_once('.')?;

    if fraction.chars().all(|ch| ch == '0') {
        integer.parse().ok()
    } else {
        None
    }
}

/// Converts a 16-byte GUID array to its standard hyphenated UUID string.
fn uuid_guid_to_string(bytes: &[u8; 16]) -> String {
    format!(
        "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
        bytes[3], bytes[2], bytes[1], bytes[0],
        bytes[5], bytes[4],
        bytes[7], bytes[6],
        bytes[8], bytes[9],
        bytes[10], bytes[11], bytes[12], bytes[13], bytes[14], bytes[15],
    )
}

/// Supported owned MSSQL ODBC value kinds.
#[derive(Debug, Clone, PartialEq)]
pub enum MssqlValueKind {
    /// NULL value.
    Null,
    /// 8-bit integer (MSSQL TINYINT is unsigned 0-255, read as i16 for safety).
    TinyInt(i16),
    /// 16-bit signed integer.
    SmallInt(i16),
    /// 32-bit signed integer.
    Integer(i32),
    /// 64-bit signed integer.
    BigInt(i64),
    /// 32-bit float.
    Real(f32),
    /// 64-bit float.
    Double(f64),
    /// Boolean value.
    Bit(bool),
    /// Text value.
    Text(String),
    /// Binary value.
    Binary(Vec<u8>),
    /// GUID / UNIQUEIDENTIFIER value (16 bytes).
    Guid([u8; 16]),
    /// Date value.
    Date(odbc_api::sys::Date),
    /// Time value.
    Time(odbc_api::sys::Time),
    /// Timestamp value.
    Timestamp(odbc_api::sys::Timestamp),
}

impl MssqlValueKind {
    fn type_info(&self) -> crate::MssqlTypeInfo {
        let data_type = match self {
            Self::Null => odbc_api::DataType::Unknown,
            Self::TinyInt(_) => odbc_api::DataType::TinyInt,
            Self::SmallInt(_) => odbc_api::DataType::SmallInt,
            Self::Integer(_) => odbc_api::DataType::Integer,
            Self::BigInt(_) => odbc_api::DataType::BigInt,
            Self::Real(_) => odbc_api::DataType::Real,
            Self::Double(_) => odbc_api::DataType::Double,
            Self::Bit(_) => odbc_api::DataType::Bit,
            Self::Text(_) => odbc_api::DataType::WVarchar { length: None },
            Self::Binary(_) => odbc_api::DataType::Varbinary { length: None },
            Self::Guid(_) => odbc_api::DataType::Other {
                data_type: odbc_api::sys::SqlDataType(-11),
                column_size: None,
                decimal_digits: 0,
            },
            Self::Date(_) => odbc_api::DataType::Date,
            Self::Time(_) => odbc_api::DataType::Time { precision: 0 },
            Self::Timestamp(_) => odbc_api::DataType::Timestamp { precision: 6 },
        };

        crate::MssqlTypeInfo::new(data_type)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn integer_values_convert_to_i64() {
        assert_eq!(MssqlValue::new(MssqlValueKind::TinyInt(1)).as_i64(), Some(1));
        assert_eq!(MssqlValue::new(MssqlValueKind::SmallInt(2)).as_i64(), Some(2));
        assert_eq!(MssqlValue::new(MssqlValueKind::Integer(3)).as_i64(), Some(3));
        assert_eq!(MssqlValue::new(MssqlValueKind::BigInt(4)).as_i64(), Some(4));
        assert_eq!(
            MssqlValue::new(MssqlValueKind::Text("42.000".to_owned())).as_i64(),
            Some(42)
        );
        assert_eq!(
            MssqlValue::new(MssqlValueKind::Text("42.5".to_owned())).as_i64(),
            None
        );
    }

    #[test]
    fn text_numeric_values_convert_to_float() {
        assert_eq!(
            MssqlValue::new(MssqlValueKind::Text("42.5".to_owned())).as_f64(),
            Some(42.5)
        );
    }

    #[test]
    fn text_and_bytes_borrow_from_value() {
        let text = MssqlValue::new(MssqlValueKind::Text("hello".to_owned()));
        assert_eq!(text.as_str().as_deref(), Some("hello"));
        assert_eq!(text.as_bytes().as_deref(), Some(b"hello".as_slice()));

        let bytes = MssqlValue::new(MssqlValueKind::Binary(vec![1, 2, 3]));
        assert_eq!(bytes.as_bytes().as_deref(), Some(&[1, 2, 3][..]));
    }

    #[test]
    fn null_reports_null() {
        assert!(MssqlValue::new(MssqlValueKind::Null).is_null());
    }

    #[test]
    fn borrowed_values_decode_basic_scalars() {
        use sqlx_core::decode::Decode;
        use sqlx_core::value::Value;

        let int = MssqlValue::new(MssqlValueKind::BigInt(42));
        assert_eq!(
            <i32 as Decode<crate::Mssql>>::decode(int.as_ref()).unwrap(),
            42
        );

        let truthy = MssqlValue::new(MssqlValueKind::Text("true".to_owned()));
        assert!(<bool as Decode<crate::Mssql>>::decode(truthy.as_ref()).unwrap());

        let text = MssqlValue::new(MssqlValueKind::Text("hello".to_owned()));
        assert_eq!(
            <String as Decode<crate::Mssql>>::decode(text.as_ref()).unwrap(),
            "hello"
        );

        let bytes = MssqlValue::new(MssqlValueKind::Binary(vec![1, 2, 3]));
        assert_eq!(
            <Vec<u8> as Decode<crate::Mssql>>::decode(bytes.as_ref()).unwrap(),
            vec![1, 2, 3]
        );

        let bytes_from_text = MssqlValue::new(MssqlValueKind::Text("abc".to_owned()));
        assert_eq!(
            <Vec<u8> as Decode<crate::Mssql>>::decode(bytes_from_text.as_ref()).unwrap(),
            b"abc".to_vec()
        );
        assert_eq!(
            <&[u8] as Decode<crate::Mssql>>::decode(bytes_from_text.as_ref()).unwrap(),
            b"abc".as_slice()
        );
    }

    #[test]
    fn borrowed_values_decode_bool_variants() {
        use sqlx_core::decode::Decode;
        use sqlx_core::value::Value;

        for value in [
            MssqlValueKind::Bit(true),
            MssqlValueKind::TinyInt(1),
            MssqlValueKind::SmallInt(-1),
            MssqlValueKind::Integer(42),
            MssqlValueKind::BigInt(1),
            MssqlValueKind::Real(1.0),
            MssqlValueKind::Double(42.5),
            MssqlValueKind::Text("true".to_owned()),
            MssqlValueKind::Text("TRUE".to_owned()),
            MssqlValueKind::Text("t".to_owned()),
            MssqlValueKind::Text("1".to_owned()),
            MssqlValueKind::Text("1.0".to_owned()),
            MssqlValueKind::Text(" 42 ".to_owned()),
        ] {
            let value = MssqlValue::new(value);
            assert!(<bool as Decode<crate::Mssql>>::decode(value.as_ref()).unwrap());
        }

        for value in [
            MssqlValueKind::Bit(false),
            MssqlValueKind::TinyInt(0),
            MssqlValueKind::SmallInt(0),
            MssqlValueKind::Integer(0),
            MssqlValueKind::BigInt(0),
            MssqlValueKind::Real(0.0),
            MssqlValueKind::Double(0.0),
            MssqlValueKind::Text("false".to_owned()),
            MssqlValueKind::Text("FALSE".to_owned()),
            MssqlValueKind::Text("f".to_owned()),
            MssqlValueKind::Text("0".to_owned()),
            MssqlValueKind::Text("0.0".to_owned()),
            MssqlValueKind::Text(" 0 ".to_owned()),
        ] {
            let value = MssqlValue::new(value);
            assert!(!<bool as Decode<crate::Mssql>>::decode(value.as_ref()).unwrap());
        }
    }

    #[test]
    fn borrowed_values_reject_invalid_bool_text() {
        use sqlx_core::decode::Decode;
        use sqlx_core::value::Value;

        let value = MssqlValue::new(MssqlValueKind::Text("not a bool".to_owned()));
        let error = <bool as Decode<crate::Mssql>>::decode(value.as_ref()).unwrap_err();

        assert!(error.to_string().contains("bool"));
        assert!(error.to_string().contains("not boolean-compatible"));
    }

    #[test]
    fn borrowed_values_decode_temporal_scalars() {
        use sqlx_core::decode::Decode;
        use sqlx_core::value::Value;

        let date = odbc_api::sys::Date {
            year: 2026,
            month: 5,
            day: 29,
        };
        let date_value = MssqlValue::new(MssqlValueKind::Date(date));
        assert_eq!(
            <odbc_api::sys::Date as Decode<crate::Mssql>>::decode(date_value.as_ref()).unwrap(),
            date
        );

        let time = odbc_api::sys::Time {
            hour: 12,
            minute: 30,
            second: 45,
        };
        let time_value = MssqlValue::new(MssqlValueKind::Time(time));
        assert_eq!(
            <odbc_api::sys::Time as Decode<crate::Mssql>>::decode(time_value.as_ref()).unwrap(),
            time
        );

        let timestamp = odbc_api::sys::Timestamp {
            year: 2026,
            month: 5,
            day: 29,
            hour: 12,
            minute: 30,
            second: 45,
            fraction: 123_456_000,
        };
        let timestamp_value = MssqlValue::new(MssqlValueKind::Timestamp(timestamp));
        assert_eq!(
            <odbc_api::sys::Timestamp as Decode<crate::Mssql>>::decode(timestamp_value.as_ref())
                .unwrap(),
            timestamp
        );
    }
}
