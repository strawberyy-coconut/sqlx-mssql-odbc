use crate::value::MssqlValueRef;
use crate::{DataTypeExt, Mssql, MssqlArgumentValue, MssqlTypeInfo};
use bigdecimal::BigDecimal;
use odbc_api::DataType;
use sqlx_core::decode::Decode;
use sqlx_core::encode::{Encode, IsNull};
use sqlx_core::error::BoxDynError;
use sqlx_core::types::Type;
use std::str::FromStr;

impl Type<Mssql> for BigDecimal {
    fn type_info() -> MssqlTypeInfo {
        MssqlTypeInfo::numeric(28, 4)
    }

    fn compatible(ty: &MssqlTypeInfo) -> bool {
        matches!(
            ty.data_type(),
            DataType::Numeric { .. }
                | DataType::Decimal { .. }
                | DataType::Double
                | DataType::Float { .. }
        ) || ty.data_type().accepts_character_data()
    }
}

impl<'q> Encode<'q, Mssql> for BigDecimal {
    fn encode_by_ref(&self, buf: &mut Vec<MssqlArgumentValue>) -> Result<IsNull, BoxDynError> {
        buf.push(MssqlArgumentValue::Text(self.to_string()));
        Ok(IsNull::No)
    }
}

impl<'r> Decode<'r, Mssql> for BigDecimal {
    fn decode(value: MssqlValueRef<'r>) -> Result<Self, BoxDynError> {
        if let Some(text) = value.as_str() {
            return BigDecimal::from_str(text.trim())
                .map_err(|error| format!("bad decimal text: {error}").into());
        }

        if let Some(bytes) = value.as_bytes() {
            let text = std::str::from_utf8(bytes)?;
            return BigDecimal::from_str(text.trim())
                .map_err(|error| format!("bad decimal bytes: {error}").into());
        }

        if let Some(integer) = value.as_i64() {
            return Ok(BigDecimal::from(integer));
        }

        if let Some(float) = value.as_f64() {
            return Ok(BigDecimal::try_from(float)?);
        }

        Err("ODBC: cannot decode BigDecimal".into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{MssqlValue, MssqlValueKind};
    use sqlx_core::value::Value;

    #[test]
    fn bigdecimal_type_compatibility_matches_old_odbc() {
        assert!(<BigDecimal as Type<Mssql>>::compatible(
            &MssqlTypeInfo::decimal(10, 2)
        ));
        assert!(<BigDecimal as Type<Mssql>>::compatible(
            &MssqlTypeInfo::numeric(15, 4)
        ));
        assert!(<BigDecimal as Type<Mssql>>::compatible(
            &MssqlTypeInfo::DOUBLE
        ));
        assert!(<BigDecimal as Type<Mssql>>::compatible(
            &MssqlTypeInfo::float(24)
        ));
        assert!(<BigDecimal as Type<Mssql>>::compatible(
            &MssqlTypeInfo::varchar(None)
        ));
        assert!(!<BigDecimal as Type<Mssql>>::compatible(
            &MssqlTypeInfo::varbinary(None)
        ));
    }

    #[test]
    fn bigdecimal_decodes_old_odbc_forms() -> Result<(), BoxDynError> {
        for (value, expected) in [
            (
                MssqlValue::new(MssqlValueKind::Text("123.456789".to_owned())),
                "123.456789",
            ),
            (
                MssqlValue::new(MssqlValueKind::Text("  987.654  ".to_owned())),
                "987.654",
            ),
            (
                MssqlValue::new(MssqlValueKind::Binary(b"-123.456".to_vec())),
                "-123.456",
            ),
            (MssqlValue::new(MssqlValueKind::BigInt(42)), "42"),
        ] {
            assert_eq!(
                <BigDecimal as Decode<Mssql>>::decode(value.as_ref())?,
                BigDecimal::from_str(expected)?
            );
        }

        Ok(())
    }

    #[test]
    fn bigdecimal_encodes_as_text() -> Result<(), BoxDynError> {
        let mut buf = Vec::new();
        let decimal = BigDecimal::from_str("123.456")?;

        let result = <BigDecimal as Encode<Mssql>>::encode(decimal, &mut buf)?;

        assert!(matches!(result, IsNull::No));
        assert_eq!(buf, vec![MssqlArgumentValue::Text("123.456".to_owned())]);
        Ok(())
    }
}
