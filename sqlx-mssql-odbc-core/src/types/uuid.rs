use crate::value::MssqlValueRef;
use crate::{DataTypeExt, Mssql, MssqlArgumentValue, MssqlTypeInfo};
use sqlx_core::decode::Decode;
use sqlx_core::encode::{Encode, IsNull};
use sqlx_core::error::BoxDynError;
use sqlx_core::types::Type;
use uuid::Uuid;

impl Type<Mssql> for Uuid {
    fn type_info() -> MssqlTypeInfo {
        MssqlTypeInfo::new(odbc_api::DataType::Other {
            data_type: odbc_api::sys::SqlDataType(-11),
            column_size: None,
            decimal_digits: 0,
        })
    }

    fn compatible(ty: &MssqlTypeInfo) -> bool {
        // UNIQUEIDENTIFIER (SQL_GUID, type code -11)
        matches!(ty.data_type(), odbc_api::DataType::Other { data_type, .. } if data_type.0 == -11)
    }
}

impl<'q> Encode<'q, Mssql> for Uuid {
    fn encode_by_ref(&self, buf: &mut Vec<MssqlArgumentValue>) -> Result<IsNull, BoxDynError> {
        buf.push(MssqlArgumentValue::Text(self.to_string()));
        Ok(IsNull::No)
    }
}

impl<'r> Decode<'r, Mssql> for Uuid {
    fn decode(value: MssqlValueRef<'r>) -> Result<Self, BoxDynError> {
        if let Some(bytes) = value.as_bytes() {
            if bytes.len() == 16 {
                return Ok(Uuid::from_slice(bytes)?);
            }

            if let Ok(text) = std::str::from_utf8(bytes) {
                if let Ok(uuid) = Uuid::parse_str(text.trim()) {
                    return Ok(uuid);
                }
            }
        }

        if let Some(text) = value.as_str() {
            if let Ok(uuid) = Uuid::parse_str(text.trim()) {
                return Ok(uuid);
            }
        }

        Err("ODBC: cannot decode Uuid".into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{MssqlValue, MssqlValueKind};
    use sqlx_core::value::Value;

    #[test]
    fn uuid_type_compatibility_matches_old_odbc() {
        assert!(<Uuid as Type<Mssql>>::compatible(&MssqlTypeInfo::varchar(
            None
        )));
        assert!(<Uuid as Type<Mssql>>::compatible(&MssqlTypeInfo::varbinary(
            None
        )));
        assert!(!<Uuid as Type<Mssql>>::compatible(&MssqlTypeInfo::INTEGER));
    }

    #[test]
    fn uuid_compatible_with_guid_type() {
        assert!(<Uuid as Type<Mssql>>::compatible(&MssqlTypeInfo::guid()));
    }

    #[test]
    fn uuid_decodes_text_and_binary_forms() -> Result<(), BoxDynError> {
        let uuid = Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000")?;

        for value in [
            MssqlValue::new(MssqlValueKind::Text(format!(" {uuid} "))),
            MssqlValue::new(MssqlValueKind::Binary(uuid.as_bytes().to_vec())),
            MssqlValue::new(MssqlValueKind::Binary(uuid.to_string().into_bytes())),
        ] {
            assert_eq!(<Uuid as Decode<Mssql>>::decode(value.as_ref())?, uuid);
        }

        Ok(())
    }

    #[test]
    fn uuid_decodes_from_guid_value() -> Result<(), BoxDynError> {
        let uuid = Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000")?;
        let guid_value = MssqlValue::new(MssqlValueKind::Guid(*uuid.as_bytes()));
        assert_eq!(<Uuid as Decode<Mssql>>::decode(guid_value.as_ref())?, uuid);
        Ok(())
    }

    #[test]
    fn uuid_encodes_as_text() -> Result<(), BoxDynError> {
        let mut buf = Vec::new();
        let uuid = Uuid::nil();

        let result = <Uuid as Encode<Mssql>>::encode(uuid, &mut buf)?;

        assert!(matches!(result, IsNull::No));
        assert_eq!(
            buf,
            vec![MssqlArgumentValue::Text(
                "00000000-0000-0000-0000-000000000000".to_owned()
            )]
        );
        Ok(())
    }
}
