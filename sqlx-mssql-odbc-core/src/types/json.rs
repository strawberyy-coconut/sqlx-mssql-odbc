use crate::value::MssqlValueRef;
use crate::{DataTypeExt, Mssql, MssqlArgumentValue, MssqlTypeInfo};
use serde::de::DeserializeOwned;
use serde::Serialize;
use sqlx_core::decode::Decode;
use sqlx_core::encode::{Encode, IsNull};
use sqlx_core::error::BoxDynError;
use sqlx_core::types::{Json, Type};

impl<T: ?Sized> Type<Mssql> for Json<T> {
    fn type_info() -> MssqlTypeInfo {
        MssqlTypeInfo::varchar(None)
    }

    fn compatible(ty: &MssqlTypeInfo) -> bool {
        ty.data_type().accepts_character_data()
    }
}

impl<'q, T> Encode<'q, Mssql> for Json<T>
where
    T: Serialize + ?Sized,
{
    fn encode_by_ref(&self, buf: &mut Vec<MssqlArgumentValue>) -> Result<IsNull, BoxDynError> {
        buf.push(MssqlArgumentValue::Text(serde_json::to_string(self)?));
        Ok(IsNull::No)
    }
}

impl<'r, T> Decode<'r, Mssql> for Json<T>
where
    T: DeserializeOwned,
{
    fn decode(value: MssqlValueRef<'r>) -> Result<Self, BoxDynError> {
        if let Some(text) = value.as_str() {
            return serde_json::from_str(text.trim()).map_err(Into::into);
        }

        if let Some(bytes) = value.as_bytes() {
            return serde_json::from_slice(bytes).map_err(Into::into);
        }

        if let Some(integer) = value.as_i64() {
            return serde_json::from_value(serde_json::Value::from(integer)).map_err(Into::into);
        }

        if let Some(float) = value.as_f64() {
            return serde_json::from_value(serde_json::Value::from(float)).map_err(Into::into);
        }

        Err("ODBC: cannot decode JSON".into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{MssqlValue, MssqlValueKind};
    use serde_json::{json, Value as JsonValue};
    use sqlx_core::type_info::TypeInfo;
    use sqlx_core::value::Value;

    #[test]
    fn json_type_compatibility_matches_old_odbc() {
        assert!(<Json<JsonValue> as Type<Mssql>>::compatible(
            &MssqlTypeInfo::varchar(None)
        ));
        assert!(<Json<JsonValue> as Type<Mssql>>::compatible(
            &MssqlTypeInfo::char(None)
        ));
        assert!(<Json<JsonValue> as Type<Mssql>>::compatible(
            &MssqlTypeInfo::INTEGER
        ));
        assert!(<Json<JsonValue> as Type<Mssql>>::compatible(
            &MssqlTypeInfo::varbinary(None)
        ));
        assert_eq!(
            <Json<JsonValue> as Type<Mssql>>::type_info().name(),
            "VARCHAR"
        );
    }

    #[test]
    fn json_value_decodes_old_odbc_forms() -> Result<(), BoxDynError> {
        for (value, expected) in [
            (
                MssqlValue::new(MssqlValueKind::Text(
                    r#"{"name":"test","value":42}"#.to_owned(),
                )),
                json!({"name": "test", "value": 42}),
            ),
            (
                MssqlValue::new(MssqlValueKind::Binary(br#""hello""#.to_vec())),
                json!("hello"),
            ),
            (MssqlValue::new(MssqlValueKind::BigInt(42)), json!(42)),
            (MssqlValue::new(MssqlValueKind::Double(3.5)), json!(3.5)),
        ] {
            assert_eq!(
                <JsonValue as Decode<Mssql>>::decode(value.as_ref())?,
                expected
            );
        }

        // Invalid JSON is rejected
        let value = MssqlValue::new(MssqlValueKind::Text(r#"{"invalid": json,}"#.to_owned()));
        assert!(<JsonValue as Decode<Mssql>>::decode(value.as_ref()).is_err());

        Ok(())
    }

    #[test]
    fn json_encodes_as_text() -> Result<(), BoxDynError> {
        let mut buf = Vec::new();
        let value = json!({"name": "test"});

        let result = <JsonValue as Encode<Mssql>>::encode(value, &mut buf)?;

        assert!(matches!(result, IsNull::No));
        let [MssqlArgumentValue::Text(text)] = &buf[..] else {
            panic!("expected one text argument");
        };
        assert_eq!(
            serde_json::from_str::<JsonValue>(text)?,
            json!({"name": "test"})
        );
        Ok(())
    }

}
