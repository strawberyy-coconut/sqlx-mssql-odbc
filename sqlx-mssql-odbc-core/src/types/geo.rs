use crate::value::MssqlValueRef;
use crate::{DataTypeExt, Mssql, MssqlArgumentValue, MssqlTypeInfo};
use geo_traits::to_geo::ToGeoGeometry;
use geo_types::Geometry;
use sqlx_core::decode::Decode;
use sqlx_core::encode::{Encode, IsNull};
use sqlx_core::error::BoxDynError;
use sqlx_core::types::Type;
use wkb::writer::{write_geometry, WriteOptions};

impl Type<Mssql> for Geometry<f64> {
    fn type_info() -> MssqlTypeInfo {
        MssqlTypeInfo::geometry()
    }

    fn compatible(ty: &MssqlTypeInfo) -> bool {
        matches!(
            ty.data_type(),
            odbc_api::DataType::Other { data_type, .. } if data_type.0 == -151
        ) || ty.data_type().accepts_binary_data()
    }
}

impl<'q> Encode<'q, Mssql> for Geometry<f64> {
    fn encode_by_ref(&self, buf: &mut Vec<MssqlArgumentValue>) -> Result<IsNull, BoxDynError> {
        let mut wkb_bytes = Vec::new();
        write_geometry(&mut wkb_bytes, self, &WriteOptions::default())
            .map_err(|e| format!("ODBC: failed to encode Geometry to WKB: {e:?}"))?;
        buf.push(MssqlArgumentValue::Bytes(wkb_bytes));
        Ok(IsNull::No)
    }
}

impl<'r> Decode<'r, Mssql> for Geometry<f64> {
    fn decode(value: MssqlValueRef<'r>) -> Result<Self, BoxDynError> {
        let bytes = value
            .as_bytes()
            .ok_or_else(|| "ODBC: cannot decode Geometry: source value is not binary".to_owned())?;

        let wkb_geom = wkb::reader::read_wkb(bytes)
            .map_err(|e| format!("ODBC: failed to decode WKB to Geometry: {e:?}"))?;
        Ok(wkb_geom.to_geometry())
    }
}
