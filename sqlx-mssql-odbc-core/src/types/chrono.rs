use crate::value::MssqlValueRef;
use crate::{DataTypeExt, Mssql, MssqlArgumentValue, MssqlTypeInfo, MssqlValueKind};
use ::chrono::{
    DateTime, Datelike, FixedOffset, Local, NaiveDate, NaiveDateTime, NaiveTime, Timelike, Utc,
};
use odbc_api::DataType;
use sqlx_core::decode::Decode;
use sqlx_core::encode::{Encode, IsNull};
use sqlx_core::error::BoxDynError;
use sqlx_core::types::Type;
use sqlx_core::value::ValueRef;

impl Type<Mssql> for NaiveDate {
    fn type_info() -> MssqlTypeInfo {
        MssqlTypeInfo::DATE
    }

    fn compatible(ty: &MssqlTypeInfo) -> bool {
        matches!(ty.data_type(), DataType::Date) 
         || ty.data_type().accepts_character_data()

    }
}

impl Type<Mssql> for NaiveTime {
    fn type_info() -> MssqlTypeInfo {
        MssqlTypeInfo::TIME
    }

    fn compatible(ty: &MssqlTypeInfo) -> bool {
        matches!(ty.data_type(), DataType::Time { .. })
         || ty.data_type().accepts_character_data()

    }
}

impl Type<Mssql> for NaiveDateTime {
    fn type_info() -> MssqlTypeInfo {
        MssqlTypeInfo::TIMESTAMP
    }

    fn compatible(ty: &MssqlTypeInfo) -> bool {
        matches!(ty.data_type(), DataType::Timestamp { .. })
        || ty.data_type().accepts_character_data()
    }
}

impl Type<Mssql> for DateTime<Utc> {
    fn type_info() -> MssqlTypeInfo {
        MssqlTypeInfo::datetimeoffset()
    }

    fn compatible(ty: &MssqlTypeInfo) -> bool {
        matches!(ty.data_type(), DataType::Timestamp { .. })
        || matches!(ty.data_type(), DataType::Other { data_type, .. } if data_type.0 == -155)
        || ty.data_type().accepts_character_data()
    }
}

impl Type<Mssql> for DateTime<FixedOffset> {
    fn type_info() -> MssqlTypeInfo {
        MssqlTypeInfo::datetimeoffset()
    }

    fn compatible(ty: &MssqlTypeInfo) -> bool {
        matches!(ty.data_type(), DataType::Timestamp { .. })
        || matches!(ty.data_type(), DataType::Other { data_type, .. } if data_type.0 == -155)
        || ty.data_type().accepts_character_data()
    }
}

impl Type<Mssql> for DateTime<Local> {
    fn type_info() -> MssqlTypeInfo {
        MssqlTypeInfo::datetimeoffset()
    }

    fn compatible(ty: &MssqlTypeInfo) -> bool {
        matches!(ty.data_type(), DataType::Timestamp { .. })
                  || matches!(ty.data_type(), DataType::Other { data_type, .. } if data_type.0 == -155)
        || ty.data_type().accepts_character_data()
    }
}

impl<'q> Encode<'q, Mssql> for NaiveDate {
    fn encode_by_ref(&self, buf: &mut Vec<MssqlArgumentValue>) -> Result<IsNull, BoxDynError> {
        buf.push(MssqlArgumentValue::Date(odbc_api::sys::Date {
            year: self.year() as i16,
            month: self.month() as u16,
            day: self.day() as u16,
        }));
        Ok(IsNull::No)
    }
}

impl<'q> Encode<'q, Mssql> for NaiveTime {
    fn encode_by_ref(&self, buf: &mut Vec<MssqlArgumentValue>) -> Result<IsNull, BoxDynError> {
        buf.push(MssqlArgumentValue::Time(odbc_api::sys::Time {
            hour: self.hour() as u16,
            minute: self.minute() as u16,
            second: self.second() as u16,
        }));
        Ok(IsNull::No)
    }
}

impl<'q> Encode<'q, Mssql> for NaiveDateTime {
    fn encode_by_ref(&self, buf: &mut Vec<MssqlArgumentValue>) -> Result<IsNull, BoxDynError> {
        buf.push(MssqlArgumentValue::Timestamp(timestamp_from_naive(*self)));
        Ok(IsNull::No)
    }
}

impl<'q> Encode<'q, Mssql> for DateTime<Utc> {
    fn encode_by_ref(&self, buf: &mut Vec<MssqlArgumentValue>) -> Result<IsNull, BoxDynError> {
        buf.push(MssqlArgumentValue::Text(self.to_rfc3339()));
        Ok(IsNull::No)
    }
}

impl<'q> Encode<'q, Mssql> for DateTime<FixedOffset> {
    fn encode_by_ref(&self, buf: &mut Vec<MssqlArgumentValue>) -> Result<IsNull, BoxDynError> {
        buf.push(MssqlArgumentValue::Text(self.to_rfc3339()));
        Ok(IsNull::No)
    }
}

impl<'q> Encode<'q, Mssql> for DateTime<Local> {
    fn encode_by_ref(&self, buf: &mut Vec<MssqlArgumentValue>) -> Result<IsNull, BoxDynError> {
        buf.push(MssqlArgumentValue::Timestamp(timestamp_from_naive(
            self.naive_local(),
        )));
        Ok(IsNull::No)
    }
}

fn timestamp_from_naive(value: NaiveDateTime) -> odbc_api::sys::Timestamp {
    odbc_api::sys::Timestamp {
        year: value.year() as i16,
        month: value.month() as u16,
        day: value.day() as u16,
        hour: value.hour() as u16,
        minute: value.minute() as u16,
        second: value.second() as u16,
        fraction: (value.nanosecond() / 1000) * 1000,
    }
}

fn parse_yyyymmdd_as_naive_date(value: i64) -> Option<NaiveDate> {
    if !(19000101..=30001231).contains(&value) {
        return None;
    }

    let year = (value / 10000) as i32;
    let month = ((value % 10000) / 100) as u32;
    let day = (value % 100) as u32;
    NaiveDate::from_ymd_opt(year, month, day)
}

fn parse_yyyymmdd_text_as_naive_date(value: &str) -> Option<NaiveDate> {
    if value.len() != 8 || !value.chars().all(|ch| ch.is_ascii_digit()) {
        return None;
    }

    let year = value[0..4].parse().ok()?;
    let month = value[4..6].parse().ok()?;
    let day = value[6..8].parse().ok()?;
    NaiveDate::from_ymd_opt(year, month, day)
}

fn raw_date(value: MssqlValueRef<'_>) -> Option<odbc_api::sys::Date> {
    match ValueRef::to_owned(&value).kind() {
        MssqlValueKind::Date(date) => Some(*date),
        _ => None,
    }
}

fn raw_time(value: MssqlValueRef<'_>) -> Option<odbc_api::sys::Time> {
    match ValueRef::to_owned(&value).kind() {
        MssqlValueKind::Time(time) => Some(*time),
        _ => None,
    }
}

fn raw_timestamp(value: MssqlValueRef<'_>) -> Option<odbc_api::sys::Timestamp> {
    match ValueRef::to_owned(&value).kind() {
        MssqlValueKind::Timestamp(timestamp) => Some(*timestamp),
        _ => None,
    }
}

fn trimmed_text(value: MssqlValueRef<'_>) -> Option<String> {
    Some(value.as_str()?.trim_end_matches('\0').trim().to_owned())
}

fn naive_from_timestamp(value: odbc_api::sys::Timestamp) -> Result<NaiveDateTime, BoxDynError> {
    let date = NaiveDate::from_ymd_opt(value.year as i32, value.month as u32, value.day as u32)
        .ok_or_else(|| "ODBC: invalid date values in timestamp".to_string())?;
    let time = NaiveTime::from_hms_nano_opt(
        value.hour as u32,
        value.minute as u32,
        value.second as u32,
        value.fraction,
    )
    .ok_or_else(|| "ODBC: invalid time values in timestamp".to_string())?;
    Ok(NaiveDateTime::new(date, time))
}

impl<'r> Decode<'r, Mssql> for NaiveDate {
    fn decode(value: MssqlValueRef<'r>) -> Result<Self, BoxDynError> {
        if let Some(date) = raw_date(value) {
            return NaiveDate::from_ymd_opt(date.year as i32, date.month as u32, date.day as u32)
                .ok_or_else(|| "ODBC: invalid date values".into());
        }

        if let Some(text) = trimmed_text(value) {
            if let Some(date) = parse_yyyymmdd_text_as_naive_date(&text) {
                return Ok(date);
            }

            if let Ok(date) = text.parse() {
                return Ok(date);
            }
        }

        if let Some(integer) = value.as_i64() {
            if let Some(date) = parse_yyyymmdd_as_naive_date(integer) {
                return Ok(date);
            }

            return Err(format!(
                "ODBC: cannot decode NaiveDate from integer '{integer}': not in YYYYMMDD range"
            )
            .into());
        }

        if let Some(float) = value.as_f64() {
            if let Some(date) = parse_yyyymmdd_as_naive_date(float as i64) {
                return Ok(date);
            }

            return Err(format!(
                "ODBC: cannot decode NaiveDate from float '{float}': not in YYYYMMDD range"
            )
            .into());
        }

        Err("ODBC: cannot decode NaiveDate".into())
    }
}

impl<'r> Decode<'r, Mssql> for NaiveTime {
    fn decode(value: MssqlValueRef<'r>) -> Result<Self, BoxDynError> {
        if let Some(time) = raw_time(value) {
            return NaiveTime::from_hms_opt(
                time.hour as u32,
                time.minute as u32,
                time.second as u32,
            )
            .ok_or_else(|| "ODBC: invalid time values".into());
        }

        let Some(text) = trimmed_text(value) else {
            return Err("ODBC: cannot decode NaiveTime".into());
        };

        text.parse()
            .map_err(|error| format!("ODBC: cannot decode NaiveTime from '{text}': {error}").into())
    }
}

impl<'r> Decode<'r, Mssql> for NaiveDateTime {
    fn decode(value: MssqlValueRef<'r>) -> Result<Self, BoxDynError> {
        if let Some(timestamp) = raw_timestamp(value) {
            return naive_from_timestamp(timestamp);
        }

        let Some(text) = trimmed_text(value) else {
            return Err("ODBC: cannot decode NaiveDateTime".into());
        };

        // Try parsing DATETIMEOFFSET text like "2026-06-18 17:39:55.1234567 +00:00"
        // by stripping the timezone suffix and parsing the datetime portion.
        if let Some((datetime_part, _offset)) = text.rsplit_once(' ') {
            if _offset.starts_with('+') || _offset.starts_with('-') || _offset == "Z" {
                if let Ok(datetime) =
                    NaiveDateTime::parse_from_str(datetime_part, "%Y-%m-%d %H:%M:%S%.f")
                {
                    return Ok(datetime);
                }
                if let Ok(datetime) =
                    NaiveDateTime::parse_from_str(datetime_part, "%Y-%m-%d %H:%M:%S")
                {
                    return Ok(datetime);
                }
            }
        }

        if let Ok(datetime) = NaiveDateTime::parse_from_str(&text, "%Y-%m-%d %H:%M:%S%.f") {
            return Ok(datetime);
        }

        if let Ok(datetime) = NaiveDateTime::parse_from_str(&text, "%Y-%m-%d %H:%M:%S") {
            return Ok(datetime);
        }

        text.parse().map_err(|error| {
            format!("ODBC: cannot decode NaiveDateTime from '{text}': {error}").into()
        })
    }
}

impl<'r> Decode<'r, Mssql> for DateTime<Utc> {
    fn decode(value: MssqlValueRef<'r>) -> Result<Self, BoxDynError> {
        if let Some(timestamp) = raw_timestamp(value) {
            return Ok(DateTime::<Utc>::from_naive_utc_and_offset(
                naive_from_timestamp(timestamp)?,
                Utc,
            ));
        }

        let Some(text) = trimmed_text(value) else {
            return Err("ODBC: cannot decode DateTime<Utc>".into());
        };

        if let Ok(datetime) = text.parse() {
            return Ok(datetime);
        }

        // Try parsing DATETIMEOFFSET text: "2026-06-18 17:39:55.1234567 +00:00"
        if let Ok(datetime) =
            DateTime::<FixedOffset>::parse_from_str(&text, "%Y-%m-%d %H:%M:%S%.f %#z")
        {
            return Ok(datetime.to_utc());
        }
        if let Ok(datetime) =
            DateTime::<FixedOffset>::parse_from_str(&text, "%Y-%m-%d %H:%M:%S %#z")
        {
            return Ok(datetime.to_utc());
        }

        if let Ok(datetime) = <NaiveDateTime as Decode<Mssql>>::decode(value) {
            return Ok(DateTime::<Utc>::from_naive_utc_and_offset(datetime, Utc));
        }

        Err(format!("ODBC: cannot decode DateTime<Utc> from '{text}'").into())
    }
}

impl<'r> Decode<'r, Mssql> for DateTime<FixedOffset> {
    fn decode(value: MssqlValueRef<'r>) -> Result<Self, BoxDynError> {
        if let Some(timestamp) = raw_timestamp(value) {
            return Ok(DateTime::<Utc>::from_naive_utc_and_offset(
                naive_from_timestamp(timestamp)?,
                Utc,
            )
            .fixed_offset());
        }

        let Some(text) = trimmed_text(value) else {
            return Err("ODBC: cannot decode DateTime<FixedOffset>".into());
        };

        if let Ok(datetime) = text.parse() {
            return Ok(datetime);
        }

        // Try parsing DATETIMEOFFSET text: "2026-06-18 17:39:55.1234567 +00:00"
        if let Ok(datetime) =
            DateTime::<FixedOffset>::parse_from_str(&text, "%Y-%m-%d %H:%M:%S%.f %#z")
        {
            return Ok(datetime);
        }
        if let Ok(datetime) =
            DateTime::<FixedOffset>::parse_from_str(&text, "%Y-%m-%d %H:%M:%S %#z")
        {
            return Ok(datetime);
        }

        if let Ok(datetime) = <NaiveDateTime as Decode<Mssql>>::decode(value) {
            return Ok(DateTime::<Utc>::from_naive_utc_and_offset(datetime, Utc).fixed_offset());
        }

        Err(format!("ODBC: cannot decode DateTime<FixedOffset> from '{text}'").into())
    }
}

impl<'r> Decode<'r, Mssql> for DateTime<Local> {
    fn decode(value: MssqlValueRef<'r>) -> Result<Self, BoxDynError> {
        if let Some(timestamp) = raw_timestamp(value) {
            return Ok(DateTime::<Utc>::from_naive_utc_and_offset(
                naive_from_timestamp(timestamp)?,
                Utc,
            )
            .with_timezone(&Local));
        }

        Ok(<DateTime<Utc> as Decode<Mssql>>::decode(value)?.with_timezone(&Local))
    }
}

// ---------------------------------------------------------------------------
// From impls bridging Option<NaiveDateTime> ↔ Option<DateTime<...>>
//
// These enable the sqlx query macros (query_as!, query!, query_scalar!) to
// convert between datetime types when using compile-time checked queries.
//
// The macros generate code like:
//   let val = row.try_get_unchecked::<Option<NaiveDateTime>, _>(i)?.into();
// which needs From<Option<NaiveDateTime>> for Option<DateTime<Utc>>.
//
// In other SQLx drivers (Postgres, MySQL) these aren't needed because
// NaiveDateTime and DateTime<Utc> map to different SQL types (TIMESTAMP vs
// TIMESTAMPTZ), so the correct type is selected by return_type_for_id.
// Here all datetime types share MssqlTypeInfo::TIMESTAMP, so NaiveDateTime
// is always the "canonical" match and .into() must convert to the user's
// requested type.
// ---------------------------------------------------------------------------





#[cfg(test)]
mod tests {
    use super::*;
    use crate::MssqlValue;
    use sqlx_core::type_info::TypeInfo;
    use sqlx_core::value::Value;

    #[test]
    fn naive_date_type_compatibility_matches_old_odbc() {
        assert!(<NaiveDate as Type<Mssql>>::compatible(&MssqlTypeInfo::DATE));
        assert!(<NaiveDate as Type<Mssql>>::compatible(
            &MssqlTypeInfo::varchar(None)
        ));
        assert!(<NaiveDate as Type<Mssql>>::compatible(
            &MssqlTypeInfo::INTEGER
        ));
    }

    #[test]
    fn naive_date_decodes_old_text_and_numeric_forms() -> Result<(), BoxDynError> {
        for value in [
            MssqlValue::new(MssqlValueKind::Text("2020-01-02".to_owned())),
            MssqlValue::new(MssqlValueKind::Text("20200102".to_owned())),
            MssqlValue::new(MssqlValueKind::BigInt(20200102)),
            MssqlValue::new(MssqlValueKind::Double(20200102.0)),
        ] {
            assert_eq!(
                <NaiveDate as Decode<Mssql>>::decode(value.as_ref())?,
                NaiveDate::from_ymd_opt(2020, 1, 2).unwrap()
            );
        }

        Ok(())
    }

    #[test]
    fn chrono_values_decode_raw_odbc_temporal_kinds() -> Result<(), BoxDynError> {
        let date = MssqlValue::new(MssqlValueKind::Date(odbc_api::sys::Date {
            year: 2020,
            month: 1,
            day: 2,
        }));
        assert_eq!(
            <NaiveDate as Decode<Mssql>>::decode(date.as_ref())?,
            NaiveDate::from_ymd_opt(2020, 1, 2).unwrap()
        );

        let time = MssqlValue::new(MssqlValueKind::Time(odbc_api::sys::Time {
            hour: 15,
            minute: 30,
            second: 45,
        }));
        assert_eq!(
            <NaiveTime as Decode<Mssql>>::decode(time.as_ref())?,
            NaiveTime::from_hms_opt(15, 30, 45).unwrap()
        );

        let timestamp = MssqlValue::new(MssqlValueKind::Timestamp(odbc_api::sys::Timestamp {
            year: 2020,
            month: 1,
            day: 2,
            hour: 15,
            minute: 30,
            second: 45,
            fraction: 123_456_789,
        }));
        assert_eq!(
            <NaiveDateTime as Decode<Mssql>>::decode(timestamp.as_ref())?,
            NaiveDate::from_ymd_opt(2020, 1, 2)
                .unwrap()
                .and_hms_nano_opt(15, 30, 45, 123_456_789)
                .unwrap()
        );

        Ok(())
    }

    #[test]
    fn chrono_values_decode_text_datetime_forms() -> Result<(), BoxDynError> {
        let value = MssqlValue::new(MssqlValueKind::Text("2020-01-02 15:30:45".to_owned()));
        let expected = NaiveDate::from_ymd_opt(2020, 1, 2)
            .unwrap()
            .and_hms_opt(15, 30, 45)
            .unwrap();

        assert_eq!(
            <NaiveDateTime as Decode<Mssql>>::decode(value.as_ref())?,
            expected
        );
        assert_eq!(
            <DateTime<Utc> as Decode<Mssql>>::decode(value.as_ref())?,
            DateTime::<Utc>::from_naive_utc_and_offset(expected, Utc)
        );

        Ok(())
    }

    #[test]
    fn chrono_values_encode_to_old_odbc_argument_forms() -> Result<(), BoxDynError> {
        let mut buf = Vec::new();
        let date = NaiveDate::from_ymd_opt(2020, 1, 2).unwrap();
        let _ = <NaiveDate as Encode<Mssql>>::encode(date, &mut buf)?;
        assert_eq!(
            buf,
            vec![MssqlArgumentValue::Date(odbc_api::sys::Date {
                year: 2020,
                month: 1,
                day: 2
            })]
        );

        buf.clear();
        let datetime = date.and_hms_nano_opt(15, 30, 45, 123_456_789).unwrap();
        let _ = <NaiveDateTime as Encode<Mssql>>::encode(datetime, &mut buf)?;
        assert_eq!(
            buf,
            vec![MssqlArgumentValue::Timestamp(odbc_api::sys::Timestamp {
                year: 2020,
                month: 1,
                day: 2,
                hour: 15,
                minute: 30,
                second: 45,
                fraction: 123_456_789
            })]
        );

        buf.clear();
        let utc = DateTime::<Utc>::from_naive_utc_and_offset(datetime, Utc);
        let _ = <DateTime<Utc> as Encode<Mssql>>::encode(utc, &mut buf)?;
        assert_eq!(buf, vec![MssqlArgumentValue::Text(utc.to_rfc3339())]);

        Ok(())
    }

    #[test]
    fn chrono_type_info_names_match_old_odbc() {
        assert_eq!(<NaiveDate as Type<Mssql>>::type_info().name(), "DATE");
        assert_eq!(<NaiveTime as Type<Mssql>>::type_info().name(), "TIME");
        assert_eq!(
            <NaiveDateTime as Type<Mssql>>::type_info().name(),
            "TIMESTAMP"
        );
        assert_eq!(
            <DateTime<Utc> as Type<Mssql>>::type_info().name(),
            "DATETIMEOFFSET"
        );
        assert_eq!(
            <DateTime<FixedOffset> as Type<Mssql>>::type_info().name(),
            "DATETIMEOFFSET"
        );
    }
}
