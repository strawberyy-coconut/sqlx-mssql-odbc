use crate::value::MssqlValueRef;
use crate::{DataTypeExt, Mssql, MssqlArgumentValue, MssqlTypeInfo, MssqlValueKind};
use odbc_api::DataType;
use sqlx_core::decode::Decode;
use sqlx_core::encode::{Encode, IsNull};
use sqlx_core::error::BoxDynError;
use sqlx_core::types::Type;
use sqlx_core::value::ValueRef;

// Use sqlx's re-exported time types so that the types match what the query
// macros generate. Under the hood these are exactly the `time` crate types.
use sqlx_core::types::time::{
    Date as TimeDate, OffsetDateTime, PrimitiveDateTime, Time as TimeTime,
};

impl Type<Mssql> for TimeDate {
    fn type_info() -> MssqlTypeInfo {
        MssqlTypeInfo::DATE
    }

    fn compatible(ty: &MssqlTypeInfo) -> bool {
        matches!(ty.data_type(), DataType::Date) || ty.data_type().accepts_character_data()
    }
}

impl Type<Mssql> for TimeTime {
    fn type_info() -> MssqlTypeInfo {
        MssqlTypeInfo::TIME
    }

    fn compatible(ty: &MssqlTypeInfo) -> bool {
        matches!(ty.data_type(), DataType::Time { .. }) || ty.data_type().accepts_character_data()
    }
}

impl Type<Mssql> for PrimitiveDateTime {
    fn type_info() -> MssqlTypeInfo {
        MssqlTypeInfo::TIMESTAMP
    }

    fn compatible(ty: &MssqlTypeInfo) -> bool {
        matches!(ty.data_type(), DataType::Timestamp { .. })
            || ty.data_type().accepts_character_data()
    }
}

impl Type<Mssql> for OffsetDateTime {
    fn type_info() -> MssqlTypeInfo {
        MssqlTypeInfo::datetimeoffset()
    }

    fn compatible(ty: &MssqlTypeInfo) -> bool {
        matches!(ty.data_type(), DataType::Timestamp { .. })
            || matches!(
                ty.data_type(),
                DataType::Other {
                    data_type, ..
                } if data_type.0 == -155
            )
            || ty.data_type().accepts_character_data()
    }
}

// ---------------------------------------------------------------------------
// Encode
// ---------------------------------------------------------------------------

impl<'q> Encode<'q, Mssql> for TimeDate {
    fn encode_by_ref(&self, buf: &mut Vec<MssqlArgumentValue>) -> Result<IsNull, BoxDynError> {
        buf.push(MssqlArgumentValue::Date(odbc_api::sys::Date {
            year: self.year() as i16,
            month: u8::from(self.month()) as u16,
            day: self.day() as u16,
        }));
        Ok(IsNull::No)
    }
}

impl<'q> Encode<'q, Mssql> for TimeTime {
    fn encode_by_ref(&self, buf: &mut Vec<MssqlArgumentValue>) -> Result<IsNull, BoxDynError> {
        buf.push(MssqlArgumentValue::Time(odbc_api::sys::Time {
            hour: self.hour() as u16,
            minute: self.minute() as u16,
            second: self.second() as u16,
        }));
        Ok(IsNull::No)
    }
}

impl<'q> Encode<'q, Mssql> for PrimitiveDateTime {
    fn encode_by_ref(&self, buf: &mut Vec<MssqlArgumentValue>) -> Result<IsNull, BoxDynError> {
        buf.push(MssqlArgumentValue::Timestamp(timestamp_from_primitive(
            *self,
        )));
        Ok(IsNull::No)
    }
}

impl<'q> Encode<'q, Mssql> for OffsetDateTime {
    fn encode_by_ref(&self, buf: &mut Vec<MssqlArgumentValue>) -> Result<IsNull, BoxDynError> {
        use time::format_description::well_known::Rfc3339;
        let text = self
            .format(&Rfc3339)
            .map_err(|e| format!("ODBC: cannot format OffsetDateTime as Rfc3339: {e}"))?;
        buf.push(MssqlArgumentValue::Text(text));
        Ok(IsNull::No)
    }
}

fn timestamp_from_primitive(value: PrimitiveDateTime) -> odbc_api::sys::Timestamp {
    let nano = value.nanosecond();
    odbc_api::sys::Timestamp {
        year: value.year() as i16,
        month: u8::from(value.month()) as u16,
        day: value.day() as u16,
        hour: value.hour() as u16,
        minute: value.minute() as u16,
        second: value.second() as u16,
        fraction: (nano / 1000) * 1000,
    }
}

// ---------------------------------------------------------------------------
// Helpers — parse raw ODBC values
// ---------------------------------------------------------------------------

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

fn parse_yyyymmdd_as_date(value: i64) -> Option<TimeDate> {
    if !(19000101..=30001231).contains(&value) {
        return None;
    }

    let year = (value / 10000) as i32;
    let month_u8 = ((value % 10000) / 100) as u8;
    let day = (value % 100) as u8;
    let month = time::Month::try_from(month_u8).ok()?;
    TimeDate::from_calendar_date(year, month, day).ok()
}

fn parse_yyyymmdd_text_as_date(value: &str) -> Option<TimeDate> {
    if value.len() != 8 || !value.chars().all(|ch| ch.is_ascii_digit()) {
        return None;
    }

    let year = value[0..4].parse::<i32>().ok()?;
    let month_u8 = value[4..6].parse::<u8>().ok()?;
    let day = value[6..8].parse::<u8>().ok()?;
    let month = time::Month::try_from(month_u8).ok()?;
    TimeDate::from_calendar_date(year, month, day).ok()
}

fn primitive_from_timestamp(value: odbc_api::sys::Timestamp) -> Result<PrimitiveDateTime, BoxDynError> {
    let month = time::Month::try_from(value.month as u8)
        .map_err(|_| format!("ODBC: invalid month value in timestamp: {}", value.month))?;
    let date = TimeDate::from_calendar_date(value.year as i32, month, value.day as u8)
        .map_err(|_| "ODBC: invalid date values in timestamp".to_string())?;
    let time = TimeTime::from_hms_nano(
        value.hour as u8,
        value.minute as u8,
        value.second as u8,
        value.fraction,
    )
    .map_err(|_| "ODBC: invalid time values in timestamp".to_string())?;
    Ok(PrimitiveDateTime::new(date, time))
}

// ---------------------------------------------------------------------------
// Decode
// ---------------------------------------------------------------------------

impl<'r> Decode<'r, Mssql> for TimeDate {
    fn decode(value: MssqlValueRef<'r>) -> Result<Self, BoxDynError> {
        if let Some(date) = raw_date(value) {
            let month = time::Month::try_from(date.month as u8)
                .map_err(|_| "ODBC: invalid month value".to_string())?;
            return TimeDate::from_calendar_date(date.year as i32, month, date.day as u8)
                .map_err(|_| "ODBC: invalid date values".into());
        }

        if let Some(text) = trimmed_text(value) {
            if let Some(date) = parse_yyyymmdd_text_as_date(&text) {
                return Ok(date);
            }

            if let Ok(date) = TimeDate::parse(&text, &time::format_description::well_known::Iso8601::DATE) {
                return Ok(date);
            }
        }

        if let Some(integer) = value.as_i64() {
            if let Some(date) = parse_yyyymmdd_as_date(integer) {
                return Ok(date);
            }

            return Err(format!(
                "ODBC: cannot decode time::Date from integer '{integer}': not in YYYYMMDD range"
            )
            .into());
        }

        if let Some(float) = value.as_f64() {
            if let Some(date) = parse_yyyymmdd_as_date(float as i64) {
                return Ok(date);
            }

            return Err(format!(
                "ODBC: cannot decode time::Date from float '{float}': not in YYYYMMDD range"
            )
            .into());
        }

        Err("ODBC: cannot decode time::Date".into())
    }
}

impl<'r> Decode<'r, Mssql> for TimeTime {
    #[allow(deprecated)]
    fn decode(value: MssqlValueRef<'r>) -> Result<Self, BoxDynError> {
        if let Some(time) = raw_time(value) {
            return TimeTime::from_hms(time.hour as u8, time.minute as u8, time.second as u8)
                .map_err(|_| "ODBC: invalid time values".into());
        }

        let Some(text) = trimmed_text(value) else {
            return Err("ODBC: cannot decode time::Time".into());
        };

        // Try ISO 8601 time parsing
        if let Ok(t) = TimeTime::parse(
            &text,
            &time::format_description::well_known::Iso8601::TIME,
        ) {
            return Ok(t);
        }

        TimeTime::parse(
            &text,
            &time::format_description::parse("[hour]:[minute]:[second]")
                .map_err(|e| format!("ODBC: invalid time format: {e}").to_string())?,
        )
        .map_err(|error| format!("ODBC: cannot decode time::Time from '{text}': {error}").into())
    }
}

impl<'r> Decode<'r, Mssql> for PrimitiveDateTime {
    fn decode(value: MssqlValueRef<'r>) -> Result<Self, BoxDynError> {
        if let Some(timestamp) = raw_timestamp(value) {
            return primitive_from_timestamp(timestamp);
        }

        let Some(text) = trimmed_text(value) else {
            return Err("ODBC: cannot decode PrimitiveDateTime".into());
        };

        // Try parsing DATETIMEOFFSET text like "2026-06-18 17:39:55.1234567 +00:00"
        // by stripping the timezone suffix and parsing the datetime portion.
        if let Some((datetime_part, _offset)) = text.rsplit_once(' ') {
            if _offset.starts_with('+') || _offset.starts_with('-') || _offset == "Z" {
                if let Ok(dt) = parse_sql_datetime_text(datetime_part) {
                    return Ok(dt);
                }
            }
        }

        if let Ok(dt) = parse_sql_datetime_text(&text) {
            return Ok(dt);
        }

        if let Ok(dt) = PrimitiveDateTime::parse(
            &text,
            &time::format_description::well_known::Iso8601::DEFAULT,
        ) {
            return Ok(dt);
        }

        Err(format!("ODBC: cannot decode PrimitiveDateTime from '{text}'").into())
    }
}

impl<'r> Decode<'r, Mssql> for OffsetDateTime {
    #[allow(deprecated)]
    fn decode(value: MssqlValueRef<'r>) -> Result<Self, BoxDynError> {
        if let Some(timestamp) = raw_timestamp(value) {
            // Timestamps from ODBC have no timezone — assume UTC.
            let primitive = primitive_from_timestamp(timestamp)?;
            return Ok(primitive.assume_utc());
        }

        let Some(text) = trimmed_text(value) else {
            return Err("ODBC: cannot decode OffsetDateTime".into());
        };

        // Try RFC 3339
        if let Ok(dt) = OffsetDateTime::parse(
            &text,
            &time::format_description::well_known::Rfc3339,
        ) {
            return Ok(dt);
        }

        // Try SQL-style DATETIMEOFFSET text: "2026-06-18 17:39:55.1234567 +00:00"
        if let Ok(dt) = OffsetDateTime::parse(
            &text,
            &time::format_description::parse(
                "[year]-[month]-[day] [hour]:[minute]:[second].[subsecond] [offset_hour sign:mandatory]:[offset_minute]",
            )
            .map_err(|e| format!("ODBC: invalid format: {e}").to_string())?,
        ) {
            return Ok(dt);
        }

        // Try without subsecond digits
        if let Ok(dt) = OffsetDateTime::parse(
            &text,
            &time::format_description::parse(
                "[year]-[month]-[day] [hour]:[minute]:[second] [offset_hour sign:mandatory]:[offset_minute]",
            )
            .map_err(|e| format!("ODBC: invalid format: {e}").to_string())?,
        ) {
            return Ok(dt);
        }

        // Try ISO 8601
        if let Ok(dt) = OffsetDateTime::parse(
            &text,
            &time::format_description::well_known::Iso8601::DEFAULT,
        ) {
            return Ok(dt);
        }

        // Fallback: decode as PrimitiveDateTime and assume UTC
        if let Ok(primitive) = <PrimitiveDateTime as Decode<Mssql>>::decode(value) {
            return Ok(primitive.assume_utc());
        }

        Err(format!("ODBC: cannot decode OffsetDateTime from '{text}'").into())
    }
}

// ---------------------------------------------------------------------------
// Helpers for parsing SQL-style datetime strings
// ---------------------------------------------------------------------------

/// Parse a SQL-style datetime string like "2026-06-18 17:39:55" or
/// "2026-06-18 17:39:55.1234567" into a `PrimitiveDateTime`.
#[allow(deprecated)]
fn parse_sql_datetime_text(text: &str) -> Result<PrimitiveDateTime, BoxDynError> {
    // Try with subsecond digits
    if let Ok(dt) = PrimitiveDateTime::parse(
        text,
        &time::format_description::parse(
            "[year]-[month]-[day] [hour]:[minute]:[second].[subsecond]",
        )
        .map_err(|e| format!("ODBC: invalid format: {e}").to_string())?,
    ) {
        return Ok(dt);
    }

    // Try without subsecond digits
    PrimitiveDateTime::parse(
        text,
        &time::format_description::parse("[year]-[month]-[day] [hour]:[minute]:[second]")
            .map_err(|e| format!("ODBC: invalid format: {e}").to_string())?,
    )
    .map_err(|e| format!("ODBC: cannot parse datetime '{text}': {e}").into())
}

// ---------------------------------------------------------------------------
// From impls bridging Option<PrimitiveDateTime> ↔ Option<OffsetDateTime>
//
// These enable the sqlx query macros (query_as!, query!, query_scalar!) to
// convert between datetime types when using compile-time checked queries.
//
// All datetime types share MssqlTypeInfo::TIMESTAMP, so PrimitiveDateTime
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
    fn time_date_type_compatibility() {
        assert!(<TimeDate as Type<Mssql>>::compatible(&MssqlTypeInfo::DATE));
        assert!(<TimeDate as Type<Mssql>>::compatible(&MssqlTypeInfo::varchar(None)));
        assert_eq!(<TimeDate as Type<Mssql>>::type_info().name(), "DATE");
        assert_eq!(<TimeTime as Type<Mssql>>::type_info().name(), "TIME");
        assert_eq!(
            <PrimitiveDateTime as Type<Mssql>>::type_info().name(),
            "TIMESTAMP"
        );
        assert_eq!(
            <OffsetDateTime as Type<Mssql>>::type_info().name(),
            "DATETIMEOFFSET"
        );
    }

    #[test]
    fn time_date_decodes_text_and_numeric_forms() -> Result<(), BoxDynError> {
        for value in [
            MssqlValue::new(MssqlValueKind::Text("2020-01-02".to_owned())),
            MssqlValue::new(MssqlValueKind::Text("20200102".to_owned())),
            MssqlValue::new(MssqlValueKind::BigInt(20200102)),
            MssqlValue::new(MssqlValueKind::Double(20200102.0)),
        ] {
            let decoded = <TimeDate as Decode<Mssql>>::decode(value.as_ref())?;
            let expected = TimeDate::from_calendar_date(2020, time::Month::January, 2).unwrap();
            assert_eq!(decoded, expected);
        }

        Ok(())
    }

    #[test]
    fn time_values_decode_raw_odbc_temporal_kinds() -> Result<(), BoxDynError> {
        let date = MssqlValue::new(MssqlValueKind::Date(odbc_api::sys::Date {
            year: 2020,
            month: 1,
            day: 2,
        }));
        assert_eq!(
            <TimeDate as Decode<Mssql>>::decode(date.as_ref())?,
            TimeDate::from_calendar_date(2020, time::Month::January, 2).unwrap()
        );

        let time = MssqlValue::new(MssqlValueKind::Time(odbc_api::sys::Time {
            hour: 15,
            minute: 30,
            second: 45,
        }));
        assert_eq!(
            <TimeTime as Decode<Mssql>>::decode(time.as_ref())?,
            TimeTime::from_hms(15, 30, 45).unwrap()
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
        let expected = PrimitiveDateTime::new(
            TimeDate::from_calendar_date(2020, time::Month::January, 2).unwrap(),
            TimeTime::from_hms_nano(15, 30, 45, 123_456_789).unwrap(),
        );
        assert_eq!(
            <PrimitiveDateTime as Decode<Mssql>>::decode(timestamp.as_ref())?,
            expected
        );

        Ok(())
    }

    #[test]
    fn time_values_decode_text_datetime_forms() -> Result<(), BoxDynError> {
        let value = MssqlValue::new(MssqlValueKind::Text("2020-01-02 15:30:45".to_owned()));
        let expected = PrimitiveDateTime::new(
            TimeDate::from_calendar_date(2020, time::Month::January, 2).unwrap(),
            TimeTime::from_hms(15, 30, 45).unwrap(),
        );

        assert_eq!(
            <PrimitiveDateTime as Decode<Mssql>>::decode(value.as_ref())?,
            expected
        );
        assert_eq!(
            <OffsetDateTime as Decode<Mssql>>::decode(value.as_ref())?,
            expected.assume_utc()
        );

        Ok(())
    }

    #[test]
    fn time_values_encode_to_odbc_argument_forms() -> Result<(), BoxDynError> {
        let mut buf = Vec::new();
        let date = TimeDate::from_calendar_date(2020, time::Month::January, 2).unwrap();
        let _ = <TimeDate as Encode<Mssql>>::encode(date, &mut buf)?;
        assert_eq!(
            buf,
            vec![MssqlArgumentValue::Date(odbc_api::sys::Date {
                year: 2020,
                month: 1,
                day: 2
            })]
        );

        buf.clear();
        let primitive = PrimitiveDateTime::new(
            date,
            TimeTime::from_hms_nano(15, 30, 45, 123_456_789).unwrap(),
        );
        let _ = <PrimitiveDateTime as Encode<Mssql>>::encode(primitive, &mut buf)?;
        assert_eq!(
            buf,
            vec![MssqlArgumentValue::Timestamp(odbc_api::sys::Timestamp {
                year: 2020,
                month: 1,
                day: 2,
                hour: 15,
                minute: 30,
                second: 45,
                fraction: 123_456_000
            })]
        );

        buf.clear();
        let offset = primitive.assume_utc();
        let _ = <OffsetDateTime as Encode<Mssql>>::encode(offset, &mut buf)?;
        let encoded_text = if let MssqlArgumentValue::Text(ref s) = buf[0] {
            s.clone()
        } else {
            panic!("expected Text variant");
        };
        assert_eq!(encoded_text, "2020-01-02T15:30:45.123456789Z");

        Ok(())
    }

}
