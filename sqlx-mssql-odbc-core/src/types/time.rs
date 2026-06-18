use crate::value::MssqlValueRef;
use crate::{DataTypeExt, Mssql, MssqlArgumentValue, MssqlTypeInfo, MssqlValueKind};
use sqlx_core::decode::Decode;
use sqlx_core::encode::{Encode, IsNull};
use sqlx_core::error::BoxDynError;
use sqlx_core::types::Type;
use sqlx_core::value::ValueRef;
use time::{Date, OffsetDateTime, PrimitiveDateTime, Time};

impl Type<Mssql> for OffsetDateTime {
    fn type_info() -> MssqlTypeInfo {
        MssqlTypeInfo::timestamp(6)
    }

    fn compatible(ty: &MssqlTypeInfo) -> bool {
        ty.data_type().accepts_datetime_data()
            || ty.data_type().accepts_character_data()
            || ty.data_type().accepts_numeric_data()
    }
}

impl Type<Mssql> for PrimitiveDateTime {
    fn type_info() -> MssqlTypeInfo {
        MssqlTypeInfo::timestamp(6)
    }

    fn compatible(ty: &MssqlTypeInfo) -> bool {
        ty.data_type().accepts_datetime_data() || ty.data_type().accepts_character_data()
    }
}

impl Type<Mssql> for Date {
    fn type_info() -> MssqlTypeInfo {
        MssqlTypeInfo::DATE
    }

    fn compatible(ty: &MssqlTypeInfo) -> bool {
        ty.data_type().accepts_datetime_data() || ty.data_type().accepts_character_data()
    }
}

impl Type<Mssql> for Time {
    fn type_info() -> MssqlTypeInfo {
        MssqlTypeInfo::time(6)
    }

    fn compatible(ty: &MssqlTypeInfo) -> bool {
        ty.data_type().accepts_datetime_data() || ty.data_type().accepts_character_data()
    }
}

impl<'q> Encode<'q, Mssql> for OffsetDateTime {
    fn encode_by_ref(&self, buf: &mut Vec<MssqlArgumentValue>) -> Result<IsNull, BoxDynError> {
        let utc_dt = self.to_offset(time::UtcOffset::UTC);
        let primitive_dt = PrimitiveDateTime::new(utc_dt.date(), utc_dt.time());
        buf.push(MssqlArgumentValue::Text(primitive_dt.to_string()));
        Ok(IsNull::No)
    }
}

impl<'q> Encode<'q, Mssql> for PrimitiveDateTime {
    fn encode_by_ref(&self, buf: &mut Vec<MssqlArgumentValue>) -> Result<IsNull, BoxDynError> {
        buf.push(MssqlArgumentValue::Text(self.to_string()));
        Ok(IsNull::No)
    }
}

impl<'q> Encode<'q, Mssql> for Date {
    fn encode_by_ref(&self, buf: &mut Vec<MssqlArgumentValue>) -> Result<IsNull, BoxDynError> {
        buf.push(MssqlArgumentValue::Text(self.to_string()));
        Ok(IsNull::No)
    }
}

impl<'q> Encode<'q, Mssql> for Time {
    fn encode_by_ref(&self, buf: &mut Vec<MssqlArgumentValue>) -> Result<IsNull, BoxDynError> {
        buf.push(MssqlArgumentValue::Text(self.to_string()));
        Ok(IsNull::No)
    }
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

fn parse_unix_timestamp_as_offset_datetime(timestamp: i64) -> Option<OffsetDateTime> {
    OffsetDateTime::from_unix_timestamp(timestamp).ok()
}

fn primitive_from_timestamp(
    value: odbc_api::sys::Timestamp,
) -> Result<PrimitiveDateTime, BoxDynError> {
    let date = date_from_raw(odbc_api::sys::Date {
        year: value.year,
        month: value.month,
        day: value.day,
    })?;
    let time = Time::from_hms_nano(
        value.hour as u8,
        value.minute as u8,
        value.second as u8,
        value.fraction,
    )?;

    Ok(PrimitiveDateTime::new(date, time))
}

fn date_from_raw(value: odbc_api::sys::Date) -> Result<Date, BoxDynError> {
    let month = time::Month::try_from(value.month as u8)
        .map_err(|_| "ODBC: invalid month value".to_owned())?;

    Ok(Date::from_calendar_date(
        value.year as i32,
        month,
        value.day as u8,
    )?)
}

impl<'r> Decode<'r, Mssql> for OffsetDateTime {
    fn decode(value: MssqlValueRef<'r>) -> Result<Self, BoxDynError> {
        if let Some(timestamp) = raw_timestamp(value) {
            return Ok(primitive_from_timestamp(timestamp)?.assume_utc());
        }

        if let Some(integer) = value.as_i64() {
            if let Some(datetime) = parse_unix_timestamp_as_offset_datetime(integer) {
                return Ok(datetime);
            }
        }

        if let Some(float) = value.as_f64() {
            if let Some(datetime) = parse_unix_timestamp_as_offset_datetime(float as i64) {
                return Ok(datetime);
            }
        }

        let Some(text) = trimmed_text(value) else {
            return Err("ODBC: cannot decode OffsetDateTime".into());
        };

        if let Ok(datetime) = OffsetDateTime::parse(
            &text,
            &time::format_description::well_known::Iso8601::DEFAULT,
        ) {
            return Ok(datetime);
        }

        if let Ok(datetime) = <PrimitiveDateTime as Decode<Mssql>>::decode(value) {
            return Ok(datetime.assume_utc());
        }

        Err("ODBC: cannot decode OffsetDateTime".into())
    }
}

impl<'r> Decode<'r, Mssql> for PrimitiveDateTime {
    fn decode(value: MssqlValueRef<'r>) -> Result<Self, BoxDynError> {
        if let Some(timestamp) = raw_timestamp(value) {
            return primitive_from_timestamp(timestamp);
        }

        if let Some(integer) = value.as_i64() {
            if let Some(offset_dt) = parse_unix_timestamp_as_offset_datetime(integer) {
                let utc_dt = offset_dt.to_offset(time::UtcOffset::UTC);
                return Ok(PrimitiveDateTime::new(utc_dt.date(), utc_dt.time()));
            }
        }

        if let Some(float) = value.as_f64() {
            if let Some(offset_dt) = parse_unix_timestamp_as_offset_datetime(float as i64) {
                let utc_dt = offset_dt.to_offset(time::UtcOffset::UTC);
                return Ok(PrimitiveDateTime::new(utc_dt.date(), utc_dt.time()));
            }
        }

        let Some(text) = trimmed_text(value) else {
            return Err("ODBC: cannot decode PrimitiveDateTime".into());
        };

        if let Ok(datetime) = PrimitiveDateTime::parse(
            &text,
            &time::format_description::well_known::Iso8601::DEFAULT,
        ) {
            return Ok(datetime);
        }

        for format in [
            time::macros::format_description!("[year]-[month]-[day] [hour]:[minute]:[second]"),
            time::macros::format_description!(
                "[year]-[month]-[day] [hour]:[minute]:[second].[subsecond]"
            ),
            time::macros::format_description!("[year]-[month]-[day]T[hour]:[minute]:[second]"),
        ] {
            if let Ok(datetime) = PrimitiveDateTime::parse(&text, format) {
                return Ok(datetime);
            }
        }

        Err("ODBC: cannot decode PrimitiveDateTime".into())
    }
}

fn parse_yyyymmdd_as_time_date(value: i64) -> Option<Date> {
    if !(19000101..=30001231).contains(&value) {
        return None;
    }

    let year = (value / 10000) as i32;
    let month = ((value % 10000) / 100) as u8;
    let day = (value % 100) as u8;
    let month = time::Month::try_from(month).ok()?;

    Date::from_calendar_date(year, month, day).ok()
}

fn parse_yyyymmdd_text_as_time_date(value: &str) -> Option<Date> {
    if value.len() != 8 || !value.chars().all(|ch| ch.is_ascii_digit()) {
        return None;
    }

    let year = value[0..4].parse::<i32>().ok()?;
    let month = value[4..6].parse::<u8>().ok()?;
    let day = value[6..8].parse::<u8>().ok()?;
    let month = time::Month::try_from(month).ok()?;

    Date::from_calendar_date(year, month, day).ok()
}

impl<'r> Decode<'r, Mssql> for Date {
    fn decode(value: MssqlValueRef<'r>) -> Result<Self, BoxDynError> {
        if let Some(date) = raw_date(value) {
            return date_from_raw(date);
        }

        if let Some(integer) = value.as_i64() {
            if let Some(date) = parse_yyyymmdd_as_time_date(integer) {
                return Ok(date);
            }

            if let Ok(days) = i32::try_from(integer) {
                let epoch = Date::from_calendar_date(1970, time::Month::January, 1)?;
                if let Some(date) = epoch.checked_add(time::Duration::days(days as i64)) {
                    return Ok(date);
                }
            }
        }

        if let Some(float) = value.as_f64() {
            if let Some(date) = parse_yyyymmdd_as_time_date(float as i64) {
                return Ok(date);
            }
        }

        let Some(text) = trimmed_text(value) else {
            return Err("ODBC: cannot decode Date".into());
        };

        if let Some(date) = parse_yyyymmdd_text_as_time_date(&text) {
            return Ok(date);
        }

        if let Ok(date) = Date::parse(
            &text,
            &time::macros::format_description!("[year]-[month]-[day]"),
        ) {
            return Ok(date);
        }

        Date::parse(
            &text,
            &time::format_description::well_known::Iso8601::DEFAULT,
        )
        .map_err(|_| "ODBC: cannot decode Date".into())
    }
}

fn parse_seconds_as_time(seconds: i64) -> Option<Time> {
    let hours = seconds / 3600;
    let minutes = (seconds % 3600) / 60;
    let seconds = seconds % 60;

    if (0..24).contains(&hours) && (0..60).contains(&minutes) && (0..60).contains(&seconds) {
        Time::from_hms(hours as u8, minutes as u8, seconds as u8).ok()
    } else {
        None
    }
}

impl<'r> Decode<'r, Mssql> for Time {
    fn decode(value: MssqlValueRef<'r>) -> Result<Self, BoxDynError> {
        if let Some(time) = raw_time(value) {
            return Ok(Time::from_hms(
                time.hour as u8,
                time.minute as u8,
                time.second as u8,
            )?);
        }

        if let Some(integer) = value.as_i64() {
            if let Some(time) = parse_seconds_as_time(integer) {
                return Ok(time);
            }
        }

        if let Some(float) = value.as_f64() {
            if let Some(time) = parse_seconds_as_time(float as i64) {
                return Ok(time);
            }
        }

        let Some(text) = trimmed_text(value) else {
            return Err("ODBC: cannot decode Time".into());
        };

        for format in [
            time::macros::format_description!("[hour]:[minute]:[second]"),
            time::macros::format_description!("[hour]:[minute]:[second].[subsecond]"),
        ] {
            if let Ok(time) = Time::parse(&text, format) {
                return Ok(time);
            }
        }

        Err("ODBC: cannot decode Time".into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::MssqlValue;
    use sqlx_core::value::Value;
    use time::{macros::date, macros::datetime, macros::time as time_macro};

    #[test]
    fn time_type_compatibility_matches_old_odbc() {
        assert!(<OffsetDateTime as Type<Mssql>>::compatible(
            &MssqlTypeInfo::TIMESTAMP
        ));
        assert!(<OffsetDateTime as Type<Mssql>>::compatible(
            &MssqlTypeInfo::varchar(None)
        ));
        assert!(<OffsetDateTime as Type<Mssql>>::compatible(
            &MssqlTypeInfo::DOUBLE
        ));
    }

    #[test]
    fn time_values_decode_old_text_and_numeric_forms() -> Result<(), BoxDynError> {
        let datetime_value = MssqlValue::new(MssqlValueKind::Text("2023-12-25 14:30:00".to_owned()));
        assert_eq!(
            <PrimitiveDateTime as Decode<Mssql>>::decode(datetime_value.as_ref())?,
            datetime!(2023-12-25 14:30:00)
        );

        let date_value = MssqlValue::new(MssqlValueKind::Text("20231225".to_owned()));
        assert_eq!(
            <Date as Decode<Mssql>>::decode(date_value.as_ref())?,
            date!(2023 - 12 - 25)
        );

        let time_value = MssqlValue::new(MssqlValueKind::BigInt(52_200));
        assert_eq!(
            <Time as Decode<Mssql>>::decode(time_value.as_ref())?,
            time_macro!(14:30:00)
        );

        Ok(())
    }

    #[test]
    fn time_values_decode_raw_odbc_temporal_kinds() -> Result<(), BoxDynError> {
        let date_value = MssqlValue::new(MssqlValueKind::Date(odbc_api::sys::Date {
            year: 2023,
            month: 12,
            day: 25,
        }));
        assert_eq!(
            <Date as Decode<Mssql>>::decode(date_value.as_ref())?,
            date!(2023 - 12 - 25)
        );

        let time_value = MssqlValue::new(MssqlValueKind::Time(odbc_api::sys::Time {
            hour: 14,
            minute: 30,
            second: 0,
        }));
        assert_eq!(
            <Time as Decode<Mssql>>::decode(time_value.as_ref())?,
            time_macro!(14:30:00)
        );

        let timestamp_value = MssqlValue::new(MssqlValueKind::Timestamp(odbc_api::sys::Timestamp {
            year: 2023,
            month: 12,
            day: 25,
            hour: 14,
            minute: 30,
            second: 0,
            fraction: 123_456_789,
        }));
        assert_eq!(
            <PrimitiveDateTime as Decode<Mssql>>::decode(timestamp_value.as_ref())?,
            datetime!(2023-12-25 14:30:00.123456789)
        );

        Ok(())
    }

    #[test]
    fn time_values_encode_to_old_odbc_argument_forms() -> Result<(), BoxDynError> {
        let mut buf = Vec::new();

        let date = date!(2023 - 12 - 25);
        let result = <Date as Encode<Mssql>>::encode(date, &mut buf)?;
        assert!(matches!(result, IsNull::No));
        assert_eq!(buf, vec![MssqlArgumentValue::Text("2023-12-25".to_owned())]);

        buf.clear();
        let time = time_macro!(14:30:00);
        let result = <Time as Encode<Mssql>>::encode(time, &mut buf)?;
        assert!(matches!(result, IsNull::No));
        assert!(
            matches!(&buf[..], [MssqlArgumentValue::Text(text)] if text.starts_with("14:30:00"))
        );

        Ok(())
    }

    #[test]
    fn time_decode_error_matches_old_odbc() {
        let value = MssqlValue::new(MssqlValueKind::Text("not_a_datetime".to_owned()));
        let result = <PrimitiveDateTime as Decode<Mssql>>::decode(value.as_ref());

        assert_eq!(
            result.unwrap_err().to_string(),
            "ODBC: cannot decode PrimitiveDateTime"
        );
    }
}
