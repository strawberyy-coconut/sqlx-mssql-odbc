use crate::Mssql;
#[allow(unused_imports)]
use sqlx_core as sqlx;

sqlx_core::impl_type_checking!(
    Mssql {
        bool,
        i8,
        i16,
        i32,
        i64,
        f32,
        f64,
        String,
        Vec<u8>,

        #[cfg(feature = "uuid")]
        sqlx::types::Uuid,
    },
    ParamChecking::Weak,
    // ODBC drivers are permissive — any type can be decoded to a basic
    // string or binary representation. Feature gates just enable typed
    // decode/encode paths, so no type requires a feature gate.
    feature-types: _info => None,
    datetime-types: {
        chrono: {
            sqlx::types::chrono::NaiveDate,
            sqlx::types::chrono::NaiveTime,
            sqlx::types::chrono::NaiveDateTime,
            sqlx::types::chrono::DateTime<sqlx::types::chrono::Utc>
                | sqlx::types::chrono::DateTime<_>,
        },
        time: {
            sqlx::types::time::OffsetDateTime,
            sqlx::types::time::PrimitiveDateTime,
            sqlx::types::time::Date,
            sqlx::types::time::Time,
        },
    },
    numeric-types: {
        bigdecimal: {
            sqlx::types::BigDecimal,
        },
        rust_decimal: {
            sqlx::types::Decimal,
        },
    },
);
