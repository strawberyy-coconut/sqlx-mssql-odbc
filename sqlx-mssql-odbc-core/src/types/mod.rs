#[cfg(feature = "bigdecimal")]
mod bigdecimal;

#[cfg(feature = "chrono")]
mod chrono;

#[cfg(feature = "time")]
mod time;

#[cfg(any(feature = "decimal", feature = "rust_decimal"))]
mod decimal;

#[cfg(feature = "spatial")]
mod geo;

#[cfg(feature = "json")]
mod json;

#[cfg(feature = "uuid")]
mod uuid;
