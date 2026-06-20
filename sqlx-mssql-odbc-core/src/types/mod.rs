#[cfg(feature = "bigdecimal")]
#[cfg_attr(docsrs, doc(cfg(feature = "bigdecimal")))]
mod bigdecimal;

#[cfg(feature = "chrono")]
#[cfg_attr(docsrs, doc(cfg(feature = "chrono")))]
mod chrono;

#[cfg(feature = "time")]
#[cfg_attr(docsrs, doc(cfg(feature = "time")))]
mod time;

#[cfg(any(feature = "decimal", feature = "rust_decimal"))]
#[cfg_attr(docsrs, doc(cfg(any(feature = "decimal", feature = "rust_decimal"))))]
mod decimal;

#[cfg(feature = "spatial")]
#[cfg_attr(docsrs, doc(cfg(feature = "spatial")))]
mod geo;

#[cfg(feature = "json")]
#[cfg_attr(docsrs, doc(cfg(feature = "json")))]
mod json;

#[cfg(feature = "uuid")]
#[cfg_attr(docsrs, doc(cfg(feature = "uuid")))]
mod uuid;
