use odbc_api::{
    handles::{slice_to_cow_utf8, Record},
    Error as OdbcApiError,
};
use std::borrow::Cow;
use std::fmt::{Display, Formatter, Result as FmtResult};

/// Result alias for this crate.
pub type Result<T, E = MssqlError> = std::result::Result<T, E>;

/// Error type returned by this crate.
#[derive(Debug, thiserror::Error)]
pub enum MssqlError {
    /// MSSQL ODBC driver-manager or database error.
    #[error(transparent)]
    Database(#[from] MssqlDatabaseError),

    /// Invalid local configuration.
    #[error("MSSQL ODBC configuration error: {0}")]
    Configuration(String),
}

impl From<OdbcApiError> for MssqlError {
    fn from(error: OdbcApiError) -> Self {
        Self::Database(MssqlDatabaseError::from(error))
    }
}

impl From<MssqlError> for sqlx_core::Error {
    fn from(error: MssqlError) -> Self {
        match error {
            MssqlError::Database(error) => sqlx_core::Error::Database(Box::new(error)),
            MssqlError::Configuration(message) => sqlx_core::Error::Configuration(message.into()),
        }
    }
}

pub(crate) fn database_error_with_context(
    error: OdbcApiError,
    context: impl Into<String>,
) -> MssqlError {
    MssqlError::Database(MssqlDatabaseError::with_context(error, context))
}

pub(crate) fn database_error_with_context_lazy(
    error: OdbcApiError,
    context: impl FnOnce() -> String,
) -> MssqlError {
    MssqlError::Database(MssqlDatabaseError::with_context(error, context()))
}

/// Database error details extracted from ODBC diagnostics.
#[derive(Debug)]
pub struct MssqlDatabaseError {
    error: OdbcApiError,
    message: String,
    code: Option<String>,
}

impl MssqlDatabaseError {
    fn with_context(error: OdbcApiError, context: impl Into<String>) -> Self {
        let context = context.into();
        let mut database_error = Self::from(error);
        database_error.message = format!("{context}: {}", database_error.message);
        database_error
    }

    fn diagnostic_record(error: &OdbcApiError) -> Option<&Record> {
        match error {
            OdbcApiError::Diagnostics { record, .. } => Some(record),
            OdbcApiError::InvalidRowArraySize { record, .. } => Some(record),
            OdbcApiError::UnsupportedOdbcApiVersion(record) => Some(record),
            OdbcApiError::UnableToRepresentNull(record) => Some(record),
            OdbcApiError::OracleOdbcDriverDoesNotSupport64Bit(record) => Some(record),
            _ => None,
        }
    }

    fn diagnostic_code(record: &Record) -> Option<String> {
        let code = record.state.as_str();

        if code.as_bytes().iter().all(|&byte| byte == 0) {
            None
        } else {
            Some(code.to_owned())
        }
    }

    /// Primary diagnostic message.
    pub fn message(&self) -> &str {
        &self.message
    }

    /// ODBC SQLSTATE code, if available.
    pub fn code(&self) -> Option<Cow<'_, str>> {
        self.code.as_deref().map(Cow::Borrowed)
    }
}

impl From<OdbcApiError> for MssqlDatabaseError {
    fn from(error: OdbcApiError) -> Self {
        let record = Self::diagnostic_record(&error);
        let message = record
            .map(|record| slice_to_cow_utf8(&record.message).into_owned())
            .filter(|message| !message.is_empty())
            .unwrap_or_else(|| error.to_string());
        let code = record.and_then(Self::diagnostic_code);

        Self {
            error,
            message,
            code,
        }
    }
}

impl Display for MssqlDatabaseError {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        f.write_str(&self.message)
    }
}

impl std::error::Error for MssqlDatabaseError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.error)
    }
}

impl sqlx_core::error::DatabaseError for MssqlDatabaseError {
    fn message(&self) -> &str {
        self.message()
    }

    fn code(&self) -> Option<Cow<'_, str>> {
        self.code()
    }

    fn as_error(&self) -> &(dyn std::error::Error + Send + Sync + 'static) {
        self
    }

    fn as_error_mut(&mut self) -> &mut (dyn std::error::Error + Send + Sync + 'static) {
        self
    }

    fn into_error(self: Box<Self>) -> Box<dyn std::error::Error + Send + Sync + 'static> {
        self
    }

    fn kind(&self) -> sqlx_core::error::ErrorKind {
        sqlx_core::error::ErrorKind::Other
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use odbc_api::handles::{Record, SqlChar, State};

    fn sql_chars(text: &str) -> Vec<SqlChar> {
        text.bytes().collect()
    }

    #[test]
    fn database_error_uses_odbc_diagnostics_for_message_and_code() {
        let error = MssqlDatabaseError::from(OdbcApiError::Diagnostics {
            function: "SQLExecDirect",
            record: Record {
                state: State(*b"HY000"),
                native_error: 1234,
                message: sql_chars("syntax error near FROM"),
            },
        });

        assert_eq!(error.message(), "syntax error near FROM");
        assert_eq!(error.code().as_deref(), Some("HY000"));
    }

    #[test]
    fn database_error_context_is_included_in_message_and_display() {
        let error = MssqlDatabaseError::with_context(
            OdbcApiError::Diagnostics {
                function: "SQLSetStmtAttr",
                record: Record {
                    state: State(*b"HY092"),
                    native_error: 0,
                    message: sql_chars("invalid attribute option identifier"),
                },
            },
            "ODBC buffered fetching could not be enabled",
        );

        assert_eq!(
            error.message(),
            "ODBC buffered fetching could not be enabled: invalid attribute option identifier"
        );
        assert_eq!(error.to_string(), error.message());
        assert_eq!(error.code().as_deref(), Some("HY092"));
    }
}
