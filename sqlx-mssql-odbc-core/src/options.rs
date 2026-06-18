use crate::{MssqlConnection, Result};
use log::LevelFilter;
use std::fmt::{self, Debug, Formatter};
use std::str::FromStr;
use std::time::Duration;
use url::Url;

/// Fetch-buffer settings used by the MSSQL ODBC driver.
///
/// `max_column_size = Some(_)` enables buffered fetching and can truncate long text or binary
/// fields to the configured size. `max_column_size = None` keeps fetching unbuffered so variable
/// sized values are not truncated by this crate's buffer allocation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MssqlBufferSettings {
    /// Number of rows fetched in each batch.
    pub batch_size: usize,
    /// Maximum text or binary column size in buffered mode, or `None` for unbuffered mode.
    pub max_column_size: Option<usize>,
}

impl Default for MssqlBufferSettings {
    fn default() -> Self {
        Self {
            batch_size: 64,
            max_column_size: None,
        }
    }
}

/// Connection options for an MSSQL ODBC data source.
#[derive(Clone)]
pub struct MssqlConnectOptions {
    pub(crate) conn_str: String,
    pub(crate) buffer_settings: MssqlBufferSettings,
    pub(crate) statement_cache_capacity: usize,
    pub(crate) log_statements: LevelFilter,
    pub(crate) log_slow_statements: LevelFilter,
    pub(crate) log_slow_statement_duration: Duration,
}

impl MssqlConnectOptions {
    /// Returns the normalized ODBC connection string.
    pub fn connection_string(&self) -> &str {
        &self.conn_str
    }

    /// Sets the buffer configuration for this connection.
    pub fn buffer_settings(&mut self, settings: MssqlBufferSettings) -> &mut Self {
        assert!(settings.batch_size > 0, "batch_size must be greater than 0");
        if let Some(size) = settings.max_column_size {
            assert!(size > 0, "max_column_size must be greater than 0");
        }

        self.buffer_settings = settings;
        self
    }

    /// Returns the current buffer settings.
    pub fn buffer_settings_ref(&self) -> &MssqlBufferSettings {
        &self.buffer_settings
    }

    /// Sets the number of rows fetched in each batch.
    pub fn batch_size(&mut self, batch_size: usize) -> &mut Self {
        assert!(batch_size > 0, "batch_size must be greater than 0");
        self.buffer_settings.batch_size = batch_size;
        self
    }

    /// Sets the maximum buffered column size, or `None` for unbuffered fetching.
    pub fn max_column_size(&mut self, max_column_size: Option<usize>) -> &mut Self {
        if let Some(size) = max_column_size {
            assert!(size > 0, "max_column_size must be greater than 0");
        }

        self.buffer_settings.max_column_size = max_column_size;
        self
    }

    /// Sets the maximum number of prepared statements kept in this connection's cache.
    pub fn statement_cache_capacity(&mut self, capacity: usize) -> &mut Self {
        self.statement_cache_capacity = capacity;
        self
    }

    /// Sets regular statement logging level.
    pub fn log_statements(&mut self, level: LevelFilter) -> &mut Self {
        self.log_statements = level;
        self
    }

    /// Sets slow statement logging level and threshold.
    pub fn log_slow_statements(&mut self, level: LevelFilter, duration: Duration) -> &mut Self {
        self.log_slow_statements = level;
        self.log_slow_statement_duration = duration;
        self
    }

    /// Enables or disables TLS encryption for the connection.
    ///
    /// When enabled, adds `Encrypt=yes` to the connection string.
    pub fn encrypt(&mut self, enable: bool) -> &mut Self {
        if enable && !self.conn_str.contains("Encrypt=") {
            self.conn_str.push_str(";Encrypt=yes");
        }
        self
    }

    /// Enables or disables server certificate validation.
    ///
    /// When enabled alongside `encrypt(true)`, adds `TrustServerCertificate=yes`
    /// to the connection string. Useful for development environments with
    /// self-signed certificates.
    pub fn trust_certificate(&mut self, enable: bool) -> &mut Self {
        if enable && !self.conn_str.contains("TrustServerCertificate=") {
            self.conn_str.push_str(";TrustServerCertificate=yes");
        }
        self
    }

    /// Opens a blocking MSSQL ODBC connection.
    pub fn connect_blocking(&self) -> Result<MssqlConnection> {
        MssqlConnection::connect_blocking(self)
    }
}

impl Debug for MssqlConnectOptions {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("MssqlConnectOptions")
            .field("conn_str", &"<redacted>")
            .field("buffer_settings", &self.buffer_settings)
            .field("statement_cache_capacity", &self.statement_cache_capacity)
            .field("log_statements", &self.log_statements)
            .field("log_slow_statements", &self.log_slow_statements)
            .field(
                "log_slow_statement_duration",
                &self.log_slow_statement_duration,
            )
            .finish()
    }
}

/// Builds an ODBC connection string from a `mssql://` URL.
///
/// Supported URL format:
/// `mssql://user:password@host:port/database?param=value`
///
/// Supported query parameters:
/// - `trust_certificate=true` — adds `TrustServerCertificate=yes`
/// - `encrypt=true` — adds `Encrypt=yes`
/// - `driver=...` — custom ODBC driver name
fn mssql_url_to_connection_string(url: &Url) -> String {
    let scheme = url.scheme();
    let is_mssql = scheme.eq_ignore_ascii_case("mssql");

    // Only handle mssql:// URLs; odbc:// or other schemes pass through
    if !is_mssql && !scheme.eq_ignore_ascii_case("odbc") {
        return url.as_str().to_owned();
    }

    let host = url.host_str().unwrap_or("localhost");
    let port = url.port().unwrap_or(1433);
    let database = url.path().trim_start_matches('/');
    let username = url.username();
    let password = url.password().unwrap_or_default();

    let mut conn_str = format!(
        "Driver={{ODBC Driver 18 for SQL Server}};Server={host},{port}"
    );

    if !database.is_empty() {
        conn_str.push_str(&format!(";Database={database}"));
    }
    if !username.is_empty() {
        conn_str.push_str(&format!(";UID={username}"));
    }
    if !password.is_empty() {
        conn_str.push_str(&format!(";PWD={password}"));
    }

    // Parse query parameters
    for (key, value) in url.query_pairs() {
        match key.as_ref() {
            "trust_certificate" if value == "true" => {
                if !conn_str.contains("TrustServerCertificate=") {
                    conn_str.push_str(";TrustServerCertificate=yes");
                }
            }
            "encrypt" if value == "true" => {
                if !conn_str.contains("Encrypt=") {
                    conn_str.push_str(";Encrypt=yes");
                }
            }
            "driver" => {
                let driver_val = format!("Driver={value}");
                if let Some(pos) = conn_str.find("Driver=") {
                    let end = conn_str[pos..].find(';').map(|i| pos + i).unwrap_or(conn_str.len());
                    conn_str.replace_range(pos..end, &driver_val);
                }
            }
            _ => {}
        }
    }

    conn_str
}

impl FromStr for MssqlConnectOptions {
    type Err = sqlx_core::Error;

    fn from_str(input: &str) -> std::result::Result<Self, Self::Err> {
        let trimmed = input.trim();

        // Legacy support: strip odbc: prefix before URL parsing
        let (trimmed, _had_odbc_prefix) = if let Some(rest) = trimmed.strip_prefix("odbc:") {
            (rest, true)
        } else {
            (trimmed, false)
        };

        // Try to parse as a mssql:// URL (only for actual mssql:// scheme)
        if trimmed.starts_with("mssql://") || trimmed.starts_with("mssql:") {
            if let Ok(url) = Url::parse(trimmed) {
                let scheme = url.scheme();
                if scheme.eq_ignore_ascii_case("mssql") {
                    let conn_str = mssql_url_to_connection_string(&url);

                    return Ok(Self {
                        conn_str,
                        buffer_settings: MssqlBufferSettings::default(),
                        statement_cache_capacity: 100,
                        log_statements: LevelFilter::Debug,
                        log_slow_statements: LevelFilter::Warn,
                        log_slow_statement_duration: Duration::from_secs(1),
                    });
                }
            }
        }

        // Treat as raw ODBC connection string (or bare DSN)
        let conn_str = if trimmed.contains('=') {
            trimmed.to_owned()
        } else {
            format!("DSN={trimmed}")
        };

        Ok(Self {
            conn_str,
            buffer_settings: MssqlBufferSettings::default(),
            statement_cache_capacity: 100,
            log_statements: LevelFilter::Debug,
            log_slow_statements: LevelFilter::Warn,
            log_slow_statement_duration: Duration::from_secs(1),
        })
    }
}

impl sqlx_core::connection::ConnectOptions for MssqlConnectOptions {
    type Connection = MssqlConnection;

    fn from_url(url: &Url) -> std::result::Result<Self, sqlx_core::Error> {
        Self::from_str(url.as_str())
    }

    async fn connect(&self) -> std::result::Result<Self::Connection, sqlx_core::Error> {
        self.connect_blocking().map_err(Into::into)
    }

    fn log_statements(mut self, level: LevelFilter) -> Self {
        self.log_statements = level;
        self
    }

    fn log_slow_statements(mut self, level: LevelFilter, duration: Duration) -> Self {
        self.log_slow_statements = level;
        self.log_slow_statement_duration = duration;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_mssql_url_with_all_components() {
        let url = "mssql://sa:Password1!@server.example.com:1433/testdb";
        let options = MssqlConnectOptions::from_str(url).unwrap();
        let cs = options.connection_string();
        assert!(cs.contains("Driver={ODBC Driver 18 for SQL Server}"));
        assert!(cs.contains("Server=server.example.com,1433"));
        assert!(cs.contains("Database=testdb"));
        assert!(cs.contains("UID=sa"));
        assert!(cs.contains("PWD=Password1!"));
    }

    #[test]
    fn parses_mssql_url_with_default_port() {
        let url = "mssql://user:pass@localhost/mydb";
        let options = MssqlConnectOptions::from_str(url).unwrap();
        let cs = options.connection_string();
        assert!(cs.contains("Server=localhost,1433"));
    }

    #[test]
    fn parses_mssql_url_without_credentials() {
        let url = "mssql://localhost/mydb";
        let options = MssqlConnectOptions::from_str(url).unwrap();
        let cs = options.connection_string();
        assert!(cs.contains("Server=localhost,1433"));
        assert!(cs.contains("Database=mydb"));
        assert!(!cs.contains("UID="));
        assert!(!cs.contains("PWD="));
    }

    #[test]
    fn parses_mssql_url_with_trust_certificate() {
        let url = "mssql://localhost/mydb?trust_certificate=true";
        let options = MssqlConnectOptions::from_str(url).unwrap();
        let cs = options.connection_string();
        assert!(cs.contains("TrustServerCertificate=yes"));
    }

    #[test]
    fn parses_mssql_url_with_encrypt() {
        let url = "mssql://localhost/mydb?encrypt=true";
        let options = MssqlConnectOptions::from_str(url).unwrap();
        let cs = options.connection_string();
        assert!(cs.contains("Encrypt=yes"));
    }

    #[test]
    fn parses_mssql_url_with_custom_driver() {
        let url = "mssql://localhost/mydb?driver={ODBC Driver 17 for SQL Server}";
        let options = MssqlConnectOptions::from_str(url).unwrap();
        let cs = options.connection_string();
        assert!(cs.contains("Driver={ODBC Driver 17 for SQL Server}"));
    }

    #[test]
    fn preserves_raw_odbc_connection_strings() {
        let input = "Driver={ODBC Driver 17 for SQL Server};Server=localhost;Database=test";
        let options = MssqlConnectOptions::from_str(input).unwrap();
        assert_eq!(options.connection_string(), input);
    }

    #[test]
    fn supports_dsn_format() {
        let options = MssqlConnectOptions::from_str("MyMssqlDSN").unwrap();
        assert_eq!(options.connection_string(), "DSN=MyMssqlDSN");
    }

    #[test]
    fn strips_legacy_odbc_prefix() {
        let options = MssqlConnectOptions::from_str("odbc:DSN=Warehouse").unwrap();
        assert_eq!(options.connection_string(), "DSN=Warehouse");
    }

    #[test]
    fn encrypt_method_adds_encrypt() {
        let mut options = MssqlConnectOptions::from_str("DSN=Test").unwrap();
        options.encrypt(true);
        assert!(options.connection_string().contains("Encrypt=yes"));
    }

    #[test]
    fn trust_certificate_method_adds_flag() {
        let mut options = MssqlConnectOptions::from_str("DSN=Test").unwrap();
        options.trust_certificate(true);
        assert!(options.connection_string().contains("TrustServerCertificate=yes"));
    }

    #[test]
    fn updates_buffer_settings_incrementally() {
        let mut options = MssqlConnectOptions::from_str("DSN=Test").unwrap();
        options.batch_size(128).max_column_size(Some(2048));
        assert_eq!(options.buffer_settings.batch_size, 128);
        assert_eq!(options.buffer_settings.max_column_size, Some(2048));
    }
}
