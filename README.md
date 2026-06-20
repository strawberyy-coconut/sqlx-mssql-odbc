# sqlx-mssql-odbc

Microsoft SQL Server driver for SQLx via ODBC.

This crate connects SQLx to Microsoft SQL Server through an ODBC driver manager
(unixODBC on Linux/macOS, built-in on Windows). It depends only on crates
published to crates.io and examples use `sqlx-core` directly.

## Minimal Query

```toml
[dependencies]
sqlx-mssql-odbc = "0.1.2"
sqlx-core = "=0.9.0"
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

```rust
use sqlx_core::connection::Connection;
use sqlx_core::row::Row;
use sqlx_mssql_odbc::MssqlConnection;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut conn = MssqlConnection::connect(
        "mssql://sa:MyPass@localhost:1433/testdb",
    ).await?;

    let row = sqlx_core::query::query("SELECT 1")
        .fetch_one(&mut conn)
        .await?;

    let value: i32 = row.try_get(0)?;
    println!("{value}");

    conn.close().await?;
    Ok(())
}
```

`MssqlConnection::connect()` accepts:
- A standard `mssql://` URL: `mssql://user:password@host:port/database?params`
- A raw ODBC connection string: `Driver={ODBC Driver 18 for SQL Server};Server=...`
- A bare DSN name: `MyMssqlDSN`

### URL query parameters

| Parameter | Description |
|---|---|
| `encrypt=true` | Enable TLS encryption (`Encrypt=yes`) |
| `trust_certificate=true` | Skip certificate validation (`TrustServerCertificate=yes`) |
| `driver=...` | Custom ODBC driver name (default: `{ODBC Driver 18 for SQL Server}`) |

### Builder methods

```rust
let mut options = MssqlConnectOptions::from_str("mssql://localhost/testdb")?;
options.encrypt(true)
       .trust_certificate(true)
       .batch_size(128)
       .statement_cache_capacity(200);
let conn = options.connect().await?;
```

## ODBC Setup

ODBC uses two native pieces outside this crate:

1. **A driver manager** - unixODBC on Linux/macOS (built into Windows).
2. **The Microsoft ODBC Driver for SQL Server** - version 17 or 18.

Enable the `vendored-unix-odbc` feature to statically link the unixODBC driver
manager into your application on Linux or macOS.

### Linux (Debian/Ubuntu)

```bash
curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add -
curl https://packages.microsoft.com/config/ubuntu/22.04/prod.list > /etc/apt/sources.list.d/mssql-release.list
apt-get update
ACCEPT_EULA=Y apt-get install -y msodbcsql18 unixodbc-dev
```

### macOS

```bash
brew install unixodbc
brew install msodbcsql18
```

### Windows

The ODBC driver manager is built into Windows. Install the [Microsoft ODBC Driver for SQL Server](https://learn.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server).

> **Platform note:** CI currently verifies Linux only. macOS and Windows are
> expected to work but are not exercised by automated tests. Community PRs for
> platform-specific fixes are welcome.

## Connection Pooling

```rust
use sqlx_mssql_odbc::MssqlPoolOptions;

let pool = MssqlPoolOptions::new()
    .max_connections(10)
    .connect("mssql://sa:MyPass@localhost:1433/testdb")
    .await?;

let row = sqlx_core::query::query("SELECT 1")
    .fetch_one(&pool)
    .await?;
```

## Features

| Feature | Description |
|---|---|
| `bigdecimal` | `BigDecimal` type support |
| `chrono` | `chrono` datetime types |
| `rust_decimal` / `decimal` | `Decimal` type support |
| `json` | `serde_json::Value` support |
| `time` | `time` crate datetime types |
| `uuid` | `uuid::Uuid` support |
| `offline` | Compile-time query checking with `query!()` |
| `macros` | `query!()`, `query_as!()` and other proc macros |
| `derive` | `Encode`, `Decode`, `Type`, `FromRow` derive macros |
| `runtime-tokio` | Tokio runtime support |
| `tls-none` | No TLS (default) |
| `vendored-unix-odbc` | Statically link unixODBC |

## CLI (`sqlx-mssql`)

A thin wrapper around `sqlx-cli` for managing MSSQL databases, running
migrations, and preparing offline query data.

### Install

```bash
cargo install sqlx-mssql-odbc-cli
```

After installation, the `sqlx-mssql` binary is available on your `PATH`.

### Usage

All standard `sqlx-cli` subcommands are supported. Provide your database URL
via `--database-url` or the `DATABASE_URL` environment variable (or a `.env`
file).

```bash
# Create / drop the database
sqlx-mssql database create
sqlx-mssql database drop

# Create and run migrations
sqlx-mssql migrate add <name>
sqlx-mssql migrate run

# Revert the last migration
sqlx-mssql migrate revert

# List migration status
sqlx-mssql migrate info

# Prepare offline query data (for compile-time checked queries)
sqlx-mssql prepare
```

**Environment variable** (add to `.env` in your project root):

```
DATABASE_URL=mssql://sa:Password1!@localhost:1433/my_database
```

Or use the `--database-url` flag:

```bash
sqlx-mssql migrate run --database-url mssql://sa:Password1!@localhost:1433/my_database
```

### Run without installing

```bash
cargo run -p sqlx-mssql-odbc-cli -- migrate run
```

## Running Tests

```bash
docker run -e "ACCEPT_EULA=Y" -e "MSSQL_SA_PASSWORD=MyPass" \
  -p 1433:1433 -d mcr.microsoft.com/mssql/server:2022-latest

MSSQL_DATABASE_URL="mssql://sa:MyPass@localhost:1433/testdb" \
MSSQL_TEST_REQUIRED=1 \
cargo test --test mssql
```
