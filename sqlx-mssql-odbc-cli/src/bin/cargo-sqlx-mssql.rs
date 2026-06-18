use clap::Parser;
use console::style;
use sqlx_cli::Opt;

/// Cargo invokes this binary as `cargo-sqlx-mssql sqlx-mssql <args>`,
/// so the parser below is defined with that in mind.
#[derive(Parser, Debug)]
#[clap(bin_name = "cargo")]
enum Cli {
    SqlxMssql(Opt),
}

#[tokio::main]
async fn main() {
    // Checks for `--no-dotenv` before parsing.
    sqlx_cli::maybe_apply_dotenv();

    // Register only the MSSQL ODBC driver for `AnyConnection` to discover.
    // We must NOT call `sqlx_core::any::driver::install_default_drivers()` as that only
    // registers the built-in drivers (mysql/postgres/sqlite).
    sqlx_core::any::driver::install_drivers(&[sqlx_mssql_odbc_core::any::DRIVER])
        .expect("failed to install MSSQL ODBC driver");

    let Cli::SqlxMssql(opt) = Cli::parse();

    // no special handling here
    if let Err(error) = sqlx_cli::run(opt).await {
        println!("{} {}", style("error:").bold().red(), error);
        std::process::exit(1);
    }
}
