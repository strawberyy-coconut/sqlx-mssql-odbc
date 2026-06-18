use clap::Parser;
use console::style;
use sqlx_cli::Opt;

#[tokio::main]
async fn main() {
    // Checks for `--no-dotenv` before parsing.
    sqlx_cli::maybe_apply_dotenv();

    // Register only the MSSQL ODBC driver for `AnyConnection` to discover.
    // We must NOT call `sqlx_core::any::driver::install_default_drivers()` as that only
    // registers the built-in drivers (mysql/postgres/sqlite).
    sqlx_core::any::driver::install_drivers(&[sqlx_mssql_odbc_core::any::DRIVER])
        .expect("failed to install MSSQL ODBC driver");

    let opt = Opt::parse();

    // no special handling here
    if let Err(error) = sqlx_cli::run(opt).await {
        println!("{} {}", style("error:").bold().red(), error);
        std::process::exit(1);
    }
}
