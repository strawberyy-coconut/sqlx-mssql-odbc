

use crate::{
    MssqlArguments,  MssqlStatement
};

use sqlx_core::sql_str::SqlStr;

// ============================================================================
// Command enum — sent from the async handle to the actor thread
// ============================================================================
pub enum Command {
    Execute {
        sql: SqlStr,
        args: Option<MssqlArguments>,
        persistent: bool,
        response: super::ExecuteSender,
    },
    Prepare {
        sql: SqlStr,
        response: flume::Sender<std::result::Result<MssqlStatement, sqlx_core::Error>>,
    },
    Ping {
        response: flume::Sender<std::result::Result<(), sqlx_core::Error>>,
    },
    Begin {
        response: flume::Sender<std::result::Result<(), sqlx_core::Error>>,
    },
    Commit {
        response: flume::Sender<std::result::Result<(), sqlx_core::Error>>,
    },
    Rollback {
        response: flume::Sender<std::result::Result<(), sqlx_core::Error>>,
    },
    StartRollback,
    ExecSql {
        sql: String,
        response: flume::Sender<std::result::Result<(), sqlx_core::Error>>,
    },
    #[allow(dead_code)]
    ScalarI64 {
        sql: String,
        response: flume::Sender<std::result::Result<Option<i64>, sqlx_core::Error>>,
    },
    #[allow(dead_code)]
    Shutdown {
        signal: flume::Sender<()>,
    },
    #[allow(dead_code)]
    /// Returns `Vec<(version, checksum_bytes)>` from the migrations table.
    ListMigrations {
        sql: String,
        response: flume::Sender<std::result::Result<Vec<(i64, Vec<u8>)>, sqlx_core::Error>>,
    },
    /// Applies a migration: starts a transaction, runs SQL, inserts tracking
    /// record, commits. If `no_tx` is true the transaction is skipped.
    #[cfg(feature = "migrate")]
    ApplyMigration {
        sql: String,
        insert_sql: String,
        version: i64,
        no_tx: bool,
        response: flume::Sender<std::result::Result<std::time::Duration, sqlx_core::Error>>,
    },
    /// Reverts a migration: starts a transaction, runs SQL, deletes tracking
    /// record, commits. If `no_tx` is true the transaction is skipped.
    #[cfg(feature = "migrate")]
    RevertMigration {
        sql: String,
        delete_sql: String,
        version: i64,
        no_tx: bool,
        response: flume::Sender<std::result::Result<std::time::Duration, sqlx_core::Error>>,
    },
}
