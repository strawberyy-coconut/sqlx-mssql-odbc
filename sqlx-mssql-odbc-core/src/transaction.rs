use crate::{Mssql, MssqlConnection};

/// Transaction manager for MSSQL via ODBC.
pub struct MssqlTransactionManager;

impl sqlx_core::transaction::TransactionManager for MssqlTransactionManager {
    type Database = Mssql;

    async fn begin(
        conn: &mut MssqlConnection,
        _statement: Option<sqlx_core::sql_str::SqlStr>,
    ) -> Result<(), sqlx_core::Error> {
        // The blocking_begin command is sent to the actor synchronously via
        // the channel. Since begin_blocking is a blocking send, we call it
        // from within offload_blocking to avoid blocking the async runtime.
        let depth = conn.transaction_depth();
        let result = conn.begin_blocking();
        if result.is_ok() {
            conn.set_transaction_depth(depth + 1);
        }
        result
    }

    async fn commit(conn: &mut MssqlConnection) -> Result<(), sqlx_core::Error> {
        let depth = conn.transaction_depth();
        if depth == 0 {
            return Ok(());
        }
        let result = conn.commit_blocking();
        if result.is_ok() {
            if depth == 1 {
                conn.set_transaction_depth(0);
            } else {
                conn.set_transaction_depth(depth - 1);
            }
        }
        result
    }

    async fn rollback(conn: &mut MssqlConnection) -> Result<(), sqlx_core::Error> {
        let depth = conn.transaction_depth();
        if depth == 0 {
            return Ok(());
        }
        let result = conn.rollback_blocking();
        if result.is_ok() {
            if depth == 1 {
                conn.set_transaction_depth(0);
            } else {
                conn.set_transaction_depth(depth - 1);
            }
        }
        result
    }

    fn start_rollback(conn: &mut MssqlConnection) {
        conn.start_rollback();
    }

    fn get_transaction_depth(conn: &MssqlConnection) -> usize {
        conn.transaction_depth()
    }
}
