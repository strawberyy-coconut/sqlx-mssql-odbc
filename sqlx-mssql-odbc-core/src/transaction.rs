use crate::connection::offload_blocking;
use crate::{Mssql, MssqlConnection};

/// Transaction manager for MSSQL via ODBC.
pub struct MssqlTransactionManager;

impl sqlx_core::transaction::TransactionManager for MssqlTransactionManager {
    type Database = Mssql;

    async fn begin(
        conn: &mut MssqlConnection,
        _statement: Option<sqlx_core::sql_str::SqlStr>,
    ) -> Result<(), sqlx_core::Error> {
        let depth = conn.transaction_depth();
        let conn_arc = conn.conn.clone();

        if depth == 0 {
            offload_blocking(move || {
                let c = conn_arc.lock().map_err(|_| {
                    sqlx_core::Error::Protocol(
                        "MSSQL ODBC begin: failed to lock connection".to_owned(),
                    )
                })?;
                c.set_autocommit(false).map_err(|error| {
                    sqlx_core::Error::from(crate::error::database_error_with_context(
                        error,
                        "failed to disable ODBC autocommit while beginning a transaction",
                    ))
                })
            })
            .await?;
        } else {
            let savepoint = format!("sqlx_sp_{depth}");
            offload_blocking(move || {
                let c = conn_arc.lock().map_err(|_| {
                    sqlx_core::Error::Protocol(
                        "MSSQL ODBC begin (savepoint): failed to lock connection".to_owned(),
                    )
                })?;
                c.execute(&format!("SAVE TRANSACTION {savepoint}"), (), None)
                    .map_err(|error| {
                        sqlx_core::Error::from(crate::error::database_error_with_context(
                            error,
                            format!(
                                "failed to create save point `{savepoint}` for nested transaction"
                            ),
                        ))
                    })?;
                Ok(())
            })
            .await?;
        }

        conn.set_transaction_depth(depth + 1);
        Ok(())
    }

    async fn commit(conn: &mut MssqlConnection) -> Result<(), sqlx_core::Error> {
        let depth = conn.transaction_depth();
        if depth == 0 {
            return Ok(());
        }

        if depth == 1 {
            let conn_arc = conn.conn.clone();
            offload_blocking(move || {
                let c = conn_arc.lock().map_err(|_| {
                    sqlx_core::Error::Protocol(
                        "MSSQL ODBC commit: failed to lock connection".to_owned(),
                    )
                })?;
                c.commit().map_err(|error| {
                    sqlx_core::Error::from(crate::error::database_error_with_context(
                        error,
                        "failed to commit the active MSSQL ODBC transaction",
                    ))
                })?;
                c.set_autocommit(true).map_err(|error| {
                    sqlx_core::Error::from(crate::error::database_error_with_context(
                        error,
                        "failed to restore ODBC autocommit after commit",
                    ))
                })
            })
            .await?;
            conn.set_transaction_depth(0);
        } else {
            // Nested commit: save points are implicitly released on outer
            // COMMIT, so just decrement the depth counter.
            conn.set_transaction_depth(depth - 1);
        }
        Ok(())
    }

    async fn rollback(conn: &mut MssqlConnection) -> Result<(), sqlx_core::Error> {
        let depth = conn.transaction_depth();
        if depth == 0 {
            return Ok(());
        }

        if depth == 1 {
            let conn_arc = conn.conn.clone();
            offload_blocking(move || {
                let c = conn_arc.lock().map_err(|_| {
                    sqlx_core::Error::Protocol(
                        "MSSQL ODBC rollback: failed to lock connection".to_owned(),
                    )
                })?;
                c.rollback().map_err(|error| {
                    sqlx_core::Error::from(crate::error::database_error_with_context(
                        error,
                        "failed to roll back the active ODBC transaction",
                    ))
                })?;
                c.set_autocommit(true).map_err(|error| {
                    sqlx_core::Error::from(crate::error::database_error_with_context(
                        error,
                        "failed to restore ODBC autocommit after rollback",
                    ))
                })
            })
            .await?;
            conn.set_transaction_depth(0);
        } else {
            let savepoint = format!("sqlx_sp_{}", depth - 1);
            let conn_arc = conn.conn.clone();
            offload_blocking(move || {
                let c = conn_arc.lock().map_err(|_| {
                    sqlx_core::Error::Protocol(
                        "MSSQL ODBC rollback (savepoint): failed to lock connection".to_owned(),
                    )
                })?;
                c.execute(&format!("ROLLBACK TRANSACTION {savepoint}"), (), None)
                    .map_err(|error| {
                        sqlx_core::Error::from(crate::error::database_error_with_context(
                            error,
                            format!("failed to roll back to save point `{savepoint}`"),
                        ))
                    })?;
                Ok(())
            })
            .await?;
            conn.set_transaction_depth(depth - 1);
        }
        Ok(())
    }

    fn start_rollback(conn: &mut MssqlConnection) {
        conn.start_rollback();
    }

    fn get_transaction_depth(conn: &MssqlConnection) -> usize {
        conn.transaction_depth()
    }
}
