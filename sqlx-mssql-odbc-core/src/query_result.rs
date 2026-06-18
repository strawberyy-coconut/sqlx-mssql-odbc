/// Result summary for an MSSQL query via ODBC.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct MssqlQueryResult {
    rows_affected: u64,
}

impl MssqlQueryResult {
    /// Creates a query result with the given affected-row count.
    pub const fn new(rows_affected: u64) -> Self {
        Self { rows_affected }
    }

    /// Returns the number of rows affected by the query.
    pub const fn rows_affected(&self) -> u64 {
        self.rows_affected
    }
}

impl Extend<Self> for MssqlQueryResult {
    fn extend<T: IntoIterator<Item = Self>>(&mut self, iter: T) {
        self.rows_affected += iter
            .into_iter()
            .map(|result| result.rows_affected)
            .sum::<u64>();
    }
}
