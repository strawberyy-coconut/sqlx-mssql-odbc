
use crate::connection::{ColumnBinding, ExecuteResult, ExecuteSender};
use crate::connection::command::Command;
use crate::{
    MssqlArguments, MssqlBufferSettings, MssqlColumn, MssqlConnectOptions, MssqlQueryResult,
    MssqlRow, MssqlStatement, MssqlTypeInfo, MssqlValue, MssqlValueKind, Result,
};
use futures_core::future::BoxFuture;
use futures_core::stream::BoxStream;
use futures_util::{StreamExt, future, stream};
use odbc_api::buffers::{AnyColumnBufferSlice, BufferDesc, ColumnarDynBuffer, NullableSlice};
use odbc_api::{ConnectionTransitions, Cursor, DataType, Nullable, ResultSetMetadata};
use sqlx_core::Either;
use sqlx_core::column::Column;


use std::sync::Arc;


// ============================================================================
// Helper: send a command and await a oneshot response (async)
// ============================================================================

pub async fn send_command_async<T: Send>(
    cmd_tx: &flume::Sender<Command>,
    make_cmd: impl FnOnce(flume::Sender<T>) -> Command,
) -> std::result::Result<T, sqlx_core::Error> {
    let (resp_tx, resp_rx) = flume::bounded(1);
    let cmd = make_cmd(resp_tx);
    cmd_tx.send(cmd).map_err(|_| {
        sqlx_core::Error::Protocol("MSSQL ODBC connection actor has shut down".to_owned())
    })?;
    resp_rx.recv_async().await.map_err(|_| {
        sqlx_core::Error::Protocol("MSSQL ODBC connection actor response channel closed".to_owned())
    })
}

// ============================================================================
// Helper: send a command and wait for a oneshot response (blocking)
// ============================================================================

pub fn send_command_blocking<T: Send>(
    cmd_tx: &flume::Sender<Command>,
    make_cmd: impl FnOnce(flume::Sender<T>) -> Command,
) -> std::result::Result<T, sqlx_core::Error> {
    let (resp_tx, resp_rx) = flume::bounded(1);
    let cmd = make_cmd(resp_tx);
    cmd_tx.send(cmd).map_err(|_| {
        sqlx_core::Error::Protocol("MSSQL ODBC connection actor has shut down".to_owned())
    })?;
    resp_rx.recv().map_err(|_| {
        sqlx_core::Error::Protocol("MSSQL ODBC connection actor response channel closed".to_owned())
    })
}

// ============================================================================
// Helper: convert a flume receiver to a BoxStream
// ============================================================================

pub fn receiver_to_stream<'e>(rx: flume::Receiver<ExecuteResult>) -> BoxStream<'e, ExecuteResult> {
    stream::unfold(rx, |rx| async move {
        rx.recv_async().await.ok().map(|item| (item, rx))
    })
    .boxed()
}

// ============================================================================
// Helper: send query-result rows via the execute channel
// ============================================================================

pub fn send_rows_affected(
    rows_affected: Option<usize>,
    tx: &ExecuteSender,
) -> std::result::Result<(), sqlx_core::Error> {
    let rows_affected = rows_affected
        .unwrap_or(0)
        .try_into()
        .map_err(|_| sqlx_core::Error::Protocol("ODBC row count does not fit in u64".to_owned()))?;
    send_done(tx, rows_affected);
    Ok(())
}

pub fn send_done(tx: &ExecuteSender, rows_affected: u64) -> bool {
    tx.send(Ok(Either::Left(MssqlQueryResult::new(rows_affected))))
        .is_ok()
}

pub fn send_row(tx: &ExecuteSender, row: MssqlRow) -> bool {
    tx.send(Ok(Either::Right(row))).is_ok()
}

pub(crate) fn collect_columns(
    cursor: &mut impl ResultSetMetadata,
) -> std::result::Result<Vec<MssqlColumn>, sqlx_core::Error> {
    let count = cursor.num_result_cols().map_err(|error| {
        crate::error::database_error_with_context(error, "failed to read ODBC result-column count")
    })?;
    let count = usize::try_from(count).map_err(|_| {
        sqlx_core::Error::Protocol(format!("ODBC returned a negative column count: {count}"))
    })?;

    let mut columns = Vec::with_capacity(count);
    for ordinal in 0..count {
        let column_number = u16::try_from(ordinal + 1).map_err(|_| {
            sqlx_core::Error::Protocol(format!("ODBC column index exceeds u16: {}", ordinal + 1))
        })?;

        let mut description = odbc_api::ColumnDescription::default();
        cursor
            .describe_col(column_number, &mut description)
            .map_err(|error| {
                crate::error::database_error_with_context(
                    error,
                    format!("failed to describe ODBC result column {column_number}"),
                )
            })?;
        let name = description
            .name_to_string()
            .unwrap_or_else(|_| format!("col{ordinal}"));

        let nullable = match description.nullability {
            odbc_api::Nullability::NoNulls => Some(false),
            odbc_api::Nullability::Nullable => Some(true),
            odbc_api::Nullability::Unknown => None,
        };

        columns.push(MssqlColumn::new(
            ordinal,
            name,
            MssqlTypeInfo::new(description.data_type),
            nullable,
        ));
    }

    Ok(columns)
}

pub fn collect_prepared_columns(
    prepared: &mut impl ResultSetMetadata,
) -> std::result::Result<Vec<MssqlColumn>, sqlx_core::Error> {
    match collect_columns(prepared) {
        Ok(columns) => Ok(columns),
        Err(error) => Err(error),
    }
}

pub fn stream_result_sets<C>(
    mut cursor: C,
    settings: MssqlBufferSettings,
    tx: &ExecuteSender,
) -> std::result::Result<(), sqlx_core::Error>
where
    C: Cursor + ResultSetMetadata,
{
    loop {
        if cursor.num_result_cols().map_err(|error| {
            crate::error::database_error_with_context(
                error,
                "failed to read ODBC result-column count",
            )
        })? == 0
        {
            send_done(tx, 0);
        } else if let Some(max_column_size) = settings.max_column_size {
            let (receiver_open, finished_cursor) =
                stream_rows_buffered(cursor, settings.batch_size, max_column_size, tx)?;
            if !receiver_open {
                return Ok(());
            }
            cursor = finished_cursor;
        } else if !stream_rows_unbuffered(&mut cursor, tx)? {
            return Ok(());
        }

        match cursor.more_results().map_err(|error| {
            crate::error::database_error_with_context(error, "failed to advance ODBC result set")
        })? {
            Some(next_cursor) => cursor = next_cursor,
            None => return Ok(()),
        }
    }
}

pub fn stream_rows_buffered<C>(
    cursor: C,
    batch_size: usize,
    max_column_size: usize,
    tx: &ExecuteSender,
) -> std::result::Result<(bool, C), sqlx_core::Error>
where
    C: Cursor + ResultSetMetadata,
{
    let mut cursor = cursor;
    let bindings = build_buffer_bindings(&mut cursor, max_column_size)?;
    let buffer_descriptions = bindings
        .iter()
        .map(|binding| binding.buffer_desc)
        .collect::<Vec<_>>();
    let mut row_set_cursor = cursor
        .bind_buffer(ColumnarDynBuffer::from_descs(
            batch_size,
            buffer_descriptions,
        ))
        .map_err(|error| {
            crate::error::database_error_with_context(
                error,
                format!(
                    "ODBC buffered fetching could not be enabled with batch_size={batch_size}; \
                     this driver may reject the row-array or row-binding statement attributes \
                     used for column-wise buffered fetching, so use \
                     MssqlConnectOptions::max_column_size(None) to fetch rows unbuffered"
                ),
            )
        })?;
    let columns: Arc<[MssqlColumn]> = bindings
        .iter()
        .map(|binding| binding.column.clone())
        .collect::<Vec<_>>()
        .into();

    while let Some(batch) = row_set_cursor.fetch().map_err(|error| {
        crate::error::database_error_with_context(error, "ODBC buffered fetch failed")
    })? {
        let column_values = bindings
            .iter()
            .enumerate()
            .map(|(index, binding)| {
                buffered_column_values(batch.column(index), binding).map_err(|error| {
                    sqlx_core::Error::Protocol(format!(
                        "ODBC buffered fetch could not convert column {} (`{}`) using buffer {:?}: {error}",
                        binding.column.ordinal() + 1,
                        binding.column.name(),
                        binding.buffer_desc
                    ))
                })
            })
            .collect::<std::result::Result<Vec<_>, _>>()?;

        let mut column_iters = column_values
            .into_iter()
            .map(Vec::into_iter)
            .collect::<Vec<_>>();

        for row_index in 0..batch.num_rows() {
            let values = column_iters
                .iter_mut()
                .map(|values| {
                    values.next().map(MssqlValue::new).ok_or_else(|| {
                        sqlx_core::Error::Protocol(format!(
                            "ODBC buffered fetch produced too few values for row {}",
                            row_index + 1
                        ))
                    })
                })
                .collect::<std::result::Result<Vec<_>, _>>()?;
            if !send_row(tx, MssqlRow::new_shared(Arc::clone(&columns), values)) {
                let (cursor, _) = row_set_cursor.unbind().map_err(|error| {
                    crate::error::database_error_with_context(
                        error,
                        "ODBC buffered fetch could not unbind row buffer after receiver closed",
                    )
                })?;
                return Ok((false, cursor));
            }
        }
    }

    send_done(tx, 0);
    let (cursor, _) = row_set_cursor.unbind().map_err(|error| {
        crate::error::database_error_with_context(
            error,
            "ODBC buffered fetch could not unbind row buffer",
        )
    })?;
    Ok((true, cursor))
}

pub fn build_buffer_bindings(
    cursor: &mut impl ResultSetMetadata,
    max_column_size: usize,
) -> std::result::Result<Vec<ColumnBinding>, sqlx_core::Error> {
    collect_columns(cursor).map(|columns| {
        columns
            .into_iter()
            .map(|column| {
                let nullable = column.nullable().unwrap_or(true);
                ColumnBinding {
                    buffer_desc: map_buffer_desc(
                        column.type_info().data_type(),
                        max_column_size,
                        nullable,
                    ),
                    column,
                }
            })
            .collect()
    })
}

pub fn map_buffer_desc(data_type: DataType, max_column_size: usize, nullable: bool) -> BufferDesc {
    match data_type {
        DataType::TinyInt | DataType::SmallInt | DataType::Integer | DataType::BigInt => {
            BufferDesc::I64 { nullable }
        }
        DataType::Real => BufferDesc::F32 { nullable },
        DataType::Float { .. } | DataType::Double => BufferDesc::F64 { nullable },
        DataType::Bit => BufferDesc::Bit { nullable },
        DataType::Date => BufferDesc::Date { nullable },
        DataType::Time { .. } => BufferDesc::Time { nullable },
        DataType::Timestamp { .. } => BufferDesc::Timestamp { nullable },
        DataType::Binary { .. } | DataType::Varbinary { .. } | DataType::LongVarbinary { .. } => {
            BufferDesc::Binary {
                max_bytes: max_column_size,
            }
        }
        // Wide character types use SQL_C_WCHAR buffers (UTF-16) to avoid
        // codepage-dependent corruption of non-ASCII data.
        DataType::WChar { .. } | DataType::WVarchar { .. } | DataType::WLongVarchar { .. } => {
            BufferDesc::WText {
                max_str_len: max_column_size,
            }
        }
        // Narrow character types and fallback types use SQL_C_CHAR.
        DataType::Char { .. }
        | DataType::Varchar { .. }
        | DataType::LongVarchar { .. }
        | DataType::Other { .. }
        | DataType::Unknown
        | DataType::Decimal { .. }
        | DataType::Numeric { .. } => BufferDesc::Text {
            max_str_len: max_column_size,
        },
    }
}

pub fn buffered_column_values(
    slice: AnyColumnBufferSlice<'_>,
    binding: &ColumnBinding,
) -> std::result::Result<Vec<MssqlValueKind>, sqlx_core::Error> {
    let desc = binding.buffer_desc;
    Ok(match desc {
        BufferDesc::I8 { nullable } => buffered_numeric(&slice, desc, nullable, |value: i8| {
            MssqlValueKind::TinyInt(i16::from(value))
        })?,
        BufferDesc::I16 { nullable } => buffered_numeric(&slice, desc, nullable, |value| {
            MssqlValueKind::SmallInt(value)
        })?,
        BufferDesc::I32 { nullable } => buffered_numeric(&slice, desc, nullable, |value| {
            MssqlValueKind::Integer(value)
        })?,
        BufferDesc::I64 { nullable } => {
            buffered_numeric(&slice, desc, nullable, MssqlValueKind::BigInt)?
        }
        BufferDesc::U8 { nullable } => buffered_numeric(&slice, desc, nullable, |value: u8| {
            MssqlValueKind::BigInt(i64::from(value))
        })?,
        BufferDesc::F32 { nullable } => {
            buffered_numeric(&slice, desc, nullable, MssqlValueKind::Real)?
        }
        BufferDesc::F64 { nullable } => {
            buffered_numeric(&slice, desc, nullable, MssqlValueKind::Double)?
        }
        BufferDesc::Bit { nullable } => {
            buffered_numeric(&slice, desc, nullable, |value: odbc_api::Bit| {
                MssqlValueKind::Bit(value.as_bool())
            })?
        }
        BufferDesc::Date { nullable } => {
            buffered_numeric(&slice, desc, nullable, MssqlValueKind::Date)?
        }
        BufferDesc::Time { nullable } => {
            buffered_numeric(&slice, desc, nullable, MssqlValueKind::Time)?
        }
        BufferDesc::Timestamp { nullable } => {
            buffered_numeric(&slice, desc, nullable, MssqlValueKind::Timestamp)?
        }
        BufferDesc::Text { .. } => {
            let text = expect_buffer_slice(slice.as_text(), desc)?;
            text.iter()
                .map(|value| {
                    value
                        .map(|bytes| {
                            MssqlValueKind::Text(String::from_utf8_lossy(bytes).into_owned())
                        })
                        .unwrap_or(MssqlValueKind::Null)
                })
                .collect()
        }
        BufferDesc::WText { .. } => {
            let text = expect_buffer_slice(slice.as_wide_text(), desc)?;
            text.iter()
                .map(|value| {
                    value
                        .map(|chars| MssqlValueKind::Text(String::from_utf16_lossy(chars.into())))
                        .unwrap_or(MssqlValueKind::Null)
                })
                .collect()
        }
        BufferDesc::Binary { .. } => {
            let binary = expect_buffer_slice(slice.as_binary(), desc)?;
            binary
                .iter()
                .map(|value| {
                    value
                        .map(|bytes| MssqlValueKind::Binary(bytes.to_vec()))
                        .unwrap_or(MssqlValueKind::Null)
                })
                .collect()
        }
        BufferDesc::Numeric => {
            return Err(sqlx_core::Error::Protocol(format!(
                "unsupported ODBC buffer descriptor: {desc:?}"
            )));
        }
    })
}

pub fn buffered_numeric<T, F>(
    slice: &AnyColumnBufferSlice<'_>,
    desc: BufferDesc,
    nullable: bool,
    map: F,
) -> std::result::Result<Vec<MssqlValueKind>, sqlx_core::Error>
where
    T: Copy + odbc_api::Pod,
    F: FnMut(T) -> MssqlValueKind,
{
    if nullable {
        Ok(buffered_nullable_numeric(
            expect_buffer_slice(slice.as_nullable_slice::<T>(), desc)?,
            map,
        ))
    } else {
        Ok(expect_buffer_slice(slice.as_slice::<T>(), desc)?
            .iter()
            .copied()
            .map(map)
            .collect())
    }
}

pub fn buffered_nullable_numeric<T, F>(slice: NullableSlice<'_, T>, mut map: F) -> Vec<MssqlValueKind>
where
    T: Copy,
    F: FnMut(T) -> MssqlValueKind,
{
    slice
        .map(|value| value.copied().map(&mut map).unwrap_or(MssqlValueKind::Null))
        .collect()
}

pub fn expect_buffer_slice<T>(
    slice: Option<T>,
    desc: BufferDesc,
) -> std::result::Result<T, sqlx_core::Error> {
    slice.ok_or_else(|| {
        sqlx_core::Error::Protocol(format!(
            "ODBC column buffer {desc:?} did not match fetched slice"
        ))
    })
}

pub fn stream_rows_unbuffered<C>(
    cursor: &mut C,
    tx: &ExecuteSender,
) -> std::result::Result<bool, sqlx_core::Error>
where
    C: Cursor + ResultSetMetadata,
{
    let columns: Arc<[MssqlColumn]> = collect_columns(cursor)?.into();

    while let Some(mut cursor_row) = cursor.next_row().map_err(|error| {
        crate::error::database_error_with_context(
            error,
            "ODBC unbuffered fetch failed while reading the next row",
        )
    })? {
        let mut values = Vec::with_capacity(columns.len());

        for column in columns.iter() {
            let column_number = u16::try_from(sqlx_core::column::Column::ordinal(column) + 1)
                .map_err(|_| {
                    sqlx_core::Error::Protocol("ODBC column index exceeds u16".to_owned())
                })?;
            values.push(fetch_value(&mut cursor_row, column_number, column)?);
        }

        if !send_row(tx, MssqlRow::new_shared(Arc::clone(&columns), values)) {
            return Ok(false);
        }
    }

    send_done(tx, 0);
    Ok(true)
}

pub fn fetch_value(
    row: &mut odbc_api::CursorRow<'_>,
    column_number: u16,
    column: &MssqlColumn,
) -> std::result::Result<MssqlValue, sqlx_core::Error> {
    let data_type = column.type_info().data_type();

    let kind = match data_type {
        DataType::Bit => {
            let mut value = Nullable::<odbc_api::Bit>::null();
            row.get_data(column_number, &mut value).map_err(|error| {
                crate::error::database_error_with_context_lazy(error, || {
                    fetch_context(column, data_type)
                })
            })?;
            value
                .into_opt()
                .map(|value| MssqlValueKind::Bit(value.as_bool()))
                .unwrap_or(MssqlValueKind::Null)
        }
        DataType::TinyInt => {
            // MSSQL TINYINT is unsigned (0-255), so read as i16 to avoid
            // signed overflow of values > 127.
            let mut value = Nullable::<i16>::null();
            row.get_data(column_number, &mut value).map_err(|error| {
                crate::error::database_error_with_context_lazy(error, || {
                    fetch_context(column, data_type)
                })
            })?;
            value
                .into_opt()
                .map(MssqlValueKind::TinyInt)
                .unwrap_or(MssqlValueKind::Null)
        }
        DataType::SmallInt => fetch_nullable(
            row,
            column_number,
            column,
            data_type,
            MssqlValueKind::SmallInt,
        )?,
        DataType::Integer => fetch_nullable(
            row,
            column_number,
            column,
            data_type,
            MssqlValueKind::Integer,
        )?,
        DataType::BigInt => fetch_nullable(
            row,
            column_number,
            column,
            data_type,
            MssqlValueKind::BigInt,
        )?,
        DataType::Real => {
            fetch_nullable(row, column_number, column, data_type, MssqlValueKind::Real)?
        }
        DataType::Float { .. } | DataType::Double => fetch_nullable(
            row,
            column_number,
            column,
            data_type,
            MssqlValueKind::Double,
        )?,
        DataType::Date => {
            fetch_nullable(row, column_number, column, data_type, MssqlValueKind::Date)?
        }
        DataType::Time { .. } => {
            fetch_nullable(row, column_number, column, data_type, MssqlValueKind::Time)?
        }
        DataType::Timestamp { .. } => fetch_nullable(
            row,
            column_number,
            column,
            data_type,
            MssqlValueKind::Timestamp,
        )?,
        DataType::Binary { .. } | DataType::Varbinary { .. } | DataType::LongVarbinary { .. } => {
            let mut value = Vec::new();
            if row.get_binary(column_number, &mut value).map_err(|error| {
                crate::error::database_error_with_context_lazy(error, || {
                    fetch_context(column, data_type)
                })
            })? {
                MssqlValueKind::Binary(value)
            } else {
                MssqlValueKind::Null
            }
        }
        DataType::Other {
            data_type: sql_type,
            ..
        } if sql_type.0 == -11 => {
            // SQL_GUID / UNIQUEIDENTIFIER in MSSQL
            let mut value = Vec::new();
            if row.get_binary(column_number, &mut value).map_err(|error| {
                crate::error::database_error_with_context_lazy(error, || {
                    fetch_context(column, data_type)
                })
            })? {
                if value.len() == 16 {
                    let mut guid = [0u8; 16];
                    guid.copy_from_slice(&value);
                    MssqlValueKind::Guid(guid)
                } else {
                    // Fallback: treat GUID data as text
                    MssqlValueKind::Text(String::from_utf16_lossy(
                        &value.iter().map(|&b| b as u16).collect::<Vec<_>>(),
                    ))
                }
            } else {
                MssqlValueKind::Null
            }
        }
        _ => {
            let mut value = Vec::new();
            if row
                .get_wide_text(column_number, &mut value)
                .map_err(|error| {
                    crate::error::database_error_with_context_lazy(error, || {
                        fetch_context(column, data_type)
                    })
                })?
            {
                MssqlValueKind::Text(String::from_utf16_lossy(&value))
            } else {
                MssqlValueKind::Null
            }
        }
    };

    Ok(MssqlValue::new(kind))
}

pub fn fetch_nullable<T, F>(
    row: &mut odbc_api::CursorRow<'_>,
    column_number: u16,
    column: &MssqlColumn,
    data_type: DataType,
    map: F,
) -> std::result::Result<MssqlValueKind, sqlx_core::Error>
where
    T: Default + Copy + odbc_api::parameter::CElement + odbc_api::handles::CDataMut,
    Nullable<T>: odbc_api::parameter::CElement + odbc_api::handles::CDataMut,
    F: FnOnce(T) -> MssqlValueKind,
{
    let mut value = Nullable::<T>::null();
    row.get_data(column_number, &mut value).map_err(|error| {
        crate::error::database_error_with_context_lazy(error, || fetch_context(column, data_type))
    })?;
    Ok(value.into_opt().map(map).unwrap_or(MssqlValueKind::Null))
}

fn fetch_context(column: &MssqlColumn, data_type: DataType) -> String {
    format!(
        "failed to fetch ODBC column {} (`{}`) as {data_type:?}",
        column.ordinal() + 1,
        column.name()
    )
}

pub fn sql_preview(sql: &str) -> String {
    const MAX_LEN: usize = 160;

    let compact = sql.split_whitespace().collect::<Vec<_>>().join(" ");
    if compact.len() <= MAX_LEN {
        compact
    } else {
        let mut preview = compact.chars().take(MAX_LEN - 3).collect::<String>();
        preview.push_str("...");
        preview
    }
}

/// Offloads a blocking operation to Tokio's blocking thread pool.
///
/// The closure must satisfy `Send + 'static` so it can be moved across
/// threads.
#[allow(dead_code)]
#[cfg(feature = "runtime-tokio")]
pub(crate) async fn offload_blocking<F, T>(f: F) -> std::result::Result<T, sqlx_core::Error>
where
    F: FnOnce() -> std::result::Result<T, sqlx_core::Error> + Send + 'static,
    T: Send + 'static,
{
    tokio::task::spawn_blocking(f)
        .await
        .map_err(|e| sqlx_core::Error::Protocol(format!("blocking task panicked: {e}")))?
}
