//! SQLite cell validation and direct conversion into dataflow tuples.

use lasso::Spur;
use ordered_float::OrderedFloat;
use rusqlite::Row as SqlRow;
use rusqlite::types::ValueRef;

use crate::error::RuntimeError;
use crate::intern::intern;
use crate::io::input::decode::Decode;

/// Reads validated dataflow values directly from SQLite columns.
/// NULL, BLOB, invalid UTF-8, non-finite floats, and numeric overflow
/// are rejected. Booleans accept only integer 0 or 1.
pub trait ReadSqlite: Sized {
    const COLUMNS: usize;
    fn read(row: &SqlRow<'_>, column: &mut usize) -> Result<Self, RuntimeError>;
}

/// Borrows the current SQLite result until it is converted into an owned tuple.
#[derive(Debug)]
pub struct SqliteRow<'row, 'stmt>(pub &'row SqlRow<'stmt>);

impl<T: ReadSqlite> Decode<SqliteRow<'_, '_>> for T {
    #[inline]
    fn decode(row: &SqliteRow<'_, '_>) -> Result<Self, RuntimeError> {
        Self::read(row.0, &mut 0)
    }
}

/// Borrows the next cell, advancing only after its index is known to exist.
#[inline]
fn cell<'a>(
    row: &'a SqlRow<'_>,
    column: &mut usize,
) -> Result<(usize, ValueRef<'a>), RuntimeError> {
    let index = *column;
    let value = row.get_ref(index)?;
    *column += 1;
    Ok((index, value))
}

fn refused(column: usize, value: ValueRef<'_>, expected: &'static str) -> RuntimeError {
    let value = match value {
        ValueRef::Null => "NULL".to_owned(),
        ValueRef::Integer(value) => value.to_string(),
        ValueRef::Real(value) => value.to_string(),
        ValueRef::Text(value) => String::from_utf8_lossy(value).into_owned(),
        ValueRef::Blob(value) => format!("BLOB ({} bytes)", value.len()),
    };
    RuntimeError::SqliteValue {
        column,
        value,
        expected,
    }
}

macro_rules! integers {
    ($($ty:ty),+ $(,)?) => {$(
        impl ReadSqlite for $ty {
            const COLUMNS: usize = 1;
            #[inline]
            fn read(row: &SqlRow<'_>, column: &mut usize) -> Result<Self, RuntimeError> {
                let (index, value) = cell(row, column)?;
                if let ValueRef::Integer(value) = value {
                    return Self::try_from(value).map_err(|_| refused(index, ValueRef::Integer(value), stringify!($ty)));
                }
                Err(refused(index, value, stringify!($ty)))
            }
        }
    )+};
}
integers!(i8, i16, i32, i64, u8, u16, u32, u64);

impl ReadSqlite for bool {
    const COLUMNS: usize = 1;
    fn read(row: &SqlRow<'_>, column: &mut usize) -> Result<Self, RuntimeError> {
        let (index, value) = cell(row, column)?;
        match value {
            ValueRef::Integer(0) => Ok(false),
            ValueRef::Integer(1) => Ok(true),
            value => Err(refused(index, value, "boolean integer 0 or 1")),
        }
    }
}

fn text<'a>(row: &'a SqlRow<'_>, column: &mut usize) -> Result<&'a str, RuntimeError> {
    let (index, value) = cell(row, column)?;
    if let ValueRef::Text(bytes) = value {
        return std::str::from_utf8(bytes).map_err(|_| refused(index, value, "UTF-8 text"));
    }
    Err(refused(index, value, "UTF-8 text"))
}

impl ReadSqlite for String {
    const COLUMNS: usize = 1;
    fn read(row: &SqlRow<'_>, column: &mut usize) -> Result<Self, RuntimeError> {
        text(row, column).map(str::to_owned)
    }
}

impl ReadSqlite for Spur {
    const COLUMNS: usize = 1;
    fn read(row: &SqlRow<'_>, column: &mut usize) -> Result<Self, RuntimeError> {
        text(row, column).map(intern)
    }
}

macro_rules! floats {
    ($($ty:ty),+ $(,)?) => {$(
        impl ReadSqlite for OrderedFloat<$ty> {
            const COLUMNS: usize = 1;
            #[inline]
            fn read(row: &SqlRow<'_>, column: &mut usize) -> Result<Self, RuntimeError> {
                let (index, value) = cell(row, column)?;
                let number = match value {
                    ValueRef::Real(number) => number as $ty,
                    ValueRef::Integer(number) => number as $ty,
                    ValueRef::Null | ValueRef::Text(_) | ValueRef::Blob(_) => {
                        return Err(refused(index, value, "a finite float"));
                    }
                };
                if !number.is_finite() { return Err(refused(index, value, "a finite float")); }
                Ok(OrderedFloat(number))
            }
        }
    )+};
}
floats!(f32, f64);

impl ReadSqlite for () {
    const COLUMNS: usize = 0;
    fn read(_row: &SqlRow<'_>, _column: &mut usize) -> Result<Self, RuntimeError> {
        Ok(())
    }
}

macro_rules! tuples {
    ($(($($field:ident),+))+) => {$(
        impl<$($field: ReadSqlite,)+> ReadSqlite for ($($field,)+) {
            const COLUMNS: usize = 0 $(+ $field::COLUMNS)+;
            #[inline]
            fn read(row: &SqlRow<'_>, column: &mut usize) -> Result<Self, RuntimeError> {
                Ok(($($field::read(row, column)?,)+))
            }
        }
    )+};
}
tuples! {
    (F0)
    (F0, F1)
    (F0, F1, F2)
    (F0, F1, F2, F3)
    (F0, F1, F2, F3, F4)
    (F0, F1, F2, F3, F4, F5)
    (F0, F1, F2, F3, F4, F5, F6)
    (F0, F1, F2, F3, F4, F5, F6, F7)
    (F0, F1, F2, F3, F4, F5, F6, F7, F8)
    (F0, F1, F2, F3, F4, F5, F6, F7, F8, F9)
    (F0, F1, F2, F3, F4, F5, F6, F7, F8, F9, F10)
    (F0, F1, F2, F3, F4, F5, F6, F7, F8, F9, F10, F11)
}

#[cfg(test)]
mod tests {
    use rstest::rstest;
    use rusqlite::Connection;
    use rusqlite::types::Value;

    use super::*;
    use crate::error::RuntimeError as Error;
    use crate::intern::resolve_out;

    /// Owned decoding hides the source pointer, so this tests text validation.
    #[test]
    fn text_validation_borrows_sqlite_storage() {
        let connection = Connection::open_in_memory().unwrap();
        let mut statement = connection.prepare("SELECT 'owned', 'interned'").unwrap();
        let mut rows = statement.query([]).unwrap();
        let row = rows.next().unwrap().unwrap();

        let mut column = 0;
        let fields = (
            text(row, &mut column).unwrap(),
            text(row, &mut column).unwrap(),
        );

        assert_eq!(fields, ("owned", "interned"));
        assert_eq!(
            fields.0.as_ptr(),
            row.get_ref(0).unwrap().as_str().unwrap().as_ptr()
        );
        assert_eq!(
            fields.1.as_ptr(),
            row.get_ref(1).unwrap().as_str().unwrap().as_ptr()
        );
    }

    #[test]
    fn decoded_strings_survive_advancing_and_closing_the_query() {
        let decoded = {
            let connection = Connection::open_in_memory().unwrap();
            let mut statement = connection
                .prepare("SELECT 'first', 'interned' UNION ALL SELECT 'next', 'other'")
                .unwrap();
            let mut rows = statement.query([]).unwrap();
            let row = rows.next().unwrap().unwrap();
            let decoded = <(String, Spur)>::decode(&SqliteRow(row)).unwrap();
            assert!(rows.next().unwrap().is_some());
            decoded
        };

        assert_eq!(decoded.0, "first");
        assert_eq!(resolve_out(decoded.1), "interned");
    }
    #[rstest]
    #[case(Value::Null, "NULL")]
    #[case(Value::Blob(vec![0]), "BLOB (1 bytes)")]
    #[case(Value::Integer(128), "128")]
    #[case(Value::Text("7".to_owned()), "7")]
    #[case(Value::Real(1.5), "1.5")]
    fn invalid_integer_cells_are_rejected(#[case] value: Value, #[case] expected: &str) {
        let connection = Connection::open_in_memory().unwrap();
        let mut query = connection.prepare("SELECT ?").unwrap();
        let mut rows = query.query([value]).unwrap();
        let error = <(i8,)>::decode(&SqliteRow(rows.next().unwrap().unwrap())).unwrap_err();
        assert!(
            matches!(error, Error::SqliteValue { column: 0, value, expected: "i8" } if value == expected)
        );
    }

    #[rstest]
    #[case("SELECT 2", "boolean integer 0 or 1")]
    #[case("SELECT NULL", "boolean integer 0 or 1")]
    fn boolean_input_requires_zero_or_one(#[case] sql: &str, #[case] message: &str) {
        let connection = Connection::open_in_memory().unwrap();
        let mut query = connection.prepare(sql).unwrap();
        let mut rows = query.query([]).unwrap();
        let error = <(bool,)>::decode(&SqliteRow(rows.next().unwrap().unwrap())).unwrap_err();
        assert!(
            matches!(error, Error::SqliteValue { column: 0, expected, .. } if expected == message)
        );
    }

    #[rstest]
    #[case(1)]
    #[case(usize::MAX)]
    fn invalid_column_cursors_return_errors_without_advancing(#[case] index: usize) {
        let connection = Connection::open_in_memory().unwrap();
        let mut statement = connection.prepare("SELECT 7").unwrap();
        let mut rows = statement.query([]).unwrap();
        let row = rows.next().unwrap().unwrap();
        let mut column = index;

        let error = i64::read(row, &mut column).unwrap_err();

        assert!(
            matches!(error, RuntimeError::Sqlite(rusqlite::Error::InvalidColumnIndex(actual)) if actual == index)
        );
        assert_eq!(column, index);
    }

    #[test]
    fn invalid_utf8_text_is_rejected_before_interning() {
        let connection = Connection::open_in_memory().unwrap();
        let mut statement = connection.prepare("SELECT CAST(X'80' AS TEXT)").unwrap();
        let mut rows = statement.query([]).unwrap();

        let error = <(Spur,)>::decode(&SqliteRow(rows.next().unwrap().unwrap())).unwrap_err();

        assert!(matches!(error, RuntimeError::SqliteValue {
            column: 0, value, expected: "UTF-8 text"
        } if value == "\u{fffd}"));
    }

    #[rstest]
    #[case("SELECT 1e100")]
    #[case("SELECT 1e999")]
    fn float_input_rejects_nonfinite_results_after_narrowing(#[case] sql: &str) {
        let connection = Connection::open_in_memory().unwrap();
        let mut statement = connection.prepare(sql).unwrap();
        let mut rows = statement.query([]).unwrap();

        let error =
            <(OrderedFloat<f32>,)>::decode(&SqliteRow(rows.next().unwrap().unwrap())).unwrap_err();

        assert!(matches!(
            error,
            RuntimeError::SqliteValue {
                column: 0,
                expected: "a finite float",
                ..
            }
        ));
    }
}
