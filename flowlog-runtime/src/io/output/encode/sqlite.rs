//! SQLite column layouts and parameter binding from borrowed dataflow values.

use lasso::Spur;
use ordered_float::OrderedFloat;
use rusqlite::Statement;
use rusqlite::ToSql;

use crate::error::RuntimeError;
use crate::intern::resolve_out;

/// Binds fields in declaration order. Text is borrowed until SQLite copies it
/// into statement storage; no intermediate owned text is created.
#[derive(Debug)]
pub struct SqliteEncoder<'borrow, 'stmt> {
    statement: &'borrow mut Statement<'stmt>,
    columns: usize,
}

impl<'borrow, 'stmt> SqliteEncoder<'borrow, 'stmt> {
    pub fn new(statement: &'borrow mut Statement<'stmt>) -> Self {
        Self {
            statement,
            columns: 0,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.columns == 0
    }

    pub fn bind(&mut self, value: impl ToSql) -> Result<(), RuntimeError> {
        self.statement.raw_bind_parameter(self.columns + 1, value)?;
        self.columns += 1;
        Ok(())
    }
}

/// Encodes scalar leaves in declaration order. Integers must fit signed
/// 64-bit storage and floats must be finite. Strings preserve their bytes.
pub trait EncodeField {
    fn columns(name: &str, columns: &mut Vec<(String, &'static str)>);
    fn write(&self, encoder: &mut SqliteEncoder<'_, '_>) -> Result<(), RuntimeError>;
}

/// Names top-level attributes and flattens nested fields into dot-separated
/// tuple indices, such as `point.0` and `point.1.0`.
pub trait EncodeRow: EncodeField {
    fn schema(names: &[&str]) -> Result<Vec<(String, &'static str)>, RuntimeError>;
}

macro_rules! integers {
    ($($ty:ty),+ $(,)?) => {$(
        impl EncodeField for $ty {
            fn columns(name: &str, columns: &mut Vec<(String, &'static str)>) { columns.push((name.to_owned(), "INTEGER")); }
            fn write(&self, encoder: &mut SqliteEncoder<'_, '_>) -> Result<(), RuntimeError> {
                let value = i64::try_from(*self).map_err(|_| RuntimeError::SqliteValue {
                    column: encoder.columns, value: self.to_string(), expected: "a signed 64-bit SQLite integer",
                })?;
                encoder.bind(value)
            }
        }
    )+};
}
integers!(i8, i16, i32, i64, u8, u16, u32, u64);

impl EncodeField for bool {
    fn columns(name: &str, columns: &mut Vec<(String, &'static str)>) {
        columns.push((name.to_owned(), "INTEGER"));
    }
    fn write(&self, encoder: &mut SqliteEncoder<'_, '_>) -> Result<(), RuntimeError> {
        encoder.bind(i64::from(*self))
    }
}

impl EncodeField for String {
    fn columns(name: &str, columns: &mut Vec<(String, &'static str)>) {
        columns.push((name.to_owned(), "TEXT"));
    }
    fn write(&self, encoder: &mut SqliteEncoder<'_, '_>) -> Result<(), RuntimeError> {
        encoder.bind(self.as_str())
    }
}

impl EncodeField for Spur {
    fn columns(name: &str, columns: &mut Vec<(String, &'static str)>) {
        String::columns(name, columns);
    }
    fn write(&self, encoder: &mut SqliteEncoder<'_, '_>) -> Result<(), RuntimeError> {
        encoder.bind(resolve_out(*self))
    }
}

macro_rules! floats {
    ($($ty:ty),+ $(,)?) => {$(
        impl EncodeField for OrderedFloat<$ty> {
            fn columns(name: &str, columns: &mut Vec<(String, &'static str)>) { columns.push((name.to_owned(), "REAL")); }
            fn write(&self, encoder: &mut SqliteEncoder<'_, '_>) -> Result<(), RuntimeError> {
                if !self.0.is_finite() {
                    return Err(RuntimeError::SqliteValue { column: encoder.columns, value: self.to_string(), expected: "a finite float" });
                }
                encoder.bind(f64::from(self.0))
            }
        }
    )+};
}
floats!(f32, f64);

impl EncodeField for () {
    fn columns(_name: &str, _columns: &mut Vec<(String, &'static str)>) {}
    fn write(&self, _encoder: &mut SqliteEncoder<'_, '_>) -> Result<(), RuntimeError> {
        Ok(())
    }
}

impl EncodeRow for () {
    fn schema(names: &[&str]) -> Result<Vec<(String, &'static str)>, RuntimeError> {
        if !names.is_empty() {
            return Err(RuntimeError::SqliteSchema(format!(
                "{} attribute names for a nullary row",
                names.len()
            )));
        }
        Ok(Vec::new())
    }
}

macro_rules! tuples {
    ($(($($field:ident . $index:tt),+))+) => {$(
        impl<$($field: EncodeField,)+> EncodeRow for ($($field,)+) {
            fn schema(names: &[&str]) -> Result<Vec<(String, &'static str)>, RuntimeError> {
                let arity = [$(stringify!($field)),+].len();
                if names.len() != arity {
                    return Err(RuntimeError::SqliteSchema(format!("{} attribute names for {arity} columns", names.len())));
                }
                let mut columns = Vec::new();
                $($field::columns(names[$index], &mut columns);)+
                Ok(columns)
            }
        }

        impl<$($field: EncodeField,)+> EncodeField for ($($field,)+) {
            fn columns(name: &str, columns: &mut Vec<(String, &'static str)>) {
                $($field::columns(&format!("{name}.{}", $index), columns);)+
            }
            fn write(&self, encoder: &mut SqliteEncoder<'_, '_>) -> Result<(), RuntimeError> {
                $(self.$index.write(encoder)?;)+
                Ok(())
            }
        }
    )+};
}
tuples! {
    (F0.0)
    (F0.0, F1.1)
    (F0.0, F1.1, F2.2)
    (F0.0, F1.1, F2.2, F3.3)
    (F0.0, F1.1, F2.2, F3.3, F4.4)
    (F0.0, F1.1, F2.2, F3.3, F4.4, F5.5)
    (F0.0, F1.1, F2.2, F3.3, F4.4, F5.5, F6.6)
    (F0.0, F1.1, F2.2, F3.3, F4.4, F5.5, F6.6, F7.7)
    (F0.0, F1.1, F2.2, F3.3, F4.4, F5.5, F6.6, F7.7, F8.8)
    (F0.0, F1.1, F2.2, F3.3, F4.4, F5.5, F6.6, F7.7, F8.8, F9.9)
    (F0.0, F1.1, F2.2, F3.3, F4.4, F5.5, F6.6, F7.7, F8.8, F9.9, F10.10)
    (F0.0, F1.1, F2.2, F3.3, F4.4, F5.5, F6.6, F7.7, F8.8, F9.9, F10.10, F11.11)
}

#[cfg(test)]
mod tests {
    use rstest::rstest;
    use rusqlite::Connection;

    use super::*;
    use crate::intern::intern;

    #[test]
    fn unsigned_overflow_is_rejected_at_its_column() {
        let connection = Connection::open_in_memory().unwrap();
        let mut statement = connection.prepare("SELECT ?, ?").unwrap();

        let error = (7i32, u64::MAX)
            .write(&mut SqliteEncoder::new(&mut statement))
            .unwrap_err();

        assert!(matches!(error, RuntimeError::SqliteValue {
            column: 1, value, expected: "a signed 64-bit SQLite integer",
        } if value == "18446744073709551615"));
    }

    #[rstest]
    #[case(f64::NAN, "NaN")]
    #[case(f64::INFINITY, "inf")]
    #[case(f64::NEG_INFINITY, "-inf")]
    fn nonfinite_floats_are_rejected(#[case] value: f64, #[case] expected: &str) {
        let connection = Connection::open_in_memory().unwrap();
        let mut statement = connection.prepare("SELECT ?").unwrap();

        let error = (OrderedFloat(value),)
            .write(&mut SqliteEncoder::new(&mut statement))
            .unwrap_err();

        assert!(matches!(error, RuntimeError::SqliteValue {
            column: 0, value, expected: "a finite float",
        } if value == expected));
    }

    #[test]
    fn repeated_bindings_replace_text_and_outlive_source_rows() {
        let connection = Connection::open_in_memory().unwrap();
        connection
            .execute_batch("CREATE TABLE Rows(owned TEXT, interned TEXT)")
            .unwrap();
        let mut statement = connection
            .prepare("INSERT INTO Rows VALUES (?, ?)")
            .unwrap();
        for (owned, interned) in [("a long first string", "first"), ("x\0y", "next"), ("", "")] {
            {
                let row = (owned.to_owned(), intern(interned));
                row.write(&mut SqliteEncoder::new(&mut statement)).unwrap();
            }
            statement.raw_execute().unwrap();
        }
        let mut query = connection
            .prepare("SELECT owned, interned FROM Rows ORDER BY rowid")
            .unwrap();
        let rows = query
            .query_map([], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
            })
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(
            rows,
            vec![
                ("a long first string".to_owned(), "first".to_owned()),
                ("x\0y".to_owned(), "next".to_owned()),
                (String::new(), String::new()),
            ]
        );
    }
}
