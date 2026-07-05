//! The primitive type environment threaded through Pass 1.

use std::collections::HashMap;

use flowlog_parser::DataType;
use flowlog_parser::Program;

/// Relation name -> its declared column primitive types.
pub(crate) type DeclTypes = HashMap<String, Vec<DataType>>;

/// UDF name -> (declared params `(name, type)`, return type).
pub(crate) type UdfSigs = HashMap<String, (Vec<(String, DataType)>, DataType)>;

/// Everything Pass 1 needs about the program's declarations, built once.
pub(crate) struct PrimitiveEnv {
    pub(crate) decls: DeclTypes,
    pub(crate) udfs: UdfSigs,
}

impl PrimitiveEnv {
    pub(crate) fn from_program(program: &Program) -> Self {
        let decls = program
            .relations()
            .iter()
            .map(|r| (r.name().to_string(), r.data_type()))
            .collect();
        let udfs = program
            .udfs()
            .iter()
            .map(|u| {
                (
                    u.name().to_string(),
                    (
                        u.params()
                            .iter()
                            .map(|p| (p.name().to_string(), p.data_type().clone()))
                            .collect(),
                        u.ret_type(),
                    ),
                )
            })
            .collect();
        Self { decls, udfs }
    }
}
