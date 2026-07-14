//! Declaration lookups for primitive type checking, gathered once from the
//! program.
//!
//! Checking a rule repeatedly needs a relation's declared column types (to
//! bind and check its variables) and a UDF's signature (to check its call
//! arguments). [`PrimitiveEnv`] holds both as by-name maps built up front, so
//! no checker re-walks the program's declarations.

use std::collections::HashMap;

use crate::DataType;
use crate::Program;

/// Relation name -> its declared column primitive types.
pub(crate) type DeclTypes = HashMap<String, Vec<DataType>>;

/// UDF name -> (declared params `(name, type)`, return type).
pub(crate) type UdfSigs = HashMap<String, (Vec<(String, DataType)>, DataType)>;

/// The relation and UDF lookups, keyed by name.
pub(crate) struct PrimitiveEnv {
    pub(crate) decls: DeclTypes,
    pub(crate) udfs: UdfSigs,
}

impl PrimitiveEnv {
    /// Snapshot `program`'s relation column types and UDF signatures into the
    /// lookup maps.
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
