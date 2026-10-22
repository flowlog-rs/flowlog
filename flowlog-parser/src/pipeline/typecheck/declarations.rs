//! Declaration lookups for type checking, gathered once from the program.
//!
//! Checking a rule repeatedly needs a relation's declared column types (to
//! constrain its variables) and a UDF's signature (to check its call
//! arguments). [`Declarations`] holds both as by-name maps built up front, so
//! no checker re-walks the program's declarations.

use std::collections::HashMap;

use crate::DataType;
use crate::Program;

/// Relation name -> its declared column types.
pub(crate) type RelationTypes = HashMap<String, Vec<DataType>>;

/// UDF name -> (declared params `(name, type)`, return type).
pub(crate) type UdfSigs = HashMap<String, (Vec<(String, DataType)>, DataType)>;

/// The relation and UDF lookups, keyed by name.
pub(crate) struct Declarations {
    pub(crate) relations: RelationTypes,
    pub(crate) udfs: UdfSigs,
}

impl Declarations {
    /// Snapshot `program`'s relation column types and UDF signatures into the
    /// lookup maps.
    pub(crate) fn from_program(program: &Program) -> Self {
        let relations = program
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
        Self { relations, udfs }
    }
}
