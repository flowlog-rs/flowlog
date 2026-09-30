//! The program's boundary: [`relation`] declares each relation and the
//! `Inputs` container of its loaders, [`input`] the dataflow's input
//! collections and handles, and [`output`] the inspectors and emitters its
//! outputs go through.

pub(crate) mod input;
pub(crate) mod output;
pub(crate) mod relation;
