//! Codegen feature tracking for the FlowLog compiler.
//!
//! `Features` records which capabilities are needed by the current
//! compilation unit so that downstream passes (imports, scaffold, type
//! declarations) emit only what is required.

// =========================================================================
// Features
// =========================================================================

/// Simple boolean mark/query pairs.
macro_rules! bool_features {
    ($(($field:ident, $marker:ident)),* $(,)?) => {
        $(
            #[inline]
            pub fn $field(&self) -> bool { self.$field }
            #[inline]
            pub(crate) fn $marker(&mut self) { self.$field = true; }
        )*
    };
}

/// Tracks which codegen features are active for the current compilation unit.
#[must_use]
#[derive(Default, Clone)]
pub struct Features {
    // -- differential-dataflow / timely --
    dd_input: bool,
    // -- dataflow features --
    recursive: bool,
    // -- library support --
    string_intern: bool,
    string_resolve: bool,
    string_resolve_out: bool,
    ordered_float: bool,
    udf: bool,
    output_buffers: bool,
    parallel_output: bool,
    itoa: bool,
}

impl Features {
    /// Clears all flags so a new compilation unit starts from scratch.
    pub(crate) fn reset(&mut self) {
        *self = Self::default();
    }

    // -- boolean features ---------------------------------------------

    bool_features! {
        // dd / timely
        (dd_input,       mark_dd_input),
        // dataflow
        (recursive,      mark_recursive),
        (string_intern,  mark_string_intern),
        (string_resolve, mark_string_resolve),
        (string_resolve_out, mark_string_resolve_out),
        (ordered_float,  mark_ordered_float),
        (udf,            mark_udf),
        (output_buffers, mark_output_buffers),
        // parallel file-output drain (binary mode only); the scaffold gates
        // the `rayon` dependency on this, and `itoa` on having integer columns
        // on that path.
        (parallel_output, mark_parallel_output),
        (itoa,           mark_itoa),
    }
}
