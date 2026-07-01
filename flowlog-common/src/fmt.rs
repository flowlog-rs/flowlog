//! Output formatting helpers: Rust token pretty-printing and the horizontal
//! bars used in diagnostic dumps (catalog / stratum reports).

/// Horizontal section bar (80 `=`). Use as outer boundary of multi-section
/// reports, e.g. the catalog dump or a stratum report.
pub const SECTION_BAR: &str =
    "================================================================================";

/// Horizontal subsection bar (40 `-`). Use between named subsections inside
/// a report bounded by [`SECTION_BAR`].
pub const SUBSECTION_BAR: &str = "----------------------------------------";

/// Parse a `TokenStream` into a `syn::File` and pretty-print it via `prettyplease`.
pub fn pretty_print(ts: proc_macro2::TokenStream) -> String {
    let ast: syn::File = syn::parse2(ts).expect("valid token stream");
    prettyplease::unparse(&ast)
}
