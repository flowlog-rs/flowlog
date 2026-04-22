//! Horizontal bars used in diagnostic dumps (catalog / stratum reports).

/// Horizontal section bar (80 `=`). Use as outer boundary of multi-section
/// reports, e.g. the catalog dump or a stratum report.
pub const SECTION_BAR: &str =
    "================================================================================";

/// Horizontal subsection bar (40 `-`). Use between named subsections inside
/// a report bounded by [`SECTION_BAR`].
pub(crate) const SUBSECTION_BAR: &str = "----------------------------------------";
