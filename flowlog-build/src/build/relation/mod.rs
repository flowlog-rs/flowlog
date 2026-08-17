//! Library-mode relation codegen.
//!
//! Emits:
//!
//! - Per-EDB `{Name}Input` structs with inherent methods (no `RelOps` trait,
//!   no dynamic dispatch) — see [`handler`].
//! - A concrete `Inputs` container the library engine holds directly.
//! - User-facing `rel::Foo` tuple aliases — see [`user`].
//!
//! Binary mode has its own relation codegen in `flowlog-compiler`.

pub(crate) mod user;

use std::collections::HashMap;
use std::io;

use flowlog_parser::Program;
use flowlog_parser::Relation;
use proc_macro2::Ident;
use proc_macro2::TokenStream;
use quote::format_ident;

use crate::build::BuildError;
use crate::codegen::CodegenError;
use crate::codegen::Features;
use crate::codegen::input_struct_ident;

/// Emit the body of the library-mode `relops` module — EDB input handlers
/// + the `Inputs` container — plus the `use` lines they depend on.
pub(crate) fn gen_input_module(
    program: &Program,
    features: &Features,
    is_batch: bool,
    uses_ord: bool,
) -> Result<TokenStream, CodegenError> {
    // The same per-relation handlers the compiled binary uses: a
    // `RelationSpec` static, the runtime's `relation!` expansion, an
    // `Ingest` impl carrying any inline facts, and the `Inputs` container
    // that holds them.
    crate::codegen::gen_relation(program, features, is_batch, uses_ord)
}

// ------------------------------------------------------------
// Naming helpers — shared across relation codegen consumers
// ------------------------------------------------------------

/// Convert a snake_case / lowercase relation name to `PascalCase`.
pub(crate) fn pascal_case(name: &str) -> String {
    let mut out = String::with_capacity(name.len());
    let mut capitalize = true;
    for c in name.chars() {
        if c == '_' || c == '-' {
            capitalize = true;
            continue;
        }
        if capitalize {
            out.extend(c.to_uppercase());
            capitalize = false;
        } else {
            out.push(c);
        }
    }
    out
}

/// Field ident on the user-facing results structs (`BatchResults` /
/// `IncrementalResults`) and their typed locals: the canonical relation
/// name, verbatim — the published API contract (`results.<name>`).
/// Unrepresentable names were already rejected by [`validate_api_surface`].
pub(crate) fn results_field_ident(rel: &Relation) -> Ident {
    format_ident!("{}", rel.name())
}

/// Field ident for a `.printsize` relation on the results structs
/// (`<name>_size`). Single owner of the suffix so the generators and
/// [`validate_api_surface`] can never desync.
pub(crate) fn printsize_field_ident(rel: &Relation) -> Ident {
    format_ident!("{}_size", rel.name())
}

/// Ident for the user-facing struct generated from a relation (e.g. `Edge`).
pub(crate) fn user_struct_ident(rel: &Relation) -> Ident {
    format_ident!("{}", pascal_case(rel.name()))
}

// ------------------------------------------------------------
// API-surface validation
// ------------------------------------------------------------

/// Reject programs whose library API cannot be generated faithfully.
///
/// The lib-mode API mirrors relation names: results fields are the
/// canonical name verbatim, `rel::` aliases are its PascalCase. Both are
/// emitted without escaping, so two failure modes exist and both are
/// rejected here with an actionable message instead of surfacing as a
/// rustc error inside generated code:
///
/// - a name no plain Rust ident can carry (a keyword: `.output Type`
///   lowers to field `type`);
/// - two relations whose names collapse to one ident (`foo_bar` and
///   `foo__bar` both pascal-case to `FooBar`).
///
/// Everything else about a keyword-named relation keeps working — EDB
/// surfaces are prefixed/suffixed (`insert_type`, `TypeInput`,
/// `type_size`), internal bindings are synthetic, and binary mode has no
/// API fields at all.
pub(crate) fn validate_api_surface(program: &Program) -> Result<(), BuildError> {
    // Results-struct field namespace: `.output` fields + `.printsize`
    // `<name>_size` fields live in the same struct.
    let mut fields: HashMap<String, String> = HashMap::new();
    for rel in program.output_idbs() {
        ensure_plain_ident(rel.name(), rel.raw_name(), "a results field")?;
        ensure_unique(
            &mut fields,
            rel.name().to_string(),
            rel.raw_name(),
            "results field",
        )?;
    }
    for rel in program.printsize_idbs() {
        let field = printsize_field_ident(rel).to_string();
        ensure_unique(&mut fields, field, rel.raw_name(), "results field")?;
    }

    // `rel::` alias namespace — iterate the *same* set the generator
    // emits ([`user::collect_user_rels`]) so the two can never desync.
    let mut aliases: HashMap<String, String> = HashMap::new();
    for rel in user::collect_user_rels(program) {
        let stem = pascal_case(rel.name());
        ensure_plain_ident(&stem, rel.raw_name(), "a `rel::` type alias")?;
        ensure_unique(&mut aliases, stem, rel.raw_name(), "`rel::` type alias")?;
    }

    // `<Pascal>Input` struct namespace — one handler struct per EDB
    // (nullary included), see [`handler`].
    let mut input_structs: HashMap<String, String> = HashMap::new();
    for rel in program.edbs() {
        let stem = input_struct_ident(rel).to_string();
        ensure_unique(
            &mut input_structs,
            stem,
            rel.raw_name(),
            "input-handler struct",
        )?;
    }

    Ok(())
}

/// `name` must be usable as a *plain* Rust ident (no `r#`, no escaping) —
/// `syn`'s ident parser is the authority, so there is no keyword list to
/// maintain.
fn ensure_plain_ident(name: &str, raw_name: &str, what: &str) -> Result<(), BuildError> {
    if syn::parse_str::<syn::Ident>(name).is_err() {
        return Err(BuildError::from(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "relation `{raw_name}` cannot be exposed through the library API: \
                 `{name}` is not usable as {what} (it is a Rust keyword) — rename \
                 the relation, or drop its `.output`/`.printsize` directive"
            ),
        )));
    }
    Ok(())
}

/// Two distinct relations must never collapse onto one generated ident —
/// that would emit duplicate fields/aliases and fail in rustc with an
/// error pointing at generated code.
fn ensure_unique(
    owners: &mut HashMap<String, String>,
    ident: String,
    raw_name: &str,
    what: &str,
) -> Result<(), BuildError> {
    if let Some(prev) = owners.insert(ident.clone(), raw_name.to_string()) {
        return Err(BuildError::from(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "relations `{prev}` and `{raw_name}` would both surface as the \
                 {what} `{ident}` in the generated library API — rename one of them"
            ),
        )));
    }
    Ok(())
}

// ------------------------------------------------------------
// Preamble
// ------------------------------------------------------------

#[cfg(test)]
mod api_surface_tests {
    use std::collections::HashMap;

    use super::ensure_plain_ident;
    use super::ensure_unique;
    use super::pascal_case;

    /// Representative keywords across the strict / reserved / non-raw-able
    /// classes must be rejected as verbatim API field names. `syn`'s ident
    /// parser is the authority for the full set — no keyword list to
    /// maintain here.
    #[test]
    fn keywords_are_rejected_as_plain_idents() {
        for kw in [
            "type", "match", "in", "loop", "self", "Self", "crate", "super", "yield", "try",
        ] {
            assert!(
                ensure_plain_ident(kw, kw, "a results field").is_err(),
                "keyword {kw:?} should be rejected as a verbatim API ident"
            );
        }
    }

    /// Ordinary names — including the underscore-twins that used to collide
    /// under escape-based handling — are accepted verbatim.
    #[test]
    fn ordinary_names_are_accepted() {
        for name in ["varpointsto", "method_lookup", "crate_", "self_", "type_"] {
            assert!(
                ensure_plain_ident(name, name, "a results field").is_ok(),
                "name {name:?} should be accepted"
            );
        }
    }

    /// Distinct relations collapsing onto one ident are rejected with both
    /// owners named.
    #[test]
    fn duplicate_idents_are_rejected() {
        let mut owners = HashMap::new();
        ensure_unique(&mut owners, "x_size".into(), "x_size", "results field").unwrap();
        // `.printsize x` also wants the `x_size` field.
        let err = ensure_unique(&mut owners, "x_size".into(), "x", "results field").unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("x_size") && msg.contains('x'), "{msg}");
    }

    /// PascalCase collapses `foo_bar` / `foo__bar`, and turns `self` into
    /// the un-emittable `Self` — both must be caught by the same guards.
    #[test]
    fn pascal_namespace_hazards_are_caught() {
        assert_eq!(pascal_case("foo_bar"), pascal_case("foo__bar"));
        let mut owners = HashMap::new();
        ensure_unique(
            &mut owners,
            pascal_case("foo_bar"),
            "foo_bar",
            "`rel::` type alias",
        )
        .unwrap();
        assert!(
            ensure_unique(
                &mut owners,
                pascal_case("foo__bar"),
                "foo__bar",
                "`rel::` type alias"
            )
            .is_err()
        );
        assert_eq!(pascal_case("self"), "Self");
        assert!(ensure_plain_ident(&pascal_case("self"), "self", "a `rel::` type alias").is_err());
    }
}
