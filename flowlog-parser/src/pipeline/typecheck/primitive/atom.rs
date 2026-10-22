//! Bind, check, and pin an atom's arguments against their declared columns.

use crate::Atom;
use crate::AtomArg;
use crate::DataType;
use crate::ParseError;
use crate::error::grammar_bug;
use crate::pipeline::typecheck::env::DeclTypes;
use crate::pipeline::typecheck::primitive::Bindings;

/// Bind the variables of a positive atom. A positive atom is a binding site:
/// each variable takes its column's type, and a later use at a different type
/// is a `TypeMismatch`. Const arguments are only checked for family fit here,
/// not pinned; [`pin_atom`] does that, so a rule's whole binding map is built
/// before any argument is mutated.
pub(super) fn bind_atom(
    atom: &Atom,
    decls: &DeclTypes,
    bindings: &mut Bindings,
) -> Result<(), ParseError> {
    for (arg, col_ty) in atom.arguments().iter().zip(declared_columns(atom, decls)?) {
        match arg {
            AtomArg::Var(v) => match bindings.get(v) {
                None => {
                    bindings.insert(v.clone(), (col_ty.clone(), atom.span()));
                }
                Some((first_ty, first_span)) if first_ty != col_ty => {
                    return Err(ParseError::TypeMismatch {
                        var: v.clone(),
                        first_ty: first_ty.clone(),
                        first_span: *first_span,
                        later_ty: col_ty.clone(),
                        later_span: atom.span(),
                    });
                }
                Some(_) => {}
            },
            AtomArg::Const(c) => {
                if !c.ty().fits(col_ty) {
                    return Err(ParseError::LiteralColumnMismatch {
                        span: atom.span(),
                        literal: c.to_string(),
                        expected: col_ty.clone(),
                    });
                }
            }
            AtomArg::Placeholder => {}
        }
    }
    Ok(())
}

/// Check the arguments of a negative atom. A negative atom is not a binding
/// site, so it introduces nothing: a variable already bound by a positive atom
/// must match this column's type, while an unbound one is left for the
/// range-restriction pass. Const arguments are checked for family fit, as in
/// [`bind_atom`].
pub(super) fn check_atom(
    atom: &Atom,
    decls: &DeclTypes,
    bindings: &Bindings,
) -> Result<(), ParseError> {
    for (arg, col_ty) in atom.arguments().iter().zip(declared_columns(atom, decls)?) {
        match arg {
            AtomArg::Var(v) => {
                if let Some((bound_ty, bound_span)) = bindings.get(v)
                    && bound_ty != col_ty
                {
                    return Err(ParseError::TypeMismatch {
                        var: v.clone(),
                        first_ty: bound_ty.clone(),
                        first_span: *bound_span,
                        later_ty: col_ty.clone(),
                        later_span: atom.span(),
                    });
                }
            }
            AtomArg::Const(c) => {
                if !c.ty().fits(col_ty) {
                    return Err(ParseError::LiteralColumnMismatch {
                        span: atom.span(),
                        literal: c.to_string(),
                        expected: col_ty.clone(),
                    });
                }
            }
            AtomArg::Placeholder => {}
        }
    }
    Ok(())
}

/// Pin every polymorphic const argument of `atom` to its declared column
/// type. Call after [`bind_atom`] / [`check_atom`] has already validated the
/// family fit.
pub(super) fn pin_atom(atom: &mut Atom, decls: &DeclTypes) -> Result<(), ParseError> {
    let span = atom.span();
    let col_types: Vec<DataType> = {
        let Some(decl) = decls.get(atom.name()) else {
            return Err(grammar_bug(format!("atom `{}` not declared", atom.name())));
        };
        decl.clone()
    };
    for (arg, col_ty) in atom.arguments_mut().iter_mut().zip(col_types.iter()) {
        if let AtomArg::Const(c) = arg
            && c.is_polymorphic()
        {
            c.pin(col_ty.clone(), span)?;
        }
    }
    Ok(())
}

/// The declared column types of `atom`'s relation, one per argument.
/// `AtomArity` if the atom's argument count differs from its `.decl`;
/// `grammar_bug` if the relation is undeclared, which assembly rules out.
fn declared_columns<'a>(atom: &Atom, decls: &'a DeclTypes) -> Result<&'a [DataType], ParseError> {
    let decl = decls
        .get(atom.name())
        .ok_or_else(|| grammar_bug(format!("atom `{}` not declared", atom.name())))?;
    if atom.arity() != decl.len() {
        return Err(ParseError::AtomArity {
            span: atom.span(),
            rel: atom.raw_name().to_string(),
            expected: decl.len(),
            found: atom.arity(),
        });
    }
    Ok(decl)
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use crate::AtomArg;
    use crate::Constant;
    use crate::DataType;
    use crate::ParseError;
    use crate::Predicate;
    use crate::test_harness::checked;

    /// Body-positive atom literal: `Flag(5)` with `.decl Flag(x: int8)` must
    /// pin `5` to `Int8(5)` via `pin_atom`. If that pass becomes a no-op,
    /// catalog calls `data_type()` on a polymorphic `Int` and panics.
    #[test]
    fn body_atom_const_pinned_to_declared_column_width() {
        let src = "\
            .decl Item(x: int8)\n\
            .decl Flag(x: int8)\n\
            .decl Out(x: int8)\n\
            .input Item(IO=\"file\", filename=\"Item.csv\", delimiter=\",\")\n\
            .input Flag(IO=\"file\", filename=\"Flag.csv\", delimiter=\",\")\n\
            .output Out\n\
            Out(x) :- Item(x), Flag(5).\n";
        let program = checked(src).expect("type-check should succeed");
        let rule = &program.rules()[0];
        let flag_atom = match &rule.rhs()[1] {
            Predicate::PositiveAtom(a) => a,
            other => panic!("expected Flag atom, got {other:?}"),
        };
        match &flag_atom.arguments()[0] {
            AtomArg::Const(c) => assert_eq!(c, &Constant::new(DataType::Int8, "5")),
            other => panic!("expected Const, got {other:?}"),
        }
    }

    /// A body atom whose argument count disagrees with its `.decl` is a user
    /// error, whichever way it is wrong and wherever the atom sits: too few
    /// arguments must not type-check, and too many must not surface as an
    /// internal error.
    // Cases: body, (expected, found).
    #[rstest]
    #[case("Edge(x)", (2, 1))]
    #[case("Edge(x, y, z)", (2, 3))]
    #[case("Edge(x, y), !Edge(y)", (2, 1))]
    #[case("Edge(x, y), !Edge(y, x, x)", (2, 3))]
    #[case("Edge(x, y), n = count : { Edge(x) }", (2, 1))]
    fn body_atom_arity_mismatch_is_rejected(
        #[case] body: &str,
        #[case] (expected, found): (usize, usize),
    ) {
        let src = format!(
            ".decl Edge(a: int32, b: int32)\n\
             .decl Out(a: int32)\n\
             .input Edge(IO=\"file\", filename=\"Edge.csv\", delimiter=\",\")\n\
             .output Out\n\
             Out(x) :- {body}.\n"
        );
        match checked(&src) {
            Err(ParseError::AtomArity {
                rel,
                expected: e,
                found: f,
                ..
            }) => assert_eq!((rel.as_str(), e, f), ("Edge", expected, found)),
            other => panic!("expected AtomArity, got {other:?}"),
        }
    }

    #[test]
    fn body_atom_matching_decl_arity_is_accepted() {
        let src = "\
            .decl Edge(a: int32, b: int32)\n\
            .decl Out(a: int32)\n\
            .input Edge(IO=\"file\", filename=\"Edge.csv\", delimiter=\",\")\n\
            .output Out\n\
            Out(x) :- Edge(x, y), !Edge(y, x).\n";
        checked(src).expect("matching arity should type-check");
    }
}
