//! Bind, check, and pin an atom's arguments against their declared columns.

use flowlog_parser::Atom;
use flowlog_parser::AtomArg;
use flowlog_parser::DataType;

use crate::TypeCheckError;
use crate::env::DeclTypes;
use crate::primitive::Bindings;
use crate::primitive::ty::LitKind;

/// Record each variable's first-seen type; validate const arg families.
/// Consts are pinned separately by [`pin_atom`] so bindings can be populated
/// for the whole rule before any mutation.
pub(super) fn bind_atom(
    atom: &Atom,
    decls: &DeclTypes,
    bindings: &mut Bindings,
) -> Result<(), TypeCheckError> {
    for (i, arg) in atom.arguments().iter().enumerate() {
        let col_ty = resolve_atom_column(atom, i, decls)?;
        match arg {
            AtomArg::Var(v) => match bindings.get(v) {
                None => {
                    bindings.insert(v.clone(), (col_ty, atom.span()));
                }
                Some((first_ty, first_span)) if first_ty != &col_ty => {
                    return Err(TypeCheckError::TypeMismatch {
                        var: v.clone(),
                        first_ty: first_ty.clone(),
                        first_span: *first_span,
                        later_ty: col_ty,
                        later_span: atom.span(),
                    });
                }
                Some(_) => {}
            },
            AtomArg::Const(c) => {
                if !LitKind::of(c)?.fits(&col_ty) {
                    return Err(TypeCheckError::LiteralColumnMismatch {
                        span: atom.span(),
                        literal: c.to_string(),
                        expected: col_ty,
                    });
                }
            }
            AtomArg::Placeholder => {}
        }
    }
    Ok(())
}

/// Check each bound variable matches its column type. Unbound vars are
/// reported separately by the range-restriction pass.
pub(super) fn check_atom(
    atom: &Atom,
    decls: &DeclTypes,
    bindings: &Bindings,
) -> Result<(), TypeCheckError> {
    for (i, arg) in atom.arguments().iter().enumerate() {
        let col_ty = resolve_atom_column(atom, i, decls)?;
        match arg {
            AtomArg::Var(v) => {
                if let Some((bound_ty, bound_span)) = bindings.get(v)
                    && bound_ty != &col_ty
                {
                    return Err(TypeCheckError::TypeMismatch {
                        var: v.clone(),
                        first_ty: bound_ty.clone(),
                        first_span: *bound_span,
                        later_ty: col_ty,
                        later_span: atom.span(),
                    });
                }
            }
            AtomArg::Const(c) => {
                if !LitKind::of(c)?.fits(&col_ty) {
                    return Err(TypeCheckError::LiteralColumnMismatch {
                        span: atom.span(),
                        literal: c.to_string(),
                        expected: col_ty,
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
pub(super) fn pin_atom(atom: &mut Atom, decls: &DeclTypes) -> Result<(), TypeCheckError> {
    let col_types: Vec<DataType> = {
        let Some(decl) = decls.get(atom.name()) else {
            return Err(TypeCheckError::internal(format!(
                "atom `{}` not declared",
                atom.name()
            )));
        };
        decl.clone()
    };
    for (arg, col_ty) in atom.arguments_mut().iter_mut().zip(col_types.iter()) {
        if let AtomArg::Const(c) = arg
            && c.is_polymorphic()
        {
            c.pin(col_ty.clone());
        }
    }
    Ok(())
}

fn resolve_atom_column(
    atom: &Atom,
    i: usize,
    decls: &DeclTypes,
) -> Result<DataType, TypeCheckError> {
    let decl = decls
        .get(atom.name())
        .ok_or_else(|| TypeCheckError::internal(format!("atom `{}` not declared", atom.name())))?;
    decl.get(i).cloned().ok_or_else(|| {
        TypeCheckError::internal(format!(
            "atom `{}` has {} arguments but `.decl` has {}",
            atom.name(),
            atom.arguments().len(),
            decl.len(),
        ))
    })
}
