//! Value-fold one expression: collapse a fully-constant `Arithmetic` to a
//! single pinned literal, recursing through groups and call/tuple arguments.

use crate::Arithmetic;
use crate::Constant;
use crate::Factor;
use crate::pipeline::fold::eval::eval_arith;

/// Post-order: fold each sub-expression, then collapse a fully-constant
/// multi-term `Arithmetic` into one `Const`.
pub(super) fn fold_arith(a: &mut Arithmetic) {
    fold_factor(a.init_mut());
    for (_, f) in a.rest_mut() {
        fold_factor(f);
    }
    if let Some(folded) = try_eval(a) {
        debug_assert!(
            !folded.is_polymorphic(),
            "fold produced a polymorphic literal"
        );
        *a = Arithmetic::new(Factor::Const(folded), Vec::new());
    }
}

/// Value-fold every `Arithmetic` nested inside a factor.
fn fold_factor(f: &mut Factor) {
    match f {
        Factor::Group(inner) => {
            fold_arith(inner);
            // Unwrap a group that folded to a lone constant so the enclosing
            // expression can fold through it.
            if inner.rest().is_empty()
                && let Factor::Const(c) = inner.init()
            {
                *f = Factor::Const(c.clone());
            }
        }
        Factor::Builtin(bc) => {
            for arg in bc.args_mut() {
                fold_arith(arg);
            }
        }
        Factor::FnCall(fc) => {
            for arg in fc.args_mut() {
                fold_arith(arg);
            }
        }
        Factor::Tuple(t) => {
            for a in t.exprs_mut() {
                fold_arith(a);
            }
        }
        Factor::TupleProj { tuple, .. } => fold_arith(tuple),
        // Casts are stripped by the subtype pass before folding runs.
        Factor::Var(_) | Factor::Const(_) | Factor::Cast(_) => {}
    }
}

/// Evaluate a fully-constant multi-term `Arithmetic` left-to-right (matching
/// the parser's left-associative, no-precedence fold). `None` if it isn't
/// fully constant or any step can't be folded (see [`eval_arith`]).
fn try_eval(a: &Arithmetic) -> Option<Constant> {
    if a.rest().is_empty() {
        return None; // a lone factor has nothing to combine
    }
    let Factor::Const(init) = a.init() else {
        return None;
    };
    let mut acc = init.clone();
    for (op, f) in a.rest() {
        let Factor::Const(rhs) = f else {
            return None;
        };
        acc = eval_arith(op, &acc, rhs)?;
    }
    Some(acc)
}
