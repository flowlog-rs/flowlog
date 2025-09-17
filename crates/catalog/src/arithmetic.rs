//! Arithmetic expression signatures for Macaron Datalog programs.

use crate::atom::AtomArgumentSignature;
use parser::{Arithmetic, ArithmeticOperator, ConstType, Factor};
use std::fmt;

/// A factor in an arithmetic expression with variables resolved to their
/// concrete positions within atoms.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum FactorPos {
    /// A variable reference identified by its atom and argument position
    Var(AtomArgumentSignature),
    /// A constant value (integer, string, etc.)
    Const(ConstType),
}

impl FactorPos {
    /// Returns a vector of argument signatures contained in this factor.
    pub fn signatures(&self) -> Vec<&AtomArgumentSignature> {
        match self {
            FactorPos::Var(atom_arg_signature) => vec![atom_arg_signature],
            FactorPos::Const(_) => vec![],
        }
    }
}

impl fmt::Display for FactorPos {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            FactorPos::Var(sig) => write!(f, "{}", sig),
            FactorPos::Const(c) => write!(f, "{}", c),
        }
    }
}

/// Positional arithmetic expression with variables resolved to their
/// concrete argument signatures.
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct ArithmeticPos {
    /// The initial factor (left-most term)
    init: FactorPos,
    /// Subsequent operations and factors (operator, right operand)
    rest: Vec<(ArithmeticOperator, FactorPos)>,
}

impl ArithmeticPos {
    /// Constructs a positional arithmetic expression from a parsed arithmetic expression.
    pub fn from_arithmetic(arith: &Arithmetic, var_signatures: &[AtomArgumentSignature]) -> Self {
        let mut var_id = 0usize;

        // Helper to map a factor from parsed to positional form
        let map_factor = |factor: &Factor, var_id: &mut usize| -> FactorPos {
            match factor {
                Factor::Var(_) => {
                    // Consume the next variable signature in order
                    let sig = var_signatures[*var_id];
                    *var_id += 1;
                    FactorPos::Var(sig)
                }
                Factor::Const(c) => FactorPos::Const(c.clone()),
            }
        };

        let init = map_factor(arith.init(), &mut var_id);
        let rest = arith
            .rest()
            .iter()
            .map(|(op, factor)| (op.clone(), map_factor(factor, &mut var_id)))
            .collect();

        ArithmeticPos { init, rest }
    }

    /// Returns the initial (left-most) factor.
    #[inline]
    pub fn init(&self) -> &FactorPos {
        &self.init
    }

    /// Returns the sequence of operators and factors after the initial term.
    #[inline]
    pub fn rest(&self) -> &[(ArithmeticOperator, FactorPos)] {
        &self.rest
    }

    /// Returns true if this expression consists of a single factor (no operations).
    #[inline]
    pub fn is_literal(&self) -> bool {
        self.rest.is_empty()
    }

    /// Returns true if this expression is a single variable.
    #[inline]
    pub fn is_var(&self) -> bool {
        self.is_literal() && matches!(self.init, FactorPos::Var(_))
    }

    /// Returns true if this expression is a single constant.
    #[inline]
    pub fn is_const(&self) -> bool {
        self.is_literal() && matches!(self.init, FactorPos::Const(_))
    }

    /// Returns a vector of all variable signatures referenced in this expression.
    pub fn signatures(&self) -> Vec<&AtomArgumentSignature> {
        let mut signatures = self.init.signatures();
        for (_, factor) in &self.rest {
            signatures.extend(factor.signatures());
        }
        signatures
    }
}

impl fmt::Display for ArithmeticPos {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.init)?;
        for (op, factor) in &self.rest {
            write!(f, " {} {}", op, factor)?;
        }
        Ok(())
    }
}
