//! Arithmetic expression signatures for FlowLog Datalog programs.

use crate::catalog::atom::AtomArgumentSignature;
use crate::parser::{Arithmetic, ArithmeticOperator, ConstType, Factor};
use std::fmt;

/// A factor in an arithmetic expression with variables resolved to their
/// concrete positions within atoms.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum FactorPos {
    /// A variable reference identified by its atom and argument position
    Var(AtomArgumentSignature),
    /// A constant value (integer, string, etc.)
    Const(ConstType),
    /// A user-defined function call.
    FnCall {
        name: String,
        args: Vec<ArithmeticPos>,
    },
}

impl FactorPos {
    /// Returns the argument signature if this factor is a single variable.
    pub fn as_var_signature(&self) -> Option<&AtomArgumentSignature> {
        match self {
            FactorPos::Var(atom_arg_signature) => Some(atom_arg_signature),
            FactorPos::Const(_) | FactorPos::FnCall { .. } => None,
        }
    }

    /// Returns all argument signatures referenced in this factor (including nested in FnCall args).
    pub fn signatures(&self) -> Vec<&AtomArgumentSignature> {
        match self {
            FactorPos::Var(sig) => vec![sig],
            FactorPos::Const(_) => vec![],
            FactorPos::FnCall { args, .. } => args.iter().flat_map(|a| a.signatures()).collect(),
        }
    }

    /// Transform every variable in this factor using `f`, recursing into FnCall args.
    pub fn map_vars(&self, f: &impl Fn(&AtomArgumentSignature) -> FactorPos) -> FactorPos {
        match self {
            FactorPos::Var(sig) => f(sig),
            FactorPos::Const(c) => FactorPos::Const(c.clone()),
            FactorPos::FnCall { name, args } => FactorPos::FnCall {
                name: name.clone(),
                args: args.iter().map(|a| a.map_vars(f)).collect(),
            },
        }
    }
}

impl fmt::Display for FactorPos {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            FactorPos::Var(sig) => write!(f, "{}", sig),
            FactorPos::Const(c) => write!(f, "{}", c),
            FactorPos::FnCall { name, args } => {
                let args_str = args
                    .iter()
                    .map(|a| a.to_string())
                    .collect::<Vec<_>>()
                    .join(", ");
                write!(f, "{name}({args_str})")
            }
        }
    }
}

/// Positional arithmetic expression with variables resolved to their
/// concrete argument signatures.
#[derive(Clone, Hash, PartialEq, Eq)]
pub struct ArithmeticPos {
    /// The initial factor (left-most term)
    init: FactorPos,
    /// Subsequent operations and factors (operator, right operand)
    rest: Vec<(ArithmeticOperator, FactorPos)>,
}

impl ArithmeticPos {
    /// Creates a new positional arithmetic expression.
    pub fn new(init: FactorPos, rest: Vec<(ArithmeticOperator, FactorPos)>) -> Self {
        Self { init, rest }
    }

    /// Creates a simple ArithmeticPos for a single variable from an AtomArgumentSignature.
    pub fn from_var_signature(signature: AtomArgumentSignature) -> Self {
        ArithmeticPos {
            init: FactorPos::Var(signature),
            rest: vec![],
        }
    }

    /// Constructs a positional arithmetic expression from a parsed arithmetic expression.
    pub fn from_arithmetic(arith: &Arithmetic, var_signatures: &[AtomArgumentSignature]) -> Self {
        fn map_factor(
            factor: &Factor,
            var_signatures: &[AtomArgumentSignature],
            var_id: &mut usize,
        ) -> FactorPos {
            match factor {
                Factor::Var(_) => {
                    let sig = var_signatures[*var_id];
                    *var_id += 1;
                    FactorPos::Var(sig)
                }
                Factor::Const(c) => FactorPos::Const(c.clone()),
                Factor::FnCall(fc) => {
                    let args = fc
                        .args()
                        .iter()
                        .map(|arg| {
                            let num_vars = arg.vars().len();
                            let sigs = &var_signatures[*var_id..*var_id + num_vars];
                            *var_id += num_vars;
                            ArithmeticPos::from_arithmetic(arg, sigs)
                        })
                        .collect();
                    FactorPos::FnCall {
                        name: fc.name().to_string(),
                        args,
                    }
                }
            }
        }

        let mut var_id = 0usize;

        let init = map_factor(arith.init(), var_signatures, &mut var_id);
        let rest = arith
            .rest()
            .iter()
            .map(|(op, factor)| (op.clone(), map_factor(factor, var_signatures, &mut var_id)))
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

    /// Returns a vector of all variable signatures referenced in this expression, in order.
    pub fn signatures(&self) -> Vec<&AtomArgumentSignature> {
        let mut sigs = self.init.signatures();
        for (_, factor) in &self.rest {
            sigs.extend(factor.signatures());
        }
        sigs
    }

    /// Transform every variable in this expression using `f`, recursing into FnCall args.
    pub fn map_vars(&self, f: &impl Fn(&AtomArgumentSignature) -> FactorPos) -> ArithmeticPos {
        let init = self.init.map_vars(f);
        let rest = self
            .rest
            .iter()
            .map(|(op, factor)| (op.clone(), factor.map_vars(f)))
            .collect();
        ArithmeticPos::new(init, rest)
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

impl fmt::Debug for ArithmeticPos {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        // Delegate Debug to Display
        fmt::Display::fmt(self, f)
    }
}
