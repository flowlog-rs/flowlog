use std::fmt::Debug;
use thiserror::Error;

#[derive(Error, Debug, PartialEq, Eq)]
pub enum ParserError {
    #[error("Parser error: Missing relation name in {0} directive")]
    IncompleteDirective(String),
    #[error("Parser error: Missing relation name")]
    MissingName,
    #[error("Parser error: Missing '{0}' for input parameter")]
    IncompleteInputParameter(String),
    #[error("Parser error: Missing '{0}' for attribute")]
    IncompleteAttribute(String),
    #[error("Parser error: invalid datatype '{dts}' for attribute '{aname}': {e}")]
    InvalidAttributeDatatype {
        aname: String,
        dts: String,
        e: String,
    },
    #[error("Parser error: unexpected rule in {0}: {1}")]
    UnexpectedRule(String, String),
    #[error("Parser error: unsupported aggregation: {0}")]
    UnsupportedAggregation(String),
    #[error("Parser error: Missing '{0}' for aggregation")]
    IncompleteAggregation(String),
    #[error("Parser error: unknown arithmetic operator '{0}'")]
    UnsupportedArithmeticOperator(String),
    #[error("Parser error: Missing '{0}' for arithmetic operator")]
    IncompleteArithmeticOperator(String),
    #[error("Parser error: invalid factor rule: {0}")]
    InvalidFactorRule(String),
    #[error("Parser error: Missing '{0}' for factor")]
    IncompleteFactor(String),
    #[error("Parser error: Missing '{0}' for arithmetic expression")]
    IncompleteArithmetic(String),
    #[error("Parser error: Invalid arithmetic expression, expected (operator, factor) pairs")]
    InvalidArithmetic(String),
    #[error("Parser error: Missing '{0}' for atom argument")]
    IncompleteAtomArg(String),
    #[error("Parser error: Invalid atom argument rule: {0}")]
    InvalidAtomArgRule(String),
    #[error("Parser error: Missing '{0}' for comparison operator")]
    IncompleteComparisonOperator(String),
    #[error("Parser error: Invalid comparison operator '{0}'")]
    InvalidComparisonOperator(String),
    #[error("Parser error: Missing '{0}' for comparison")]
    IncompleteComparison(String),
    #[error("Parser error: Missing '{0}' for head argument")]
    IncompleteHeadArg(String),
    #[error("Parser error: Missing '{0}' for head")]
    IncompleteHead(String),
    #[error("Parser error: Missing '{0}' for predicate")]
    IncompletePredicate(String),
    #[error("Parser error: Boolean rule head must contain only constants: {0}")]
    InvalidBoolRule(String),
    #[error("Parser error: Missing rule head")]
    MissingHead,
    #[error("Parser error: Missing rule predicate")]
    MissingPredicate,
    #[error("Parser error: Missing '{0}' for constant rule")]
    IncompleteConstantRule(String),
    #[error("Parser error: failed to parse number literal as i32")]
    FailedToParseNumberLiteral,
    #[error("Parser error: Invalid data type '{0}', expected 'number' or 'string'")]
    InvalidDataType(String),
    #[error("Parser error: Failed to parse Macaron program")]
    FailedToParseProgram,
    #[error("Parser error: Failed to parse Macaron rule")]
    FailedToParseRule,
    #[error("IO error: {0}")]
    Io(String),
    #[error("Parser error: Duplicate directive for relation '{0}'")]
    DuplicateDirective(String),
    #[error("Parser error: Relation '{0}' cannot have both input and output directives. Relations must be either EDB (input only) or IDB (output/printsize only).")]
    ConflictingDirectives(String),
    #[error("Parser error: Input directive for UNKNOWN relation '{0}'. Relation must be declared with .decl before using .input directive.")]
    UnknownInputRelation(String),
    #[error("Parser error: Output directive for UNKNOWN relation '{0}'. Relation must be declared with .decl before using .output directive.")]
    UnknownOutputRelation(String),
    #[error("Parser error: Print size directive for UNKNOWN relation '{0}'. Relation must be declared with .decl before using .printsize directive.")]
    UnknownPrintSizeRelation(String),
}

impl From<std::io::Error> for ParserError {
    fn from(e: std::io::Error) -> Self {
        Self::Io(e.to_string())
    }
}
