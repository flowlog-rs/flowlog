#[cfg(test)]
mod program_tests {
    use crate::error::ParserError;
    use crate::Program;
    use crate::{Lexeme, MacaronParser, Result, Rule};
    use pest::Parser; // needed for MacaronParser::parse

    fn parse_program(src: &str) -> Result<Program> {
        let mut pairs = MacaronParser::parse(Rule::main_grammar, src)
            .map_err(|_| ParserError::FailedToParseProgram)?;
        let top = pairs.next().ok_or(ParserError::FailedToParseRule)?;
        Program::from_parsed_rule(top)
    }

    #[test]
    fn program_parse_minimal() {
        let src = "
            .decl A()
            .output A
        ";
        let p = parse_program(src).expect("parse minimal program");
        assert_eq!(p.relations().len(), 1);
        assert_eq!(p.rules().len(), 0);
        assert!(p.output_relations().iter().any(|r| r.name() == "A"));
    }

    #[test]
    fn program_bool_fact_extraction() {
        let src = "
            .decl Flag()
            .output Flag
            Flag() :- true.
        ";
        let p = parse_program(src).expect("parse bool fact program");
        assert_eq!(p.rules().len(), 0, "boolean rule should be extracted");
        let facts = p.bool_facts();
        assert!(facts.contains_key("Flag"));
        assert_eq!(facts["Flag"].len(), 1);
    }

    #[test]
    fn program_prune_dead_components() {
        let src = "
            .decl A(x: number)
            .decl B(x: number)
            .decl C(x: number)
            .output A
            A(x) :- B(x).
            C(x) :- B(x).
        ";
        let p = parse_program(src).expect("parse pruning program");
        let pruned = p.prune_dead_components();
        assert!(pruned.relations().iter().any(|r| r.name() == "A"));
        assert!(pruned.relations().iter().any(|r| r.name() == "B"));
        assert!(!pruned.relations().iter().any(|r| r.name() == "C"));
    }
}
