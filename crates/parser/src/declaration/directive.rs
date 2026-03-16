//! Input/Output directive types for FlowLog Datalog programs.

use crate::{Lexeme, Rule};
use pest::iterators::Pair;
use std::collections::HashMap;

/// Represents an input directive (EDB source + parameters like file path)
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InputDirective {
    relation_name: String,
    parameters: HashMap<String, String>,
}

impl InputDirective {
    /// Create a new InputDirective with a relation name and parameters
    ///
    /// Converts the relation name to lowercase.
    #[must_use]
    pub fn new(relation_name: String, parameters: HashMap<String, String>) -> Self {
        Self {
            relation_name: relation_name.to_lowercase(),
            parameters,
        }
    }

    /// Get the relation name
    #[must_use]
    pub fn relation_name(&self) -> &str {
        &self.relation_name
    }

    /// Get all input parameters (IO type, filename, etc.)
    #[must_use]
    pub fn parameters(&self) -> &HashMap<String, String> {
        &self.parameters
    }
}

/// Parse `io_params` children from a directive's inner pairs into a key→value map.
fn parse_io_params<'i>(inner: impl Iterator<Item = pest::iterators::Pair<'i, Rule>>) -> HashMap<String, String> {
    let mut parameters = HashMap::new();
    for node in inner {
        if node.as_rule() == Rule::io_params {
            for io_param in node.into_inner() {
                let mut kv = io_param.into_inner();
                let key = kv.next().expect("Parser error: io parameter missing name").as_str().to_string();
                let value = kv.next().expect("Parser error: io parameter missing value").as_str().trim_matches('"').to_string();
                parameters.insert(key, value);
            }
        }
    }
    parameters
}

impl Lexeme for InputDirective {
    fn from_parsed_rule(parsed_rule: Pair<Rule>) -> Self {
        let mut inner = parsed_rule.into_inner();
        let relation_name = inner
            .next()
            .expect("Parser error: input directive missing relation name")
            .as_str()
            .to_string();
        Self::new(relation_name, parse_io_params(inner))
    }
}

/// Represents an output directive (which relation to write, with optional parameters)
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutputDirective {
    relation_name: String,
    parameters: HashMap<String, String>,
}

impl OutputDirective {
    /// Create a new OutputDirective
    ///
    /// Converts the relation name to lowercase.
    #[must_use]
    pub fn new(relation_name: String, parameters: HashMap<String, String>) -> Self {
        Self {
            relation_name: relation_name.to_lowercase(),
            parameters,
        }
    }

    /// Get the relation name
    #[must_use]
    pub fn relation_name(&self) -> &str {
        &self.relation_name
    }

    /// Get all output parameters
    #[must_use]
    pub fn parameters(&self) -> &HashMap<String, String> {
        &self.parameters
    }
}

impl Lexeme for OutputDirective {
    fn from_parsed_rule(parsed_rule: Pair<Rule>) -> Self {
        let mut inner = parsed_rule.into_inner();
        let relation_name = inner
            .next()
            .expect("Parser error: output directive missing relation name")
            .as_str()
            .to_string();
        Self::new(relation_name, parse_io_params(inner))
    }
}

/// Directive for printing the size of an EDB relation
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PrintSizeDirective {
    relation_name: String,
}

impl PrintSizeDirective {
    /// Create a new PrintSizeDirective
    ///
    /// Converts the relation name to lowercase.
    #[must_use]
    pub fn new(relation_name: String) -> Self {
        Self {
            relation_name: relation_name.to_lowercase(),
        }
    }

    /// Get the relation name
    #[must_use]
    pub fn relation_name(&self) -> &str {
        &self.relation_name
    }
}

impl Lexeme for PrintSizeDirective {
    fn from_parsed_rule(parsed_rule: Pair<Rule>) -> Self {
        let mut inner = parsed_rule.into_inner();

        // First child is the relation name
        let relation_name = inner
            .next()
            .expect("Parser error: printsize directive missing relation name")
            .as_str()
            .to_string();

        Self::new(relation_name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_input_directive_creation() {
        let mut params = HashMap::new();
        params.insert("IO".to_string(), "file".to_string());
        params.insert("filename".to_string(), "test.csv".to_string());

        let input_dir = InputDirective::new("TestRelation".to_string(), params);

        assert_eq!(input_dir.relation_name(), "testrelation");
        assert_eq!(input_dir.parameters().get("IO"), Some(&"file".to_string()));
        assert_eq!(
            input_dir.parameters().get("filename"),
            Some(&"test.csv".to_string())
        );
    }

    #[test]
    fn test_output_directive_creation() {
        let output_dir = OutputDirective::new("OutputRelation".to_string(), HashMap::new());
        assert_eq!(output_dir.relation_name(), "outputrelation");
    }

    #[test]
    fn test_printsize_directive_creation() {
        let printsize_dir = PrintSizeDirective::new("SizeRelation".to_string());
        assert_eq!(printsize_dir.relation_name(), "sizerelation");
    }

    #[test]
    fn test_directive_equality() {
        let input1 = InputDirective::new("Test".to_string(), HashMap::new());
        let input2 = InputDirective::new("Test".to_string(), HashMap::new());
        let input3 = InputDirective::new("Other".to_string(), HashMap::new());

        assert_eq!(input1, input2);
        assert_ne!(input1, input3);

        let output1 = OutputDirective::new("Test".to_string(), HashMap::new());
        let output2 = OutputDirective::new("Test".to_string(), HashMap::new());
        let output3 = OutputDirective::new("Other".to_string(), HashMap::new());

        assert_eq!(output1, output2);
        assert_ne!(output1, output3);
    }
}
