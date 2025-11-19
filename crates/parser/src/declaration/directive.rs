//! Input/Output directive types for Macaron Datalog programs.

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

impl Lexeme for InputDirective {
    fn from_parsed_rule(parsed_rule: Pair<Rule>) -> Self {
        let mut inner = parsed_rule.into_inner();

        // First child is always the relation name
        let relation_name = inner
            .next()
            .expect("Parser error: input directive missing relation name")
            .as_str()
            .to_string();

        let mut parameters = HashMap::new();

        // Parse all key=value style parameters
        for param in inner {
            if param.as_rule() == Rule::input_params {
                for input_param in param.into_inner() {
                    let mut param_inner = input_param.into_inner();
                    let key = param_inner
                        .next()
                        .expect("Parser error: input parameter missing name")
                        .as_str()
                        .to_string();
                    let value = param_inner
                        .next()
                        .expect("Parser error: input parameter missing value")
                        .as_str()
                        .trim_matches('"')
                        .to_string();
                    parameters.insert(key, value);
                }
            }
        }

        Self::new(relation_name, parameters)
    }
}

/// Represents an output directive (which relation to write)
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutputDirective {
    relation_name: String,
}

impl OutputDirective {
    /// Create a new OutputDirective
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

impl Lexeme for OutputDirective {
    fn from_parsed_rule(parsed_rule: Pair<Rule>) -> Self {
        let mut inner = parsed_rule.into_inner();

        // First child is relation name
        let relation_name = inner
            .next()
            .expect("Parser error: output directive missing relation name")
            .as_str()
            .to_string();

        Self::new(relation_name)
    }
}

/// Directive for printing the size of an EDB relation
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PrintSizeDirective {
    relation_name: String,
}

impl PrintSizeDirective {
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
        let output_dir = OutputDirective::new("OutputRelation".to_string());
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

        let output1 = OutputDirective::new("Test".to_string());
        let output2 = OutputDirective::new("Test".to_string());
        let output3 = OutputDirective::new("Other".to_string());

        assert_eq!(output1, output2);
        assert_ne!(output1, output3);
    }
}
