//! Tab completion for the shell's line editor.

use rustyline::Context;
use rustyline::Helper;
use rustyline::completion::Completer;
use rustyline::completion::FilenameCompleter;
use rustyline::completion::Pair;
use rustyline::highlight::Highlighter;
use rustyline::hint::Hinter;
use rustyline::validate::Validator;

use crate::shell::command::VERBS;
use crate::shell::command::Verb;
use crate::shell::command::split;

/// Completes the word before the cursor with what the words ahead of it
/// allow: a verb first, a relation after `insert` or `delete`, and a path
/// in a word that opens with `@`. Verbs and relations match ignoring ASCII
/// case and complete in lowercase.
pub(super) struct Completion {
    /// Lowercase, sorted, and without duplicates.
    relations: Vec<String>,
    files: FilenameCompleter,
}

impl Completion {
    pub(super) fn new(relations: &[&str]) -> Self {
        let mut relations: Vec<String> = relations
            .iter()
            .map(|name| name.to_ascii_lowercase())
            .collect();
        relations.sort_unstable();
        relations.dedup();
        Self {
            relations,
            files: FilenameCompleter::new(),
        }
    }
}

impl Completer for Completion {
    type Candidate = Pair;

    fn complete(
        &self,
        line: &str,
        pos: usize,
        _: &Context<'_>,
    ) -> rustyline::Result<(usize, Vec<Pair>)> {
        let before = &line[..pos];
        // A word ends at any whitespace, as `split` ends it; trimming whole
        // characters keeps `start` on a character boundary.
        let start = before.trim_end_matches(|c: char| !c.is_whitespace()).len();
        let word = &before[start..];

        // Rustyline's path completer starts after the `@`, one of its break
        // characters, and puts a `\` before every break character in a name,
        // such as a space or `(`. The shell reads `\` literally outside
        // quotes, so such a name completes to a path the shell cannot read;
        // reusing the completer is worth that rare case over keeping a
        // directory lister here.
        if word.starts_with('@') {
            return self.files.complete_path(line, pos);
        }

        let names: Vec<&str> = match split(&before[..start]).as_deref() {
            Ok([]) => VERBS.iter().map(|&(spelling, _)| spelling).collect(),
            Ok([verb]) if matches!(verb.parse::<Verb>(), Ok(Verb::Insert | Verb::Delete)) => {
                self.relations.iter().map(String::as_str).collect()
            }
            _ => Vec::new(),
        };
        let prefix = word.to_ascii_lowercase();
        let candidates = names
            .into_iter()
            .filter(|name| name.starts_with(&prefix))
            .map(|name| Pair {
                display: name.to_owned(),
                replacement: name.to_owned(),
            })
            .collect();
        Ok((start, candidates))
    }
}

// The shell only completes: no hints, highlighting, or validation.
impl Helper for Completion {}

impl Hinter for Completion {
    type Hint = String;
}

impl Highlighter for Completion {}

impl Validator for Completion {}

#[cfg(test)]
mod tests {
    use std::fs;

    use rstest::rstest;
    use rustyline::history::DefaultHistory;

    use super::*;

    /// Completes `line` at byte `pos` over the relations `edge` and `fixed`,
    /// given in mixed case and twice, and returns the start of the word
    /// replaced and the replacements offered.
    fn complete_at(line: &str, pos: usize) -> (usize, Vec<String>) {
        let completion = Completion::new(&["fixed", "Edge", "edge"]);
        let history = DefaultHistory::new();
        let (start, candidates) = completion
            .complete(line, pos, &Context::new(&history))
            .expect("completion reads no terminal");
        let replacements = candidates.into_iter().map(|pair| pair.replacement);
        (start, replacements.collect())
    }

    /// Walks the table: every spelling is offered on an empty line.
    #[test]
    fn an_empty_line_offers_every_verb_spelling() {
        let spellings: Vec<String> = VERBS
            .iter()
            .map(|(spelling, _)| (*spelling).to_owned())
            .collect();
        assert_eq!(complete_at("", 0), (0, spellings));
    }

    // Cases: line, start of the word completed, replacements offered.
    #[rstest]
    #[case::verbs_by_prefix("d", 0, &["delete", "done"])]
    #[case::verbs_ignoring_case("INS", 0, &["insert"])]
    #[case::relations_after_insert("insert e", 7, &["edge"])]
    #[case::relations_after_delete_in_any_case("DELETE ", 7, &["edge", "fixed"])]
    #[case::nothing_after_a_verb_without_relation("commit ", 7, &[])]
    #[case::nothing_for_a_tuple("insert edge 1", 12, &[])]
    #[case::nothing_after_an_unknown_verb("frob e", 5, &[])]
    #[case::nothing_inside_an_open_quote("insert \"x e", 10, &[])]
    #[case::any_whitespace_ends_a_word("insert\u{a0}e", 8, &["edge"])]
    fn completes_the_word_before_the_cursor(
        #[case] line: &str,
        #[case] start: usize,
        #[case] expected: &[&str],
    ) {
        let expected: Vec<String> = expected.iter().map(|name| (*name).to_owned()).collect();
        assert_eq!(complete_at(line, line.len()), (start, expected));
    }

    #[test]
    fn only_the_text_before_the_cursor_is_completed() {
        assert_eq!(
            complete_at("insert ed 1,2", 9),
            (7, vec!["edge".to_owned()])
        );
    }

    /// The `@` stays in the line: the completed path starts after it.
    #[test]
    fn a_word_after_at_completes_to_a_path() {
        let dir = tempfile::tempdir().expect("temp dir");
        fs::write(dir.path().join("edges.csv"), "").expect("write file");
        fs::create_dir(dir.path().join("edits")).expect("create dir");
        fs::write(dir.path().join("other.csv"), "").expect("write file");
        let base = dir.path().to_str().expect("temp dir path is UTF-8");

        let line = format!("insert edge @{base}/ed");
        let start = "insert edge @".len();
        let expected = vec![format!("{base}/edges.csv"), format!("{base}/edits/")];
        assert_eq!(complete_at(&line, line.len()), (start, expected));
    }
}
