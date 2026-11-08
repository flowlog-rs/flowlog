//! Tab completion for the shell's line editor.

use std::fs;
use std::ops::Range;
use std::path::MAIN_SEPARATOR;
use std::path::is_separator;

use rustyline::Changeset;
use rustyline::Context;
use rustyline::Helper;
use rustyline::completion::Completer;
use rustyline::completion::Pair;
use rustyline::highlight::Highlighter;
use rustyline::hint::Hinter;
use rustyline::line_buffer::LineBuffer;
use rustyline::validate::Validator;

use crate::shell::command::CommandError;
use crate::shell::command::VERBS;
use crate::shell::command::Verb;
use crate::shell::command::Word;
use crate::shell::command::Words;
use crate::shell::command::quote_word;

/// Completes the word before the cursor with what the words ahead of it
/// allow: a verb first, a relation after `insert` or `delete`, and a path
/// in an update argument whose decoded text opens with `@`. Verbs and
/// relations match ignoring ASCII case and complete in lowercase.
#[derive(Debug)]
pub(super) struct Completion {
    /// Lowercase, sorted, and without duplicates.
    relations: Vec<String>,
}

impl Completion {
    #[must_use]
    pub(super) fn new(relations: &[&str]) -> Self {
        let mut relations: Vec<String> = relations
            .iter()
            .map(|name| name.to_ascii_lowercase())
            .collect();
        relations.sort_unstable();
        relations.dedup();
        Self { relations }
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
        let Some(before) = line.get(..pos) else {
            return Ok((pos, Vec::new()));
        };
        let parsed: Result<Vec<Word>, CommandError> = Words::new(before).collect();
        let Ok(mut words) = parsed else {
            return Ok((pos, Vec::new()));
        };
        let (start, word) = match words.pop() {
            Some(word) if word.end == pos => (word.start, word.text),
            Some(word) => {
                words.push(word);
                (pos, String::new())
            }
            None => (pos, String::new()),
        };
        let preceding: Vec<&str> = words.iter().map(|word| word.text.as_str()).collect();
        let names: Vec<&str> = match preceding.as_slice() {
            [] => VERBS.iter().map(|&(spelling, _)| spelling).collect(),
            [verb] if verb.parse().is_ok_and(Verb::is_update) => {
                self.relations.iter().map(String::as_str).collect()
            }
            [verb, _] if verb.parse().is_ok_and(Verb::is_update) => {
                let candidates = word
                    .strip_prefix('@')
                    .map(complete_path)
                    .unwrap_or_default();
                return Ok((start, candidates));
            }
            _ => Vec::new(),
        };
        let prefix = word.to_ascii_lowercase();
        let candidates: Vec<Pair> = names
            .into_iter()
            .filter(|name| name.starts_with(&prefix))
            .map(|name| Pair {
                display: name.to_owned(),
                replacement: name.to_owned(),
            })
            .collect();
        Ok((start, candidates))
    }

    fn update(&self, line: &mut LineBuffer, start: usize, elected: &str, changes: &mut Changeset) {
        if let Some(range) = replacement_range(line.as_str(), start, line.pos()) {
            line.replace(range, elected, changes);
        }
    }
}

impl Helper for Completion {}

impl Hinter for Completion {
    type Hint = String;
}

impl Highlighter for Completion {}

impl Validator for Completion {}

/// Returns the entire argument's bounds, including text after the cursor,
/// or `None` for invalid byte positions or a cursor past that argument.
#[must_use]
fn replacement_range(line: &str, start: usize, pos: usize) -> Option<Range<usize>> {
    line.get(start..pos)?;
    let end = match Words::new(line.get(start..)?).next() {
        Some(Ok(word)) if word.start == 0 => start + word.end,
        Some(Ok(_)) | Some(Err(_)) | None => pos,
    };
    if end < pos {
        return None;
    }
    Some(start..end)
}

/// Lists matching paths and encodes each as one shell argument.
#[must_use]
fn complete_path(path: &str) -> Vec<Pair> {
    // Rustyline applies different quoting and escape rules. Listing here
    // keeps paths literal and uses the command parser's quoting, at the
    // cost of maintaining directory enumeration and platform case matching.
    let start = path.rfind(is_separator).map_or(0, |index| index + 1);
    let (directory, prefix) = path.split_at(start);
    let lookup = if directory.is_empty() { "." } else { directory };
    let Ok(entries) = fs::read_dir(lookup) else {
        return Vec::new();
    };
    let mut candidates = Vec::new();
    let entries = entries.flatten();
    for entry in entries {
        let Ok(name) = entry.file_name().into_string() else {
            continue;
        };
        let matches = if cfg!(any(windows, target_os = "macos")) {
            name.to_lowercase().starts_with(&prefix.to_lowercase())
        } else {
            name.starts_with(prefix)
        };
        if !matches {
            continue;
        }
        let Ok(metadata) = fs::metadata(entry.path()) else {
            continue;
        };
        let mut argument = format!("@{directory}{name}");
        if metadata.is_dir() {
            argument.push(MAIN_SEPARATOR);
        }
        candidates.push(Pair {
            display: name,
            replacement: quote_word(&argument),
        });
    }
    candidates.sort_by(|left, right| left.display.cmp(&right.display));
    candidates
}

#[cfg(test)]
mod tests {
    use rstest::rstest;
    use rustyline::history::DefaultHistory;

    use super::*;
    use crate::shell::command::Command;
    use crate::txn::Rows;
    use crate::txn::TxnOp;

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
    #[case::nothing_inside_an_open_quote("insert \"x e", 7, &[])]
    #[case::quoted_relation("insert \"ed", 7, &["edge"])]
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

    // Cases: line, cursor position.
    #[rstest]
    #[case::past_end("insert e", 9)]
    #[case::inside_character("insert\u{a0}e", 7)]
    fn invalid_cursor_offers_no_completion(#[case] line: &str, #[case] pos: usize) {
        assert!(complete_at(line, pos).1.is_empty());
    }

    #[test]
    fn an_unfinished_quoted_escape_offers_no_completion() {
        let line = "insert edge \"@data\\";
        assert!(complete_at(line, line.len()).1.is_empty());
    }

    #[test]
    fn a_missing_directory_offers_no_paths() {
        let dir = tempfile::tempdir_in(".").unwrap();
        let prefix = dir.path().join("missing").join("ed");
        let line = format!("insert edge @{}", prefix.display());
        assert!(complete_at(&line, line.len()).1.is_empty());
    }

    /// Replacing the full argument preserves its `@` marker.
    #[test]
    fn a_word_after_at_completes_to_a_path() {
        // Relative paths keep TMPDIR and checkout names out of completion.
        let dir = tempfile::tempdir_in(".").expect("temp dir");
        fs::write(dir.path().join("edges.csv"), "").expect("write file");
        fs::create_dir(dir.path().join("edits")).expect("create dir");
        fs::write(dir.path().join("other.csv"), "").expect("write file");
        let prefix = dir.path().join("ed");
        let line = format!("insert edge @{}", prefix.display());
        let start = "insert edge ".len();
        let expected = vec![
            format!("@{}", dir.path().join("edges.csv").display()),
            format!("@{}{MAIN_SEPARATOR}", dir.path().join("edits").display()),
        ];
        assert_eq!(complete_at(&line, line.len()), (start, expected));
    }

    /// Applies candidates using the range used by `Completer::update`.
    /// Rustyline's private `Changeset` constructor prevents calling that
    /// method directly from tests.
    fn completed_lines(line: &str, pos: usize) -> Vec<String> {
        let (start, replacements) = complete_at(line, pos);
        let range = replacement_range(line, start, pos).unwrap();
        replacements
            .into_iter()
            .map(|replacement| {
                let mut completed = line.to_owned();
                completed.replace_range(range.clone(), &replacement);
                completed
            })
            .collect()
    }

    /// Rustyline's `Changeset` constructor is private, so validate rejected
    /// replacement bounds through the helper used by `Completer::update`.
    // Cases: line, start, cursor position.
    #[rstest]
    #[case::reversed("insert edge", 8, 7)]
    #[case::start_past_end("insert edge", 12, 12)]
    #[case::cursor_past_end("insert edge", 7, 12)]
    #[case::start_inside_character("\u{e9}dge", 1, 5)]
    #[case::cursor_inside_character("\u{e9}dge", 0, 1)]
    #[case::cursor_past_argument("insert edge", 0, 8)]
    fn invalid_replacement_bounds_are_rejected(
        #[case] line: &str,
        #[case] start: usize,
        #[case] pos: usize,
    ) {
        assert_eq!(replacement_range(line, start, pos), None);
    }

    #[test]
    fn completing_a_quoted_relation_preserves_the_tuple() {
        assert_eq!(
            completed_lines("insert \"ed\" 1,2", "insert \"ed".len()),
            vec!["insert edge 1,2"],
        );
    }

    // Cases: filename.
    #[rstest]
    #[case("data(1).csv")]
    #[case("data 1.csv")]
    #[case("data'1.csv")]
    #[case("data\u{a0}1.csv")]
    #[cfg_attr(unix, case("data\"1.csv"))]
    #[cfg_attr(unix, case("data\\1.csv"))]
    #[cfg_attr(unix, case("data \\t\\n\\x.csv"))]
    #[cfg_attr(unix, case("data\t\n.csv"))]
    fn completed_filename_parses_to_the_actual_path(#[case] name: &str) {
        let dir = tempfile::tempdir_in(".").unwrap();
        let path = dir.path().join(name);
        fs::write(&path, "").unwrap();
        let line = format!("insert edge @{}", dir.path().join("data").display());
        let completed = completed_lines(&line, line.len());
        assert_eq!(completed.len(), 1);
        assert_eq!(
            Command::parse(&completed[0]),
            Ok(Some(Command::Update(TxnOp::Insert {
                rel: "edge".to_owned(),
                rows: Rows::File(path),
            })))
        );
    }

    // Cases: head, tail, cursor offset into the tail.
    #[rstest]
    #[case::quote_before_at("insert edge \"@", "", 0)]
    #[case::quote_after_at("insert edge @\"", "", 0)]
    #[case::closed_quote("insert edge \"@", "\"", 1)]
    #[case::cursor_before_closing_quote("insert edge @\"", "\"", 0)]
    #[case::cursor_inside_filename("insert edge \"@", ".csv\" ", 0)]
    fn quoted_path_completion_roundtrips(
        #[case] head: &str,
        #[case] tail: &str,
        #[case] advance: usize,
    ) {
        let dir = tempfile::tempdir_in(".").unwrap();
        let directory = dir.path().join("my data");
        fs::create_dir(&directory).unwrap();
        let path = directory.join("ed.csv");
        fs::write(&path, "").unwrap();
        let prefix = directory
            .join("ed")
            .display()
            .to_string()
            .replace('\\', "\\\\");
        let line = format!("{head}{prefix}{tail}");
        let completed = completed_lines(&line, head.len() + prefix.len() + advance);
        assert_eq!(completed.len(), 1);
        assert_eq!(
            Command::parse(&completed[0]),
            Ok(Some(Command::Update(TxnOp::Insert {
                rel: "edge".to_owned(),
                rows: Rows::File(path),
            })))
        );
    }

    // Cases: text before the path.
    #[rstest]
    #[case("insert edge \"value @")]
    #[case("insert edge \"value \\\" @")]
    #[case("insert edge 1 @")]
    #[case("help @")]
    #[case("frob edge @")]
    fn filenames_are_not_offered_outside_a_file_argument(#[case] head: &str) {
        let dir = tempfile::tempdir_in(".").unwrap();
        fs::write(dir.path().join("edges.csv"), "").unwrap();
        let line = format!("{head}{}", dir.path().join("ed").display());
        assert!(complete_at(&line, line.len()).1.is_empty());
    }
}
