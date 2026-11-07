//! Parsing one line of shell input into a [`Command`].
//!
//! [`split`] cuts a line into words, a [`Verb`] spelled as in [`VERBS`]
//! opens every command, and [`HELP`] documents them all.

use std::path::PathBuf;
use std::str::FromStr;

use thiserror::Error;

use crate::txn::Rows;
use crate::txn::TxnOp;

// =============================================================================
// Command
// =============================================================================

/// One line of shell input.
#[derive(Debug, PartialEq, Eq)]
pub(super) enum Command {
    /// Opens a transaction, discarding the open one's updates.
    Begin,
    /// Stages one update, opening a transaction if none is open.
    Update(TxnOp),
    /// Commits the open transaction.
    Commit,
    /// Discards the open transaction.
    Abort,
    /// Shows [`HELP`].
    Help,
    /// Ends the session, discarding the open transaction.
    Quit,
}

impl Command {
    /// Parses `line`, or returns `None` for a line without words. Help and
    /// quit ignore any words after the verb.
    pub(super) fn parse(line: &str) -> Result<Option<Self>, CommandError> {
        let words = split(line)?;
        let Some((first, args)) = words.split_first() else {
            return Ok(None);
        };
        let verb: Verb = first.parse()?;
        let command = match (verb, args) {
            (Verb::Begin, []) => Some(Self::Begin),
            (Verb::Commit, []) => Some(Self::Commit),
            (Verb::Abort, []) => Some(Self::Abort),
            (Verb::Help, _) => Some(Self::Help),
            (Verb::Quit, _) => Some(Self::Quit),
            (Verb::Insert, _) => {
                update_args(args).map(|(rel, rows)| Self::Update(TxnOp::Insert { rel, rows }))
            }
            (Verb::Delete, _) => {
                update_args(args).map(|(rel, rows)| Self::Update(TxnOp::Delete { rel, rows }))
            }
            (Verb::Begin | Verb::Commit | Verb::Abort, _) => None,
        };
        command
            .map(Some)
            .ok_or_else(|| CommandError::Usage(verb.usage()))
    }
}

/// Returns the relation and rows that an update's arguments,
/// `<rel> [<tuple> | @<path>]`, name, or `None` for any other number of
/// words. Without a last word, the update names the empty tuple: the fact
/// of a nullary relation.
fn update_args(args: &[String]) -> Option<(String, Rows)> {
    let (rel, rows) = match args {
        [rel] => (rel, Rows::Tuple(String::new())),
        [rel, word] => match word.strip_prefix('@') {
            Some(path) => (rel, Rows::File(PathBuf::from(path))),
            None => (rel, Rows::Tuple(word.clone())),
        },
        _ => return None,
    };
    Some((rel.clone(), rows))
}

// =============================================================================
// Words
// =============================================================================

/// Splits `line` into words at whitespace.
///
/// A `"` opens or closes a quoted run, even inside a word, and is dropped.
/// A run keeps its whitespace, and in it `\t`, `\n`, `\\`, and `\"`
/// unescape while any other `\` stays as written; outside a run, `\` is an
/// ordinary character. A run carries a tuple whose values hold spaces, such
/// as a tab-delimited DOOP `_LoadInstanceField` row, as one word.
pub(super) fn split(line: &str) -> Result<Vec<String>, CommandError> {
    let mut words = Vec::new();
    // The word being read, or `None` between words.
    let mut word: Option<String> = None;
    let mut quoted = false;
    let mut chars = line.chars();
    while let Some(c) = chars.next() {
        match c {
            '"' => {
                quoted = !quoted;
                word.get_or_insert_default();
            }
            '\\' if quoted => {
                let text = word.get_or_insert_default();
                match chars.next() {
                    Some('t') => text.push('\t'),
                    Some('n') => text.push('\n'),
                    Some(escaped @ ('\\' | '"')) => text.push(escaped),
                    Some(other) => {
                        text.push('\\');
                        text.push(other);
                    }
                    None => return Err(CommandError::TrailingBackslash),
                }
            }
            c if c.is_whitespace() && !quoted => words.extend(word.take()),
            c => word.get_or_insert_default().push(c),
        }
    }
    if quoted {
        return Err(CommandError::UnterminatedQuote);
    }
    words.extend(word);
    Ok(words)
}

// =============================================================================
// Verb
// =============================================================================

/// The word that opens a command.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Verb {
    Begin,
    Insert,
    Delete,
    Commit,
    Abort,
    Help,
    Quit,
}

/// Every spelling of every verb, in the order [`HELP`] lists them.
pub(super) const VERBS: &[(&str, Verb)] = &[
    ("txn", Verb::Begin),
    ("begin", Verb::Begin),
    ("insert", Verb::Insert),
    ("delete", Verb::Delete),
    ("commit", Verb::Commit),
    ("done", Verb::Commit),
    ("abort", Verb::Abort),
    ("rollback", Verb::Abort),
    ("help", Verb::Help),
    ("h", Verb::Help),
    ("?", Verb::Help),
    ("quit", Verb::Quit),
    ("exit", Verb::Quit),
    ("q", Verb::Quit),
];

impl Verb {
    /// Returns the syntax of the verb's command, which a usage error quotes.
    fn usage(self) -> &'static str {
        match self {
            Self::Begin => "txn",
            Self::Insert => "insert <rel> [<tuple> | @<path>]",
            Self::Delete => "delete <rel> [<tuple> | @<path>]",
            Self::Commit => "commit",
            Self::Abort => "abort",
            Self::Help => "help",
            Self::Quit => "quit",
        }
    }
}

impl FromStr for Verb {
    type Err = CommandError;

    /// Parses any spelling in [`VERBS`], ignoring ASCII case.
    fn from_str(word: &str) -> Result<Self, Self::Err> {
        VERBS
            .iter()
            .find(|(spelling, _)| spelling.eq_ignore_ascii_case(word))
            .map(|&(_, verb)| verb)
            .ok_or_else(|| CommandError::Unknown(word.to_owned()))
    }
}

// =============================================================================
// Help
// =============================================================================

/// The shell's help text.
pub(super) const HELP: &str = r#"Usage:
  txn | begin
  insert <rel> [<tuple> | @<path>]
  delete <rel> [<tuple> | @<path>]
  commit | done
  abort | rollback
  help | h | ?
  quit | exit | q

Commands:
  txn, begin
      Begin a transaction.

  insert <rel> <tuple>
  delete <rel> <tuple>
      Insert or delete one row of relation <rel>.
      <tuple> is comma-separated (e.g., 1,2 or 7). A relation is a set:
      inserting a present row changes nothing, one delete removes a row
      however often it was inserted, and deleting an absent row changes
      nothing. Within one transaction a row's inserts and deletes cancel
      and the surplus decides.

      Quote a tuple to preserve internal spaces; \t inside quotes is a
      column tab (for tab-delimited relations like DOOP):
        delete _loadinstancefield "<base>\t<field>\t<to>\t<method>"
      A tuple cannot begin with `@`, which marks a path.

  insert <rel> @<path>
  delete <rel> @<path>
      Insert or delete every row of the CSV file at <path>.

  insert <rel>
  delete <rel>
      Assert or retract the fact of a nullary relation (arity 0).

      A static relation refuses every command. An append relation
      accepts insert only and refuses delete.

  commit, done
      Commit the transaction and advance time.

  abort, rollback
      Abort the transaction (discard staged updates).

  help, h, ?
      Show this help text.

  quit, exit, q
      Exit.

Tab completes a command, a relation after insert or delete, and a path
after @."#;

// =============================================================================
// CommandError
// =============================================================================

/// Why a line is not a command. The shell reports it and reads on.
#[derive(Debug, PartialEq, Eq, Error)]
pub(super) enum CommandError {
    #[error("unterminated quote: the line ends inside a quoted word")]
    UnterminatedQuote,
    #[error("trailing backslash: the line ends inside a quoted word, after a `\\`")]
    TrailingBackslash,
    /// The words after the verb do not fit its syntax.
    #[error("usage: {0}")]
    Usage(&'static str),
    /// The first word spells no verb; kept as typed.
    #[error("unknown command: '{0}'. Type 'help' to see commands.")]
    Unknown(String),
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use rstest::rstest;

    use super::*;

    fn insert(rel: &str, rows: Rows) -> Command {
        Command::Update(TxnOp::Insert {
            rel: rel.to_owned(),
            rows,
        })
    }

    fn delete(rel: &str, rows: Rows) -> Command {
        Command::Update(TxnOp::Delete {
            rel: rel.to_owned(),
            rows,
        })
    }

    fn tuple(text: &str) -> Rows {
        Rows::Tuple(text.to_owned())
    }

    fn file(path: &str) -> Rows {
        Rows::File(PathBuf::from(path))
    }

    /// Walks the table, so a spelling added to it is covered as it lands.
    #[test]
    fn every_verb_spelling_parses_in_any_case() {
        for &(spelling, verb) in VERBS {
            assert_eq!(spelling.parse::<Verb>(), Ok(verb), "{spelling}");
            let upper = spelling.to_ascii_uppercase();
            assert_eq!(upper.parse::<Verb>(), Ok(verb), "{upper}");
        }
    }

    /// The help text is written by hand, so it must keep up with the table.
    #[test]
    fn help_lists_every_verb_spelling() {
        let words: HashSet<&str> = HELP
            .split(|c: char| !c.is_ascii_alphanumeric() && c != '?')
            .collect();
        for (spelling, _) in VERBS {
            assert!(words.contains(spelling), "HELP omits `{spelling}`");
        }
    }

    #[rstest]
    #[case::begin("txn", Command::Begin)]
    #[case::verb_in_any_case("BeGiN", Command::Begin)]
    #[case::words_amid_whitespace(" \tdone  ", Command::Commit)]
    #[case::abort("rollback", Command::Abort)]
    #[case::help_ignores_more_words("help me", Command::Help)]
    #[case::quit_ignores_more_words("q now", Command::Quit)]
    #[case::insert_a_tuple("insert edge 1,2", insert("edge", tuple("1,2")))]
    #[case::relation_kept_as_typed("DELETE Edge 1,2", delete("Edge", tuple("1,2")))]
    #[case::nullary_fact("insert flag", insert("flag", tuple("")))]
    #[case::file_after_at("insert edge @data/e.csv", insert("edge", file("data/e.csv")))]
    #[case::quoted_file_after_at(
        r#"delete edge "@my data.csv""#,
        delete("edge", file("my data.csv"))
    )]
    fn line_parses_to_its_command(#[case] line: &str, #[case] expected: Command) {
        assert_eq!(Command::parse(line), Ok(Some(expected)));
    }

    // Cases: the tuple word as typed, the tuple it names.
    #[rstest]
    #[case::quotes_keep_spaces(r#""a b""#, "a b")]
    #[case::quotes_join_mid_word(r#"x"y z"w"#, "xy zw")]
    #[case::empty_quotes_are_a_word(r#""""#, "")]
    #[case::escapes_in_quotes(r#""\t\n\\\"""#, "\t\n\\\"")]
    #[case::unknown_escape_stays(r#""\x""#, "\\x")]
    #[case::backslash_outside_quotes_stays(r"a\tb", "a\\tb")]
    #[case::any_whitespace_separates("1,2\u{a0}", "1,2")]
    fn quoting_shapes_the_tuple(#[case] word: &str, #[case] expected: &str) {
        let line = format!("insert r {word}");
        assert_eq!(
            Command::parse(&line),
            Ok(Some(insert("r", tuple(expected))))
        );
    }

    #[rstest]
    #[case::empty("")]
    #[case::whitespace(" \t ")]
    fn a_line_without_words_is_no_command(#[case] line: &str) {
        assert_eq!(Command::parse(line), Ok(None));
    }

    #[rstest]
    #[case::unterminated_quote(r#"insert r "1,2"#, CommandError::UnterminatedQuote)]
    #[case::trailing_backslash(r#"insert r "1,2\"#, CommandError::TrailingBackslash)]
    #[case::quoting_before_verb(r#"frob "x"#, CommandError::UnterminatedQuote)]
    #[case::begin_with_words("begin now", CommandError::Usage("txn"))]
    #[case::commit_with_words("done r", CommandError::Usage("commit"))]
    #[case::abort_with_words("abort r", CommandError::Usage("abort"))]
    #[case::insert_without_relation(
        "insert",
        CommandError::Usage("insert <rel> [<tuple> | @<path>]")
    )]
    #[case::delete_with_extra_word(
        "delete r 1 2",
        CommandError::Usage("delete <rel> [<tuple> | @<path>]")
    )]
    #[case::unknown_verb_kept_as_typed("Frob r", CommandError::Unknown("Frob".to_owned()))]
    #[case::empty_verb(r#""" r"#, CommandError::Unknown(String::new()))]
    fn malformed_line_is_refused(#[case] line: &str, #[case] expected: CommandError) {
        assert_eq!(Command::parse(line), Err(expected));
    }
}
