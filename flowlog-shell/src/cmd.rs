//! The shell's command language: one input line as one [`Cmd`].
//!
//! [`parse_line`] is the whole surface. It tokenizes shell-style, so a
//! quoted run is one argument, then reads the head word as a command and
//! the rest as its arguments. A line it cannot read reports to stderr and
//! yields `None`, because a prompt keeps going where a file load would
//! stop.

use std::path::PathBuf;

/// How many copies of a tuple an update applies, matching the runtime's own
/// `txn::Diff`.
///
/// Spelled here rather than imported so the shell needs no dependency on the
/// runtime: it is a transparent alias either way, so a program's generated
/// code passes one where the other is expected. The shell exists only in
/// incremental mode, so this is signed and nothing here has to work for the
/// batch semiring.
pub type Diff = i32;

/// One command a shell line asks for.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Cmd {
    /// `txn` / `begin`
    Begin,
    /// `put <rel> <tuple> [diff]`
    Put {
        rel: String,
        tuple: String,
        diff: Diff,
    },
    /// `file <rel> <path> [diff]`
    File {
        rel: String,
        path: PathBuf,
        diff: Diff,
    },
    /// `commit` / `done`
    Commit,
    /// `abort` / `rollback`
    Abort,
    /// `quit` / `exit` / `q`
    Quit,
    /// `help` / `h` / `?`
    Help,
}

/// Every command and alias the shell answers to, for tab completion.
///
/// Kept beside the parser that reads them, so a command added to one is a
/// candidate in the other.
pub const COMMANDS: &[&str] = &[
    "txn", "begin", "put", "file", "commit", "done", "abort", "rollback", "quit", "exit", "q",
    "help", "h", "?",
];

/// The text `help` prints.
#[must_use]
pub fn help_text() -> &'static str {
    r#"Usage:
  txn | begin
  put  <rel> <tuple> [diff]
  file <rel> <path>  [diff]
  commit | done
  abort | rollback
  help | h | ?
  quit | exit | q

Commands:
  txn, begin
      Begin a transaction.

  put <rel> <tuple> [diff]
      Apply an update to relation <rel>.
      <tuple> is comma-separated (e.g., 1,2 or 7).
      [diff] defaults to +1.

      Quote a tuple to preserve internal spaces; \t inside quotes is a
      column tab (for tab-delimited relations like DOOP):
        put _loadinstancefield "<base>\t<field>\t<to>\t<method>" -1

      Nullary relations (arity 0):
        Toggle presence with a boolean tuple:
          put <rel> True    # assert
          put <rel> False   # retract
        [diff] scales it: `put <rel> False 3` retracts 3.

  file <rel> <path> [diff]
      Apply updates from CSV file <path> to relation <rel>.
      [diff] defaults to +1.

      A nullary relation reads a file as one assertion per line, so an
      empty file asserts nothing and a one-line file asserts once.

  commit, done
      Commit the transaction and advance time.

  abort, rollback
      Abort the transaction (discard staged updates).

  help, h, ?
      Show this help text.

  quit, exit, q
      Exit."#
}

/// Report why a line could not be read, and yield nothing.
///
/// Generic in the result so every refusal in this module routes through one
/// `eprintln!`, whatever it was refusing.
fn err<T>(msg: impl AsRef<str>) -> Option<T> {
    eprintln!("{}", msg.as_ref());
    None
}

/// A command that takes no arguments, or `usage` naming the one that does.
fn bare(args: &[String], cmd: Cmd, usage: &str) -> Option<Cmd> {
    if args.is_empty() {
        Some(cmd)
    } else {
        err(format!("usage: {usage}"))
    }
}

/// The `[diff]` argument, defaulting to `+1` when absent.
fn parse_diff(maybe: Option<&str>) -> Option<Diff> {
    let Some(s) = maybe else { return Some(1) };
    s.parse::<Diff>().ok().or_else(|| {
        err(format!(
            "invalid diff: '{s}', expected an integer like +1, -1, 2"
        ))
    })
}

/// Split a line into arguments, shell-style: whitespace separates them, but a
/// `"..."` run is one argument whose interior whitespace survives.
///
/// Inside quotes, `\t` `\n` `\\` `\"` unescape. That is how a tuple whose
/// columns are tab-delimited and whose values contain spaces (a DOOP
/// `_LoadInstanceField` row) arrives as a single `<tuple>` argument.
///
/// `Err` names what was left open: an unterminated quote or a trailing
/// backslash.
fn tokenize(line: &str) -> Result<Vec<String>, String> {
    let mut toks = Vec::new();
    // `Some` once an argument has started, which a quote alone is enough to
    // do: `""` is an empty argument, not an absent one.
    let mut cur: Option<String> = None;
    let mut quoted = false;
    let mut chars = line.chars();
    while let Some(c) = chars.next() {
        if c.is_whitespace() && !quoted {
            toks.extend(cur.take());
            continue;
        }
        let arg = cur.get_or_insert_default();
        match c {
            '"' => quoted = !quoted,
            '\\' if quoted => match chars.next() {
                Some('t') => arg.push('\t'),
                Some('n') => arg.push('\n'),
                Some('\\') => arg.push('\\'),
                Some('"') => arg.push('"'),
                // An escape FlowLog does not read stays as written, so a
                // path keeps its backslashes.
                Some(o) => {
                    arg.push('\\');
                    arg.push(o);
                }
                None => return Err("trailing backslash".into()),
            },
            c => arg.push(c),
        }
    }
    if quoted {
        return Err("unterminated quote".into());
    }
    toks.extend(cur);
    Ok(toks)
}

/// Read one input line as a command.
///
/// `None` for a line with nothing to do (blank) and for one that cannot be
/// read, which is reported to stderr first: a prompt carries on either way,
/// so the caller needs no distinction.
///
/// The head word is matched without regard to case; a relation name and a
/// tuple are passed through as written.
#[must_use]
pub fn parse_line(line: &str) -> Option<Cmd> {
    let mut parts = match tokenize(line.trim()) {
        Ok(parts) => parts,
        Err(open) => return err(open),
    };
    // A trimmed line yields at least one argument, so the head is present.
    let (head, args) = parts.split_first_mut()?;
    head.make_ascii_lowercase();

    match head.as_str() {
        "q" | "quit" | "exit" => Some(Cmd::Quit),
        "help" | "h" | "?" => Some(Cmd::Help),
        "abort" | "rollback" => bare(args, Cmd::Abort, "abort"),
        "commit" | "done" => bare(args, Cmd::Commit, "commit"),
        "txn" | "begin" => bare(args, Cmd::Begin, "txn"),

        // The arity is the pattern, so the optional `[diff]` needs no count:
        // a third argument is the tail, and a fourth fails to match.
        "put" => match args {
            [rel, tuple, diff @ ..] if diff.len() <= 1 => Some(Cmd::Put {
                diff: parse_diff(diff.first().map(String::as_str))?,
                rel: std::mem::take(rel),
                tuple: std::mem::take(tuple),
            }),
            _ => err("usage: put <rel> <tuple> [diff]"),
        },

        "file" => match args {
            [rel, path, diff @ ..] if diff.len() <= 1 => Some(Cmd::File {
                diff: parse_diff(diff.first().map(String::as_str))?,
                rel: std::mem::take(rel),
                path: PathBuf::from(std::mem::take(path)),
            }),
            _ => err("usage: file <rel> <path> [diff]"),
        },

        _ => err(format!(
            "unknown command: '{head}'. Type 'help' to see commands."
        )),
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    /// The arguments a line splits into, or the message it failed with.
    fn toks(line: &str) -> Result<Vec<String>, String> {
        tokenize(line)
    }

    // --- tokenize ---

    /// Whitespace separates arguments, and runs of it collapse.
    #[test]
    fn whitespace_separates_arguments() {
        assert_eq!(toks("put R 1,2 +1").unwrap(), ["put", "R", "1,2", "+1"]);
        assert_eq!(toks("  put   R   1,2  ").unwrap(), ["put", "R", "1,2"]);
    }

    /// A quoted run is one argument and keeps its spaces, which is the only
    /// way to put a value containing one.
    #[test]
    fn a_quoted_run_keeps_its_spaces() {
        assert_eq!(
            toks(r#"put R "hello world",x"#).unwrap(),
            ["put", "R", "hello world,x"]
        );
    }

    /// The quotes group; they are not part of the value, so a quoted and an
    /// unquoted spelling of the same characters agree.
    #[test]
    fn quotes_group_without_becoming_data() {
        assert_eq!(toks(r#"put R "1","2""#).unwrap(), ["put", "R", "1,2"]);
        assert_eq!(toks("put R 1,2").unwrap(), ["put", "R", "1,2"]);
    }

    /// A quoted empty run is an empty argument, not an absent one: it is
    /// what distinguishes "an argument has started" from "the buffer is
    /// empty", and the only input that tells the two apart.
    #[test]
    fn a_quoted_empty_run_is_an_empty_argument() {
        assert_eq!(toks(r#"put R """#).unwrap(), ["put", "R", ""]);
        assert_eq!(toks("put R").unwrap(), ["put", "R"], "no third argument");
    }

    /// Every escape the quoted alphabet reads.
    #[rstest]
    #[case(r#""a\tb""#, "a\tb")]
    #[case(r#""a\nb""#, "a\nb")]
    #[case(r#""a\\b""#, "a\\b")]
    #[case(r#""a\"b""#, "a\"b")]
    fn quoted_escapes_decode(#[case] arg: &str, #[case] expected: &str) {
        assert_eq!(toks(&format!("put R {arg}")).unwrap()[2], expected);
    }

    /// An escape FlowLog does not know keeps both characters rather than
    /// being refused, so a Windows path survives a quoted argument.
    #[test]
    fn an_unknown_escape_keeps_its_backslash() {
        assert_eq!(toks(r#"put R "a\db""#).unwrap()[2], r"a\db");
    }

    /// Outside quotes a backslash is data, so no escape is read.
    #[test]
    fn a_backslash_outside_quotes_is_data() {
        assert_eq!(toks(r"put R a\tb").unwrap()[2], r"a\tb");
    }

    /// What was left open is named, rather than reported as a wrong argument
    /// count further on.
    #[rstest]
    #[case(r#"put R "unterminated"#, "unterminated quote")]
    #[case(r#"put R "trailing\"#, "trailing backslash")]
    fn an_unclosed_argument_says_what_is_open(#[case] line: &str, #[case] message: &str) {
        assert_eq!(toks(line).unwrap_err(), message);
    }

    /// Parens are not syntax here: they are data, and a space still splits.
    #[test]
    fn parentheses_are_data_not_grouping() {
        assert_eq!(toks("put R (1,2)").unwrap(), ["put", "R", "(1,2)"]);
        assert_eq!(
            toks("put R (1, 2)").unwrap(),
            ["put", "R", "(1,", "2)"],
            "the space splits, so this reaches `parse_line` as too many arguments"
        );
    }

    // --- parse_line ---

    /// Nothing to do, and nothing reported.
    #[rstest]
    #[case("")]
    #[case("   ")]
    fn a_blank_line_is_no_command(#[case] line: &str) {
        assert_eq!(parse_line(line), None);
    }

    /// Every word and alias that names a command with no arguments.
    #[rstest]
    #[case("txn", Cmd::Begin)]
    #[case("begin", Cmd::Begin)]
    #[case("commit", Cmd::Commit)]
    #[case("done", Cmd::Commit)]
    #[case("abort", Cmd::Abort)]
    #[case("rollback", Cmd::Abort)]
    #[case("quit", Cmd::Quit)]
    #[case("exit", Cmd::Quit)]
    #[case("q", Cmd::Quit)]
    #[case("help", Cmd::Help)]
    #[case("h", Cmd::Help)]
    #[case("?", Cmd::Help)]
    fn each_alias_names_its_command(#[case] line: &str, #[case] expected: Cmd) {
        assert_eq!(parse_line(line), Some(expected));
    }

    /// A command word is matched however it was cased.
    #[rstest]
    #[case("TXN", Cmd::Begin)]
    #[case("Commit", Cmd::Commit)]
    #[case("QuIt", Cmd::Quit)]
    fn a_command_word_is_case_insensitive(#[case] line: &str, #[case] expected: Cmd) {
        assert_eq!(parse_line(line), Some(expected));
    }

    /// `put` keeps the relation name and tuple as written, and defaults the
    /// diff to an assertion.
    #[test]
    fn put_defaults_to_asserting_once() {
        assert_eq!(
            parse_line("put Edge 1,2"),
            Some(Cmd::Put {
                rel: "Edge".to_string(),
                tuple: "1,2".to_string(),
                diff: 1,
            })
        );
    }

    /// A diff is read as written, including a sign and a magnitude past one.
    #[rstest]
    #[case("put R x +1", 1)]
    #[case("put R x 1", 1)]
    #[case("put R x -1", -1)]
    #[case("put R x 3", 3)]
    #[case("put R x -3", -3)]
    fn a_diff_carries_its_sign_and_magnitude(#[case] line: &str, #[case] expected: Diff) {
        let Some(Cmd::Put { diff, .. }) = parse_line(line) else {
            panic!("expected a put")
        };
        assert_eq!(diff, expected);
    }

    /// `file` reads its argument as a path rather than a tuple.
    #[test]
    fn file_reads_a_path() {
        assert_eq!(
            parse_line("file Edge data/edge.csv -1"),
            Some(Cmd::File {
                rel: "Edge".to_string(),
                path: PathBuf::from("data/edge.csv"),
                diff: -1,
            })
        );
    }

    /// Every shape refused: an unknown word, a command given arguments it
    /// takes none of, too few or too many arguments, and a diff that is not
    /// an integer.
    #[rstest]
    #[case("bogus")]
    #[case("txn now")]
    #[case("commit now")]
    #[case("abort now")]
    #[case("put")]
    #[case("put R")]
    #[case("put R 1,2 +1 extra")]
    #[case("put R 1,2 notanumber")]
    #[case("file R")]
    #[case("file R p.csv +1 extra")]
    fn an_unreadable_line_yields_no_command(#[case] line: &str) {
        assert_eq!(parse_line(line), None);
    }

    /// The completion words and the parser agree, so no word is offered that
    /// the parser would call unknown. Some take arguments and some refuse
    /// them, so a word counts as reached if either spelling parses.
    #[test]
    fn every_completion_word_reaches_a_command() {
        for word in COMMANDS {
            let bare = parse_line(word).is_some();
            let with_args = parse_line(&format!("{word} R x")).is_some();
            assert!(
                bare || with_args,
                "`{word}` completes but no spelling of it parses"
            );
        }
    }

    /// And the other direction, which is the one that silently loses a
    /// command: an alias the parser accepts but completion never offers.
    /// Checked against the aliases the help text names, since that is the
    /// list a user reads.
    #[test]
    fn every_alias_the_help_names_is_a_completion_word() {
        for alias in COMMANDS {
            assert!(
                help_text().contains(alias),
                "`{alias}` completes but the help text never names it"
            );
        }
        // The help text's own usage block, as the aliases it lists.
        for alias in help_text()
            .lines()
            .take_while(|l| !l.starts_with("Commands:"))
            .flat_map(|l| l.split_whitespace())
            .filter(|w| w.chars().all(|c| c.is_ascii_lowercase() || c == '?'))
            .filter(|w| !w.is_empty())
        {
            assert!(
                COMMANDS.contains(&alias),
                "the help text names `{alias}`, which completion never offers"
            );
        }
    }
}
