//! The interactive shell of incremental executables.
//!
//! [`Shell::next_request`] reads lines, with editing, in-memory history, and
//! tab completion, until one asks the driver to act: to commit the staged
//! transaction, or to quit. The shell answers every other line itself.
//! `command` parses a line, `completion` completes one, and `Session` stages
//! updates between commits.

use rustyline::Editor;
use rustyline::error::ReadlineError;
use rustyline::history::DefaultHistory;

use crate::error::RuntimeError;
use crate::shell::command::Command;
use crate::shell::command::HELP;
use crate::shell::completion::Completion;
use crate::txn::TxnOp;

mod command;
mod completion;

// =============================================================================
// Shell
// =============================================================================

/// What the driver of a [`Shell`] does next.
#[derive(Debug, PartialEq, Eq)]
pub enum Request {
    /// Applies the updates, in order, as one transaction, then advances
    /// time. The list is empty for a transaction opened and committed
    /// without updates.
    Commit(Vec<TxnOp>),
    /// Ends the session, discarding any open transaction.
    Quit,
}

/// Reads commands from the terminal, or from piped input, and stages the
/// updates they name until a commit.
pub struct Shell {
    editor: Editor<Completion, DefaultHistory>,
    session: Session,
}

impl Shell {
    /// Returns a shell whose updates name the input relations `relations`,
    /// which tab completion offers. `check` vets each update before it is
    /// staged: `None` for an unknown relation, or the error a relation
    /// refuses it with.
    ///
    /// # Panics
    ///
    /// Never in practice: rustyline opens an editor on every platform it
    /// supports, whether or not the input is a terminal.
    #[must_use]
    pub fn new(relations: &[&str], check: fn(&TxnOp) -> Option<Result<(), RuntimeError>>) -> Self {
        let mut editor: Editor<Completion, DefaultHistory> =
            Editor::new().expect("rustyline opens an editor on every platform it supports");
        editor.set_helper(Some(Completion::new(relations)));
        Self {
            editor,
            session: Session {
                check,
                staged: None,
            },
        }
    }

    /// Reads lines until one asks the driver to act, and returns that
    /// request. The prompt shows `time`, the engine's current time.
    ///
    /// A blank line or Ctrl-C reads on. A malformed line, or an update its
    /// relation refuses, is reported on stderr, and the shell reads on. The
    /// end of the input quits, as does input that cannot be read, which is
    /// reported first.
    #[must_use]
    pub fn next_request(&mut self, time: u32) -> Request {
        let prompt = format!("[t={time}] >> ");
        loop {
            let line = match self.editor.readline(&prompt) {
                Ok(line) => line,
                Err(ReadlineError::Interrupted) => continue,
                Err(ReadlineError::Eof) => return Request::Quit,
                Err(error) => {
                    eprintln!("cannot read the next command: {error}");
                    return Request::Quit;
                }
            };
            let Some(parsed) = Command::parse(&line).transpose() else {
                continue;
            };
            // History is a convenience that the in-memory store never fails
            // at. A malformed line is kept too, to be recalled and fixed.
            let _ = self.editor.add_history_entry(line.trim());
            let command = match parsed {
                Ok(command) => command,
                Err(error) => {
                    eprintln!("{error}");
                    continue;
                }
            };
            match self.session.handle(command) {
                Reply::Note(note) => println!("{note}"),
                Reply::Refusal(reason) => eprintln!("{reason}"),
                Reply::Request(request) => return request,
            }
        }
    }
}

// =============================================================================
// Session
// =============================================================================

/// The transaction that the shell's commands build.
struct Session {
    /// Vets an update before it is staged, as [`Shell::new`] describes.
    check: fn(&TxnOp) -> Option<Result<(), RuntimeError>>,
    /// The open transaction's updates, in command order, or `None` outside
    /// a transaction.
    staged: Option<Vec<TxnOp>>,
}

impl Session {
    /// Runs `command` against the open transaction.
    fn handle(&mut self, command: Command) -> Reply {
        match command {
            Command::Begin => {
                self.staged = Some(Vec::new());
                Reply::Note("(txn begin)")
            }
            // An update its relation cannot accept is refused before it is
            // staged: like a typo, it is a mistake in the command, not in
            // the data, so the session goes on.
            Command::Update(op) => match (self.check)(&op) {
                Some(Ok(())) => {
                    self.staged.get_or_insert_default().push(op);
                    Reply::Note("(queued)")
                }
                Some(Err(error)) => Reply::Refusal(error.to_string()),
                None => Reply::Refusal(format!("unknown relation: '{}'", op.rel())),
            },
            Command::Commit => match self.staged.take() {
                Some(updates) => Reply::Request(Request::Commit(updates)),
                None => Reply::Note("(no active txn)"),
            },
            Command::Abort => {
                self.staged = None;
                Reply::Note("(txn aborted)")
            }
            Command::Help => Reply::Note(HELP),
            Command::Quit => Reply::Request(Request::Quit),
        }
    }
}

// =============================================================================
// Reply
// =============================================================================

/// The shell's answer to one command.
#[derive(Debug, PartialEq, Eq)]
enum Reply {
    /// Printed on stdout.
    Note(&'static str),
    /// Why the command was refused, printed on stderr. The session goes on.
    Refusal(String),
    /// Returned to the driver.
    Request(Request),
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;
    use crate::txn::Rows;

    /// Accepts updates to `edge`, refuses those to the static `fixed`, and
    /// knows no other relation.
    fn check(op: &TxnOp) -> Option<Result<(), RuntimeError>> {
        match op.rel() {
            "edge" => Some(Ok(())),
            "fixed" => Some(Err(RuntimeError::StaticRelation { relation: "fixed" })),
            _ => None,
        }
    }

    /// Returns the reply to each line in turn, from a fresh session.
    fn replies(lines: &[&str]) -> Vec<Reply> {
        let mut session = Session {
            check,
            staged: None,
        };
        lines
            .iter()
            .map(|line| {
                let command = Command::parse(line)
                    .expect("the test line is well-formed")
                    .expect("the test line is not blank");
                session.handle(command)
            })
            .collect()
    }

    fn insert(tuple: &str) -> TxnOp {
        TxnOp::Insert {
            rel: "edge".to_owned(),
            rows: Rows::Tuple(tuple.to_owned()),
        }
    }

    fn delete(tuple: &str) -> TxnOp {
        TxnOp::Delete {
            rel: "edge".to_owned(),
            rows: Rows::Tuple(tuple.to_owned()),
        }
    }

    fn commit(updates: Vec<TxnOp>) -> Reply {
        Reply::Request(Request::Commit(updates))
    }

    const QUEUED: Reply = Reply::Note("(queued)");
    const BEGUN: Reply = Reply::Note("(txn begin)");
    const ABORTED: Reply = Reply::Note("(txn aborted)");
    const NO_TXN: Reply = Reply::Note("(no active txn)");

    fn static_refusal() -> Reply {
        Reply::Refusal(
            "relation `fixed` is static and cannot change after its initial load".to_owned(),
        )
    }

    // Cases: the lines typed, the reply to each.
    #[rstest]
    #[case::an_update_opens_a_transaction(
        &["insert edge 1,2", "commit"],
        vec![QUEUED, commit(vec![insert("1,2")])],
    )]
    #[case::commit_without_a_transaction(&["commit"], vec![NO_TXN])]
    #[case::begin_then_commit_commits_nothing(&["begin", "commit"], vec![BEGUN, commit(vec![])])]
    #[case::begin_discards_staged_updates(
        &["insert edge 1,2", "begin", "commit"],
        vec![QUEUED, BEGUN, commit(vec![])],
    )]
    #[case::abort_discards_the_transaction(
        &["insert edge 1,2", "abort", "commit"],
        vec![QUEUED, ABORTED, NO_TXN],
    )]
    #[case::abort_without_a_transaction(&["abort"], vec![ABORTED])]
    #[case::refused_update_opens_no_transaction(
        &["insert fixed 1", "commit"],
        vec![static_refusal(), NO_TXN],
    )]
    #[case::unknown_relation_kept_as_typed(
        &["delete Nope 1", "commit"],
        vec![Reply::Refusal("unknown relation: 'Nope'".to_owned()), NO_TXN],
    )]
    #[case::refusal_keeps_the_open_transaction(
        &["insert edge 1,2", "insert fixed 1", "commit"],
        vec![QUEUED, static_refusal(), commit(vec![insert("1,2")])],
    )]
    #[case::updates_commit_in_command_order(
        &["delete edge 1,2", "insert edge 1,2", "commit"],
        vec![QUEUED, QUEUED, commit(vec![delete("1,2"), insert("1,2")])],
    )]
    #[case::commit_closes_the_transaction(
        &["insert edge 1,2", "commit", "commit"],
        vec![QUEUED, commit(vec![insert("1,2")]), NO_TXN],
    )]
    #[case::help(&["help"], vec![Reply::Note(HELP)])]
    #[case::quit_requests_shutdown(
        &["insert edge 1,2", "quit"],
        vec![QUEUED, Reply::Request(Request::Quit)],
    )]
    fn session_replies_to_each_command(#[case] lines: &[&str], #[case] expected: Vec<Reply>) {
        assert_eq!(replies(lines), expected);
    }
}
