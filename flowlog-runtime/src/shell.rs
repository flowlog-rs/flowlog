//! The interactive shell of incremental executables.
//!
//! [`Shell`] reads commands and returns a [`Request`] when a transaction
//! should commit or the session should end. `command` parses input,
//! `completion` completes it, and `Session` stages updates between commits.

use rustyline::Editor;
use rustyline::error::ReadlineError;
use rustyline::history::DefaultHistory;

use crate::error::RuntimeError;
use crate::shell::command::Command;
use crate::shell::command::HELP;
use crate::shell::completion::Completion;
use crate::txn::Request;
use crate::txn::TxnOp;

mod command;
mod completion;

// =============================================================================
// Shell
// =============================================================================

/// Reads commands from the terminal, or from piped input, and stages the
/// updates they name until a commit.
#[derive(Debug)]
pub struct Shell {
    editor: Editor<Completion, DefaultHistory>,
    session: Session,
}

impl Shell {
    /// Returns a shell whose updates name the input relations `relations`,
    /// which tab completion offers. `check` vets each update before it is
    /// staged: `None` for an unknown relation, `Some(Ok(()))` to accept it,
    /// or `Some(Err(_))` to refuse it.
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
#[derive(Debug)]
struct Session {
    /// Vets an update before it is staged, as [`Shell::new`] describes.
    check: fn(&TxnOp) -> Option<Result<(), RuntimeError>>,
    /// The open transaction's updates, in command order, or `None` outside
    /// a transaction.
    staged: Option<Vec<TxnOp>>,
}

impl Session {
    #[must_use]
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

    fn replies(commands: Vec<Command>) -> Vec<Reply> {
        let mut session = Session {
            check,
            staged: None,
        };
        commands
            .into_iter()
            .map(|command| session.handle(command))
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

    // Cases: commands, replies.
    #[rstest]
    #[case::an_update_opens_a_transaction(
        vec![Command::Update(insert("1,2")), Command::Commit],
        vec![QUEUED, commit(vec![insert("1,2")])],
    )]
    #[case::commit_without_a_transaction(vec![Command::Commit], vec![NO_TXN])]
    #[case::begin_then_commit_commits_nothing(
        vec![Command::Begin, Command::Commit],
        vec![BEGUN, commit(vec![])],
    )]
    #[case::begin_discards_staged_updates(
        vec![Command::Update(insert("1,2")), Command::Begin, Command::Commit],
        vec![QUEUED, BEGUN, commit(vec![])],
    )]
    #[case::abort_discards_the_transaction(
        vec![Command::Update(insert("1,2")), Command::Abort, Command::Commit],
        vec![QUEUED, ABORTED, NO_TXN],
    )]
    #[case::abort_without_a_transaction(vec![Command::Abort], vec![ABORTED])]
    #[case::refused_update_opens_no_transaction(
        vec![Command::Update(TxnOp::Insert {
            rel: "fixed".to_owned(),
            rows: Rows::Tuple("1".to_owned()),
        }), Command::Commit],
        vec![static_refusal(), NO_TXN],
    )]
    #[case::unknown_relation_kept_as_typed(
        vec![Command::Update(TxnOp::Delete {
            rel: "Nope".to_owned(),
            rows: Rows::Tuple("1".to_owned()),
        }), Command::Commit],
        vec![Reply::Refusal("unknown relation: 'Nope'".to_owned()), NO_TXN],
    )]
    #[case::refusal_keeps_the_open_transaction(
        vec![Command::Update(insert("1,2")), Command::Update(TxnOp::Insert {
            rel: "fixed".to_owned(),
            rows: Rows::Tuple("1".to_owned()),
        }), Command::Commit],
        vec![QUEUED, static_refusal(), commit(vec![insert("1,2")])],
    )]
    #[case::updates_commit_in_command_order(
        vec![Command::Update(delete("1,2")), Command::Update(insert("1,2")), Command::Commit],
        vec![QUEUED, QUEUED, commit(vec![delete("1,2"), insert("1,2")])],
    )]
    #[case::commit_closes_the_transaction(
        vec![Command::Update(insert("1,2")), Command::Commit, Command::Commit],
        vec![QUEUED, commit(vec![insert("1,2")]), NO_TXN],
    )]
    #[case::help(vec![Command::Help], vec![Reply::Note(HELP)])]
    #[case::quit_requests_shutdown(
        vec![Command::Update(insert("1,2")), Command::Quit],
        vec![QUEUED, Reply::Request(Request::Quit)],
    )]
    fn session_replies_to_each_command(
        #[case] commands: Vec<Command>,
        #[case] expected: Vec<Reply>,
    ) {
        assert_eq!(replies(commands), expected);
    }
}
