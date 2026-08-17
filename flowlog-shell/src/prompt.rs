//! The prompt a compiled program reads commands at.
//!
//! [`Prompt`] owns a rustyline editor and hands each line to
//! [`parse_line`](crate::cmd::parse_line). Completion offers the shell's own
//! commands, the relation names the caller supplies, and filenames, so
//! `file` finds a path and `put` finds a relation.

use rustyline::Context;
use rustyline::Editor;
use rustyline::Helper;
use rustyline::completion::Completer;
use rustyline::completion::FilenameCompleter;
use rustyline::completion::Pair;
use rustyline::error::ReadlineError;
use rustyline::highlight::Highlighter;
use rustyline::hint::Hinter;
use rustyline::history::DefaultHistory;
use rustyline::validate::Validator;

use crate::cmd;
use crate::cmd::Cmd;

/// Completion over the shell's words plus the filesystem.
struct ShellHelper {
    file: FilenameCompleter,
    /// Commands and relation names, lowercased, as completion matches them.
    words: Vec<String>,
}

impl Helper for ShellHelper {}
impl Validator for ShellHelper {}
impl Highlighter for ShellHelper {}
impl Hinter for ShellHelper {
    type Hint = String;
}

impl Completer for ShellHelper {
    type Candidate = Pair;

    /// Offer both word and filename candidates for the token under the
    /// cursor, deduplicated so a name that is also a path appears once.
    fn complete(
        &self,
        line: &str,
        pos: usize,
        ctx: &Context<'_>,
    ) -> rustyline::Result<(usize, Vec<Pair>)> {
        // Split on whitespace alone, which is enough because completion runs
        // on the token being typed: a quoted run is only ambiguous once it is
        // closed, and by then there is nothing left to complete.
        let start = line[..pos].rfind(char::is_whitespace).map_or(0, |i| i + 1);
        let token = line[start..pos].to_ascii_lowercase();

        let mut out: Vec<Pair> = self
            .words
            .iter()
            .filter(|w| w.starts_with(&token))
            .map(|w| Pair {
                display: w.clone(),
                replacement: w.clone(),
            })
            .collect();

        if let Ok((_, paths)) = self.file.complete(line, pos, ctx) {
            out.extend(paths);
        }

        out.sort_by(|a, b| a.replacement.cmp(&b.replacement));
        out.dedup_by(|a, b| a.replacement == b.replacement);

        Ok((start, out))
    }
}

/// A prompt over one program's relations, holding its own history.
pub struct Prompt {
    editor: Editor<ShellHelper, DefaultHistory>,
}

impl Prompt {
    /// Open a prompt completing `relations` alongside the shell's commands.
    ///
    /// # Panics
    ///
    /// If the terminal cannot be opened for editing, which leaves the caller
    /// nothing to read commands from.
    #[must_use]
    pub fn new(relations: impl IntoIterator<Item = String>) -> Self {
        let words = cmd::COMMANDS
            .iter()
            .map(|command| command.to_ascii_lowercase())
            .chain(relations.into_iter().map(|mut name| {
                name.make_ascii_lowercase();
                name
            }))
            .collect();

        let helper = ShellHelper {
            file: FilenameCompleter::new(),
            words,
        };
        let mut editor = Editor::new().expect("rustyline editor");
        editor.set_helper(Some(helper));
        Self { editor }
    }

    /// Read one command, prompting with the current time `t`.
    ///
    /// `None` when there is nothing to do and the caller should read again: a
    /// blank line, an unreadable one, or Ctrl-C. End of input and a terminal
    /// error both read as [`Cmd::Quit`], because neither leaves anything more
    /// to read.
    pub fn next_cmd(&mut self, t: u32) -> Option<Cmd> {
        match self.editor.readline(&format!("[t={t}] >> ")) {
            Ok(line) => {
                // Both of these refuse a blank line on their own, so it needs
                // no guard here.
                let _ = self.editor.add_history_entry(line.trim());
                cmd::parse_line(&line)
            }
            Err(ReadlineError::Interrupted) => None,
            Err(_) => Some(Cmd::Quit),
        }
    }
}
