// prompt.rs
//
// Rustyline-based prompt for FlowLog incremental mode.
// - Tab completion: commands + extra words + filenames
// - In-memory command history
//
// Requires:
//   - Cargo.toml: rustyline = "17"

use crate::cmd::{self, Cmd};

use rustyline::completion::{Completer, FilenameCompleter, Pair};
use rustyline::error::ReadlineError;
use rustyline::highlight::Highlighter;
use rustyline::hint::Hinter;
use rustyline::history::DefaultHistory;
use rustyline::validate::Validator;
use rustyline::{Context, Editor, Helper};

struct FlowLogHelper {
    file: FilenameCompleter,
    words: Vec<String>, // lowercased commands + extra words (e.g., relations)
}

impl Helper for FlowLogHelper {}
impl Validator for FlowLogHelper {}
impl Highlighter for FlowLogHelper {}
impl Hinter for FlowLogHelper {
    type Hint = String;
}

impl Completer for FlowLogHelper {
    type Candidate = Pair;

    fn complete(
        &self,
        line: &str,
        pos: usize,
        ctx: &Context<'_>,
    ) -> rustyline::Result<(usize, Vec<Pair>)> {
        // Find token start (whitespace-split, matches cmd.rs parsing style).
        let start = line[..pos]
            .rfind(|c: char| c.is_whitespace())
            .map(|i| i + 1)
            .unwrap_or(0);
        let token = &line[start..pos];
        let token_lc = token.to_ascii_lowercase();

        // 1) Word completion (commands + extra words)
        let mut out: Vec<Pair> = self
            .words
            .iter()
            .filter(|w| w.starts_with(&token_lc))
            .map(|w| Pair {
                display: w.clone(),
                replacement: w.clone(),
            })
            .collect();

        // 2) Filename completion (merge)
        if let Ok((_fs_start, mut fs)) = self.file.complete(line, pos, ctx) {
            out.append(&mut fs);
        }

        // De-dup
        out.sort_by(|a, b| a.replacement.cmp(&b.replacement));
        out.dedup_by(|a, b| a.replacement == b.replacement);

        Ok((start, out))
    }
}

/// Interactive prompt state (owns the rustyline editor).
pub struct Prompt {
    rl: Editor<FlowLogHelper, DefaultHistory>,
}

impl Prompt {
    /// Create a prompt with optional extra completion words (e.g., relation names).
    pub fn new(extra_words: impl IntoIterator<Item = String>) -> Self {
        // Commands/aliases from your cmd.rs help text (case-insensitive).
        let mut words: Vec<String> = vec![
            "txn", "begin", "put", "file", "commit", "done", "abort", "rollback", "quit", "exit",
            "q", "help", "h", "?",
        ]
        .into_iter()
        .map(|s| s.to_string())
        .collect();

        words.extend(extra_words);

        // Normalize to lowercase for matching.
        let words: Vec<String> = words.into_iter().map(|s| s.to_ascii_lowercase()).collect();

        let helper = FlowLogHelper {
            file: FilenameCompleter::new(),
            words,
        };

        // Pin the Editor type so rustyline knows which History type to use.
        let mut rl: Editor<FlowLogHelper, DefaultHistory> =
            Editor::new().expect("rustyline editor");
        rl.set_helper(Some(helper));

        Self { rl }
    }

    /// Read one command from the user.
    ///
    /// - returns None on empty line or Ctrl-C (caller can continue loop)
    /// - returns Some(Cmd::Quit) on EOF / Ctrl-D or readline error
    /// - otherwise parses via cmd::parse_line
    pub fn next_cmd(&mut self) -> Option<Cmd> {
        match self.rl.readline(">> ") {
            Ok(line) => {
                let t = line.trim();
                if t.is_empty() {
                    return None;
                }
                // In-memory history only
                let _ = self.rl.add_history_entry(t);
                cmd::parse_line(&line)
            }
            Err(ReadlineError::Interrupted) => None, // Ctrl-C
            Err(ReadlineError::Eof) => Some(Cmd::Quit),
            Err(_) => Some(Cmd::Quit),
        }
    }
}
