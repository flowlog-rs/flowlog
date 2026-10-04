// cmd.rs template for the incremental-mode REPL driver.

use std::path::PathBuf;

use ::flowlog_runtime::txn::Rows;
use ::flowlog_runtime::txn::TxnOp;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Cmd {
    Begin, // txn / begin
    Op(TxnOp),
    Commit, // commit / done
    Abort,  // abort / rollback
    Quit,
    Help,
}

pub fn help_text() -> &'static str {
    r#"Usage:
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
      Exit."#
}

fn usage(verb: &str) -> String {
    format!("usage: {verb} <rel> [<tuple> | @<path>]")
}

/// Print an error and return None.
fn err(msg: impl AsRef<str>) -> Option<Cmd> {
    eprintln!("invalid {}", msg.as_ref());
    None
}

/// Shell-style tokenizer: whitespace separates tokens, but a `"..."` run is a
/// single token whose interior whitespace is preserved. Inside quotes,
/// `\t` `\n` `\\` `\"` unescape — this is how a tuple whose columns are
/// tab-delimited and whose values contain spaces (a DOOP `_LoadInstanceField`
/// row) reaches `apply_tuple` as one `<tuple>` argument.
fn tokenize(line: &str) -> Result<Vec<String>, String> {
    let mut toks = Vec::new();
    let mut cur = String::new();
    let (mut in_tok, mut quoted) = (false, false);
    let mut chars = line.chars();
    while let Some(c) = chars.next() {
        match c {
            '"' => { quoted = !quoted; in_tok = true; }
            '\\' if quoted => match chars.next() {
                Some('t')  => cur.push('\t'),
                Some('n')  => cur.push('\n'),
                Some('\\') => cur.push('\\'),
                Some('"')  => cur.push('"'),
                Some(o)    => { cur.push('\\'); cur.push(o); }
                None       => return Err("trailing backslash".into()),
            },
            c if c.is_whitespace() && !quoted => {
                if in_tok { toks.push(std::mem::take(&mut cur)); in_tok = false; }
            }
            c => { cur.push(c); in_tok = true; }
        }
    }
    if quoted { return Err("unterminated quote".into()); }
    if in_tok { toks.push(cur); }
    Ok(toks)
}

/// Parse one input line into an optional Cmd.
/// - Empty line => None (caller should do nothing)
/// - Tokens are whitespace-split; quote a token to preserve interior spaces
///   and use `\t` for embedded tabs (see `tokenize`).
/// - On invalid input => prints an error and returns None.
pub fn parse_line(line: &str) -> Option<Cmd> {
    let line = line.trim();
    if line.is_empty() {
        return None;
    }

    let parts: Vec<String> = match tokenize(line) {
        Ok(p) => p,
        Err(e) => return err(e),
    };
    if parts.is_empty() {
        return None;
    }
    let parts: Vec<&str> = parts.iter().map(String::as_str).collect();

    let head = parts[0].to_ascii_lowercase();

    match head.as_str() {
        "q" | "quit" | "exit" => Some(Cmd::Quit),
        "help" | "h" | "?" => Some(Cmd::Help),

        "abort" | "rollback" => {
            if parts.len() != 1 {
                return err("usage: abort");
            }
            Some(Cmd::Abort)
        }

        "commit" | "done" => {
            if parts.len() != 1 {
                return err("usage: commit");
            }
            Some(Cmd::Commit)
        }

        "txn" | "begin" => {
            if parts.len() != 1 {
                return err("usage: txn");
            }
            Some(Cmd::Begin)
        }

        "insert" | "delete" => {
            if parts.len() < 2 || parts.len() > 3 {
                return err(usage(&head));
            }
            let rel = parts[1].to_string();
            // A nullary relation's fact is the empty tuple.
            let rows = match parts.get(2) {
                None => Rows::Tuple(String::new()),
                Some(arg) => match arg.strip_prefix('@') {
                    Some(path) => Rows::File(PathBuf::from(path)),
                    None => Rows::Tuple((*arg).to_string()),
                },
            };
            Some(Cmd::Op(if head == "insert" {
                TxnOp::Insert { rel, rows }
            } else {
                TxnOp::Delete { rel, rows }
            }))
        }

        _ => err(format!(
            "unknown command: '{}'. Type 'help' to see commands.",
            parts[0]
        )),
    }
}
