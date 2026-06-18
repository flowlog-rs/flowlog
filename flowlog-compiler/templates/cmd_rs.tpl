// cmd.rs template for the incremental-mode REPL driver.

use std::path::PathBuf;

use ::flowlog_runtime::txn::Diff;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Cmd {
    Begin, // txn / begin
    Put {
        rel: String,
        tuple: String,
        diff: Diff,
    },
    File {
        rel: String,
        path: PathBuf,
        diff: Diff,
    },
    Commit, // commit / done
    Abort,  // abort / rollback
    Quit,
    Help,
}

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
        Use boolean tuples to toggle presence:
          put <rel> True    # insert (diff = +1)
          put <rel> False   # delete (diff = -1)
        For nullary relations, any [diff] you provide is ignored.

  file <rel> <path> [diff]
      Apply updates from CSV file <path> to relation <rel>.
      [diff] defaults to +1.

      Nullary relations (arity 0):
        File ingestion is not supported; use `put <rel> True|False`.

  commit, done
      Commit the transaction and advance time.

  abort, rollback
      Abort the transaction (discard staged updates).

  help, h, ?
      Show this help text.

  quit, exit, q
      Exit."#
}

fn usage_put() -> &'static str {
    "usage: put <rel> <tuple> [diff]"
}
fn usage_file() -> &'static str {
    "usage: file <rel> <path> [diff]"
}

/// Print an error and return None.
fn err(msg: impl AsRef<str>) -> Option<Cmd> {
    eprintln!("invalid {}", msg.as_ref());
    None
}

fn parse_diff(maybe: Option<&str>) -> Option<Diff> {
    match maybe {
        None => Some(1),
        Some(s) => match s.parse::<Diff>() {
            Ok(d) => Some(d),
            Err(_) => {
                eprintln!("invalid diff: '{s}' (expected an integer like +1, -1, 2)");
                None
            }
        },
    }
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

        "put" => {
            if parts.len() < 3 || parts.len() > 4 {
                return err(usage_put());
            }
            let rel = parts[1].to_string();
            let tuple = parts[2].to_string();
            let diff = parse_diff(parts.get(3).copied())?;
            Some(Cmd::Put { rel, tuple, diff })
        }

        "file" => {
            if parts.len() < 3 || parts.len() > 4 {
                return err(usage_file());
            }
            let rel = parts[1].to_string();
            let path = PathBuf::from(parts[2]);
            let diff = parse_diff(parts.get(3).copied())?;
            Some(Cmd::File { rel, path, diff })
        }

        _ => err(format!(
            "unknown command: '{}'. Type 'help' to see commands.",
            parts[0]
        )),
    }
}
