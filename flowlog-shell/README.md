# flowlog-shell

The interactive shell a compiled [FlowLog](https://github.com/flowlog-rs/flowlog) program reads transactions at. Internal dependency of programs the FlowLog compiler emits in incremental mode; you typically don't depend on it directly.

A program compiled for incremental evaluation stays running and accepts updates one transaction at a time. This crate is the front end for that: a command language over `put` / `file` / `txn` / `commit` / `abort`, and a prompt with completion and history.

## Layout

- `cmd` — the command language: one input line as one `Cmd`. No terminal, so it is tested directly.
- `prompt` — the terminal: a `rustyline` editor completing commands, relation names, and filenames.

Batch programs have no shell, so they depend on neither this crate nor `rustyline`.

## License

Apache-2.0 — see [LICENSE](./LICENSE).
