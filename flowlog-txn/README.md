# flowlog-shell

The interactive shell a compiled [FlowLog](https://github.com/flowlog-rs/flowlog) program reads transactions at. Internal dependency of programs the FlowLog compiler emits in incremental mode; you typically don't depend on it directly.

A program compiled for incremental evaluation stays running and accepts updates one transaction at a time. This crate is the front end for that: a command language over `put` / `file` / `txn` / `commit` / `abort`, the epoch protocol that runs each committed transaction across the program's workers, and a prompt with completion and history.

## Layout

- `cmd` — what a line means: a control word, or an op to stage (`Cmd`, `TxnOp`). No terminal, so it is tested directly.
- `driver` — how a commit runs: one worker `drive`s at the prompt, every other worker `follow`s, and the program answers `Event`s for what only it knows.
- `prompt` — the terminal: a `rustyline` editor completing commands, relation names, and filenames. Behind the default `prompt` feature.

Batch programs have no shell, so they depend on neither this crate nor `rustyline`. A library-mode host depends on it with `default-features = false`: its generated engine speaks `txn` but never opens a prompt.

## License

Apache-2.0 — see [LICENSE](./LICENSE).
