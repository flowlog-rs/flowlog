#![no_main]

use flowlog_common::Config;
use flowlog_common::ExecutionMode;
use flowlog_error::SourceMap;
use flowlog_parser::parse;
use libfuzzer_sys::fuzz_target;

// Fuzz the Datalog parser end-to-end. Arbitrary input must never panic: a
// malformed program is expected to return a `ParseError`, not crash. The
// input is written to an isolated temp file so `.include` directives resolve
// against an empty directory: their failures are correct behavior, not bugs.
fuzz_target!(|data: &[u8]| {
    let Ok(src) = std::str::from_utf8(data) else {
        return;
    };
    let Ok(dir) = tempfile::tempdir() else {
        return;
    };
    let path = dir.path().join("fuzz.dl");
    if std::fs::write(&path, src).is_err() {
        return;
    }
    let Some(path_str) = path.to_str() else {
        return;
    };
    let mut sm = SourceMap::new();
    let mut config = Config {
        mode: ExecutionMode::ExtendBatch,
        ..Config::default()
    };
    let _ = parse(path_str, &[], &mut sm, &mut config);
});
