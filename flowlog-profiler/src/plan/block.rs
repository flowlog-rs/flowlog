//! [`Block`], the dataflow region a node belongs to.

use std::fmt;
use std::str::FromStr;

use serde::Deserialize;
use serde::Serialize;

use crate::ProfilerError;

/// Which part of the dataflow a node lives in. Serialized in `ops.json`
/// as its `Display` form (`input`, `stratum N`, `inspect`).
#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Default)]
#[serde(into = "String", try_from = "String")]
pub enum Block {
    /// EDB loading, before any stratum runs.
    #[default]
    Input,
    /// One stratum of the rule program.
    Stratum(usize),
    /// Output inspection after the last stratum.
    Inspect,
}

impl fmt::Display for Block {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Block::Input => f.write_str("input"),
            Block::Stratum(id) => write!(f, "stratum {id}"),
            Block::Inspect => f.write_str("inspect"),
        }
    }
}

impl From<Block> for String {
    fn from(block: Block) -> String {
        block.to_string()
    }
}

impl FromStr for Block {
    type Err = ProfilerError;

    /// Parses the `Display` form back.
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "input" => Ok(Block::Input),
            "inspect" => Ok(Block::Inspect),
            _ => s
                .strip_prefix("stratum ")
                .and_then(|id| id.parse().ok())
                .map(Block::Stratum)
                .ok_or_else(|| ProfilerError::ParseBlock(format!("unknown block label: {s}"))),
        }
    }
}

impl TryFrom<String> for Block {
    type Error = ProfilerError;

    fn try_from(s: String) -> Result<Self, Self::Error> {
        s.parse()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[rstest::rstest]
    #[case(Block::Input)]
    #[case(Block::Stratum(7))]
    #[case(Block::Inspect)]
    fn display_and_parse_round_trip(#[case] block: Block) {
        assert_eq!(block.to_string().parse::<Block>().unwrap(), block);
    }

    #[test]
    fn unknown_label_is_rejected() {
        let Err(ProfilerError::ParseBlock(msg)) = "stratum x".parse::<Block>() else {
            panic!("expected ParseBlock");
        };
        assert_eq!(msg, "unknown block label: stratum x");
    }
}
