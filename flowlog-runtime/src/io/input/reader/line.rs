//! One broadcast `put` line as a reader.
//!
//! Every worker receives every REPL line, so a share here is not a range:
//! `open` decodes column 0 and answers whether this worker owns the line,
//! which is the broadcast source's way of deriving a share. The ownership
//! hashes live here too: every worker computes the same answer for the
//! same value, and that agreement is what settles ownership without the
//! workers exchanging a message. The owner's cursor yields the line once.

use std::marker::PhantomData;

use lasso::Spur;

use crate::error::RuntimeError;
use crate::io::input::decode::Decode;
use crate::io::input::decode::text::DecodeCell;
use crate::io::input::decode::text::Line;
use crate::io::input::reader::Reader;
use crate::io::spec::InputSpec;
use crate::io::spec::ShardKey;

// FNV-1a, hand-rolled rather than reached for from `std`: ownership must
// come out the same on every worker and every run, and the standard hasher
// is seeded per process. FNV spreads short keys that share a prefix, which
// relation names and identifiers usually do.
const FNV_OFFSET_BASIS: u32 = 0x811c_9dc5;
const FNV_PRIME: u32 = 0x0100_0193;

/// Returns `true` if worker `index` owns a tuple whose first column is the
/// integer `first`.
fn shard_int(first: i64, peers: usize, index: usize) -> bool {
    // `rem_euclid` rather than `%`: a negative value takes the sign of the
    // dividend under `%`, which no worker index can match, so those tuples
    // would reach nobody.
    first.rem_euclid(peers as i64) as usize == index
}

/// Returns `true` if worker `index` owns a tuple whose first column is the
/// string `first`.
fn shard_str(first: &str, peers: usize, index: usize) -> bool {
    let mut hash = FNV_OFFSET_BASIS;
    for &byte in first.as_bytes() {
        hash ^= u32::from(byte);
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    (hash as usize) % peers == index
}

/// The first cell of `line`, trimmed.
fn first_cell(line: &str, delim: u8) -> &str {
    match line.as_bytes().iter().position(|&b| b == delim) {
        Some(i) => line[..i].trim(),
        None => line.trim(),
    }
}

/// The owning worker's cursor over one `put` line.
pub(crate) struct LineReader<'src, T> {
    line: &'src str,
    delim: u8,
    done: bool,
    slot: PhantomData<T>,
}

impl<'src, T: for<'l> Decode<Line<'l>>> Reader<'src, T> for LineReader<'src, T> {
    type Source = str;

    /// Answers ownership from column 0: the worker it hashes to under the
    /// relation's [`ShardKey`], or worker 0 under `uses_ord` (a put
    /// decodes strings, and one worker interning in command order is
    /// deterministic where racing workers are not). A line whose first
    /// column does not decode is an error on every worker; nothing was
    /// applied anywhere, and the caller owns the report.
    fn open(spec: &InputSpec<'src, str>) -> Result<Option<Self>, RuntimeError> {
        if spec.rel.uses_ord && spec.index != 0 {
            return Ok(None);
        }

        let line = spec.source.trim();
        let first = first_cell(line, spec.rel.delim);
        let (peers, index) = (spec.peers, spec.index);
        let owned = spec.rel.uses_ord
            || match spec.rel.shard {
                ShardKey::Int => shard_int(i64::decode_cell(first, 0, 0)?, peers, index),
                ShardKey::UInt => shard_int(u64::decode_cell(first, 0, 0)? as i64, peers, index),
                ShardKey::Bool => shard_int(bool::decode_cell(first, 0, 0)? as i64, peers, index),
                ShardKey::F32Bits => shard_int(
                    f32::decode_cell(first, 0, 0)?.to_bits() as i64,
                    peers,
                    index,
                ),
                ShardKey::F64Bits => shard_int(
                    f64::decode_cell(first, 0, 0)?.to_bits() as i64,
                    peers,
                    index,
                ),
                ShardKey::Str => shard_str(first, peers, index),
                ShardKey::Spur => {
                    let key = Spur::decode_cell(first, 0, 0)?;
                    (key.into_inner().get() as usize) % peers == index
                }
            };

        Ok(owned.then_some(Self {
            line,
            delim: spec.rel.delim,
            done: false,
            slot: PhantomData,
        }))
    }

    fn next(&mut self) -> Result<Option<Result<T, RuntimeError>>, RuntimeError> {
        if self.done {
            return Ok(None);
        }
        self.done = true;
        // A put has no position in a file; 0 marks it as such.
        Ok(Some(T::decode(&Line {
            text: self.line,
            delim: self.delim,
            position: 0,
        })))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::*;
    use crate::intern::intern;

    const PEERS: usize = 4;

    /// How many of `PEERS` workers claim the value `shard` closes over.
    fn owners(shard: impl Fn(usize) -> bool) -> usize {
        (0..PEERS).filter(|&index| shard(index)).count()
    }

    /// A broadcast tuple is applied once, so exactly one worker claims any
    /// integer -- including a negative one, which a plain remainder would
    /// send outside the worker range and so to nobody.
    #[test]
    fn every_integer_has_exactly_one_owner() {
        for first in [i64::MIN, -7, -1, 0, 1, 7, i64::MAX] {
            assert_eq!(owners(|index| shard_int(first, PEERS, index)), 1, "{first}");
        }
    }

    /// The same, for a string column: one claimant, empty string included.
    #[test]
    fn every_string_has_exactly_one_owner() {
        for first in ["", "a", "alpha", "a longer key with spaces"] {
            assert_eq!(
                owners(|index| shard_str(first, PEERS, index)),
                1,
                "{first:?}"
            );
        }
    }

    /// A lone worker owns everything, which is what lets a single-worker
    /// run load every tuple.
    #[test]
    fn a_lone_worker_owns_every_tuple() {
        assert!(shard_int(-3, 1, 0));
        assert!(shard_str("alpha", 1, 0));
    }

    /// Distinct strings reach every worker: sending them all to one worker
    /// would satisfy exactly-one-owner while serialising the load.
    #[test]
    fn strings_spread_over_every_worker() {
        let claimed: BTreeSet<usize> = (0..64)
            .map(|i| format!("key{i}"))
            .map(|key| {
                (0..PEERS)
                    .find(|&index| shard_str(&key, PEERS, index))
                    .expect("some worker owns it")
            })
            .collect();

        assert_eq!(claimed.len(), PEERS);
    }

    /// An interned first column is owned by exactly one worker too.
    #[test]
    fn every_interned_string_has_exactly_one_owner() {
        for first in ["shard_alpha", "shard_beta", "shard_gamma"] {
            let key = intern(first);
            let hash = key.into_inner().get() as usize;
            assert_eq!(owners(|index| hash % PEERS == index), 1, "{first:?}");
        }
    }
}
