//! The wire channel table: what a worker's dataflow moved, one row per
//! channel.
//!
//! In timely dataflow, a channel is the pipe between two operators: one
//! operator's output port feeding another operator's input port, inside
//! a scope. Timely logs each channel's topology (a `Channels` event) and
//! its traffic (`Messages` events); the profiled runtime accumulates
//! both into one table row.
//!
//! ```text
//! channels_worker_t{t}_{i}.log
//!     scope  src  src_port  tgt  tgt_port  batch  sent  recvd
//! ```
//!
//! - [`channels`]: parse that table into one worker's rows

use crate::Addr;

/// One channel as observed by a worker: topology from timely's `Channels`
/// event, volumes accumulated from its `Messages` events.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Channel {
    /// Path of the scope containing the channel.
    pub(crate) scope_addr: Addr,
    /// Source operator index within the scope; `0` is the scope boundary.
    pub(crate) src: u32,
    pub(crate) src_port: u32,
    /// Target operator index within the scope; `0` is the scope boundary.
    pub(crate) tgt: u32,
    pub(crate) tgt_port: u32,
    /// Whether the payload is arrangement batches rather than tuples.
    pub(crate) ships_batches: bool,
    /// Records sent / received on this worker. Batch handles, not tuples,
    /// when `ships_batches`.
    pub(crate) sent: i64,
    pub(crate) recvd: i64,
}

/// Parse one worker's channel table; malformed rows are skipped.
pub(crate) fn channels(text: &str) -> Vec<Channel> {
    text.lines().filter_map(channel_row).collect()
}

/// Parse one channel row; `None` for anything that is not a complete row.
fn channel_row(line: &str) -> Option<Channel> {
    // The scope address may contain internal spaces, so it splits off at
    // `]`; the remaining cells are fixed-arity.
    let line = line.trim();
    if !line.starts_with('[') {
        return None;
    }
    let close = line.find(']')?;
    let scope_addr: Addr = line[..=close].parse().ok()?;

    let cells: Vec<&str> = line[close + 1..].split_whitespace().collect();
    let [src, src_port, tgt, tgt_port, batch, sent, recvd] = cells.as_slice() else {
        return None;
    };
    Some(Channel {
        scope_addr,
        src: src.parse().ok()?,
        src_port: src_port.parse().ok()?,
        tgt: tgt.parse().ok()?,
        tgt_port: tgt_port.parse().ok()?,
        ships_batches: match *batch {
            "0" => false,
            "1" => true,
            _ => return None,
        },
        sent: sent.parse().ok()?,
        recvd: recvd.parse().ok()?,
    })
}

#[cfg(test)]
pub(crate) mod test_support {
    use std::collections::BTreeMap;

    use super::Channel;
    use crate::Addr;

    pub(crate) fn addr(path: &[u32]) -> Addr {
        Addr(path.to_vec())
    }

    pub(crate) fn chan(
        scope: &[u32],
        (src, src_port): (u32, u32),
        (tgt, tgt_port): (u32, u32),
        batches: bool,
        sent: i64,
        recvd: i64,
    ) -> Channel {
        Channel {
            scope_addr: addr(scope),
            src,
            src_port,
            tgt,
            tgt_port,
            ships_batches: batches,
            sent,
            recvd,
        }
    }

    pub(crate) fn names(pairs: &[(&[u32], &str)]) -> BTreeMap<Addr, String> {
        pairs
            .iter()
            .map(|(p, n)| (addr(p), n.to_string()))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn channel_row_parses_topology_and_volumes() {
        let c = channel_row(
            "[0, 14]              1     0         6     1         1      1234         1234",
        )
        .unwrap();
        assert_eq!(c.scope_addr, Addr(vec![0, 14]));
        assert_eq!((c.src, c.src_port, c.tgt, c.tgt_port), (1, 0, 6, 1));
        assert!(c.ships_batches);
        assert_eq!((c.sent, c.recvd), (1234, 1234));
    }

    #[test]
    fn truncated_channel_row_is_skipped() {
        assert!(channel_row("[0, 14]   1   0   6").is_none());
    }

    #[test]
    fn channel_row_with_unknown_batch_flag_is_skipped() {
        assert!(channel_row("[0, 14]   1   0   6   1   2   10   10").is_none());
    }
}
