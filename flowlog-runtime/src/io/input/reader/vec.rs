//! Host-supplied typed rows as a reader.
//!
//! The engine holds one flat `Vec` of host tuples per relation; workers
//! share it read-only, each opening its index range the way a text reader
//! opens a byte range. A row is one tuple, converted per position — the
//! host already parsed everything, so nothing here can fail.

use std::marker::PhantomData;

use crate::error::RuntimeError;
use crate::io::input::decode::Decode;
use crate::io::input::reader::Reader;
use crate::io::spec::InputSpec;

/// A worker's cursor over its index range of one relation's rows.
pub(crate) struct VecReader<'a, U, T> {
    rows: &'a [U],
    next: usize,
    end: usize,
    slot: PhantomData<T>,
}

impl<'src, U, T: Decode<U>> Reader<'src, T> for VecReader<'src, U, T> {
    type Source = [U];

    /// The share is the index range `[len*index/peers, len*(index+1)/peers)`,
    /// so the workers' shares cover the rows exactly once. The conversion
    /// interns on the worker, so under `uses_ord` it collapses to worker 0
    /// per [`Reader`]'s rule.
    fn open(spec: &InputSpec<'src, [U]>) -> Result<Option<Self>, RuntimeError> {
        let rows = spec.source;
        let (peers, index) = if spec.rel.uses_ord {
            if spec.index != 0 {
                return Ok(None);
            }
            (1, 0)
        } else {
            (spec.peers, spec.index)
        };
        let len = rows.len();
        let next = len * index / peers;
        let end = len * (index + 1) / peers;
        Ok((next < end).then_some(Self {
            rows,
            next,
            end,
            slot: PhantomData,
        }))
    }

    fn next(&mut self) -> Result<Option<Result<T, RuntimeError>>, RuntimeError> {
        let Some(row) = self.rows.get(self.next).filter(|_| self.next < self.end) else {
            return Ok(None);
        };
        self.next += 1;
        Ok(Some(T::decode(row)))
    }
}

#[cfg(test)]
mod tests {
    use lasso::Spur;
    use ordered_float::OrderedFloat;

    use super::*;
    use crate::intern::intern;
    use crate::io::Format;
    use crate::io::spec::RelationSpec;
    use crate::io::spec::ShardKey;

    const fn rel(uses_ord: bool) -> RelationSpec {
        RelationSpec {
            name: "R",
            arity: 4,
            delim: b',',
            format: Format::Text {
                delim: b',',
                has_header: false,
            },
            shard: ShardKey::Int,
            uses_ord,
        }
    }

    static R: RelationSpec = rel(false);
    static R_ORD: RelationSpec = rel(true);

    fn rows() -> Vec<(i64, String, bool, f64)> {
        (0..10)
            .map(|i| (i, format!("s{i}"), i % 2 == 0, i as f64 / 2.0))
            .collect()
    }

    type Slot = (i64, Spur, bool, OrderedFloat<f64>);

    fn decode_share(
        data: &[(i64, String, bool, f64)],
        ord: bool,
        peers: usize,
        index: usize,
    ) -> Vec<Slot> {
        let spec = InputSpec {
            rel: if ord { &R_ORD } else { &R },
            source: data,
            peers,
            index,
        };
        let mut seen = Vec::new();
        if let Some(mut r) = <VecReader<'_, _, Slot>>::open(&spec).expect("open") {
            while let Some(row) = r.next().expect("cursor") {
                seen.push(row.expect("typed rows cannot fail"));
            }
        }
        seen
    }

    /// Every worker converts its own share, and together the shares cover
    /// the rows exactly once, in order within each share.
    #[test]
    fn shares_cover_the_rows_exactly_once() {
        let data = rows();
        for peers in [1, 3, 4, 11] {
            let mut seen = Vec::new();
            for index in 0..peers {
                seen.extend(decode_share(&data, false, peers, index));
            }
            let expect: Vec<Slot> = data
                .iter()
                .map(|(i, s, b, f)| (*i, intern(s), *b, OrderedFloat(*f)))
                .collect();
            assert_eq!(seen, expect, "peers={peers}");
        }
    }

    /// Under ord, worker 0 takes the whole share and the rest take none,
    /// so interning order cannot vary with worker count.
    #[test]
    fn ord_collapses_the_scan_to_worker_zero() {
        let data = rows();
        assert!(decode_share(&data, true, 4, 1).is_empty());
        assert_eq!(decode_share(&data, true, 4, 0).len(), data.len());
    }

    /// An empty relation opens as no share on every worker.
    #[test]
    fn empty_rows_open_as_no_share() {
        let data: Vec<(i64, String, bool, f64)> = Vec::new();
        assert!(decode_share(&data, false, 2, 0).is_empty());
        assert!(decode_share(&data, false, 2, 1).is_empty());
    }
}
