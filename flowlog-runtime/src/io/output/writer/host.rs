//! Moving converted rows into the final library result.
//!
//! [`HostWriter`] owns the vector or nullary scalar returned on completion.
//! [`HostResult`] initializes that storage with a row capacity hint.
//! Batch snapshots and weighted deltas use separate implementations.

use std::convert::Infallible;

use crate::io::Relation;
use crate::io::output::encode::Encode;
use crate::io::output::encode::typed::TypedEncoder;
use crate::io::output::writer::Writer;

// =============================================================================
// HostResult
// =============================================================================

/// Initializes an empty host result, reserving space only for vectors.
pub trait HostResult {
    fn with_capacity(rows: usize) -> Self;
}

impl<D> HostResult for Vec<D> {
    #[inline]
    fn with_capacity(rows: usize) -> Self {
        Vec::with_capacity(rows)
    }
}

impl HostResult for bool {
    #[inline]
    fn with_capacity(_: usize) -> Self {
        false
    }
}

impl HostResult for i32 {
    #[inline]
    fn with_capacity(_: usize) -> Self {
        0
    }
}

// =============================================================================
// HostWriter
// =============================================================================

/// Accumulates the final result without retaining a second row collection.
#[derive(Debug)]
pub struct HostWriter<S> {
    result: S,
}

impl<S> HostWriter<S> {
    /// Takes final result storage without clearing or reserving it.
    pub(in crate::io::output) fn new(result: S) -> Self {
        Self { result }
    }
}

impl<R: Relation, T, D> Writer<R, T> for HostWriter<Vec<D>>
where
    TypedEncoder: Encode<R::Tuple, Output = D>,
{
    type Output = Vec<D>;
    type Error = Infallible;

    #[inline]
    fn write_row(&mut self, (row, ..): (R::Tuple, T, i32)) -> Result<(), Infallible> {
        self.result.push(TypedEncoder.encode(row));
        Ok(())
    }

    fn write_batch(&mut self, batches: &mut [Vec<(R::Tuple, T, i32)>]) -> Result<(), Infallible> {
        for rows in batches {
            self.result
                .extend(rows.drain(..).map(|(row, ..)| TypedEncoder.encode(row)));
        }
        Ok(())
    }

    fn finish(self) -> Result<Self::Output, Infallible> {
        Ok(self.result)
    }
}

impl<R: Relation, T, D> Writer<R, T, true> for HostWriter<Vec<(D, i32)>>
where
    TypedEncoder: Encode<R::Tuple, Output = D>,
{
    type Output = Vec<(D, i32)>;
    type Error = Infallible;

    #[inline]
    fn write_row(&mut self, (row, _, diff): (R::Tuple, T, i32)) -> Result<(), Infallible> {
        self.result.push((TypedEncoder.encode(row), diff));
        Ok(())
    }

    fn write_batch(&mut self, batches: &mut [Vec<(R::Tuple, T, i32)>]) -> Result<(), Infallible> {
        for rows in batches {
            self.result.extend(
                rows.drain(..)
                    .map(|(row, _, diff)| (TypedEncoder.encode(row), diff)),
            );
        }
        Ok(())
    }

    fn finish(self) -> Result<Self::Output, Infallible> {
        Ok(self.result)
    }
}

impl<R: Relation<Tuple = ()>, T> Writer<R, T> for HostWriter<bool> {
    type Output = bool;
    type Error = Infallible;

    fn write_row(&mut self, _: ((), T, i32)) -> Result<(), Infallible> {
        self.result = true;
        Ok(())
    }

    fn write_batch(&mut self, batches: &mut [Vec<((), T, i32)>]) -> Result<(), Infallible> {
        // Presence needs only the first nonempty partition, not every update.
        self.result |= batches.iter().any(|rows| !rows.is_empty());
        Ok(())
    }

    fn finish(self) -> Result<bool, Infallible> {
        Ok(self.result)
    }
}

impl<R: Relation<Tuple = ()>, T> Writer<R, T, true> for HostWriter<i32> {
    type Output = i32;
    type Error = Infallible;

    fn write_row(&mut self, (_, _, diff): ((), T, i32)) -> Result<(), Infallible> {
        self.result += diff;
        Ok(())
    }

    fn write_batch(&mut self, batches: &mut [Vec<((), T, i32)>]) -> Result<(), Infallible> {
        for rows in batches {
            for (_, _, diff) in rows.drain(..) {
                self.result += diff;
            }
        }
        Ok(())
    }

    fn finish(self) -> Result<i32, Infallible> {
        Ok(self.result)
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use ordered_float::OrderedFloat;
    use rstest::rstest;

    use super::*;

    #[derive(Debug)]
    struct TestRelation<D, const ARITY: usize>(D);

    impl<D: differential_dataflow::Data, const ARITY: usize> Relation for TestRelation<D, ARITY> {
        const NAME: &'static str = "Out";
        const ARITY: usize = ARITY;
        type Tuple = D;
    }

    #[rstest]
    #[case::rows(false)]
    #[case::batch(true)]
    fn snapshot_rows_move_into_the_reserved_result_in_delivery_order(#[case] batch: bool) {
        type Rows = TestRelation<(i32, String, OrderedFloat<f64>), 3>;
        let label = String::from("owned");
        let label_storage = label.as_ptr();
        let result: Vec<_> = HostResult::with_capacity(2);
        let result_storage = result.as_ptr();
        let mut writer = HostWriter::new(result);
        let updates = [
            ((7, label, OrderedFloat(2.5)), (), 1),
            ((3, String::from("next"), OrderedFloat(1.0)), (), 1),
        ];
        if batch {
            let [first, second] = updates;
            let mut batches = [Vec::new(), vec![first], Vec::new(), vec![second]];
            let storage = batches.each_ref().map(Vec::as_ptr);
            Writer::<Rows, ()>::write_batch(&mut writer, &mut batches).unwrap();
            assert!(batches.iter().all(Vec::is_empty));
            assert_eq!(batches.each_ref().map(Vec::as_ptr), storage);
        } else {
            for update in updates {
                Writer::<Rows, ()>::write_row(&mut writer, update).unwrap();
            }
        }
        let result = Writer::<Rows, ()>::finish(writer).unwrap();
        assert_eq!(
            result,
            [
                (7, String::from("owned"), 2.5),
                (3, String::from("next"), 1.0)
            ],
        );
        assert_eq!(result[0].1.as_ptr(), label_storage);
        assert_eq!(result.as_ptr(), result_storage);
    }

    #[rstest]
    #[case::rows(false)]
    #[case::batch(true)]
    fn deltas_preserve_retractions_zeroes_duplicates_and_order(#[case] batch: bool) {
        type Rows = TestRelation<(i32,), 1>;
        let mut writer = HostWriter::new(Vec::with_capacity(4));
        if batch {
            let mut batches = [
                vec![((7,), (), -2), ((7,), (), 0)],
                Vec::new(),
                vec![((3,), (), 5), ((7,), (), 2)],
            ];
            Writer::<Rows, (), true>::write_batch(&mut writer, &mut batches).unwrap();
            assert!(batches.iter().all(Vec::is_empty));
        } else {
            for (row, diff) in [((7,), -2), ((7,), 0), ((3,), 5), ((7,), 2)] {
                Writer::<Rows, (), true>::write_row(&mut writer, (row, (), diff)).unwrap();
            }
        }
        let result = Writer::<Rows, (), true>::finish(writer).unwrap();
        assert_eq!(result, [((7,), -2), ((7,), 0), ((3,), 5), ((7,), 2)]);
    }

    #[test]
    fn empty_results_remain_empty() {
        let mut rows = HostWriter::new(Vec::<(i32,)>::new());
        let mut flag = HostWriter::new(false);
        let mut diff = HostWriter::new(0i32);
        Writer::<TestRelation<(i32,), 1>, ()>::write_batch(&mut rows, &mut [Vec::new()]).unwrap();
        Writer::<TestRelation<(), 0>, ()>::write_batch(&mut flag, &mut [Vec::new()]).unwrap();
        Writer::<TestRelation<(), 0>, (), true>::write_batch(&mut diff, &mut []).unwrap();
        assert!(
            Writer::<TestRelation<(i32,), 1>, ()>::finish(rows)
                .unwrap()
                .is_empty()
        );
        assert!(!Writer::<TestRelation<(), 0>, ()>::finish(flag).unwrap());
        assert_eq!(
            Writer::<TestRelation<(), 0>, (), true>::finish(diff).unwrap(),
            0
        );
    }

    #[test]
    fn nullary_batch_results_record_presence() {
        let mut writer = HostWriter::new(false);
        Writer::<TestRelation<(), 0>, ()>::write_row(&mut writer, ((), (), 1)).unwrap();
        Writer::<TestRelation<(), 0>, ()>::write_row(&mut writer, ((), (), 1)).unwrap();
        assert!(Writer::<TestRelation<(), 0>, ()>::finish(writer).unwrap());
    }

    #[test]
    fn nullary_deltas_sum_only_into_the_supplied_result() {
        let mut first = HostWriter::new(0i32);
        let mut batches = [vec![((), (), 3)], Vec::new(), vec![((), (), -1)]];
        Writer::<TestRelation<(), 0>, (), true>::write_batch(&mut first, &mut batches).unwrap();
        assert!(batches.iter().all(Vec::is_empty));
        let mut second = HostWriter::new(0i32);
        Writer::<TestRelation<(), 0>, (), true>::write_row(&mut second, ((), (), -3)).unwrap();
        assert_eq!(
            Writer::<TestRelation<(), 0>, (), true>::finish(first).unwrap(),
            2
        );
        assert_eq!(
            Writer::<TestRelation<(), 0>, (), true>::finish(second).unwrap(),
            -3
        );
    }
}
