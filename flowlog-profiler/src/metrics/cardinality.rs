//! The flow measure: [`Cardinality`], tuple counts through one profiled
//! subject.

/// The tuple counts crossing one profiled subject (an operator, or a plan
/// node's operator group at its boundary): totals in and out. A direction
/// is `None` when unmeasured (no edge there, or an unobservable edge),
/// never zero.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Cardinality {
    pub tup_in: Option<i64>,
    pub tup_out: Option<i64>,
}

impl Cardinality {
    /// Sum per-worker cardinalities into a run total: workers sum, never
    /// average. A silent worker adds nothing; a direction no worker
    /// measured stays `None`.
    pub(crate) fn sum(counts: impl IntoIterator<Item = Cardinality>) -> Cardinality {
        let mut total = Cardinality::default();
        for count in counts {
            if let Some(v) = count.tup_in {
                *total.tup_in.get_or_insert(0) += v;
            }
            if let Some(v) = count.tup_out {
                *total.tup_out.get_or_insert(0) += v;
            }
        }
        total
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn card(tup_in: Option<i64>, tup_out: Option<i64>) -> Cardinality {
        Cardinality { tup_in, tup_out }
    }

    #[test]
    fn sum_totals_each_direction_independently() {
        let total = Cardinality::sum([card(Some(100), Some(400)), card(Some(300), Some(200))]);
        assert_eq!(total, card(Some(400), Some(600)));
    }

    /// A silent worker contributes nothing, but one measured worker is
    /// enough to keep the direction measured.
    #[test]
    fn sum_keeps_a_direction_one_worker_measured() {
        let total = Cardinality::sum([card(Some(100), None), card(None, None)]);
        assert_eq!(total, card(Some(100), None));
    }

    #[test]
    fn sum_of_all_unmeasured_stays_unmeasured() {
        let total = Cardinality::sum([card(None, None), card(None, None)]);
        assert_eq!(total, card(None, None));
    }

    #[test]
    fn sum_of_no_counts_is_unmeasured() {
        assert_eq!(Cardinality::sum([]), card(None, None));
    }
}
