//! Transactions, and the sessions that run them on every worker.
//!
//! A transaction is a list of [`TxnOp`]s, each naming the [`Rows`] it
//! inserts or deletes. One party, the driver, decides what the workers do
//! next and asks it of them as a [`Request`]: to commit a transaction, or
//! to quit. A [`Broadcast`] runs the session: it hands each request to
//! every worker, in lock step, and has each worker run its part of it, one
//! [`Phase`] at a time.

use std::fmt::Debug;
use std::ops::ControlFlow;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Barrier;
use std::sync::RwLock;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::time::Instant;

use crate::time::Epoch;

// =============================================================================
// Transactions
// =============================================================================

/// A single update queued inside a transaction: the rows it names are
/// inserted into `rel` or deleted from it. A relation is a set, so a
/// command never counts.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TxnOp {
    Insert { rel: String, rows: Rows },
    Delete { rel: String, rows: Rows },
}

impl TxnOp {
    /// The relation the update names, as the command spelled it.
    #[must_use]
    pub fn rel(&self) -> &str {
        match self {
            Self::Insert { rel, .. } | Self::Delete { rel, .. } => rel,
        }
    }
}

/// The rows a command names.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Rows {
    /// One tuple in serialized form; empty for a nullary relation's fact.
    Tuple(String),
    /// Every row of a file.
    File(PathBuf),
}

// =============================================================================
// Request
// =============================================================================

/// What a driver asks every worker to do next.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Request {
    /// Applies the updates, in order, as one transaction, then advances
    /// time. The list is empty for a transaction committed without updates,
    /// and for one whose rows reach the workers another way.
    Commit(Vec<TxnOp>),
    /// Ends the session, discarding any open transaction.
    Quit,
}

// =============================================================================
// Phase
// =============================================================================

/// A part of a session that a worker runs. Each commit is a [`Self::Load`]
/// then a [`Self::Run`]; once the driver quits, a [`Self::Close`] ends the
/// session.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Phase<'a> {
    /// Loads a transaction into the inputs at `time`, the epoch it fills:
    /// its `updates`, in order, and any rows that reach the worker another
    /// way.
    Load { time: Epoch, updates: &'a [TxnOp] },
    /// Advances the inputs to `time`, which ends the epoch before it, and
    /// runs the dataflow until that epoch is done.
    Run(Epoch),
    /// Closes the inputs at `time`, and runs the dataflow until every epoch
    /// before it is done.
    Close(Epoch),
}

// =============================================================================
// Broadcast
// =============================================================================

/// Runs a session: hands every worker each [`Request`] the driver makes, in
/// lock step, and has each worker run its part of it.
///
/// Every party holds a clone of one broadcast: each worker, and the driver
/// too when it runs on a thread of its own, as a host. Either one worker
/// drives, with [`Self::drive`], or a host does, with [`Self::commit`] and
/// [`Self::quit`]; every other worker runs [`Self::serve`].
///
/// Parties meet at barriers, and no party passes one before every party
/// has reached it. Each request is received at one. A commit that names
/// updates meets again once every worker has loaded them, to learn whether
/// they loaded on every worker. Each commit ends at another, once every
/// worker has run its epoch, so the driver reads the results only then.
///
/// Parties share it through memory, so it reaches one process's workers.
/// Should a run span timely processes, requests would have to travel
/// through a dataflow instead; the interface here would stay, with the
/// transport behind it replaced.
#[derive(Clone, Debug)]
pub struct Broadcast {
    // Each party owns its handle on a thread of its own, so the barrier and
    // the latest request live behind an `Arc`.
    shared: Arc<Shared>,
    /// The number of the request this handle received last; 0 before its
    /// first.
    received: u64,
}

impl Broadcast {
    /// Returns a broadcast for `parties` parties: every worker, and the
    /// driver when it runs on a thread of its own.
    #[must_use]
    pub fn new(parties: usize) -> Self {
        Self {
            shared: Arc::new(Shared {
                barrier: Barrier::new(parties),
                latest: RwLock::new(None),
                failed: AtomicBool::new(false),
            }),
            received: 0,
        }
    }

    /// Runs this worker's part of a session another party drives, from
    /// `time`, the epoch the inputs are at, until the driver quits.
    ///
    /// `run_phase` may fail only to load a commit's updates. The session
    /// then ends before any worker runs the epoch, each worker returning its
    /// own result: `Ok` where the updates loaded.
    ///
    /// # Panics
    ///
    /// Panics if `run_phase` fails in any other phase.
    pub fn serve<E: Debug>(
        &mut self,
        mut time: Epoch,
        mut run_phase: impl FnMut(Phase<'_>) -> Result<(), E>,
    ) -> Result<(), E> {
        loop {
            match self.run_request(time, &mut run_phase) {
                ControlFlow::Continue(next) => time = next,
                ControlFlow::Break(ended) => return ended,
            }
        }
    }

    /// Drives a session from this worker, and runs this worker's part of it
    /// as [`Self::serve`] does.
    ///
    /// `next_request` makes each request, given the epoch the inputs are
    /// at. Once every worker has run a commit, and before the next request
    /// is made, `committed` gets the epoch the commit advanced to and the
    /// instant its request was made.
    ///
    /// # Panics
    ///
    /// Panics if `run_phase` fails in any phase but loading a commit's
    /// updates, as [`Self::serve`] does.
    pub fn drive<E: Debug>(
        &mut self,
        mut time: Epoch,
        mut next_request: impl FnMut(Epoch) -> Request,
        mut committed: impl FnMut(Epoch, Instant),
        mut run_phase: impl FnMut(Phase<'_>) -> Result<(), E>,
    ) -> Result<(), E> {
        loop {
            let request = next_request(time);
            let made = Instant::now();
            self.publish(request);
            match self.run_request(time, &mut run_phase) {
                ControlFlow::Continue(next) => time = next,
                ControlFlow::Break(ended) => return ended,
            }
            // Every worker has received the commit's updates, so the shared
            // copy need not hold their rows and paths while the next request
            // is made.
            self.clear_updates();
            committed(time, made);
        }
    }

    /// From a host, asks every worker to commit a transaction whose rows
    /// reach them another way, and returns once every worker has run it.
    pub fn commit(&self) {
        self.publish(Request::Commit(Vec::new()));
        // A commit without updates has no failure to agree on, so the
        // barrier after the one where the workers receive it is where its
        // epoch has run.
        self.wait();
        self.wait();
    }

    /// From a host, asks every worker to end the session, and returns once
    /// every worker has received the request. Each worker then closes its
    /// inputs without waiting for the others; joining them waits for that.
    pub fn quit(&self) {
        self.publish(Request::Quit);
        self.wait();
    }

    /// Blocks at the next barrier until every party has reached it: for the
    /// parties to meet outside a session, such as before it starts.
    pub fn wait(&self) {
        self.shared.barrier.wait();
    }

    /// Receives the next request and runs this worker's part of it.
    /// Continues with the epoch a commit advanced to, or breaks with this
    /// worker's result once the session ends.
    fn run_request<E: Debug>(
        &mut self,
        time: Epoch,
        run_phase: &mut impl FnMut(Phase<'_>) -> Result<(), E>,
    ) -> ControlFlow<Result<(), E>, Epoch> {
        let updates = match self.receive() {
            Request::Commit(updates) => updates,
            Request::Quit => {
                run_phase(Phase::Close(time)).expect(ONLY_UPDATES_FAIL);
                return ControlFlow::Break(Ok(()));
            }
        };
        let loaded = run_phase(Phase::Load {
            time,
            updates: &updates,
        });
        // Only a commit's updates can fail to load: they are read from text
        // or files, where rows that reach the workers another way arrive
        // typed. The workers agree on a failure before any runs the epoch,
        // and every party knows from the request whether to meet for it.
        if updates.is_empty() {
            loaded.expect(ONLY_UPDATES_FAIL);
        } else if self.failed_anywhere(loaded.is_err()) {
            return ControlFlow::Break(loaded);
        }
        let time = time + 1;
        run_phase(Phase::Run(time)).expect(ONLY_UPDATES_FAIL);
        self.wait();
        ControlFlow::Continue(time)
    }

    /// Makes `request` the one the workers receive at the next barrier.
    fn publish(&self, request: Request) {
        let mut latest = self.shared.latest.write().expect("broadcast poisoned");
        let number = latest.as_ref().map_or(1, |published| published.number + 1);
        *latest = Some(Published { number, request });
    }

    /// Waits at the next barrier, then returns the request the driver
    /// published before it.
    ///
    /// # Panics
    ///
    /// Panics unless the driver published exactly one request since this
    /// handle last received one, or before its first receive: the worker
    /// would otherwise act on a request twice, or skip one.
    #[must_use]
    fn receive(&mut self) -> Request {
        self.wait();
        let latest = self.shared.latest.read().expect("broadcast poisoned");
        let published = latest
            .as_ref()
            .expect("a request is published before the first receive");
        assert_eq!(
            published.number,
            self.received + 1,
            "exactly one request is published between two receives"
        );
        self.received = published.number;
        published.request.clone()
    }

    /// Releases the latest commit's shared updates without advancing its
    /// request number. Workers keep their received copies.
    ///
    /// The driver must call this after a barrier following every worker's
    /// receive, and before publishing the next request.
    fn clear_updates(&self) {
        let mut latest = self.shared.latest.write().expect("broadcast poisoned");
        if let Some(published) = latest.as_mut() {
            match &mut published.request {
                Request::Commit(updates) => updates.clear(),
                Request::Quit => {}
            }
        }
    }

    /// Meets the other workers once each has loaded a commit's updates,
    /// given whether they `failed` here; returns whether they failed on any
    /// worker.
    fn failed_anywhere(&self, failed: bool) -> bool {
        if failed {
            self.shared.failed.store(true, Ordering::Relaxed);
        }
        // The barrier orders every worker's store before any worker's load.
        self.wait();
        self.shared.failed.load(Ordering::Relaxed)
    }
}

/// Why a phase other than loading a commit's updates must not fail: the
/// workers agree on a failure only there, so a worker that stopped anywhere
/// else would leave the others waiting for it.
const ONLY_UPDATES_FAIL: &str = "only loading a commit's updates may fail";

#[derive(Debug)]
struct Shared {
    barrier: Barrier,
    /// The request published last, or `None` before the first.
    latest: RwLock<Option<Published>>,
    /// Set once a commit's updates fail to load on any worker, which ends
    /// the session.
    failed: AtomicBool,
}

/// A published request, numbered from 1 in publishing order.
#[derive(Debug)]
struct Published {
    number: u64,
    request: Request,
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use std::convert::Infallible;
    use std::sync::atomic::AtomicUsize;
    use std::thread;
    use std::time::Duration;

    use rstest::rstest;

    use super::*;

    /// Both verbs name their relation as the command spelled it.
    #[test]
    fn an_update_names_its_relation_under_either_verb() {
        let rows = Rows::Tuple("1,2".to_string());
        let insert = TxnOp::Insert {
            rel: "Edge".to_string(),
            rows: rows.clone(),
        };
        let delete = TxnOp::Delete {
            rel: "Edge".to_string(),
            rows,
        };
        assert_eq!(insert.rel(), "Edge");
        assert_eq!(delete.rel(), "Edge");
    }

    fn insert(tuple: &str) -> TxnOp {
        TxnOp::Insert {
            rel: "Edge".to_owned(),
            rows: Rows::Tuple(tuple.to_owned()),
        }
    }

    fn commit(tuple: &str) -> Request {
        Request::Commit(vec![insert(tuple)])
    }

    const WORKERS: usize = 3;

    /// A phase a worker ran, holding the updates it loaded.
    #[derive(Debug, PartialEq, Eq)]
    enum Ran {
        Load(Epoch, Vec<TxnOp>),
        Run(Epoch),
        Close(Epoch),
    }

    impl From<Phase<'_>> for Ran {
        fn from(phase: Phase<'_>) -> Self {
            match phase {
                Phase::Load { time, updates } => Self::Load(time, updates.to_vec()),
                Phase::Run(time) => Self::Run(time),
                Phase::Close(time) => Self::Close(time),
            }
        }
    }

    /// Runs a phase as a worker would: records it, counts it in `runs` if
    /// it is a run, which takes a while, and fails to load updates if this
    /// is the `failing` worker.
    fn run_phase(
        index: usize,
        failing: Option<usize>,
        runs: &AtomicUsize,
        ran: &mut Vec<Ran>,
        phase: Phase<'_>,
    ) -> Result<(), &'static str> {
        ran.push(Ran::from(phase));
        match phase {
            Phase::Load { updates, .. } if !updates.is_empty() && failing == Some(index) => {
                Err("bad tuple")
            }
            Phase::Run(_) => {
                // A party that read the count before every worker had run
                // would see it short.
                thread::sleep(Duration::from_millis(5));
                runs.fetch_add(1, Ordering::Relaxed);
                Ok(())
            }
            Phase::Load { .. } | Phase::Close(_) => Ok(()),
        }
    }

    /// What a session a host drove came to.
    struct Hosted {
        /// The phases each worker ran.
        ran: Vec<Vec<Ran>>,
        /// How many runs the workers had finished as each commit returned.
        runs_by_commit: Vec<usize>,
    }

    /// Has a host commit `commits` times, then quit.
    fn host(commits: usize) -> Hosted {
        let host = Broadcast::new(WORKERS + 1);
        let runs = AtomicUsize::new(0);
        thread::scope(|scope| {
            let workers: Vec<_> = (0..WORKERS)
                .map(|index| {
                    let mut broadcast = host.clone();
                    let runs = &runs;
                    scope.spawn(move || {
                        let mut ran = Vec::new();
                        let served = broadcast
                            .serve(0, |phase| run_phase(index, None, runs, &mut ran, phase));
                        assert_eq!(served, Ok(()));
                        ran
                    })
                })
                .collect();
            let runs_by_commit = (0..commits)
                .map(|_| {
                    host.commit();
                    runs.load(Ordering::Relaxed)
                })
                .collect();
            host.quit();
            Hosted {
                ran: workers
                    .into_iter()
                    .map(|worker| worker.join().expect("the worker finishes"))
                    .collect(),
                runs_by_commit,
            }
        })
    }

    /// What a session worker 0 drove came to.
    struct Driven {
        /// Each worker's result, and the phases it ran, by index.
        workers: Vec<(Result<(), &'static str>, Vec<Ran>)>,
        /// The epoch the driver made each request at.
        made_at: Vec<Epoch>,
        /// Each commit the driver heard of: the epoch it advanced to, and
        /// how many runs the workers had finished by then.
        committed: Vec<(Epoch, usize)>,
    }

    /// Has worker 0 drive `requests` from epoch 1, as after a preload;
    /// loading updates fails on the `failing` worker.
    fn drive(requests: &[Request], failing: Option<usize>) -> Driven {
        let mut broadcast = Broadcast::new(WORKERS);
        let runs = AtomicUsize::new(0);
        thread::scope(|scope| {
            let others: Vec<_> = (1..WORKERS)
                .map(|index| {
                    let mut broadcast = broadcast.clone();
                    let runs = &runs;
                    scope.spawn(move || {
                        let mut ran = Vec::new();
                        let served = broadcast
                            .serve(1, |phase| run_phase(index, failing, runs, &mut ran, phase));
                        (served, ran)
                    })
                })
                .collect();
            let mut requests = requests.iter().cloned();
            let mut made_at = Vec::new();
            let mut committed = Vec::new();
            let mut ran = Vec::new();
            let driven = broadcast.drive(
                1,
                |time| {
                    made_at.push(time);
                    requests.next().expect("the session quits")
                },
                |time, _| committed.push((time, runs.load(Ordering::Relaxed))),
                |phase| run_phase(0, failing, &runs, &mut ran, phase),
            );
            let mut workers = vec![(driven, ran)];
            workers.extend(
                others
                    .into_iter()
                    .map(|worker| worker.join().expect("the worker finishes")),
            );
            Driven {
                workers,
                made_at,
                committed,
            }
        })
    }

    /// A host's commits reach every worker as a load then a run, each
    /// advancing time by one, and its quit as a close.
    #[rstest]
    #[case::quitting_at_once(0, vec![Ran::Close(0)])]
    #[case::after_two_commits(2, vec![
        Ran::Load(0, Vec::new()),
        Ran::Run(1),
        Ran::Load(1, Vec::new()),
        Ran::Run(2),
        Ran::Close(2),
    ])]
    fn a_host_runs_each_commit_on_every_worker(#[case] commits: usize, #[case] expected: Vec<Ran>) {
        for ran in host(commits).ran {
            assert_eq!(ran, expected);
        }
    }

    /// A host reads a commit's results once the call returns, so every
    /// worker has run its epoch by then.
    #[test]
    fn a_host_commit_returns_once_every_worker_has_run_it() {
        assert_eq!(host(2).runs_by_commit, [WORKERS, 2 * WORKERS]);
    }

    /// The driving worker runs its own requests too, and time starts where
    /// the caller left it.
    #[test]
    fn a_driving_worker_runs_each_request_on_every_worker() {
        let driven = drive(
            &[commit("1,2"), Request::Commit(Vec::new()), Request::Quit],
            None,
        );
        let expected = [
            Ran::Load(1, vec![insert("1,2")]),
            Ran::Run(2),
            Ran::Load(2, Vec::new()),
            Ran::Run(3),
            Ran::Close(3),
        ];
        for (served, ran) in driven.workers {
            assert_eq!(served, Ok(()));
            assert_eq!(ran, expected);
        }
    }

    /// The shell shows the epoch the inputs are at in its prompt, so the
    /// driver makes each request knowing it.
    #[test]
    fn the_driver_makes_each_request_at_the_epoch_the_inputs_are_at() {
        let driven = drive(
            &[commit("1,2"), Request::Commit(Vec::new()), Request::Quit],
            None,
        );
        assert_eq!(driven.made_at, [1, 2, 3]);
    }

    /// The driver emits a commit's results when it hears of it, so every
    /// worker has run its epoch by then.
    #[test]
    fn the_driver_hears_of_each_commit_once_every_worker_has_run_it() {
        let driven = drive(
            &[commit("1,2"), Request::Commit(Vec::new()), Request::Quit],
            None,
        );
        assert_eq!(driven.committed, [(2, WORKERS), (3, 2 * WORKERS)]);
    }

    /// Wherever the updates fail to load, every worker stops before any
    /// runs the epoch, and the driver makes no further request.
    #[rstest]
    #[case::on_the_driver(0)]
    #[case::on_another_worker(2)]
    fn a_failed_load_stops_every_worker_before_the_epoch_runs(#[case] failing: usize) {
        let driven = drive(
            &[commit("1,2"), commit("3,4"), Request::Quit],
            Some(failing),
        );
        for (index, (served, ran)) in driven.workers.into_iter().enumerate() {
            let expected = if index == failing {
                Err("bad tuple")
            } else {
                Ok(())
            };
            assert_eq!(served, expected);
            assert_eq!(ran, [Ran::Load(1, vec![insert("1,2")])]);
        }
        assert_eq!(driven.made_at, [1]);
        assert!(driven.committed.is_empty());
    }

    /// Every worker holds its own copy of a commit's updates once it has
    /// received them, so the driver releases the shared one.
    #[test]
    fn a_driver_releases_the_shared_updates_of_each_commit() {
        let mut broadcast = Broadcast::new(1);
        let observer = broadcast.clone();
        let mut requests = [commit("1,2"), Request::Quit].into_iter();
        let mut shared = Vec::new();
        let Ok(()) = broadcast.drive(
            0,
            |_| requests.next().expect("the session quits"),
            |_, _| {
                let latest = observer.shared.latest.read().expect("broadcast poisoned");
                shared.push(latest.as_ref().expect("a request").request.clone());
            },
            |_| Ok::<(), Infallible>(()),
        );
        assert_eq!(shared, [Request::Commit(Vec::new())]);
    }

    /// The workers agree on a failure only after loading updates, so one
    /// anywhere else would leave the others waiting.
    #[rstest]
    #[case::loading_a_commit_without_updates("load")]
    #[case::running("run")]
    #[case::closing("close")]
    #[should_panic(expected = "only loading a commit's updates may fail")]
    fn a_phase_other_than_loading_updates_must_not_fail(#[case] failing: &str) {
        let mut broadcast = Broadcast::new(1);
        let mut requests = [Request::Commit(Vec::new()), Request::Quit].into_iter();
        let _ = broadcast.drive(
            0,
            |_| requests.next().expect("the session quits"),
            |_, _| {},
            |phase| {
                let name = match phase {
                    Phase::Load { .. } => "load",
                    Phase::Run(_) => "run",
                    Phase::Close(_) => "close",
                };
                if name == failing {
                    Err("broken")
                } else {
                    Ok(())
                }
            },
        );
    }

    /// Shared retention is not visible through the receiving API, so inspect
    /// the shared request after releasing its updates.
    #[test]
    fn clearing_updates_releases_only_the_shared_copy() {
        let mut broadcast = Broadcast::new(1);
        broadcast.publish(commit("1,2"));
        let received = broadcast.receive();
        broadcast.wait();
        broadcast.clear_updates();

        let latest = broadcast.shared.latest.read().unwrap();
        assert_eq!(
            latest.as_ref().unwrap().request,
            Request::Commit(Vec::new())
        );
        assert_eq!(received, commit("1,2"));
    }

    #[test]
    fn clearing_updates_preserves_request_order() {
        let mut broadcast = Broadcast::new(1);
        broadcast.publish(commit("1,2"));
        let _ = broadcast.receive();
        broadcast.wait();
        broadcast.clear_updates();
        broadcast.publish(commit("3,4"));
        assert_eq!(broadcast.receive(), commit("3,4"));
    }

    #[test]
    #[should_panic(expected = "exactly one request is published between two receives")]
    fn clearing_updates_does_not_allow_receiving_twice() {
        let mut broadcast = Broadcast::new(1);
        broadcast.publish(commit("1,2"));
        let _ = broadcast.receive();
        broadcast.wait();
        broadcast.clear_updates();
        let _ = broadcast.receive();
    }

    #[test]
    #[should_panic(expected = "a request is published before the first receive")]
    fn receiving_before_any_publish_panics() {
        let _ = Broadcast::new(1).receive();
    }

    /// A worker that received twice from one publish would apply the same
    /// transaction twice.
    #[test]
    #[should_panic(expected = "exactly one request is published between two receives")]
    fn receiving_twice_from_one_publish_panics() {
        let mut broadcast = Broadcast::new(1);
        broadcast.publish(commit("1,2"));
        let _ = broadcast.receive();
        let _ = broadcast.receive();
    }

    /// A worker that missed a request would skip a transaction.
    #[test]
    #[should_panic(expected = "exactly one request is published between two receives")]
    fn publishing_twice_before_a_receive_panics() {
        let mut broadcast = Broadcast::new(1);
        broadcast.publish(commit("1,2"));
        broadcast.publish(Request::Quit);
        let _ = broadcast.receive();
    }
}
