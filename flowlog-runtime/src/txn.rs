//! Transactions, and how a driver hands them to the workers.
//!
//! A transaction is a list of [`TxnOp`]s, each naming the [`Rows`] it
//! inserts or deletes. One thread, the driver, decides what the workers do
//! next and asks it of them as a [`Request`]: to commit a transaction, or
//! to quit. [`Broadcast`] hands each request to every worker, in lock step.

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Barrier;
use std::sync::RwLock;

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
// Broadcast
// =============================================================================

/// Hands every worker each [`Request`] a driver publishes, in lock step.
///
/// Every party holds a clone of one broadcast: each worker, and the driver
/// too when it runs on a thread of its own. Parties meet at barriers, each
/// call to [`Self::receive`] or [`Self::wait`] being one, and no party
/// passes a barrier before every party has reached it. The driver publishes
/// a request before the barrier at which the workers receive it, and the
/// next only past a later barrier, once every worker has received this one.
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
    /// Returns a broadcast whose barriers each wait for `parties` parties.
    #[must_use]
    pub fn new(parties: usize) -> Self {
        Self {
            shared: Arc::new(Shared {
                barrier: Barrier::new(parties),
                latest: RwLock::new(None),
            }),
            received: 0,
        }
    }

    /// Makes `request` the one the workers receive at the next barrier.
    pub fn publish(&self, request: Request) {
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
    pub fn receive(&mut self) -> Request {
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

    /// Blocks at the next barrier until every party has reached it.
    pub fn wait(&self) {
        self.shared.barrier.wait();
    }
}

#[derive(Debug)]
struct Shared {
    barrier: Barrier,
    /// The request published last, or `None` before the first.
    latest: RwLock<Option<Published>>,
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
    use std::thread;

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

    fn commit(tuple: &str) -> Request {
        Request::Commit(vec![TxnOp::Insert {
            rel: "Edge".to_owned(),
            rows: Rows::Tuple(tuple.to_owned()),
        }])
    }

    fn session() -> Vec<Request> {
        vec![commit("1,2"), Request::Commit(Vec::new()), Request::Quit]
    }

    /// A driver on a thread of its own publishes, then meets the workers at
    /// two barriers: one where they receive, one where they finish.
    #[test]
    fn every_worker_receives_each_request_in_order() {
        const WORKERS: usize = 3;
        let driver = Broadcast::new(WORKERS + 1);
        let workers: Vec<thread::JoinHandle<Vec<Request>>> = (0..WORKERS)
            .map(|_| {
                let mut broadcast = driver.clone();
                thread::spawn(move || {
                    let mut received = Vec::new();
                    loop {
                        let request = broadcast.receive();
                        broadcast.wait();
                        let quit = request == Request::Quit;
                        received.push(request);
                        if quit {
                            return received;
                        }
                    }
                })
            })
            .collect();
        for request in session() {
            driver.publish(request);
            driver.wait();
            driver.wait();
        }
        for worker in workers {
            assert_eq!(worker.join().expect("the worker finishes"), session());
        }
    }

    /// A worker may drive the others, and receives its own requests too.
    #[test]
    fn a_worker_can_drive_the_others() {
        const WORKERS: usize = 3;
        let broadcast = Broadcast::new(WORKERS);
        let workers: Vec<thread::JoinHandle<Vec<Request>>> = (0..WORKERS)
            .map(|index| {
                let mut broadcast = broadcast.clone();
                thread::spawn(move || {
                    let mut received = Vec::new();
                    for request in session() {
                        if index == 0 {
                            broadcast.publish(request);
                        }
                        received.push(broadcast.receive());
                        broadcast.wait();
                    }
                    received
                })
            })
            .collect();
        for worker in workers {
            assert_eq!(worker.join().expect("the worker finishes"), session());
        }
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
