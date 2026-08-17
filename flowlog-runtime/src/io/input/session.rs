//! Owning a differential input session that can be closed through `&mut`.

use differential_dataflow::Data;
use differential_dataflow::difference::Semigroup;
use differential_dataflow::input::InputSession;
use timely::progress::Timestamp;

/// One relation's handle on the dataflow.
///
/// Exists because differential's `InputSession::close` consumes the
/// session, while a relation handler lives in a struct and is only ever
/// reached through `&mut`: the `Option` swap that resolves those two is
/// written once here rather than emitted per relation.
///
/// The handle is dropped by [`close`](Self::close); calling anything else
/// afterwards panics.
pub struct Session<T: Timestamp + Clone, D: Data, R: Semigroup + 'static> {
    h: Option<InputSession<T, D, R>>,
}

impl<T: Timestamp + Clone, D: Data, R: Semigroup + 'static> Session<T, D, R> {
    pub fn new(h: InputSession<T, D, R>) -> Self {
        Self { h: Some(h) }
    }

    /// # Panics
    ///
    /// If the session is already closed.
    #[inline]
    fn open(&mut self) -> &mut InputSession<T, D, R> {
        self.h.as_mut().expect("input session is closed")
    }

    /// Apply `tuple` with weight `diff`.
    ///
    /// # Panics
    ///
    /// If the session is already closed.
    #[inline]
    pub fn update(&mut self, tuple: D, diff: R) {
        self.open().update(tuple, diff);
    }

    /// # Panics
    ///
    /// If the session is already closed.
    pub fn advance_to(&mut self, t: T) {
        self.open().advance_to(t);
    }

    /// # Panics
    ///
    /// If the session is already closed.
    pub fn flush(&mut self) {
        self.open().flush();
    }

    /// Close the session, releasing the dataflow's input handle. Idempotent.
    pub fn close(&mut self) {
        if let Some(h) = self.h.take() {
            h.close();
        }
    }
}
