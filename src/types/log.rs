use super::primitives::Term;

/// A single entry in the replicated log.
#[derive(Clone)]
pub struct LogEntry<C> {
    pub term: Term,
    pub command: C,
}
