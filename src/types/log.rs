use super::primitives::Term;

/// A single entry in the replicated log.
pub struct LogEntry<C> {
    pub term: Term,
    pub command: C,
}
