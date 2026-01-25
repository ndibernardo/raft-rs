use super::primitives::Term;

/// A single entry in the replicated log.
#[derive(Clone)]
pub struct LogEntry<Cmd> {
    pub term: Term,
    pub command: Cmd,
}
