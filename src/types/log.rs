use super::primitives::Term;

/// A single entry in the replicated log.
/// command is None for no-op entries appended on leader election (§8).
#[derive(Clone)]
pub struct LogEntry<Cmd> {
    pub term: Term,
    pub command: Option<Cmd>,
}
