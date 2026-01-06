use std::fmt;

/// Monotonically increasing term number.
///
/// Terms act as logical clocks in Raft and are used to detect stale information.
pub struct Term {
    value: u64,
}

impl fmt::Display for Term {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "T{}", self.value)
    }
}

impl From<u64> for Term {
    fn from(value: u64) -> Self {
        Term { value }
    }
}

/// 1-based log index.
///
/// LogIndex 0 represents "no entries" or "before the first entry".
/// Valid log entries start at index 1.
pub struct LogIndex {
    value: u64,
}

impl fmt::Display for LogIndex {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "I{}", self.value)
    }
}

impl From<u64> for LogIndex {
    fn from(value: u64) -> Self {
        LogIndex { value }
    }
}

/// Unique server identifier.
pub struct NodeId {
    value: u64,
}

impl fmt::Display for NodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "N{}", self.value)
    }
}

impl From<u64> for NodeId {
    fn from(value: u64) -> Self {
        NodeId { value }
    }
}
