use std::fmt;

/// Monotonically increasing term number.
///
/// Terms act as logical clocks in Raft and are used to detect stale information.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord)]
pub struct Term {
    value: u64,
}

impl Term {
    pub fn increment(self) -> Term {
        Term {
            value: self.value.saturating_add(1),
        }
    }
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
#[derive(Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord)]
pub struct LogIndex {
    value: u64,
}

impl LogIndex {
    /// Create from array length (0-based length becomes 1-based index).
    pub fn from_length(len: usize) -> LogIndex {
        LogIndex { value: len as u64 }
    }

    pub fn next(self) -> LogIndex {
        LogIndex {
            value: self.value.saturating_add(1),
        }
    }

    pub fn prev(self) -> Option<LogIndex> {
        if self.value == 0 {
            None
        } else {
            Some(LogIndex {
                value: self.value - 1,
            })
        }
    }

    /// Convert to array index (0-based). Returns None for index 0.
    pub fn to_array_index(self) -> Option<usize> {
        if self.value == 0 {
            None
        } else {
            Some((self.value - 1) as usize)
        }
    }
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
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
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
