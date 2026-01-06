//! Core type definitions for Raft.

use std::num::NonZeroU64;

/// Unique identifier for a node in the cluster.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct NodeId(NonZeroU64);

impl NodeId {
    /// Creates a new node identifier.
    ///
    /// # Returns
    /// `None` if the value is zero.
    pub const fn new(value: u64) -> Option<Self> {
        match NonZeroU64::new(value) {
            Some(inner) => Some(Self(inner)),
            None => None,
        }
    }

    pub const fn get(self) -> u64 {
        self.0.get()
    }
}

/// A term number in Raft.
///
/// Terms are logical clocks that increase monotonically.
/// Term 0 represents the initial state before any election.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Default)]
pub struct Term(u64);

impl Term {
    pub const ZERO: Self = Self(0);

    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    pub const fn get(self) -> u64 {
        self.0
    }

    pub const fn increment(self) -> Self {
        Self(self.0 + 1)
    }
}

/// Index into the replicated log.
///
/// Log indices are 1-based as per the Raft specification.
/// Index 0 represents "no previous entry".
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Default)]
pub struct LogIndex(u64);

impl LogIndex {
    pub const ZERO: Self = Self(0);
    pub const FIRST: Self = Self(1);

    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    pub const fn get(self) -> u64 {
        self.0
    }

    pub const fn increment(self) -> Self {
        Self(self.0 + 1)
    }

    pub const fn decrement_saturating(self) -> Self {
        Self(self.0.saturating_sub(1))
    }

    pub const fn is_valid_entry(self) -> bool {
        self.0 > 0
    }

    /// Converts to a zero-based array index.
    pub const fn to_array_index(self) -> Option<usize> {
        if self.0 == 0 {
            None
        } else {
            Some((self.0 - 1) as usize)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn node_id_rejects_zero() {
        assert!(NodeId::new(0).is_none());
        assert!(NodeId::new(1).is_some());
    }

    #[test]
    fn term_increment() {
        let term = Term::ZERO;
        assert_eq!(term.increment().get(), 1);
    }

    #[test]
    fn log_index_array_conversion() {
        assert!(LogIndex::ZERO.to_array_index().is_none());
        assert_eq!(LogIndex::FIRST.to_array_index(), Some(0));
        assert_eq!(LogIndex::new(5).to_array_index(), Some(4));
    }
}
