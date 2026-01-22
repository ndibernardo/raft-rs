use crate::types::{LogEntry, LogIndex, NodeId, Term};

/// Storage abstraction for Raft persistent state. §5.1, Figure 2 (Persistent state on all
/// servers): currentTerm, votedFor, and log must be stored on stable storage and survive
/// crashes. Implementations must flush to durable media before returning from any mutating
/// method — responding to an RPC before persisting violates Raft's safety guarantees.
pub trait Storage<C> {
    type Error;

    /// Get the current term.
    fn current_term(&self) -> Result<Term, Self::Error>;

    /// Set the current term. Must be durable before returning.
    fn set_current_term(&mut self, term: Term) -> Result<(), Self::Error>;

    /// Get the node this server voted for in the current term.
    fn voted_for(&self) -> Result<Option<NodeId>, Self::Error>;

    /// Set the voted_for field. Must be durable before returning.
    fn set_voted_for(&mut self, candidate: Option<NodeId>) -> Result<(), Self::Error>;

    /// Get the last log index.
    fn last_log_index(&self) -> Result<LogIndex, Self::Error>;

    /// Get the term at a specific log index. Returns None if index is out of bounds.
    fn term_at(&self, index: LogIndex) -> Result<Option<Term>, Self::Error>;

    /// Get a log entry by index.
    fn entry(&self, index: LogIndex) -> Result<Option<LogEntry<C>>, Self::Error>;

    /// Get log entries starting from an index.
    fn entries_from(&self, start: LogIndex) -> Result<Vec<LogEntry<C>>, Self::Error>;

    /// Append an entry to the log. Returns the index of the new entry.
    fn append(&mut self, entry: LogEntry<C>) -> Result<LogIndex, Self::Error>;

    /// Truncate the log from the given index (inclusive).
    fn truncate_from(&mut self, index: LogIndex) -> Result<(), Self::Error>;

    /// Append multiple entries, handling conflicts per Raft rules.
    /// If an existing entry conflicts with a new one (same index, different term),
    /// delete the existing entry and all that follow it.
    fn append_entries(
        &mut self,
        prev_log_index: LogIndex,
        entries: Vec<LogEntry<C>>,
    ) -> Result<(), Self::Error>;
}

/// In-memory storage for testing.
pub struct MemoryStorage<C> {
    current_term: Term,
    voted_for: Option<NodeId>,
    log: Vec<LogEntry<C>>,
}

impl<C> MemoryStorage<C> {
    pub fn new() -> Self {
        Self {
            current_term: Term::default(),
            voted_for: None,
            log: Vec::new(),
        }
    }
}

impl<C> Default for MemoryStorage<C> {
    fn default() -> Self {
        Self::new()
    }
}

impl<C: Clone> Storage<C> for MemoryStorage<C> {
    type Error = std::convert::Infallible;

    fn current_term(&self) -> Result<Term, Self::Error> {
        Ok(self.current_term)
    }

    fn set_current_term(&mut self, term: Term) -> Result<(), Self::Error> {
        self.current_term = term;
        Ok(())
    }

    fn voted_for(&self) -> Result<Option<NodeId>, Self::Error> {
        Ok(self.voted_for)
    }

    fn set_voted_for(&mut self, candidate: Option<NodeId>) -> Result<(), Self::Error> {
        self.voted_for = candidate;
        Ok(())
    }

    fn last_log_index(&self) -> Result<LogIndex, Self::Error> {
        Ok(LogIndex::from_length(self.log.len()))
    }

    fn term_at(&self, index: LogIndex) -> Result<Option<Term>, Self::Error> {
        match index.to_array_index() {
            None => Ok(Some(Term::default())),
            Some(idx) => Ok(self.log.get(idx).map(|e| e.term)),
        }
    }

    fn entry(&self, index: LogIndex) -> Result<Option<LogEntry<C>>, Self::Error> {
        match index.to_array_index() {
            None => Ok(None),
            Some(idx) => Ok(self.log.get(idx).cloned()),
        }
    }

    fn entries_from(&self, start: LogIndex) -> Result<Vec<LogEntry<C>>, Self::Error> {
        match start.to_array_index() {
            None => Ok(self.log.clone()),
            Some(idx) => Ok(self.log.get(idx..).unwrap_or_default().to_vec()),
        }
    }

    fn append(&mut self, entry: LogEntry<C>) -> Result<LogIndex, Self::Error> {
        self.log.push(entry);
        Ok(LogIndex::from_length(self.log.len()))
    }

    fn truncate_from(&mut self, index: LogIndex) -> Result<(), Self::Error> {
        if let Some(idx) = index.to_array_index() {
            self.log.truncate(idx);
        }
        Ok(())
    }

    fn append_entries(
        &mut self,
        prev_log_index: LogIndex,
        entries: Vec<LogEntry<C>>,
    ) -> Result<(), Self::Error> {
        let mut insert_index = prev_log_index.next();

        for entry in entries {
            match insert_index.to_array_index() {
                Some(idx) if idx < self.log.len() => {
                    if self.log[idx].term != entry.term {
                        self.log.truncate(idx);
                        self.log.push(entry);
                    }
                }
                _ => {
                    self.log.push(entry);
                }
            }
            insert_index = insert_index.next();
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn memory_storage_term_and_vote() {
        let mut storage: MemoryStorage<String> = MemoryStorage::new();

        assert_eq!(storage.current_term().unwrap(), Term::default());
        assert_eq!(storage.voted_for().unwrap(), None);

        storage.set_current_term(Term::from(5)).unwrap();
        storage.set_voted_for(Some(NodeId::from(3))).unwrap();

        assert_eq!(storage.current_term().unwrap(), Term::from(5));
        assert_eq!(storage.voted_for().unwrap(), Some(NodeId::from(3)));
    }

    #[test]
    fn memory_storage_append_and_read() {
        let mut storage: MemoryStorage<String> = MemoryStorage::new();

        let idx = storage
            .append(LogEntry {
                term: Term::from(1),
                command: "a".to_string(),
            })
            .unwrap();
        assert_eq!(idx, LogIndex::from(1));

        let idx = storage
            .append(LogEntry {
                term: Term::from(1),
                command: "b".to_string(),
            })
            .unwrap();
        assert_eq!(idx, LogIndex::from(2));

        assert_eq!(storage.last_log_index().unwrap(), LogIndex::from(2));
        assert_eq!(storage.term_at(LogIndex::from(1)).unwrap(), Some(Term::from(1)));
        assert_eq!(
            storage.entry(LogIndex::from(1)).unwrap().map(|e| e.command),
            Some("a".to_string())
        );
    }

    #[test]
    fn memory_storage_truncate() {
        let mut storage: MemoryStorage<String> = MemoryStorage::new();

        storage
            .append(LogEntry {
                term: Term::from(1),
                command: "a".to_string(),
            })
            .unwrap();
        storage
            .append(LogEntry {
                term: Term::from(1),
                command: "b".to_string(),
            })
            .unwrap();
        storage
            .append(LogEntry {
                term: Term::from(1),
                command: "c".to_string(),
            })
            .unwrap();

        storage.truncate_from(LogIndex::from(2)).unwrap();

        assert_eq!(storage.last_log_index().unwrap(), LogIndex::from(1));
    }

    #[test]
    fn memory_storage_append_entries_with_conflict() {
        let mut storage: MemoryStorage<String> = MemoryStorage::new();

        storage
            .append(LogEntry {
                term: Term::from(1),
                command: "a".to_string(),
            })
            .unwrap();
        storage
            .append(LogEntry {
                term: Term::from(1),
                command: "old".to_string(),
            })
            .unwrap();

        storage
            .append_entries(
                LogIndex::from(1),
                vec![LogEntry {
                    term: Term::from(2),
                    command: "new".to_string(),
                }],
            )
            .unwrap();

        assert_eq!(storage.last_log_index().unwrap(), LogIndex::from(2));
        assert_eq!(
            storage.entry(LogIndex::from(2)).unwrap().map(|e| e.command),
            Some("new".to_string())
        );
    }
}
