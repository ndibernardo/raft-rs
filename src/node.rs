use crate::command::Command;
use crate::state::{Candidate, Follower, Leader};
use crate::types::{LogEntry, LogIndex, Message, NodeId, RequestVote, Term};

/// Persistent state on all servers.
pub struct PersistentState<C> {
    pub current_term: Term,
    pub voted_for: Option<NodeId>,
    pub log: Vec<LogEntry<C>>,
}

/// Volatile state on all servers.
pub struct VolatileState {
    pub commit_index: LogIndex,
    pub last_applied: LogIndex,
}

/// Server role with associated state.
pub enum Role {
    Follower(Follower),
    Candidate(Candidate),
    Leader(Leader),
}

/// A Raft node.
pub struct Node<C> {
    pub id: NodeId,
    pub peers: Vec<NodeId>,
    pub persistent: PersistentState<C>,
    pub volatile: VolatileState,
    pub role: Role,
}

impl<C> Node<C> {
    /// Create a new node. Starts as follower with no known leader.
    pub fn new(id: NodeId, peers: Vec<NodeId>) -> Self {
        Self {
            id,
            peers,
            persistent: PersistentState {
                current_term: Term::default(),
                voted_for: None,
                log: Vec::new(),
            },
            volatile: VolatileState {
                commit_index: LogIndex::default(),
                last_applied: LogIndex::default(),
            },
            role: Role::Follower(Follower { leader_id: None }),
        }
    }

    fn last_log_index(&self) -> LogIndex {
        LogIndex::from_length(self.persistent.log.len())
    }

    fn last_log_term(&self) -> Term {
        self.persistent
            .log
            .last()
            .map_or(Term::default(), |entry| entry.term)
    }

    /// Called when election timer fires. Follower/Candidate starts new election.
    pub fn election_timeout(&mut self) -> Vec<Command<C>> {
        match &self.role {
            Role::Leader(_) => Vec::new(),
            Role::Follower(_) | Role::Candidate(_) => self.start_election(),
        }
    }

    fn start_election(&mut self) -> Vec<Command<C>> {
        self.persistent.current_term = self.persistent.current_term.increment();
        self.persistent.voted_for = Some(self.id);
        self.role = Role::Candidate(Candidate {
            votes_received: vec![self.id],
        });

        let request = RequestVote {
            term: self.persistent.current_term,
            candidate_id: self.id,
            last_log_index: self.last_log_index(),
            last_log_term: self.last_log_term(),
        };

        let mut commands = Vec::new();
        for &peer in &self.peers {
            commands.push(Command::Send {
                to: peer,
                message: Message::RequestVote(request.clone()),
            });
        }
        commands.push(Command::ResetElectionTimer);
        commands
    }
}
