use crate::command::Command;
use crate::state::{Candidate, Follower, Leader};
use crate::types::{LogEntry, LogIndex, Message, NodeId, RequestVote, RequestVoteResponse, Term};

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

    /// Handle incoming RequestVote RPC.
    pub fn handle_request_vote(&mut self, from: NodeId, req: RequestVote) -> Vec<Command<C>> {
        // If RPC request contains term > currentTerm, update currentTerm and convert to follower.
        if req.term > self.persistent.current_term {
            self.persistent.current_term = req.term;
            self.persistent.voted_for = None;
            self.role = Role::Follower(Follower { leader_id: None });
        }

        let vote_granted = self.should_grant_vote(&req);

        if vote_granted {
            self.persistent.voted_for = Some(req.candidate_id);
        }

        let response = RequestVoteResponse {
            term: self.persistent.current_term,
            vote_granted,
        };

        vec![Command::Send {
            to: from,
            message: Message::RequestVoteResponse(response),
        }]
    }

    fn should_grant_vote(&self, req: &RequestVote) -> bool {
        // Reject if candidate's term < currentTerm.
        if req.term < self.persistent.current_term {
            return false;
        }

        // Check if we already voted for someone else this term.
        match self.persistent.voted_for {
            Some(voted_for) if voted_for != req.candidate_id => return false,
            _ => {}
        }

        // Check if candidate's log is at least as up-to-date as ours.
        self.is_log_up_to_date(req.last_log_term, req.last_log_index)
    }

    fn is_log_up_to_date(&self, candidate_term: Term, candidate_index: LogIndex) -> bool {
        let my_term = self.last_log_term();
        let my_index = self.last_log_index();

        // Raft determines which log is more up-to-date by comparing
        // the index and term of the last entries.
        // If the logs have last entries with different terms, the log
        // with the later term is more up-to-date.
        // If the logs end with the same term, the longer log is more up-to-date.
        match candidate_term.cmp(&my_term) {
            std::cmp::Ordering::Greater => true,
            std::cmp::Ordering::Less => false,
            std::cmp::Ordering::Equal => candidate_index >= my_index,
        }
    }
}
