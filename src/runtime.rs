use std::time::{Duration, Instant};

use rand::Rng;

use crate::command::Command;
use crate::node::{Node, Role};
use crate::types::{LogIndex, Message, NodeId};

/// Trait for state machines that can apply commands.
pub trait StateMachine<C> {
    type Output;
    fn apply(&mut self, command: C) -> Self::Output;
}

/// Events that drive the runtime.
pub enum Event<C> {
    ElectionTimeout,
    HeartbeatTimeout,
    Message { from: NodeId, message: Message<C> },
}

/// Timer configuration.
pub struct TimerConfig {
    pub election_timeout: Duration,
    pub heartbeat_interval: Duration,
}

impl Default for TimerConfig {
    fn default() -> Self {
        Self {
            election_timeout: Duration::from_millis(300),
            heartbeat_interval: Duration::from_millis(100),
        }
    }
}

/// Runtime that wraps a Raft node with timer management.
pub struct Runtime<C, S> {
    node: Node<C>,
    state_machine: S,
    config: TimerConfig,
    election_deadline: Instant,
    heartbeat_deadline: Instant,
}

impl<C: Clone, S: StateMachine<C>> Runtime<C, S> {
    pub fn new(node: Node<C>, state_machine: S, config: TimerConfig) -> Self {
        let now = Instant::now();
        Self {
            node,
            state_machine,
            election_deadline: now + config.election_timeout,
            heartbeat_deadline: now + config.heartbeat_interval,
            config,
        }
    }

    pub fn node(&self) -> &Node<C> {
        &self.node
    }

    pub fn state_machine(&self) -> &S {
        &self.state_machine
    }

    pub fn state_machine_mut(&mut self) -> &mut S {
        &mut self.state_machine
    }

    /// Process an event and return commands to execute.
    pub fn handle(&mut self, event: Event<C>) -> Vec<Command<C>> {
        let commands = match event {
            Event::ElectionTimeout => self.node.election_timeout(),
            Event::HeartbeatTimeout => self.node.heartbeat_timeout(),
            Event::Message { from, message } => self.handle_message(from, message),
        };

        self.process_commands(&commands);
        self.apply_committed();

        commands
    }

    /// §5.2: if election timeout elapses without receiving AppendEntries or granting a vote,
    /// the server starts an election. Leaders suppress elections by sending heartbeats
    /// within each interval. Timeouts should be randomized in [T, 2T] to avoid split votes.
    pub fn poll_timers(&self) -> Option<Event<C>> {
        let now = Instant::now();

        if now >= self.election_deadline {
            return Some(Event::ElectionTimeout);
        }

        if matches!(self.node.role, Role::Leader(_)) && now >= self.heartbeat_deadline {
            return Some(Event::HeartbeatTimeout);
        }

        None
    }

    /// Time until next timer fires.
    pub fn next_deadline(&self) -> Instant {
        if matches!(self.node.role, Role::Leader(_)) {
            self.election_deadline.min(self.heartbeat_deadline)
        } else {
            self.election_deadline
        }
    }

    /// Submit a client command. Returns log index if leader, None otherwise.
    pub fn submit(&mut self, command: C) -> Option<LogIndex> {
        self.node.submit_command(command)
    }

    fn handle_message(&mut self, from: NodeId, message: Message<C>) -> Vec<Command<C>> {
        match message {
            Message::RequestVote(req) => self.node.handle_request_vote(from, req),
            Message::RequestVoteResponse(resp) => self.node.handle_request_vote_response(from, resp),
            Message::AppendEntries(req) => self.node.handle_append_entries(from, req),
            Message::AppendEntriesResponse(resp) => {
                self.node.handle_append_entries_response(from, resp)
            }
        }
    }

    fn process_commands(&mut self, commands: &[Command<C>]) {
        for command in commands {
            match command {
                Command::ResetElectionTimer => {
                    // §5.2: randomize in [T, 2T] so nodes time out at different moments,
                    // preventing repeated split votes when multiple candidates start at once.
                    let base = self.config.election_timeout;
                    let jitter_ms = rand::rng().random_range(0..base.as_millis() as u64);
                    self.election_deadline = Instant::now() + base + Duration::from_millis(jitter_ms);
                }
                Command::ResetHeartbeatTimer => {
                    self.heartbeat_deadline = Instant::now() + self.config.heartbeat_interval;
                }
                Command::Send { .. } => {
                    // Sending is handled by caller.
                }
            }
        }
    }

    // Figure 2, Rules for Servers (All Servers): if commitIndex > lastApplied, apply the
    // next entry to the state machine. §5.3: state machines process entries in log order.
    fn apply_committed(&mut self) {
        while let Some(applied) = self.node.take_entry_to_apply() {
            self.state_machine.apply(applied.command.clone());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kv::{KvCommand, KvResult, KvStore};
    use crate::types::{AppendEntriesResponse, RequestVoteResponse, Term};

    fn runtime(id: u64, peers: &[u64]) -> Runtime<KvCommand, KvStore> {
        let node = Node::new(
            NodeId::from(id),
            peers.iter().map(|&p| NodeId::from(p)).collect(),
        );
        Runtime::new(node, KvStore::new(), TimerConfig::default())
    }

    #[test]
    fn election_timeout_starts_election() {
        let mut rt = runtime(1, &[2, 3]);

        let commands = rt.handle(Event::ElectionTimeout);

        assert!(matches!(rt.node().role, Role::Candidate(_)));
        assert!(!commands.is_empty());
    }

    #[test]
    fn leader_applies_committed_entries() {
        let mut rt = runtime(1, &[2, 3]);

        // Become leader.
        rt.handle(Event::ElectionTimeout);
        rt.handle(Event::Message {
            from: NodeId::from(2),
            message: Message::RequestVoteResponse(RequestVoteResponse {
                term: Term::from(1),
                vote_granted: true,
            }),
        });
        assert!(matches!(rt.node().role, Role::Leader(_)));

        // Submit command.
        let index = rt.submit(KvCommand::Set {
            key: "foo".to_string(),
            value: "bar".to_string(),
        });
        assert!(index.is_some());

        // Simulate replication success.
        rt.handle(Event::Message {
            from: NodeId::from(2),
            message: Message::AppendEntriesResponse(AppendEntriesResponse {
                term: Term::from(1),
                success: true,
                match_index: LogIndex::from(1),
            }),
        });

        // Verify command was applied to state machine.
        let result = rt.state_machine.apply(KvCommand::Get {
            key: "foo".to_string(),
        });
        assert_eq!(result, KvResult::Value(Some("bar".to_string())));
    }

    #[test]
    fn timer_reset_on_election_timeout() {
        let mut rt = runtime(1, &[2, 3]);
        let initial_deadline = rt.election_deadline;

        std::thread::sleep(Duration::from_millis(10));
        rt.handle(Event::ElectionTimeout);

        assert!(rt.election_deadline > initial_deadline);
    }
}
