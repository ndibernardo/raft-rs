use std::collections::VecDeque;

use crate::command::Command;
use crate::node::{Node, Role};
use crate::runtime::{Runtime, StateMachine, TimerConfig};
use crate::types::{Message, NodeId};

/// A message in flight between nodes.
struct InFlight<C> {
    from: NodeId,
    to: NodeId,
    message: Message<C>,
}

/// Simulated cluster for testing.
pub struct Cluster<C, S> {
    runtimes: Vec<Runtime<C, S>>,
    messages: VecDeque<InFlight<C>>,
}

impl<C: Clone, S: StateMachine<C> + Default> Cluster<C, S> {
    /// Create a cluster with the given number of nodes.
    pub fn new(size: usize) -> Self {
        let ids: Vec<NodeId> = (1..=size).map(|i| NodeId::from(i as u64)).collect();

        let runtimes = ids
            .iter()
            .map(|&id| {
                let peers: Vec<NodeId> = ids.iter().filter(|&&p| p != id).copied().collect();
                let node = Node::new(id, peers);
                Runtime::new(node, S::default(), TimerConfig::default())
            })
            .collect();

        Self {
            runtimes,
            messages: VecDeque::new(),
        }
    }

    /// Get a reference to a node's runtime by index (0-based).
    pub fn runtime(&self, index: usize) -> &Runtime<C, S> {
        &self.runtimes[index]
    }

    /// Get a mutable reference to a node's runtime by index (0-based).
    pub fn runtime_mut(&mut self, index: usize) -> &mut Runtime<C, S> {
        &mut self.runtimes[index]
    }

    /// Trigger election timeout on a specific node.
    pub fn election_timeout(&mut self, index: usize) {
        let commands = self.runtimes[index].handle(crate::runtime::Event::ElectionTimeout);
        self.queue_commands(index, commands);
    }

    /// Trigger heartbeat timeout on a specific node.
    pub fn heartbeat_timeout(&mut self, index: usize) {
        let commands = self.runtimes[index].handle(crate::runtime::Event::HeartbeatTimeout);
        self.queue_commands(index, commands);
    }

    /// Deliver all pending messages.
    pub fn deliver_all(&mut self) {
        while let Some(msg) = self.messages.pop_front() {
            self.deliver(msg);
        }
    }

    /// Deliver a single message and queue any responses.
    fn deliver(&mut self, inflight: InFlight<C>) {
        let to_index = self.node_index(inflight.to);
        if let Some(index) = to_index {
            let commands = self.runtimes[index].handle(crate::runtime::Event::Message {
                from: inflight.from,
                message: inflight.message,
            });
            self.queue_commands(index, commands);
        }
    }

    /// Queue outgoing commands from a node.
    fn queue_commands(&mut self, from_index: usize, commands: Vec<Command<C>>) {
        let from_id = self.runtimes[from_index].node().id;
        for command in commands {
            if let Command::Send { to, message } = command {
                self.messages.push_back(InFlight {
                    from: from_id,
                    to,
                    message,
                });
            }
        }
    }

    /// Find runtime index by node ID.
    fn node_index(&self, id: NodeId) -> Option<usize> {
        self.runtimes
            .iter()
            .position(|rt| rt.node().id == id)
    }

    /// Find the current leader, if any.
    pub fn leader(&self) -> Option<usize> {
        self.runtimes
            .iter()
            .position(|rt| matches!(rt.node().role, Role::Leader(_)))
    }

    /// Count nodes in each role.
    pub fn role_counts(&self) -> (usize, usize, usize) {
        let mut followers = 0;
        let mut candidates = 0;
        let mut leaders = 0;

        for rt in &self.runtimes {
            match rt.node().role {
                Role::Follower(_) => followers += 1,
                Role::Candidate(_) => candidates += 1,
                Role::Leader(_) => leaders += 1,
            }
        }

        (followers, candidates, leaders)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kv::{KvCommand, KvResult, KvStore};
    use crate::types::LogIndex;

    #[test]
    fn single_node_becomes_leader() {
        let mut cluster: Cluster<KvCommand, KvStore> = Cluster::new(1);

        cluster.election_timeout(0);

        assert!(cluster.leader().is_some());
    }

    #[test]
    fn three_node_leader_election() {
        let mut cluster: Cluster<KvCommand, KvStore> = Cluster::new(3);

        // Node 0 starts election.
        cluster.election_timeout(0);
        assert_eq!(cluster.role_counts(), (2, 1, 0));

        // Deliver vote requests and responses.
        cluster.deliver_all();

        // Node 0 should be leader.
        assert_eq!(cluster.leader(), Some(0));
        assert_eq!(cluster.role_counts(), (2, 0, 1));
    }

    #[test]
    fn leader_replicates_to_followers() {
        let mut cluster: Cluster<KvCommand, KvStore> = Cluster::new(3);

        // Elect leader.
        cluster.election_timeout(0);
        cluster.deliver_all();
        assert_eq!(cluster.leader(), Some(0));

        // Submit command to leader.
        let index = cluster.runtime_mut(0).submit(KvCommand::Set {
            key: "x".to_string(),
            value: "1".to_string(),
        });
        assert_eq!(index, Some(LogIndex::from(1)));

        // Send heartbeats with new entry.
        cluster.heartbeat_timeout(0);
        cluster.deliver_all();

        // Verify all nodes have the entry.
        for i in 0..3 {
            assert_eq!(cluster.runtime(i).node().persistent.log.len(), 1);
        }

        // Verify leader committed and applied.
        assert_eq!(
            cluster.runtime(0).node().volatile.commit_index,
            LogIndex::from(1)
        );

        // Verify state machine applied on leader.
        let result = cluster.runtime_mut(0).state_machine_mut().apply(KvCommand::Get {
            key: "x".to_string(),
        });
        assert_eq!(result, KvResult::Value(Some("1".to_string())));
    }

    #[test]
    fn followers_commit_on_leader_heartbeat() {
        let mut cluster: Cluster<KvCommand, KvStore> = Cluster::new(3);

        // Elect leader and submit command.
        cluster.election_timeout(0);
        cluster.deliver_all();

        cluster.runtime_mut(0).submit(KvCommand::Set {
            key: "y".to_string(),
            value: "2".to_string(),
        });

        // First heartbeat: replicate entry.
        cluster.heartbeat_timeout(0);
        cluster.deliver_all();

        // Second heartbeat: propagate commit index.
        cluster.heartbeat_timeout(0);
        cluster.deliver_all();

        // Verify followers committed.
        for i in 1..3 {
            assert_eq!(
                cluster.runtime(i).node().volatile.commit_index,
                LogIndex::from(1)
            );
        }
    }
}
