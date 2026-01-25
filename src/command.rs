use crate::types::{Message, NodeId};

/// Commands that the node issues to the runtime.
pub enum Command<Cmd> {
    /// Send a message to a specific peer.
    Send { to: NodeId, message: Message<Cmd> },
    /// Reset the election timer.
    ResetElectionTimer,
    /// Reset the heartbeat timer (leader only).
    ResetHeartbeatTimer,
}
