use crate::types::{Message, NodeId};

/// Commands that the node issues to the runtime.
pub enum Command<C> {
    /// Send a message to a specific peer.
    Send { to: NodeId, message: Message<C> },
    /// Reset the election timer.
    ResetElectionTimer,
    /// Reset the heartbeat timer (leader only).
    ResetHeartbeatTimer,
}
