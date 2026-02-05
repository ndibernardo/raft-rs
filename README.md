# raft-rs

A Raft consensus implementation in Rust, designed as the foundation for a distributed key-value store.

## Overview

This library implements the Raft consensus algorithm as described in "In Search of an Understandable Consensus Algorithm" by Ongaro and Ousterhout.
The Raft layer is generic over command type. The key-value store is implemented as a state machine that applies committed log entries.

## Running a cluster

Build the release binary:

```bash
cargo build --release
```

Start three nodes in separate terminals. Each node needs a unique `--id`, its own `--addr`, and one `--peer` flag per other cluster member.

**Node 1**
```bash
./target/release/raft \
  --id 1 \
  --addr 127.0.0.1:9001 \
  --peer 2=127.0.0.1:9002 \
  --peer 3=127.0.0.1:9003 \
  --data-dir ./data/node1
```

**Node 2**
```bash
./target/release/raft \
  --id 2 \
  --addr 127.0.0.1:9002 \
  --peer 1=127.0.0.1:9001 \
  --peer 3=127.0.0.1:9003 \
  --data-dir ./data/node2
```

**Node 3**
```bash
./target/release/raft \
  --id 3 \
  --addr 127.0.0.1:9003 \
  --peer 1=127.0.0.1:9001 \
  --peer 2=127.0.0.1:9002 \
  --data-dir ./data/node3
```

Each node prints its listen address to stderr on startup. The cluster elects a leader automatically once a majority of nodes are reachable. Persistent state (term, vote, log) is stored in the `--data-dir` directory and survives restarts.

## Testing

```bash
cargo test
```

## References

- [Raft Paper](https://raft.github.io/raft.pdf)
- [Raft PhD Dissertation](https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)
- [TLA+ Specification](https://github.com/ongardie/raft.tla)
