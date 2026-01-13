# raft-rs

A Raft consensus implementation in Rust, designed as the foundation for a distributed key-value store.

## Overview

This library implements the Raft consensus algorithm as described in "In Search of an Understandable Consensus Algorithm" by Ongaro and Ousterhout. 
The Raft layer is generic over command type. The key-value store is implemented as a state machine that applies committed log entries.

## Features

- Leader election with randomized timeouts
- Log replication with consistency checks
- Safety: at most one leader per term

## References

- [Raft Paper](https://raft.github.io/raft.pdf)
- [Raft PhD Dissertation](https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)
- [TLA+ Specification](https://github.com/ongardie/raft.tla)
