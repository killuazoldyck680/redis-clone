<div align="center">

# 🦀 Redis Clone in Rust

**A Redis-compatible in-memory data store built from scratch in Rust, with persistence, replication, streams, transactions, pub/sub, and more.**

[![Rust](https://img.shields.io/badge/Rust-2021-orange?logo=rust)](https://www.rust-lang.org/)
[![CodeCrafters](https://img.shields.io/badge/CodeCrafters-Build%20your%20own%20Redis-blue)](https://codecrafters.io/challenges/redis)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

</div>

---

## Overview

This project is a from-scratch implementation of a Redis server written in Rust, built for the
[CodeCrafters "Build Your Own Redis" challenge](https://codecrafters.io/challenges/redis). It speaks the
**RESP (Redis Serialization Protocol)**, so it works with the official `redis-cli` and standard Redis client libraries.

It implements every stage of the challenge, from a basic TCP server up to replication, append-only-file persistence,
geospatial indexes, and ACL-based authentication.

## Features

- ✅ **TCP server** with concurrent client handling
- ✅ **RESP protocol** parser and serializer
- ✅ **Strings:** `SET` / `GET` with expiry (`EX`, `PX`), `INCR`
- ✅ **Lists:** push, pop, range, length, and blocking pops
- ✅ **Streams:** `XADD`, `XRANGE`, `XREAD` (including blocking reads)
- ✅ **Sorted sets:** scoring, ranking, and range queries
- ✅ **Geospatial:** `GEOADD`, `GEOPOS`, `GEODIST`, `GEOSEARCH`
- ✅ **Transactions:** `MULTI`, `EXEC`, `DISCARD`
- ✅ **Pub/Sub:** channel subscriptions and message publishing
- ✅ **RDB persistence:** loading and parsing RDB files on startup
- ✅ **AOF persistence:** append-only file logging and replay
- ✅ **Replication:** master/replica handshake (`PSYNC`, `REPLCONF`), command propagation, and `WAIT`
- ✅ **Authentication:** ACL users and `AUTH`

## Supported Commands

| Category | Commands |
|----------|----------|
| Connection | `PING`, `ECHO`, `AUTH` |
| Strings | `SET`, `GET`, `INCR`, `TYPE` |
| Keys & Config | `KEYS`, `CONFIG GET` |
| Lists | `RPUSH`, `LPUSH`, `LRANGE`, `LLEN`, `LPOP`, `BLPOP` |
| Streams | `XADD`, `XRANGE`, `XREAD` |
| Sorted Sets | `ZADD`, `ZRANK`, `ZRANGE`, `ZCARD`, `ZSCORE`, `ZREM` |
| Geospatial | `GEOADD`, `GEOPOS`, `GEODIST`, `GEOSEARCH` |
| Transactions | `MULTI`, `EXEC`, `DISCARD` |
| Pub/Sub | `SUBSCRIBE`, `UNSUBSCRIBE`, `PUBLISH` |
| Replication | `INFO`, `REPLCONF`, `PSYNC`, `WAIT` |
| ACL | `ACL WHOAMI`, `ACL GETUSER`, `ACL SETUSER` |

## Getting Started

### Prerequisites

- [Rust](https://www.rust-lang.org/tools/install) and Cargo
- `redis-cli` (optional, for manual testing)

### Build and Run

```bash
# Clone the repository
git clone https://github.com/<your-username>/<your-repo>.git
cd <your-repo>

# Build in release mode
cargo build --release

# Start the server (default port 6379)
cargo run --release
```

### Command-Line Options

| Flag | Description | Default |
|------|-------------|---------|
| `--port <port>` | Port to listen on | `6379` |
| `--dir <path>` | Directory for persistence files | `.` |
| `--dbfilename <name>` | RDB file name | `dump.rdb` |
| `--appendonly <yes\|no>` | Enable append-only file persistence | `no` |
| `--appenddirname <name>` | AOF directory name | `appendonlydir` |
| `--appendfilename <name>` | AOF file name | `appendonly.aof` |
| `--replicaof "<host> <port>"` | Start as a replica of another server | none |

## Usage

Start the server, then connect with `redis-cli`:

```bash
$ redis-cli -p 6379
127.0.0.1:6379> PING
PONG
127.0.0.1:6379> SET session:42 "active" PX 5000
OK
127.0.0.1:6379> GET session:42
"active"
127.0.0.1:6379> RPUSH tasks "build" "test" "ship"
(integer) 3
127.0.0.1:6379> XADD events * type login user alice
"1718000000000-0"
127.0.0.1:6379> GEOADD places 77.2090 28.6139 "Delhi"
(integer) 1
```

### Running a Replica

```bash
# Terminal 1: master
cargo run --release -- --port 6379

# Terminal 2: replica
cargo run --release -- --port 6380 --replicaof "127.0.0.1 6379"
```

## Project Structure

```
redis-clone/
├── src/
│   ├── main.rs          # Entry point, TCP listener, command handling, persistence, replication
│   └── resp.rs          # RESP protocol parser and encoder
├── appendonlydir/       # AOF directory generated at runtime
├── test_dir/            # Sample data directory used during testing
├── test_dir_multi/      # Sample multi-file AOF directory used during testing
├── Cargo.toml
├── Cargo.lock
└── README.md
```

**Design highlights**

- **Protocol layer:** RESP parsing and encoding live in their own module (`resp.rs`), separate from command logic.
- **Concurrency:** Each client connection is handled concurrently, with shared state safely guarded across tasks.
- **Expiry:** Keys carry an optional expiration timestamp and are evicted on access.
- **Durability:** State can be restored from either an RDB snapshot or by replaying the append-only file.
- **Replication:** Replicas perform a full handshake with the master, then receive propagated write commands and acknowledge offsets.

## Testing

The implementation passes the full CodeCrafters test suite. For manual checks:

```bash
redis-cli -p 6379 PING
```

## What I Learned

- Implementing a wire protocol (RESP) from its specification
- Concurrent network programming and shared state in Rust
- How Redis handles persistence (RDB and AOF), replication, and expiry internally
- Designing blocking operations (`BLPOP`, `XREAD BLOCK`) without stalling other clients

## Roadmap

- [ ] Split `main.rs` into focused modules (commands, store, persistence, replication)
- [ ] Active key expiration with a background sweeper
- [ ] Additional data types (hashes, sets)
- [ ] Benchmarks against real Redis using `redis-benchmark`

## Acknowledgements

- [CodeCrafters](https://codecrafters.io) for the challenge
- [Redis documentation](https://redis.io/docs/) and the [RESP specification](https://redis.io/docs/reference/protocol-spec/)

## License

Distributed under the MIT License. See [`LICENSE`](LICENSE) for details.

## Author

**Chirag Mishra**
[GitHub](https://github.com/killuazoldyck680) 