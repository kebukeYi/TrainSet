<div align="center">
<strong>
<samp>

[English](https://github.com/kebukeYi/TrainDis/blob/main/README.md) · [简体中文](https://github.com/kebukeYi/TrainDis/blob/main/README_zh.md)

</samp>
</strong>
</div>

# TrainDis

[![C++](https://img.shields.io/badge/C++17-00ADD8?style=flat&logo=go)](https://go.dev/)
[![License](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

TrainDis is a high-performance, Redis-compatible in-memory database implemented in C++. It provides key-value storage with support for multiple data types, persistence mechanisms (AOF and RDB), and master-slave replication.

## Features

- **Multiple Data Types**: Supports strings, hashes, and sorted sets (zsets)
- **Redis Protocol**: Compatible with RESP (Redis Serialization Protocol)
- **Persistence Options**:
  - AOF (Append-Only File) with three modes: Always, EverySec, and None
  - RDB (Redis Database) snapshotting
- **Master-Slave Replication**: Support for data replication between nodes
- **Thread-Safe Operations**: Concurrency-safe data structures
- **Configurable Settings**: Load configuration from file or command line
- **Memory Efficient**: Optimized memory usage with expiration support

## Architecture

### Core Components

1. **Server**: Main server implementation using epoll for I/O multiplexing
2. **KV Storage**: In-memory key-value storage supporting different data types
3. **AOF Manager**: Handles Append-Only File persistence with various sync modes
4. **RDB Manager**: Manages database snapshots in RDB format
5. **RESP Parser**: Handles Redis Serialization Protocol for client communication
6. **Config Loader**: Parses configuration from files or command-line arguments

### Data Types Supported

- **Strings**: Simple key-value pairs with expiration support
- **Hashes**: Key-value maps stored under a single key
- **Sorted Sets (ZSets)**: Collections of unique members ordered by scores, implemented with skip lists

## Configuration

The server can be configured via command-line arguments or configuration files:

### Command Line Options

```bash
./train_set [--port <port>] [--bind <ip>] [--config <file>]
```

### Configuration File Options

- `port`: Server port (default: 6379)
- `bind_address`: IP address to bind to (default: 127.0.0.1)
- `aof.enabled`: Enable AOF persistence (true/false)
- `aof.mode`: AOF sync mode (always/everysec/none)
- `aof.dir`: Directory for AOF file
- `aof.filename`: AOF file name
- `rdb.enabled`: Enable RDB persistence (true/false)
- `rdb.dir`: Directory for RDB file
- `rdb.filename`: RDB file name
- `replicate.enabled`: Enable replication (true/false)
- `replicate.master_host`: Master host for replication
- `replicate.master_port`: Master port for replication

## Supported Commands

### String Operations
- `SET key value [EX seconds]` - Set a key-value pair
- `GET key` - Get value by key
- `DEL key1 [key2 ...]` - Delete one or more keys
- `EXISTS key` - Check if key exists
- `TTL key` - Get time to live for a key
- `EXPIRE key seconds` - Set expiration time

### Hash Operations
- `HSET key field value` - Set field in hash
- `HGET key field` - Get field value from hash
- `HDEL key field1 [field2 ...]` - Delete fields from hash
- `HEXISTS key field` - Check if field exists in hash
- `HLEN key` - Get number of fields in hash
- `HGETALL key` - Get all fields and values in hash

### Sorted Set Operations
- `ZADD key score member` - Add member to sorted set
- `ZREM key member [member ...]` - Remove members from sorted set
- `ZRANGE key start stop` - Get range of members by rank
- `ZSCORE key member` - Get score of member in sorted set

### Persistence Commands
- `SAVE` - Save data to RDB file
- `BGSAVE` - Save data to RDB file in background
- `BGREWRITEAOF` - Rewrite AOF file in background

### Server Commands
- `PING` - Ping the server
- `INFO` - Get server information
- `FLUSHALL` - Remove all keys
- `KEYS *` - List keys matching pattern

## Building and Running

### Prerequisites

- C++17 compatible compiler
- CMake 3.15 or higher
- POSIX-compliant system (Linux/macOS)

### Build Instructions

```bash
mkdir build
cd build
cmake ..
make
```

### Running with Configuration

```bash
# Using a config file
./train_set --config ../conf/always.conf

# Using command line arguments
./train_set --port 6380 --bind 127.0.0.1
```

### Example Configuration Files

The repository includes several example configuration files:
- `always.conf`: AOF enabled with "always" sync mode
- `everysec.conf`: AOF enabled with "every second" sync mode
- `none.conf`: AOF disabled
- `master.conf`: Master node configuration
- `replica.conf`: Replica node configuration

## Performance Features

- **Efficient AOF Implementation**: Multiple sync modes for balancing performance and durability
- **Batch Writes**: AOF supports configurable batch sizes to improve write performance
- **Skip Lists**: Sorted sets implemented with skip lists for efficient range operations
- **Memory Pre-allocation**: AOF files can be pre-allocated to reduce fragmentation
- **Background Operations**: AOF rewriting and RDB saving run in background threads

## Replication

TrainDis supports master-slave replication:
- Slaves can connect to masters to receive data updates
- Initial synchronization happens via RDB dump
- Incremental updates are sent as commands over the replication channel

## Testing

The project includes comprehensive unit tests for all major components:
- AOF functionality tests
- RDB persistence tests
- Key-value storage tests
- Configuration loading tests
- RESP protocol parsing tests