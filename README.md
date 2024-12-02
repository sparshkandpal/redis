YourRedisServer - A Custom Redis Server Implementation

This repository contains an implementation of a custom Redis-compatible server written in Ruby. It mimics key Redis functionalities, including command parsing, replication, persistence, and client handling. The server can act as a master or slave, support blocking commands, handle streams, and load an RDB file for initial state.

Table of Contents
Features
Setup and Installation
Usage
Commands Supported
Replication
Persistence
Development Notes
License
Features
Supports core Redis-like commands: GET, SET, INCR, PING, KEYS, ECHO, etc.
Implements Streams: commands like XADD, XREAD, XRANGE.
Replication:
Handles master-slave replication using REPLCONF, PSYNC, and FULLRESYNC.
Maintains offsets for replica synchronization.
Persistence:
Loads and parses a Redis Database (RDB) file on startup.
RDB parsing includes basic data types, expiries, and metadata.
Blocking Commands:
Supports blocking commands like XREAD BLOCK.
Handles multiple clients with non-blocking I/O.
Simulates acknowledgment (ACK) from replicas for write commands.
Setup and Installation
Prerequisites:

Ruby >= 2.5.
Bundler (optional, for managing dependencies).
Clone the repository:

Copy code
git clone https://github.com/yourusername/YourRedisServer.git
cd YourRedisServer
Install dependencies: This project doesn't rely on any external gems, so installation should be straightforward. Just ensure your Ruby version matches the prerequisites.

Run the server:

Copy code
ruby your_redis_server.rb --port 6379 --dir ./data --dbfilename dump.rdb
Usage
Running as Master
To run as a standalone master:

Copy code
ruby your_redis_server.rb --port 6379 --dir ./data --dbfilename dump.rdb
Running as Slave
To run as a replica of another Redis instance:

Copy code
ruby your_redis_server.rb --port 6380 --replicaof 127.0.0.1 6379 --dir ./data --dbfilename dump.rdb
Commands Supported
Basic Commands
Command	Description
PING	Responds with PONG.
ECHO <message>	Echoes back the <message>.
SET <key> <value> [PX <ms>]	Sets a key-value pair with optional expiration.
GET <key>	Gets the value of the specified key.
INCR <key>	Increments a numeric key by 1.
KEYS	Returns all keys in the database.
CONFIG	Returns configuration info for dir and dbfilename.
Replication Commands
Command	Description
PSYNC	Partial resynchronization command.
REPLCONF	Handles replica configuration and acknowledgments.
FULLRESYNC	Initiates a full database sync.
Stream Commands
Command	Description
XADD	Appends an entry to a stream.
XREAD	Reads data from a stream, supports blocking.
XRANGE	Queries a range of entries in a stream.
Advanced Commands
Command	Description
MULTI/EXEC	Transaction support.
WAIT	Waits for acknowledgments from replicas.
DISCARD	Discards a transaction.
Replication
Master-Slave Synchronization:
The server supports replication commands to synchronize state between master and replicas.
Replicas register with the master using REPLCONF.
Master sends FULLRESYNC or incremental updates (PSYNC).
Acknowledgments:
Simulates replica acknowledgments using REPLCONF ACK.
Master tracks replication offsets for all connected replicas.
Persistence
The server can load data from an RDB file at startup:

Specify the --dir and --dbfilename options to define the RDB location.
The RDB loader supports basic types and expiries.
Example:

Copy code
ruby your_redis_server.rb --port 6379 --dir ./data --dbfilename dump.rdb

Development Notes
The implementation is designed for educational purposes and may not handle all edge cases of a production-grade Redis server.
Additional features like Lua scripting and cluster support are not included.
Streams are implemented with basic support for commands like XADD, XREAD, and XRANGE.
License
This project is licensed under the MIT License. You are free to use, modify, and distribute it as per the license terms.

For any questions or contributions, feel free to create an issue or a pull request.
