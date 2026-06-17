# Gossip Protocol Simulation

This project implements a peer-to-peer Gossip protocol for information
dissemination in distributed networks. Each node communicates over UDP, discovers
other peers through a bootstrap process, forwards messages to a random subset of
neighbors, limits propagation with TTL, and uses a push-pull recovery mechanism
to repair missed deliveries.

The implementation is intentionally compact and educational. It demonstrates the
core behavior of a decentralized gossip network: a node joins through a seed
peer, learns about other peers, sends and receives gossip messages, detects
duplicates, checks neighbor liveness, and recovers missing messages without a
central coordinator.

## Features

- UDP-based peer-to-peer communication
- JSON message format for all protocol messages
- Bootstrap-based network joining with `HELLO`
- Peer discovery using `PEERS_GET` and `PEERS_LIST`
- TTL-limited `GOSSIP` message propagation
- Random fanout-based forwarding to reduce flooding
- Duplicate detection with a seen-message set
- Temporary message store for recovery requests
- Peer liveness management with `PING` and `PONG`
- Background removal of inactive peers
- Hybrid push-pull recovery with `IHAVE` and `IWANT`
- Proof-of-Work on `HELLO` messages for basic Sybil resistance
- Threaded node execution with protected shared state
- Simulation framework for 10, 20, and 50 node networks
- Measurement of convergence time and message overhead
- Plot generation for convergence and overhead analysis

## Architecture Overview

```text
Interactive user command
        |
        | gossip <message>
        v
Local node state and message store
        |
        | GOSSIP / IHAVE / IWANT / PING / PONG
        v
UDP socket communication
        |
        v
Random subset of known peers
        |
        v
Distributed gossip propagation
```

Each node keeps its own local view of the network. Since the system is fully
decentralized, different nodes may temporarily know different peer sets or
message sets. The protocol is designed so that the network eventually converges
through gossip forwarding and periodic recovery.

## Node State

Each running node maintains the following state:

| State | Purpose |
| --- | --- |
| `node_id` | Unique identifier for the node |
| `self_addr` | Node IP address and UDP port |
| `peers` | Known neighboring peers and their last activity time |
| `seen` | Message IDs that were already processed |
| `message_store` | Temporarily stored messages for later recovery |
| `config` | Runtime parameters such as fanout, TTL, and timeouts |
| `stats` | Counters for sent and received messages |

The peer list is bounded to prevent unlimited growth. Old or inactive peers are
removed when they stop responding to liveness checks.

## Protocol Messages

All messages are encoded as JSON and sent over UDP.

| Message | Purpose |
| --- | --- |
| `HELLO` | Introduces a new node to the network |
| `PEERS_GET` | Requests a random list of known peers |
| `PEERS_LIST` | Sends known peers to another node |
| `GOSSIP` | Carries an application message through the network |
| `PING` | Checks whether a peer is alive |
| `PONG` | Confirms that a peer is alive |
| `IHAVE` | Announces message IDs stored by a node |
| `IWANT` | Requests a missing message from another node |

## Join Path

1. A new node starts with its own UDP port and unique node ID.
2. If a bootstrap address is provided, the node sends a `HELLO` message to the
   seed node.
3. When Proof-of-Work is enabled, the `HELLO` message includes a valid nonce.
4. The seed node validates the join message and adds the new node as a peer.
5. The new node sends `PEERS_GET` to request more peers.
6. The seed node replies with `PEERS_LIST`.
7. The joining node contacts the returned peers and gradually becomes connected
   to the network.

This process avoids a central registry. Only one known seed address is needed to
join the existing peer-to-peer network.

## Gossip Propagation Path

1. A user enters `gossip <message>` in a running node.
2. The node creates a unique message ID and stores the full message locally.
3. The message is sent as a `GOSSIP` packet to a random subset of known peers.
4. Each receiving peer checks whether the message ID already exists in `seen`.
5. If the message is new, the peer stores it and decreases the TTL.
6. If the TTL is still positive, the peer forwards the message to another random
   subset of peers.
7. Duplicate messages are ignored to reduce unnecessary traffic.

This random fanout behavior is the main gossip mechanism. It allows information
to spread quickly without requiring every node to send every message to every
other node.

## Duplicate Control

Gossip protocols naturally create duplicate transmissions because the same
message can arrive through different paths. To control this, each node stores a
set of seen message IDs.

If a received message ID is already in the set, the node does not process or
forward it again. This prevents infinite re-forwarding and keeps the protocol
practical even when the network has cycles.

## Liveness Management

Each node periodically checks whether its known peers are still active.

1. The node sends `PING` messages to peers.
2. Active peers reply with `PONG`.
3. The sender updates the peer's last activity time.
4. A background task removes peers that have not responded for longer than the
   configured timeout.

This allows the topology to adapt when peers terminate or become unreachable.

## Push-Pull Recovery

UDP does not guarantee delivery, so the project includes a recovery layer.

1. Nodes periodically announce stored message IDs using `IHAVE`.
2. A receiving peer compares the announced IDs with its own `seen` set.
3. If the peer is missing a message, it sends an `IWANT` request.
4. The node that has the message retransmits the full message.

The initial push-based gossip spreads messages quickly. The later pull-based
recovery improves reliability and helps nodes catch up if they missed the first
propagation wave.

## Proof-of-Work Defense

The project includes a lightweight Proof-of-Work mechanism on `HELLO` messages.

A joining node must find a nonce such that the hash of the join data satisfies a
difficulty rule, such as a required number of leading zeros. This makes it cheap
for existing nodes to verify a join request, but more expensive for an attacker
to create many fake identities.

This is a basic Sybil-resistance mechanism. It is not a complete production
security model, but it demonstrates an important idea used in decentralized
systems.

## Concurrency Design

Each node runs multiple tasks concurrently:

- A receiver loop for incoming UDP packets
- A message handler for parsing and processing messages
- A liveness manager for `PING` and `PONG`
- A recovery worker for `IHAVE` and `IWANT`
- A discovery worker for periodic peer exchange
- A main interactive loop for user commands

Shared structures such as the peer list, seen-message set, message store, and
statistics are protected with locks to avoid inconsistent updates between
threads.

## Requirements

- Python 3.7 or newer
- Standard Python library for the core implementation
- Optional plotting packages from `requirements.txt`

Install optional plotting dependencies with:

```sh
pip install -r requirements.txt
```

## Running Nodes

Start the first node as the seed node:

```sh
python3 node.py --port 8080
```

Start additional nodes with the seed node as bootstrap:

```sh
python3 node.py --port 8081 --bootstrap 127.0.0.1:8080
python3 node.py --port 8082 --bootstrap 127.0.0.1:8080
python3 node.py --port 8083 --bootstrap 127.0.0.1:8080
```

Send a message from any running node:

```text
gossip Hello from the gossip network
```

## Interactive Commands

| Command | Description |
| --- | --- |
| `gossip <message>` | Sends a new message to the network |
| `peers` | Shows the current known peer list |
| `stats` | Shows local message statistics |
| `quit` | Stops the node |

## Configuration

The node supports runtime configuration through command-line arguments.

| Option | Description |
| --- | --- |
| `--port` | UDP port used by the node |
| `--bootstrap` | Bootstrap peer address in `ip:port` format |
| `--fanout` | Number of peers selected for each gossip forward |
| `--peer_limit` | Maximum number of peers stored by the node |
| `--ping_interval` | Interval between liveness checks |
| `--peer_timeout` | Time before an inactive peer is removed |
| `--ttl` | Initial TTL for gossip messages |
| `--pow_k` | Proof-of-Work difficulty |
| `--push_pull_interval` | Interval for recovery announcements |

Example:

```sh
python3 node.py \
  --port 8081 \
  --bootstrap 127.0.0.1:8080 \
  --fanout 4 \
  --ttl 10 \
  --peer_limit 15
```

## Simulation Framework

The repository includes a simulation script for evaluating the protocol under
controlled local conditions.

The simulator:

1. Starts a selected number of node processes.
2. Chooses one node as the seed node.
3. Gives the network time to form peer connections.
4. Injects a test gossip message.
5. Monitors node output.
6. Measures convergence once 95% of nodes receive the message.
7. Counts total message overhead.
8. Repeats experiments to account for randomness.

Run the simulation with:

```sh
python3 simulate.py
```

The simulation results are saved in:

```text
simulation_results.json
```

## Evaluation Metrics

| Metric | Meaning |
| --- | --- |
| Convergence time | Time until 95% of nodes receive the message |
| Message overhead | Total number of protocol messages exchanged |
| Nodes reached | Number of nodes that received the test message |
| Mean and standard deviation | Stability of results across repeated runs |

The experiments evaluate networks with 10, 20, and 50 nodes. Additional runs
vary fanout and TTL to study how protocol parameters affect speed and overhead.

## Plot Generation

Generate plots from the simulation results with:

```sh
python3 plot_simulation_results.py
```

The project includes plots such as:

| File | Purpose |
| --- | --- |
| `convergence.png` | Convergence time by network size |
| `overhead.png` | Message overhead by network size |
| `convergence_20_by_fanout.png` | Effect of fanout on convergence for 20 nodes |
| `convergence_20_by_ttl.png` | Effect of TTL on convergence for 20 nodes |
| `overhead_20_by_fanout.png` | Effect of fanout on overhead for 20 nodes |
| `overhead_20_by_ttl.png` | Effect of TTL on overhead for 20 nodes |

## Results Summary

The experiments show that the protocol forms networks successfully and spreads
messages without failed runs in the tested local environment.

The main observations are:

- Larger networks require more time to converge.
- Message overhead increases as the number of nodes grows.
- Very low fanout can slow down propagation.
- Increasing fanout can improve speed, but also increases traffic.
- Increasing TTL is useful only up to a practical threshold.
- After a certain point, larger TTL values add little benefit.
- Push-pull recovery improves eventual delivery when the initial propagation
  does not reach every node.

The results highlight the main gossip trade-off: faster dissemination usually
requires more redundant communication.

## Testing

Run the test script with:

```sh
python3 test.py
```

The tests validate core behavior such as:

- Node startup
- Bootstrap joining
- Peer discovery
- Gossip propagation
- Duplicate message handling
- PING/PONG liveness checking
- Configurable parameters
- Multi-node propagation

## Repository Structure

| Path | Purpose |
| --- | --- |
| `node.py` | Main peer implementation, UDP socket handling, protocol logic, and CLI |
| `simulate.py` | Starts local node processes and measures convergence and overhead |
| `plot_simulation_results.py` | Generates plots from saved simulation results |
| `test.py` | Test script for validating protocol behavior |
| `requirements.txt` | Optional dependencies for plotting and analysis |
| `simulation_results.json` | Stored simulation output in JSON format |
| `simulation.txt` | Text output from simulation runs |
| `convergence.png` | Network-size convergence plot |
| `overhead.png` | Network-size overhead plot |
| `convergence_20_by_fanout.png` | Convergence plot for different fanout values |
| `convergence_20_by_ttl.png` | Convergence plot for different TTL values |
| `overhead_20_by_fanout.png` | Overhead plot for different fanout values |
| `overhead_20_by_ttl.png` | Overhead plot for different TTL values |
| `CN_Project_402105665_402170913_Final.pdf` | Full project report |

## Known Limitations

- The simulation runs locally and does not fully model real Internet latency.
- Packet loss is not deeply simulated even though the protocol uses UDP.
- The current implementation is intended for experimentation, not production
  deployment.
- The seen-message set and message store need cleanup policies for long-running
  networks.
- Proof-of-Work is basic and does not replace authentication.
- Messages are not encrypted or digitally signed.
- Peer trust and reputation are not implemented.
- The protocol does not yet adapt fanout or TTL automatically based on network
  conditions.

## Possible Improvements

- Add digital signatures for message authenticity.
- Add encryption for private communication.
- Add configurable packet loss, delay, and churn to the simulator.
- Implement LRU cleanup for the seen-message set.
- Add expiration for stored messages.
- Add adaptive fanout and TTL selection.
- Add peer reputation or scoring.
- Support multi-machine deployment instead of localhost-only experiments.
- Add structured logging and better experiment reports.
- Add a live visualization dashboard for network topology and propagation.

## Contributors

Student project team:

- Parmis Hemasian
- Sobhan Aghasi Zadeh

## Course Context

This project was implemented for a Computer Networks course at Sharif University
of Technology. The goal was to design, implement, and evaluate a gossip-based
protocol for decentralized message dissemination in a peer-to-peer network.

## License

This project is provided for educational purposes.
