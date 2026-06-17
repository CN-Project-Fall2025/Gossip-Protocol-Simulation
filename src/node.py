#!/usr/bin/env python3
"""
Gossip Protocol Node Implementation
Implements a P2P gossip protocol for information dissemination using UDP.
"""

import socket
import json
import uuid
import time
import random
import threading
import hashlib
import argparse
import sys
from typing import Dict, List, Set, Optional, Tuple
from dataclasses import dataclass, asdict
from collections import defaultdict


@dataclass
class Config:
    """Node configuration parameters"""
    fanout: int = 3
    peer_limit: int = 10
    ping_interval: float = 5.0
    peer_timeout: float = 15.0
    ttl: int = 10
    pow_k: int = 4
    push_pull_interval: float = 10.0


@dataclass
class Peer:
    """Represents a peer node"""
    node_id: str
    addr: str  # ip:port
    last_seen: float
    last_ping: float = 0.0


class MessageType:
    """Message type constants"""
    HELLO = "HELLO"
    GET_PEERS = "GET_PEERS"
    PEERS_LIST = "PEERS_LIST"
    GOSSIP = "GOSSIP"
    PING = "PING"
    PONG = "PONG"
    IHAVE = "IHAVE"
    IWANT = "IWANT"


class Node:
    """Gossip Protocol Node"""
    
    def __init__(self, port: int, bootstrap: Optional[str] = None, config: Optional[Config] = None):
        self.node_id = str(uuid.uuid4())
        self.port = port
        self.self_addr = f"127.0.0.1:{port}"
        self.config = config or Config()
        
        # Node state
        self.peers: Dict[str, Peer] = {}  # key: addr
        self.seen_set: Set[str] = set()  # message IDs
        self.message_store: Dict[str, dict] = {}  # msg_id -> message data
        
        # Network socket
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind(('127.0.0.1', port))
        self.sock.settimeout(1.0)  # For periodic checks
        
        # Threading
        self.running = True
        self.lock = threading.Lock()
        
        # Statistics
        self.stats = {
            'messages_sent': 0,
            'messages_received': 0,
            'gossip_sent': 0,
            'gossip_received': 0,
        }
        
        # Bootstrap
        if bootstrap:
            self.bootstrap_node = bootstrap
        else:
            self.bootstrap_node = None
    
    def start(self):
        """Start the node"""
        print(f"[Node {self.node_id[:8]}] Starting on {self.self_addr}")
        
        # Bootstrap if needed
        if self.bootstrap_node:
            threading.Thread(target=self._bootstrap, daemon=True).start()
            time.sleep(1)  # Wait for bootstrap
        
        # Start threads
        threading.Thread(target=self._receive_loop, daemon=True).start()
        threading.Thread(target=self._ping_loop, daemon=True).start()
        threading.Thread(target=self._push_pull_loop, daemon=True).start()
        threading.Thread(target=self._cleanup_loop, daemon=True).start()
        
        # Main input loop
        self._input_loop()
    
    def _bootstrap(self):
        """Bootstrap to seed node"""
        try:
            bootstrap_addr = self.bootstrap_node.split(':')
            bootstrap_ip = bootstrap_addr[0]
            bootstrap_port = int(bootstrap_addr[1])
            
            # Send HELLO with PoW
            nonce = self._solve_pow(self.node_id, self.config.pow_k)
            hello_msg = {
                'type': MessageType.HELLO,
                'node_id': self.node_id,
                'addr': self.self_addr,
                'nonce': nonce,
                'pow_k': self.config.pow_k
            }
            print(f"[Node {self.node_id[:8]}] Sending HELLO")
            self._send_message(hello_msg, (bootstrap_ip, bootstrap_port))
            
            # Request peers
            get_peers_msg = {
                'type': MessageType.GET_PEERS,
                'node_id': self.node_id,
                'addr': self.self_addr
            }
            print(f"[Node {self.node_id[:8]}] Sending GET_PEERS")
            self._send_message(get_peers_msg, (bootstrap_ip, bootstrap_port))
            
            print(f"[Node {self.node_id[:8]}] Bootstrapped to {self.bootstrap_node}")
        except Exception as e:
            print(f"[Node {self.node_id[:8]}] Bootstrap failed: {e}")
    
    def _receive_loop(self):
        """Main message receiving loop"""
        while self.running:
            try:
                data, addr = self.sock.recvfrom(4096)
                message = json.loads(data.decode())
                self._handle_message(message, addr)
            except socket.timeout:
                continue
            except Exception as e:
                if self.running:
                    print(f"[Node {self.node_id[:8]}] Receive error: {e}")
    
    def _handle_message(self, message: dict, addr: Tuple[str, int]):
        """Handle incoming message"""
        msg_type = message.get('type')
        sender_addr = f"{addr[0]}:{addr[1]}"
        
        with self.lock:
            self.stats['messages_received'] += 1
        
        if msg_type == MessageType.HELLO:
            self._handle_hello(message, addr)
        elif msg_type == MessageType.GET_PEERS:
            self._handle_get_peers(message, addr)
        elif msg_type == MessageType.PEERS_LIST:
            self._handle_peers_list(message, addr)
        elif msg_type == MessageType.GOSSIP:
            self._handle_gossip(message, addr)
        elif msg_type == MessageType.PING:
            self._handle_ping(message, addr)
        elif msg_type == MessageType.PONG:
            self._handle_pong(message, addr)
        elif msg_type == MessageType.IHAVE:
            self._handle_ihave(message, addr)
        elif msg_type == MessageType.IWANT:
            self._handle_iwant(message, addr)
        else:
            print(f"[Node {self.node_id[:8]}] Unknown message type: {msg_type}")
    
    def _handle_hello(self, message: dict, addr: Tuple[str, int]):
        """Handle HELLO message"""
        node_id = message.get('node_id')
        sender_addr = message.get('addr')
        nonce = message.get('nonce')
        pow_k = message.get('pow_k', self.config.pow_k)
        
        # Verify PoW
        if not self._verify_pow(node_id, nonce, pow_k):
            print(f"[Node {self.node_id[:8]}] Invalid PoW from {sender_addr}")
            return
        
        # Add peer
        with self.lock:
            if len(self.peers) < self.config.peer_limit:
                peer = Peer(
                    node_id=node_id,
                    addr=sender_addr,
                    last_seen=time.time()
                )
                self.peers[sender_addr] = peer
                print(f"[Node {self.node_id[:8]}] Added peer {node_id[:8]} at {sender_addr}")
            else:
                print(f"[Node {self.node_id[:8]}] Peer limit reached, ignoring {sender_addr}")
    
    def _handle_get_peers(self, message: dict, addr: Tuple[str, int]):
        """Handle GET_PEERS request"""
        with self.lock:
            peers_copy = list(self.peers.values())
        # shuffle to return a random subset
        random.shuffle(peers_copy)
        peer_list = []
        for peer in peers_copy[:self.config.peer_limit]:
            peer_list.append({
                'node_id': peer.node_id,
                'addr': peer.addr
            })
        
        response = {
            'type': MessageType.PEERS_LIST,
            'node_id': self.node_id,
            'peers': peer_list
        }
        print(f"[Node {self.node_id[:8]}] Sending PEERS_LIST ({len(peer_list)} entries)")
        self._send_message(response, addr)
    
    def _handle_peers_list(self, message: dict, addr: Tuple[str, int]):
        """Handle PEERS_LIST response"""
        peers_list = message.get('peers', [])
        
        with self.lock:
            for peer_info in peers_list:
                peer_addr = peer_info['addr']
                if peer_addr != self.self_addr and peer_addr not in self.peers:
                    if len(self.peers) < self.config.peer_limit:
                        peer = Peer(
                            node_id=peer_info['node_id'],
                            addr=peer_addr,
                            last_seen=time.time()
                        )
                        self.peers[peer_addr] = peer
                        print(f"[Node {self.node_id[:8]}] Added peer from list: {peer_addr}")
        # after learning new peers, send HELLO back to establish bidirectional edge
        for peer_info in peers_list:
            peer_addr = peer_info['addr']
            if peer_addr != self.self_addr:
                addr_parts = peer_addr.split(':')
                nonce = self._solve_pow(self.node_id, self.config.pow_k)
                hello_msg = {
                    'type': MessageType.HELLO,
                    'node_id': self.node_id,
                    'addr': self.self_addr,
                    'nonce': nonce,
                    'pow_k': self.config.pow_k
                }
                print(f"[Node {self.node_id[:8]}] Replying HELLO to {peer_addr}")
                try:
                    self._send_message(hello_msg, (addr_parts[0], int(addr_parts[1])))
                except Exception:
                    pass
        
        # Establish connections to discovered peers by sending HELLO
        for peer_info in peers_list:
            peer_addr = peer_info['addr']
            if peer_addr != self.self_addr:
                addr_parts = peer_addr.split(':')
                nonce = self._solve_pow(self.node_id, self.config.pow_k)
                hello_msg = {
                    'type': MessageType.HELLO,
                    'node_id': self.node_id,
                    'addr': self.self_addr,
                    'nonce': nonce,
                    'pow_k': self.config.pow_k
                }
                try:
                    self._send_message(hello_msg, (addr_parts[0], int(addr_parts[1])))
                except Exception as e:
                    pass
    
    def _handle_gossip(self, message: dict, addr: Tuple[str, int]):
        """Handle GOSSIP message"""
        msg_id = message.get('msg_id')
        ttl = message.get('ttl', 0)
        data = message.get('data', '')
        
        with self.lock:
            self.stats['gossip_received'] += 1
            
            # Check if already seen
            if msg_id in self.seen_set:
                return
            
            # Add to seen set and store
            self.seen_set.add(msg_id)
            self.message_store[msg_id] = message
        
        print(f"[Node {self.node_id[:8]}] Received GOSSIP: {msg_id[:8]} (TTL={ttl}) at {time.time()}")
        
        # Forward if TTL > 0
        if ttl > 0:
            new_ttl = ttl - 1
            forward_msg = {
                'type': MessageType.GOSSIP,
                'msg_id': msg_id,
                'ttl': new_ttl,
                'data': data
            }
            self._forward_gossip(forward_msg)
    
    def _handle_ping(self, message: dict, addr: Tuple[str, int]):
        """Handle PING message"""
        sender_addr = f"{addr[0]}:{addr[1]}"
        
        # Update last seen
        with self.lock:
            if sender_addr in self.peers:
                self.peers[sender_addr].last_seen = time.time()
        
        # Send PONG
        pong_msg = {
            'type': MessageType.PONG,
            'node_id': self.node_id
        }
        print(f"[Node {self.node_id[:8]}] Sending PONG")
        self._send_message(pong_msg, addr)
    
    def _handle_pong(self, message: dict, addr: Tuple[str, int]):
        """Handle PONG message"""
        sender_addr = f"{addr[0]}:{addr[1]}"
        
        with self.lock:
            if sender_addr in self.peers:
                self.peers[sender_addr].last_seen = time.time()
    
    def _handle_ihave(self, message: dict, addr: Tuple[str, int]):
        """Handle IHAVE message (push-pull)"""
        msg_ids = message.get('msg_ids', [])
        sender_addr = f"{addr[0]}:{addr[1]}"
        
        # Find missing messages
        missing = []
        with self.lock:
            for msg_id in msg_ids:
                if msg_id not in self.seen_set:
                    missing.append(msg_id)
        
        # Request missing messages
        if missing:
            iwant_msg = {
                'type': MessageType.IWANT,
                'node_id': self.node_id,
                'msg_ids': missing
            }
            print(f"[Node {self.node_id[:8]}] Sending IWANT")
            self._send_message(iwant_msg, addr)
    
    def _handle_iwant(self, message: dict, addr: Tuple[str, int]):
        """Handle IWANT message (push-pull)"""
        requested_ids = message.get('msg_ids', [])
        
        with self.lock:
            for msg_id in requested_ids:
                if msg_id in self.message_store:
                    gossip_msg = self.message_store[msg_id].copy()
                    gossip_msg['ttl'] = self.config.ttl  # Reset TTL for push-pull
                    self._send_message(gossip_msg, addr)
    
    def _forward_gossip(self, message: dict):
        """Forward gossip message to fanout random peers"""
        with self.lock:
            available_peers = [p for p in self.peers.values()]
        
        if not available_peers:
            return
        
        # Select fanout random peers
        selected = random.sample(
            available_peers,
            min(self.config.fanout, len(available_peers))
        )
        
        for peer in selected:
            addr = peer.addr.split(':')
            self._send_message(message, (addr[0], int(addr[1])))
            with self.lock:
                self.stats['gossip_sent'] += 1
    
    def _ping_loop(self):
        """Periodically ping peers"""
        while self.running:
            time.sleep(self.config.ping_interval)
            
            with self.lock:
                peers_to_ping = list(self.peers.values())
            
            for peer in peers_to_ping:
                ping_msg = {
                    'type': MessageType.PING,
                    'node_id': self.node_id
                }
                addr = peer.addr.split(':')
                print(f"[Node {self.node_id[:8]}] Sending PING")
                self._send_message(ping_msg, (addr[0], int(addr[1])))
                
                with self.lock:
                    if peer.addr in self.peers:
                        self.peers[peer.addr].last_ping = time.time()
    
    def _push_pull_loop(self):
        """Periodically send IHAVE messages"""
        while self.running:
            time.sleep(self.config.push_pull_interval)
            
            with self.lock:
                msg_ids = list(self.seen_set)
                peers_to_contact = list(self.peers.values())
            
            if not msg_ids or not peers_to_contact:
                continue
            
            # Send IHAVE to random peer
            if peers_to_contact:
                peer = random.choice(peers_to_contact)
                ihave_msg = {
                    'type': MessageType.IHAVE,
                    'node_id': self.node_id,
                    'msg_ids': msg_ids[:50]  # Limit to avoid large messages
                }
                addr = peer.addr.split(':')
                print(f"[Node {self.node_id[:8]}] Sending IHAVE")
                self._send_message(ihave_msg, (addr[0], int(addr[1])))
    
    def _cleanup_loop(self):
        """Remove timed-out peers"""
        while self.running:
            time.sleep(5.0)
            
            current_time = time.time()
            with self.lock:
                to_remove = []
                for addr, peer in self.peers.items():
                    if current_time - peer.last_seen > self.config.peer_timeout:
                        to_remove.append(addr)
                
                for addr in to_remove:
                    del self.peers[addr]
                    print(f"[Node {self.node_id[:8]}] Removed timed-out peer: {addr}")
    
    def _send_message(self, message: dict, addr: Tuple[str, int]):
        """Send message via UDP"""
        try:
            data = json.dumps(message).encode()
            self.sock.sendto(data, addr)
            with self.lock:
                self.stats['messages_sent'] += 1
        except Exception as e:
            print(f"[Node {self.node_id[:8]}] Send error: {e}")
    
    def send_gossip(self, data: str):
        """Send a new gossip message"""
        msg_id = str(uuid.uuid4())
        message = {
            'type': MessageType.GOSSIP,
            'msg_id': msg_id,
            'ttl': self.config.ttl,
            'data': data
        }
        
        with self.lock:
            self.seen_set.add(msg_id)
            self.message_store[msg_id] = message
            self.stats['gossip_sent'] += 1
        
        print(f"[Node {self.node_id[:8]}] Sending GOSSIP: {msg_id[:8]}")
        self._forward_gossip(message)
    
    def _input_loop(self):
        """Handle user input"""
        print(f"\n[Node {self.node_id[:8]}] Ready! Commands:")
        print("  'gossip <message>' - Send a gossip message")
        print("  'peers' - List all peers")
        print("  'stats' - Show statistics")
        print("  'quit' - Shutdown node\n")
        
        # Check if stdin is available (not running in background)
        has_stdin = sys.stdin.isatty()
        
        if not has_stdin:
            print(f"[Node {self.node_id[:8]}] Running in background mode (no stdin)")
            # Keep running until explicitly stopped
            while self.running:
                time.sleep(1)
            print(f"[Node {self.node_id[:8]}] Shutting down...")
            self.sock.close()
            return
        
        while self.running:
            try:
                cmd = input().strip().split(' ', 1)
                if not cmd:
                    continue
                
                if cmd[0] == 'quit':
                    self.running = False
                    break
                elif cmd[0] == 'peers':
                    with self.lock:
                        print(f"\n[Node {self.node_id[:8]}] Peers ({len(self.peers)}):")
                        for peer in self.peers.values():
                            print(f"  {peer.node_id[:8]} @ {peer.addr}")
                        print()
                elif cmd[0] == 'stats':
                    with self.lock:
                        print(f"\n[Node {self.node_id[:8]}] Statistics:")
                        print(f"  Messages sent: {self.stats['messages_sent']}")
                        print(f"  Messages received: {self.stats['messages_received']}")
                        print(f"  Gossip sent: {self.stats['gossip_sent']}")
                        print(f"  Gossip received: {self.stats['gossip_received']}")
                        print(f"  Seen messages: {len(self.seen_set)}")
                        print()
                elif cmd[0] == 'gossip' and len(cmd) > 1:
                    self.send_gossip(cmd[1])
                else:
                    print("Unknown command")
            except EOFError:
                # EOF when stdin closes - keep running in background
                print(f"[Node {self.node_id[:8]}] Stdin closed, continuing in background...")
                while self.running:
                    time.sleep(1)
                break
            except Exception as e:
                print(f"Input error: {e}")
        
        print(f"[Node {self.node_id[:8]}] Shutting down...")
        self.sock.close()
    
    def _solve_pow(self, node_id: str, k: int) -> int:
        """Solve proof-of-work: find nonce such that hash(node_id + nonce) has k leading zeros"""
        nonce = 0
        target = '0' * k
        
        while True:
            data = f"{node_id}{nonce}".encode()
            hash_result = hashlib.sha256(data).hexdigest()
            if hash_result[:k] == target:
                return nonce
            nonce += 1
    
    def _verify_pow(self, node_id: str, nonce: int, k: int) -> bool:
        """Verify proof-of-work"""
        data = f"{node_id}{nonce}".encode()
        hash_result = hashlib.sha256(data).hexdigest()
        return hash_result[:k] == '0' * k


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(description='Gossip Protocol Node')
    parser.add_argument('--port', type=int, required=True, help='Port to bind to')
    parser.add_argument('--bootstrap', type=str, help='Bootstrap node address (ip:port)')
    parser.add_argument('--fanout', type=int, default=3, help='Fanout parameter (default: 3)')
    parser.add_argument('--peer_limit', type=int, default=10, help='Maximum number of peers (default: 10)')
    parser.add_argument('--ping_interval', type=float, default=5.0, help='PING interval in seconds (default: 5.0)')
    parser.add_argument('--peer_timeout', type=float, default=15.0, help='Peer timeout in seconds (default: 15.0)')
    parser.add_argument('--ttl', type=int, default=10, help='TTL for gossip messages (default: 10)')
    parser.add_argument('--pow_k', type=int, default=4, help='PoW difficulty (leading zeros, default: 4)')
    parser.add_argument('--push_pull_interval', type=float, default=10.0, help='Push-pull interval in seconds (default: 10.0)')
    
    args = parser.parse_args()
    
    config = Config(
        fanout=args.fanout,
        peer_limit=args.peer_limit,
        ping_interval=args.ping_interval,
        peer_timeout=args.peer_timeout,
        ttl=args.ttl,
        pow_k=args.pow_k,
        push_pull_interval=args.push_pull_interval
    )
    
    node = Node(args.port, args.bootstrap, config)
    node.start()


if __name__ == '__main__':
    main()

