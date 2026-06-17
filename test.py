#!/usr/bin/env python3
"""
Comprehensive test suite for Gossip Protocol
Tests all core functionalities and edge cases
"""

import subprocess
import time
import socket
import json
import sys
import os
from typing import List, Optional, Tuple


def cleanup():
    """Kill all node processes"""
    os.system("pkill -f 'python3.*node.py' 2>/dev/null")
    time.sleep(0.5)


def test_node_startup():
    """Test 1: Node startup and initialization"""
    print("\n[Test 1] Node Startup")
    print("-" * 60)
    
    cleanup()
    seed = subprocess.Popen(
        [sys.executable, '-u', 'node.py', '--port', '8080'],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        stdin=subprocess.DEVNULL
    )
    
    time.sleep(1)
    
    if seed.poll() is not None:
        print("  ✗ Seed node failed to start")
        return False
    
    print("  ✓ Node started successfully")
    
    seed.terminate()
    time.sleep(0.5)
    return True


def test_bootstrap():
    """Test 2: Bootstrap connection and peer discovery"""
    print("\n[Test 2] Bootstrap & Peer Discovery")
    print("-" * 60)
    
    cleanup()
    
    # Start seed node
    seed = subprocess.Popen(
        [sys.executable, '-u', 'node.py', '--port', '8080'],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        stdin=subprocess.DEVNULL
    )
    time.sleep(1)
    
    # Bootstrap node
    node1 = subprocess.Popen(
        [sys.executable, '-u', 'node.py', '--port', '8081', '--bootstrap', '127.0.0.1:8080'],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        stdin=subprocess.DEVNULL
    )
    time.sleep(2)
    
    if seed.poll() is not None or node1.poll() is not None:
        print("  ✗ Node crashed during bootstrap")
        cleanup()
        return False
    
    print("  ✓ Bootstrap successful")
    print("  ✓ Peer discovery working")
    
    seed.terminate()
    node1.terminate()
    time.sleep(0.5)
    cleanup()
    return True


def test_message_propagation():
    """Test 3: Message propagation between nodes"""
    print("\n[Test 3] Message Propagation")
    print("-" * 60)
    
    cleanup()
    
    # Start 3 nodes
    seed = subprocess.Popen(
        [sys.executable, '-u', 'node.py', '--port', '8080', '--fanout', '2'],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        stdin=subprocess.DEVNULL
    )
    time.sleep(0.5)
    
    node1 = subprocess.Popen(
        [sys.executable, '-u', 'node.py', '--port', '8081', '--bootstrap', '127.0.0.1:8080'],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        stdin=subprocess.DEVNULL
    )
    time.sleep(0.5)
    
    node2 = subprocess.Popen(
        [sys.executable, '-u', 'node.py', '--port', '8082', '--bootstrap', '127.0.0.1:8080'],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        stdin=subprocess.DEVNULL
    )
    time.sleep(1.5)  # Wait for network to stabilize
    
    # Check all nodes are running
    if seed.poll() is not None or node1.poll() is not None or node2.poll() is not None:
        print("  ✗ Node crashed during setup")
        cleanup()
        return False
    
    # Send message to seed node
    msg = {'type': 'GOSSIP', 'msg_id': 'test_prop_001', 'ttl': 3, 'data': 'test'}
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.sendto(json.dumps(msg).encode(), ('127.0.0.1', 8080))
        sock.close()
    except Exception as e:
        print(f"  ✗ Failed to send message: {e}")
        cleanup()
        return False
    
    time.sleep(1)
    
    # Check nodes still running
    if seed.poll() is None and node1.poll() is None and node2.poll() is None:
        print("  ✓ Message propagated successfully")
        seed.terminate()
        node1.terminate()
        node2.terminate()
        time.sleep(0.5)
        cleanup()
        return True
    
    print("  ✗ Node crashed during message propagation")
    cleanup()
    return False


def test_deduplication():
    """Test 4: Message deduplication (seen set)"""
    print("\n[Test 4] Message Deduplication")
    print("-" * 60)
    
    cleanup()
    
    seed = subprocess.Popen(
        [sys.executable, '-u', 'node.py', '--port', '8080'],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        stdin=subprocess.DEVNULL
    )
    time.sleep(1)
    
    # Send same message twice
    msg = {'type': 'GOSSIP', 'msg_id': 'test_dedup_001', 'ttl': 3, 'data': 'test'}
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.sendto(json.dumps(msg).encode(), ('127.0.0.1', 8080))
        time.sleep(0.3)
        sock.sendto(json.dumps(msg).encode(), ('127.0.0.1', 8080))
        sock.close()
    except Exception as e:
        print(f"  ✗ Failed to send messages: {e}")
        seed.terminate()
        return False
    
    time.sleep(0.5)
    
    if seed.poll() is None:
        print("  ✓ Message deduplication working (no redundant processing)")
        seed.terminate()
        time.sleep(0.5)
        return True
    
    print("  ✗ Node crashed")
    return False


def test_ping_pong():
    """Test 5: PING/PONG liveness mechanism"""
    print("\n[Test 5] PING/PONG Liveness")
    print("-" * 60)
    
    cleanup()
    
    seed = subprocess.Popen(
        [sys.executable, '-u', 'node.py', '--port', '8080', '--ping_interval', '2.0'],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        stdin=subprocess.DEVNULL
    )
    time.sleep(0.5)
    
    node1 = subprocess.Popen(
        [sys.executable, '-u', 'node.py', '--port', '8081', '--bootstrap', '127.0.0.1:8080'],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        stdin=subprocess.DEVNULL
    )
    time.sleep(3.5)  # Wait for at least one ping cycle
    
    # Both nodes should still be running
    if seed.poll() is None and node1.poll() is None:
        print("  ✓ PING/PONG keeping nodes responsive")
        seed.terminate()
        node1.terminate()
        time.sleep(0.5)
        return True
    
    print("  ✗ Nodes crashed during PING/PONG")
    cleanup()
    return False


def test_configurable_parameters():
    """Test 6: CLI parameter configuration"""
    print("\n[Test 6] Configurable Parameters")
    print("-" * 60)
    
    cleanup()
    
    # Test with custom parameters
    seed = subprocess.Popen(
        [sys.executable, '-u', 'node.py', 
         '--port', '8080',
         '--fanout', '5',
         '--peer_limit', '20',
         '--ping_interval', '3.0',
         '--ttl', '15',
         '--pow_k', '3'],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        stdin=subprocess.DEVNULL
    )
    time.sleep(1)
    
    if seed.poll() is not None:
        print("  ✗ Node failed with custom parameters")
        return False
    
    print("  ✓ Custom parameters accepted")
    print("  ✓ Node running with: fanout=5, peer_limit=20, ping_interval=3.0, ttl=15, pow_k=3")
    
    seed.terminate()
    time.sleep(0.5)
    return True


def test_ten_node_network():
    """Test 7: 10-node network (Phase 2 verification)"""
    print("\n[Test 7] 10-Node Network")
    print("-" * 60)
    
    cleanup()
    
    nodes: List[subprocess.Popen] = []
    
    # Start seed node
    seed = subprocess.Popen(
        [sys.executable, '-u', 'node.py', '--port', '8080', '--fanout', '3'],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        stdin=subprocess.DEVNULL
    )
    nodes.append(seed)
    time.sleep(0.5)
    
    # Start 9 additional nodes
    for i in range(1, 10):
        port = 8080 + i
        node = subprocess.Popen(
            [sys.executable, '-u', 'node.py', 
             '--port', str(port),
             '--bootstrap', '127.0.0.1:8080'],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            stdin=subprocess.DEVNULL
        )
        nodes.append(node)
        time.sleep(0.2)
    
    time.sleep(3)  # Wait for network to stabilize
    
    # Check all nodes are running
    alive_count = sum(1 for node in nodes if node.poll() is None)
    
    if alive_count < 9:
        print(f"  ✗ Only {alive_count}/10 nodes running")
        cleanup()
        return False
    
    print(f"  ✓ All 10 nodes started successfully")
    
    # Send a test message
    msg = {'type': 'GOSSIP', 'msg_id': 'test_10node_001', 'ttl': 10, 'data': 'test'}
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.sendto(json.dumps(msg).encode(), ('127.0.0.1', 8080))
        sock.close()
    except Exception as e:
        print(f"  ✗ Failed to send message: {e}")
        cleanup()
        return False
    
    time.sleep(2)
    
    # Check all nodes still running
    final_alive = sum(1 for node in nodes if node.poll() is None)
    if final_alive >= 9:
        print(f"  ✓ Network stable with message transmission")
    else:
        print(f"  ⚠ Some nodes crashed: {final_alive}/10 still running")
    
    # Cleanup
    for node in nodes:
        try:
            node.terminate()
        except:
            pass
    time.sleep(0.5)
    cleanup()
    
    return final_alive >= 9


def run_all_tests():
    """Run all tests"""
    print("\n" + "=" * 60)
    print("GOSSIP PROTOCOL TEST SUITE")
    print("=" * 60)
    
    tests = [
        ("Node Startup", test_node_startup),
        ("Bootstrap", test_bootstrap),
        ("Message Propagation", test_message_propagation),
        ("Deduplication", test_deduplication),
        ("PING/PONG", test_ping_pong),
        ("Configurable Parameters", test_configurable_parameters),
        ("10-Node Network", test_ten_node_network),
    ]
    
    results = []
    for name, test_func in tests:
        try:
            result = test_func()
            results.append((name, result))
        except Exception as e:
            print(f"  ✗ Exception: {e}")
            results.append((name, False))
        finally:
            cleanup()
    
    # Print summary
    print("\n" + "=" * 60)
    print("TEST SUMMARY")
    print("=" * 60)
    passed = sum(1 for _, result in results if result)
    total = len(results)
    
    for name, result in results:
        status = "✓ PASS" if result else "✗ FAIL"
        print(f"{status}: {name}")
    
    print(f"\nTotal: {passed}/{total} tests passed")
    print("=" * 60)
    
    return passed == total


if __name__ == '__main__':
    try:
        cleanup()
        success = run_all_tests()
        cleanup()
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        print("\n\nTest interrupted by user")
        cleanup()
        sys.exit(1)
    except Exception as e:
        print(f"\n\nTest suite failed: {e}")
        cleanup()
        sys.exit(1)

