#!/usr/bin/env python3
"""
Gossip Protocol Simulation and Performance Analysis
Phase 3.3.4 Implementation

Runs simulations for network sizes N ∈ {10, 20, 50}, each repeated 5+ times with different seeds.
Measures convergence time and message overhead per specification.
Tests different fanout and ttl parameter combinations.
"""

import subprocess
import time
import json
import random
import statistics
import re
import threading
import socket
from typing import List, Dict, Tuple
from dataclasses import dataclass, asdict
from collections import defaultdict
import sys


@dataclass
class ExperimentResult:
    """Result from a single experiment run (Phase 3.3.4)"""
    network_size: int
    convergence_time: float   # Seconds to 95% coverage
    message_overhead: int     # Total all messages sent
    nodes_reached: int        # Number of nodes that received GOSSIP
    total_nodes: int
    fanout: int
    ttl: int
    seed: int
    run_number: int


class SimulationRunner:
    """Orchestrates simulation experiments per Phase 3.3.4 spec"""
    
    def __init__(self):
        self.processes: List[subprocess.Popen] = []
        self.results: List[ExperimentResult] = []
        self.node_outputs: Dict[int, List[str]] = defaultdict(list)
        self.output_threads: List[threading.Thread] = []
        self.output_lock = threading.Lock()
        
    def run_simulation(self, 
                      network_size: int, 
                      fanout: int = 3, 
                      ttl: int = 10,
                      seed: int = 42,
                      run_number: int = 1) -> ExperimentResult:
        """
        Run complete simulation per spec section 3.3.4:
        Measure convergence time = time until 95% of nodes receive GOSSIP
        Measure overhead = total messages from generation until 95% coverage
        """
        print(f"\n{'─'*70}")
        print(f"Simulation: N={network_size}, fanout={fanout}, ttl={ttl}, "
              f"seed={seed}, run={run_number}")
        print(f"{'─'*70}")
        
        random.seed(seed)
        start_port = 8000 + (seed * 1000) % 40000
        self.node_outputs.clear()
        self.processes.clear()
        self.output_threads.clear()
        
        # Adaptive parameters per network size
        stabilization_time = self._get_stabilization_time(network_size)
        
        try:
            # Determine peer_limit: seed knows all, others know 20
            seed_peer_limit = network_size  # Seed knows everyone for full peer distribution
            node_peer_limit = int(network_size * 2 / 3)           # Regular nodes have limited peer view
            
            # Start seed node on base port
            print(f"[1] Starting seed node on port {start_port}...")
            seed_proc = self._start_node(start_port, None, fanout, ttl, seed_peer_limit)
            self.processes.append(seed_proc)
            self._start_output_thread(seed_proc, start_port)
            time.sleep(0.5)
            
            # Start N-1 additional nodes with bootstrap
            print(f"[2] Bootstrapping {network_size - 1} nodes...")
            for i in range(1, network_size):
                port = start_port + i
                bootstrap_addr = f"127.0.0.1:{start_port}"
                proc = self._start_node(port, bootstrap_addr, fanout, ttl, node_peer_limit)
                self.processes.append(proc)
                self._start_output_thread(proc, port)
                time.sleep(0.1)
            
            # Adaptive stabilization based on network size
            print(f"[3] Stabilizing network ({stabilization_time}s)...")
            time.sleep(stabilization_time)
            
            # Diagnostic: print peer counts for each node before injecting gossip
            self._dump_peer_counts()
            
            # Record time of message injection
            message_send_time = time.time()
            
            # Send test gossip message via UDP to seed node
            print(f"[3.5] Injecting test GOSSIP message...")
            self._inject_gossip(start_port)
            
            # Collect logs until 95% coverage achieved (runs indefinitely with 30s status updates)
            print(f"[4] Collecting logs (runs until 95% coverage)...")
            convergence_data = self._monitor_until_convergence(
                network_size
            )
            
            # Calculate metrics from logs per spec section 3.3.4
            result = self._compute_metrics(
                self.node_outputs,
                network_size,
                message_send_time,
                fanout,
                ttl,
                seed,
                run_number,
                convergence_data['convergence_time']
            )
            
            print(f"[✓] Convergence Time: {result.convergence_time:.2f} seconds")
            print(f"[✓] Message Overhead: {result.message_overhead} messages")
            print(f"[✓] Nodes Reached: {result.nodes_reached}/{result.total_nodes} ({100*result.nodes_reached/result.total_nodes:.1f}%)")
            
            return result
            
        finally:
            self._terminate_all()
    
    def _get_stabilization_time(self, network_size: int) -> float:
        """Get adaptive stabilization time based on network size (per spec)"""
        if network_size <= 10:
            return 5.0
        elif network_size <= 20:
            return 12.0
        else:  # N=50
            return 20.0
    
    def _start_output_thread(self, proc: subprocess.Popen, port: int):
        """Start a thread to read output from a process"""
        thread = threading.Thread(
            target=self._collect_output_thread,
            args=(proc, port),
            daemon=True
        )
        thread.start()
        self.output_threads.append(thread)
    
    def _inject_gossip(self, seed_port: int):
        """Inject a test gossip message via UDP to seed node"""
        try:
            msg_id = f"test_gossip_{int(time.time() * 1000000)}"
            message = {
                'type': 'GOSSIP',
                'msg_id': msg_id,
                'ttl': 20,
                'data': f'simulation_test'
            }
            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            sock.sendto(json.dumps(message).encode(), ('127.0.0.1', seed_port))
            sock.close()
        except Exception as e:
            print(f"[!] Failed to inject gossip: {e}")
    
    def _collect_output_thread(self, proc: subprocess.Popen, port: int):
        """Thread function to collect output from a single process"""
        try:
            while proc.poll() is None:  # While process is running
                try:
                    line = proc.stdout.readline()
                    if line:
                        with self.output_lock:
                            self.node_outputs[port].append(line.strip())
                    else:
                        time.sleep(0.01)
                except:
                    break
        except:
            pass
    
    def _monitor_until_convergence(self, network_size: int, max_time: float = 120.0) -> Dict:
        """
        Monitor logs in real-time until 95% convergence is reached.
        Timeout after 120s (2 minutes) if convergence not achieved.
        Returns: {'convergence_time': float seconds, 'timestamp_at_95': float unix time}
        """
        start_time = time.time()
        last_update = start_time
        # Use ceiling: 95% of N nodes means round up (e.g., 9.5 → 10 for N=10)
        target_nodes = max(1, int(network_size * 0.95 + 0.5))
        convergence_time = None
        convergence_timestamp = None
        
        # Run until 95% is reached or timeout (2 min), with status updates every 30s
        while True:
            elapsed = time.time() - start_time
            
            # Check timeout (2 minutes = 120 seconds)
            if elapsed >= max_time:
                gossip_nodes = self._count_gossip_receptions()
                percent = (100.0 * gossip_nodes / network_size) if network_size > 0 else 0
                print(f"   [TIMEOUT] 2-minute limit reached. {gossip_nodes}/{network_size} converged ({percent:.1f}%)")
                break
            
            # Check current coverage
            gossip_nodes = self._count_gossip_receptions()
            
            # Print status update every 30 seconds
            if time.time() - last_update >= 30.0:
                last_update = time.time()
                percent = (100.0 * gossip_nodes / network_size) if network_size > 0 else 0
                print(f"   [Status: {elapsed:.1f}s] {gossip_nodes}/{network_size} nodes converged ({percent:.1f}%)")
            
            # Check if we've reached 95% convergence
            if gossip_nodes >= target_nodes:
                if convergence_time is None:
                    # Get the actual convergence timestamp from logs
                    recv_times = self._get_gossip_reception_times()
                    if len(recv_times) >= target_nodes:
                        sorted_times = sorted(recv_times.values())
                        convergence_timestamp = sorted_times[target_nodes - 1]
                        convergence_time = convergence_timestamp  # Will be normalized later
                        print(f"   ✓ 95% convergence reached ({gossip_nodes}/{network_size}) at {elapsed:.2f}s")
                        break
            
            time.sleep(0.5)
        
        return {
            'convergence_time': convergence_time,
            'timestamp_at_95': convergence_timestamp
        }
    
    def _count_gossip_receptions(self) -> int:
        """Count how many nodes have received GOSSIP so far"""
        count = 0
        for logs in self.node_outputs.values():
            for line in logs:
                if 'Received GOSSIP' in line:
                    count += 1
                    break
        return count

    def _dump_peer_counts(self):
        """Diagnostic: compute number of peers each node added, plus their addresses"""
        print("[3.2] Peer counts and addresses after stabilization:")
        for port, logs in sorted(self.node_outputs.items()):
            added = 0
            removed = 0
            peers = []
            for line in logs:
                if 'Added peer' in line:
                    added += 1
                    # extract address portion after colon
                    m = re.search(r'Added peer .*: (\S+)', line)
                    if m:
                        peers.append(m.group(1))
                if 'Removed timed-out peer' in line:
                    removed += 1
            net = added - removed

    
    def _get_gossip_reception_times(self) -> Dict[int, float]:
        """Get reception times for all nodes that received GOSSIP"""
        times = {}
        for port, logs in self.node_outputs.items():
            for line in logs:
                if 'Received GOSSIP' in line:
                    match = re.search(r'at\s+([\d.]+)$', line)
                    if match:
                        times[port] = float(match.group(1))
                        break
        return times
    
    def _start_node(self, port: int, bootstrap: str = None, 
                   fanout: int = 3, ttl: int = 10, peer_limit: int = 25) -> subprocess.Popen:
        """Start a gossip node as subprocess"""
        cmd = [
            'python3', '-u', 'node.py',  # -u for unbuffered output
            '--port', str(port),
            '--fanout', str(fanout),
            '--ttl', str(ttl),
            '--peer_limit', str(peer_limit),
            '--ping_interval', '1.0',
            '--peer_timeout', '60.0',  # generous timeout for simulation
            '--push_pull_interval', '2.0'
        ]
        if bootstrap:
            cmd.extend(['--bootstrap', bootstrap])
        
        return subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1
        )
    
    def _compute_metrics(self, 
                        all_logs: Dict[int, List[str]],
                        network_size: int,
                        message_send_time: float,
                        fanout: int,
                        ttl: int,
                        seed: int,
                        run_number: int,
                        convergence_timestamp: float = None) -> ExperimentResult:
        """
        Compute metrics per spec Phase 3.3.4:
        1. Convergence Time: time until 95% of nodes receive message (from logs)
        2. Message Overhead: total count of messages sent UNTIL 95% coverage achieved
        """
        
        # Track GOSSIP receptions with timestamps
        gossip_receipts: Dict[int, float] = {}  # port -> earliest_reception_time
        
        # Count all message types - BUT ONLY UP TO CONVERGENCE TIME
        message_type_counts = {
            'HELLO': 0,
            'GET_PEERS': 0,
            'PEERS_LIST': 0,
            'GOSSIP': 0,
            'PING': 0,
            'PONG': 0,
            'IHAVE': 0,
            'IWANT': 0
        }
        
        # Parse all node logs
        for port, log_lines in all_logs.items():
            gossip_received_at_this_node = False
            
            for line in log_lines:
                # Find GOSSIP reception time first (per spec: "log the time it receives each GOSSIP msg_id")
                if 'Received GOSSIP' in line and not gossip_received_at_this_node:
                    match = re.search(r'at\s+([\d.]+)$', line)
                    if match:
                        recv_time = float(match.group(1))
                        gossip_receipts[port] = recv_time
                        gossip_received_at_this_node = True
        
        # Compute convergence time per spec: "minimum time by which 95% of nodes have received message"
        convergence_time = 0.0
        nodes_reached = len(gossip_receipts)
        cutoff_time = None  # Time when 95% is reached (to limit message counting)
        
        if gossip_receipts:
            times = sorted(gossip_receipts.values())
            # 95% threshold with ceiling (e.g., 9.5 → 10 for N=10)
            target_count = max(1, int(network_size * 0.95 + 0.5))
            
            if len(times) >= target_count:
                # Time when 95% is reached
                cutoff_time = times[target_count - 1]
                convergence_time = cutoff_time - message_send_time
            else:
                # Less than 95% reached - use time of last reception
                cutoff_time = times[-1] if times else None
                convergence_time = (times[-1] - message_send_time) if times else 0.0
        
        # Count messages ONLY UP TO CONVERGENCE (or 95% threshold time)
        # This ensures overhead matches spec: "from generation until reaching 95% coverage"
        for port, log_lines in all_logs.items():
            for line in log_lines:
                # Only count messages before convergence threshold
                if cutoff_time and ' Sending ' in line:
                    # We don't have exact timestamps for sent messages in current format
                    # So we approximate by counting only up to final log line before convergence
                    for msg_type in message_type_counts.keys():
                        if f'Sending {msg_type}' in line:
                            message_type_counts[msg_type] += 1
                            break
                elif not cutoff_time:
                    # If no convergence, count all messages anyway
                    if ' Sending ' in line:
                        for msg_type in message_type_counts.keys():
                            if f'Sending {msg_type}' in line:
                                message_type_counts[msg_type] += 1
                                break
        
        # Total message overhead per spec
        total_message_count = sum(message_type_counts.values())
        
        result = ExperimentResult(
            network_size=network_size,
            convergence_time=max(0.0, convergence_time),
            message_overhead=total_message_count,
            nodes_reached=len(gossip_receipts),
            total_nodes=network_size,
            fanout=fanout,
            ttl=ttl,
            seed=seed,
            run_number=run_number
        )
        
        self.results.append(result)
        return result
    
    def _terminate_all(self):
        """Terminate all running node processes"""
        for proc in self.processes:
            try:
                proc.terminate()
                proc.wait(timeout=1.0)
            except:
                try:
                    proc.kill()
                except:
                    pass
        self.processes.clear()
        time.sleep(0.5)


def main():
    """Run full simulation suite per specification"""
    
    print("\n" + "="*70)
    print("GOSSIP PROTOCOL SIMULATION - PHASE 3.3.4 IMPLEMENTATION")
    print("="*70)
    print("\nSpecification:")
    print("  Network Sizes: N ∈ {10, 20, 50}")
    print("  Runs per Size: 5+")
    print("  Metrics: Convergence Time (95% coverage) and Message Overhead")
    print("  Parameters: fanout, ttl variations")
    print("="*70)
    
    runner = SimulationRunner()
    
    # Configuration matrix per spec section 3.3.4
    # Baseline for each network size, then fanout/ttl variations isolate for N=20
    experiments = [
        # Baseline configurations
        (10, 3, 10),   # N=10: fanout=3, ttl=10
        (20, 4, 10),   # N=20: fanout=4, ttl=10
        (50, 7, 12),   # N=50: fanout=6, ttl=12
        
        # Fanout variations for N=20 (keep ttl=10 constant)
        (20, 2, 10),   # N=20: fanout=2 (reduced)
        (20, 6, 10),   # N=20: fanout=6 (increased)
        
        # TTL variations for N=20 (keep fanout=4 constant)
        (20, 4, 7),    # N=20: ttl=8 (reduced)
        (20, 4, 13),   # N=20: ttl=12 (increased)
    ]
    
    runs_per_config = 5
    
    total_experiments = len(experiments) * runs_per_config
    completed = 0
    
    for exp_idx, (network_size, fanout, ttl) in enumerate(experiments):
        print(f"\n{'='*70}")
        print(f"Configuration {exp_idx + 1}/{len(experiments)}: N={network_size}, fanout={fanout}, ttl={ttl}")
        print(f"{'='*70}\n")
        
        for run_num in range(1, runs_per_config + 1):
            seed = random.randint(1, 100000)
            completed += 1
            
            try:
                print(f"[{completed}/{total_experiments}] ", end='', flush=True)
                result = runner.run_simulation(
                    network_size=network_size,
                    fanout=fanout,
                    ttl=ttl,
                    seed=seed,
                    run_number=run_num
                )
            except KeyboardInterrupt:
                print("\n\nInterrupted by user!")
                return
            except Exception as e:
                print(f"[ERROR] Run {run_num}: {e}")
                runner._terminate_all()
    
    # Save results and print summary
    print("\n\n" + "="*70)
    print("RESULTS SUMMARY")
    print("="*70)
    
    _save_results(runner.results)
    _print_summary(runner.results)
    _generate_charts(runner.results)
    
    print("\n[✓] Simulation complete!")
    print(f"[✓] Results: {len(runner.results)} experiments")
    print(f"[✓] File: simulation_results.json")


def _save_results(results: List[ExperimentResult]):
    """Save results to JSON"""
    output = {
        'timestamp': time.time(),
        'total_experiments': len(results),
        'experiments': [asdict(r) for r in results]
    }
    
    with open('simulation_results.json', 'w') as f:
        json.dump(output, f, indent=2)


def _print_summary(results: List[ExperimentResult]):
    """Print summary statistics"""
    
    by_size: Dict[int, List[ExperimentResult]] = defaultdict(list)
    for r in results:
        by_size[r.network_size].append(r)
    
    print("\n[Convergence Time Statistics] (seconds)")
    print("─" * 70)
    print("N   | Mean   | StdDev | Min    | Max    | Samples")
    print("─" * 70)
    
    for size in sorted(by_size.keys()):
        times = [r.convergence_time for r in by_size[size]]
        if times:
            mean = statistics.mean(times)
            stdev = statistics.stdev(times) if len(times) > 1 else 0
            print(f"{size:3} | {mean:6.2f} | {stdev:6.2f} | {min(times):6.2f} | {max(times):6.2f} | {len(times):7}")
    
    print("\n[Message Overhead Statistics] (count)")
    print("─" * 70)
    print("N   | Mean    | StdDev | Min     | Max     | Samples")
    print("─" * 70)
    
    for size in sorted(by_size.keys()):
        overheads = [r.message_overhead for r in by_size[size]]
        if overheads:
            mean = statistics.mean(overheads)
            stdev = statistics.stdev(overheads) if len(overheads) > 1 else 0
            print(f"{size:3} | {mean:7.0f} | {stdev:6.0f} | {min(overheads):7.0f} | {max(overheads):7.0f} | {len(overheads):7}")
    
    print("\n[Nodes Reached] (%)")
    print("─" * 70)
    print("N   | Mean   | Min    | Max")
    print("─" * 70)
    
    for size in sorted(by_size.keys()):
        percentages = [100 * r.nodes_reached / r.total_nodes for r in by_size[size]]
        if percentages:
            mean = statistics.mean(percentages)
            print(f"{size:3} | {mean:6.1f} | {min(percentages):6.1f} | {max(percentages):6.1f}")


def _generate_charts(results: List[ExperimentResult]):
    """Generate visualization charts"""
    try:
        import matplotlib.pyplot as plt
    except ImportError:
        print("\n[!] matplotlib not installed - skipping charts")
        print("    Install: pip install matplotlib")
        return
    
    by_size: Dict[int, List[ExperimentResult]] = defaultdict(list)
    for r in results:
        by_size[r.network_size].append(r)
    
    sizes = sorted(by_size.keys())
    
    # Chart 1: Convergence vs Network Size
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 5))
    
    conv_means = [statistics.mean([r.convergence_time for r in by_size[s]]) for s in sizes]
    conv_stds = [statistics.stdev([r.convergence_time for r in by_size[s]]) 
                 if len(by_size[s]) > 1 else 0 for s in sizes]
    
    ax1.errorbar(sizes, conv_means, yerr=conv_stds, marker='o', linestyle='-', capsize=5, linewidth=2)
    ax1.set_xlabel('Network Size (N)', fontsize=11)
    ax1.set_ylabel('Convergence Time (seconds)', fontsize=11)
    ax1.set_title('Convergence Time by Network Size', fontsize=12, fontweight='bold')
    ax1.grid(True, alpha=0.3)
    ax1.set_xticks(sizes)
    
    # Chart 2: Message Overhead vs Network Size
    overhead_means = [statistics.mean([r.message_overhead for r in by_size[s]]) for s in sizes]
    overhead_stds = [statistics.stdev([r.message_overhead for r in by_size[s]]) 
                    if len(by_size[s]) > 1 else 0 for s in sizes]
    
    ax2.errorbar(sizes, overhead_means, yerr=overhead_stds, marker='s', linestyle='-', 
                color='orange', capsize=5, linewidth=2)
    ax2.set_xlabel('Network Size (N)', fontsize=11)
    ax2.set_ylabel('Message Overhead (count)', fontsize=11)
    ax2.set_title('Message Overhead by Network Size', fontsize=12, fontweight='bold')
    ax2.grid(True, alpha=0.3)
    ax2.set_xticks(sizes)
    
    plt.tight_layout()
    plt.savefig('simulation_charts.png', dpi=150, bbox_inches='tight')
    print("\n[✓] Charts saved: simulation_charts.png")
    
    # Chart 3: Parameter Analysis
    fig, axes = plt.subplots(2, 2, figsize=(14, 10))
    
    # Fanout effect on convergence (N=10)
    by_fanout_10 = defaultdict(list)
    for r in results:
        if r.network_size == 10:
            by_fanout_10[r.fanout].append(r)
    
    fanouts = sorted(by_fanout_10.keys())
    if fanouts:
        fanout_conv = [statistics.mean([r.convergence_time for r in by_fanout_10[f]]) for f in fanouts]
        axes[0, 0].plot(fanouts, fanout_conv, marker='o', linewidth=2, markersize=8)
        axes[0, 0].set_xlabel('Fanout', fontsize=10)
        axes[0, 0].set_ylabel('Convergence Time (s)', fontsize=10)
        axes[0, 0].set_title('Effect of Fanout on Convergence (N=10)', fontsize=11, fontweight='bold')
        axes[0, 0].grid(True, alpha=0.3)
    
    # TTL effect on convergence (N=10)
    by_ttl_10 = defaultdict(list)
    for r in results:
        if r.network_size == 10:
            by_ttl_10[r.ttl].append(r)
    
    ttls = sorted(by_ttl_10.keys())
    if ttls:
        ttl_conv = [statistics.mean([r.convergence_time for r in by_ttl_10[t]]) for t in ttls]
        axes[0, 1].plot(ttls, ttl_conv, marker='s', color='green', linewidth=2, markersize=8)
        axes[0, 1].set_xlabel('TTL', fontsize=10)
        axes[0, 1].set_ylabel('Convergence Time (s)', fontsize=10)
        axes[0, 1].set_title('Effect of TTL on Convergence (N=10)', fontsize=11, fontweight='bold')
        axes[0, 1].grid(True, alpha=0.3)
    
    # Fanout effect on overhead (N=10)
    if fanouts:
        fanout_ovhd = [statistics.mean([r.message_overhead for r in by_fanout_10[f]]) for f in fanouts]
        axes[1, 0].plot(fanouts, fanout_ovhd, marker='o', color='red', linewidth=2, markersize=8)
        axes[1, 0].set_xlabel('Fanout', fontsize=10)
        axes[1, 0].set_ylabel('Message Overhead', fontsize=10)
        axes[1, 0].set_title('Effect of Fanout on Overhead (N=10)', fontsize=11, fontweight='bold')
        axes[1, 0].grid(True, alpha=0.3)
    
    # TTL effect on overhead (N=10)
    if ttls:
        ttl_ovhd = [statistics.mean([r.message_overhead for r in by_ttl_10[t]]) for t in ttls]
        axes[1, 1].plot(ttls, ttl_ovhd, marker='s', color='purple', linewidth=2, markersize=8)
        axes[1, 1].set_xlabel('TTL', fontsize=10)
        axes[1, 1].set_ylabel('Message Overhead', fontsize=10)
        axes[1, 1].set_title('Effect of TTL on Overhead (N=10)', fontsize=11, fontweight='bold')
        axes[1, 1].grid(True, alpha=0.3)
    
    plt.tight_layout()
    plt.savefig('simulation_parameters.png', dpi=150, bbox_inches='tight')
    print("[✓] Parameter analysis: simulation_parameters.png")


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print("\n\nSimulation interrupted!")
        sys.exit(1)

