#!/usr/bin/env python3
"""Create convergence and overhead charts from simulation_results.json.

Usage:
  python3 plot_simulation_results.py [path/to/simulation_results.json]

Saves `convergence.png` and `overhead.png` in the current directory.
"""
import json
import sys
from statistics import mean, stdev

# later we will import matplotlib inside plotting functions


def load_results(path):
    with open(path, 'r') as f:
        return json.load(f)


def extract_metrics(data):
    """Support two formats:
    1) legacy: top-level keys per size with 'runs' lists
    2) current: top-level 'experiments' list with dicts containing 'network_size', 'convergence_time', 'message_overhead'
    """
    sizes = []
    conv_means = []
    conv_stds = []
    ov_means = []
    ov_stds = []

    # Case: new format with experiments list
    if isinstance(data, dict) and 'experiments' in data and isinstance(data['experiments'], list):
        groups = {}
        for e in data['experiments']:
            try:
                n = int(e.get('network_size'))
            except Exception:
                continue
            groups.setdefault(n, []).append(e)

        for size_key in sorted(groups.keys()):
            runs = groups[size_key]
            convs = [float(r.get('convergence_time') or r.get('convergence_secs') or 0.0) for r in runs if r.get('convergence_time') is not None or r.get('convergence_secs') is not None]
            ovs = [float(r.get('message_overhead') or r.get('overhead') or 0.0) for r in runs if r.get('message_overhead') is not None or r.get('overhead') is not None]

            sizes.append(size_key)
            if convs:
                conv_means.append(mean(convs))
                conv_stds.append(stdev(convs) if len(convs) > 1 else 0.0)
            else:
                conv_means.append(0.0)
                conv_stds.append(0.0)

            if ovs:
                ov_means.append(mean(ovs))
                ov_stds.append(stdev(ovs) if len(ovs) > 1 else 0.0)
            else:
                ov_means.append(0.0)
                ov_stds.append(0.0)

        return sizes, conv_means, conv_stds, ov_means, ov_stds

    # Fallback: legacy format
    try:
        for size_key in sorted(data.keys(), key=lambda k: int(k)):
            entry = data[size_key]
            runs = entry.get('runs') if isinstance(entry, dict) else entry
            convs = []
            ovs = []
            if runs and isinstance(runs, list):
                for r in runs:
                    if isinstance(r, dict):
                        if 'convergence_secs' in r:
                            convs.append(r['convergence_secs'])
                        if 'overhead' in r:
                            ovs.append(r['overhead'])
                    elif isinstance(r, (int, float)):
                        convs.append(float(r))
            # fallback to values stored directly
            if not convs and isinstance(entry, list):
                convs = [float(x) for x in entry]

            sizes.append(int(size_key))
            if convs:
                conv_means.append(mean(convs))
                conv_stds.append(stdev(convs) if len(convs) > 1 else 0.0)
            else:
                conv_means.append(0.0)
                conv_stds.append(0.0)

            if ovs:
                ov_means.append(mean(ovs))
                ov_stds.append(stdev(ovs) if len(ovs) > 1 else 0.0)
            else:
                ov_means.append(0.0)
                ov_stds.append(0.0)
    except Exception:
        pass

    return sizes, conv_means, conv_stds, ov_means, ov_stds


def plot(sizes, means, errs, ylabel, outname):
    try:
        import matplotlib.pyplot as plt
    except ImportError:
        print('matplotlib is required to plot charts. Install with: pip3 install matplotlib')
        sys.exit(2)

    fig, ax = plt.subplots(figsize=(6, 4))
    ax.errorbar(sizes, means, yerr=errs, fmt='-o', capsize=5)
    ax.set_xlabel('Network size (N)')
    ax.set_ylabel(ylabel)
    ax.set_xticks(sizes)
    ax.grid(True, linestyle='--', alpha=0.4)
    fig.tight_layout()
    fig.savefig(outname)
    print(f'Wrote {outname}')


def plot_size20_variants(experiments):
    """Generate additional plots for N=20 showing fanout/ttl variants."""
    # group by (fanout, ttl)
    groups = {}
    for e in experiments:
        if e.get('network_size') != 20:
            continue
        key = (e.get('fanout'), e.get('ttl'))
        groups.setdefault(key, []).append(e)

    if not groups:
        return

    # compute means for each pair
    conv_stats = {}  # key -> (mean, std)
    ov_stats = {}
    for key, runs in groups.items():
        convs = [r['convergence_time'] for r in runs if 'convergence_time' in r]
        ovs = [r['message_overhead'] for r in runs if 'message_overhead' in r]
        if convs:
            conv_stats[key] = (mean(convs), stdev(convs) if len(convs) > 1 else 0.0)
        if ovs:
            ov_stats[key] = (mean(ovs), stdev(ovs) if len(ovs) > 1 else 0.0)

    try:
        import matplotlib.pyplot as plt
    except ImportError:
        print('matplotlib required for variant charts; skipping size-20 detail plots')
        return

    # plot convergence vs fanout colored by ttl
    plt.figure(figsize=(6,4))
    for (fanout, ttl), (m, s) in conv_stats.items():
        plt.errorbar(fanout, m, yerr=s, fmt='o', label=f'ttl={ttl}')
    plt.xlabel('fanout (N=20)')
    plt.ylabel('mean convergence (s)')
    plt.legend(title='TTL')
    plt.grid(True, linestyle='--', alpha=0.4)
    plt.tight_layout()
    plt.savefig('convergence_20_by_fanout.png')
    print('Wrote convergence_20_by_fanout.png')

    # plot convergence vs ttl colored by fanout
    plt.figure(figsize=(6,4))
    for (fanout, ttl), (m, s) in conv_stats.items():
        plt.errorbar(ttl, m, yerr=s, fmt='o', label=f'fanout={fanout}')
    plt.xlabel('ttl (N=20)')
    plt.ylabel('mean convergence (s)')
    plt.legend(title='Fanout')
    plt.grid(True, linestyle='--', alpha=0.4)
    plt.tight_layout()
    plt.savefig('convergence_20_by_ttl.png')
    print('Wrote convergence_20_by_ttl.png')

    # similar overhead plots
    plt.figure(figsize=(6,4))
    for (fanout, ttl), (m, s) in ov_stats.items():
        plt.errorbar(fanout, m, yerr=s, fmt='s', label=f'ttl={ttl}')
    plt.xlabel('fanout (N=20)')
    plt.ylabel('mean overhead')
    plt.legend(title='TTL')
    plt.grid(True, linestyle='--', alpha=0.4)
    plt.tight_layout()
    plt.savefig('overhead_20_by_fanout.png')
    print('Wrote overhead_20_by_fanout.png')

    plt.figure(figsize=(6,4))
    for (fanout, ttl), (m, s) in ov_stats.items():
        plt.errorbar(ttl, m, yerr=s, fmt='s', label=f'fanout={fanout}')
    plt.xlabel('ttl (N=20)')
    plt.ylabel('mean overhead')
    plt.legend(title='Fanout')
    plt.grid(True, linestyle='--', alpha=0.4)
    plt.tight_layout()
    plt.savefig('overhead_20_by_ttl.png')
    print('Wrote overhead_20_by_ttl.png')


def main():
    path = sys.argv[1] if len(sys.argv) > 1 else 'simulation_results.json'
    try:
        data = load_results(path)
    except FileNotFoundError:
        print(f'File not found: {path}')
        sys.exit(1)

    sizes, conv_means, conv_errs, ov_means, ov_errs = extract_metrics(data)

    if all(m == 0 for m in conv_means) and all(m == 0 for m in ov_means):
        print('No numeric metrics found in the JSON file. Please check its format.')
        sys.exit(1)

    plot(sizes, conv_means, conv_errs, 'Convergence time (s)', 'convergence.png')
    plot(sizes, ov_means, ov_errs, 'Message overhead (count)', 'overhead.png')

    # additional detailed charts for network size 20 parameter experiments
    if isinstance(data, dict) and 'experiments' in data:
        plot_size20_variants(data['experiments'])


if __name__ == '__main__':
    main()
