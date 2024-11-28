import subprocess
import time
import random

def start_node(node_id, port, peers, save_file, load=False):
    """Start a Raft node as a subprocess."""
    command = [
        "python3", "src/server.py",  # Replace with your actual server script
        "--node-id", node_id,
        "--port", str(port),
        "--peers", *list(map(str, peers)),
        "--save-file", save_file
    ]
    if load:
        command += ["--load"]

    process = subprocess.Popen(command, stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True)
    return process


def send_command(process, command):
    """Send a command to the node process and get its response."""
    process.stdin.write(command + '\n')
    process.stdin.flush()
    output = process.stdout.readline()
    return output


def stop_node(process):
    """Terminate a node process."""
    process.stdin.write("stop\n")
    process.stdin.flush()
    process.stdin.close()
    process.stdout.close()
    if process.stderr is not None:
        process.stderr.close()
    process.terminate()
    try:
        process.wait(timeout=5)
    except subprocess.TimeoutExpired:
        process.kill()


def benchmark_leader_election(num_nodes, base_port=5000):
    """Benchmark the time taken for leader election with a given number of nodes."""
    processes = []
    node_ids = [f"node{i}" for i in range(num_nodes)]
    ports = [base_port + i for i in range(num_nodes)]

    try:
        # Start nodes
        for node_id, port in zip(node_ids, ports):
            peers = set(ports) - {port}
            process = start_node(node_id, port, peers, f"{node_id}.json")
            processes.append(process)

        # Measure election time
        start_time = time.time()
        leader_elected = False
        while not leader_elected:
            num_leaders = 0
            num_followers = 0
            outputs = []
            for process in processes:
                output = send_command(process, "exec print(raft.status)")
                outputs.append(output.strip()[12:])
                if 'Leader' in output:
                    num_leaders += 1
                elif 'Follower' in output:
                    num_followers += 1
            print(f"{time.time():.3f} {outputs}")
            if num_leaders == 1 and num_followers + num_leaders == num_nodes:
                leader_elected = True
            time.sleep(0.01)  # Avoid tight polling
        print()
        election_time = time.time() - start_time
        print(f"Leader election with {num_nodes} nodes took {election_time:.2f} seconds.")
        return election_time

    finally:
        # Cleanup
        for process in processes:
            stop_node(process)


def main():
    """Run the benchmarking script for different numbers of nodes."""
    num_trials = 3  # Number of times to repeat each test
    results = {}

    for num_nodes in range(3, 40):  # Test from 3 to 10 nodes
        print(f"Benchmarking with {num_nodes} nodes...")
        times = []
        for trial in range(num_trials):
            election_time = benchmark_leader_election(num_nodes)
            times.append(election_time)

        avg_time = sum(times) / num_trials
        results[num_nodes] = {
            "times": times,
            "average": avg_time
        }

    # Print summary
    print("\nBenchmarking Results:")
    for num_nodes, data in results.items():
        print(f"{num_nodes} nodes: {data['times']} (avg: {data['average']:.2f} seconds)")


if __name__ == "__main__":
    main()
