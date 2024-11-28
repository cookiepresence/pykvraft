import subprocess
import time
import unittest
import random


def start_node(node_id, port, peers, save_file, load=False):
    """Start a Raft node as a subprocess."""
    command = [
        "python3", "src/server.py",  # Replace with the script that starts the node
        "--node-id", node_id,
        "--port", str(port),
        "--peers", *list(map(str, peers)),
        "--save-file", save_file
    ]
    if load:
        command += ["--load"]

    # Start the node as a subprocess
    process = subprocess.Popen(command, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    return process


def send_command(process, command):
    """Send a command to the node process and get its response."""
    process.stdin.write(command + '\n')  # Send the command to stdin
    process.stdin.flush()  # Make sure the input is sent immediately
    output = process.stdout.readline()  # Read stdout and stderr
    return output


def stop_node(process):
    """Terminate a node process."""
    process.stdin.write("stop\n")
    process.stdin.flush()
    process.stdin.close()
    process.stdout.close()
    process.stderr.close()
    process.terminate()
    try:
        process.wait(timeout=5)  # Wait up to 5 seconds for the process to stop
    except subprocess.TimeoutExpired:
        process.kill()  # Force kill the process if it takes too long


class TestRaftCluster(unittest.TestCase):
    def setUp(self):
        """Start the cluster before each test."""
        print("Starting the cluster...")
        self.node_ids = ["node0", "node1", "node2"]
        self.ports = [5001, 5002, 5003]
        self.processes = []

        # Spin up nodes
        for node_id, port in zip(self.node_ids, self.ports):
            print(f"Starting {node_id} on port {port}...")
            process = start_node(node_id, port, set(self.ports) - {port}, f"{node_id}.json")
            self.processes.append(process)

        # allow the nodes to settle down
        time.sleep(3)

    def tearDown(self):
        """Stop the cluster after each test."""
        print("\nShutting down the cluster...")
        for process in self.processes:
            stop_node(process)
        print("Cluster shut down successfully.")

    def test_node_alive(self):
        """Test that each node is alive."""
        for node_id, process in zip(self.node_ids, self.processes):
            command = "exec print('hello world')"
            output = send_command(process, command)
            print(f"Output from {node_id}: {output[12:-1]}")  # stripping new lines

    def check_one_leader(self):
        num_leaders = 0
        leader_idx = -1
        for idx, (node_id, process) in enumerate(zip(self.node_ids, self.processes)):
            command = "exec print(raft.status)"
            output = send_command(process, command)
            if 'Leader' in output:
                num_leaders += 1
                leader_idx = idx
            print(f"Output from {node_id}: {output[12:-1]}")
        self.assertEqual(num_leaders, 1, "there should only be one leader")
        return leader_idx

    def test_single_leader(self):
        """Test that there is only a single leader"""
        self.check_one_leader()

    def test_terms(self):
        terms = []
        for node_id, process in zip(self.node_ids, self.processes):
            command = "exec print(raft.state.persistent_state.current_term)"
            output = send_command(process, command)
            print(f"Output from {node_id}: {output[12:-1]}")
            terms.append(int(output.strip()[12:]))

        self.assertEqual(len(set(terms)), 1, "all terms should be the same")

    def test_disconnect(self):
        idx = random.randrange(len(self.processes))
        stop_node(self.processes[idx])

        node_id = self.node_ids[idx]
        port = self.ports[idx]
        self.processes[idx] = start_node(node_id, port, set(self.ports) - {port}, f"{node_id}.json")

        # wait for the cluster to stabilise
        time.sleep(1)
        self.check_one_leader()

    def test_replication(self):
        leader_idx = self.check_one_leader()
        leader = self.processes[leader_idx]

        # check if the command has been successful
        output = send_command(leader, "set foo bar")
        self.assertIn("succeeded", output)

        # wait for commands to commit
        time.sleep(1)

        # check if command has been replicated successfully
        for node_id, process in zip(self.node_ids, self.processes):
            command = "get foo"
            output = send_command(process, command)
            print(f"Output from {node_id}: {output[12:-1]}")
            self.assertIn("bar", output)

    # def test_key_value_operations(self):
    #     """Test setting and getting key-value pairs."""
    #     for node_id, process in zip(self.node_ids, self.processes):
    #         command = "set key1 value1"  # Replace with the correct command to set key-value
    #         output, errors = send_command(process, command)
    #         print(f"Output from {node_id}: {output}")
    #         self.assertIsNone(errors, f"Errors from {node_id}: {errors}")
    #         self.assertIn("SET", output, f"Key-value set failed in {node_id}")

    #         command = "get key1"  # Replace with the correct command to get key-value
    #         output, errors = send_command(process, command)
    #         print(f"Output from {node_id}: {output}")
    #         self.assertIsNone(errors, f"Errors from {node_id}: {errors}")
    #         self.assertIn("value1", output, f"Key retrieval failed in {node_id}")


class TestLargeRaftCluster(unittest.TestCase):
    def setUp(self):
        """Start the cluster before each test."""
        print("Starting the cluster...")
        self.node_ids = [f"node{i}" for i in range(40)]
        self.ports = [5000 + i for i in range(40)]
        self.processes = []

        # Spin up nodes
        for node_id, port in zip(self.node_ids, self.ports):
            print(f"Starting {node_id} on port {port}...")
            process = start_node(node_id, port, set(self.ports) - {port}, f"{node_id}.json")
            self.processes.append(process)

    def tearDown(self):
        """Stop the cluster after each test."""
        print("\nShutting down the cluster...")
        for process in self.processes:
            stop_node(process)
        print("Cluster shut down successfully.")

    def check_one_leader(self):
        num_leaders = 0
        leader_idx = -1
        for idx, (node_id, process) in enumerate(zip(self.node_ids, self.processes)):
            command = "exec print(raft.status)"
            output = send_command(process, command)
            if 'Leader' in output:
                num_leaders += 1
                leader_idx = idx
            print(f"Output from {node_id}: {output[12:-1]}")
        # self.assertEqual(num_leaders, 1, "there should only be one leader")
        return leader_idx, num_leaders

    def test_single_leader(self):
        """Test that there is only a single leader"""
        time.sleep(1)
        while True:
            _, num_leaders = self.check_one_leader()
            if num_leaders == 1:
                break
            time.sleep(1)
    
if __name__ == "__main__":
    unittest.main()
