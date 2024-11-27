import json
import argparse
import os
import sys

def verify_snapshot(save_file):
    if not os.path.exists(save_file):
        print(f"Save file {save_file} does not exist.")
        return

    with open(save_file, 'r') as f:
        try:
            data = json.load(f)
        except json.JSONDecodeError as e:
            print(f"Failed to parse JSON from {save_file}: {e}")
            return

    current_term = data.get('current_term')
    voted_for = data.get('voted_for')
    log = data.get('log', [])
    snapshot = data.get('snapshot')
    last_included_index = data.get('last_included_index')
    last_included_term = data.get('last_included_term')

    print(f"=== Verification for {save_file} ===")
    print(f"Current Term: {current_term}")
    print(f"Voted For: {voted_for}")
    print(f"Last Included Index: {last_included_index}")
    print(f"Last Included Term: {last_included_term}")
    print(f"Number of Log Entries: {len(log)}")
    print(f"Snapshot Exists: {'Yes' if snapshot else 'No'}")

    if snapshot:
        # Verify that log is truncated appropriately
        expected_log_length = last_included_index + 1  # +1 for the dummy entry at index 0
        actual_log_length = len(log)
        if actual_log_length == expected_log_length:
            print(f"Log truncation verified: Log length is {actual_log_length} (expected {expected_log_length}).")
        else:
            print(f"Log truncation FAILED: Log length is {actual_log_length} (expected {expected_log_length}).")

        # verify snapshot data matches the key-value store
        # requires that the snapshot includes the key-value store data
        snapshot_data = snapshot.get('data')
        if snapshot_data:
            print(f"Snapshot Data: {snapshot_data}")
        else:
            print("Snapshot data is missing.")
    else:
        print("Snapshot does not exist. Log compaction has not been triggered.")

    print("=== End of Verification ===\n")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Verify Raft Log Compaction")
    parser.add_argument("--save-file", type=str, required=True, help="Path to the save file (e.g., n1.save)")

    args = parser.parse_args()

    verify_snapshot(args.save_file)
