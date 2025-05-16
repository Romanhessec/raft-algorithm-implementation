import json
import os

LOG_DIR = "logs"

def load_logs():
    logs = {}
    for fname in os.listdir(LOG_DIR):
        if fname.endswith("_log.json"):
            node_id = fname.split("_")[0].replace("node", "")
            with open(os.path.join(LOG_DIR, fname)) as f:
                logs[node_id] = json.load(f)
    return logs

def compare_logs(logs):
    print("Checking log consistency across nodes...\n")

    reference_id, reference_log = next(iter(logs.items()))
    consistent = True

    for node_id, log in logs.items():
        min_len = min(len(reference_log), len(log))
        for i in range(min_len):
            if reference_log[i] != log[i]:
                print(f"Mismatch at index {i} between node {reference_id} and node {node_id}:")
                print(f"    {reference_id}: {reference_log[i]}")
                print(f"    {node_id}: {log[i]}")
                consistent = False
                break
        else:
            # Logs match up to min_len, now check length
            if len(log) != len(reference_log):
                print(f"Node {node_id} has different log length ({len(log)}) vs Node {reference_id} ({len(reference_log)})")
                consistent = False
            else:
                print(f"Node {node_id}'s log matches reference node {reference_id}.")

    if consistent:
        print("\nAll nodes have consistent logs.\n")
    else:
        print("\nInconsistencies found in committed logs.\n")

if __name__ == "__main__":
    logs = load_logs()
    if not logs:
        print("No logs found in 'logs/' directory.")
    else:
        compare_logs(logs)
