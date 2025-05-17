from multiprocessing import Process, Queue
from multiprocessing.managers import BaseManager
from multiprocessing import Manager
from node import RaftNode
import time
import threading

# client queue for CLI
client_queue = Queue()

# register client queue for sharing
class ClientManager(BaseManager): pass
ClientManager.register('get_client_queue', callable=lambda: client_queue)

# partition table queue for network partitioning
manager = Manager()
blocked_peers = manager.list()  # holds tuples like (0, 1)

def start_node(node_id, peers, message_queues, client_queue, blocked_peers):
    node = RaftNode(node_id, peers, message_queues, client_queue, blocked_peers)
    node.run()

def create_partition(blocked_peers, node_a, node_b, duration=10):
    """
    Temporarily block communication between node_a and node_b for 'duration' seconds.
    This blocks messages in both directions.
    """
    if (node_a, node_b) in blocked_peers or (node_b, node_a) in blocked_peers:
        print(f"[PARTITION] Partition already active between {node_a} and {node_b}")
        return

    blocked_peers.append((node_a, node_b))
    blocked_peers.append((node_b, node_a))
    print(f"[PARTITION] Simulated partition between node {node_a} and node {node_b}")

    def heal():
        time.sleep(duration)
        try:
            blocked_peers.remove((node_a, node_b))
            blocked_peers.remove((node_b, node_a))
            print(f"[PARTITION] Healed partition between node {node_a} and node {node_b}")
        except ValueError:
            pass  # already removed

    threading.Thread(target=heal, daemon=True).start() # we don't want the whole main.py thread to be stuck!

if __name__ == '__main__':
    num_nodes = 3
    processes = {}
    partition_active = False

    # Step 3: Start the manager after registering
    manager = ClientManager(address=('', 50000), authkey=b'raft')
    manager.start()

    message_queues = {i: Queue() for i in range(num_nodes)}

    for i in range(num_nodes):
        peers = [j for j in range(num_nodes) if j != i]
        p = Process(target=start_node, args=(i, peers, message_queues, client_queue, blocked_peers))
        p.start()
        processes[i] = p

    try:
        while True:
            for i, p in list(processes.items()):
                if not p.is_alive():
                    print(f"[RECOVERY] Node {i} crashed. Restarting...")
                    peers = [j for j in range(num_nodes) if j != i]
                    new_p = Process(target=start_node, args=(i, peers, message_queues, client_queue, blocked_peers))
                    new_p.start()
                    processes[i] = new_p
            
            # network partition logic
            # example: trigger a partition after 10s
            if time.time() > 10 and not partition_active:
                create_partition(blocked_peers, 0, 1, duration=10)
                partition_active = True

            time.sleep(1)
    except KeyboardInterrupt:
        print("Shutting down cluster...")
        for p in processes.values():
            p.terminate()
        for p in processes.values():
            p.join()
